package producer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/internal/repository"
)

type producerRepo interface {
	PushTask(ctx context.Context, task *repository.PushTaskParams) (int64, error)
	PushTaskWithExecutor(ctx context.Context, exec asynqpg.Querier, task *repository.PushTaskParams) (int64, error)
	PushTasks(ctx context.Context, params repository.PushTasksParams) ([]int64, error)
	PushTasksWithExecutor(ctx context.Context, exec asynqpg.Querier, params repository.PushTasksParams) ([]int64, error)
}

type Producer struct {
	repo            producerRepo
	logger          *slog.Logger
	defaultMaxRetry int
	metrics         *asynqpg.Metrics
	tracer          trace.Tracer
}

type Config struct {
	Pool            asynqpg.Pool
	Logger          *slog.Logger
	DefaultMaxRetry int

	// MeterProvider for metrics. If nil, global OTel MeterProvider is used.
	MeterProvider metric.MeterProvider
	// TracerProvider for tracing. If nil, global OTel TracerProvider is used.
	TracerProvider trace.TracerProvider
}

func New(config Config) (*Producer, error) {
	if config.Pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	m, err := asynqpg.NewMetrics(config.MeterProvider)
	if err != nil {
		return nil, fmt.Errorf("create metrics: %w", err)
	}

	producer := &Producer{
		repo:            repository.NewRepository(config.Pool),
		logger:          config.Logger,
		defaultMaxRetry: config.DefaultMaxRetry,
		metrics:         m,
		tracer:          asynqpg.NewTracer(config.TracerProvider),
	}

	producer.setDefaults()
	return producer, nil
}

func (p *Producer) setDefaults() {
	if p.logger == nil {
		p.logger = slog.Default()
	}

	if p.defaultMaxRetry <= 0 {
		p.defaultMaxRetry = 3
	}
}

func (p *Producer) Enqueue(ctx context.Context, task *asynqpg.Task, opts ...EnqueueOption) (int64, error) {
	err := validateTask(task)
	if err != nil {
		return 0, err
	}

	ctx, span := p.tracer.Start(ctx, "asynqpg.enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(asynqpg.AttrTaskType.String(task.Type)),
	)
	defer span.End()

	delay := p.calculateDelay(task)
	maxRetry := p.calculateMaxRetry(task)

	span.SetAttributes(attribute.Bool("has_delay", delay > 0))

	params := &repository.PushTaskParams{
		Type:             task.Type,
		IdempotencyToken: task.IdempotencyToken,
		Payload:          task.Payload,
		Delay:            db.NewDuration(delay),
		AttemptsLeft:     maxRetry,
	}

	start := time.Now()
	id, err := p.repo.PushTask(ctx, params)
	dur := time.Since(start)

	taskTypeAttr := asynqpg.AttrTaskType.String(task.Type)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue failed")
		p.metrics.TasksErrors.Add(ctx, 1, metric.WithAttributes(
			taskTypeAttr, asynqpg.AttrErrorType.String(asynqpg.ErrorTypeDB),
		))
		return 0, fmt.Errorf("enqueue task: %w", err)
	}

	span.SetAttributes(attribute.Int64("task_id", id))

	p.metrics.TasksEnqueued.Add(ctx, 1, metric.WithAttributes(taskTypeAttr))
	p.metrics.EnqueueDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(taskTypeAttr))

	p.logger.Info("task enqueued successfully",
		"task_id", id,
		"task_type", task.Type,
		"delay", delay,
		"max_retry", maxRetry,
		"idempotency_token", ptr.DerefOrDefault(task.IdempotencyToken, ""),
	)

	return id, nil
}

func (p *Producer) calculateDelay(task *asynqpg.Task) time.Duration {
	delay := task.Delay
	if !task.ProcessAt.IsZero() {
		delay = time.Until(task.ProcessAt)
		if delay < 0 {
			delay = 0
		}
	}
	return delay
}

func (p *Producer) calculateMaxRetry(task *asynqpg.Task) int {
	if task.MaxRetry != nil {
		return *task.MaxRetry
	}
	return p.defaultMaxRetry
}

// EnqueueTx enqueues a task using the provided executor (typically a transaction).
// This allows the task enqueue to be part of a larger transaction,
// ensuring atomicity with other database operations.
func (p *Producer) EnqueueTx(ctx context.Context, tx asynqpg.Querier, task *asynqpg.Task, opts ...EnqueueOption) (int64, error) {
	if tx == nil {
		return 0, fmt.Errorf("executor cannot be nil")
	}

	err := validateTask(task)
	if err != nil {
		return 0, err
	}

	ctx, span := p.tracer.Start(ctx, "asynqpg.enqueue",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(asynqpg.AttrTaskType.String(task.Type)),
	)
	defer span.End()

	delay := p.calculateDelay(task)
	maxRetry := p.calculateMaxRetry(task)

	params := &repository.PushTaskParams{
		Type:             task.Type,
		IdempotencyToken: task.IdempotencyToken,
		Payload:          task.Payload,
		Delay:            db.NewDuration(delay),
		AttemptsLeft:     maxRetry,
	}

	start := time.Now()
	id, err := p.repo.PushTaskWithExecutor(ctx, tx, params)
	dur := time.Since(start)

	taskTypeAttr := asynqpg.AttrTaskType.String(task.Type)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue failed")
		p.metrics.TasksErrors.Add(ctx, 1, metric.WithAttributes(
			taskTypeAttr, asynqpg.AttrErrorType.String(asynqpg.ErrorTypeDB),
		))
		p.logger.Error("enqueue task in transaction",
			"task_type", task.Type,
			"error", err,
		)
		return 0, fmt.Errorf("enqueue task: %w", err)
	}

	span.SetAttributes(attribute.Int64("task_id", id))

	p.metrics.TasksEnqueued.Add(ctx, 1, metric.WithAttributes(taskTypeAttr))
	p.metrics.EnqueueDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(taskTypeAttr))

	p.logger.Info("task enqueued successfully in transaction",
		"task_id", id,
		"task_type", task.Type,
		"delay", delay,
		"max_retry", maxRetry,
		"has_idempotency_token", task.IdempotencyToken != nil,
	)

	return id, nil
}

// EnqueueMany enqueues multiple tasks in a single SQL call using UNNEST-based
// batch insert. Returns the IDs of inserted tasks; duplicates (by idempotency
// token) are skipped.
//
// No automatic batch splitting is performed. The UNNEST approach uses one array
// parameter per column, so the PostgreSQL 65535 query parameter
// limit does not apply. However, very large batches (100k+) may hit other limits
// such as memory pressure, wire protocol message size, or statement timeouts.
func (p *Producer) EnqueueMany(ctx context.Context, tasks []*asynqpg.Task) ([]int64, error) {
	if len(tasks) == 0 {
		return nil, nil
	}

	ctx, span := p.tracer.Start(ctx, "asynqpg.enqueue_many",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attribute.Int("batch_size", len(tasks))),
	)
	defer span.End()

	err := validateTasks(tasks)
	if err != nil {
		return nil, err
	}

	start := time.Now()

	ids, err := p.enqueueBatch(ctx, tasks)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "batch enqueue failed")
		return nil, err
	}

	p.recordBatchMetrics(ctx, tasks, time.Since(start))

	p.logger.Info("batch enqueue completed",
		"total_tasks", len(tasks),
		"inserted", len(ids),
	)

	return ids, nil
}

func (p *Producer) recordBatchMetrics(ctx context.Context, tasks []*asynqpg.Task, dur time.Duration) {
	counts := make(map[string]int64)
	for _, t := range tasks {
		counts[t.Type]++
	}
	for taskType, count := range counts {
		attrs := metric.WithAttributes(asynqpg.AttrTaskType.String(taskType))
		p.metrics.TasksEnqueued.Add(ctx, count, attrs)
		p.metrics.EnqueueDuration.Record(ctx, dur.Seconds(), attrs)
	}
}

func (p *Producer) enqueueBatch(ctx context.Context, tasks []*asynqpg.Task) ([]int64, error) {
	repoParams := make([]repository.PushTaskParams, len(tasks))
	for i, task := range tasks {
		delay := p.calculateDelay(task)
		maxRetry := p.calculateMaxRetry(task)

		repoParams[i] = repository.PushTaskParams{
			Type:             task.Type,
			IdempotencyToken: task.IdempotencyToken,
			Payload:          task.Payload,
			Delay:            db.NewDuration(delay),
			AttemptsLeft:     maxRetry,
		}
	}

	ids, err := p.repo.PushTasks(ctx, repository.PushTasksParams{Tasks: repoParams})
	if err != nil {
		return nil, fmt.Errorf("batch insert tasks: %w", err)
	}

	return ids, nil
}

// EnqueueManyTx enqueues multiple tasks in a single batch operation using the provided executor.
// This allows the batch enqueue to be part of a larger transaction.
func (p *Producer) EnqueueManyTx(ctx context.Context, tx asynqpg.Querier, tasks []*asynqpg.Task) ([]int64, error) {
	if tx == nil {
		return nil, fmt.Errorf("executor cannot be nil")
	}

	if len(tasks) == 0 {
		return nil, nil
	}

	err := validateTasks(tasks)
	if err != nil {
		return nil, err
	}

	repoParams := make([]repository.PushTaskParams, len(tasks))
	for i, task := range tasks {
		delay := p.calculateDelay(task)
		maxRetry := p.calculateMaxRetry(task)

		repoParams[i] = repository.PushTaskParams{
			Type:             task.Type,
			IdempotencyToken: task.IdempotencyToken,
			Payload:          task.Payload,
			Delay:            db.NewDuration(delay),
			AttemptsLeft:     maxRetry,
		}
	}

	start := time.Now()
	ids, err := p.repo.PushTasksWithExecutor(ctx, tx, repository.PushTasksParams{Tasks: repoParams})
	dur := time.Since(start)

	if err != nil {
		p.logger.Error("failed to batch enqueue tasks in transaction",
			"count", len(tasks),
			"error", err,
		)
		return nil, fmt.Errorf("batch insert tasks: %w", err)
	}

	p.recordBatchMetrics(ctx, tasks, dur)

	p.logger.Info("batch enqueue in transaction completed",
		"total_tasks", len(tasks),
		"inserted", len(ids),
	)

	return ids, nil
}

// todo: add enqueue option to enqueue many methods, add enqueue option with max batch size for auto-chunking

// EnqueueOption configures enqueue behavior.
// Reserved for future use (e.g., queue selection, priority, tags).
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct{}

func validateTask(task *asynqpg.Task) error {
	if task == nil {
		return errors.New("task cannot be nil")
	}

	if task.Type == "" {
		return errors.New("task type cannot be empty")
	}

	if task.Payload == nil {
		return errors.New("task payload cannot be nil")
	}

	return nil
}

func validateTasks(tasks []*asynqpg.Task) error {
	for i, task := range tasks {
		err := validateTask(task)
		if err != nil {
			return fmt.Errorf("validate task at index %d: %w", i, err)
		}
	}

	return nil
}
