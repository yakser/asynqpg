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
	PushTask(ctx context.Context, task *repository.PushTaskParams) (repository.PushTaskResult, error)
	PushTaskWithExecutor(ctx context.Context, exec asynqpg.Querier, task *repository.PushTaskParams) (repository.PushTaskResult, error)
	PushTasks(ctx context.Context, params repository.PushTasksParams) ([]repository.PushTaskResult, error)
	PushTasksWithExecutor(ctx context.Context, exec asynqpg.Querier, params repository.PushTasksParams) ([]repository.PushTaskResult, error)
}

// EnqueueResult contains the result of a single task enqueue operation.
type EnqueueResult struct {
	// ID is the database ID of the task (existing ID if duplicate).
	ID int64
	// Duplicate is true when a task with the same type and idempotency token already existed.
	Duplicate bool
}

// EnqueueManyResult contains the results of a batch enqueue operation.
type EnqueueManyResult struct {
	// Results contains per-task results in the same order as the input.
	Results []EnqueueResult
}

// InsertedCount returns the number of tasks that were newly inserted (not duplicates).
func (r *EnqueueManyResult) InsertedCount() int {
	count := 0
	for _, res := range r.Results {
		if !res.Duplicate {
			count++
		}
	}
	return count
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

// Enqueue inserts a single task into the queue. Returns an EnqueueResult with
// the task's database ID and a Duplicate flag. When the task has an idempotency
// token that already exists, Duplicate is true and ID is the existing task's ID.
// Duplicates are not errors. Task delay is determined by ProcessAt (absolute) or
// Delay (relative); max retry falls back to the producer's DefaultMaxRetry.
func (p *Producer) Enqueue(ctx context.Context, task *asynqpg.Task, opts ...EnqueueOption) (*EnqueueResult, error) {
	err := validateTask(task)
	if err != nil {
		return nil, err
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
	result, err := p.repo.PushTask(ctx, params)
	dur := time.Since(start)

	taskTypeAttr := asynqpg.AttrTaskType.String(task.Type)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue failed")
		p.metrics.TasksErrors.Add(ctx, 1, metric.WithAttributes(
			taskTypeAttr, asynqpg.AttrErrorType.String(asynqpg.ErrorTypeDB),
		))
		return nil, fmt.Errorf("enqueue task: %w", err)
	}

	span.SetAttributes(attribute.Int64("task_id", result.ID))

	if result.Duplicate {
		span.SetAttributes(attribute.Bool("duplicate", true))
		p.logger.Info("task already exists",
			"task_id", result.ID,
			"task_type", task.Type,
			"idempotency_token", ptr.DerefOrDefault(task.IdempotencyToken, ""),
		)
	} else {
		p.metrics.TasksEnqueued.Add(ctx, 1, metric.WithAttributes(taskTypeAttr))
		p.logger.Info("task enqueued successfully",
			"task_id", result.ID,
			"task_type", task.Type,
			"delay", delay,
			"max_retry", maxRetry,
			"idempotency_token", ptr.DerefOrDefault(task.IdempotencyToken, ""),
		)
	}

	p.metrics.EnqueueDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(taskTypeAttr))

	return &EnqueueResult{ID: result.ID, Duplicate: result.Duplicate}, nil
}

// EnqueueTx enqueues a task using the provided Querier (typically a
// transaction). This allows the task enqueue to be part of a larger
// transaction, ensuring atomicity with other database operations.
func (p *Producer) EnqueueTx(ctx context.Context, tx asynqpg.Querier, task *asynqpg.Task, opts ...EnqueueOption) (*EnqueueResult, error) {
	if tx == nil {
		return nil, fmt.Errorf("querier cannot be nil")
	}

	err := validateTask(task)
	if err != nil {
		return nil, err
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
	result, err := p.repo.PushTaskWithExecutor(ctx, tx, params)
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
		return nil, fmt.Errorf("enqueue task: %w", err)
	}

	span.SetAttributes(attribute.Int64("task_id", result.ID))

	if result.Duplicate {
		span.SetAttributes(attribute.Bool("duplicate", true))
		p.logger.Info("task already exists in transaction",
			"task_id", result.ID,
			"task_type", task.Type,
			"has_idempotency_token", task.IdempotencyToken != nil,
		)
	} else {
		p.metrics.TasksEnqueued.Add(ctx, 1, metric.WithAttributes(taskTypeAttr))
		p.logger.Info("task enqueued successfully in transaction",
			"task_id", result.ID,
			"task_type", task.Type,
			"delay", delay,
			"max_retry", maxRetry,
			"has_idempotency_token", task.IdempotencyToken != nil,
		)
	}

	p.metrics.EnqueueDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(taskTypeAttr))

	return &EnqueueResult{ID: result.ID, Duplicate: result.Duplicate}, nil
}

// EnqueueMany enqueues multiple tasks in a single SQL call using UNNEST-based
// batch insert. Returns per-task results with IDs and duplicate flags.
//
// No automatic batch splitting is performed. The UNNEST approach uses one array
// parameter per column, so the PostgreSQL 65535 query parameter
// limit does not apply. However, very large batches (100k+) may hit other limits
// such as memory pressure, wire protocol message size, or statement timeouts.
func (p *Producer) EnqueueMany(ctx context.Context, tasks []*asynqpg.Task) (*EnqueueManyResult, error) {
	if len(tasks) == 0 {
		return &EnqueueManyResult{}, nil
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

	repoResults, err := p.enqueueBatch(ctx, tasks)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "batch enqueue failed")
		return nil, err
	}

	result := toBatchResult(repoResults)

	p.recordBatchMetrics(ctx, tasks, repoResults, time.Since(start))

	p.logger.Info("batch enqueue completed",
		"total_tasks", len(tasks),
		"inserted", result.InsertedCount(),
		"duplicates", len(tasks)-result.InsertedCount(),
	)

	return result, nil
}

func (p *Producer) recordBatchMetrics(ctx context.Context, tasks []*asynqpg.Task, results []repository.PushTaskResult, dur time.Duration) {
	counts := make(map[string]int64)
	for i, t := range tasks {
		if i < len(results) && !results[i].Duplicate {
			counts[t.Type]++
		}
	}
	for taskType, count := range counts {
		attrs := metric.WithAttributes(asynqpg.AttrTaskType.String(taskType))
		p.metrics.TasksEnqueued.Add(ctx, count, attrs)
	}
	// Record duration for all task types in the batch.
	seen := make(map[string]bool)
	for _, t := range tasks {
		if !seen[t.Type] {
			seen[t.Type] = true
			attrs := metric.WithAttributes(asynqpg.AttrTaskType.String(t.Type))
			p.metrics.EnqueueDuration.Record(ctx, dur.Seconds(), attrs)
		}
	}
}

func (p *Producer) enqueueBatch(ctx context.Context, tasks []*asynqpg.Task) ([]repository.PushTaskResult, error) {
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

	results, err := p.repo.PushTasks(ctx, repository.PushTasksParams{Tasks: repoParams})
	if err != nil {
		return nil, fmt.Errorf("batch insert tasks: %w", err)
	}

	return results, nil
}

// EnqueueManyTx enqueues multiple tasks in a single batch operation using the
// provided Querier (typically a transaction). This allows the batch enqueue to
// be part of a larger transaction, ensuring atomicity with other database
// operations.
func (p *Producer) EnqueueManyTx(ctx context.Context, tx asynqpg.Querier, tasks []*asynqpg.Task) (*EnqueueManyResult, error) {
	if tx == nil {
		return nil, fmt.Errorf("querier cannot be nil")
	}

	if len(tasks) == 0 {
		return &EnqueueManyResult{}, nil
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
	repoResults, err := p.repo.PushTasksWithExecutor(ctx, tx, repository.PushTasksParams{Tasks: repoParams})
	dur := time.Since(start)

	if err != nil {
		p.logger.Error("failed to batch enqueue tasks in transaction",
			"count", len(tasks),
			"error", err,
		)
		return nil, fmt.Errorf("batch insert tasks: %w", err)
	}

	result := toBatchResult(repoResults)

	p.recordBatchMetrics(ctx, tasks, repoResults, dur)

	p.logger.Info("batch enqueue in transaction completed",
		"total_tasks", len(tasks),
		"inserted", result.InsertedCount(),
		"duplicates", len(tasks)-result.InsertedCount(),
	)

	return result, nil
}

func toBatchResult(repoResults []repository.PushTaskResult) *EnqueueManyResult {
	results := make([]EnqueueResult, len(repoResults))
	for i, r := range repoResults {
		results[i] = EnqueueResult{ID: r.ID, Duplicate: r.Duplicate}
	}
	return &EnqueueManyResult{Results: results}
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
