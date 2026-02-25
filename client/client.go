package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/repository"
)

// taskRepository defines the repository operations needed by Client.
type taskRepository interface {
	GetTaskByID(ctx context.Context, id int64) (*repository.FullTask, error)
	GetTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*repository.FullTask, error)
	ListTasks(ctx context.Context, params repository.ListTasksParams) (*repository.ListTasksResult, error)
	ListTasksWithExecutor(ctx context.Context, exec asynqpg.Querier, params repository.ListTasksParams) (*repository.ListTasksResult, error)
	CancelTaskByID(ctx context.Context, id int64) (*repository.FullTask, bool, error)
	CancelTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*repository.FullTask, bool, error)
	RetryTaskByID(ctx context.Context, id int64) (*repository.FullTask, bool, error)
	RetryTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*repository.FullTask, bool, error)
	DeleteTaskByID(ctx context.Context, id int64) (*repository.FullTask, bool, error)
	DeleteTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*repository.FullTask, bool, error)
}

// Client provides task management and inspection operations.
// It is separate from Producer (which creates tasks) and Consumer (which processes them).
type Client struct {
	repo   taskRepository
	logger *slog.Logger
	tracer trace.Tracer
}

// Config holds configuration for creating a new Client.
type Config struct {
	Pool   asynqpg.Pool
	Logger *slog.Logger
	// TracerProvider for tracing. If nil, global OTel TracerProvider is used.
	TracerProvider trace.TracerProvider
}

// New creates a new Client with the given configuration.
func New(config Config) (*Client, error) {
	if config.Pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Client{
		repo:   repository.NewRepository(config.Pool),
		logger: logger,
		tracer: asynqpg.NewTracer(config.TracerProvider),
	}, nil
}

//nolint:unused // used in unit tests within the same package
func newWithRepo(repo taskRepository) *Client {
	return &Client{repo: repo, logger: slog.Default(), tracer: asynqpg.NewTracer(nil)}
}

// GetTask returns the full information about a task by its ID.
func (c *Client) GetTask(ctx context.Context, id int64) (*TaskInfo, error) {
	ctx, span := c.tracer.Start(ctx, "asynqpg.get_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, err := c.repo.GetTaskByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "get task failed")
		return nil, fmt.Errorf("get task: %w", err)
	}

	return fullTaskToInfo(task), nil
}

// GetTaskTx returns the full information about a task using the provided executor.
func (c *Client) GetTaskTx(ctx context.Context, tx asynqpg.Querier, id int64) (*TaskInfo, error) {
	if tx == nil {
		return nil, fmt.Errorf("executor cannot be nil")
	}

	ctx, span := c.tracer.Start(ctx, "asynqpg.get_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, err := c.repo.GetTaskByIDWithExecutor(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "get task failed")
		return nil, fmt.Errorf("get task: %w", err)
	}

	return fullTaskToInfo(task), nil
}

// ListTasks returns tasks matching the given filters with pagination.
func (c *Client) ListTasks(ctx context.Context, params *ListParams) (*ListResult, error) {
	if params == nil {
		params = NewListParams()
	}

	ctx, span := c.tracer.Start(ctx, "asynqpg.list_tasks",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	repoParams := params.toRepoParams()

	result, err := c.repo.ListTasks(ctx, repoParams)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "list tasks failed")
		return nil, fmt.Errorf("list tasks: %w", err)
	}

	span.SetAttributes(attribute.Int("result_count", len(result.Tasks)))
	return repoResultToListResult(result), nil
}

// ListTasksTx returns tasks matching the given filters using the provided executor.
func (c *Client) ListTasksTx(ctx context.Context, tx asynqpg.Querier, params *ListParams) (*ListResult, error) {
	if tx == nil {
		return nil, fmt.Errorf("executor cannot be nil")
	}

	if params == nil {
		params = NewListParams()
	}

	ctx, span := c.tracer.Start(ctx, "asynqpg.list_tasks",
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	repoParams := params.toRepoParams()

	result, err := c.repo.ListTasksWithExecutor(ctx, tx, repoParams)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "list tasks failed")
		return nil, fmt.Errorf("list tasks: %w", err)
	}

	span.SetAttributes(attribute.Int("result_count", len(result.Tasks)))
	return repoResultToListResult(result), nil
}
