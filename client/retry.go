package client

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/yakser/asynqpg"
)

// RetryTask moves a failed or cancelled task back to pending state.
//
// If the task has exhausted all attempts (attempts_left = 0), it is set to 1
// to allow at least one more processing attempt.
//
// Behavior by current task status:
//   - failed:    set to pending, clear finalized_at, ensure attempts_left >= 1
//   - cancelled: set to pending, clear finalized_at, ensure attempts_left >= 1
//   - pending:   returns ErrTaskAlreadyAvailable
//   - running:   returns ErrTaskRunning
//   - completed: returns ErrTaskAlreadyFinalized
//   - not found: returns ErrTaskNotFound
func (c *Client) RetryTask(ctx context.Context, id int64) (*TaskInfo, error) {
	ctx, span := c.tracer.Start(ctx, "asynqpg.retry_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, updated, err := c.repo.RetryTaskByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "retry task failed")
		return nil, fmt.Errorf("retry task: %w", err)
	}

	info := fullTaskToInfo(task)

	if !updated {
		return info, retryErrorForStatus(info.Status)
	}

	return info, nil
}

// RetryTaskTx moves a task back to pending using the provided executor (transaction).
func (c *Client) RetryTaskTx(ctx context.Context, tx asynqpg.Querier, id int64) (*TaskInfo, error) {
	if tx == nil {
		return nil, fmt.Errorf("executor cannot be nil")
	}

	ctx, span := c.tracer.Start(ctx, "asynqpg.retry_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, updated, err := c.repo.RetryTaskByIDWithExecutor(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "retry task failed")
		return nil, fmt.Errorf("retry task: %w", err)
	}

	info := fullTaskToInfo(task)

	if !updated {
		return info, retryErrorForStatus(info.Status)
	}

	return info, nil
}

func retryErrorForStatus(status asynqpg.TaskStatus) error {
	switch status {
	case asynqpg.TaskStatusPending:
		return ErrTaskAlreadyAvailable
	case asynqpg.TaskStatusRunning:
		return ErrTaskRunning
	case asynqpg.TaskStatusCompleted:
		return ErrTaskAlreadyFinalized
	case asynqpg.TaskStatusFailed, asynqpg.TaskStatusCancelled:
		return nil
	default:
		return nil
	}
}
