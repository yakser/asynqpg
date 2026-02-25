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

// CancelTask cancels a task by its ID.
//
// Behavior by current task status:
//   - pending:   immediately set to cancelled with finalized_at = now()
//   - failed:    immediately set to cancelled with finalized_at = now()
//   - running:   set to cancelled; the consumer detects this and cancels the handler's context
//   - cancelled: no-op, returns the existing task (idempotent)
//   - completed: returns ErrTaskAlreadyFinalized
//   - not found: returns ErrTaskNotFound
func (c *Client) CancelTask(ctx context.Context, id int64) (*TaskInfo, error) {
	ctx, span := c.tracer.Start(ctx, "asynqpg.cancel_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, updated, err := c.repo.CancelTaskByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "cancel task failed")
		return nil, fmt.Errorf("cancel task: %w", err)
	}

	info := fullTaskToInfo(task)

	if !updated {
		return info, cancelErrorForStatus(info.Status)
	}

	return info, nil
}

// CancelTaskTx cancels a task using the provided executor (transaction).
func (c *Client) CancelTaskTx(ctx context.Context, tx asynqpg.Querier, id int64) (*TaskInfo, error) {
	if tx == nil {
		return nil, fmt.Errorf("executor cannot be nil")
	}

	ctx, span := c.tracer.Start(ctx, "asynqpg.cancel_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, updated, err := c.repo.CancelTaskByIDWithExecutor(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "cancel task failed")
		return nil, fmt.Errorf("cancel task: %w", err)
	}

	info := fullTaskToInfo(task)

	if !updated {
		return info, cancelErrorForStatus(info.Status)
	}

	return info, nil
}

func cancelErrorForStatus(status asynqpg.TaskStatus) error {
	switch status {
	case asynqpg.TaskStatusCompleted:
		return ErrTaskAlreadyFinalized
	case asynqpg.TaskStatusPending, asynqpg.TaskStatusFailed, asynqpg.TaskStatusRunning:
		return nil
	case asynqpg.TaskStatusCancelled:
		return nil
	default:
		return nil
	}
}
