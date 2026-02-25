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

// DeleteTask deletes a task by its ID.
//
// Running tasks (status = running) cannot be deleted. All other states are deletable.
// The returned TaskInfo contains the task data as it was before deletion.
//
// Behavior by current task status:
//   - pending:   deleted, returns the deleted task info
//   - failed:    deleted, returns the deleted task info
//   - cancelled: deleted, returns the deleted task info
//   - completed: deleted, returns the deleted task info
//   - running:   returns ErrTaskRunning (running tasks cannot be deleted)
//   - not found: returns ErrTaskNotFound
func (c *Client) DeleteTask(ctx context.Context, id int64) (*TaskInfo, error) {
	ctx, span := c.tracer.Start(ctx, "asynqpg.delete_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, deleted, err := c.repo.DeleteTaskByID(ctx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "delete task failed")
		return nil, fmt.Errorf("delete task: %w", err)
	}

	info := fullTaskToInfo(task)

	if !deleted {
		return info, ErrTaskRunning
	}

	return info, nil
}

// DeleteTaskTx deletes a task using the provided executor (transaction).
func (c *Client) DeleteTaskTx(ctx context.Context, tx asynqpg.Querier, id int64) (*TaskInfo, error) {
	if tx == nil {
		return nil, fmt.Errorf("executor cannot be nil")
	}

	ctx, span := c.tracer.Start(ctx, "asynqpg.delete_task",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.Int64("task_id", id)),
	)
	defer span.End()

	task, deleted, err := c.repo.DeleteTaskByIDWithExecutor(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrTaskNotFound
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "delete task failed")
		return nil, fmt.Errorf("delete task: %w", err)
	}

	info := fullTaskToInfo(task)

	if !deleted {
		return info, ErrTaskRunning
	}

	return info, nil
}
