package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/yakser/asynqpg"
)

// ClientRepository provides task inspection and management operations.
// It covers client/management use cases: get, list, cancel, retry, delete by ID.
type ClientRepository struct {
	db asynqpg.Querier
}

// NewClientRepository creates a new ClientRepository backed by the given querier.
func NewClientRepository(db asynqpg.Querier) *ClientRepository {
	return &ClientRepository{db: db}
}

// FullTask represents a complete task row from the database.
type FullTask struct {
	ID               int64          `db:"id"`
	Type             string         `db:"type"`
	Payload          []byte         `db:"payload"`
	Status           string         `db:"status"`
	IdempotencyToken *string        `db:"idempotency_token"`
	Messages         pq.StringArray `db:"messages"`
	BlockedTill      time.Time      `db:"blocked_till"`
	AttemptsLeft     int            `db:"attempts_left"`
	AttemptsElapsed  int            `db:"attempts_elapsed"`
	CreatedAt        time.Time      `db:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at"`
	FinalizedAt      *time.Time     `db:"finalized_at"`
	AttemptedAt      *time.Time     `db:"attempted_at"`
}

// fullTaskWithFlag is used internally by CTE queries that need to distinguish
// whether the action (update/delete) was actually performed.
type fullTaskWithFlag struct {
	FullTask
	WasModified bool `db:"was_modified"`
}

const fullTaskColumns = `id, type, payload, status, idempotency_token, messages,
	blocked_till, attempts_left, attempts_elapsed,
	created_at, updated_at, finalized_at, attempted_at`

const fullTaskColumnsAliased = `t.id, t.type, t.payload, t.status, t.idempotency_token, t.messages,
	t.blocked_till, t.attempts_left, t.attempts_elapsed,
	t.created_at, t.updated_at, t.finalized_at, t.attempted_at`

// GetTaskByID returns a single task by its ID.
// Returns sql.ErrNoRows if the task is not found.
func (r *ClientRepository) GetTaskByID(ctx context.Context, id int64) (*FullTask, error) {
	query := fmt.Sprintf(`SELECT %s FROM asynqpg_tasks WHERE id = $1`, fullTaskColumns)

	var task FullTask
	err := r.db.GetContext(ctx, &task, query, id)
	if err != nil {
		return nil, fmt.Errorf("get task by id: %w", err)
	}

	return &task, nil
}

// GetTaskByIDWithExecutor returns a single task using the provided executor.
func (r *ClientRepository) GetTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, error) {
	query := fmt.Sprintf(`SELECT %s FROM asynqpg_tasks WHERE id = $1`, fullTaskColumns)

	var task FullTask
	err := exec.GetContext(ctx, &task, query, id)
	if err != nil {
		return nil, fmt.Errorf("get task by id: %w", err)
	}

	return &task, nil
}

// ListTasksParams contains parameters for listing tasks with filters.
type ListTasksParams struct {
	Statuses []string
	Types    []string
	IDs      []int64
	Limit    int
	Offset   int
	OrderBy  string // column name (whitelisted)
	OrderDir string // "ASC" or "DESC"
}

// ListTasksResult contains the list of tasks and the total count matching the filters.
type ListTasksResult struct {
	Tasks []FullTask
	Total int
}

// ListTasks returns tasks matching the given filters with pagination.
func (r *ClientRepository) ListTasks(ctx context.Context, params ListTasksParams) (*ListTasksResult, error) {
	return r.listTasksInternal(ctx, r.db, params)
}

// ListTasksWithExecutor returns tasks matching the given filters using the provided executor.
func (r *ClientRepository) ListTasksWithExecutor(ctx context.Context, exec asynqpg.Querier, params ListTasksParams) (*ListTasksResult, error) {
	return r.listTasksInternal(ctx, exec, params)
}

func (r *ClientRepository) listTasksInternal(ctx context.Context, exec asynqpg.Querier, params ListTasksParams) (*ListTasksResult, error) {
	orderColumn := "id"
	switch params.OrderBy {
	case "created_at", "updated_at", "blocked_till":
		orderColumn = params.OrderBy
	}

	orderDir := "ASC"
	if params.OrderDir == "DESC" {
		orderDir = "DESC"
	}

	query := fmt.Sprintf(`SELECT %s FROM asynqpg_tasks
		WHERE ($1::text[] IS NULL OR status = ANY($1))
		  AND ($2::text[] IS NULL OR type = ANY($2))
		  AND ($3::bigint[] IS NULL OR id = ANY($3))
		ORDER BY %s %s
		LIMIT $4 OFFSET $5`, fullTaskColumns, orderColumn, orderDir)

	var statuses, types *pq.StringArray
	var ids *pq.Int64Array

	if len(params.Statuses) > 0 {
		arr := pq.StringArray(params.Statuses)
		statuses = &arr
	}
	if len(params.Types) > 0 {
		arr := pq.StringArray(params.Types)
		types = &arr
	}
	if len(params.IDs) > 0 {
		arr := pq.Int64Array(params.IDs)
		ids = &arr
	}

	var tasks []FullTask
	err := exec.SelectContext(ctx, &tasks, query, statuses, types, ids, params.Limit, params.Offset)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}

	// Count total matching rows
	countQuery := `SELECT count(*) FROM asynqpg_tasks
		WHERE ($1::text[] IS NULL OR status = ANY($1))
		  AND ($2::text[] IS NULL OR type = ANY($2))
		  AND ($3::bigint[] IS NULL OR id = ANY($3))`

	var total int
	row := exec.QueryRowContext(ctx, countQuery, statuses, types, ids)
	if err := row.Scan(&total); err != nil {
		return nil, fmt.Errorf("count tasks: %w", err)
	}

	return &ListTasksResult{
		Tasks: tasks,
		Total: total,
	}, nil
}

// CancelTaskByID cancels a task if it's in a cancellable state (pending, failed, running).
// Running tasks are marked as cancelled; the consumer detects this and cancels the handler context.
// Returns the task row (updated or unchanged) and whether the update was performed.
func (r *ClientRepository) CancelTaskByID(ctx context.Context, id int64) (*FullTask, bool, error) {
	return r.cancelTaskInternal(ctx, r.db, id)
}

// CancelTaskByIDWithExecutor cancels a task using the provided executor.
func (r *ClientRepository) CancelTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, bool, error) {
	return r.cancelTaskInternal(ctx, exec, id)
}

func (r *ClientRepository) cancelTaskInternal(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, bool, error) {
	query := fmt.Sprintf(`WITH locked_task AS (
		SELECT id, status, finalized_at
		FROM asynqpg_tasks WHERE id = $1 FOR UPDATE
	),
	updated_task AS (
		UPDATE asynqpg_tasks
		SET status = 'cancelled',
			finalized_at = COALESCE(asynqpg_tasks.finalized_at, now()),
			updated_at = now()
		FROM locked_task
		WHERE asynqpg_tasks.id = locked_task.id
		  AND locked_task.status NOT IN ('completed', 'cancelled')
		RETURNING asynqpg_tasks.*
	)
	SELECT %s, true AS was_modified FROM updated_task
	UNION ALL
	SELECT %s, false AS was_modified FROM asynqpg_tasks t
	JOIN locked_task lt ON t.id = lt.id
	WHERE lt.id NOT IN (SELECT id FROM updated_task)`, fullTaskColumns, fullTaskColumnsAliased)

	var results []fullTaskWithFlag
	err := exec.SelectContext(ctx, &results, query, id)
	if err != nil {
		return nil, false, fmt.Errorf("cancel task: %w", err)
	}

	if len(results) == 0 {
		return nil, false, sql.ErrNoRows
	}

	return &results[0].FullTask, results[0].WasModified, nil
}

// RetryTaskByID moves a failed/cancelled task back to pending state.
// If attempts_left is 0, sets it to 1 to allow at least one more attempt.
// Returns the task row (updated or unchanged) and whether the update was performed.
func (r *ClientRepository) RetryTaskByID(ctx context.Context, id int64) (*FullTask, bool, error) {
	return r.retryTaskInternal(ctx, r.db, id)
}

// RetryTaskByIDWithExecutor retries a task using the provided executor.
func (r *ClientRepository) RetryTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, bool, error) {
	return r.retryTaskInternal(ctx, exec, id)
}

func (r *ClientRepository) retryTaskInternal(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, bool, error) {
	query := fmt.Sprintf(`WITH locked_task AS (
		SELECT id, status, attempts_left
		FROM asynqpg_tasks WHERE id = $1 FOR UPDATE
	),
	updated_task AS (
		UPDATE asynqpg_tasks
		SET status = 'pending',
			finalized_at = NULL,
			blocked_till = now(),
			attempts_left = CASE WHEN locked_task.attempts_left = 0 THEN 1
								 ELSE locked_task.attempts_left END,
			updated_at = now()
		FROM locked_task
		WHERE asynqpg_tasks.id = locked_task.id
		  AND locked_task.status IN ('failed', 'cancelled')
		RETURNING asynqpg_tasks.*
	)
	SELECT %s, true AS was_modified FROM updated_task
	UNION ALL
	SELECT %s, false AS was_modified FROM asynqpg_tasks t
	JOIN locked_task lt ON t.id = lt.id
	WHERE lt.id NOT IN (SELECT id FROM updated_task)`, fullTaskColumns, fullTaskColumnsAliased)

	var results []fullTaskWithFlag
	err := exec.SelectContext(ctx, &results, query, id)
	if err != nil {
		return nil, false, fmt.Errorf("retry task: %w", err)
	}

	if len(results) == 0 {
		return nil, false, sql.ErrNoRows
	}

	return &results[0].FullTask, results[0].WasModified, nil
}

// DeleteTaskByID deletes a task if it's not currently running.
// Returns the deleted task row and whether deletion was performed.
func (r *ClientRepository) DeleteTaskByID(ctx context.Context, id int64) (*FullTask, bool, error) {
	return r.deleteTaskInternal(ctx, r.db, id)
}

// DeleteTaskByIDWithExecutor deletes a task using the provided executor.
func (r *ClientRepository) DeleteTaskByIDWithExecutor(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, bool, error) {
	return r.deleteTaskInternal(ctx, exec, id)
}

func (r *ClientRepository) deleteTaskInternal(ctx context.Context, exec asynqpg.Querier, id int64) (*FullTask, bool, error) {
	query := fmt.Sprintf(`WITH locked_task AS (
		SELECT id, status FROM asynqpg_tasks WHERE id = $1 FOR UPDATE
	),
	deleted_task AS (
		DELETE FROM asynqpg_tasks
		USING locked_task
		WHERE asynqpg_tasks.id = locked_task.id
		  AND locked_task.status != 'running'
		RETURNING asynqpg_tasks.*
	)
	SELECT %s, true AS was_modified FROM deleted_task
	UNION ALL
	SELECT %s, false AS was_modified FROM asynqpg_tasks t
	JOIN locked_task lt ON t.id = lt.id
	WHERE lt.id NOT IN (SELECT id FROM deleted_task)`, fullTaskColumns, fullTaskColumnsAliased)

	var results []fullTaskWithFlag
	err := exec.SelectContext(ctx, &results, query, id)
	if err != nil {
		return nil, false, fmt.Errorf("delete task: %w", err)
	}

	if len(results) == 0 {
		return nil, false, sql.ErrNoRows
	}

	return &results[0].FullTask, results[0].WasModified, nil
}
