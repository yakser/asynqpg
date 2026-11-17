package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/publisher"
)

type Repository struct {
	db        asynqpg.Querier
	publisher publisher.Limiter
}

func NewRepository(db asynqpg.Querier) *Repository {
	return &Repository{db: db}
}

type PushTaskParams struct {
	Type             string      `db:"type"`
	IdempotencyToken *string     `db:"idempotency_token"`
	Payload          []byte      `db:"payload"`
	Delay            db.Duration `db:"delay"`
	AttemptsLeft     int         `db:"attempts_left"`
}

// PushTaskResult contains the result of a single task insertion.
// Duplicate is true when the task was already present (matched by idempotency token).
type PushTaskResult struct {
	ID        int64 `db:"id"`
	Duplicate bool  `db:"duplicate"`
}

type Task struct {
	ID               int64          `db:"id"`
	Type             string         `db:"type"`
	IdempotencyToken *string        `db:"idempotency_token"`
	Payload          []byte         `db:"payload"`
	Status           string         `db:"status"`
	Messages         pq.StringArray `db:"messages"`
	BlockedTill      time.Time      `db:"blocked_till"`
	AttemptsLeft     int            `db:"attempts_left"`
	AttemptsElapsed  int            `db:"attempts_elapsed"`
}

func (p *Repository) PushTask(ctx context.Context, task *PushTaskParams) (PushTaskResult, error) {
	return p.pushTaskInternal(ctx, p.db, task)
}

func (p *Repository) PushTaskWithTx(ctx context.Context, tx asynqpg.Tx, task *PushTaskParams) (PushTaskResult, error) {
	return p.pushTaskInternal(ctx, tx, task)
}

const pushTaskPlainQuery = `
		INSERT INTO asynqpg_tasks (type, idempotency_token, payload, blocked_till, attempts_left)
		VALUES ($1, $2, $3, now() + $4, $5)
		ON CONFLICT (type, idempotency_token) WHERE idempotency_token IS NOT NULL
		DO UPDATE SET type = EXCLUDED.type
		RETURNING id, (xmax != 0) AS duplicate
	`

const pushTasksPlainQuery = `
	INSERT INTO asynqpg_tasks (type, idempotency_token, payload, blocked_till, attempts_left)
	SELECT
		unnest($1::text[]),
		unnest($2::text[]),
		unnest($3::bytea[]),
		now() + unnest($4::interval[]),
		unnest($5::int[])
	ON CONFLICT (type, idempotency_token) WHERE idempotency_token IS NOT NULL
	DO UPDATE SET type = EXCLUDED.type
	RETURNING id, (xmax != 0) AS duplicate
`

var pushTaskNotifyQuery = `WITH inserted AS (
		INSERT INTO asynqpg_tasks (type, idempotency_token, payload, blocked_till, attempts_left)
		VALUES ($1, $2, $3, now() + $4, $5)
		ON CONFLICT (type, idempotency_token) WHERE idempotency_token IS NOT NULL
		DO UPDATE SET type = EXCLUDED.type
		RETURNING id, type, (xmax != 0) AS duplicate
 )` + publisher.CTE("inserted", "NOT duplicate", 6) + `SELECT id, duplicate FROM inserted CROSS JOIN (SELECT count(*) FROM readiness_hints) published`

func (p *Repository) pushTaskInternal(ctx context.Context, q asynqpg.Querier, task *PushTaskParams) (PushTaskResult, error) {
	if task == nil {
		return PushTaskResult{}, fmt.Errorf("task cannot be nil")
	}

	query := pushTaskNotifyQuery

	allowed := p.publisher.Reserve([]string{task.Type})
	args := []any{task.Type, task.IdempotencyToken, task.Payload, task.Delay, task.AttemptsLeft}
	if len(allowed) == 0 {
		query = pushTaskPlainQuery
	} else {
		args = append(args, pq.Array(allowed))
	}
	var result PushTaskResult
	err := q.QueryRowContext(ctx, query, args...).Scan(&result.ID, &result.Duplicate)
	if err != nil {
		return PushTaskResult{}, fmt.Errorf("enqueue task: %w", err)
	}

	return result, nil
}

// PushTasksParams contains parameters for batch task insertion.
type PushTasksParams struct {
	Tasks []PushTaskParams
}

var pushTasksQuery = `WITH inserted AS (
	INSERT INTO asynqpg_tasks (type, idempotency_token, payload, blocked_till, attempts_left)
	SELECT
		unnest($1::text[]),
		unnest($2::text[]),
		unnest($3::bytea[]),
		now() + unnest($4::interval[]),
		unnest($5::int[])
	ON CONFLICT (type, idempotency_token) WHERE idempotency_token IS NOT NULL
	DO UPDATE SET type = EXCLUDED.type
	RETURNING id, type, (xmax != 0) AS duplicate
)` + publisher.CTE("inserted", "NOT duplicate", 6) + `SELECT id, duplicate FROM inserted CROSS JOIN (SELECT count(*) FROM readiness_hints) published`

// PushTasks inserts multiple tasks in a single batch operation.
// Returns per-task results with IDs and duplicate flags.
func (p *Repository) PushTasks(ctx context.Context, params PushTasksParams) ([]PushTaskResult, error) {
	if len(params.Tasks) == 0 {
		return nil, nil
	}

	types := make([]string, len(params.Tasks))
	tokens := make([]*string, len(params.Tasks))
	payloads := make([][]byte, len(params.Tasks))
	delays := make([]string, len(params.Tasks))
	attemptsLeft := make([]int, len(params.Tasks))

	for i, t := range params.Tasks {
		types[i] = t.Type
		tokens[i] = t.IdempotencyToken
		payloads[i] = t.Payload
		delays[i] = t.Delay.String()
		attemptsLeft[i] = t.AttemptsLeft
	}

	allowed := p.publisher.Reserve(types)
	query := pushTasksPlainQuery
	args := []any{pq.Array(types), pq.Array(tokens), pq.Array(payloads), pq.Array(delays), pq.Array(attemptsLeft)}
	if len(allowed) > 0 {
		query = pushTasksQuery
		args = append(args, pq.Array(allowed))
	}
	var results []PushTaskResult
	err := p.db.SelectContext(ctx, &results, query, args...)
	if err != nil {
		return nil, fmt.Errorf("batch insert tasks: %w", err)
	}

	return results, nil
}

// PushTasksWithTx inserts multiple tasks using the provided transaction.
func (p *Repository) PushTasksWithTx(ctx context.Context, tx asynqpg.Tx, params PushTasksParams) ([]PushTaskResult, error) {
	if len(params.Tasks) == 0 {
		return nil, nil
	}

	types := make([]string, len(params.Tasks))
	tokens := make([]*string, len(params.Tasks))
	payloads := make([][]byte, len(params.Tasks))
	delays := make([]string, len(params.Tasks))
	attemptsLeft := make([]int, len(params.Tasks))

	for i, t := range params.Tasks {
		types[i] = t.Type
		tokens[i] = t.IdempotencyToken
		payloads[i] = t.Payload
		delays[i] = t.Delay.String()
		attemptsLeft[i] = t.AttemptsLeft
	}

	allowed := p.publisher.Reserve(types)
	query := pushTasksPlainQuery
	args := []any{pq.Array(types), pq.Array(tokens), pq.Array(payloads), pq.Array(delays), pq.Array(attemptsLeft)}
	if len(allowed) > 0 {
		query = pushTasksQuery
		args = append(args, pq.Array(allowed))
	}
	var results []PushTaskResult
	err := tx.SelectContext(ctx, &results, query, args...)
	if err != nil {
		return nil, fmt.Errorf("batch insert tasks: %w", err)
	}

	return results, nil
}

type GetReadyTasksParams struct {
	Type  string        `db:"type"`
	Limit int           `db:"limit"`
	Delay time.Duration `db:"delay"`
}

type ReadyTask struct {
	ClaimGeneration  int64          `db:"claim_generation"`
	ID               int64          `db:"id"`
	Type             string         `db:"type"`
	Payload          []byte         `db:"payload"`
	IdempotencyToken *string        `db:"idempotency_token"`
	AttemptsLeft     int            `db:"attempts_left"`
	AttemptsElapsed  int            `db:"attempts_elapsed"`
	CreatedAt        time.Time      `db:"created_at"`
	Messages         pq.StringArray `db:"messages"`
	AttemptedAt      time.Time      `db:"attempted_at"`
}

func (p *Repository) GetReadyTasks(ctx context.Context, params GetReadyTasksParams) ([]ReadyTask, error) {
	const query = `
	update asynqpg_tasks set
		status = 'running',
		claim_generation = claim_generation + 1,
		blocked_till = now() + $3::interval,
		attempted_at = now(),
		updated_at = now()
	where id in (
		select id from asynqpg_tasks sub
		where
			sub.type = $1 and
			sub.blocked_till <= now() and
			sub.status in ('pending', 'running')
		order by sub.blocked_till asc
		limit $2
		for no key update skip locked
	)
	returning id, claim_generation, type, payload, idempotency_token, attempts_left, attempts_elapsed, created_at, messages, attempted_at
	`

	var tasks []ReadyTask
	err := p.db.SelectContext(ctx, &tasks, query, params.Type, params.Limit, db.NewDuration(params.Delay))
	if err != nil {
		return nil, fmt.Errorf("get tasks: %w", err)
	}

	return tasks, nil
}

// CompleteTasks applies worker outcomes only to the matching running claims.
func (p *Repository) CompleteTasks(ctx context.Context, ids, generations []int64) error {
	_, err := p.CompleteTasksMany(ctx, CompleteTasksManyParams{IDs: ids, Generations: generations})
	return err
}

func (p *Repository) DeleteTasks(ctx context.Context, ids []int64) error {
	const query = `
	DELETE FROM asynqpg_tasks
	WHERE id = ANY($1) AND status != 'running'
	`

	_, err := p.db.ExecContext(ctx, query, pq.Array(ids))
	if err != nil {
		return fmt.Errorf("delete tasks: %w", err)
	}

	return nil
}

func (p *Repository) FailTasks(ctx context.Context, ids, generations []int64, message string) error {
	messages := make([]string, len(ids))
	for i := range messages {
		messages[i] = message
	}
	_, err := p.FailTasksMany(ctx, FailTasksManyParams{IDs: ids, Generations: generations, Messages: messages})
	return err
}

type RetryTaskParams struct {
	ClaimGeneration int64
	ID              int64
	BlockedTill     time.Time
	Message         string
}

// RetryTask schedules a task for retry with a new blocked_till time.
// Decrements attempts_left and increments attempts_elapsed.
func (p *Repository) RetryTask(ctx context.Context, params RetryTaskParams) error {
	_, err := p.RetryTasksMany(ctx, RetryTasksManyParams{IDs: []int64{params.ID}, Generations: []int64{params.ClaimGeneration}, BlockedTills: []time.Time{params.BlockedTill}, Messages: []string{params.Message}})
	return err
}

// CompleteTasksManyParams contains parameters for batch task completion.
type CompleteTasksManyParams struct {
	Generations []int64 // One claim generation per ID; required, including generation zero.
	IDs         []int64
}

// CompleteTasksMany marks multiple tasks as completed in a single batch operation.
// Returns the number of tasks actually completed.
func (p *Repository) CompleteTasksMany(ctx context.Context, params CompleteTasksManyParams) (int, error) {
	if len(params.IDs) != len(params.Generations) {
		return 0, fmt.Errorf("ids and generations must have same length")
	}
	if len(params.IDs) == 0 {
		return 0, nil
	}

	// attempts_elapsed is bumped because the attempt that completed did run;
	// without this, a task that succeeded on its first try would still report
	// attempts_elapsed=0.
	const query = `
	UPDATE asynqpg_tasks SET
		status = 'completed',
		finalized_at = now(),
		attempts_elapsed = attempts_elapsed + 1,
		updated_at = now()
	FROM unnest($1::bigint[], $2::bigint[]) AS input(id, generation)
	WHERE asynqpg_tasks.id = input.id AND asynqpg_tasks.claim_generation = input.generation AND status = 'running'
	`

	result, err := p.db.ExecContext(ctx, query, pq.Array(params.IDs), pq.Array(params.Generations))
	if err != nil {
		return 0, fmt.Errorf("complete tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// FailTasksManyParams contains parameters for batch task failure.
type FailTasksManyParams struct {
	Generations []int64 // One claim generation per ID; required, including generation zero.
	IDs         []int64
	Messages    []string // One message per task (must be same length as IDs)
}

// FailTasksMany marks multiple tasks as failed with individual error messages.
// Returns the number of tasks actually failed.
func (p *Repository) FailTasksMany(ctx context.Context, params FailTasksManyParams) (int, error) {
	if len(params.IDs) != len(params.Generations) {
		return 0, fmt.Errorf("ids and generations must have same length")
	}
	if len(params.IDs) == 0 {
		return 0, nil
	}

	if len(params.IDs) != len(params.Messages) {
		return 0, fmt.Errorf("ids and messages must have same length")
	}

	// attempts_left is forced to 0 so dead-letter classification
	// (status='failed' AND attempts_left=0) holds even on the SkipRetry path,
	// where a task may be terminally failed before its retries are exhausted.
	// attempts_elapsed is bumped because the attempt that produced this error
	// did run; this keeps len(messages) == attempts_elapsed in both retry and
	// terminal paths.
	const query = `
	WITH task_input AS (
		SELECT
			unnest($1::bigint[]) AS id,
			unnest($3::bigint[]) AS generation,
			unnest($2::text[]) AS message
	)
	UPDATE asynqpg_tasks SET
		status = 'failed',
		finalized_at = now(),
		attempts_left = 0,
		attempts_elapsed = asynqpg_tasks.attempts_elapsed + 1,
		messages = array_append(asynqpg_tasks.messages, task_input.message),
		updated_at = now()
	FROM task_input
	WHERE asynqpg_tasks.id = task_input.id AND asynqpg_tasks.claim_generation = task_input.generation AND asynqpg_tasks.status = 'running'
	`

	result, err := p.db.ExecContext(ctx, query, pq.Array(params.IDs), pq.Array(params.Messages), pq.Array(params.Generations))
	if err != nil {
		return 0, fmt.Errorf("fail tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// RetryTasksManyParams contains parameters for batch task retry.
type RetryTasksManyParams struct {
	Generations  []int64 // One claim generation per ID; required, including generation zero.
	IDs          []int64
	BlockedTills []time.Time // One blocked_till per task
	Messages     []string    // One message per task
}

// RetryTasksMany schedules multiple tasks for retry with individual blocked_till times.
// Decrements attempts_left and increments attempts_elapsed for each task.
// Returns the number of tasks actually retried.
func (p *Repository) RetryTasksMany(ctx context.Context, params RetryTasksManyParams) (int, error) {
	if len(params.IDs) != len(params.Generations) {
		return 0, fmt.Errorf("ids and generations must have same length")
	}
	if len(params.IDs) == 0 {
		return 0, nil
	}

	if len(params.IDs) != len(params.BlockedTills) || len(params.IDs) != len(params.Messages) {
		return 0, fmt.Errorf("ids, blocked tills, and messages must have same length")
	}

	mutation := `
	WITH task_input AS (
		SELECT
			unnest($1::bigint[]) AS id,
			unnest($4::bigint[]) AS generation,
			unnest($2::timestamptz[]) AS blocked_till,
			unnest($3::text[]) AS message
	)
	UPDATE asynqpg_tasks SET
		status = 'pending',
		finalized_at = NULL,
		blocked_till = task_input.blocked_till,
		attempts_left = asynqpg_tasks.attempts_left - 1,
		attempts_elapsed = asynqpg_tasks.attempts_elapsed + 1,
		messages = array_append(asynqpg_tasks.messages, task_input.message),
		updated_at = now()
	FROM task_input
	WHERE asynqpg_tasks.id = task_input.id AND asynqpg_tasks.claim_generation = task_input.generation AND asynqpg_tasks.status = 'running'
 RETURNING asynqpg_tasks.type
	`

	allowed, err := p.publisher.ForIDs(ctx, p.db, params.IDs)
	if err != nil {
		return 0, err
	}
	query := "WITH changed AS (" + mutation + ")" + publisher.CTE("changed", "true", 5) + `SELECT type FROM changed CROSS JOIN (SELECT count(*) FROM readiness_hints) published`

	result, err := p.db.ExecContext(ctx, query,
		pq.Array(params.IDs),
		pq.Array(params.BlockedTills),
		pq.Array(params.Messages),
		pq.Array(params.Generations),
		pq.Array(allowed),
	)
	if err != nil {
		return 0, fmt.Errorf("retry tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// SnoozeTaskParams contains parameters for snoozing a single task.
type SnoozeTaskParams struct {
	ClaimGeneration int64
	ID              int64
	BlockedTill     time.Time
}

// SnoozeTask reschedules a task without counting it as an attempt.
// Unlike RetryTask, it does NOT decrement attempts_left or increment attempts_elapsed.
func (p *Repository) SnoozeTask(ctx context.Context, params SnoozeTaskParams) error {
	_, err := p.SnoozeTasksMany(ctx, SnoozeTasksManyParams{IDs: []int64{params.ID}, Generations: []int64{params.ClaimGeneration}, BlockedTills: []time.Time{params.BlockedTill}})
	return err
}

// SnoozeTasksManyParams contains parameters for batch task snooze.
type SnoozeTasksManyParams struct {
	Generations  []int64 // One claim generation per ID; required, including generation zero.
	IDs          []int64
	BlockedTills []time.Time
}

// SnoozeTasksMany reschedules multiple tasks without counting as attempts.
// Returns the number of tasks actually snoozed.
func (p *Repository) SnoozeTasksMany(ctx context.Context, params SnoozeTasksManyParams) (int, error) {
	if len(params.IDs) != len(params.Generations) {
		return 0, fmt.Errorf("ids and generations must have same length")
	}
	if len(params.IDs) == 0 {
		return 0, nil
	}

	if len(params.IDs) != len(params.BlockedTills) {
		return 0, fmt.Errorf("ids and blocked tills must have same length")
	}

	mutation := `
	WITH task_input AS (
		SELECT
			unnest($1::bigint[]) AS id,
			unnest($3::bigint[]) AS generation,
			unnest($2::timestamptz[]) AS blocked_till
	)
	UPDATE asynqpg_tasks SET
		status = 'pending',
		finalized_at = NULL,
		blocked_till = task_input.blocked_till,
		updated_at = now()
	FROM task_input
	WHERE asynqpg_tasks.id = task_input.id AND asynqpg_tasks.claim_generation = task_input.generation AND asynqpg_tasks.status = 'running'
 RETURNING asynqpg_tasks.type
	`

	allowed, err := p.publisher.ForIDs(ctx, p.db, params.IDs)
	if err != nil {
		return 0, err
	}
	query := "WITH changed AS (" + mutation + ")" + publisher.CTE("changed", "true", 4) + `SELECT type FROM changed CROSS JOIN (SELECT count(*) FROM readiness_hints) published`

	result, err := p.db.ExecContext(ctx, query,
		pq.Array(params.IDs),
		pq.Array(params.BlockedTills),
		pq.Array(params.Generations),
		pq.Array(allowed),
	)
	if err != nil {
		return 0, fmt.Errorf("snooze tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// GetStuckTasksParams contains parameters for getting stuck tasks.
type GetStuckTasksParams struct {
	// StuckHorizon is the time before which tasks are considered stuck.
	// Tasks with attempted_at < StuckHorizon and status 'running' are stuck.
	StuckHorizon time.Time
	Limit        int
}

// StuckTask represents a task that appears to be stuck (running too long).
type StuckTask struct {
	ClaimGeneration int64  `db:"claim_generation"`
	ID              int64  `db:"id"`
	Type            string `db:"type"`
	AttemptsLeft    int    `db:"attempts_left"`
	AttemptsElapsed int    `db:"attempts_elapsed"`
}

// GetStuckTasks returns tasks that have been running for too long.
// A task is considered stuck if its status is 'running' and attempted_at < stuckHorizon.
func (p *Repository) GetStuckTasks(ctx context.Context, params GetStuckTasksParams) ([]StuckTask, error) {
	const query = `
	SELECT id, claim_generation, type, attempts_left, attempts_elapsed
	FROM asynqpg_tasks
	WHERE status = 'running'
	  AND attempted_at < $1
	ORDER BY id
	LIMIT $2
	FOR NO KEY UPDATE SKIP LOCKED
	`

	var tasks []StuckTask
	err := p.db.SelectContext(ctx, &tasks, query, params.StuckHorizon, params.Limit)
	if err != nil {
		return nil, fmt.Errorf("get stuck tasks: %w", err)
	}

	return tasks, nil
}

// DeleteOldTasksParams contains parameters for deleting old finalized tasks.
type DeleteOldTasksParams struct {
	CompletedBefore time.Time
	FailedBefore    time.Time
	CancelledBefore time.Time
	Limit           int
}

// DeleteOldTasks deletes tasks that have been finalized for longer than the retention period.
// Returns the number of deleted tasks.
func (p *Repository) DeleteOldTasks(ctx context.Context, params DeleteOldTasksParams) (int, error) {
	const query = `
	DELETE FROM asynqpg_tasks
	WHERE id IN (
		SELECT id FROM asynqpg_tasks
		WHERE (status = 'completed' AND finalized_at < $1)
		   OR (status = 'failed' AND finalized_at < $2)
		   OR (status = 'cancelled' AND finalized_at < $3)
		ORDER BY id
		LIMIT $4
	)
	`

	result, err := p.db.ExecContext(ctx, query,
		params.CompletedBefore,
		params.FailedBefore,
		params.CancelledBefore,
		params.Limit,
	)
	if err != nil {
		return 0, fmt.Errorf("delete old tasks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// GetCancelledTaskIDs returns which of the given task IDs have been cancelled in the database.
// Used by the consumer to detect tasks that were cancelled while running.
func (p *Repository) GetCancelledTaskIDs(ctx context.Context, ids []int64) ([]int64, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	const query = `SELECT id FROM asynqpg_tasks WHERE id = ANY($1) AND status = 'cancelled'`

	var cancelledIDs []int64
	err := p.db.SelectContext(ctx, &cancelledIDs, query, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("get cancelled task ids: %w", err)
	}

	return cancelledIDs, nil
}
