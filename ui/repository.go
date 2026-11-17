package ui

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/yakser/asynqpg/internal/publisher"
)

// ErrNoLeader is returned by GetLeader when the asynqpg_leader table has no row yet.
var ErrNoLeader = errors.New("no leader row")

// repository provides database queries specific to the UI layer.
type repository struct {
	db        pool
	publisher publisher.Limiter

	typesCacheMu    sync.RWMutex
	typesCacheData  []string
	typesCacheUntil time.Time
}

const typesCacheTTL = 30 * time.Second

func newRepository(db pool) *repository {
	return &repository{db: db}
}

// TaskTypeStat represents a count of tasks grouped by type and status.
type TaskTypeStat struct {
	Type   string `db:"type" json:"type"`
	Status string `db:"status" json:"status"`
	Count  int64  `db:"count" json:"count"`
}

// GetTaskTypeStats returns task counts grouped by type and status.
func (r *repository) GetTaskTypeStats(ctx context.Context) ([]TaskTypeStat, error) {
	query := `SELECT type, status, COUNT(*) as count
		FROM asynqpg_tasks
		GROUP BY type, status
		ORDER BY type, status`

	var stats []TaskTypeStat
	if err := r.db.SelectContext(ctx, &stats, query); err != nil {
		return nil, fmt.Errorf("get task type stats: %w", err)
	}

	return stats, nil
}

// GetDistinctTaskTypes returns all unique task types, cached for 30 seconds.
func (r *repository) GetDistinctTaskTypes(ctx context.Context) ([]string, error) {
	r.typesCacheMu.RLock()
	if time.Now().Before(r.typesCacheUntil) && r.typesCacheData != nil {
		data := r.typesCacheData
		r.typesCacheMu.RUnlock()
		return data, nil
	}
	r.typesCacheMu.RUnlock()

	query := `SELECT DISTINCT type FROM asynqpg_tasks ORDER BY type`

	var types []string
	if err := r.db.SelectContext(ctx, &types, query); err != nil {
		return nil, fmt.Errorf("get distinct task types: %w", err)
	}

	r.typesCacheMu.Lock()
	r.typesCacheData = types
	r.typesCacheUntil = time.Now().Add(typesCacheTTL)
	r.typesCacheMu.Unlock()

	return types, nil
}

// TaskListItem represents a task in the list view (without payload, with payload_size).
type TaskListItem struct {
	ID               int64          `db:"id" json:"id"`
	Type             string         `db:"type" json:"type"`
	Status           string         `db:"status" json:"status"`
	IdempotencyToken *string        `db:"idempotency_token" json:"idempotency_token"`
	Messages         pq.StringArray `db:"messages" json:"messages"`
	BlockedTill      time.Time      `db:"blocked_till" json:"blocked_till"`
	AttemptsLeft     int            `db:"attempts_left" json:"attempts_left"`
	AttemptsElapsed  int            `db:"attempts_elapsed" json:"attempts_elapsed"`
	CreatedAt        time.Time      `db:"created_at" json:"created_at"`
	UpdatedAt        time.Time      `db:"updated_at" json:"updated_at"`
	FinalizedAt      *time.Time     `db:"finalized_at" json:"finalized_at"`
	AttemptedAt      *time.Time     `db:"attempted_at" json:"attempted_at"`
	PayloadSize      int64          `db:"payload_size" json:"payload_size"`
}

// TaskListResult contains the list of tasks and total count matching filters.
type TaskListResult struct {
	Tasks []TaskListItem `json:"tasks"`
	Total int            `json:"total"`
}

// IdempotencyTokenFilter values for ListTasksParams.IdempotencyToken.
const (
	IdempotencyHas  = "has"
	IdempotencyNone = "none"
)

// ListTasksParams contains parameters for listing tasks.
type ListTasksParams struct {
	Statuses      []string
	Types         []string
	IDs           []int64
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	// IdempotencyToken filters by idempotency_token presence: "has", "none", or "" for unset.
	IdempotencyToken string
	Limit            int
	Offset           int
	OrderBy          string
	OrderDir         string
}

const listTasksColumns = `id, type, status, idempotency_token, messages,
	blocked_till, attempts_left, attempts_elapsed,
	created_at, updated_at, finalized_at, attempted_at,
	octet_length(payload) as payload_size`

// ListTasks returns tasks without payload, including payload_size.
func (r *repository) ListTasks(ctx context.Context, params ListTasksParams) (*TaskListResult, error) {
	orderColumn := orderByID
	switch params.OrderBy {
	case orderByCreatedAt, orderByUpdatedAt, orderByBlockedTill:
		orderColumn = params.OrderBy
	}

	orderDir := orderASC
	if params.OrderDir == orderDESC {
		orderDir = orderDESC
	}

	idempotencyClause := ""
	switch params.IdempotencyToken {
	case IdempotencyHas:
		idempotencyClause = " AND idempotency_token IS NOT NULL"
	case IdempotencyNone:
		idempotencyClause = " AND idempotency_token IS NULL"
	}

	query := fmt.Sprintf(`SELECT %s FROM asynqpg_tasks
		WHERE ($1::asynqpg_task_status[] IS NULL OR status = ANY($1))
		  AND ($2::text[] IS NULL OR type = ANY($2))
		  AND ($3::bigint[] IS NULL OR id = ANY($3))
		  AND ($6::timestamptz IS NULL OR created_at >= $6)
		  AND ($7::timestamptz IS NULL OR created_at <= $7)%s
		ORDER BY %s %s
		LIMIT $4 OFFSET $5`, listTasksColumns, idempotencyClause, orderColumn, orderDir)

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

	var tasks []TaskListItem
	if err := r.db.SelectContext(ctx, &tasks, query,
		statuses, types, ids, params.Limit, params.Offset,
		params.CreatedAfter, params.CreatedBefore,
	); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}

	countQuery := fmt.Sprintf(`SELECT count(*) FROM asynqpg_tasks
		WHERE ($1::asynqpg_task_status[] IS NULL OR status = ANY($1))
		  AND ($2::text[] IS NULL OR type = ANY($2))
		  AND ($3::bigint[] IS NULL OR id = ANY($3))
		  AND ($4::timestamptz IS NULL OR created_at >= $4)
		  AND ($5::timestamptz IS NULL OR created_at <= $5)%s`, idempotencyClause)

	var total int
	row := r.db.QueryRowContext(ctx, countQuery, statuses, types, ids,
		params.CreatedAfter, params.CreatedBefore,
	)
	if err := row.Scan(&total); err != nil {
		return nil, fmt.Errorf("count tasks: %w", err)
	}

	if tasks == nil {
		tasks = []TaskListItem{}
	}

	return &TaskListResult{
		Tasks: tasks,
		Total: total,
	}, nil
}

// LeaderRow mirrors a row in the asynqpg_leader table.
type LeaderRow struct {
	Name      string    `db:"name" json:"name"`
	LeaderID  string    `db:"leader_id" json:"leader_id"`
	ElectedAt time.Time `db:"elected_at" json:"elected_at"`
	ExpiresAt time.Time `db:"expires_at" json:"expires_at"`
}

// GetLeader returns the current leader row, or ErrNoLeader if no row exists yet.
func (r *repository) GetLeader(ctx context.Context) (*LeaderRow, error) {
	var rows []LeaderRow
	const query = `SELECT name, leader_id, elected_at, expires_at
		FROM asynqpg_leader
		WHERE name = 'default'
		LIMIT 1`
	if err := r.db.SelectContext(ctx, &rows, query); err != nil {
		return nil, fmt.Errorf("get leader: %w", err)
	}
	if len(rows) == 0 {
		return nil, ErrNoLeader
	}
	return &rows[0], nil
}

// RescuePreview reports how many running tasks the Rescuer would act on right
// now: tasks running past the stuck threshold, split by whether they still have
// retries left.
type RescuePreview struct {
	Total       int64 `db:"total"`
	Retriable   int64 `db:"retriable"`
	Discardable int64 `db:"discardable"`
}

// PreviewRescue counts running tasks stuck past stuckBefore, mirroring the
// Rescuer's selection (internal/maintenance/rescuer.go). It performs no writes.
func (r *repository) PreviewRescue(ctx context.Context, stuckBefore time.Time) (RescuePreview, error) {
	const query = `SELECT
		count(*) FILTER (WHERE attempts_left > 0)  AS retriable,
		count(*) FILTER (WHERE attempts_left = 0)  AS discardable,
		count(*)                                   AS total
	FROM asynqpg_tasks
	WHERE status = 'running' AND attempted_at < $1`

	var preview RescuePreview
	if err := r.db.GetContext(ctx, &preview, query, stuckBefore); err != nil {
		return RescuePreview{}, fmt.Errorf("preview rescue: %w", err)
	}
	return preview, nil
}

// CleanupPreview reports how many finalized tasks the Cleaner would delete right
// now, broken down by status. Total is the sum of the three.
type CleanupPreview struct {
	Total     int64 `db:"-"`
	Completed int64 `db:"completed"`
	Failed    int64 `db:"failed"`
	Cancelled int64 `db:"cancelled"`
}

// PreviewCleanup counts finalized tasks past their retention windows, mirroring
// the Cleaner's selection (internal/maintenance/cleaner.go). It performs no writes.
func (r *repository) PreviewCleanup(ctx context.Context, completedBefore, failedBefore, cancelledBefore time.Time) (CleanupPreview, error) {
	const query = `SELECT
		count(*) FILTER (WHERE status = 'completed' AND finalized_at < $1) AS completed,
		count(*) FILTER (WHERE status = 'failed'    AND finalized_at < $2) AS failed,
		count(*) FILTER (WHERE status = 'cancelled' AND finalized_at < $3) AS cancelled
	FROM asynqpg_tasks
	WHERE (status = 'completed' AND finalized_at < $1)
	   OR (status = 'failed'    AND finalized_at < $2)
	   OR (status = 'cancelled' AND finalized_at < $3)`

	var preview CleanupPreview
	if err := r.db.GetContext(ctx, &preview, query, completedBefore, failedBefore, cancelledBefore); err != nil {
		return CleanupPreview{}, fmt.Errorf("preview cleanup: %w", err)
	}
	preview.Total = preview.Completed + preview.Failed + preview.Cancelled
	return preview, nil
}

// QueueHealth holds general queue-health metrics derived from the task table.
type QueueHealth struct {
	// OldestPendingMs is the age in milliseconds of the oldest pending task,
	// or nil when there are no pending tasks.
	OldestPendingMs  *float64 `db:"oldest_pending_ms"`
	CompletedLast15m int64    `db:"completed_15m"`
	FailedLast15m    int64    `db:"failed_15m"`
}

// GetQueueHealth returns the age of the oldest pending task and recent
// throughput (completed/failed in the last 15 minutes).
func (r *repository) GetQueueHealth(ctx context.Context) (QueueHealth, error) {
	const query = `SELECT
		EXTRACT(EPOCH FROM (now() - min(created_at) FILTER (WHERE status = 'pending'))) * 1000 AS oldest_pending_ms,
		count(*) FILTER (WHERE status = 'completed') AS completed_15m,
		count(*) FILTER (WHERE status = 'failed'    AND finalized_at > now() - interval '15 minutes') AS failed_15m
	FROM asynqpg_tasks
 WHERE status = 'pending' OR (status IN ('completed', 'failed') AND finalized_at > now() - interval '15 minutes')`

	var health QueueHealth
	if err := r.db.GetContext(ctx, &health, query); err != nil {
		return QueueHealth{}, fmt.Errorf("get queue health: %w", err)
	}
	return health, nil
}

const bulkBatchSize = 1000

// BulkRetryFailed moves failed tasks back to pending in batches.
// Returns the total number of affected rows.
func (r *repository) BulkRetryFailed(ctx context.Context, taskType *string) (int64, error) {
	query := `WITH batch AS (
		SELECT id FROM asynqpg_tasks
		WHERE status = 'failed'
		  AND ($1::text IS NULL OR type = $1)
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	) , changed AS (
	UPDATE asynqpg_tasks
	SET status = 'pending',
		finalized_at = NULL,
		blocked_till = now(),
		attempts_left = CASE WHEN asynqpg_tasks.attempts_left = 0 THEN 1 ELSE asynqpg_tasks.attempts_left END,
		updated_at = now()
	FROM batch
	WHERE asynqpg_tasks.id = batch.id RETURNING asynqpg_tasks.type)` + publisher.CTE("changed", "true", 3) + `SELECT type FROM changed CROSS JOIN (SELECT count(*) FROM readiness_hints) published`

	var totalAffected int64
	for {
		var types []string
		if err := r.db.SelectContext(ctx, &types, `SELECT DISTINCT type FROM asynqpg_tasks WHERE status='failed' AND ($1::text IS NULL OR type=$1)`, taskType); err != nil {
			return totalAffected, fmt.Errorf("resolve retry types: %w", err)
		}
		allowed := r.publisher.Reserve(types)
		result, err := r.db.ExecContext(ctx, query, taskType, bulkBatchSize, pq.Array(allowed))
		if err != nil {
			return totalAffected, fmt.Errorf("bulk retry tasks: %w", err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return totalAffected, fmt.Errorf("get rows affected: %w", err)
		}

		totalAffected += affected
		if affected < bulkBatchSize {
			break
		}
	}

	return totalAffected, nil
}

// BulkDeleteFailed deletes failed tasks in batches.
// Returns the total number of deleted rows.
func (r *repository) BulkDeleteFailed(ctx context.Context, taskType *string) (int64, error) {
	query := `WITH batch AS (
		SELECT id FROM asynqpg_tasks
		WHERE status = 'failed'
		  AND ($1::text IS NULL OR type = $1)
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	)
	DELETE FROM asynqpg_tasks
	USING batch
	WHERE asynqpg_tasks.id = batch.id`

	var totalAffected int64
	for {
		result, err := r.db.ExecContext(ctx, query, taskType, bulkBatchSize)
		if err != nil {
			return totalAffected, fmt.Errorf("bulk delete tasks: %w", err)
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return totalAffected, fmt.Errorf("get rows affected: %w", err)
		}

		totalAffected += affected
		if affected < bulkBatchSize {
			break
		}
	}

	return totalAffected, nil
}
