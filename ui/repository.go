package ui

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lib/pq"

	"github.com/yakser/asynqpg"
)

// repository provides database queries specific to the UI layer.
type repository struct {
	db asynqpg.Pool

	typesCacheMu    sync.RWMutex
	typesCacheData  []string
	typesCacheUntil time.Time
}

const typesCacheTTL = 30 * time.Second

func newRepository(pool asynqpg.Pool) *repository {
	return &repository{db: pool}
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

// ListTasksParams contains parameters for listing tasks.
type ListTasksParams struct {
	Statuses      []string
	Types         []string
	IDs           []int64
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Limit         int
	Offset        int
	OrderBy       string
	OrderDir      string
}

const listTasksColumns = `id, type, status, idempotency_token, messages,
	blocked_till, attempts_left, attempts_elapsed,
	created_at, updated_at, finalized_at, attempted_at,
	octet_length(payload) as payload_size`

// ListTasks returns tasks without payload, including payload_size.
func (r *repository) ListTasks(ctx context.Context, params ListTasksParams) (*TaskListResult, error) {
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
		  AND ($6::timestamptz IS NULL OR created_at >= $6)
		  AND ($7::timestamptz IS NULL OR created_at <= $7)
		ORDER BY %s %s
		LIMIT $4 OFFSET $5`, listTasksColumns, orderColumn, orderDir)

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

	countQuery := `SELECT count(*) FROM asynqpg_tasks
		WHERE ($1::text[] IS NULL OR status = ANY($1))
		  AND ($2::text[] IS NULL OR type = ANY($2))
		  AND ($3::bigint[] IS NULL OR id = ANY($3))
		  AND ($4::timestamptz IS NULL OR created_at >= $4)
		  AND ($5::timestamptz IS NULL OR created_at <= $5)`

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
	)
	UPDATE asynqpg_tasks
	SET status = 'pending',
		finalized_at = NULL,
		blocked_till = now(),
		attempts_left = CASE WHEN asynqpg_tasks.attempts_left = 0 THEN 1 ELSE asynqpg_tasks.attempts_left END,
		updated_at = now()
	FROM batch
	WHERE asynqpg_tasks.id = batch.id`

	var totalAffected int64
	for {
		result, err := r.db.ExecContext(ctx, query, taskType, bulkBatchSize)
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
