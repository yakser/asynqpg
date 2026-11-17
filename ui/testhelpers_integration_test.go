//go:build integration

package ui

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/testutils"
)

func setupTestDB(t *testing.T) *sqlx.DB {
	t.Helper()
	return testutils.SetupTestDatabase(t)
}

func insertPendingTask(t *testing.T, db *sqlx.DB, taskType string, payload []byte) int64 {
	t.Helper()

	var id int64
	err := db.QueryRowContext(context.Background(),
		`INSERT INTO asynqpg_tasks (type, payload, status, attempts_left, blocked_till)
		 VALUES ($1, $2, 'pending', 3, now())
		 RETURNING id`,
		taskType, payload,
	)
	require.NoError(t, err.Scan(&id))
	return id
}

func insertPendingTaskWithToken(t *testing.T, db *sqlx.DB, taskType, token string) {
	t.Helper()

	_, err := db.ExecContext(context.Background(),
		`INSERT INTO asynqpg_tasks (type, payload, status, attempts_left, blocked_till, idempotency_token)
		 VALUES ($1, '{}', 'pending', 3, now(), $2)`,
		taskType, token,
	)
	require.NoError(t, err)
}

func insertRunningTask(t *testing.T, db *sqlx.DB, taskType string) int64 {
	t.Helper()

	id := insertPendingTask(t, db, taskType, []byte(`{}`))
	_, err := db.ExecContext(context.Background(),
		`UPDATE asynqpg_tasks SET status = 'running', blocked_till = now() + interval '1 minute' WHERE id = $1`,
		id,
	)
	require.NoError(t, err)
	return id
}

func insertFailedTask(t *testing.T, db *sqlx.DB, taskType string) int64 {
	t.Helper()

	id := insertRunningTask(t, db, taskType)
	_, err := db.ExecContext(context.Background(),
		`UPDATE asynqpg_tasks SET status = 'failed', messages = ARRAY['test failure'], finalized_at = now() WHERE id = $1`,
		id,
	)
	require.NoError(t, err)
	return id
}

func insertCompletedTask(t *testing.T, db *sqlx.DB, taskType string) int64 {
	t.Helper()

	id := insertRunningTask(t, db, taskType)
	_, err := db.ExecContext(context.Background(),
		`UPDATE asynqpg_tasks SET status = 'completed', finalized_at = now() WHERE id = $1`,
		id,
	)
	require.NoError(t, err)
	return id
}

// insertStuckRunningTask inserts a running task with an explicit attempted_at and
// attempts_left, so rescue-preview tests can control whether it counts as stuck.
func insertStuckRunningTask(t *testing.T, db *sqlx.DB, taskType string, attemptsLeft int, attemptedAt time.Time) {
	t.Helper()

	_, err := db.ExecContext(context.Background(),
		`INSERT INTO asynqpg_tasks (type, payload, status, attempts_left, blocked_till, attempted_at)
		 VALUES ($1, '{}', 'running', $2, now() + interval '1 minute', $3)`,
		taskType, attemptsLeft, attemptedAt,
	)
	require.NoError(t, err)
}

// insertFinalizedTask inserts a finalized task (completed/failed/cancelled) with an
// explicit finalized_at, so cleanup-preview tests can control retention eligibility.
func insertFinalizedTask(t *testing.T, db *sqlx.DB, taskType, status string, finalizedAt time.Time) {
	t.Helper()

	_, err := db.ExecContext(context.Background(),
		`INSERT INTO asynqpg_tasks (type, payload, status, attempts_left, blocked_till, finalized_at)
		 VALUES ($1, '{}', $2, 0, now(), $3)`,
		taskType, status, finalizedAt,
	)
	require.NoError(t, err)
}

func insertFailedTaskWithZeroAttempts(t *testing.T, db *sqlx.DB, taskType string) int64 {
	t.Helper()

	ctx := context.Background()

	// Insert with attempts_left = 1
	var id int64
	err := db.QueryRowContext(ctx,
		`INSERT INTO asynqpg_tasks (type, payload, status, attempts_left, blocked_till)
		 VALUES ($1, '{}', 'pending', 1, now())
		 RETURNING id`,
		taskType,
	)
	require.NoError(t, err.Scan(&id))

	// Simulate: pending -> running -> retry (decrement) -> pending -> running -> failed
	_, execErr := db.ExecContext(ctx,
		`UPDATE asynqpg_tasks SET status = 'running', blocked_till = now() + interval '1 minute' WHERE id = $1`,
		id,
	)
	require.NoError(t, execErr)

	_, execErr = db.ExecContext(ctx,
		`UPDATE asynqpg_tasks SET status = 'pending', attempts_left = attempts_left - 1, attempts_elapsed = attempts_elapsed + 1, blocked_till = now(), messages = array_append(messages, 'decrement') WHERE id = $1`,
		id,
	)
	require.NoError(t, execErr)

	_, execErr = db.ExecContext(ctx,
		`UPDATE asynqpg_tasks SET status = 'running', blocked_till = now() + interval '1 minute' WHERE id = $1`,
		id,
	)
	require.NoError(t, execErr)

	_, execErr = db.ExecContext(ctx,
		`UPDATE asynqpg_tasks SET status = 'failed', messages = array_append(messages, 'final failure'), finalized_at = now() WHERE id = $1`,
		id,
	)
	require.NoError(t, execErr)

	return id
}
