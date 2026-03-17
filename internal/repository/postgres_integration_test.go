//go:build integration

package repository_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

func ptr(s string) *string { return &s }

func TestPushTask_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	task := &repository.PushTaskParams{
		Type:         "push-basic",
		Payload:      []byte(`{"key":"value"}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	}

	// Act
	got, err := repo.PushTask(ctx, task)

	// Assert
	require.NoError(t, err)
	assert.Greater(t, got, int64(0))

	var status string
	err = database.Get(&status, "SELECT status FROM asynqpg_tasks WHERE id = $1", got)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)
}

func TestPushTask_WithIdempotencyToken(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	token := ptr("unique-1")
	task := &repository.PushTaskParams{
		Type:             "push-idempotent",
		IdempotencyToken: token,
		Payload:          []byte(`{}`),
		AttemptsLeft:     3,
		Delay:            db.NewDuration(0),
	}

	// Act
	id1, err := repo.PushTask(ctx, task)
	require.NoError(t, err)
	assert.Greater(t, id1, int64(0))

	_, err = repo.PushTask(ctx, task)

	// Assert
	require.Error(t, err)
	assert.ErrorIs(t, err, sql.ErrNoRows)
}

func TestPushTask_WithDelay(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	task := &repository.PushTaskParams{
		Type:         "push-delay",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(5 * time.Second),
	}

	// Act
	id, err := repo.PushTask(ctx, task)

	// Assert
	require.NoError(t, err)

	var blockedTill time.Time
	err = database.Get(&blockedTill, "SELECT blocked_till FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.True(t, blockedTill.After(time.Now()), "blocked_till should be in the future")
}

func TestGetReadyTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	_, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "ready-basic",
		Payload:      []byte(`{"data":"test"}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	// Act
	got, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "ready-basic",
		Limit: 10,
		Delay: time.Minute,
	})

	// Assert
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "ready-basic", got[0].Type)
	assert.Equal(t, []byte(`{"data":"test"}`), got[0].Payload)
	assert.Equal(t, 3, got[0].AttemptsLeft)
}

func TestGetReadyTasks_RespectsLimit(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	for i := 0; i < 5; i++ {
		_, err := repo.PushTask(ctx, &repository.PushTaskParams{
			Type:         "ready-limit",
			Payload:      []byte(`{}`),
			AttemptsLeft: 3,
			Delay:        db.NewDuration(0),
		})
		require.NoError(t, err)
	}

	// Act
	got, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "ready-limit",
		Limit: 2,
		Delay: time.Minute,
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, got, 2)
}

func TestGetReadyTasks_SkipsLockedTasks(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	for i := 0; i < 3; i++ {
		_, err := repo.PushTask(ctx, &repository.PushTaskParams{
			Type:         "ready-locked",
			Payload:      []byte(`{}`),
			AttemptsLeft: 3,
			Delay:        db.NewDuration(0),
		})
		require.NoError(t, err)
	}

	// Act – first fetch sets them to running with future blocked_till
	got1, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "ready-locked",
		Limit: 3,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, got1, 3)

	got2, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "ready-locked",
		Limit: 3,
		Delay: time.Minute,
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, got2, 0)
}

func TestGetReadyTasks_IgnoresBlockedTasks(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	_, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "ready-blocked",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(time.Hour),
	})
	require.NoError(t, err)

	// Act
	got, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "ready-blocked",
		Limit: 10,
		Delay: time.Minute,
	})

	// Assert
	require.NoError(t, err)
	assert.Len(t, got, 0)
}

func TestCompleteTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "complete-basic",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "complete-basic",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	// Act
	err = repo.CompleteTasks(ctx, []int64{id})

	// Assert
	require.NoError(t, err)

	var status string
	err = database.Get(&status, "SELECT status FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, "completed", status)
}

func TestFailTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "fail-basic",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "fail-basic",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	// Act
	err = repo.FailTasks(ctx, []int64{id}, "something went wrong")

	// Assert
	require.NoError(t, err)

	var status string
	err = database.Get(&status, "SELECT status FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, "failed", status)

	var messages []string
	err = database.Select(&messages, "SELECT unnest(messages) FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Contains(t, messages, "something went wrong")
}

func TestRetryTask_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "retry-basic",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "retry-basic",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	// Act
	err = repo.RetryTask(ctx, repository.RetryTaskParams{
		ID:          id,
		BlockedTill: time.Now(),
		Message:     "retrying due to transient error",
	})

	// Assert
	require.NoError(t, err)

	var status string
	var attemptsLeft int
	var attemptsElapsed int
	err = database.QueryRow(
		"SELECT status, attempts_left, attempts_elapsed FROM asynqpg_tasks WHERE id = $1", id,
	).Scan(&status, &attemptsLeft, &attemptsElapsed)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)
	assert.Equal(t, 2, attemptsLeft)
	assert.Equal(t, 1, attemptsElapsed)

	var messages []string
	err = database.Select(&messages, "SELECT unnest(messages) FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Contains(t, messages, "retrying due to transient error")
}

func TestSnoozeTask_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "snooze-basic",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "snooze-basic",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	snoozeTill := time.Now().Add(10 * time.Minute)

	// Act
	err = repo.SnoozeTask(ctx, repository.SnoozeTaskParams{
		ID:          id,
		BlockedTill: snoozeTill,
	})

	// Assert
	require.NoError(t, err)

	var status string
	var attemptsLeft int
	err = database.QueryRow(
		"SELECT status, attempts_left FROM asynqpg_tasks WHERE id = $1", id,
	).Scan(&status, &attemptsLeft)
	require.NoError(t, err)
	assert.Equal(t, "pending", status)
	assert.Equal(t, 3, attemptsLeft, "attempts_left should NOT be decremented by snooze")
}

func TestDeleteTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "delete-basic",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	// Act
	err = repo.DeleteTasks(ctx, []int64{id})

	// Assert
	require.NoError(t, err)

	var count int
	err = database.Get(&count, "SELECT count(*) FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestDeleteTasks_SkipsRunning(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "delete-running",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "delete-running",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	// Act
	err = repo.DeleteTasks(ctx, []int64{id})

	// Assert
	require.NoError(t, err)

	var count int
	err = database.Get(&count, "SELECT count(*) FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "running task should not be deleted")
}

func TestGetStuckTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "stuck-basic",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "stuck-basic",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	// Manually backdate attempted_at to simulate a stuck task
	_, err = database.Exec(
		"UPDATE asynqpg_tasks SET attempted_at = $1 WHERE id = $2",
		time.Now().Add(-1*time.Hour), id,
	)
	require.NoError(t, err)

	// Act
	got, err := repo.GetStuckTasks(ctx, repository.GetStuckTasksParams{
		StuckHorizon: time.Now().Add(-30 * time.Minute),
		Limit:        10,
	})

	// Assert
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, id, got[0].ID)
	assert.Equal(t, "stuck-basic", got[0].Type)
}

func TestGetCancelledTaskIDs_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	clientRepo := repository.NewClientRepository(database)

	id1 := pushTask(t, repo, "cancelled-ids-1")
	id2 := pushTask(t, repo, "cancelled-ids-2")
	id3 := pushTask(t, repo, "cancelled-ids-3")
	makeCancelled(t, clientRepo, id1)
	makeCancelled(t, clientRepo, id2)

	got, err := repo.GetCancelledTaskIDs(context.Background(), []int64{id1, id2, id3})

	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.ElementsMatch(t, []int64{id1, id2}, got)
}

func TestDeleteOldTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Arrange
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type:         "old-task",
		Payload:      []byte(`{}`),
		AttemptsLeft: 3,
		Delay:        db.NewDuration(0),
	})
	require.NoError(t, err)

	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "old-task",
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	err = repo.CompleteTasks(ctx, []int64{id})
	require.NoError(t, err)

	// Manually backdate finalized_at
	_, err = database.Exec(
		"UPDATE asynqpg_tasks SET finalized_at = $1 WHERE id = $2",
		time.Now().Add(-48*time.Hour), id,
	)
	require.NoError(t, err)

	// Act
	got, err := repo.DeleteOldTasks(ctx, repository.DeleteOldTasksParams{
		CompletedBefore: time.Now().Add(-24 * time.Hour),
		FailedBefore:    time.Now().Add(-24 * time.Hour),
		CancelledBefore: time.Now().Add(-24 * time.Hour),
		Limit:           100,
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, 1, got)

	var count int
	err = database.Get(&count, "SELECT count(*) FROM asynqpg_tasks WHERE id = $1", id)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
