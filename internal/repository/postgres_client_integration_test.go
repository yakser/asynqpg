//go:build integration

package repository_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/lib/testutils"
	"github.com/yakser/asynqpg/internal/repository"
)

// pushTask creates a pending task with the given type and 3 attempts.
func pushTask(t *testing.T, repo *repository.Repository, taskType string) int64 {
	t.Helper()
	ctx := context.Background()
	id, err := repo.PushTask(ctx, &repository.PushTaskParams{
		Type: taskType, Payload: []byte(`{"test":true}`),
		AttemptsLeft: 3, Delay: db.NewDuration(0),
	})
	require.NoError(t, err)
	return id
}

// makeRunning transitions a pending task to running by fetching it via GetReadyTasks.
func makeRunning(t *testing.T, repo *repository.Repository, taskType string, id int64) {
	t.Helper()
	tasks, err := repo.GetReadyTasks(context.Background(), repository.GetReadyTasksParams{
		Type: taskType, Limit: 1, Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, id, tasks[0].ID)
}

// makeFailed transitions a pending task to failed (pending -> running -> failed).
func makeFailed(t *testing.T, repo *repository.Repository, taskType string, id int64) {
	t.Helper()
	makeRunning(t, repo, taskType, id)
	err := repo.FailTasks(context.Background(), []int64{id}, "test failure")
	require.NoError(t, err)
}

// makeCompleted transitions a pending task to completed (pending -> running -> completed).
func makeCompleted(t *testing.T, repo *repository.Repository, taskType string, id int64) {
	t.Helper()
	makeRunning(t, repo, taskType, id)
	err := repo.CompleteTasks(context.Background(), []int64{id})
	require.NoError(t, err)
}

// makeCancelled cancels a pending task.
func makeCancelled(t *testing.T, repo *repository.Repository, id int64) {
	t.Helper()
	_, _, err := repo.CancelTaskByID(context.Background(), id)
	require.NoError(t, err)
}

func TestGetTaskByID_Found(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "get-by-id-found")

	got, err := repo.GetTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "get-by-id-found", got.Type)
	assert.Equal(t, []byte(`{"test":true}`), got.Payload)
	assert.Equal(t, "pending", got.Status)
	assert.Equal(t, 3, got.AttemptsLeft)
	assert.Equal(t, 0, got.AttemptsElapsed)
	assert.Nil(t, got.IdempotencyToken)
	assert.Nil(t, got.FinalizedAt)
	assert.Nil(t, got.AttemptedAt)
	assert.False(t, got.CreatedAt.IsZero())
	assert.False(t, got.UpdatedAt.IsZero())
}

func TestGetTaskByID_NotFound(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	_, err := repo.GetTaskByID(context.Background(), 999999)

	require.Error(t, err)
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestListTasks_FilterByStatus(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id1 := pushTask(t, repo, "list-filter-status-1")
	_ = pushTask(t, repo, "list-filter-status-2")
	makeFailed(t, repo, "list-filter-status-1", id1)

	got, err := repo.ListTasks(context.Background(), repository.ListTasksParams{
		Statuses: []string{"failed"},
		Limit:    10,
		Offset:   0,
	})

	require.NoError(t, err)
	assert.Equal(t, 1, got.Total)
	require.Len(t, got.Tasks, 1)
	assert.Equal(t, id1, got.Tasks[0].ID)
	assert.Equal(t, "failed", got.Tasks[0].Status)
}

func TestListTasks_Pagination(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = pushTask(t, repo, "list-pagination")
	}

	// First page
	page1, err := repo.ListTasks(context.Background(), repository.ListTasksParams{
		Types:    []string{"list-pagination"},
		Limit:    2,
		Offset:   0,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Equal(t, 5, page1.Total)
	assert.Len(t, page1.Tasks, 2)

	// Second page
	page2, err := repo.ListTasks(context.Background(), repository.ListTasksParams{
		Types:    []string{"list-pagination"},
		Limit:    2,
		Offset:   2,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Equal(t, 5, page2.Total)
	assert.Len(t, page2.Tasks, 2)

	// Pages must not overlap
	assert.NotEqual(t, page1.Tasks[0].ID, page2.Tasks[0].ID)
	assert.NotEqual(t, page1.Tasks[1].ID, page2.Tasks[0].ID)
}

func TestListTasks_OrderBy(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id1 := pushTask(t, repo, "list-order")
	id2 := pushTask(t, repo, "list-order")
	id3 := pushTask(t, repo, "list-order")

	got, err := repo.ListTasks(context.Background(), repository.ListTasksParams{
		Types:    []string{"list-order"},
		Limit:    10,
		Offset:   0,
		OrderBy:  "id",
		OrderDir: "DESC",
	})

	require.NoError(t, err)
	require.Len(t, got.Tasks, 3)
	assert.Equal(t, id3, got.Tasks[0].ID)
	assert.Equal(t, id2, got.Tasks[1].ID)
	assert.Equal(t, id1, got.Tasks[2].ID)
}

func TestCancelTaskByID_PendingTask(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "cancel-pending")

	got, wasModified, err := repo.CancelTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.True(t, wasModified)
	assert.Equal(t, "cancelled", got.Status)
	assert.NotNil(t, got.FinalizedAt)
}

func TestCancelTaskByID_RunningTask(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "cancel-running")
	makeRunning(t, repo, "cancel-running", id)

	got, wasModified, err := repo.CancelTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.True(t, wasModified)
	assert.Equal(t, "cancelled", got.Status)
}

func TestCancelTaskByID_CompletedTask(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "cancel-completed")
	makeCompleted(t, repo, "cancel-completed", id)

	got, wasModified, err := repo.CancelTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.False(t, wasModified)
	assert.Equal(t, "completed", got.Status)
}

func TestRetryTaskByID_FailedTask(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "retry-failed")
	makeFailed(t, repo, "retry-failed", id)

	got, wasModified, err := repo.RetryTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.True(t, wasModified)
	assert.Equal(t, "pending", got.Status)
	assert.GreaterOrEqual(t, got.AttemptsLeft, 1)
	assert.Nil(t, got.FinalizedAt)
}

func TestRetryTaskByID_PendingTask(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "retry-pending")

	_, wasModified, err := repo.RetryTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.False(t, wasModified)
}

func TestDeleteTaskByID_Success(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "delete-success")

	got, wasModified, err := repo.DeleteTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.True(t, wasModified)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "delete-success", got.Type)

	// Verify the task is actually gone
	_, err = repo.GetTaskByID(context.Background(), id)
	assert.True(t, errors.Is(err, sql.ErrNoRows))
}

func TestDeleteTaskByID_RunningTask(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id := pushTask(t, repo, "delete-running")
	makeRunning(t, repo, "delete-running", id)

	_, wasModified, err := repo.DeleteTaskByID(context.Background(), id)

	require.NoError(t, err)
	assert.False(t, wasModified)

	// Verify the task still exists
	got, err := repo.GetTaskByID(context.Background(), id)
	require.NoError(t, err)
	assert.Equal(t, "running", got.Status)
}

func TestGetCancelledTaskIDs_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id1 := pushTask(t, repo, "cancelled-ids-1")
	id2 := pushTask(t, repo, "cancelled-ids-2")
	id3 := pushTask(t, repo, "cancelled-ids-3")
	makeCancelled(t, repo, id1)
	makeCancelled(t, repo, id2)

	got, err := repo.GetCancelledTaskIDs(context.Background(), []int64{id1, id2, id3})

	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.ElementsMatch(t, []int64{id1, id2}, got)
}

func TestSnoozeTasksMany_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	id1 := pushTask(t, repo, "snooze-many-1")
	id2 := pushTask(t, repo, "snooze-many-2")
	makeRunning(t, repo, "snooze-many-1", id1)
	makeRunning(t, repo, "snooze-many-2", id2)

	futureTime := time.Now().Add(time.Hour)
	count, err := repo.SnoozeTasksMany(context.Background(), repository.SnoozeTasksManyParams{
		IDs:          []int64{id1, id2},
		BlockedTills: []time.Time{futureTime, futureTime},
	})

	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Verify tasks are now pending with updated blocked_till
	task1, err := repo.GetTaskByID(context.Background(), id1)
	require.NoError(t, err)
	assert.Equal(t, "pending", task1.Status)
	assert.WithinDuration(t, futureTime, task1.BlockedTill, time.Second)

	task2, err := repo.GetTaskByID(context.Background(), id2)
	require.NoError(t, err)
	assert.Equal(t, "pending", task2.Status)
	assert.WithinDuration(t, futureTime, task2.BlockedTill, time.Second)
}
