//go:build integration

package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

func setupClient(t *testing.T) (*client.Client, *repository.Repository) {
	t.Helper()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	c, err := client.New(client.Config{Pool: database})
	require.NoError(t, err)

	return c, repo
}

func createPendingTask(t *testing.T, repo *repository.Repository, taskType string) int64 {
	t.Helper()

	ctx := context.Background()
	tasks := []repository.PushTaskParams{
		{Type: taskType, Payload: []byte(`{"test":true}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	ids, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	return ids[0]
}

func createRunningTask(t *testing.T, repo *repository.Repository, taskType string) int64 {
	t.Helper()

	ctx := context.Background()
	id := createPendingTask(t, repo, taskType)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskType,
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, 1)
	require.Equal(t, id, readyTasks[0].ID)

	return id
}

func createFailedTask(t *testing.T, repo *repository.Repository, taskType string) int64 {
	t.Helper()

	ctx := context.Background()
	id := createRunningTask(t, repo, taskType)
	err := repo.FailTasks(ctx, []int64{id}, "test failure")
	require.NoError(t, err)
	return id
}

func createCompletedTask(t *testing.T, repo *repository.Repository, taskType string) int64 {
	t.Helper()

	ctx := context.Background()
	id := createRunningTask(t, repo, taskType)
	err := repo.CompleteTasks(ctx, []int64{id})
	require.NoError(t, err)
	return id
}

// --- GetTask tests ---

func TestGetTask_Found(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "get-task-test")

	got, err := c.GetTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "get-task-test", got.Type)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Equal(t, []byte(`{"test":true}`), got.Payload)
	assert.Equal(t, 3, got.AttemptsLeft)
}

func TestGetTask_NotFound(t *testing.T) {
	t.Parallel()
	c, _ := setupClient(t)
	ctx := context.Background()

	_, err := c.GetTask(ctx, 999999)
	assert.ErrorIs(t, err, client.ErrTaskNotFound)
}

// --- ListTasks tests ---

func TestListTasks_NoFilters(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	createPendingTask(t, repo, "list-nofilter-1")
	createPendingTask(t, repo, "list-nofilter-2")

	result, err := c.ListTasks(ctx, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, result.Total, 2)
	assert.GreaterOrEqual(t, len(result.Tasks), 2)
}

func TestListTasks_ByStatus(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	createFailedTask(t, repo, "list-status-test")
	createPendingTask(t, repo, "list-status-test")

	result, err := c.ListTasks(ctx, client.NewListParams().
		States(asynqpg.TaskStatusFailed))
	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
	assert.Len(t, result.Tasks, 1)
	assert.Equal(t, asynqpg.TaskStatusFailed, result.Tasks[0].Status)
}

func TestListTasks_ByType(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	createPendingTask(t, repo, "list-type-alpha")
	createPendingTask(t, repo, "list-type-beta")
	createPendingTask(t, repo, "list-type-alpha")

	result, err := c.ListTasks(ctx, client.NewListParams().
		Types("list-type-alpha"))
	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	for _, task := range result.Tasks {
		assert.Equal(t, "list-type-alpha", task.Type)
	}
}

func TestListTasks_ByIDs(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id1 := createPendingTask(t, repo, "list-ids-test")
	createPendingTask(t, repo, "list-ids-test")
	id3 := createPendingTask(t, repo, "list-ids-test")

	result, err := c.ListTasks(ctx, client.NewListParams().
		IDs(id1, id3))
	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	assert.Len(t, result.Tasks, 2)
}

func TestListTasks_Pagination(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		createPendingTask(t, repo, "list-page-test")
	}

	page1, err := c.ListTasks(ctx, client.NewListParams().
		Types("list-page-test").
		Limit(2).
		Offset(0))
	require.NoError(t, err)
	assert.Len(t, page1.Tasks, 2)
	assert.Equal(t, 5, page1.Total)

	page2, err := c.ListTasks(ctx, client.NewListParams().
		Types("list-page-test").
		Limit(2).
		Offset(2))
	require.NoError(t, err)
	assert.Len(t, page2.Tasks, 2)

	// Ensure different tasks
	assert.NotEqual(t, page1.Tasks[0].ID, page2.Tasks[0].ID)
}

func TestListTasks_OrderBy(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	createPendingTask(t, repo, "list-order-test")
	createPendingTask(t, repo, "list-order-test")
	createPendingTask(t, repo, "list-order-test")

	result, err := c.ListTasks(ctx, client.NewListParams().
		Types("list-order-test").
		OrderBy(client.OrderByID, client.SortDesc))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(result.Tasks), 3)

	for i := 1; i < len(result.Tasks); i++ {
		assert.Greater(t, result.Tasks[i-1].ID, result.Tasks[i].ID)
	}
}

func TestListTasks_EmptyResult(t *testing.T) {
	t.Parallel()
	c, _ := setupClient(t)
	ctx := context.Background()

	result, err := c.ListTasks(ctx, client.NewListParams().
		Types("nonexistent-type-xyz"))
	require.NoError(t, err)
	assert.Equal(t, 0, result.Total)
	assert.Empty(t, result.Tasks)
}

func TestListTasks_CombinedFilters(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	// Create failed task first to avoid GetReadyTasks picking up the wrong pending task
	createFailedTask(t, repo, "list-combined-a")
	createPendingTask(t, repo, "list-combined-a")
	createPendingTask(t, repo, "list-combined-b")

	result, err := c.ListTasks(ctx, client.NewListParams().
		Types("list-combined-a").
		States(asynqpg.TaskStatusPending))
	require.NoError(t, err)
	require.Equal(t, 1, result.Total)
	assert.Equal(t, "list-combined-a", result.Tasks[0].Type)
	assert.Equal(t, asynqpg.TaskStatusPending, result.Tasks[0].Status)
}

// --- CancelTask tests ---

func TestCancelTask_Pending(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "cancel-pending-test")

	got, err := c.CancelTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
	assert.NotNil(t, got.FinalizedAt)
}

func TestCancelTask_Failed(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createFailedTask(t, repo, "cancel-failed-test")

	got, err := c.CancelTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
}

func TestCancelTask_Cancelled_Idempotent(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "cancel-idempotent-test")

	_, err := c.CancelTask(ctx, id)
	require.NoError(t, err)

	got, err := c.CancelTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
}

func TestCancelTask_Running(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createRunningTask(t, repo, "cancel-running-test")

	got, err := c.CancelTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
}

func TestCancelTask_Completed(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createCompletedTask(t, repo, "cancel-completed-test")

	got, err := c.CancelTask(ctx, id)
	assert.ErrorIs(t, err, client.ErrTaskAlreadyFinalized)
	assert.NotNil(t, got)
	assert.Equal(t, asynqpg.TaskStatusCompleted, got.Status)
}

func TestCancelTask_NotFound(t *testing.T) {
	t.Parallel()
	c, _ := setupClient(t)
	ctx := context.Background()

	_, err := c.CancelTask(ctx, 999999)
	assert.ErrorIs(t, err, client.ErrTaskNotFound)
}

// --- RetryTask tests ---

func TestRetryTask_Failed(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createFailedTask(t, repo, "retry-failed-test")

	got, err := c.RetryTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Nil(t, got.FinalizedAt)
	assert.GreaterOrEqual(t, got.AttemptsLeft, 1)
}

func TestRetryTask_Cancelled(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "retry-cancelled-test")
	_, err := c.CancelTask(ctx, id)
	require.NoError(t, err)

	got, err := c.RetryTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Nil(t, got.FinalizedAt)
}

func TestRetryTask_ExhaustedAttempts(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	// Create a task with 1 attempt, run it, then fail it -> attempts_left becomes 0
	tasks := []repository.PushTaskParams{
		{Type: "retry-exhausted-test", Payload: []byte(`{}`), AttemptsLeft: 1, Delay: db.NewDuration(0)},
	}
	ids, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)
	id := ids[0]

	// Fetch to set status to running
	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type: "retry-exhausted-test", Limit: 1, Delay: time.Minute,
	})
	require.NoError(t, err)

	// Fail the task (sets attempts_left via normal flow – but FailTasks doesn't decrement)
	// We need to simulate exhaustion: set attempts_left = 0 via RetryTask then fail
	// Actually FailTasks doesn't change attempts_left. The task has attempts_left=1, status=running.
	// Let's fail it directly.
	err = repo.FailTasks(ctx, []int64{id}, "exhausted")
	require.NoError(t, err)

	// Now the task is failed with attempts_left=1. Let's verify retry sets to pending.
	// For true exhaustion test, we need attempts_left=0. Use RetryTask from repo to decrement, then fail again.
	err = repo.RetryTask(ctx, repository.RetryTaskParams{
		ID: id, BlockedTill: time.Now(), Message: "retry",
	})
	require.NoError(t, err)

	// Now it's pending with attempts_left=0. Fetch and fail again.
	_, err = repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type: "retry-exhausted-test", Limit: 1, Delay: time.Minute,
	})
	require.NoError(t, err)
	err = repo.FailTasks(ctx, []int64{id}, "exhausted again")
	require.NoError(t, err)

	// Now task is failed with attempts_left=0
	got, err := c.RetryTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Equal(t, 1, got.AttemptsLeft, "should set attempts_left to 1 when exhausted")
}

func TestRetryTask_Pending(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "retry-pending-test")

	got, err := c.RetryTask(ctx, id)
	assert.ErrorIs(t, err, client.ErrTaskAlreadyAvailable)
	assert.NotNil(t, got)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
}

func TestRetryTask_Running(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createRunningTask(t, repo, "retry-running-test")

	got, err := c.RetryTask(ctx, id)
	assert.ErrorIs(t, err, client.ErrTaskRunning)
	assert.NotNil(t, got)
	assert.Equal(t, asynqpg.TaskStatusRunning, got.Status)
}

func TestRetryTask_Completed(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createCompletedTask(t, repo, "retry-completed-test")

	got, err := c.RetryTask(ctx, id)
	assert.ErrorIs(t, err, client.ErrTaskAlreadyFinalized)
	assert.NotNil(t, got)
}

func TestRetryTask_NotFound(t *testing.T) {
	t.Parallel()
	c, _ := setupClient(t)
	ctx := context.Background()

	_, err := c.RetryTask(ctx, 999999)
	assert.ErrorIs(t, err, client.ErrTaskNotFound)
}

// --- DeleteTask tests ---

func TestDeleteTask_Pending(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "delete-pending-test")

	got, err := c.DeleteTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)

	// Verify task is actually gone
	_, err = c.GetTask(ctx, id)
	assert.ErrorIs(t, err, client.ErrTaskNotFound)
}

func TestDeleteTask_Failed(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createFailedTask(t, repo, "delete-failed-test")

	got, err := c.DeleteTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
}

func TestDeleteTask_Cancelled(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "delete-cancelled-test")
	_, err := c.CancelTask(ctx, id)
	require.NoError(t, err)

	got, err := c.DeleteTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
}

func TestDeleteTask_Completed(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createCompletedTask(t, repo, "delete-completed-test")

	got, err := c.DeleteTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
}

func TestDeleteTask_Running(t *testing.T) {
	t.Parallel()
	c, repo := setupClient(t)
	ctx := context.Background()

	id := createRunningTask(t, repo, "delete-running-test")

	got, err := c.DeleteTask(ctx, id)
	assert.ErrorIs(t, err, client.ErrTaskRunning)
	assert.NotNil(t, got)
	assert.Equal(t, asynqpg.TaskStatusRunning, got.Status)

	// Verify task still exists
	existing, err := c.GetTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, existing.ID)
}

func TestDeleteTask_NotFound(t *testing.T) {
	t.Parallel()
	c, _ := setupClient(t)
	ctx := context.Background()

	_, err := c.DeleteTask(ctx, 999999)
	assert.ErrorIs(t, err, client.ErrTaskNotFound)
}

// --- New client validation ---

func TestNew_NilDB(t *testing.T) {
	t.Parallel()

	_, err := client.New(client.Config{})
	assert.Error(t, err)
}

// --- Helpers for Tx tests ---

func setupClientWithDB(t *testing.T) (*client.Client, *repository.Repository, *sqlx.DB) {
	t.Helper()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	c, err := client.New(client.Config{Pool: database})
	require.NoError(t, err)

	return c, repo, database
}

// --- Tx method tests ---

func TestGetTaskTx_Success(t *testing.T) {
	t.Parallel()
	c, repo, database := setupClientWithDB(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "get-task-tx-test")

	tx, err := database.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	got, err := c.GetTaskTx(ctx, tx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "get-task-tx-test", got.Type)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
}

func TestCancelTaskTx_CommitPersists(t *testing.T) {
	t.Parallel()
	c, repo, database := setupClientWithDB(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "cancel-tx-commit-test")

	tx, err := database.BeginTxx(ctx, nil)
	require.NoError(t, err)

	got, err := c.CancelTaskTx(ctx, tx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify cancelled outside the transaction
	got, err = c.GetTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
}

func TestDeleteTaskTx_RollbackReverts(t *testing.T) {
	t.Parallel()
	c, repo, database := setupClientWithDB(t)
	ctx := context.Background()

	id := createPendingTask(t, repo, "delete-tx-rollback-test")

	tx, err := database.BeginTxx(ctx, nil)
	require.NoError(t, err)

	_, err = c.DeleteTaskTx(ctx, tx, id)
	require.NoError(t, err)

	err = tx.Rollback()
	require.NoError(t, err)

	// Verify task still exists after rollback
	got, err := c.GetTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
}

func TestRetryTaskTx_CommitPersists(t *testing.T) {
	t.Parallel()
	c, repo, database := setupClientWithDB(t)
	ctx := context.Background()

	id := createFailedTask(t, repo, "retry-tx-commit-test")

	tx, err := database.BeginTxx(ctx, nil)
	require.NoError(t, err)

	got, err := c.RetryTaskTx(ctx, tx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)

	err = tx.Commit()
	require.NoError(t, err)

	// Verify task is back to pending outside the transaction
	got, err = c.GetTask(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
}

func TestListTasksTx_Success(t *testing.T) {
	t.Parallel()
	c, repo, database := setupClientWithDB(t)
	ctx := context.Background()

	createPendingTask(t, repo, "list-tx-test")
	createPendingTask(t, repo, "list-tx-test")

	tx, err := database.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	result, err := c.ListTasksTx(ctx, tx, client.NewListParams().
		Types("list-tx-test"))
	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	assert.Len(t, result.Tasks, 2)
}
