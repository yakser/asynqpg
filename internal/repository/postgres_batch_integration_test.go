//go:build integration

package repository_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

const (
	taskTypeBatch    = "batch-test"
	taskTypeComplete = "complete-test"
	taskTypeFail     = "fail-test"
	taskTypeFailMsg  = "fail-msg-test"
	taskTypeRetry    = "retry-test"
	taskTypeMixed    = "mixed-test"
)

func TestPushTasks_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: taskTypeBatch, Payload: []byte(`{"id":1}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeBatch, Payload: []byte(`{"id":2}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeBatch, Payload: []byte(`{"id":3}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}

	results, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)
	assert.Len(t, results, 3)
	for _, r := range results {
		assert.False(t, r.Duplicate)
		assert.NotZero(t, r.ID)
	}
}

func TestPushTasks_EmptyArray(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	results, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: []repository.PushTaskParams{}})
	require.NoError(t, err)
	assert.Len(t, results, 0)
}

func TestPushTasks_Idempotency(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	token := "unique-token-1"
	tasks := []repository.PushTaskParams{
		{Type: taskTypeBatch, Payload: []byte(`{"id":1}`), AttemptsLeft: 3, Delay: db.NewDuration(0), IdempotencyToken: &token},
	}

	// First insert
	results1, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)
	assert.Len(t, results1, 1)
	assert.False(t, results1[0].Duplicate)

	// Second insert with same token - flagged as duplicate
	results2, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)
	assert.Len(t, results2, 1)
	assert.True(t, results2[0].Duplicate)
	assert.Equal(t, results1[0].ID, results2[0].ID)
}

func TestCompleteTasksMany_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Create and fetch tasks to set them to 'running' status
	tasks := []repository.PushTaskParams{
		{Type: taskTypeComplete, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeComplete, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	// Fetch to set status to 'running'
	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskTypeComplete,
		Limit: 10,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, 2)

	ids := []int64{readyTasks[0].ID, readyTasks[1].ID}
	count, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: ids})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestCompleteTasksMany_EmptyArray(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	count, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{}})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestCompleteTasksMany_NonExistentIDs(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	count, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{999999, 999998}})
	require.NoError(t, err)
	assert.Equal(t, 0, count) // No tasks updated
}

func TestCompleteTasksMany_AlreadyCompleted(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Create and complete a task
	tasks := []repository.PushTaskParams{
		{Type: taskTypeComplete, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskTypeComplete,
		Limit: 1,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, 1)

	// Complete once
	count1, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{readyTasks[0].ID}})
	require.NoError(t, err)
	assert.Equal(t, 1, count1)

	// Complete again - should be idempotent (0 affected)
	count2, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{readyTasks[0].ID}})
	require.NoError(t, err)
	assert.Equal(t, 0, count2)
}

func TestFailTasksMany_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: taskTypeFail, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeFail, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskTypeFail,
		Limit: 10,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, 2)

	ids := []int64{readyTasks[0].ID, readyTasks[1].ID}
	messages := []string{"error 1", "error 2"}

	count, err := repo.FailTasksMany(ctx, repository.FailTasksManyParams{IDs: ids, Messages: messages})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestFailTasksMany_DifferentMessages(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: taskTypeFailMsg, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeFailMsg, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskTypeFailMsg,
		Limit: 10,
		Delay: time.Minute,
	})
	require.NoError(t, err)

	ids := []int64{readyTasks[0].ID, readyTasks[1].ID}
	messages := []string{"unique error for task 1", "unique error for task 2"}

	_, err = repo.FailTasksMany(ctx, repository.FailTasksManyParams{IDs: ids, Messages: messages})
	require.NoError(t, err)

	// Verify messages were stored correctly (would need a GetTask method to verify)
}

func TestFailTasksMany_MismatchedLengths(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	_, err := repo.FailTasksMany(ctx, repository.FailTasksManyParams{
		IDs:      []int64{1, 2, 3},
		Messages: []string{"only one message"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same length")
}

func TestRetryTasksMany_Basic(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: taskTypeRetry, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeRetry, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskTypeRetry,
		Limit: 10,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, 2)

	ids := []int64{readyTasks[0].ID, readyTasks[1].ID}
	blockedTills := []time.Time{
		time.Now().Add(10 * time.Second),
		time.Now().Add(20 * time.Second),
	}
	messages := []string{"retry reason 1", "retry reason 2"}

	count, err := repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{
		IDs:          ids,
		BlockedTills: blockedTills,
		Messages:     messages,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestRetryTasksMany_MismatchedLengths(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	_, err := repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{
		IDs:          []int64{1, 2},
		BlockedTills: []time.Time{time.Now()}, // Only 1
		Messages:     []string{"msg1", "msg2"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "same length")
}

func TestRetryTasksMany_MixedStates(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Create 3 tasks
	tasks := []repository.PushTaskParams{
		{Type: taskTypeMixed, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeMixed, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: taskTypeMixed, Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	// Fetch all to set to 'running'
	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskTypeMixed,
		Limit: 10,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, 3)

	// Complete one task
	_, err = repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{readyTasks[0].ID}})
	require.NoError(t, err)

	// Try to retry all 3 - only 2 should be affected (the completed one won't be)
	ids := []int64{readyTasks[0].ID, readyTasks[1].ID, readyTasks[2].ID}
	blockedTills := []time.Time{time.Now().Add(time.Second), time.Now().Add(time.Second), time.Now().Add(time.Second)}
	messages := []string{"msg1", "msg2", "msg3"}

	count, err := repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{
		IDs:          ids,
		BlockedTills: blockedTills,
		Messages:     messages,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, count) // Only 2 tasks were in 'running' state
}
