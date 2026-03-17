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

func TestPushTasksMany_Basic(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: "batch-test", Payload: []byte(`{"id":1}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "batch-test", Payload: []byte(`{"id":2}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "batch-test", Payload: []byte(`{"id":3}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}

	ids, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)
	assert.Len(t, ids, 3)
}

func TestPushTasksMany_EmptyArray(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	ids, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: []repository.PushTaskParams{}})
	require.NoError(t, err)
	assert.Len(t, ids, 0)
}

func TestPushTasksMany_Idempotency(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	token := "unique-token-1"
	tasks := []repository.PushTaskParams{
		{Type: "batch-test", Payload: []byte(`{"id":1}`), AttemptsLeft: 3, Delay: db.NewDuration(0), IdempotencyToken: &token},
	}

	// First insert
	ids1, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)
	assert.Len(t, ids1, 1)

	// Second insert with same token - should be skipped
	ids2, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)
	assert.Len(t, ids2, 0) // No new tasks inserted
}

func TestCompleteTasksMany_Basic(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Create and fetch tasks to set them to 'running' status
	tasks := []repository.PushTaskParams{
		{Type: "complete-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "complete-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)

	// Fetch to set status to 'running'
	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "complete-test",
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
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	count, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{}})
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestCompleteTasksMany_NonExistentIDs(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	count, err := repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: []int64{999999, 999998}})
	require.NoError(t, err)
	assert.Equal(t, 0, count) // No tasks updated
}

func TestCompleteTasksMany_AlreadyCompleted(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Create and complete a task
	tasks := []repository.PushTaskParams{
		{Type: "complete-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "complete-test",
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
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: "fail-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "fail-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "fail-test",
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
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: "fail-msg-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "fail-msg-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "fail-msg-test",
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
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	tasks := []repository.PushTaskParams{
		{Type: "retry-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "retry-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "retry-test",
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
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	// Create 3 tasks
	tasks := []repository.PushTaskParams{
		{Type: "mixed-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "mixed-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
		{Type: "mixed-test", Payload: []byte(`{}`), AttemptsLeft: 3, Delay: db.NewDuration(0)},
	}
	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
	require.NoError(t, err)

	// Fetch all to set to 'running'
	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  "mixed-test",
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
