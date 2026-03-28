//go:build integration

package completer_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/completer"
	"github.com/yakser/asynqpg/internal/lib/db"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

func setupTest(t *testing.T) (*repository.Repository, *repository.ClientRepository, *completer.BatchCompleter) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	clientRepo := repository.NewClientRepository(database)

	cfg := completer.Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 10,
		MaxBatchSize:   100,
		MaxBacklog:     50,
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	return repo, clientRepo, bc
}

func createAndFetchTasks(t *testing.T, repo *repository.Repository, count int, taskType string) []int64 {
	ctx := context.Background()

	tasks := make([]repository.PushTaskParams, count)
	for i := 0; i < count; i++ {
		tasks[i] = repository.PushTaskParams{
			Type:         taskType,
			Payload:      []byte(`{}`),
			AttemptsLeft: 3,
			Delay:        db.NewDuration(0),
		}
	}

	_, err := repo.PushTasks(ctx, repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)

	readyTasks, err := repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
		Type:  taskType,
		Limit: count,
		Delay: time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, readyTasks, count)

	ids := make([]int64, count)
	for i, task := range readyTasks {
		ids[i] = task.ID
	}
	return ids
}

func TestBatchCompleter_Complete_Single(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 1, "complete-single-test")

	require.NoError(t, bc.Start(ctx))

	require.NoError(t, bc.Complete(ids[0]))

	bc.Stop()

	task, err := clientRepo.GetTaskByID(ctx, ids[0])
	require.NoError(t, err)
	assert.Equal(t, "completed", task.Status)
	assert.NotNil(t, task.FinalizedAt)
}

func TestBatchCompleter_Complete_Multiple(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 5, "complete-multiple-test")

	require.NoError(t, bc.Start(ctx))

	for _, id := range ids {
		require.NoError(t, bc.Complete(id))
	}

	bc.Stop()

	for _, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status)
		assert.NotNil(t, task.FinalizedAt)
	}
}

func TestBatchCompleter_Complete_FlushOnThreshold(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	clientRepo := repository.NewClientRepository(database)
	ctx := context.Background()

	cfg := completer.Config{
		FlushInterval:  10 * time.Second, // Long interval -- threshold should trigger flush
		FlushThreshold: 5,
		MaxBatchSize:   100,
		MaxBacklog:     50,
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	ids := createAndFetchTasks(t, repo, 5, "threshold-test")

	require.NoError(t, bc.Start(ctx))
	defer bc.Stop()

	for _, id := range ids {
		require.NoError(t, bc.Complete(id))
	}

	// Threshold reached -- flush should happen without waiting for the 10s interval
	require.Eventually(t, func() bool {
		task, _ := clientRepo.GetTaskByID(ctx, ids[0])
		return task != nil && task.Status == "completed"
	}, 2*time.Second, 10*time.Millisecond)

	for _, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status)
	}
}

func TestBatchCompleter_Complete_FlushOnInterval(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	clientRepo := repository.NewClientRepository(database)
	ctx := context.Background()

	cfg := completer.Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 1000, // High threshold -- interval should trigger flush
		MaxBatchSize:   100,
		MaxBacklog:     50,
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	ids := createAndFetchTasks(t, repo, 2, "interval-test")

	require.NoError(t, bc.Start(ctx))
	defer bc.Stop()

	for _, id := range ids {
		require.NoError(t, bc.Complete(id))
	}

	// Interval flush should complete tasks within a few ticks
	require.Eventually(t, func() bool {
		task, _ := clientRepo.GetTaskByID(ctx, ids[0])
		return task != nil && task.Status == "completed"
	}, 2*time.Second, 10*time.Millisecond)

	for _, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status)
	}
}

func TestBatchCompleter_Fail_Basic(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 3, "fail-test")

	require.NoError(t, bc.Start(ctx))

	for i, id := range ids {
		require.NoError(t, bc.Fail(id, "error message "+string(rune('A'+i))))
	}

	bc.Stop()

	for i, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status)
		assert.NotNil(t, task.FinalizedAt)
		assert.Contains(t, []string(task.Messages), "error message "+string(rune('A'+i)))
	}
}

func TestBatchCompleter_Retry_Basic(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 3, "retry-test")

	require.NoError(t, bc.Start(ctx))

	blockedTills := make([]time.Time, len(ids))
	for i, id := range ids {
		blockedTills[i] = time.Now().Add(time.Duration(i+1) * time.Second)
		require.NoError(t, bc.Retry(id, blockedTills[i], "retry reason"))
	}

	bc.Stop()

	for i, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "pending", task.Status)
		assert.Nil(t, task.FinalizedAt)
		assert.Equal(t, 2, task.AttemptsLeft)
		assert.Equal(t, 1, task.AttemptsElapsed)
		assert.Contains(t, []string(task.Messages), "retry reason")
		assert.WithinDuration(t, blockedTills[i], task.BlockedTill, time.Second)
	}
}

func TestBatchCompleter_MixedOperations(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 6, "mixed-test")

	require.NoError(t, bc.Start(ctx))

	// Complete first 2
	require.NoError(t, bc.Complete(ids[0]))
	require.NoError(t, bc.Complete(ids[1]))

	// Fail next 2
	require.NoError(t, bc.Fail(ids[2], "error 1"))
	require.NoError(t, bc.Fail(ids[3], "error 2"))

	// Retry last 2
	retryTime := time.Now().Add(time.Second)
	require.NoError(t, bc.Retry(ids[4], retryTime, "retry 1"))
	require.NoError(t, bc.Retry(ids[5], retryTime, "retry 2"))

	bc.Stop()

	// Verify completed
	for _, id := range ids[:2] {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status)
	}

	// Verify failed
	for _, id := range ids[2:4] {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status)
	}

	// Verify retried
	for _, id := range ids[4:6] {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "pending", task.Status)
		assert.Equal(t, 2, task.AttemptsLeft)
		assert.Equal(t, 1, task.AttemptsElapsed)
	}
}

func TestBatchCompleter_GracefulShutdown(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 3, "shutdown-test")

	require.NoError(t, bc.Start(ctx))

	for _, id := range ids {
		require.NoError(t, bc.Complete(id))
	}

	// Stop immediately -- should flush pending operations before returning
	bc.Stop()

	for _, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status)
	}
}

func TestBatchCompleter_GracefulShutdown_Empty(t *testing.T) {
	t.Parallel()

	_, _, bc := setupTest(t)
	ctx := context.Background()

	require.NoError(t, bc.Start(ctx))

	start := time.Now()
	bc.Stop()
	elapsed := time.Since(start)

	// Should be fast with no pending work
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestBatchCompleter_Backpressure_Block(t *testing.T) {
	t.Parallel()

	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	cfg := completer.Config{
		FlushInterval:  10 * time.Second, // Long interval to prevent automatic flush
		FlushThreshold: 1000,             // High threshold
		MaxBatchSize:   100,
		MaxBacklog:     5, // Low backlog limit
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	ids := createAndFetchTasks(t, repo, 10, "backpressure-test")

	require.NoError(t, bc.Start(ctx))
	defer bc.Stop()

	var blocked atomic.Bool
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for i, id := range ids {
			if i >= 5 {
				blocked.Store(true)
			}
			_ = bc.Complete(id)
		}
	}()

	// The goroutine should be blocked after 5 operations
	assert.Eventually(t, blocked.Load, 2*time.Second, 10*time.Millisecond, "should have reached backpressure point")
}

func TestBatchCompleter_DoubleStart(t *testing.T) {
	t.Parallel()

	_, _, bc := setupTest(t)
	ctx := context.Background()

	require.NoError(t, bc.Start(ctx))
	defer bc.Stop()

	err := bc.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
}

func TestBatchCompleter_StopWithoutStart(t *testing.T) {
	t.Parallel()

	_, _, bc := setupTest(t)

	// Should not panic
	bc.Stop()
}

func TestBatchCompleter_SameTaskMultipleOperations(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 1, "same-task-test")

	require.NoError(t, bc.Start(ctx))

	// Same task ID, different operations -- each type uses a separate map.
	// All three UPDATEs require status='running', so only the first to execute
	// will match; the others will affect 0 rows.
	require.NoError(t, bc.Retry(ids[0], time.Now().Add(time.Second), "retry"))
	require.NoError(t, bc.Fail(ids[0], "fail"))
	require.NoError(t, bc.Complete(ids[0]))

	bc.Stop()

	task, err := clientRepo.GetTaskByID(ctx, ids[0])
	require.NoError(t, err)
	assert.NotEqual(t, "running", task.Status, "task should have been transitioned from running")
}

func TestBatchCompleter_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	repo, clientRepo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 20, "concurrent-test")

	require.NoError(t, bc.Start(ctx))

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(taskID int64) {
			defer wg.Done()
			_ = bc.Complete(taskID)
		}(id)
	}

	wg.Wait()
	bc.Stop()

	for _, id := range ids {
		task, err := clientRepo.GetTaskByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status)
	}
}
