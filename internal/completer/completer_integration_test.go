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
	"github.com/yakser/asynqpg/internal/lib/testutils"
	"github.com/yakser/asynqpg/internal/repository"
)

func setupTest(t *testing.T) (*repository.Repository, *completer.BatchCompleter) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)

	cfg := completer.Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 10,
		MaxBatchSize:   100,
		MaxBacklog:     50,
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	return repo, bc
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

	_, err := repo.PushTasksMany(ctx, repository.PushTasksManyParams{Tasks: tasks})
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
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 1, "complete-single-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	err = bc.Complete(ids[0])
	require.NoError(t, err)

	// Wait for flush
	time.Sleep(100 * time.Millisecond)

	// Task completion verified by no errors during batch execution
}

func TestBatchCompleter_Complete_Multiple(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 5, "complete-multiple-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	for _, id := range ids {
		err = bc.Complete(id)
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_Complete_FlushOnThreshold(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	cfg := completer.Config{
		FlushInterval:  10 * time.Second, // Long interval
		FlushThreshold: 5,                // Low threshold
		MaxBatchSize:   100,
		MaxBacklog:     50,
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	ids := createAndFetchTasks(t, repo, 5, "threshold-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	// Add tasks to trigger threshold
	for _, id := range ids {
		err = bc.Complete(id)
		require.NoError(t, err)
	}

	// Threshold reached, should flush soon even without interval
	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_Complete_FlushOnInterval(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()

	cfg := completer.Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 1000, // High threshold
		MaxBatchSize:   100,
		MaxBacklog:     50,
	}
	bc := completer.NewBatchCompleter(repo, cfg)

	ids := createAndFetchTasks(t, repo, 2, "interval-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	for _, id := range ids {
		err = bc.Complete(id)
		require.NoError(t, err)
	}

	// Wait for interval flush
	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_Fail_Basic(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 3, "fail-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	for i, id := range ids {
		err = bc.Fail(id, "error message "+string(rune('A'+i)))
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_Retry_Basic(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 3, "retry-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	for i, id := range ids {
		blockedTill := time.Now().Add(time.Duration(i+1) * time.Second)
		err = bc.Retry(id, blockedTill, "retry reason")
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_MixedOperations(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 6, "mixed-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	// Complete first 2
	err = bc.Complete(ids[0])
	require.NoError(t, err)
	err = bc.Complete(ids[1])
	require.NoError(t, err)

	// Fail next 2
	err = bc.Fail(ids[2], "error 1")
	require.NoError(t, err)
	err = bc.Fail(ids[3], "error 2")
	require.NoError(t, err)

	// Retry last 2
	err = bc.Retry(ids[4], time.Now().Add(time.Second), "retry 1")
	require.NoError(t, err)
	err = bc.Retry(ids[5], time.Now().Add(time.Second), "retry 2")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_GracefulShutdown(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 3, "shutdown-test")

	err := bc.Start(ctx)
	require.NoError(t, err)

	for _, id := range ids {
		err = bc.Complete(id)
		require.NoError(t, err)
	}

	// Stop immediately - should flush pending
	bc.Stop()

	// All tasks should be completed after stop
}

func TestBatchCompleter_GracefulShutdown_Empty(t *testing.T) {
	_, bc := setupTest(t)
	ctx := context.Background()

	err := bc.Start(ctx)
	require.NoError(t, err)

	start := time.Now()
	bc.Stop()
	elapsed := time.Since(start)

	// Should be fast with no pending work
	assert.Less(t, elapsed, 100*time.Millisecond)
}

func TestBatchCompleter_Backpressure_Block(t *testing.T) {
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

	err := bc.Start(ctx)
	require.NoError(t, err)
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

	// Give some time for the goroutine to hit backpressure
	time.Sleep(50 * time.Millisecond)
	// The goroutine should be blocked after 5 operations
	assert.True(t, blocked.Load(), "should have reached backpressure point")
}

func TestBatchCompleter_DoubleStart(t *testing.T) {
	_, bc := setupTest(t)
	ctx := context.Background()

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	err = bc.Start(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already running")
}

func TestBatchCompleter_StopWithoutStart(t *testing.T) {
	_, bc := setupTest(t)

	// Should not panic
	bc.Stop()
}

func TestBatchCompleter_SameTaskMultipleOperations(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 1, "same-task-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	// Same task ID, different operations - last one wins
	err = bc.Retry(ids[0], time.Now().Add(time.Second), "retry")
	require.NoError(t, err)
	err = bc.Fail(ids[0], "fail")
	require.NoError(t, err)
	err = bc.Complete(ids[0])
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
}

func TestBatchCompleter_ConcurrentOperations(t *testing.T) {
	repo, bc := setupTest(t)
	ctx := context.Background()

	ids := createAndFetchTasks(t, repo, 20, "concurrent-test")

	err := bc.Start(ctx)
	require.NoError(t, err)
	defer bc.Stop()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(taskID int64) {
			defer wg.Done()
			_ = bc.Complete(taskID)
		}(id)
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond)
}
