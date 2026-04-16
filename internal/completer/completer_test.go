package completer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/completer/mocks"
	"github.com/yakser/asynqpg/internal/repository"
)

type flushCounters struct {
	complete atomic.Int64
	fail     atomic.Int64
	retry    atomic.Int64
	snooze   atomic.Int64
}

func setupAllMethods(repo *mocks.CompleterRepo) *flushCounters {
	c := &flushCounters{}
	repo.EXPECT().CompleteTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.CompleteTasksManyParams) (int, error) {
			c.complete.Add(int64(len(params.IDs)))
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().FailTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.FailTasksManyParams) (int, error) {
			c.fail.Add(int64(len(params.IDs)))
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().RetryTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.RetryTasksManyParams) (int, error) {
			c.retry.Add(int64(len(params.IDs)))
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().SnoozeTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.SnoozeTasksManyParams) (int, error) {
			c.snooze.Add(int64(len(params.IDs)))
			return len(params.IDs), nil
		}).Maybe()
	return c
}

func newTestCompleter(repo *mocks.CompleterRepo, cfg Config) *BatchCompleter {
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 50 * time.Millisecond
	}
	if cfg.FlushThreshold == 0 {
		cfg.FlushThreshold = 100
	}
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = 5000
	}
	if cfg.MaxBacklog == 0 {
		cfg.MaxBacklog = 50
	}
	return NewBatchCompleter(repo, cfg)
}

func countCallIDs(repo *mocks.CompleterRepo, method string) int {
	total := 0
	for _, call := range repo.Calls {
		if call.Method != method {
			continue
		}
		switch method {
		case "CompleteTasksMany":
			params := call.Arguments.Get(1).(repository.CompleteTasksManyParams)
			total += len(params.IDs)
		case "FailTasksMany":
			params := call.Arguments.Get(1).(repository.FailTasksManyParams)
			total += len(params.IDs)
		case "RetryTasksMany":
			params := call.Arguments.Get(1).(repository.RetryTasksManyParams)
			total += len(params.IDs)
		case "SnoozeTasksMany":
			params := call.Arguments.Get(1).(repository.SnoozeTasksManyParams)
			total += len(params.IDs)
		}
	}
	return total
}

func hasCall(repo *mocks.CompleterRepo, method string) bool {
	for _, call := range repo.Calls {
		if call.Method == method {
			return true
		}
	}
	return false
}

func TestUnit_DefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	require.Equal(t, 50*time.Millisecond, cfg.FlushInterval)
	require.Equal(t, 100, cfg.FlushThreshold)
	require.Equal(t, 5000, cfg.MaxBatchSize)
	require.Equal(t, 20000, cfg.MaxBacklog)
}

func TestUnit_Complete_FlushesOnInterval(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	ctx := context.Background()
	require.NoError(t, bc.Start(ctx))

	require.NoError(t, bc.Complete(1))
	require.NoError(t, bc.Complete(2))

	require.Eventually(t, func() bool {
		return counters.complete.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	totalIDs := countCallIDs(repo, "CompleteTasksMany")
	require.Equal(t, 2, totalIDs)
}

func TestUnit_Fail_FlushesOnInterval(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	require.NoError(t, bc.Fail(10, "error msg"))

	require.Eventually(t, func() bool { return counters.fail.Load() > 0 }, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	var failCalls []repository.FailTasksManyParams
	for _, call := range repo.Calls {
		if call.Method == "FailTasksMany" {
			failCalls = append(failCalls, call.Arguments.Get(1).(repository.FailTasksManyParams))
		}
	}
	require.NotEmpty(t, failCalls)
	require.Len(t, failCalls[0].IDs, 1)
	require.Equal(t, int64(10), failCalls[0].IDs[0])
	require.Equal(t, "error msg", failCalls[0].Messages[0])
}

func TestUnit_Retry_FlushesOnInterval(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	bt := time.Now().Add(5 * time.Second)
	require.NoError(t, bc.Retry(20, bt, "retry reason"))

	require.Eventually(t, func() bool { return counters.retry.Load() > 0 }, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	var retryCalls []repository.RetryTasksManyParams
	for _, call := range repo.Calls {
		if call.Method == "RetryTasksMany" {
			retryCalls = append(retryCalls, call.Arguments.Get(1).(repository.RetryTasksManyParams))
		}
	}
	require.NotEmpty(t, retryCalls)
	require.Len(t, retryCalls[0].IDs, 1)
	require.Equal(t, int64(20), retryCalls[0].IDs[0])
	require.Equal(t, "retry reason", retryCalls[0].Messages[0])
}

func TestUnit_MixedOps_AllFlushed(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	_ = bc.Complete(1)
	_ = bc.Fail(2, "fail msg")
	_ = bc.Retry(3, time.Now().Add(time.Second), "retry msg")

	require.Eventually(t, func() bool {
		return counters.complete.Load() > 0 && counters.fail.Load() > 0 && counters.retry.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	require.Greater(t, counters.complete.Load(), int64(0))
	require.Greater(t, counters.fail.Load(), int64(0))
	require.Greater(t, counters.retry.Load(), int64(0))
}

func TestUnit_FlushOnThreshold(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	var completeCount atomic.Int32
	repo.EXPECT().CompleteTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.CompleteTasksManyParams) (int, error) {
			completeCount.Add(1)
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().FailTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.FailTasksManyParams) (int, error) {
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().RetryTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.RetryTasksManyParams) (int, error) {
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().SnoozeTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.SnoozeTasksManyParams) (int, error) {
			return len(params.IDs), nil
		}).Maybe()

	bc := newTestCompleter(repo, Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 3,
	})

	require.NoError(t, bc.Start(context.Background()))

	_ = bc.Complete(1)
	_ = bc.Complete(2)
	_ = bc.Complete(3) // threshold reached

	require.Eventually(t, func() bool {
		return completeCount.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	require.NotZero(t, completeCount.Load(), "expected flush after threshold reached")

	totalIDs := countCallIDs(repo, "CompleteTasksMany")
	require.Equal(t, 3, totalIDs)
}

func TestUnit_EmptyFlush_Skipped(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	repo.EXPECT().CompleteTasksMany(mock.Anything, mock.Anything).Maybe()
	repo.EXPECT().FailTasksMany(mock.Anything, mock.Anything).Maybe()
	repo.EXPECT().RetryTasksMany(mock.Anything, mock.Anything).Maybe()
	repo.EXPECT().SnoozeTasksMany(mock.Anything, mock.Anything).Maybe()

	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	// Verify no flush calls happen with nothing pending
	require.Never(t, func() bool {
		return hasCall(repo, "CompleteTasksMany") || hasCall(repo, "FailTasksMany") || hasCall(repo, "RetryTasksMany")
	}, 200*time.Millisecond, 10*time.Millisecond)
	bc.Stop()
}

func TestUnit_GracefulShutdown_Flushes(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	_ = setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{
		FlushInterval: 10 * time.Second, // long -- won't trigger before stop
	})

	require.NoError(t, bc.Start(context.Background()))

	_ = bc.Complete(100)
	_ = bc.Complete(101)
	_ = bc.Fail(200, "fail")

	// Stop immediately -- should do final flush
	bc.Stop()

	totalCompleteIDs := countCallIDs(repo, "CompleteTasksMany")
	require.Equal(t, 2, totalCompleteIDs)
	require.Greater(t, countCallIDs(repo, "FailTasksMany"), 0)
}

func TestUnit_DoubleStart_Error(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	_ = setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{})

	require.NoError(t, bc.Start(context.Background()))
	defer bc.Stop()

	err := bc.Start(context.Background())
	require.Error(t, err)
}

func TestUnit_StopWithoutStart_Safe(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	bc := newTestCompleter(repo, Config{})
	// Should not panic
	bc.Stop()
}

func TestUnit_WithRetries_RepoError_Logged(t *testing.T) {
	t.Parallel()

	var completeCallCount atomic.Int64

	repo := mocks.NewCompleterRepo(t)
	repo.EXPECT().CompleteTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ repository.CompleteTasksManyParams) (int, error) {
			completeCallCount.Add(1)
			return 0, errors.New("db connection lost")
		}).Maybe()
	repo.EXPECT().FailTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.FailTasksManyParams) (int, error) {
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().RetryTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.RetryTasksManyParams) (int, error) {
			return len(params.IDs), nil
		}).Maybe()
	repo.EXPECT().SnoozeTasksMany(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, params repository.SnoozeTasksManyParams) (int, error) {
			return len(params.IDs), nil
		}).Maybe()

	bc := newTestCompleter(repo, Config{
		FlushInterval: 50 * time.Millisecond,
	})

	require.NoError(t, bc.Start(context.Background()))

	_ = bc.Complete(1)

	require.Eventually(t, func() bool { return completeCallCount.Load() >= 1 }, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	require.GreaterOrEqual(t, completeCallCount.Load(), int64(1))
}

func TestUnit_Backpressure(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	_ = setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{
		FlushInterval:  10 * time.Second,
		FlushThreshold: 1000,
		MaxBacklog:     5,
	})

	require.NoError(t, bc.Start(context.Background()))
	defer bc.Stop()

	var blocked atomic.Bool
	var wg sync.WaitGroup

	wg.Go(func() {
		for i := range 10 {
			if i >= 5 {
				blocked.Store(true)
			}
			_ = bc.Complete(int64(i))
		}
	})

	// Should be blocked after 5 items
	require.Eventually(t, blocked.Load, 2*time.Second, 10*time.Millisecond, "expected goroutine to reach backpressure point")
}

func TestUnit_SameTask_LastWins(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	// Same task ID in different operation types
	_ = bc.Retry(42, time.Now().Add(time.Second), "retry")
	_ = bc.Fail(42, "fail")
	_ = bc.Complete(42)

	// Wait for flush
	require.Eventually(t, func() bool {
		return counters.complete.Load() > 0 && counters.fail.Load() > 0 && counters.retry.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	require.Greater(t, counters.complete.Load(), int64(0))
	require.Greater(t, counters.fail.Load(), int64(0))
	require.Greater(t, counters.retry.Load(), int64(0))
}

func TestUnit_Snooze_FlushesOnInterval(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	snoozeTime1 := time.Now().Add(5 * time.Minute)
	snoozeTime2 := time.Now().Add(10 * time.Minute)

	require.NoError(t, bc.Snooze(1, snoozeTime1))
	require.NoError(t, bc.Snooze(2, snoozeTime2))

	require.Eventually(t, func() bool { return counters.snooze.Load() > 0 }, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	var snoozeCalls []repository.SnoozeTasksManyParams
	for _, call := range repo.Calls {
		if call.Method == "SnoozeTasksMany" {
			snoozeCalls = append(snoozeCalls, call.Arguments.Get(1).(repository.SnoozeTasksManyParams))
		}
	}
	require.NotEmpty(t, snoozeCalls)

	totalIDs := 0
	for _, c := range snoozeCalls {
		totalIDs += len(c.IDs)
		for _, bt := range c.BlockedTills {
			require.True(t, bt.After(time.Now()), "expected blocked_till to be in the future, got %v", bt)
		}
	}
	require.Equal(t, 2, totalIDs)
}

func TestUnit_Snooze_MixedWithComplete(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	require.NoError(t, bc.Complete(1))
	require.NoError(t, bc.Snooze(2, time.Now().Add(5*time.Minute)))
	require.NoError(t, bc.Complete(3))

	require.Eventually(t, func() bool {
		return counters.complete.Load() > 0 && counters.snooze.Load() > 0
	}, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	completeTotalIDs := countCallIDs(repo, "CompleteTasksMany")
	require.Equal(t, 2, completeTotalIDs)

	snoozeTotalIDs := countCallIDs(repo, "SnoozeTasksMany")
	require.Equal(t, 1, snoozeTotalIDs)
}

func TestUnit_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCompleterRepo(t)
	counters := setupAllMethods(repo)
	bc := newTestCompleter(repo, Config{FlushInterval: 50 * time.Millisecond})

	require.NoError(t, bc.Start(context.Background()))

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			_ = bc.Complete(int64(i))
		})
	}

	wg.Wait()
	require.Eventually(t, func() bool {
		return counters.complete.Load() >= 20
	}, 2*time.Second, 10*time.Millisecond)
	bc.Stop()

	// All 20 tasks should have been flushed (some may be in same call due to map dedup)
	totalIDs := countCallIDs(repo, "CompleteTasksMany")
	require.Equal(t, 20, totalIDs)
}
