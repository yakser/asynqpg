package periodic

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("valid config", func(t *testing.T) {
		t.Parallel()

		task := func(ctx context.Context) error { return nil }
		runner, err := New(Config{
			Interval: time.Second,
			Task:     task,
		})

		require.NoError(t, err)
		assert.NotNil(t, runner)
		assert.Equal(t, time.Second, runner.interval)
	})

	t.Run("zero interval", func(t *testing.T) {
		t.Parallel()

		task := func(ctx context.Context) error { return nil }
		runner, err := New(Config{
			Interval: 0,
			Task:     task,
		})

		require.Error(t, err)
		assert.Nil(t, runner)
		assert.Contains(t, err.Error(), "interval must be positive")
	})

	t.Run("negative interval", func(t *testing.T) {
		t.Parallel()

		task := func(ctx context.Context) error { return nil }
		runner, err := New(Config{
			Interval: -time.Second,
			Task:     task,
		})

		require.Error(t, err)
		assert.Nil(t, runner)
	})

	t.Run("nil task", func(t *testing.T) {
		t.Parallel()

		runner, err := New(Config{
			Interval: time.Second,
			Task:     nil,
		})

		require.Error(t, err)
		assert.Nil(t, runner)
		assert.Contains(t, err.Error(), "task cannot be nil")
	})
}

func TestRunner_StartStop(t *testing.T) {
	t.Parallel()

	t.Run("start and stop", func(t *testing.T) {
		t.Parallel()

		var callCount atomic.Int64
		task := func(ctx context.Context) error {
			callCount.Add(1)
			return nil
		}

		runner, err := New(Config{
			Interval: 50 * time.Millisecond,
			Task:     task,
		})
		require.NoError(t, err)

		ctx := context.Background()
		err = runner.Start(ctx)
		require.NoError(t, err)
		assert.True(t, runner.IsRunning())

		time.Sleep(120 * time.Millisecond)

		runner.Stop()
		assert.False(t, runner.IsRunning())

		count := callCount.Load()
		assert.GreaterOrEqual(t, count, int64(2))
	})

	t.Run("start twice returns error", func(t *testing.T) {
		t.Parallel()

		task := func(ctx context.Context) error { return nil }
		runner, err := New(Config{
			Interval: time.Second,
			Task:     task,
		})
		require.NoError(t, err)

		ctx := context.Background()
		err = runner.Start(ctx)
		require.NoError(t, err)

		err = runner.Start(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already running")

		runner.Stop()
	})

	t.Run("stop without start is safe", func(t *testing.T) {
		t.Parallel()

		task := func(ctx context.Context) error { return nil }
		runner, err := New(Config{
			Interval: time.Second,
			Task:     task,
		})
		require.NoError(t, err)

		runner.Stop()
	})
}

func TestRunner_ContextCancellation(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64
	task := func(ctx context.Context) error {
		callCount.Add(1)
		return nil
	}

	runner, err := New(Config{
		Interval: 50 * time.Millisecond,
		Task:     task,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	err = runner.Start(ctx)
	require.NoError(t, err)

	time.Sleep(120 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)

	count := callCount.Load()
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestRunner_TaskError(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64
	task := func(ctx context.Context) error {
		callCount.Add(1)
		return errors.New("task failed")
	}

	runner, err := New(Config{
		Interval: 50 * time.Millisecond,
		Task:     task,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = runner.Start(ctx)
	require.NoError(t, err)

	time.Sleep(120 * time.Millisecond)

	runner.Stop()

	count := callCount.Load()
	assert.GreaterOrEqual(t, count, int64(2))
}

func TestRunner_LongRunningTask(t *testing.T) {
	t.Parallel()

	var callCount atomic.Int64
	var lastExecution atomic.Int64

	task := func(ctx context.Context) error {
		now := time.Now().UnixNano()
		callCount.Add(1)
		lastExecution.Store(now)

		// Simulate long-running task (longer than interval)
		time.Sleep(150 * time.Millisecond)
		return nil
	}

	runner, err := New(Config{
		Interval: 50 * time.Millisecond,
		Task:     task,
	})
	require.NoError(t, err)

	ctx := context.Background()
	err = runner.Start(ctx)
	require.NoError(t, err)

	// Let it run for enough time to observe the behavior
	time.Sleep(500 * time.Millisecond)

	runner.Stop()

	count := callCount.Load()
	// With 50ms interval and 150ms execution time, we expect:
	// - First execution: 0ms
	// - Second execution: ~150ms (immediately after first completes)
	// - Third execution: ~300ms (immediately after second completes)
	// - Possibly fourth: ~450ms
	// So we should have at least 3 executions in 500ms
	assert.GreaterOrEqual(t, count, int64(3), "should execute multiple times despite long execution")
}
