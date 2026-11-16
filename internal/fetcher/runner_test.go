package fetcher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunner_InitialFetch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var reasons []Reason
		r, err := New(Config{PollInterval: time.Second, Cooldown: 10 * time.Millisecond, Fetch: func(_ context.Context, reason Reason) (Result, error) {
			reasons = append(reasons, reason)
			return Result{Limit: 1}, nil
		}})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		synctest.Wait()
		assert.Equal(t, []Reason{ReasonStartup}, reasons)
	})
}
func TestRunner_CooldownCoalesces(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r, err := New(Config{PollInterval: time.Second, Cooldown: 10 * time.Millisecond, Fetch: func(context.Context, Reason) (Result, error) { calls.Add(1); return Result{Limit: 1}, nil }})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		synctest.Wait()
		for range 10000 {
			r.Wake(ReasonNotify)
		}
		time.Sleep(9 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(1), calls.Load())
		time.Sleep(time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(2), calls.Load())
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(2), calls.Load())
	})
}
func TestRunner_WakeDuringFetch(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int64
		r, err := New(Config{PollInterval: time.Second, Cooldown: time.Millisecond, Fetch: func(context.Context, Reason) (Result, error) {
			calls.Add(1)
			if calls.Load() == 1 {
				close(entered)
				<-release
			}
			return Result{Limit: 1}, nil
		}})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		<-entered
		r.Wake(ReasonNotify)
		close(release)
		time.Sleep(2 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(2), calls.Load())
	})
}
func TestRunner_BusyThenCapacity(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		capacity := make(chan struct{}, 1)
		var calls atomic.Int64
		r, err := New(Config{PollInterval: time.Second, Cooldown: 10 * time.Millisecond, CapacityAvailable: capacity, Fetch: func(context.Context, Reason) (Result, error) {
			calls.Add(1)
			switch calls.Load() {
			case 1:
				return Result{}, nil
			case 2:
				return Result{Fetched: 2, Limit: 2}, nil
			default:
				return Result{Limit: 2}, nil
			}
		}})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		synctest.Wait()
		for range 100 {
			r.Wake(ReasonNotify)
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(1), calls.Load())
		capacity <- struct{}{}
		synctest.Wait()
		assert.Equal(t, int64(2), calls.Load())
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(3), calls.Load())
		capacity <- struct{}{}
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(3), calls.Load())
	})
}
func TestRunner_PollFallback(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r, err := New(Config{PollInterval: 100 * time.Millisecond, Cooldown: time.Millisecond, Fetch: func(context.Context, Reason) (Result, error) { calls.Add(1); return Result{Limit: 1}, nil }})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		synctest.Wait()
		time.Sleep(110 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(2), calls.Load())
	})
}
func TestRunner_ErrorDelay(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		r, err := New(Config{PollInterval: 100 * time.Millisecond, Cooldown: time.Millisecond, Fetch: func(context.Context, Reason) (Result, error) { calls.Add(1); return Result{}, errors.New("offline") }})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		synctest.Wait()
		for range 90 {
			r.Wake(ReasonNotify)
			time.Sleep(time.Millisecond)
		}
		synctest.Wait()
		assert.Equal(t, int64(1), calls.Load())
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		assert.Equal(t, int64(2), calls.Load())
	})
}
func TestRunner_Stop(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		r, err := New(Config{PollInterval: time.Second, Cooldown: time.Millisecond, Fetch: func(ctx context.Context, _ Reason) (Result, error) { <-ctx.Done(); return Result{}, ctx.Err() }})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		synctest.Wait()
		r.Stop()
		r.Stop()
		r.Wake(ReasonNotify)
		require.Error(t, r.Start(t.Context()))
	})
}

func TestRunner_CapacityCheckDoesNotConsumeCooldown(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int64
		capacity := make(chan struct{}, 1)
		r, err := New(Config{PollInterval: time.Second, Cooldown: 10 * time.Millisecond, CapacityAvailable: capacity, Fetch: func(context.Context, Reason) (Result, error) {
			switch calls.Add(1) {
			case 1:
				return Result{Fetched: 1, Limit: 1}, nil
			case 2:
				return Result{}, nil
			default:
				return Result{Limit: 1}, nil
			}
		}})
		require.NoError(t, err)
		require.NoError(t, r.Start(t.Context()))
		defer r.Stop()
		synctest.Wait()
		time.Sleep(10 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int64(2), calls.Load())
		time.Sleep(time.Millisecond)
		capacity <- struct{}{}
		synctest.Wait()
		assert.Equal(t, int64(3), calls.Load(), "capacity is available and the last DB fetch cooldown has elapsed")
	})
}
