package workerpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPoolExecutesAllTasks(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(4)
	t.Cleanup(pool.Close)

	const n = 100
	var done atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)

	for range n {
		err := pool.Submit(func() {
			done.Add(1)
			wg.Done()
		})
		require.NoError(t, err)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for tasks")
	}

	require.Equal(t, int64(n), done.Load())
}

func TestWorkerPoolResizeIncrease(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(1)
	t.Cleanup(pool.Close)

	const tasksCount = 50
	var wg sync.WaitGroup
	wg.Add(tasksCount)

	var (
		activeWorkers          atomic.Int64
		maxObservedParallelism atomic.Int64
	)

	work := func() {
		concurrencyAtStart := activeWorkers.Add(1)

		time.Sleep(3 * time.Millisecond)

		for {
			currentMax := maxObservedParallelism.Load()
			if concurrencyAtStart <= currentMax {
				break
			}
			if maxObservedParallelism.CompareAndSwap(currentMax, concurrencyAtStart) {
				break
			}
		}

		activeWorkers.Add(-1)
		wg.Done()
	}

	for i := range tasksCount {
		err := pool.Submit(work)
		require.NoError(t, err)
		if i == 5 {
			pool.Resize(8)
		}
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for tasks after resize")
	}

	require.GreaterOrEqual(t, maxObservedParallelism.Load(), int64(2), "expected parallelism to increase after resize")
}

func TestWorkerPoolResizeDecreaseNoDeadlock(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(8)

	const tasksCount = 50
	var wg sync.WaitGroup
	wg.Add(tasksCount)

	for i := range tasksCount {
		err := pool.Submit(func() {
			time.Sleep(5 * time.Millisecond)
			wg.Done()
		})
		require.NoError(t, err)
		if i == 10 {
			pool.Resize(2)
		}
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		pool.Close()
		close(doneCh)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout: resize down or close caused deadlock")
	}
}

func TestWorkerPoolSubmitAfterClose(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(2)

	err := pool.Submit(func() {})
	require.NoError(t, err)

	pool.Close()

	err = pool.Submit(func() {})
	require.Error(t, err)
}

func TestWorkerPoolCloseIdempotent(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(2)

	err := pool.Submit(func() {})
	require.NoError(t, err)

	pool.Close()

	done := make(chan struct{})
	go func() {
		pool.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("second Close call blocked")
	}
}

func TestWorkerPoolFreeWorkers(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(3)
	t.Cleanup(pool.Close)

	started := make(chan struct{})
	release := make(chan struct{})

	for range 3 {
		go func() {
			err := pool.Submit(func() {
				started <- struct{}{}
				<-release
			})
			if err != nil {
				t.Errorf("unexpected error on Submit: %v", err)
			}
		}()
	}

	for range 3 {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout waiting for workers to start")
		}
	}

	require.Eventually(t, func() bool {
		return pool.FreeWorkers() == 0
	}, 500*time.Millisecond, 10*time.Millisecond)

	close(release)

	require.Eventually(t, func() bool {
		return pool.FreeWorkers() == 3
	}, 2*time.Second, 10*time.Millisecond)
}

func TestWorkerPool_ResizeToZero(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(3)
	t.Cleanup(pool.Close)

	pool.Resize(0)

	require.Eventually(t, func() bool {
		return pool.FreeWorkers() == 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestWorkerPool_ResizeNegative(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(3)
	t.Cleanup(pool.Close)

	pool.Resize(-1)

	require.Eventually(t, func() bool {
		return pool.FreeWorkers() == 0
	}, 500*time.Millisecond, 10*time.Millisecond)
}

func TestWorkerPool_SubmitNilTask(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(2)
	t.Cleanup(pool.Close)

	err := pool.Submit(nil)
	require.NoError(t, err)

	// Verify the pool is still functional after submitting nil.
	done := make(chan struct{})
	err = pool.Submit(func() {
		close(done)
	})
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout: pool not functional after nil task")
	}
}
