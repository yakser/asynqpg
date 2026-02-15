package workerpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func TestWorkerPoolExecutesAllTasks(t *testing.T) {
	pool := NewWorkerPool(4)
	t.Cleanup(pool.Close)

	const n = 100
	var done int64
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		if err := pool.Submit(func() {
			atomic.AddInt64(&done, 1)
			wg.Done()
		}); err != nil {
			t.Fatalf("unexpected error on Submit: %v", err)
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
		t.Fatal("timeout waiting for tasks")
	}

	if got := atomic.LoadInt64(&done); got != n {
		t.Fatalf("expected %d tasks done, got %d", n, got)
	}
}

func TestWorkerPoolResizeIncrease(t *testing.T) {
	pool := NewWorkerPool(1)
	t.Cleanup(pool.Close)

	const tasksCount = 50
	var wg sync.WaitGroup
	wg.Add(tasksCount)

	var (
		activeWorkers          int64
		maxObservedParallelism int64
	)

	work := func() {
		concurrencyAtStart := atomic.AddInt64(&activeWorkers, 1)

		time.Sleep(3 * time.Millisecond)

		for {
			currentMax := atomic.LoadInt64(&maxObservedParallelism)
			if concurrencyAtStart <= currentMax {
				break
			}
			if atomic.CompareAndSwapInt64(&maxObservedParallelism, currentMax, concurrencyAtStart) {
				break
			}
		}

		atomic.AddInt64(&activeWorkers, -1)
		wg.Done()
	}

	for i := 0; i < tasksCount; i++ {
		if err := pool.Submit(work); err != nil {
			t.Fatalf("unexpected error on Submit: %v", err)
		}
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

	if maxObservedParallelism < 2 {
		t.Fatalf("expected parallelism to increase after resize, got %d", maxObservedParallelism)
	}
}

func TestWorkerPoolResizeDecreaseNoDeadlock(t *testing.T) {
	pool := NewWorkerPool(8)

	const tasksCount = 50
	var wg sync.WaitGroup
	wg.Add(tasksCount)

	for i := 0; i < tasksCount; i++ {
		if err := pool.Submit(func() {
			time.Sleep(5 * time.Millisecond)
			wg.Done()
		}); err != nil {
			t.Fatalf("unexpected error on Submit: %v", err)
		}
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
	pool := NewWorkerPool(2)

	if err := pool.Submit(func() {}); err != nil {
		t.Fatalf("unexpected error on Submit before Close: %v", err)
	}

	pool.Close()

	if err := pool.Submit(func() {}); err == nil {
		t.Fatal("expected error on Submit after Close, got nil")
	}
}

func TestWorkerPoolCloseIdempotent(t *testing.T) {
	pool := NewWorkerPool(2)

	if err := pool.Submit(func() {}); err != nil {
		t.Fatalf("unexpected error on Submit: %v", err)
	}

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
	pool := NewWorkerPool(3)
	t.Cleanup(pool.Close)

	started := make(chan struct{})
	release := make(chan struct{})

	for i := 0; i < 3; i++ {
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

	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("timeout waiting for workers to start")
		}
	}

	waitUntil(t, func() bool {
		return pool.FreeWorkers() == 0
	}, 500*time.Millisecond)

	close(release)

	waitUntil(t, func() bool {
		return pool.FreeWorkers() == 3
	}, 2*time.Second)
}

func TestWorkerPool_ResizeToZero(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(3)
	t.Cleanup(pool.Close)

	pool.Resize(0)

	waitUntil(t, func() bool {
		return pool.FreeWorkers() == 0
	}, 500*time.Millisecond)
}

func TestWorkerPool_ResizeNegative(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(3)
	t.Cleanup(pool.Close)

	pool.Resize(-1)

	waitUntil(t, func() bool {
		return pool.FreeWorkers() == 0
	}, 500*time.Millisecond)
}

func TestWorkerPool_SubmitNilTask(t *testing.T) {
	t.Parallel()

	pool := NewWorkerPool(2)
	t.Cleanup(pool.Close)

	err := pool.Submit(nil)
	if err != nil {
		t.Fatalf("unexpected error on Submit(nil): %v", err)
	}

	// Verify the pool is still functional after submitting nil.
	done := make(chan struct{})
	err = pool.Submit(func() {
		close(done)
	})
	if err != nil {
		t.Fatalf("unexpected error on Submit after nil: %v", err)
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout: pool not functional after nil task")
	}
}
