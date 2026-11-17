package workerpool

import (
	"errors"
	"sync"
	"sync/atomic"
)

type Task func()

type WorkerPool struct {
	mu        sync.Mutex
	tasks     chan Task
	workers   int
	stopChans []chan struct{}
	wg        sync.WaitGroup
	closed    bool
	closedCh  chan struct{}
	capacity  chan struct{}

	busy atomic.Int64
}

func NewWorkerPool(workerCount int) *WorkerPool {
	p := &WorkerPool{
		tasks:    make(chan Task),
		closedCh: make(chan struct{}),
		capacity: make(chan struct{}, 1),
		workers:  0,
	}
	p.Resize(workerCount)
	return p
}

func (p *WorkerPool) startWorker(stopCh <-chan struct{}) {
	p.wg.Go(func() {
		for {
			select {
			case <-stopCh:
				return
			case task, ok := <-p.tasks:
				if !ok {
					return
				}
				if task != nil {
					task()
					p.busy.Add(-1)
					p.signalCapacity()
				}
			}
		}
	})
}

func (p *WorkerPool) Resize(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}
	if n < 0 {
		n = 0
	}

	if n > p.workers {
		diff := n - p.workers
		for range diff {
			stopCh := make(chan struct{})
			p.stopChans = append(p.stopChans, stopCh)
			p.startWorker(stopCh)
		}
		p.workers = n
		p.signalCapacity()
		return
	}

	if n < p.workers {
		diff := p.workers - n
		for range diff {
			lastIdx := len(p.stopChans) - 1
			close(p.stopChans[lastIdx])
			p.stopChans = p.stopChans[:lastIdx]
		}
		p.workers = n
	}
}

func (p *WorkerPool) Submit(task Task) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return errors.New("worker pool is closed")
	}
	if task != nil {
		p.busy.Add(1)
	}
	p.mu.Unlock()

	select {
	case p.tasks <- task:
		return nil
	case <-p.closedCh:
		if task != nil {
			p.busy.Add(-1)
			p.signalCapacity()
		}
		return errors.New("worker pool is closed")
	}
}

// CapacityAvailable coalesces capacity releases and increases into a nonblocking hint.
// Receivers must recheck FreeWorkers; the channel is never closed.
func (p *WorkerPool) CapacityAvailable() <-chan struct{} { return p.capacity }

func (p *WorkerPool) signalCapacity() {
	select {
	case p.capacity <- struct{}{}:
	default:
	}
}

func (p *WorkerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.wg.Wait()
		return
	}
	p.closed = true
	close(p.closedCh)

	for _, ch := range p.stopChans {
		close(ch)
	}
	p.stopChans = nil
	p.mu.Unlock()

	p.wg.Wait()
}

// Workers returns the current size of the worker pool.
func (p *WorkerPool) Workers() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers
}

func (p *WorkerPool) FreeWorkers() int {
	p.mu.Lock()
	workers := p.workers
	p.mu.Unlock()

	busy := int(p.busy.Load())
	if busy <= 0 {
		return workers
	}
	if busy >= workers {
		return 0
	}
	return workers - busy
}
