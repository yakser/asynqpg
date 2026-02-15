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

	busy int64
}

func NewWorkerPool(workerCount int) *WorkerPool {
	p := &WorkerPool{
		tasks:   make(chan Task),
		workers: 0,
	}
	p.Resize(workerCount)
	return p
}

func (p *WorkerPool) startWorker(stopCh <-chan struct{}) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case task, ok := <-p.tasks:
				if !ok {
					return
				}
				if task != nil {
					atomic.AddInt64(&p.busy, 1)
					task()
					atomic.AddInt64(&p.busy, -1)
				}
			}
		}
	}()
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
		for i := 0; i < diff; i++ {
			stopCh := make(chan struct{})
			p.stopChans = append(p.stopChans, stopCh)
			p.startWorker(stopCh)
		}
		p.workers = n
		return
	}

	if n < p.workers {
		diff := p.workers - n
		for i := 0; i < diff; i++ {
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
	p.mu.Unlock()

	p.tasks <- task
	return nil
}

func (p *WorkerPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true

	for _, ch := range p.stopChans {
		close(ch)
	}
	p.stopChans = nil
	p.mu.Unlock()

	close(p.tasks)
	p.wg.Wait()
}

func (p *WorkerPool) FreeWorkers() int {
	p.mu.Lock()
	workers := p.workers
	p.mu.Unlock()

	busy := int(atomic.LoadInt64(&p.busy))
	if busy <= 0 {
		return workers
	}
	if busy >= workers {
		return 0
	}
	return workers - busy
}
