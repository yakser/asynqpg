package periodic

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Task represents a function to be executed periodically.
type Task func(ctx context.Context) error

// Runner manages periodic task execution with graceful shutdown support.
//
// If a task execution takes longer than the configured interval, the next
// execution will start immediately after the current one completes, without
// waiting for the full interval. This ensures tasks don't accumulate in the
// queue when processing is slow.
//
// Example: If interval is 100ms and task execution takes 150ms:
//   - First execution starts at 0ms
//   - Second execution starts at 150ms (immediately after first completes)
//   - Third execution starts at 300ms (immediately after second completes)
//
// This behavior is useful for task fetchers in queue systems, where falling
// behind on fetching can lead to task accumulation.
type Runner struct {
	interval time.Duration
	task     Task
	logger   *slog.Logger

	mu       sync.Mutex
	running  bool
	stopChan chan struct{}
	doneChan chan struct{}
}

// Config configures periodic task runner.
type Config struct {
	// Interval between task executions.
	Interval time.Duration

	// Task to execute periodically.
	Task Task

	// Logger for logging task execution (optional).
	Logger *slog.Logger
}

// New creates a new periodic task runner.
func New(config Config) (*Runner, error) {
	if config.Interval <= 0 {
		return nil, fmt.Errorf("interval must be positive")
	}

	if config.Task == nil {
		return nil, fmt.Errorf("task cannot be nil")
	}

	runner := &Runner{
		interval: config.Interval,
		task:     config.Task,
		logger:   config.Logger,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
	}

	if runner.logger == nil {
		runner.logger = slog.Default()
	}

	return runner, nil
}

// Start begins periodic task execution in a separate goroutine.
// Returns an error if the runner is already running.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return fmt.Errorf("runner is already running")
	}
	r.running = true
	r.mu.Unlock()

	go r.run(ctx)
	return nil
}

// Stop gracefully stops the runner and waits for the current task to complete.
func (r *Runner) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	close(r.stopChan)
	<-r.doneChan

	r.mu.Lock()
	r.running = false
	r.mu.Unlock()
}

func (r *Runner) run(ctx context.Context) {
	defer close(r.doneChan)

	timer := time.NewTimer(r.interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Debug("periodic runner stopped due to context cancellation")
			return
		case <-r.stopChan:
			r.logger.Debug("periodic runner stopped")
			return
		case <-timer.C:
			executionStart := time.Now()
			r.executeTask(ctx)
			executionDuration := time.Since(executionStart)

			// If task execution took longer than interval, start next execution immediately
			if executionDuration >= r.interval {
				r.logger.Debug("task execution exceeded interval, starting next execution immediately",
					"execution_duration", executionDuration,
					"interval", r.interval,
				)
				timer.Reset(0)
			} else {
				// Wait for the remaining time to maintain consistent interval
				timer.Reset(r.interval - executionDuration)
			}
		}
	}
}

func (r *Runner) executeTask(ctx context.Context) {
	start := time.Now()

	if err := r.task(ctx); err != nil {
		r.logger.Error("periodic task execution failed",
			"error", err,
			"duration", time.Since(start),
		)
		return
	}

	r.logger.Debug("periodic task executed successfully",
		"duration", time.Since(start),
	)
}

// IsRunning returns true if the runner is currently active.
func (r *Runner) IsRunning() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}
