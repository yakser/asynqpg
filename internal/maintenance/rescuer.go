package maintenance

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/periodic"
	"github.com/yakser/asynqpg/internal/repository"
)

const (
	defaultRescueAfter     = 1 * time.Hour
	defaultRescueInterval  = 30 * time.Second
	defaultRescueBatchSize = 1000
)

// RescuerConfig configures the task rescuer service.
type RescuerConfig struct {
	// RescueAfter is the duration after which a running task is considered stuck.
	// Default: 1 hour.
	RescueAfter time.Duration

	// Interval is the frequency of rescue checks.
	// Default: 30 seconds.
	Interval time.Duration

	// BatchSize is the maximum number of tasks to rescue in one iteration.
	// Default: 1000.
	BatchSize int

	// RetryPolicy determines retry delays for rescued tasks.
	// If nil, DefaultRetryPolicy is used.
	RetryPolicy asynqpg.RetryPolicy

	// Logger for the rescuer.
	Logger *slog.Logger
}

func (c *RescuerConfig) setDefaults() {
	if c.RescueAfter <= 0 {
		c.RescueAfter = defaultRescueAfter
	}
	if c.Interval <= 0 {
		c.Interval = defaultRescueInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultRescueBatchSize
	}
	if c.RetryPolicy == nil {
		c.RetryPolicy = &asynqpg.DefaultRetryPolicy{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type rescuerRepo interface {
	GetStuckTasks(ctx context.Context, params repository.GetStuckTasksParams) ([]repository.StuckTask, error)
	RetryTask(ctx context.Context, params repository.RetryTaskParams) error
	FailTasks(ctx context.Context, ids []int64, message string) error
}

// Rescuer periodically rescues stuck tasks that have been running too long.
// A task is considered stuck if:
// - Status is 'running'
// - attempted_at + RescueAfter < now()
//
// For stuck tasks:
// - If attempts_left > 0: retry with exponential backoff
// - If attempts_left == 0: discard (mark as failed)
type Rescuer struct {
	config *RescuerConfig
	repo   rescuerRepo

	mu      sync.Mutex
	started bool
	runner  *periodic.Runner
}

// NewRescuer creates a new Rescuer service.
func NewRescuer(repo rescuerRepo, config RescuerConfig) *Rescuer {
	config.setDefaults()
	return &Rescuer{
		config: &config,
		repo:   repo,
	}
}

func (r *Rescuer) Name() string {
	return "rescuer"
}

func (r *Rescuer) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return nil
	}

	runner, err := periodic.New(periodic.Config{
		Interval: r.config.Interval,
		Task:     r.runOnce,
		Logger:   r.config.Logger.With("component", "rescuer"),
	})
	if err != nil {
		return fmt.Errorf("create periodic runner: %w", err)
	}

	if err := runner.Start(ctx); err != nil {
		return fmt.Errorf("start rescuer: %w", err)
	}

	r.runner = runner
	r.started = true
	r.config.Logger.Info("rescuer started",
		"rescue_after", r.config.RescueAfter,
		"interval", r.config.Interval,
		"batch_size", r.config.BatchSize,
	)
	return nil
}

func (r *Rescuer) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		return
	}

	if r.runner != nil {
		r.runner.Stop()
	}
	r.started = false
	r.config.Logger.Info("rescuer stopped")
}

func (r *Rescuer) runOnce(ctx context.Context) error {
	stuckHorizon := time.Now().Add(-r.config.RescueAfter)

	var totalRescued, totalRetried, totalDiscarded int

	for {
		tasks, err := r.repo.GetStuckTasks(ctx, repository.GetStuckTasksParams{
			StuckHorizon: stuckHorizon,
			Limit:        r.config.BatchSize,
		})
		if err != nil {
			return fmt.Errorf("get stuck tasks: %w", err)
		}

		if len(tasks) == 0 {
			break
		}

		retried, discarded, err := r.rescueBatch(ctx, tasks)
		if err != nil {
			return fmt.Errorf("rescue batch: %w", err)
		}

		totalRescued += len(tasks)
		totalRetried += retried
		totalDiscarded += discarded

		// If we got fewer tasks than batch size, we're done
		if len(tasks) < r.config.BatchSize {
			break
		}
	}

	if totalRescued > 0 {
		r.config.Logger.Info("rescued stuck tasks",
			"total_rescued", totalRescued,
			"retried", totalRetried,
			"discarded", totalDiscarded,
		)
	}

	return nil
}

func (r *Rescuer) rescueBatch(ctx context.Context, tasks []repository.StuckTask) (retried, discarded int, err error) {
	const errorMsg = "Stuck task rescued by Rescuer"

	for _, task := range tasks {
		if task.AttemptsLeft > 0 {
			// Retry the task
			attempt := task.AttemptsElapsed + 1
			nextRetryDelay := r.config.RetryPolicy.NextRetry(attempt)
			nextRetryAt := time.Now().Add(nextRetryDelay)

			if err := r.repo.RetryTask(ctx, repository.RetryTaskParams{
				ID:          task.ID,
				BlockedTill: nextRetryAt,
				Message:     errorMsg,
			}); err != nil {
				return retried, discarded, fmt.Errorf("retry task %d: %w", task.ID, err)
			}

			r.config.Logger.Debug("retried stuck task",
				"task_id", task.ID,
				"task_type", task.Type,
				"attempt", attempt,
				"next_retry_at", nextRetryAt,
			)
			retried++
		} else {
			// Discard the task
			if err := r.repo.FailTasks(ctx, []int64{task.ID}, errorMsg); err != nil {
				return retried, discarded, fmt.Errorf("discard task %d: %w", task.ID, err)
			}

			r.config.Logger.Debug("discarded stuck task",
				"task_id", task.ID,
				"task_type", task.Type,
			)
			discarded++
		}
	}

	return retried, discarded, nil
}
