package maintenance

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/yakser/asynqpg/internal/lib/periodic"
	"github.com/yakser/asynqpg/internal/repository"
)

const (
	defaultCompletedRetention = 24 * time.Hour
	defaultFailedRetention    = 7 * 24 * time.Hour // 7 days
	defaultCancelledRetention = 24 * time.Hour
	defaultCleanerInterval    = 30 * time.Second
	defaultCleanerBatchSize   = 1000
)

// CleanerConfig configures the task cleaner service.
type CleanerConfig struct {
	// CompletedRetention is how long to keep completed tasks.
	// Default: 24 hours.
	CompletedRetention time.Duration

	// FailedRetention is how long to keep failed (discarded) tasks.
	// Default: 7 days.
	FailedRetention time.Duration

	// CancelledRetention is how long to keep cancelled tasks.
	// Default: 24 hours.
	CancelledRetention time.Duration

	// Interval is the frequency of cleanup runs.
	// Default: 30 seconds.
	Interval time.Duration

	// BatchSize is the maximum number of tasks to delete in one iteration.
	// Default: 1000.
	BatchSize int

	// Logger for the cleaner.
	Logger *slog.Logger
}

func (c *CleanerConfig) setDefaults() {
	if c.CompletedRetention <= 0 {
		c.CompletedRetention = defaultCompletedRetention
	}
	if c.FailedRetention <= 0 {
		c.FailedRetention = defaultFailedRetention
	}
	if c.CancelledRetention <= 0 {
		c.CancelledRetention = defaultCancelledRetention
	}
	if c.Interval <= 0 {
		c.Interval = defaultCleanerInterval
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultCleanerBatchSize
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type cleanerRepo interface {
	DeleteOldTasks(ctx context.Context, params repository.DeleteOldTasksParams) (int, error)
}

// Cleaner periodically deletes finalized tasks after their retention period.
// This helps keep the asynqpg_tasks table small and performant.
type Cleaner struct {
	config *CleanerConfig
	repo   cleanerRepo

	mu      sync.Mutex
	started bool
	runner  *periodic.Runner
}

// NewCleaner creates a new Cleaner service.
func NewCleaner(repo cleanerRepo, config CleanerConfig) *Cleaner {
	config.setDefaults()
	return &Cleaner{
		config: &config,
		repo:   repo,
	}
}

func (c *Cleaner) Name() string {
	return "cleaner"
}

func (c *Cleaner) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return nil
	}

	runner, err := periodic.New(periodic.Config{
		Interval: c.config.Interval,
		Task:     c.runOnce,
		Logger:   c.config.Logger.With("component", "cleaner"),
	})
	if err != nil {
		return fmt.Errorf("create periodic runner: %w", err)
	}

	if err := runner.Start(ctx); err != nil {
		return fmt.Errorf("start cleaner: %w", err)
	}

	c.runner = runner
	c.started = true
	c.config.Logger.Info("cleaner started",
		"completed_retention", c.config.CompletedRetention,
		"failed_retention", c.config.FailedRetention,
		"cancelled_retention", c.config.CancelledRetention,
		"interval", c.config.Interval,
		"batch_size", c.config.BatchSize,
	)
	return nil
}

func (c *Cleaner) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.started {
		return
	}

	if c.runner != nil {
		c.runner.Stop()
	}
	c.started = false
	c.config.Logger.Info("cleaner stopped")
}

func (c *Cleaner) runOnce(ctx context.Context) error {
	now := time.Now()

	params := repository.DeleteOldTasksParams{
		CompletedBefore: now.Add(-c.config.CompletedRetention),
		FailedBefore:    now.Add(-c.config.FailedRetention),
		CancelledBefore: now.Add(-c.config.CancelledRetention),
		Limit:           c.config.BatchSize,
	}

	var totalDeleted int

	for {
		deleted, err := c.repo.DeleteOldTasks(ctx, params)
		if err != nil {
			return fmt.Errorf("delete old tasks: %w", err)
		}

		totalDeleted += deleted

		// If we deleted fewer than batch size, we're done
		if deleted < c.config.BatchSize {
			break
		}
	}

	if totalDeleted > 0 {
		c.config.Logger.Info("cleaned up old tasks",
			"total_deleted", totalDeleted,
		)
	}

	return nil
}
