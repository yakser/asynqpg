package completer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/yakser/asynqpg/internal/repository"
)

// Completer defines the interface for task completion operations.
type Completer interface {
	Start(ctx context.Context) error
	Stop()
	Complete(taskID int64) error
	Fail(taskID int64, message string) error
	Retry(taskID int64, blockedTill time.Time, message string) error
	Snooze(taskID int64, blockedTill time.Time) error
}

// FailRequest represents a pending fail operation.
type FailRequest struct {
	Message string
}

// RetryRequest represents a pending retry operation.
type RetryRequest struct {
	BlockedTill time.Time
	Message     string
}

// SnoozeRequest represents a pending snooze operation.
type SnoozeRequest struct {
	BlockedTill time.Time
}

// Config configures the BatchCompleter.
type Config struct {
	FlushInterval  time.Duration
	FlushThreshold int
	MaxBatchSize   int
	MaxBacklog     int
	Logger         *slog.Logger
}

// DefaultConfig returns default configuration for BatchCompleter.
func DefaultConfig() Config {
	return Config{
		FlushInterval:  50 * time.Millisecond,
		FlushThreshold: 100,
		MaxBatchSize:   5000,
		MaxBacklog:     20000,
		Logger:         slog.Default(),
	}
}

type completerRepo interface {
	CompleteTasksMany(ctx context.Context, params repository.CompleteTasksManyParams) (int, error)
	FailTasksMany(ctx context.Context, params repository.FailTasksManyParams) (int, error)
	RetryTasksMany(ctx context.Context, params repository.RetryTasksManyParams) (int, error)
	SnoozeTasksMany(ctx context.Context, params repository.SnoozeTasksManyParams) (int, error)
}

// BatchCompleter accumulates task completions and executes them in batches.
type BatchCompleter struct {
	repo   completerRepo
	config Config
	logger *slog.Logger

	mu              sync.Mutex
	pendingComplete map[int64]struct{}
	pendingFail     map[int64]*FailRequest
	pendingRetry    map[int64]*RetryRequest
	pendingSnooze   map[int64]*SnoozeRequest

	backlogCond *sync.Cond
	backlogSize int

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	isRunning bool
}

// NewBatchCompleter creates a new BatchCompleter.
func NewBatchCompleter(repo completerRepo, cfg Config) *BatchCompleter {
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = DefaultConfig().FlushInterval
	}
	if cfg.FlushThreshold == 0 {
		cfg.FlushThreshold = DefaultConfig().FlushThreshold
	}
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = DefaultConfig().MaxBatchSize
	}
	if cfg.MaxBacklog == 0 {
		cfg.MaxBacklog = DefaultConfig().MaxBacklog
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	bc := &BatchCompleter{
		repo:            repo,
		config:          cfg,
		logger:          cfg.Logger,
		pendingComplete: make(map[int64]struct{}),
		pendingFail:     make(map[int64]*FailRequest),
		pendingRetry:    make(map[int64]*RetryRequest),
		pendingSnooze:   make(map[int64]*SnoozeRequest),
	}
	bc.backlogCond = sync.NewCond(&bc.mu)

	return bc
}

// Start begins the batch processing loop.
func (bc *BatchCompleter) Start(ctx context.Context) error {
	bc.mu.Lock()
	if bc.isRunning {
		bc.mu.Unlock()
		return errors.New("completer already running")
	}
	bc.ctx, bc.cancel = context.WithCancel(ctx)
	bc.isRunning = true
	bc.mu.Unlock()

	bc.wg.Add(1)
	go bc.runLoop()

	return nil
}

// Stop gracefully stops the BatchCompleter, flushing any pending operations.
func (bc *BatchCompleter) Stop() {
	bc.mu.Lock()
	if !bc.isRunning {
		bc.mu.Unlock()
		return
	}
	bc.mu.Unlock()

	bc.cancel()
	bc.wg.Wait()

	bc.mu.Lock()
	bc.isRunning = false
	bc.mu.Unlock()
}

// Complete marks a task as completed.
func (bc *BatchCompleter) Complete(taskID int64) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.waitForBacklog(); err != nil {
		return err
	}

	bc.pendingComplete[taskID] = struct{}{}
	bc.backlogSize++

	if bc.shouldFlush() {
		bc.triggerFlush()
	}

	return nil
}

// Fail marks a task as failed with an error message.
func (bc *BatchCompleter) Fail(taskID int64, message string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.waitForBacklog(); err != nil {
		return err
	}

	bc.pendingFail[taskID] = &FailRequest{Message: message}
	bc.backlogSize++

	if bc.shouldFlush() {
		bc.triggerFlush()
	}

	return nil
}

// Retry schedules a task for retry.
func (bc *BatchCompleter) Retry(taskID int64, blockedTill time.Time, message string) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.waitForBacklog(); err != nil {
		return err
	}

	bc.pendingRetry[taskID] = &RetryRequest{
		BlockedTill: blockedTill,
		Message:     message,
	}
	bc.backlogSize++

	if bc.shouldFlush() {
		bc.triggerFlush()
	}

	return nil
}

// Snooze reschedules a task without counting it as an attempt.
func (bc *BatchCompleter) Snooze(taskID int64, blockedTill time.Time) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if err := bc.waitForBacklog(); err != nil {
		return err
	}

	bc.pendingSnooze[taskID] = &SnoozeRequest{
		BlockedTill: blockedTill,
	}
	bc.backlogSize++

	if bc.shouldFlush() {
		bc.triggerFlush()
	}

	return nil
}

func (bc *BatchCompleter) waitForBacklog() error {
	for bc.backlogSize >= bc.config.MaxBacklog {
		if bc.ctx.Err() != nil {
			return bc.ctx.Err()
		}
		bc.backlogCond.Wait()
	}
	return nil
}

func (bc *BatchCompleter) shouldFlush() bool {
	return bc.backlogSize >= bc.config.FlushThreshold
}

func (bc *BatchCompleter) triggerFlush() {
	// Signal the loop to flush immediately
	bc.backlogCond.Broadcast()
}

func (bc *BatchCompleter) runLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(bc.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.ctx.Done():
			bc.finalFlush()
			return
		case <-ticker.C:
			bc.flush()
		}
	}
}

func (bc *BatchCompleter) flush() {
	bc.mu.Lock()

	if len(bc.pendingComplete) == 0 && len(bc.pendingFail) == 0 && len(bc.pendingRetry) == 0 && len(bc.pendingSnooze) == 0 {
		bc.mu.Unlock()
		return
	}

	// Swap maps
	completeIDs := bc.pendingComplete
	failRequests := bc.pendingFail
	retryRequests := bc.pendingRetry
	snoozeRequests := bc.pendingSnooze

	bc.pendingComplete = make(map[int64]struct{})
	bc.pendingFail = make(map[int64]*FailRequest)
	bc.pendingRetry = make(map[int64]*RetryRequest)
	bc.pendingSnooze = make(map[int64]*SnoozeRequest)
	bc.backlogSize = 0
	bc.backlogCond.Broadcast()

	bc.mu.Unlock()

	bc.executeBatch(bc.ctx, completeIDs, failRequests, retryRequests, snoozeRequests)
}

func (bc *BatchCompleter) finalFlush() {
	bc.mu.Lock()

	if len(bc.pendingComplete) == 0 && len(bc.pendingFail) == 0 && len(bc.pendingRetry) == 0 && len(bc.pendingSnooze) == 0 {
		bc.mu.Unlock()
		return
	}

	completeIDs := bc.pendingComplete
	failRequests := bc.pendingFail
	retryRequests := bc.pendingRetry
	snoozeRequests := bc.pendingSnooze

	bc.pendingComplete = make(map[int64]struct{})
	bc.pendingFail = make(map[int64]*FailRequest)
	bc.pendingRetry = make(map[int64]*RetryRequest)
	bc.pendingSnooze = make(map[int64]*SnoozeRequest)
	bc.backlogSize = 0

	bc.mu.Unlock()

	// Use context.WithoutCancel for final flush
	ctx := context.WithoutCancel(bc.ctx)
	bc.executeBatch(ctx, completeIDs, failRequests, retryRequests, snoozeRequests)
}

func (bc *BatchCompleter) executeBatch(
	ctx context.Context,
	completeIDs map[int64]struct{},
	failRequests map[int64]*FailRequest,
	retryRequests map[int64]*RetryRequest,
	snoozeRequests map[int64]*SnoozeRequest,
) {
	// Execute complete batch
	if len(completeIDs) > 0 {
		ids := make([]int64, 0, len(completeIDs))
		for id := range completeIDs {
			ids = append(ids, id)
		}

		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: ids})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to complete tasks batch", "count", len(ids), "error", err)
		} else {
			bc.logger.Debug("completed tasks batch", "count", len(ids))
		}
	}

	// Execute fail batch
	if len(failRequests) > 0 {
		ids := make([]int64, 0, len(failRequests))
		messages := make([]string, 0, len(failRequests))
		for id, req := range failRequests {
			ids = append(ids, id)
			messages = append(messages, req.Message)
		}

		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.FailTasksMany(ctx, repository.FailTasksManyParams{
				IDs:      ids,
				Messages: messages,
			})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to fail tasks batch", "count", len(ids), "error", err)
		} else {
			bc.logger.Debug("failed tasks batch", "count", len(ids))
		}
	}

	// Execute retry batch
	if len(retryRequests) > 0 {
		ids := make([]int64, 0, len(retryRequests))
		blockedTills := make([]time.Time, 0, len(retryRequests))
		messages := make([]string, 0, len(retryRequests))
		for id, req := range retryRequests {
			ids = append(ids, id)
			blockedTills = append(blockedTills, req.BlockedTill)
			messages = append(messages, req.Message)
		}

		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{
				IDs:          ids,
				BlockedTills: blockedTills,
				Messages:     messages,
			})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to retry tasks batch", "count", len(ids), "error", err)
		} else {
			bc.logger.Debug("retried tasks batch", "count", len(ids))
		}
	}

	// Execute snooze batch
	if len(snoozeRequests) > 0 {
		ids := make([]int64, 0, len(snoozeRequests))
		blockedTills := make([]time.Time, 0, len(snoozeRequests))
		for id, req := range snoozeRequests {
			ids = append(ids, id)
			blockedTills = append(blockedTills, req.BlockedTill)
		}

		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.SnoozeTasksMany(ctx, repository.SnoozeTasksManyParams{
				IDs:          ids,
				BlockedTills: blockedTills,
			})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to snooze tasks batch", "count", len(ids), "error", err)
		} else {
			bc.logger.Debug("snoozed tasks batch", "count", len(ids))
		}
	}
}

func (bc *BatchCompleter) withRetries(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, context.Canceled) {
			return lastErr
		}

		sleep := time.Duration(1<<attempt) * time.Second // 2s, 4s, 8s
		bc.logger.Warn("batch operation failed, retrying",
			"attempt", attempt,
			"sleep", sleep,
			"error", lastErr,
		)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
	}
	return lastErr
}
