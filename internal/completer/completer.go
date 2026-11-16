package completer

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/yakser/asynqpg/internal/repository"
)

// Config configures the BatchCompleter.
type Config struct {
	FlushInterval  time.Duration
	FlushThreshold int
	MaxBatchSize   int
	MaxBacklog     int
	Logger         *slog.Logger
}

//go:generate go tool mockery --case underscore --with-expecter --exported --name completerRepo
type completerRepo interface {
	CompleteTasksMany(ctx context.Context, params repository.CompleteTasksManyParams) (int, error)
	FailTasksMany(ctx context.Context, params repository.FailTasksManyParams) (int, error)
	RetryTasksMany(ctx context.Context, params repository.RetryTasksManyParams) (int, error)
	SnoozeTasksMany(ctx context.Context, params repository.SnoozeTasksManyParams) (int, error)
}

type opKind uint8

const (
	opComplete opKind = iota + 1
	opFail
	opRetry
	opSnooze
)

type claimKey struct{ id, generation int64 }

type pendingOp struct {
	kind        opKind
	blockedTill time.Time
	message     string
}

// BatchCompleter accumulates task state transitions and flushes them to
// PostgreSQL in batches. The same task claim submitted multiple times across
// different operations collapses to the last write.
type BatchCompleter struct {
	repo   completerRepo
	config Config
	logger *slog.Logger

	mu      sync.Mutex
	pending map[claimKey]pendingOp
	// waitCh != nil iff backlog has hit MaxBacklog; closed when capacity frees.
	waitCh chan struct{}

	// Buffered(1) signal: non-blocking send from producers wakes runLoop
	// for an immediate flush when FlushThreshold is reached.
	triggerCh chan struct{}

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
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

// NewBatchCompleter creates a new BatchCompleter.
func NewBatchCompleter(repo completerRepo, cfg Config) *BatchCompleter {
	defaultCfg := DefaultConfig()
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = defaultCfg.FlushInterval
	}
	if cfg.FlushThreshold == 0 {
		cfg.FlushThreshold = defaultCfg.FlushThreshold
	}
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = defaultCfg.MaxBatchSize
	}
	if cfg.MaxBacklog == 0 {
		cfg.MaxBacklog = defaultCfg.MaxBacklog
	}
	if cfg.Logger == nil {
		cfg.Logger = defaultCfg.Logger
	}

	return &BatchCompleter{
		repo:      repo,
		config:    cfg,
		logger:    cfg.Logger,
		pending:   make(map[claimKey]pendingOp),
		triggerCh: make(chan struct{}, 1),
	}
}

// Start begins the batch processing loop. Calling Start twice returns an error.
func (bc *BatchCompleter) Start(ctx context.Context) error {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if bc.started {
		return errors.New("completer already running")
	}
	bc.ctx, bc.cancel = context.WithCancel(ctx)
	bc.started = true
	bc.wg.Go(bc.runLoop)
	return nil
}

// Stop signals the run loop to exit, waits for the final flush, and returns.
// Safe to call multiple times and safe to call without Start.
func (bc *BatchCompleter) Stop() {
	bc.mu.Lock()
	if !bc.started || bc.cancel == nil {
		bc.mu.Unlock()
		return
	}
	cancel := bc.cancel
	bc.cancel = nil
	bc.mu.Unlock()

	cancel()
	bc.wg.Wait()
}

// Complete marks a task as completed.
func (bc *BatchCompleter) Complete(taskID, generation int64) error {
	return bc.enqueue(claimKey{taskID, generation}, pendingOp{kind: opComplete})
}

// Fail marks a task as failed with an error message.
func (bc *BatchCompleter) Fail(taskID, generation int64, message string) error {
	return bc.enqueue(claimKey{taskID, generation}, pendingOp{kind: opFail, message: message})
}

// Retry schedules a task for retry.
func (bc *BatchCompleter) Retry(taskID, generation int64, blockedTill time.Time, message string) error {
	return bc.enqueue(claimKey{taskID, generation}, pendingOp{kind: opRetry, blockedTill: blockedTill, message: message})
}

// Snooze reschedules a task without counting it as an attempt.
func (bc *BatchCompleter) Snooze(taskID, generation int64, blockedTill time.Time) error {
	return bc.enqueue(claimKey{taskID, generation}, pendingOp{kind: opSnooze, blockedTill: blockedTill})
}

func (bc *BatchCompleter) enqueue(taskID claimKey, op pendingOp) error {
	if err := bc.waitForCapacity(); err != nil {
		return err
	}

	bc.mu.Lock()
	bc.pending[taskID] = op
	overThreshold := len(bc.pending) >= bc.config.FlushThreshold
	bc.mu.Unlock()

	if overThreshold {
		select {
		case bc.triggerCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// waitForCapacity blocks until len(pending) < MaxBacklog. Uses a shared channel
// closed by the flusher.
func (bc *BatchCompleter) waitForCapacity() error {
	bc.mu.Lock()
	if len(bc.pending) < bc.config.MaxBacklog {
		bc.mu.Unlock()
		return nil
	}
	if bc.waitCh == nil {
		bc.waitCh = make(chan struct{})
	}
	waitCh := bc.waitCh
	ctx := bc.ctx
	bc.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitCh:
		return nil
	}
}

func (bc *BatchCompleter) runLoop() {
	ticker := time.NewTicker(bc.config.FlushInterval)
	defer ticker.Stop()
	defer bc.finalFlush()

	for {
		select {
		case <-bc.ctx.Done():
			return
		case <-ticker.C:
			bc.flush()
		case <-bc.triggerCh:
			bc.flush()
		}
	}
}

// flush swaps out the pending map under the lock and executes the batch
// without holding the lock.
func (bc *BatchCompleter) flush() {
	batch := bc.swapPending()
	if len(batch) == 0 {
		return
	}
	bc.executeBatch(context.WithoutCancel(bc.ctx), batch)
}

// finalFlush runs once on shutdown to drain any leftover pending operations.
// Uses a detached context so a cancelled bc.ctx does not abort the SQL calls.
func (bc *BatchCompleter) finalFlush() {
	batch := bc.swapPending()
	if len(batch) == 0 {
		return
	}
	bc.executeBatch(context.WithoutCancel(bc.ctx), batch)
}

func (bc *BatchCompleter) swapPending() map[claimKey]pendingOp {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.pending) == 0 {
		return nil
	}
	batch := bc.pending
	bc.pending = make(map[claimKey]pendingOp)
	// Wake backpressure waiters now that the backlog is drained.
	if bc.waitCh != nil {
		close(bc.waitCh)
		bc.waitCh = nil
	}
	return batch
}

func (bc *BatchCompleter) executeBatch(ctx context.Context, batch map[claimKey]pendingOp) {
	var (
		completeIDs, completeGenerations []int64
		failIDs, failGenerations         []int64
		failMessages                     []string
		retryIDs, retryGenerations       []int64
		retryBlockedTills                []time.Time
		retryMessages                    []string
		snoozeIDs, snoozeGenerations     []int64
		snoozeBlockedTills               []time.Time
	)

	for claim, op := range batch {
		id := claim.id
		switch op.kind {
		case opComplete:
			completeIDs = append(completeIDs, id)
			completeGenerations = append(completeGenerations, claim.generation)
		case opFail:
			failIDs = append(failIDs, id)
			failGenerations = append(failGenerations, claim.generation)
			failMessages = append(failMessages, op.message)
		case opRetry:
			retryIDs = append(retryIDs, id)
			retryGenerations = append(retryGenerations, claim.generation)
			retryBlockedTills = append(retryBlockedTills, op.blockedTill)
			retryMessages = append(retryMessages, op.message)
		case opSnooze:
			snoozeIDs = append(snoozeIDs, id)
			snoozeGenerations = append(snoozeGenerations, claim.generation)
			snoozeBlockedTills = append(snoozeBlockedTills, op.blockedTill)
		}
	}

	bc.runComplete(ctx, completeIDs, completeGenerations)
	bc.runFail(ctx, failIDs, failGenerations, failMessages)
	bc.runRetry(ctx, retryIDs, retryGenerations, retryBlockedTills, retryMessages)
	bc.runSnooze(ctx, snoozeIDs, snoozeGenerations, snoozeBlockedTills)
}

func (bc *BatchCompleter) runComplete(ctx context.Context, ids, generations []int64) {
	bc.forEachChunk(len(ids), func(lo, hi int) {
		chunk := ids[lo:hi]
		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: chunk, Generations: generations[lo:hi]})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to complete tasks batch", "count", len(chunk), "error", err)
			return
		}
		bc.logger.Debug("completed tasks batch", "count", len(chunk))
	})
}

func (bc *BatchCompleter) runFail(ctx context.Context, ids, generations []int64, messages []string) {
	bc.forEachChunk(len(ids), func(lo, hi int) {
		idsChunk := ids[lo:hi]
		msgChunk := messages[lo:hi]
		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.FailTasksMany(ctx, repository.FailTasksManyParams{
				Generations: generations[lo:hi],
				IDs:         idsChunk,
				Messages:    msgChunk,
			})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to fail tasks batch", "count", len(idsChunk), "error", err)
			return
		}
		bc.logger.Debug("failed tasks batch", "count", len(idsChunk))
	})
}

func (bc *BatchCompleter) runRetry(ctx context.Context, ids, generations []int64, blockedTills []time.Time, messages []string) {
	bc.forEachChunk(len(ids), func(lo, hi int) {
		idsChunk := ids[lo:hi]
		btChunk := blockedTills[lo:hi]
		msgChunk := messages[lo:hi]
		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{
				Generations:  generations[lo:hi],
				IDs:          idsChunk,
				BlockedTills: btChunk,
				Messages:     msgChunk,
			})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to retry tasks batch", "count", len(idsChunk), "error", err)
			return
		}
		bc.logger.Debug("retried tasks batch", "count", len(idsChunk))
	})
}

func (bc *BatchCompleter) runSnooze(ctx context.Context, ids, generations []int64, blockedTills []time.Time) {
	bc.forEachChunk(len(ids), func(lo, hi int) {
		idsChunk := ids[lo:hi]
		btChunk := blockedTills[lo:hi]
		err := bc.withRetries(ctx, func() error {
			_, err := bc.repo.SnoozeTasksMany(ctx, repository.SnoozeTasksManyParams{
				Generations:  generations[lo:hi],
				IDs:          idsChunk,
				BlockedTills: btChunk,
			})
			return err
		})
		if err != nil {
			bc.logger.Error("failed to snooze tasks batch", "count", len(idsChunk), "error", err)
			return
		}
		bc.logger.Debug("snoozed tasks batch", "count", len(idsChunk))
	})
}

func (bc *BatchCompleter) forEachChunk(total int, fn func(lo, hi int)) {
	for lo := 0; lo < total; lo += bc.config.MaxBatchSize {
		hi := min(lo+bc.config.MaxBatchSize, total)
		fn(lo, hi)
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

		sleep := time.Duration(1<<attempt) * time.Second
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
