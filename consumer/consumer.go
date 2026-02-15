package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/completer"
	"github.com/yakser/asynqpg/internal/leadership"
	"github.com/yakser/asynqpg/internal/lib/call"
	"github.com/yakser/asynqpg/internal/lib/periodic"
	"github.com/yakser/asynqpg/internal/lib/workerpool"
	"github.com/yakser/asynqpg/internal/maintenance"
	"github.com/yakser/asynqpg/internal/repository"
)

const (
	defaultShutdownTimeout     = 30 * time.Second
	defaultFetchInterval       = 100 * time.Millisecond
	defaultJanitorInterval     = 30 * time.Second
	defaultStuckThreshold      = 5 * time.Minute
	defaultWorkersCount        = 10
	defaultMaxAttempts         = 3
	defaultTimeout             = 30 * time.Second
	lockMargin                 = 5 * time.Second
	defaultCancelCheckInterval = 1 * time.Second
)

type TaskHandler interface {
	Handle(ctx context.Context, task *asynqpg.TaskInfo) error
}

type Consumer struct {
	pool        asynqpg.Pool
	repo        *repository.Repository
	logger      *slog.Logger
	retryPolicy asynqpg.RetryPolicy
	metrics     *asynqpg.Metrics
	tracer      trace.Tracer

	// Leadership and maintenance
	elector    *leadership.Elector
	maintainer *maintenance.Maintainer

	// Batch completer for efficient DB operations
	completer            completer.Completer
	enableBatchCompleter bool

	// ErrorHandler for permanent task failures
	errorHandler asynqpg.ErrorHandler

	// activeTaskCancels tracks cancel functions for running tasks.
	// Used to cancel handler context when a task is cancelled via CancelTask.
	activeTaskCancels sync.Map // map[int64]context.CancelFunc

	mu               sync.Mutex
	started          bool
	ctx              context.Context
	cancel           context.CancelFunc
	handlers         map[string]TaskHandler
	wp               map[string]*workerpool.WorkerPool
	fetchers         map[string]*periodic.Runner
	janitors         map[string]*periodic.Runner
	cancelChecker    *periodic.Runner
	taskOptions      map[string]*TaskTypeOptions
	globalMiddleware []MiddlewareFunc

	shutdownTimeout     time.Duration
	fetchInterval       time.Duration
	janitorInterval     time.Duration
	stuckThreshold      time.Duration
	cancelCheckInterval time.Duration

	defaultWorkersCount int
	defaultMaxAttempts  int
	defaultTimeout      time.Duration

	// Maintenance configuration
	enableMaintenance  bool
	completedRetention time.Duration
	failedRetention    time.Duration
	cancelledRetention time.Duration
}

type Config struct {
	Pool   asynqpg.Pool
	Logger *slog.Logger

	// RetryPolicy determines retry delays for failed tasks.
	// If nil, DefaultRetryPolicy is used.
	RetryPolicy asynqpg.RetryPolicy

	ShutdownTimeout time.Duration
	FetchInterval   time.Duration
	JanitorInterval time.Duration
	// StuckThreshold is the duration after which a running task is considered stuck.
	// Used by the rescuer to detect and recover stuck tasks.
	StuckThreshold time.Duration

	DefaultWorkersCount int
	DefaultMaxAttempts  int
	DefaultTimeout      time.Duration

	// CancelCheckInterval is how often the consumer checks for tasks cancelled while running.
	// Default: 1s.
	CancelCheckInterval time.Duration

	// DisableMaintenance disables maintenance services (rescuer, cleaner).
	// By default, maintenance is enabled and the consumer participates in leader election.
	// Only the leader runs maintenance services.
	DisableMaintenance bool

	// ClientID is a unique identifier for this consumer instance.
	// Used for leader election. If empty, a UUID will be generated.
	ClientID string

	// Retention periods for task cleanup (used by cleaner)
	CompletedRetention time.Duration // Default: 24h
	FailedRetention    time.Duration // Default: 7 days
	CancelledRetention time.Duration // Default: 24h

	// DisableBatchCompleter disables batching of task completions.
	// By default, completions are accumulated and flushed in batches for better throughput.
	DisableBatchCompleter bool

	// BatchCompleterConfig configures the batch completer.
	// Only used when batch completer is enabled (not disabled).
	BatchCompleterConfig *completer.Config

	// ErrorHandler is called when a task fails permanently (all retries exhausted,
	// ErrSkipRetry, or panic). If nil, permanent failures are only logged.
	ErrorHandler asynqpg.ErrorHandler

	// MeterProvider for metrics. If nil, global OTel MeterProvider is used.
	MeterProvider metric.MeterProvider
	// TracerProvider for tracing. If nil, global OTel TracerProvider is used.
	TracerProvider trace.TracerProvider
}

func New(config Config) (*Consumer, error) {
	if config.Pool == nil {
		return nil, fmt.Errorf("database pool is required")
	}

	ctx, cancel := context.WithCancel(context.Background())

	completedRetention := config.CompletedRetention

	enableBatchCompleter := !config.DisableBatchCompleter

	repo := repository.NewRepository(config.Pool)

	m, err := asynqpg.NewMetrics(config.MeterProvider)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create metrics: %w", err)
	}

	consumer := &Consumer{
		pool:                 config.Pool,
		repo:                 repo,
		logger:               config.Logger,
		retryPolicy:          config.RetryPolicy,
		metrics:              m,
		tracer:               asynqpg.NewTracer(config.TracerProvider),
		ctx:                  ctx,
		cancel:               cancel,
		handlers:             make(map[string]TaskHandler),
		wp:                   make(map[string]*workerpool.WorkerPool),
		fetchers:             make(map[string]*periodic.Runner),
		janitors:             make(map[string]*periodic.Runner),
		taskOptions:          make(map[string]*TaskTypeOptions),
		shutdownTimeout:      config.ShutdownTimeout,
		fetchInterval:        config.FetchInterval,
		janitorInterval:      config.JanitorInterval,
		stuckThreshold:       config.StuckThreshold,
		cancelCheckInterval:  config.CancelCheckInterval,
		defaultWorkersCount:  config.DefaultWorkersCount,
		defaultMaxAttempts:   config.DefaultMaxAttempts,
		defaultTimeout:       config.DefaultTimeout,
		enableMaintenance:    !config.DisableMaintenance,
		completedRetention:   completedRetention,
		failedRetention:      config.FailedRetention,
		cancelledRetention:   config.CancelledRetention,
		enableBatchCompleter: enableBatchCompleter,
		errorHandler:         config.ErrorHandler,
	}
	consumer.setDefaults()

	// Initialize batch completer
	if consumer.enableBatchCompleter {
		consumer.initBatchCompleter(repo, config.BatchCompleterConfig)
	}

	// Initialize leadership and maintenance if enabled
	if consumer.enableMaintenance {
		consumer.initMaintenanceServices(config.ClientID)
	}

	return consumer, nil
}

func (c *Consumer) setDefaults() {
	if c.logger == nil {
		c.logger = slog.Default()
	}
	if c.retryPolicy == nil {
		c.retryPolicy = &asynqpg.DefaultRetryPolicy{}
	}
	if c.shutdownTimeout <= 0 {
		c.shutdownTimeout = defaultShutdownTimeout
	}
	if c.fetchInterval <= 0 {
		c.fetchInterval = defaultFetchInterval
	}
	if c.janitorInterval <= 0 {
		c.janitorInterval = defaultJanitorInterval
	}
	if c.stuckThreshold <= 0 {
		c.stuckThreshold = defaultStuckThreshold
	}
	if c.defaultWorkersCount <= 0 {
		c.defaultWorkersCount = defaultWorkersCount
	}
	if c.defaultMaxAttempts <= 0 {
		c.defaultMaxAttempts = defaultMaxAttempts
	}
	if c.defaultTimeout <= 0 {
		c.defaultTimeout = defaultTimeout
	}
	if c.cancelCheckInterval <= 0 {
		c.cancelCheckInterval = defaultCancelCheckInterval
	}
	// Maintenance retention defaults
	if c.completedRetention <= 0 {
		c.completedRetention = 24 * time.Hour
	}
	if c.failedRetention <= 0 {
		c.failedRetention = 7 * 24 * time.Hour
	}
	if c.cancelledRetention <= 0 {
		c.cancelledRetention = 24 * time.Hour
	}
}

func (c *Consumer) initBatchCompleter(repo *repository.Repository, cfg *completer.Config) {
	var completerCfg completer.Config
	if cfg != nil {
		completerCfg = *cfg
	} else {
		completerCfg = completer.DefaultConfig()
	}
	completerCfg.Logger = c.logger.With("component", "batch_completer")

	c.completer = completer.NewBatchCompleter(repo, completerCfg)
}

func (c *Consumer) initMaintenanceServices(clientID string) {
	// Create elector for leader election
	c.elector = leadership.NewElector(c.pool, leadership.ElectorConfig{
		ClientID: clientID,
		Logger:   c.logger.With("component", "elector"),
	})

	// Create maintenance services
	rescuer := maintenance.NewRescuer(c.repo, maintenance.RescuerConfig{
		RescueAfter: c.stuckThreshold,
		RetryPolicy: c.retryPolicy,
		Logger:      c.logger.With("component", "rescuer"),
	})

	cleaner := maintenance.NewCleaner(c.repo, maintenance.CleanerConfig{
		CompletedRetention: c.completedRetention,
		FailedRetention:    c.failedRetention,
		CancelledRetention: c.cancelledRetention,
		Logger:             c.logger.With("component", "cleaner"),
	})

	// Create maintainer that manages all maintenance services
	c.maintainer = maintenance.NewMaintainer(
		c.logger.With("component", "maintainer"),
		rescuer,
		cleaner,
	)
}

// Start starts the consumer and begins processing tasks.
// If maintenance is enabled, it also starts leader election.
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("consumer is already started")
	}

	if len(c.handlers) == 0 {
		return fmt.Errorf("no task handlers registered")
	}

	// Start batch completer if enabled
	if c.enableBatchCompleter && c.completer != nil {
		if err := c.completer.Start(c.ctx); err != nil {
			return fmt.Errorf("start batch completer: %w", err)
		}
	}

	// Start task fetchers
	for taskType := range c.handlers {
		if err := c.startFetcher(taskType); err != nil {
			c.stopAll()
			return fmt.Errorf("start fetcher for task type %s: %w", taskType, err)
		}
	}

	// Start cancel checker for graceful cancellation of running tasks
	if err := c.startCancelChecker(); err != nil {
		c.stopAll()
		return fmt.Errorf("start cancel checker: %w", err)
	}

	// Start leadership election if maintenance is enabled
	if c.enableMaintenance && c.elector != nil {
		if err := c.elector.Start(c.ctx); err != nil {
			c.stopAll()
			return fmt.Errorf("start elector: %w", err)
		}

		// Start goroutine to handle leadership changes
		go c.handleLeadershipChanges()
	}

	c.started = true
	c.logger.Info("consumer started",
		"task_types", len(c.handlers),
		"maintenance_enabled", c.enableMaintenance,
		"batch_completer_enabled", c.enableBatchCompleter,
	)
	return nil
}

func (c *Consumer) handleLeadershipChanges() {
	if c.elector == nil {
		return
	}

	leaderCh := c.elector.Subscribe()
	for {
		select {
		case <-c.ctx.Done():
			return
		case isLeader := <-leaderCh:
			if isLeader {
				c.logger.Info("this instance is now the leader, starting maintenance services")
				if err := c.maintainer.Start(c.ctx); err != nil {
					c.logger.Error("failed to start maintenance services", "error", err)
				}
			} else if c.maintainer.IsStarted() {
				c.logger.Info("this instance is no longer the leader, stopping maintenance services")
				c.maintainer.Stop()
			}
		}
	}
}

func (c *Consumer) Stop() error {
	return c.shutdown(c.shutdownTimeout)
}

func (c *Consumer) Shutdown(timeout time.Duration) error {
	return c.shutdown(timeout)
}

func (c *Consumer) shutdown(timeout time.Duration) error {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return nil
	}

	c.logger.Info("consumer shutting down", "timeout", timeout.String())
	c.cancel()
	c.mu.Unlock()

	done := make(chan struct{})
	go func() {
		c.stopAll()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("consumer stopped gracefully")
		return nil
	case <-time.After(timeout):
		c.logger.Error("consumer shutdown timeout exceeded")
		return fmt.Errorf("shutdown timeout exceeded")
	}
}

func (c *Consumer) stopAll() {
	// Stop maintenance services first
	if c.maintainer != nil {
		c.logger.Debug("stopping maintainer")
		c.maintainer.Stop()
	}

	// Stop elector
	if c.elector != nil {
		c.logger.Debug("stopping elector")
		c.elector.Stop()
	}

	// Stop cancel checker
	if c.cancelChecker != nil {
		c.logger.Debug("stopping cancel checker")
		c.cancelChecker.Stop()
	}

	for taskType, fetcher := range c.fetchers {
		c.logger.Debug("stopping fetcher", "task_type", taskType)
		fetcher.Stop()
	}

	for taskType, janitor := range c.janitors {
		c.logger.Debug("stopping janitor", "task_type", taskType)
		janitor.Stop()
	}

	for taskType, pool := range c.wp {
		c.logger.Debug("closing worker pool", "task_type", taskType)
		pool.Close()
	}

	// Stop batch completer last to ensure all pending completions are flushed
	if c.completer != nil {
		c.logger.Debug("stopping batch completer")
		c.completer.Stop()
	}

	c.mu.Lock()
	c.started = false
	c.mu.Unlock()
}

func (c *Consumer) startCancelChecker() error {
	checkerTask := func(ctx context.Context) error {
		// Collect active task IDs
		var activeIDs []int64
		c.activeTaskCancels.Range(func(key, _ any) bool {
			activeIDs = append(activeIDs, key.(int64))
			return true
		})

		if len(activeIDs) == 0 {
			return nil
		}

		cancelledIDs, err := c.repo.GetCancelledTaskIDs(ctx, activeIDs)
		if err != nil {
			return fmt.Errorf("check cancelled tasks: %w", err)
		}

		for _, id := range cancelledIDs {
			if cancelFn, ok := c.activeTaskCancels.LoadAndDelete(id); ok {
				c.logger.Info("cancelling running task", "task_id", id)
				cancelFn.(context.CancelFunc)()
			}
		}

		return nil
	}

	runner, err := periodic.New(periodic.Config{
		Interval: c.cancelCheckInterval,
		Task:     checkerTask,
		Logger:   c.logger.With("component", "cancel_checker"),
	})
	if err != nil {
		return fmt.Errorf("create cancel checker: %w", err)
	}

	if err := runner.Start(c.ctx); err != nil {
		return fmt.Errorf("start cancel checker: %w", err)
	}

	c.cancelChecker = runner
	return nil
}

type TaskTypeOptions struct {
	WorkersCount int
	MaxAttempts  int
	Timeout      time.Duration
	Middleware   []MiddlewareFunc
}

type TaskTypeOption func(*TaskTypeOptions)

func WithWorkersCount(count int) TaskTypeOption {
	return func(o *TaskTypeOptions) {
		o.WorkersCount = count
	}
}

func WithMaxAttempts(attempts int) TaskTypeOption {
	return func(o *TaskTypeOptions) {
		o.MaxAttempts = attempts
	}
}

func WithTimeout(timeout time.Duration) TaskTypeOption {
	return func(o *TaskTypeOptions) {
		o.Timeout = timeout
	}
}

// WithMiddleware sets per-task-type middleware applied after global middleware.
func WithMiddleware(mws ...MiddlewareFunc) TaskTypeOption {
	return func(o *TaskTypeOptions) {
		o.Middleware = append(o.Middleware, mws...)
	}
}

// Use registers global middleware that wraps every task handler.
// Middleware is applied in registration order: first registered = outermost.
// Must be called before Start().
func (c *Consumer) Use(mws ...MiddlewareFunc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("cannot add middleware after consumer is started")
	}

	for _, mw := range mws {
		if mw != nil {
			c.globalMiddleware = append(c.globalMiddleware, mw)
		}
	}

	return nil
}

func (c *Consumer) RegisterTaskHandler(taskType string, handler TaskHandler, opts ...TaskTypeOption) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return fmt.Errorf("cannot register handler after consumer is started")
	}

	if _, ok := c.handlers[taskType]; ok {
		return ErrTaskHandlerAlreadyRegistered
	}

	options := &TaskTypeOptions{
		WorkersCount: c.defaultWorkersCount,
		MaxAttempts:  c.defaultMaxAttempts,
		Timeout:      c.defaultTimeout,
	}
	for _, opt := range opts {
		opt(options)
	}

	c.handlers[taskType] = handler
	c.wp[taskType] = workerpool.NewWorkerPool(options.WorkersCount)
	c.taskOptions[taskType] = options
	c.logger.Debug("registered task handler",
		"task_type", taskType,
		"workers_count", options.WorkersCount,
		"max_attempts", options.MaxAttempts,
		"timeout", options.Timeout,
	)

	return nil
}

func (c *Consumer) startFetcher(taskType string) error {
	handler := buildHandlerChain(c.handlers[taskType], c.globalMiddleware, c.taskOptions[taskType].Middleware)
	wp := c.wp[taskType]
	options := c.taskOptions[taskType]

	fetcherTask := func(ctx context.Context) error {
		freeWorkers := wp.FreeWorkers()
		if freeWorkers == 0 {
			c.logger.Debug("fetcher no workers", "task_type", taskType)
			return nil
		}

		// Lock duration should cover task execution timeout plus safety margin
		// to prevent other pods from picking up the same task while it's being processed
		lockDuration := options.Timeout + lockMargin
		tasks, err := c.repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{
			Type:  taskType,
			Limit: freeWorkers,
			Delay: lockDuration,
		})
		if err != nil {
			return fmt.Errorf("get ready tasks: %w", err)
		}

		if len(tasks) == 0 {
			c.logger.Debug("no ready tasks available",
				"task_type", taskType,
			)
			return nil
		}

		c.logger.Debug("got ready tasks",
			"task_type", taskType,
			"count", len(tasks),
			"free_workers", freeWorkers,
		)

		for _, t := range tasks {
			task := t
			submitErr := wp.Submit(func() {
				c.processTask(handler, &task, options.Timeout)
			})
			if submitErr != nil {
				c.logger.Error("failed to submit task to worker pool",
					"task_id", task.ID,
					"error", submitErr,
				)
			}
		}

		return nil
	}

	runner, err := periodic.New(periodic.Config{
		Interval: c.fetchInterval,
		Task:     fetcherTask,
		Logger:   c.logger.With("component", "fetcher", "task_type", taskType),
	})
	if err != nil {
		return fmt.Errorf("create fetcher: %w", err)
	}

	if err := runner.Start(c.ctx); err != nil {
		return fmt.Errorf("start fetcher: %w", err)
	}

	c.fetchers[taskType] = runner
	return nil
}

func (c *Consumer) processTask(handler TaskHandler, readyTask *repository.ReadyTask, timeout time.Duration) {
	taskCtx, cancel := context.WithTimeout(c.ctx, timeout)
	defer cancel()

	// Track this task's cancel func so CancelTask can stop it.
	c.activeTaskCancels.Store(readyTask.ID, cancel)
	defer c.activeTaskCancels.Delete(readyTask.ID)

	taskCtx = asynqpg.WithTaskMetadata(taskCtx, asynqpg.TaskMetadata{
		ID:         readyTask.ID,
		RetryCount: readyTask.AttemptsElapsed,
		MaxRetry:   readyTask.AttemptsLeft + readyTask.AttemptsElapsed,
		CreatedAt:  readyTask.CreatedAt,
	})

	taskTypeAttr := asynqpg.AttrTaskType.String(readyTask.Type)

	taskCtx, span := c.tracer.Start(taskCtx, "asynqpg.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			taskTypeAttr,
			attribute.Int64("task_id", readyTask.ID),
			attribute.Int("attempt", readyTask.AttemptsElapsed+1),
		),
	)
	defer span.End()

	attemptedAt := readyTask.AttemptedAt
	taskInfo := &asynqpg.TaskInfo{
		ID:               readyTask.ID,
		Type:             readyTask.Type,
		Payload:          readyTask.Payload,
		IdempotencyToken: readyTask.IdempotencyToken,
		AttemptsLeft:     readyTask.AttemptsLeft,
		AttemptsElapsed:  readyTask.AttemptsElapsed,
		CreatedAt:        readyTask.CreatedAt,
		AttemptedAt:      &attemptedAt,
		Messages:         []string(readyTask.Messages),
	}

	c.logger.Debug("processing task",
		"task_id", readyTask.ID,
		"task_type", readyTask.Type,
		"attempt", readyTask.AttemptsElapsed+1,
		"timeout", timeout,
	)

	c.metrics.TasksInFlight.Add(taskCtx, 1, metric.WithAttributes(taskTypeAttr))
	started := time.Now()

	err := call.WithRecover(func() error {
		return handler.Handle(taskCtx, taskInfo)
	})
	duration := time.Since(started)

	c.metrics.TasksInFlight.Add(taskCtx, -1, metric.WithAttributes(taskTypeAttr))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "task processing failed")

		errorType := asynqpg.ErrorTypeHandler
		if taskCtx.Err() != nil {
			errorType = "timeout"
		}
		c.metrics.TasksErrors.Add(taskCtx, 1, metric.WithAttributes(
			taskTypeAttr, asynqpg.AttrErrorType.String(errorType),
		))
	}

	// Determine status for metrics
	status := c.resolveTaskStatus(readyTask, err)
	c.metrics.TasksProcessed.Add(taskCtx, 1, metric.WithAttributes(
		taskTypeAttr, asynqpg.AttrStatus.String(status),
	))
	c.metrics.TaskDuration.Record(taskCtx, duration.Seconds(), metric.WithAttributes(
		taskTypeAttr, asynqpg.AttrStatus.String(status),
	))

	// Invoke error handler on permanent failure
	if err != nil && c.errorHandler != nil && status == asynqpg.StatusFailed {
		c.errorHandler.HandleError(taskCtx, taskInfo, err)
	}

	handleErr := c.handleProcessResult(readyTask, err)
	if handleErr != nil {
		c.logger.Error("failed to handle task process result", "task_id", readyTask.ID, "error", handleErr)
	}
}

func (c *Consumer) resolveTaskStatus(task *repository.ReadyTask, err error) string {
	if err == nil {
		return asynqpg.StatusCompleted
	}
	if errors.Is(err, asynqpg.ErrSkipRetry) {
		return asynqpg.StatusFailed
	}

	var snoozeErr *asynqpg.TaskSnoozeError
	if errors.As(err, &snoozeErr) {
		return asynqpg.StatusSnoozed
	}

	var snoozeWithErrErr *asynqpg.TaskSnoozeWithErrError
	if errors.As(err, &snoozeWithErrErr) {
		if task.AttemptsLeft-1 <= 0 {
			return asynqpg.StatusFailed
		}
		return asynqpg.StatusSnoozed
	}

	if task.AttemptsLeft-1 > 0 {
		return asynqpg.StatusRetried
	}
	return asynqpg.StatusFailed
}

func (c *Consumer) handleProcessResult(task *repository.ReadyTask, processErr error) error {
	// If the task handler returned context.Canceled and the consumer is NOT shutting down,
	// the task was cancelled via CancelTask. The DB status is already 'cancelled',
	// so skip further state transitions.
	if processErr != nil && errors.Is(processErr, context.Canceled) && c.ctx.Err() == nil {
		c.logger.Info("task was cancelled while running",
			"task_id", task.ID,
			"task_type", task.Type,
		)
		return nil
	}

	if processErr != nil {
		return c.handleTaskError(task, processErr)
	}

	// Use batch completer if enabled
	if c.enableBatchCompleter && c.completer != nil {
		if err := c.completer.Complete(task.ID); err != nil {
			c.logger.Error("failed to queue task completion",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	} else {
		// Fallback to direct completion
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := c.repo.CompleteTasks(cleanupCtx, []int64{task.ID}); err != nil {
			c.logger.Error("failed to mark task as completed",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	}

	c.logger.Debug("task processed successfully",
		"task_id", task.ID,
		"task_type", task.Type,
	)
	return nil
}

func (c *Consumer) handleTaskError(task *repository.ReadyTask, processErr error) error {
	attempt := task.AttemptsElapsed + 1
	attemptsLeft := task.AttemptsLeft - 1

	var snoozeErr *asynqpg.TaskSnoozeError
	if errors.As(processErr, &snoozeErr) {
		return c.snoozeTask(task, snoozeErr.Duration)
	}

	var snoozeWithErrErr *asynqpg.TaskSnoozeWithErrError
	if errors.As(processErr, &snoozeWithErrErr) {
		if attemptsLeft <= 0 {
			return c.failTask(task, processErr)
		}
		return c.retryTaskWithDelay(task, snoozeWithErrErr.Duration, processErr)
	}

	if errors.Is(processErr, asynqpg.ErrSkipRetry) {
		c.logger.Warn("task failed with SkipRetry, skipping remaining retries",
			"task_id", task.ID,
			"task_type", task.Type,
			"error", processErr,
			"attempt", attempt,
			"attempts_left", attemptsLeft,
		)
		return c.failTask(task, processErr)
	}

	if attemptsLeft > 0 {
		// Retry the task
		nextRetryDelay := c.retryPolicy.NextRetry(attempt)
		nextRetryAt := time.Now().Add(nextRetryDelay)

		c.logger.Info("task failed, scheduling retry",
			"task_id", task.ID,
			"task_type", task.Type,
			"error", processErr,
			"attempt", attempt,
			"attempts_left", attemptsLeft,
			"next_retry_at", nextRetryAt,
			"retry_delay", nextRetryDelay,
		)

		// Use batch completer if enabled
		if c.enableBatchCompleter && c.completer != nil {
			if err := c.completer.Retry(task.ID, nextRetryAt, processErr.Error()); err != nil {
				c.logger.Error("failed to queue task retry",
					"task_id", task.ID,
					"error", err,
				)
				return err
			}
		} else {
			// Fallback to direct retry
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			if err := c.repo.RetryTask(ctx, repository.RetryTaskParams{
				ID:          task.ID,
				BlockedTill: nextRetryAt,
				Message:     processErr.Error(),
			}); err != nil {
				c.logger.Error("failed to schedule task retry",
					"task_id", task.ID,
					"error", err,
				)
				return err
			}
		}
		return nil
	}

	// No more retries - discard the task
	c.logger.Error("task failed, no retries left - discarding",
		"task_id", task.ID,
		"task_type", task.Type,
		"error", processErr,
		"attempt", attempt,
	)

	return c.failTask(task, processErr)
}

func (c *Consumer) failTask(task *repository.ReadyTask, processErr error) error {
	// Use batch completer if enabled
	if c.enableBatchCompleter && c.completer != nil {
		if err := c.completer.Fail(task.ID, processErr.Error()); err != nil {
			c.logger.Error("failed to queue task failure",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	} else {
		// Fallback to direct fail
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := c.repo.FailTasks(ctx, []int64{task.ID}, processErr.Error()); err != nil {
			c.logger.Error("failed to discard task",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	}
	return nil
}

func (c *Consumer) snoozeTask(task *repository.ReadyTask, duration time.Duration) error {
	snoozeTill := time.Now().Add(duration)

	c.logger.Info("task snoozed",
		"task_id", task.ID,
		"task_type", task.Type,
		"snooze_duration", duration,
		"snooze_till", snoozeTill,
	)

	if c.enableBatchCompleter && c.completer != nil {
		if err := c.completer.Snooze(task.ID, snoozeTill); err != nil {
			c.logger.Error("failed to queue task snooze",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := c.repo.SnoozeTask(ctx, repository.SnoozeTaskParams{
			ID:          task.ID,
			BlockedTill: snoozeTill,
		}); err != nil {
			c.logger.Error("failed to snooze task",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	}
	return nil
}

func (c *Consumer) retryTaskWithDelay(task *repository.ReadyTask, delay time.Duration, processErr error) error {
	attempt := task.AttemptsElapsed + 1
	attemptsLeft := task.AttemptsLeft - 1
	nextRetryAt := time.Now().Add(delay)

	c.logger.Info("task snoozed with error, scheduling retry",
		"task_id", task.ID,
		"task_type", task.Type,
		"error", processErr,
		"attempt", attempt,
		"attempts_left", attemptsLeft,
		"next_retry_at", nextRetryAt,
		"snooze_delay", delay,
	)

	if c.enableBatchCompleter && c.completer != nil {
		if err := c.completer.Retry(task.ID, nextRetryAt, processErr.Error()); err != nil {
			c.logger.Error("failed to queue task retry (snooze with error)",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := c.repo.RetryTask(ctx, repository.RetryTaskParams{
			ID:          task.ID,
			BlockedTill: nextRetryAt,
			Message:     processErr.Error(),
		}); err != nil {
			c.logger.Error("failed to schedule task retry (snooze with error)",
				"task_id", task.ID,
				"error", err,
			)
			return err
		}
	}
	return nil
}
