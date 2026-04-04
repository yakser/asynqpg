package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/ui"
)

const (
	defaultDSN  = "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable"
	defaultAddr = ":8080"
	serviceName = "asynqpg-fullstack-demo"
)

type AppConfig struct {
	LogLevel     slog.Level
	InitialTasks int
	AutoGenerate bool
	LogCh        chan string
}

type Stats struct {
	EmailProcessed  int64
	NotifProcessed  int64
	ReportProcessed int64
}

type App struct {
	ctx    context.Context
	cancel context.CancelFunc

	producer   *producer.Producer
	consumer1  *consumer.Consumer
	consumer2  *consumer.Consumer
	httpServer *http.Server
	otelProvs  *otelProviders
	db         *sqlx.DB
	logger     *slog.Logger

	emailProcessed  atomic.Int64
	notifProcessed  atomic.Int64
	reportProcessed atomic.Int64

	autoRunning atomic.Bool
	autoCancel  context.CancelFunc
	autoMu      sync.Mutex

	Addr     string
	AuthMode string
	Seeded   int
}

func NewApp(cfg AppConfig) (*App, error) {
	ctx, cancel := context.WithCancel(context.Background())

	handler := NewChannelHandler(cfg.LogCh, cfg.LogLevel)
	logger := slog.New(handler)

	a := &App{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
	}

	ok := false
	defer func() {
		if !ok {
			a.Shutdown()
		}
	}()

	otelProvs, err := otelInit(ctx, serviceName)
	if err != nil {
		return nil, fmt.Errorf("init OTel: %w", err)
	}
	a.otelProvs = otelProvs

	dsn := envOr("DATABASE_URL", defaultDSN)
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	a.db = db
	logger.Info("connected to database")

	p, err := producer.New(producer.Config{
		Pool:           db,
		Logger:         logger.With("component", "producer"),
		MeterProvider:  otelProvs.MeterProvider,
		TracerProvider: otelProvs.TracerProvider,
	})
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}
	a.producer = p

	if cfg.InitialTasks > 0 {
		a.Seeded = a.seedTasks(cfg.InitialTasks)
	}

	c1, err := consumer.New(consumer.Config{
		Pool:               db,
		Logger:             logger.With("component", "consumer-1"),
		MeterProvider:      otelProvs.MeterProvider,
		TracerProvider:     otelProvs.TracerProvider,
		ClientID:           "consumer-1",
		FetchInterval:      200 * time.Millisecond,
		StuckThreshold:     2 * time.Minute,
		CompletedRetention: 1 * time.Hour,
		FailedRetention:    2 * time.Hour,
		CancelledRetention: 1 * time.Hour,
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer-1: %w", err)
	}

	if err := c1.RegisterTaskHandler("email.send",
		consumer.TaskHandlerFunc(func(_ context.Context, _ *asynqpg.TaskInfo) error {
			return simulateWork(3000, 5, &a.emailProcessed)
		}),
		consumer.WithWorkersCount(5),
		consumer.WithTimeout(30*time.Second),
	); err != nil {
		return nil, fmt.Errorf("register email handler: %w", err)
	}

	if err := c1.RegisterTaskHandler("notification.push",
		consumer.TaskHandlerFunc(func(_ context.Context, _ *asynqpg.TaskInfo) error {
			return simulateWork(1000, 2, &a.notifProcessed)
		}),
		consumer.WithWorkersCount(3),
		consumer.WithTimeout(15*time.Second),
	); err != nil {
		return nil, fmt.Errorf("register notification handler: %w", err)
	}

	c2, err := consumer.New(consumer.Config{
		Pool:               db,
		Logger:             logger.With("component", "consumer-2"),
		MeterProvider:      otelProvs.MeterProvider,
		TracerProvider:     otelProvs.TracerProvider,
		ClientID:           "consumer-2",
		DisableMaintenance: true,
		FetchInterval:      200 * time.Millisecond,
		RetryPolicy:        &asynqpg.ConstantRetryPolicy{Delay: 5 * time.Second},
	})
	if err != nil {
		return nil, fmt.Errorf("create consumer-2: %w", err)
	}

	if err := c2.RegisterTaskHandler("report.generate",
		consumer.TaskHandlerFunc(func(_ context.Context, _ *asynqpg.TaskInfo) error {
			return simulateWork(2000, 10, &a.reportProcessed)
		}),
		consumer.WithWorkersCount(3),
		consumer.WithTimeout(60*time.Second),
	); err != nil {
		return nil, fmt.Errorf("register report handler: %w", err)
	}

	if err := c1.Start(); err != nil {
		return nil, fmt.Errorf("start consumer-1: %w", err)
	}
	a.consumer1 = c1

	if err := c2.Start(); err != nil {
		return nil, fmt.Errorf("start consumer-2: %w", err)
	}
	a.consumer2 = c2

	addr := envOr("ADDR", defaultAddr)
	a.Addr = addr

	uiHandler, authMode, err := buildUIHandler(db, logger)
	if err != nil {
		return nil, fmt.Errorf("create UI handler: %w", err)
	}

	srv := &http.Server{
		Addr:         addr,
		Handler:      uiHandler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
		}
	}()
	a.httpServer = srv
	a.AuthMode = authMode

	if cfg.AutoGenerate {
		a.StartAutoGenerate()
	}

	logger.Info("demo started",
		"addr", addr,
		"consumers", 2,
		"seeded", a.Seeded,
		"auto_generate", cfg.AutoGenerate,
	)

	ok = true
	return a, nil
}

func (a *App) StartAutoGenerate() {
	a.autoMu.Lock()
	defer a.autoMu.Unlock()

	if a.autoRunning.Load() {
		return
	}

	ctx, cancel := context.WithCancel(a.ctx)
	a.autoCancel = cancel
	a.autoRunning.Store(true)
	go a.generateTasks(ctx)
}

func (a *App) StopAutoGenerate() {
	a.autoMu.Lock()
	defer a.autoMu.Unlock()

	if !a.autoRunning.Load() {
		return
	}

	a.autoCancel()
	a.autoRunning.Store(false)
}

func (a *App) AutoRunning() bool {
	return a.autoRunning.Load()
}

func (a *App) GetStats() Stats {
	return Stats{
		EmailProcessed:  a.emailProcessed.Load(),
		NotifProcessed:  a.notifProcessed.Load(),
		ReportProcessed: a.reportProcessed.Load(),
	}
}

func (a *App) EnqueueTasks(shortType string, count int) error {
	var fullType string
	var makePayload func(int) []byte

	switch shortType {
	case "email":
		fullType = "email.send"
		makePayload = func(i int) []byte {
			return []byte(fmt.Sprintf(`{"to":"cli-%s@example.com","subject":"CLI task #%d"}`, uuid.New().String()[:8], i))
		}
	case "notification":
		fullType = "notification.push"
		makePayload = func(i int) []byte {
			return []byte(fmt.Sprintf(`{"user_id":%d,"message":"CLI notification"}`, i))
		}
	case "report":
		fullType = "report.generate"
		makePayload = func(i int) []byte {
			return []byte(fmt.Sprintf(`{"report_id":%d,"type":"cli"}`, i))
		}
	default:
		return fmt.Errorf("unknown task type: %s (use email, notification, or report)", shortType)
	}

	tasks := make([]*asynqpg.Task, count)
	for i := range tasks {
		tasks[i] = asynqpg.NewTask(fullType, makePayload(i),
			asynqpg.WithIdempotencyToken("cli:"+fullType+":"+uuid.New().String()),
		)
	}

	result, err := a.producer.EnqueueMany(a.ctx, tasks)
	if err != nil {
		return fmt.Errorf("enqueue %s tasks: %w", shortType, err)
	}

	a.logger.Info("enqueued tasks via CLI", "type", fullType, "count", result.InsertedCount())
	return nil
}

func (a *App) Shutdown() {
	if a.autoRunning.Load() {
		a.StopAutoGenerate()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if a.httpServer != nil {
		_ = a.httpServer.Shutdown(shutdownCtx)
	}
	if a.consumer1 != nil {
		_ = a.consumer1.Stop()
	}
	if a.consumer2 != nil {
		_ = a.consumer2.Stop()
	}
	if a.otelProvs != nil {
		a.otelProvs.Shutdown(shutdownCtx)
	}
	if a.db != nil {
		_ = a.db.Close()
	}

	a.cancel()
}

func (a *App) seedTasks(total int) int {
	emailCount := total / 2
	notifCount := total * 3 / 10
	reportCount := total - emailCount - notifCount

	var seeded int

	if emailCount > 0 {
		tasks := make([]*asynqpg.Task, emailCount)
		for i := range tasks {
			tasks[i] = asynqpg.NewTask("email.send",
				[]byte(fmt.Sprintf(`{"to":"user%d@example.com","subject":"Hello #%d"}`, i, i)),
				asynqpg.WithIdempotencyToken("seed:email.send:"+uuid.New().String()),
			)
		}
		result, err := a.producer.EnqueueMany(a.ctx, tasks)
		if err != nil {
			a.logger.Error("failed to seed email tasks", "error", err)
		} else {
			seeded += result.InsertedCount()
			a.logger.Info("seeded email tasks", "count", result.InsertedCount())
		}
	}

	if notifCount > 0 {
		tasks := make([]*asynqpg.Task, notifCount)
		for i := range tasks {
			opts := []asynqpg.TaskOption{
				asynqpg.WithIdempotencyToken("seed:notification.push:" + uuid.New().String()),
			}
			if i%5 == 0 {
				opts = append(opts, asynqpg.WithDelay(time.Duration(i)*100*time.Millisecond))
			}
			tasks[i] = asynqpg.NewTask("notification.push",
				[]byte(fmt.Sprintf(`{"user_id":%d,"message":"notification #%d"}`, i, i)),
				opts...,
			)
		}
		result, err := a.producer.EnqueueMany(a.ctx, tasks)
		if err != nil {
			a.logger.Error("failed to seed notification tasks", "error", err)
		} else {
			seeded += result.InsertedCount()
			a.logger.Info("seeded notification tasks", "count", result.InsertedCount())
		}
	}

	for i := range reportCount {
		task := asynqpg.NewTask("report.generate",
			[]byte(fmt.Sprintf(`{"report_id":%d,"type":"monthly"}`, i)),
			asynqpg.WithMaxRetry(2),
			asynqpg.WithIdempotencyToken("seed:report.generate:"+uuid.New().String()),
		)
		if _, err := a.producer.Enqueue(a.ctx, task); err != nil {
			a.logger.Error("failed to seed report task", "index", i, "error", err)
		} else {
			seeded++
		}
	}
	if reportCount > 0 {
		a.logger.Info("seeded report tasks", "count", reportCount)
	}

	return seeded
}

func (a *App) generateTasks(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emailTasks := make([]*asynqpg.Task, 5)
			for i := range emailTasks {
				emailTasks[i] = asynqpg.NewTask("email.send",
					[]byte(fmt.Sprintf(`{"to":"gen-%s@example.com","subject":"Generated"}`, uuid.New().String()[:8])),
					asynqpg.WithIdempotencyToken("gen:email.send:"+uuid.New().String()),
				)
			}
			if _, err := a.producer.EnqueueMany(ctx, emailTasks); err != nil {
				a.logger.Error("failed to enqueue generated emails", "error", err)
			}

			delayedNotif := asynqpg.NewTask("notification.push",
				[]byte(`{"user_id":0,"message":"delayed notification"}`),
				asynqpg.WithIdempotencyToken("gen:notification.push:"+uuid.New().String()),
				asynqpg.WithDelay(10*time.Second),
			)
			if _, err := a.producer.Enqueue(ctx, delayedNotif); err != nil {
				a.logger.Error("failed to enqueue delayed notification", "error", err)
			}

			scheduledReport := asynqpg.NewTask("report.generate",
				[]byte(`{"report_id":0,"type":"scheduled"}`),
				asynqpg.WithIdempotencyToken("gen:report.generate:"+uuid.New().String()),
			)
			scheduledReport.ProcessAt = time.Now().Add(30 * time.Second)
			if _, err := a.producer.Enqueue(ctx, scheduledReport); err != nil {
				a.logger.Error("failed to enqueue scheduled report", "error", err)
			}

			retryNotif := asynqpg.NewTask("notification.push",
				[]byte(`{"user_id":0,"message":"high-retry notification"}`),
				asynqpg.WithIdempotencyToken("gen:notification.push:retry:"+uuid.New().String()),
				asynqpg.WithMaxRetry(5),
			)
			if _, err := a.producer.Enqueue(ctx, retryNotif); err != nil {
				a.logger.Error("failed to enqueue retry notification", "error", err)
			}
		}
	}
}

func buildUIHandler(db *sqlx.DB, logger *slog.Logger) (http.Handler, string, error) {
	opts := ui.HandlerOpts{
		Pool:   db,
		Logger: logger.With("component", "ui"),
	}

	authMode := "No auth"

	basicUser := os.Getenv("BASIC_AUTH_USER")
	basicPass := os.Getenv("BASIC_AUTH_PASS")
	clientID := os.Getenv("GITHUB_CLIENT_ID")
	clientSecret := os.Getenv("GITHUB_CLIENT_SECRET")

	switch {
	case basicUser != "" && basicPass != "":
		logger.Info("Basic Auth enabled", "user", basicUser)
		opts.BasicAuth = &ui.BasicAuth{
			Username: basicUser,
			Password: basicPass,
		}
		authMode = "Basic Auth"
	case clientID != "" && clientSecret != "":
		logger.Info("GitHub OAuth enabled")
		opts.AuthProviders = []ui.AuthProvider{
			NewGitHubAuthProvider(clientID, clientSecret),
		}
		authMode = "GitHub OAuth"
	default:
		logger.Info("running without authentication (set BASIC_AUTH_USER/BASIC_AUTH_PASS or GITHUB_CLIENT_ID/GITHUB_CLIENT_SECRET to enable)")
	}

	handler, err := ui.NewHandler(opts)
	return handler, authMode, err
}

func simulateWork(baseMs, errorPct int, counter *atomic.Int64) error {
	jitter := baseMs / 5
	ms := baseMs - jitter + rand.IntN(2*jitter+1)
	duration := time.Duration(ms) * time.Millisecond
	time.Sleep(duration)

	if rand.IntN(100) < errorPct {
		return fmt.Errorf("simulated error after %v", duration)
	}

	counter.Add(1)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
