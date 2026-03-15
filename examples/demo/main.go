package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/internal/otelsetup"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/ui"
)

const (
	defaultDSN  = "postgres://postgres:password@localhost:5432/asynqpg?sslmode=disable"
	defaultAddr = ":8080"
	serviceName = "asynqpg-fullstack-demo"
)

func main() {
	loadEnvFile(".env")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// OTel SDK setup.
	otel, err := otelsetup.Init(ctx, serviceName)
	if err != nil {
		log.Fatalf("failed to init OTel: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		otel.Shutdown(shutdownCtx)
	}()

	// Database.
	dsn := envOr("DATABASE_URL", defaultDSN)
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer db.Close()
	logger.Info("connected to database")

	// Producer.
	p, err := producer.New(producer.Config{
		Pool:           db,
		Logger:         logger.With("component", "producer"),
		MeterProvider:  otel.MeterProvider,
		TracerProvider: otel.TracerProvider,
	})
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}

	// Seed initial tasks.
	enqueued := seedTasks(ctx, p, logger)

	// Start continuous task generator.
	go generateTasks(ctx, p, logger)

	// Counters for final stats.
	var emailProcessed, notifProcessed, reportProcessed atomic.Int64

	// Consumer 1: leader, maintenance ON.
	c1, err := consumer.New(consumer.Config{
		Pool:               db,
		Logger:             logger.With("component", "consumer-1"),
		MeterProvider:      otel.MeterProvider,
		TracerProvider:     otel.TracerProvider,
		ClientID:           "consumer-1",
		FetchInterval:      200 * time.Millisecond,
		StuckThreshold:     2 * time.Minute,
		CompletedRetention: 1 * time.Hour,
		FailedRetention:    2 * time.Hour,
		CancelledRetention: 1 * time.Hour,
	})
	if err != nil {
		log.Fatalf("failed to create consumer-1: %v", err)
	}

	must(c1.RegisterTaskHandler("email.send",
		consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			return simulateWork(3000, 5, &emailProcessed)
		}),
		consumer.WithWorkersCount(5),
		consumer.WithTimeout(30*time.Second),
	))

	must(c1.RegisterTaskHandler("notification.push",
		consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			return simulateWork(1000, 2, &notifProcessed)
		}),
		consumer.WithWorkersCount(3),
		consumer.WithTimeout(15*time.Second),
	))

	// Consumer 2: follower, custom retry policy.
	c2, err := consumer.New(consumer.Config{
		Pool:               db,
		Logger:             logger.With("component", "consumer-2"),
		MeterProvider:      otel.MeterProvider,
		TracerProvider:     otel.TracerProvider,
		ClientID:           "consumer-2",
		DisableMaintenance: true,
		FetchInterval:      200 * time.Millisecond,
		RetryPolicy:        &asynqpg.ConstantRetryPolicy{Delay: 5 * time.Second},
	})
	if err != nil {
		log.Fatalf("failed to create consumer-2: %v", err)
	}

	must(c2.RegisterTaskHandler("report.generate",
		consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			return simulateWork(2000, 10, &reportProcessed)
		}),
		consumer.WithWorkersCount(3),
		consumer.WithTimeout(60*time.Second),
	))

	// Start consumers.
	must(c1.Start())
	must(c2.Start())

	// Web UI server.
	addr := envOr("ADDR", defaultAddr)
	uiHandler, err := buildUIHandler(db, logger)
	if err != nil {
		log.Fatalf("failed to create UI handler: %v", err)
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
			log.Fatalf("server error: %v", err)
		}
	}()

	// Determine auth mode for banner.
	authMode := "No auth"
	if os.Getenv("GITHUB_CLIENT_ID") != "" && os.Getenv("GITHUB_CLIENT_SECRET") != "" {
		authMode = "GitHub OAuth"
	}

	fmt.Println()
	fmt.Println("=========================================")
	fmt.Println("  asynqpg Full-Stack Demo")
	fmt.Println("=========================================")
	fmt.Println()
	fmt.Printf("  Web UI:       http://localhost%s\n", addr)
	fmt.Println("  Jaeger UI:    http://localhost:16686")
	fmt.Println("  Grafana:      http://localhost:3000")
	fmt.Println("  Prometheus:   http://localhost:9090")
	fmt.Println()
	fmt.Println("  Consumers:    2 (consumer-1: leader, consumer-2: follower)")
	fmt.Println("  Task types:   email.send, notification.push, report.generate")
	fmt.Printf("  Auth:         %s\n", authMode)
	fmt.Printf("  Seeds:        %d tasks enqueued\n", enqueued)
	fmt.Println()
	fmt.Println("  Tasks are continuously generated every 3s.")
	fmt.Println("  Press Ctrl+C to stop.")
	fmt.Println("=========================================")
	fmt.Println()

	// Wait for shutdown signal.
	<-ctx.Done()
	logger.Info("shutting down...")

	// Graceful shutdown.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	_ = c1.Stop()
	_ = c2.Stop()

	logger.Info("demo finished",
		"email_processed", emailProcessed.Load(),
		"notification_processed", notifProcessed.Load(),
		"report_processed", reportProcessed.Load(),
	)
}

func buildUIHandler(db *sqlx.DB, logger *slog.Logger) (http.Handler, error) {
	opts := ui.HandlerOpts{
		Pool:   db,
		Logger: logger.With("component", "ui"),
	}

	clientID := os.Getenv("GITHUB_CLIENT_ID")
	clientSecret := os.Getenv("GITHUB_CLIENT_SECRET")

	if clientID != "" && clientSecret != "" {
		logger.Info("GitHub OAuth enabled")
		opts.AuthProviders = []ui.AuthProvider{
			NewGitHubAuthProvider(clientID, clientSecret),
		}
	} else {
		logger.Info("running without authentication (set GITHUB_CLIENT_ID and GITHUB_CLIENT_SECRET to enable OAuth)")
	}

	return ui.NewHandler(opts)
}

func seedTasks(ctx context.Context, p *producer.Producer, logger *slog.Logger) int {
	var total int

	// Batch: 50 email tasks.
	emailTasks := make([]*asynqpg.Task, 50)
	for i := range emailTasks {
		emailTasks[i] = asynqpg.NewTask("email.send",
			[]byte(fmt.Sprintf(`{"to":"user%d@example.com","subject":"Hello #%d"}`, i, i)),
			asynqpg.WithIdempotencyToken("seed:email.send:"+uuid.New().String()),
		)
	}
	ids, err := p.EnqueueMany(ctx, emailTasks)
	if err != nil {
		logger.Error("failed to enqueue email tasks", "error", err)
	} else {
		total += len(ids)
		logger.Info("seeded email tasks", "count", len(ids))
	}

	// Batch: 30 notification tasks (some with delay).
	notifTasks := make([]*asynqpg.Task, 30)
	for i := range notifTasks {
		opts := []asynqpg.TaskOption{
			asynqpg.WithIdempotencyToken("seed:notification.push:" + uuid.New().String()),
		}
		if i%5 == 0 {
			opts = append(opts, asynqpg.WithDelay(time.Duration(i)*100*time.Millisecond))
		}
		notifTasks[i] = asynqpg.NewTask("notification.push",
			[]byte(fmt.Sprintf(`{"user_id":%d,"message":"notification #%d"}`, i, i)),
			opts...,
		)
	}
	ids, err = p.EnqueueMany(ctx, notifTasks)
	if err != nil {
		logger.Error("failed to enqueue notification tasks", "error", err)
	} else {
		total += len(ids)
		logger.Info("seeded notification tasks", "count", len(ids))
	}

	// Individual: 20 report tasks.
	for i := 0; i < 20; i++ {
		task := asynqpg.NewTask("report.generate",
			[]byte(fmt.Sprintf(`{"report_id":%d,"type":"monthly"}`, i)),
			asynqpg.WithMaxRetry(2),
			asynqpg.WithIdempotencyToken("seed:report.generate:"+uuid.New().String()),
		)
		if _, err := p.Enqueue(ctx, task); err != nil {
			logger.Error("failed to enqueue report task", "index", i, "error", err)
		} else {
			total++
		}
	}
	logger.Info("seeded report tasks", "count", 20)

	return total
}

func generateTasks(ctx context.Context, p *producer.Producer, logger *slog.Logger) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Batch: 5 email tasks with idempotency tokens.
			emailTasks := make([]*asynqpg.Task, 5)
			for i := range emailTasks {
				emailTasks[i] = asynqpg.NewTask("email.send",
					[]byte(fmt.Sprintf(`{"to":"gen-%s@example.com","subject":"Generated"}`, uuid.New().String()[:8])),
					asynqpg.WithIdempotencyToken("gen:email.send:"+uuid.New().String()),
				)
			}
			if _, err := p.EnqueueMany(ctx, emailTasks); err != nil {
				logger.Error("failed to enqueue generated emails", "error", err)
			}

			// Delayed notification (10s delay).
			delayedNotif := asynqpg.NewTask("notification.push",
				[]byte(`{"user_id":0,"message":"delayed notification"}`),
				asynqpg.WithIdempotencyToken("gen:notification.push:"+uuid.New().String()),
				asynqpg.WithDelay(10*time.Second),
			)
			if _, err := p.Enqueue(ctx, delayedNotif); err != nil {
				logger.Error("failed to enqueue delayed notification", "error", err)
			}

			// Scheduled report (ProcessAt now + 30s).
			scheduledReport := asynqpg.NewTask("report.generate",
				[]byte(`{"report_id":0,"type":"scheduled"}`),
				asynqpg.WithIdempotencyToken("gen:report.generate:"+uuid.New().String()),
			)
			scheduledReport.ProcessAt = time.Now().Add(30 * time.Second)
			if _, err := p.Enqueue(ctx, scheduledReport); err != nil {
				logger.Error("failed to enqueue scheduled report", "error", err)
			}

			// Notification with custom retry limit.
			retryNotif := asynqpg.NewTask("notification.push",
				[]byte(`{"user_id":0,"message":"high-retry notification"}`),
				asynqpg.WithIdempotencyToken("gen:notification.push:retry:"+uuid.New().String()),
				asynqpg.WithMaxRetry(5),
			)
			if _, err := p.Enqueue(ctx, retryNotif); err != nil {
				logger.Error("failed to enqueue retry notification", "error", err)
			}
		}
	}
}

// simulateWork sleeps for baseMs +-20% and returns an error with errorPct probability.
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

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

// loadEnvFile reads a .env file and sets environment variables that are not already set.
func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
