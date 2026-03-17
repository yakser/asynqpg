//go:build integration

package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

type mockTaskHandler struct {
	handleFunc func(ctx context.Context, task *asynqpg.TaskInfo) error
	callCount  int32
}

func (m *mockTaskHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
	atomic.AddInt32(&m.callCount, 1)
	if m.handleFunc != nil {
		return m.handleFunc(ctx, task)
	}
	return nil
}

func (m *mockTaskHandler) CallCount() int {
	return int(atomic.LoadInt32(&m.callCount))
}

func TestNew(t *testing.T) {
	t.Run("successfully creates consumer with valid config", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:   db,
			Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
		})

		require.NoError(t, err)
		require.NotNil(t, c)
	})

	t.Run("applies custom configuration", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:                db,
			ShutdownTimeout:     10 * time.Second,
			FetchInterval:       50 * time.Millisecond,
			JanitorInterval:     15 * time.Second,
			StuckThreshold:      2 * time.Minute,
			CompletedRetention:  12 * time.Hour,
			DefaultWorkersCount: 5,
			DefaultMaxAttempts:  2,
		})

		require.NoError(t, err)
		require.NotNil(t, c)
	})

	t.Run("returns error when Pool is nil", func(t *testing.T) {
		c, err := consumer.New(consumer.Config{
			Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
		})

		require.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "database pool is required")
	})
}

func TestRegisterTaskHandler(t *testing.T) {
	t.Run("successfully registers task handler", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)

		require.NoError(t, err)
	})

	t.Run("applies custom options", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler(
			"test-task",
			handler,
			consumer.WithWorkersCount(20),
			consumer.WithMaxAttempts(5),
		)

		require.NoError(t, err)
	})

	t.Run("returns error when handler already registered", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.RegisterTaskHandler("test-task", handler)
		require.Error(t, err)
		assert.Equal(t, consumer.ErrTaskHandlerAlreadyRegistered, err)
	})

	t.Run("returns error when registering after start", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)
		defer c.Stop()

		err = c.RegisterTaskHandler("another-task", handler)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot register handler after consumer is started")
	})
}

func TestStartStop(t *testing.T) {
	t.Run("successfully starts and stops consumer", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		err = c.Stop()
		require.NoError(t, err)
	})

	t.Run("returns error when starting without handlers", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		err = c.Start()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no task handlers registered")
	})

	t.Run("returns error when starting already started consumer", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)
		defer c.Stop()

		err = c.Start()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "consumer is already started")
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		err = c.Stop()
		require.NoError(t, err)

		err = c.Stop()
		require.NoError(t, err)
	})
}

func TestGracefulShutdown(t *testing.T) {
	t.Run("shuts down within timeout", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:                db,
			ShutdownTimeout:     5 * time.Second,
			FetchInterval:       50 * time.Millisecond,
			JanitorInterval:     1 * time.Second,
			DefaultWorkersCount: 10,
		})
		require.NoError(t, err)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
				Type:             "test-task",
				Payload:          []byte("{}"),
				IdempotencyToken: nil,
				Delay:            0,
				MaxRetry:         ptr.Get(3),
				ProcessAt:        time.Time{},
			})
			require.NoError(t, err)
		}

		handler := &mockTaskHandler{}
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return handler.CallCount() == 10
		}, 1*time.Second, 50*time.Millisecond, "should start 10 tasks")

		start := time.Now()
		err = c.Shutdown(2 * time.Second)
		duration := time.Since(start)
		require.NoError(t, err)
		assert.Less(t, duration, 2*time.Second)
	})

	t.Run("returns error when shutdown times out", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
				Type:             "test-task",
				Payload:          []byte("{}"),
				IdempotencyToken: nil,
				Delay:            0,
				MaxRetry:         ptr.Get(3),
				ProcessAt:        time.Time{},
			})
			require.NoError(t, err)
		}

		started := make(chan struct{})
		blockingChan := make(chan struct{})
		blockingHandler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				select {
				case started <- struct{}{}:
				default:
				}
				<-blockingChan
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:                db,
			FetchInterval:       50 * time.Millisecond,
			JanitorInterval:     10 * time.Second,
			DefaultWorkersCount: 5,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler("test-task", blockingHandler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		<-started

		err = c.Shutdown(100 * time.Millisecond)
		close(blockingChan)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "shutdown timeout exceeded")
	})
}

func TestTaskProcessing(t *testing.T) {
	t.Run("processes tasks successfully", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "success_task_" + t.Name()
		var processedCount int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&processedCount, 1)
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler)
		require.NoError(t, err)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		for i := 0; i < 2; i++ {
			_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
				Type:             taskType,
				Payload:          []byte("{}"),
				IdempotencyToken: nil,
				Delay:            0,
				MaxRetry:         ptr.Get(3),
				ProcessAt:        time.Time{},
			})
			require.NoError(t, err)
		}

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processedCount) == 2
		}, 1*time.Second, 50*time.Millisecond, "should process 2 tasks")

		err = c.Stop()
		require.NoError(t, err)
	})

	t.Run("handles task processing errors", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "error_task_" + t.Name()
		expectedErr := errors.New("processing error")
		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				return expectedErr
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler)
		require.NoError(t, err)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
			Type:             taskType,
			Payload:          []byte("{}"),
			IdempotencyToken: nil,
			Delay:            0,
			MaxRetry:         ptr.Get(3),
			ProcessAt:        time.Time{},
		})
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return handler.CallCount() == 1
		}, 1*time.Second, 50*time.Millisecond, "handler should be called once")

		err = c.Stop()
		require.NoError(t, err)
	})
}

func TestConcurrency(t *testing.T) {
	t.Run("processes tasks concurrently", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "concurrent_task_" + t.Name()
		var concurrent int32
		var maxConcurrent int32
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				curr := atomic.AddInt32(&concurrent, 1)
				defer atomic.AddInt32(&concurrent, -1)
				defer atomic.AddInt32(&processed, 1)
				for {
					maxConc := atomic.LoadInt32(&maxConcurrent)
					if curr <= maxConc || atomic.CompareAndSwapInt32(&maxConcurrent, maxConc, curr) {
						break
					}
				}

				time.Sleep(100 * time.Millisecond)
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:                db,
			FetchInterval:       30 * time.Millisecond,
			JanitorInterval:     10 * time.Second,
			DefaultWorkersCount: 3,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler)
		require.NoError(t, err)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		for i := 0; i < 6; i++ {
			_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
				Type:             taskType,
				Payload:          []byte("{}"),
				IdempotencyToken: nil,
				Delay:            0,
				MaxRetry:         ptr.Get(3),
				ProcessAt:        time.Time{},
			})
			require.NoError(t, err)
		}

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) >= 3
		}, 1*time.Second, 100*time.Millisecond, "should process at least 3 tasks")

		err = c.Stop()
		require.NoError(t, err)

		maxConcurrentTasks := atomic.LoadInt32(&maxConcurrent)
		assert.Equal(t, maxConcurrentTasks, int32(3), "should process tasks concurrently")
	})

	t.Run("handles multiple task types concurrently", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		emailType := "email_multi_" + t.Name()
		smsType := "sms_multi_" + t.Name()
		var emailCount, smsCount int32

		emailHandler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&emailCount, 1)
				return nil
			},
		}

		smsHandler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&smsCount, 1)
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(emailType, emailHandler)
		require.NoError(t, err)

		err = c.RegisterTaskHandler(smsType, smsHandler)
		require.NoError(t, err)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
				Type:             emailType,
				Payload:          []byte("{}"),
				IdempotencyToken: nil,
				Delay:            0,
				MaxRetry:         ptr.Get(3),
				ProcessAt:        time.Time{},
			})
			require.NoError(t, err)
			_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
				Type:             smsType,
				Payload:          []byte("{}"),
				IdempotencyToken: nil,
				Delay:            0,
				MaxRetry:         ptr.Get(3),
				ProcessAt:        time.Time{},
			})
			require.NoError(t, err)
		}

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			emails := atomic.LoadInt32(&emailCount)
			sms := atomic.LoadInt32(&smsCount)
			return emails > 0 && sms > 0
		}, 1*time.Second, 100*time.Millisecond, "should process tasks of both types")

		err = c.Stop()
		require.NoError(t, err)
	})
}

func TestTaskLockDuringExecution(t *testing.T) {
	t.Run("prevents other consumers from picking up locked task", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "lock_test_" + t.Name()
		var consumer1Calls, consumer2Calls int32
		taskStarted := make(chan struct{})
		taskCanFinish := make(chan struct{})

		// Consumer 1 will hold the task for a while
		handler1 := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&consumer1Calls, 1)
				close(taskStarted)
				<-taskCanFinish
				return nil
			},
		}

		// Consumer 2 should not be able to pick up the same task
		handler2 := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&consumer2Calls, 1)
				return nil
			},
		}

		// Create two consumers with short timeout to speed up test
		timeout := 1 * time.Second
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		c1, err := consumer.New(consumer.Config{
			Pool:            db,
			Logger:          logger,
			FetchInterval:   50 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		c2, err := consumer.New(consumer.Config{
			Pool:            db,
			Logger:          logger,
			FetchInterval:   50 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c1.RegisterTaskHandler(taskType, handler1, consumer.WithTimeout(timeout))
		require.NoError(t, err)

		err = c2.RegisterTaskHandler(taskType, handler2, consumer.WithTimeout(timeout))
		require.NoError(t, err)

		// Insert one task BEFORE starting consumers
		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		const idempotencyToken = "task-idempotency-token-1"
		_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
			Type:             taskType,
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(idempotencyToken),
			Delay:            0,
			MaxRetry:         ptr.Get(3),
			ProcessAt:        time.Time{},
		})
		require.NoError(t, err)

		// Verify task is in DB
		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM asynqpg_tasks WHERE idempotency_token = $1 AND status = 'pending'", idempotencyToken)
		require.NoError(t, err)
		require.Equal(t, 1, count, "Task should be in pending status")

		// Start both consumers
		err = c1.Start()
		require.NoError(t, err)
		defer c1.Stop()

		// Wait for consumer1 to pick up the task
		select {
		case <-taskStarted:
		case <-time.After(1 * time.Second):
			// Debug: check task status
			var status string
			var blockedTill time.Time
			err = db.QueryRow("SELECT status, blocked_till FROM asynqpg_tasks WHERE idempotency_token = $1", idempotencyToken).Scan(&status, &blockedTill)
			if err != nil {
				t.Logf("Error getting task status: %v", err)
			} else {
				t.Logf("Task status: %s, blocked_till: %v, now: %v", status, blockedTill, time.Now())
			}
			t.Fatal("task was not picked up by consumer1")
		}

		err = c2.Start()
		require.NoError(t, err)
		defer c2.Stop()

		// Give consumer2 some time to potentially pick up the task (it shouldn't)
		time.Sleep(100 * time.Millisecond)

		// At this point, only consumer1 should have processed the task
		assert.Equal(t, int32(1), atomic.LoadInt32(&consumer1Calls), "consumer1 should process the task once")
		assert.Equal(t, int32(0), atomic.LoadInt32(&consumer2Calls), "consumer2 should not process the locked task")

		// Release the task
		close(taskCanFinish)

		// Wait for everything to settle
		time.Sleep(100 * time.Millisecond)

		// Still, only consumer1 should have processed it
		assert.Equal(t, int32(1), atomic.LoadInt32(&consumer1Calls))
		assert.Equal(t, int32(0), atomic.LoadInt32(&consumer2Calls))
	})

	t.Run("task becomes available after lock expires", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "lock_expiry_test_" + t.Name()
		var consumer1Started, consumer2Started atomic.Bool
		var taskProcessingCount int32

		// Shared handler that tracks which consumer picked up the task
		createHandler := func(consumerName string, isConsumer1 bool) *mockTaskHandler {
			return &mockTaskHandler{
				handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
					count := atomic.AddInt32(&taskProcessingCount, 1)
					if isConsumer1 {
						consumer1Started.Store(true)
						t.Logf("[%s] Started processing (attempt #%d)", consumerName, count)
						// Simulate task taking longer than lock duration
						time.Sleep(7 * time.Second)
					} else {
						consumer2Started.Store(true)
						t.Logf("[%s] Started processing (attempt #%d)", consumerName, count)
						// Process quickly
						time.Sleep(100 * time.Millisecond)
					}
					return nil
				},
			}
		}

		handler1 := createHandler("Consumer1", true)
		handler2 := createHandler("Consumer2", false)

		// Create consumers with very short timeout so lock expires quickly
		timeout := 1 * time.Second
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

		c1, err := consumer.New(consumer.Config{
			Pool:            db,
			Logger:          logger.With("consumer", "c1"),
			FetchInterval:   100 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		c2, err := consumer.New(consumer.Config{
			Pool:            db,
			Logger:          logger.With("consumer", "c2"),
			FetchInterval:   100 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c1.RegisterTaskHandler(taskType, handler1, consumer.WithTimeout(timeout), consumer.WithWorkersCount(1))
		require.NoError(t, err)

		err = c2.RegisterTaskHandler(taskType, handler2, consumer.WithTimeout(timeout), consumer.WithWorkersCount(1))
		require.NoError(t, err)

		// Insert one task with multiple retry attempts
		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		const idempotencyToken = "task-idempotency-token-1"
		_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
			Type:             taskType,
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(idempotencyToken),
			Delay:            0,
			MaxRetry:         ptr.Get(5),
			ProcessAt:        time.Time{},
		})
		require.NoError(t, err)

		// Start consumer1 first
		err = c1.Start()
		require.NoError(t, err)
		defer c1.Stop()

		// Wait for consumer1 to pick up the task
		assert.Eventually(t, func() bool {
			return consumer1Started.Load()
		}, 1*time.Second, 20*time.Millisecond, "consumer1 should pick up the task")

		// Verify task is in DB
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		t.Logf("Inserted task: %+v", task)
		require.Equal(t, idempotencyToken, *task.IdempotencyToken, "task idempotency token should be equal")

		// Now start consumer2
		err = c2.Start()
		require.NoError(t, err)
		defer c2.Stop()

		// Lock duration = timeout + lockMargin = 1s + 5s = 6s
		// Consumer1 sleeps for 3s, so task will still be locked
		// After lock expires (6s from pickup), consumer2 should be able to pick it up
		t.Logf("Waiting for lock to expire and consumer2 to pick up task...")

		assert.Eventually(t, func() bool {
			//task, err := testutils.GetTaskByID(t, db, taskID)
			//task.Payload = nil
			//require.NoError(t, err)
			return consumer2Started.Load()
		}, 7*time.Second, 20*time.Millisecond, "consumer2 should pick up the task after lock expires")

		// Both consumers should have processed the task
		assert.True(t, consumer1Started.Load(), "consumer1 should have started")
		assert.True(t, consumer2Started.Load(), "consumer2 should have started after lock expired")
		assert.Equal(t, atomic.LoadInt32(&taskProcessingCount), int32(2),
			"task should be taken twice (by both consumers)")
		//  msg="fetch ready tasks" consumer=c2 now=2026-01-24 12:15:24.605
		//  BlockedTill:						    2026-01-24 12:15:23.657684
	})

	t.Run("task context cancelled after timeout", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "timeout_test_" + t.Name()
		var taskStarted, taskCancelled atomic.Bool
		taskStartedChan := make(chan struct{})

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				taskStarted.Store(true)
				close(taskStartedChan)

				// Wait for context cancellation
				<-ctx.Done()

				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					taskCancelled.Store(true)
					return ctx.Err()
				}

				return fmt.Errorf("unexpected context error: %v", ctx.Err())
			},
		}

		timeout := 500 * time.Millisecond
		c, err := consumer.New(consumer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			FetchInterval:   50 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithTimeout(timeout))
		require.NoError(t, err)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		const idempotencyToken = "task-idempotency-token-1"
		_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
			Type:             taskType,
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(idempotencyToken),
			Delay:            0,
			MaxRetry:         ptr.Get(5),
			ProcessAt:        time.Time{},
		})
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)
		defer c.Stop()

		// Wait for task to start
		select {
		case <-taskStartedChan:
		case <-time.After(2 * time.Second):
			t.Fatal("task was not picked up")
		}

		// Wait for timeout to trigger
		assert.Eventually(t, func() bool {
			return taskCancelled.Load()
		}, 1*time.Second, 100*time.Millisecond, "task context should be cancelled after timeout")

		assert.True(t, taskStarted.Load(), "task should have started")
		assert.True(t, taskCancelled.Load(), "task context should have been cancelled due to timeout")
	})

	t.Run("proper lock duration based on task timeout", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "lock_duration_test_" + t.Name()
		var processedCount int32
		taskStarted := make(chan struct{}, 1)
		processingDone := make(chan struct{})

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := atomic.AddInt32(&processedCount, 1)
				if count == 1 {
					select {
					case taskStarted <- struct{}{}:
					default:
					}
					// Simulate task taking time but within timeout
					time.Sleep(1500 * time.Millisecond)
				}
				return nil
			},
		}

		// Timeout of 2 seconds should be enough
		timeout := 2 * time.Second
		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   50 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithTimeout(timeout))
		require.NoError(t, err)

		// Insert one task
		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		const idempotencyToken = "task-idempotency-token-1"
		_, err = producer.Enqueue(t.Context(), &asynqpg.Task{
			Type:             taskType,
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(idempotencyToken),
			Delay:            0,
			MaxRetry:         ptr.Get(5),
			ProcessAt:        time.Time{},
		})
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		// Wait for task to start processing
		select {
		case <-taskStarted:
		case <-time.After(5 * time.Second):
			t.Fatal("task was not picked up")
		}

		// Wait for task to finish naturally
		go func() {
			// Task sleeps 1.5s, give it 3s to complete
			time.Sleep(3 * time.Second)
			close(processingDone)
		}()

		select {
		case <-processingDone:
		case <-time.After(5 * time.Second):
			t.Fatal("task processing took too long")
		}

		err = c.Stop()
		require.NoError(t, err)

		// Task should be processed exactly once (no re-picking due to proper lock)
		assert.Equal(t, int32(1), atomic.LoadInt32(&processedCount),
			"task should be processed exactly once with proper lock duration")
	})
}

func TestContextUtilities(t *testing.T) {
	t.Run("provides task metadata via context", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "ctx_utils_task_" + t.Name()
		maxRetry := 5

		var (
			gotID         int64
			gotRetryCount int
			gotMaxRetry   int
			gotIDOk       bool
			gotRetryOk    bool
			gotMaxOk      bool
			processed     int32
		)

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				gotID, gotIDOk = asynqpg.GetTaskID(ctx)
				gotRetryCount, gotRetryOk = asynqpg.GetRetryCount(ctx)
				gotMaxRetry, gotMaxOk = asynqpg.GetMaxRetry(ctx)
				atomic.StoreInt32(&processed, 1)
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`), asynqpg.WithMaxRetry(maxRetry)))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) == 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed")

		err = c.Stop()
		require.NoError(t, err)

		assert.True(t, gotIDOk, "GetTaskID should return ok=true")
		assert.Greater(t, gotID, int64(0), "task ID should be a positive database ID")

		assert.True(t, gotRetryOk, "GetRetryCount should return ok=true")
		assert.Equal(t, 0, gotRetryCount, "retry count should be 0 on first attempt")

		assert.True(t, gotMaxOk, "GetMaxRetry should return ok=true")
		assert.Equal(t, maxRetry, gotMaxRetry, "max retry should match configured value")
	})
}

func TestTaskSnooze(t *testing.T) {
	t.Run("reschedules task without counting attempt", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_task_" + t.Name()
		idempotencyToken := "snooze-token-1"
		maxRetry := 5
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := atomic.AddInt32(&processed, 1)
				if count == 1 {
					return asynqpg.TaskSnooze(100 * time.Millisecond)
				}
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(maxRetry),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		// Handler should be called twice: first returns snooze, second succeeds
		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) >= 2
		}, 2*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		// Give consumer time to persist the result
		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		// Verify task completed and attempts_elapsed was NOT incremented by snooze
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status, "task should be completed")
		// attempts_elapsed should be 0 because snooze doesn't count and the second call succeeded
		assert.Equal(t, 0, task.AttemptsElapsed, "attempts_elapsed should be 0 (snooze doesn't count)")
		assert.Equal(t, maxRetry, task.AttemptsLeft, "attempts_left should be unchanged")
	})

	t.Run("wrapped snooze error detected", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_wrapped_" + t.Name()
		idempotencyToken := "snooze-wrapped-token-1"
		maxRetry := 5
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := atomic.AddInt32(&processed, 1)
				if count == 1 {
					return fmt.Errorf("not ready yet: %w", asynqpg.TaskSnooze(100*time.Millisecond))
				}
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(maxRetry),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) >= 2
		}, 2*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status, "task should be completed after wrapped snooze")
	})
}

func TestTaskSnoozeWithError(t *testing.T) {
	t.Run("reschedules with error and counts attempt", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_err_task_" + t.Name()
		idempotencyToken := "snooze-err-token-1"
		maxRetry := 5
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := atomic.AddInt32(&processed, 1)
				if count == 1 {
					return asynqpg.TaskSnoozeWithError(100 * time.Millisecond)
				}
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(maxRetry),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) >= 2
		}, 2*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status, "task should be completed")
		// attempts_elapsed should be 1 because snooze with error counts as attempt
		assert.Equal(t, 1, task.AttemptsElapsed, "attempts_elapsed should be 1 (snooze with error counts)")
		assert.Equal(t, maxRetry-1, task.AttemptsLeft, "attempts_left should be decremented by 1")
		assert.NotEmpty(t, task.Messages, "error message should be stored")
	})

	t.Run("fails when no attempts left", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_err_exhaust_" + t.Name()
		idempotencyToken := "snooze-err-exhaust-token-1"
		maxRetry := 1
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&processed, 1)
				return asynqpg.TaskSnoozeWithError(100 * time.Millisecond)
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(maxRetry),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) >= 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed")

		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		// With maxRetry=1, the task has attempts_left=1, after processing attemptsLeft becomes 0
		// So snooze with error should fail the task instead of snoozing
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status, "task should be failed when no attempts left")
		assert.Equal(t, int32(1), atomic.LoadInt32(&processed), "handler should be called exactly once")
	})
}

func TestProcessTask_PopulatesRuntimeFields(t *testing.T) {
	t.Run("populates CreatedAt, Messages, AttemptedAt on first attempt", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "runtime_fields_" + t.Name()
		var (
			gotCreatedAt   time.Time
			gotMessages    []string
			gotAttemptedAt *time.Time
			gotCtxCreated  time.Time
			gotCtxOk       bool
			processed      int32
		)

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				gotCreatedAt = task.CreatedAt
				gotMessages = task.Messages
				gotAttemptedAt = task.AttemptedAt
				gotCtxCreated, gotCtxOk = asynqpg.GetCreatedAt(ctx)
				atomic.StoreInt32(&processed, 1)
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(3))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		beforeEnqueue := time.Now().Add(-1 * time.Second)
		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{"key":"value"}`), asynqpg.WithMaxRetry(3)))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) == 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed")

		err = c.Stop()
		require.NoError(t, err)

		// CreatedAt should be set and roughly around enqueue time
		assert.False(t, gotCreatedAt.IsZero(), "CreatedAt should be non-zero")
		assert.True(t, gotCreatedAt.After(beforeEnqueue), "CreatedAt should be after test start")

		// Messages should be empty on first attempt
		assert.Empty(t, gotMessages, "Messages should be empty on first attempt")

		// AttemptedAt should be non-nil
		assert.NotNil(t, gotAttemptedAt, "AttemptedAt should be non-nil")
		assert.False(t, gotAttemptedAt.IsZero(), "AttemptedAt should be non-zero")

		// Context CreatedAt should match Task CreatedAt
		assert.True(t, gotCtxOk, "GetCreatedAt should return ok=true")
		assert.Equal(t, gotCreatedAt, gotCtxCreated, "context CreatedAt should match task CreatedAt")
	})
}

func TestProcessTask_MessagesPopulatedOnRetry(t *testing.T) {
	t.Run("Messages contains error from previous attempt", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "retry_messages_" + t.Name()
		idempotencyToken := "retry-msg-token-1"
		var (
			gotMessages []string
			processed   int32
		)

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := atomic.AddInt32(&processed, 1)
				if count == 1 {
					return fmt.Errorf("first attempt failed")
				}
				gotMessages = task.Messages
				return nil
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(3))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(3),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) >= 2
		}, 10*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		// On second attempt, Messages should contain the error from the first attempt
		require.Len(t, gotMessages, 1, "Messages should contain 1 error from previous attempt")
		assert.Equal(t, "first attempt failed", gotMessages[0])
	})
}

func TestSkipRetry(t *testing.T) {
	t.Run("immediately fails task when handler returns SkipRetry", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "skip_retry_task_" + t.Name()
		idempotencyToken := "skip-retry-token-1"
		maxRetry := 5
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&processed, 1)
				return asynqpg.ErrSkipRetry
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(maxRetry),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) == 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed once")

		// Give consumer time to persist the result
		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		// Verify task is in failed status after only 1 attempt
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status, "task should be in failed status")
		assert.Equal(t, int32(1), atomic.LoadInt32(&processed), "handler should be called exactly once")
	})

	t.Run("immediately fails task when handler returns wrapped SkipRetry", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "skip_retry_wrapped_" + t.Name()
		idempotencyToken := "skip-retry-wrapped-token-1"
		maxRetry := 5
		var processed int32

		handler := &mockTaskHandler{
			handleFunc: func(ctx context.Context, task *asynqpg.TaskInfo) error {
				atomic.AddInt32(&processed, 1)
				return fmt.Errorf("invalid payload: %w", asynqpg.ErrSkipRetry)
			},
		}

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler(taskType, handler, consumer.WithMaxAttempts(maxRetry))
		require.NoError(t, err)

		p, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: maxRetry,
		})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), asynqpg.NewTask(taskType, []byte(`{}`),
			asynqpg.WithMaxRetry(maxRetry),
			asynqpg.WithIdempotencyToken(idempotencyToken),
		))
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return atomic.LoadInt32(&processed) == 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed once")

		// Give consumer time to persist the result
		time.Sleep(200 * time.Millisecond)

		err = c.Stop()
		require.NoError(t, err)

		// Verify task is in failed status after only 1 attempt
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status, "task should be in failed status")
	})
}
