//go:build integration

package consumer_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/consumer/mocks"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("successfully creates consumer with valid config", func(t *testing.T) {
		t.Parallel()
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:   db,
			Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
		})

		require.NoError(t, err)
		require.NotNil(t, c)
	})

	t.Run("applies custom configuration", func(t *testing.T) {
		t.Parallel()
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
		t.Parallel()

		c, err := consumer.New(consumer.Config{
			Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)),
		})

		require.Error(t, err)
		assert.Nil(t, c)
		assert.Contains(t, err.Error(), "database pool is required")
	})
}

func TestRegisterTaskHandler(t *testing.T) {
	t.Parallel()

	t.Run("successfully registers task handler", func(t *testing.T) {
		t.Parallel()
		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		err = c.RegisterTaskHandler("test-task", handler)

		require.NoError(t, err)
	})

	t.Run("applies custom options", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		err = c.RegisterTaskHandler(
			"test-task",
			handler,
			consumer.WithWorkersCount(20),
			consumer.WithMaxAttempts(5),
		)

		require.NoError(t, err)
	})

	t.Run("returns error when handler already registered", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.RegisterTaskHandler("test-task", handler)
		require.Error(t, err)
		assert.Equal(t, consumer.ErrTaskHandlerAlreadyRegistered, err)
	})

	t.Run("returns error when registering after start", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).Return(nil).Maybe()
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
	t.Parallel()

	t.Run("successfully starts and stops consumer", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).Return(nil).Maybe()
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		err = c.Stop()
		require.NoError(t, err)
	})

	t.Run("returns error when starting without handlers", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{Pool: db})
		require.NoError(t, err)

		err = c.Start()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no task handlers registered")
	})

	t.Run("returns error when starting already started consumer", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).Return(nil).Maybe()
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
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   500 * time.Millisecond,
			JanitorInterval: 1 * time.Second,
		})
		require.NoError(t, err)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).Return(nil).Maybe()
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
	t.Parallel()

	t.Run("shuts down within timeout", func(t *testing.T) {
		t.Parallel()

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

		for range 10 {
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

		var callCount atomic.Int32
		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				callCount.Add(1)
				return nil
			}).Maybe()
		err = c.RegisterTaskHandler("test-task", handler)
		require.NoError(t, err)

		err = c.Start()
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			return callCount.Load() == 10
		}, 1*time.Second, 50*time.Millisecond, "should start 10 tasks")

		start := time.Now()
		err = c.Shutdown(2 * time.Second)
		duration := time.Since(start)
		require.NoError(t, err)
		assert.Less(t, duration, 2*time.Second)
	})

	t.Run("returns error when shutdown times out", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		producer, err := producer.New(producer.Config{
			Pool:            db,
			Logger:          slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})),
			DefaultMaxRetry: 3,
		})
		require.NoError(t, err)

		for range 10 {
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

		// Preserve the signal even if the handler starts before the test waits.
		started := make(chan struct{}, 1)
		blockingChan := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(blockingChan) }) }
		blockingHandler := mocks.NewTaskHandler(t)
		blockingHandler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				select {
				case started <- struct{}{}:
				default:
				}
				<-blockingChan
				return nil
			}).Maybe()

		c, err := consumer.New(consumer.Config{
			Pool:                db,
			FetchInterval:       50 * time.Millisecond,
			JanitorInterval:     10 * time.Second,
			DefaultWorkersCount: 5,
		})
		require.NoError(t, err)

		err = c.RegisterTaskHandler("test-task", blockingHandler)
		require.NoError(t, err)

		t.Cleanup(func() {
			release()
			require.NoError(t, c.Stop())
		})

		err = c.Start()
		require.NoError(t, err)

		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not start")
		}

		err = c.Shutdown(100 * time.Millisecond)
		release()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "shutdown timeout exceeded")
	})
}

func TestTaskProcessing(t *testing.T) {
	t.Parallel()

	t.Run("processes tasks successfully", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "success_task_" + t.Name()
		var processedCount atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				processedCount.Add(1)
				return nil
			}).Maybe()

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

		for range 2 {
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
			return processedCount.Load() == 2
		}, 1*time.Second, 50*time.Millisecond, "should process 2 tasks")

		err = c.Stop()
		require.NoError(t, err)
	})

	t.Run("handles task processing errors", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "error_task_" + t.Name()
		expectedErr := errors.New("processing error")
		var callCount atomic.Int32
		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				callCount.Add(1)
				return expectedErr
			}).Maybe()

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
			return callCount.Load() == 1
		}, 1*time.Second, 50*time.Millisecond, "handler should be called once")

		err = c.Stop()
		require.NoError(t, err)
	})
}

func TestConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("processes tasks concurrently", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "concurrent_task_" + t.Name()
		var concurrent atomic.Int32
		var maxConcurrent atomic.Int32
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				curr := concurrent.Add(1)
				defer concurrent.Add(-1)
				defer processed.Add(1)
				for {
					maxConc := maxConcurrent.Load()
					if curr <= maxConc || maxConcurrent.CompareAndSwap(maxConc, curr) {
						break
					}
				}

				time.Sleep(100 * time.Millisecond)
				return nil
			}).Maybe()

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

		for range 6 {
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
			return processed.Load() >= 3
		}, 1*time.Second, 100*time.Millisecond, "should process at least 3 tasks")

		err = c.Stop()
		require.NoError(t, err)

		maxConcurrentTasks := maxConcurrent.Load()
		assert.Equal(t, maxConcurrentTasks, int32(3), "should process tasks concurrently")
	})

	t.Run("handles multiple task types concurrently", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		emailType := "email_multi_" + t.Name()
		smsType := "sms_multi_" + t.Name()
		var emailCount, smsCount atomic.Int32

		emailHandler := mocks.NewTaskHandler(t)
		emailHandler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				emailCount.Add(1)
				return nil
			}).Maybe()

		smsHandler := mocks.NewTaskHandler(t)
		smsHandler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				smsCount.Add(1)
				return nil
			}).Maybe()

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

		for range 10 {
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
			emails := emailCount.Load()
			sms := smsCount.Load()
			return emails > 0 && sms > 0
		}, 1*time.Second, 100*time.Millisecond, "should process tasks of both types")

		err = c.Stop()
		require.NoError(t, err)
	})
}

func TestTaskLockDuringExecution(t *testing.T) {
	t.Parallel()

	t.Run("prevents other consumers from picking up locked task", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "lock_test_" + t.Name()
		var consumer1Calls, consumer2Calls atomic.Int32
		taskStarted := make(chan struct{})
		taskCanFinish := make(chan struct{})

		// Consumer 1 will hold the task for a while
		handler1 := mocks.NewTaskHandler(t)
		handler1.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				consumer1Calls.Add(1)
				close(taskStarted)
				<-taskCanFinish
				return nil
			}).Maybe()

		// Consumer 2 should not be able to pick up the same task
		handler2 := mocks.NewTaskHandler(t)
		handler2.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				consumer2Calls.Add(1)
				return nil
			}).Maybe()

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

		// Consumer2 should not pick up the locked task
		require.Never(t, func() bool {
			return consumer2Calls.Load() > 0
		}, 100*time.Millisecond, 10*time.Millisecond, "consumer2 should not process the locked task")
		assert.Equal(t, int32(1), consumer1Calls.Load(), "consumer1 should process the task once")

		// Release the task
		close(taskCanFinish)

		// Wait for everything to settle
		time.Sleep(100 * time.Millisecond)

		// Still, only consumer1 should have processed it
		assert.Equal(t, int32(1), consumer1Calls.Load())
		assert.Equal(t, int32(0), consumer2Calls.Load())
	})

	t.Run("task becomes available after lock expires", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "lock_expiry_test_" + t.Name()
		var consumer1Started, consumer2Started atomic.Bool
		var taskProcessingCount atomic.Int32

		// Shared handler that tracks which consumer picked up the task
		createHandler := func(consumerName string, isConsumer1 bool) *mocks.TaskHandler {
			h := mocks.NewTaskHandler(t)
			h.EXPECT().Handle(mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
					count := taskProcessingCount.Add(1)
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
				}).Maybe()
			return h
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
		assert.Equal(t, taskProcessingCount.Load(), int32(2),
			"task should be taken twice (by both consumers)")
		//  msg="fetch ready tasks" consumer=c2 now=2026-01-24 12:15:24.605
		//  BlockedTill:						    2026-01-24 12:15:23.657684
	})

	t.Run("task context cancelled after timeout", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "timeout_test_" + t.Name()
		var taskStarted, taskCancelled atomic.Bool
		taskStartedChan := make(chan struct{})

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				taskStarted.Store(true)
				close(taskStartedChan)

				// Wait for context cancellation
				<-ctx.Done()

				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					taskCancelled.Store(true)
					return ctx.Err()
				}

				return fmt.Errorf("unexpected context error: %v", ctx.Err())
			}).Maybe()

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
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "lock_duration_test_" + t.Name()
		var processedCount atomic.Int32
		taskStarted := make(chan struct{}, 1)
		processingDone := make(chan struct{})

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := processedCount.Add(1)
				if count == 1 {
					select {
					case taskStarted <- struct{}{}:
					default:
					}
					// Simulate task taking time but within timeout
					time.Sleep(1500 * time.Millisecond)
				}
				return nil
			}).Maybe()

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
		assert.Equal(t, int32(1), processedCount.Load(),
			"task should be processed exactly once with proper lock duration")
	})
}

func TestContextUtilities(t *testing.T) {
	t.Parallel()

	t.Run("provides task metadata via context", func(t *testing.T) {
		t.Parallel()

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
			processed     atomic.Int32
		)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				gotID, gotIDOk = asynqpg.GetTaskID(ctx)
				gotRetryCount, gotRetryOk = asynqpg.GetRetryCount(ctx)
				gotMaxRetry, gotMaxOk = asynqpg.GetMaxRetry(ctx)
				processed.Store(1)
				return nil
			}).Maybe()

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
			return processed.Load() == 1
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
	t.Parallel()

	t.Run("reschedules task without counting attempt", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_task_" + t.Name()
		idempotencyToken := "snooze-token-1"
		maxRetry := 5
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := processed.Add(1)
				if count == 1 {
					return asynqpg.TaskSnooze(100 * time.Millisecond)
				}
				return nil
			}).Maybe()

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
			return processed.Load() >= 2
		}, 2*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		err = c.Stop()
		require.NoError(t, err)

		// Verify task completed and that the snooze itself did NOT count as an attempt:
		// attempts_elapsed reflects only the second (successful) run, and attempts_left
		// is left untouched.
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status, "task should be completed")
		assert.Equal(t, 1, task.AttemptsElapsed, "attempts_elapsed should be 1 (snooze does not count, only the successful run)")
		assert.Equal(t, maxRetry, task.AttemptsLeft, "attempts_left should be unchanged by the snooze")
		assert.Empty(t, task.Messages, "successful run should not append an error message")
	})

	t.Run("wrapped snooze error detected", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_wrapped_" + t.Name()
		idempotencyToken := "snooze-wrapped-token-1"
		maxRetry := 5
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := processed.Add(1)
				if count == 1 {
					return fmt.Errorf("not ready yet: %w", asynqpg.TaskSnooze(100*time.Millisecond))
				}
				return nil
			}).Maybe()

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
			return processed.Load() >= 2
		}, 2*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status, "task should be completed after wrapped snooze")
	})
}

func TestTaskSnoozeWithError(t *testing.T) {
	t.Parallel()

	t.Run("reschedules with error and counts attempt", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_err_task_" + t.Name()
		idempotencyToken := "snooze-err-token-1"
		maxRetry := 5
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := processed.Add(1)
				if count == 1 {
					return asynqpg.TaskSnoozeWithError(100 * time.Millisecond)
				}
				return nil
			}).Maybe()

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
			return processed.Load() >= 2
		}, 2*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "completed", task.Status, "task should be completed")
		// snooze-with-error counts as an attempt (retry path bumps elapsed),
		// then the successful run bumps elapsed again.
		assert.Equal(t, 2, task.AttemptsElapsed, "attempts_elapsed should be 2 (snooze-with-error + successful run)")
		assert.Equal(t, maxRetry-1, task.AttemptsLeft, "attempts_left should be decremented by 1 (only the snooze-with-error consumed a retry)")
		assert.NotEmpty(t, task.Messages, "error message should be stored")
	})

	t.Run("fails when no attempts left", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "snooze_err_exhaust_" + t.Name()
		idempotencyToken := "snooze-err-exhaust-token-1"
		maxRetry := 1
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				processed.Add(1)
				return asynqpg.TaskSnoozeWithError(100 * time.Millisecond)
			}).Maybe()

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
			return processed.Load() >= 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed")

		err = c.Stop()
		require.NoError(t, err)

		// With maxRetry=1, the task has attempts_left=1, after processing attemptsLeft becomes 0
		// So snooze with error should fail the task instead of snoozing.
		// Final state must be dead-letter (attempts_left=0) so the UI's needs-retry vs
		// dead-letter classification routes the task correctly.
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status, "task should be failed when no attempts left")
		assert.Equal(t, 0, task.AttemptsLeft, "attempts_left must be 0 in dead-letter state")
		assert.Equal(t, 1, task.AttemptsElapsed, "attempts_elapsed should count the single attempt")
		require.Len(t, task.Messages, 1, "messages should contain the snooze-with-error message")
		assert.Equal(t, int32(1), processed.Load(), "handler should be called exactly once")
	})
}

func TestProcessTask_PopulatesRuntimeFields(t *testing.T) {
	t.Parallel()

	t.Run("populates CreatedAt, Messages, AttemptedAt on first attempt", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		taskType := "runtime_fields_" + t.Name()
		var (
			gotCreatedAt   time.Time
			gotMessages    []string
			gotAttemptedAt *time.Time
			gotCtxCreated  time.Time
			gotCtxOk       bool
			processed      atomic.Int32
		)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				gotCreatedAt = task.CreatedAt
				gotMessages = task.Messages
				gotAttemptedAt = task.AttemptedAt
				gotCtxCreated, gotCtxOk = asynqpg.GetCreatedAt(ctx)
				processed.Store(1)
				return nil
			}).Maybe()

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
			return processed.Load() == 1
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
	t.Parallel()

	t.Run("Messages contains error from previous attempt", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "retry_messages_" + t.Name()
		idempotencyToken := "retry-msg-token-1"
		var (
			gotMessages []string
			processed   atomic.Int32
		)

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				count := processed.Add(1)
				if count == 1 {
					return fmt.Errorf("first attempt failed")
				}
				gotMessages = task.Messages
				return nil
			}).Maybe()

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
			return processed.Load() >= 2
		}, 10*time.Second, 50*time.Millisecond, "task should be processed at least twice")

		err = c.Stop()
		require.NoError(t, err)

		// On second attempt, Messages should contain the error from the first attempt
		require.Len(t, gotMessages, 1, "Messages should contain 1 error from previous attempt")
		assert.Equal(t, "first attempt failed", gotMessages[0])
	})
}

func TestSkipRetry(t *testing.T) {
	t.Parallel()

	t.Run("immediately fails task when handler returns SkipRetry", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "skip_retry_task_" + t.Name()
		idempotencyToken := "skip-retry-token-1"
		maxRetry := 5
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				processed.Add(1)
				return asynqpg.ErrSkipRetry
			}).Maybe()

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
			return processed.Load() == 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed once")

		err = c.Stop()
		require.NoError(t, err)

		// Verify task is in dead-letter state after only 1 attempt
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status, "task should be in failed status")
		assert.Equal(t, 0, task.AttemptsLeft, "attempts_left must be 0 so the task lands in dead-letter, not needs-retry")
		assert.Equal(t, 1, task.AttemptsElapsed, "attempts_elapsed should reflect the single attempt that ran")
		require.Len(t, task.Messages, 1, "messages should contain exactly one entry from the failed attempt")
		assert.Contains(t, task.Messages[0], asynqpg.ErrSkipRetry.Error(), "stored message should carry the SkipRetry sentinel")
		assert.Equal(t, int32(1), processed.Load(), "handler should be called exactly once")
	})

	t.Run("immediately fails task when handler returns wrapped SkipRetry", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "skip_retry_wrapped_" + t.Name()
		idempotencyToken := "skip-retry-wrapped-token-1"
		maxRetry := 5
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				processed.Add(1)
				return fmt.Errorf("invalid payload: %w", asynqpg.ErrSkipRetry)
			}).Maybe()

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
			return processed.Load() == 1
		}, 2*time.Second, 50*time.Millisecond, "task should be processed once")

		err = c.Stop()
		require.NoError(t, err)

		// Verify task is in dead-letter state after only 1 attempt (wrapped sentinel still detected)
		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)
		assert.Equal(t, "failed", task.Status, "task should be in failed status")
		assert.Equal(t, 0, task.AttemptsLeft, "attempts_left must be 0 so the task lands in dead-letter, not needs-retry")
		assert.Equal(t, 1, task.AttemptsElapsed, "attempts_elapsed should reflect the single attempt that ran")
		require.Len(t, task.Messages, 1, "messages should contain exactly one entry from the failed attempt")
		assert.Contains(t, task.Messages[0], "invalid payload", "stored message should preserve the wrapping context")
		assert.Contains(t, task.Messages[0], asynqpg.ErrSkipRetry.Error(), "stored message should still carry the wrapped sentinel")
	})
}

// TestRetryExhaustion locks the dead-letter terminal state down: a task whose
// handler always returns a regular error must end up at status='failed' with
// attempts_left=0 and attempts_elapsed=MaxRetry. Until the FailTasks SQL was
// fixed, attempts_left remained at 1 after the last failure, which silently
// routed every exhausted task into the UI's "needs retry" view instead of
// "dead-letter".
func TestRetryExhaustion(t *testing.T) {
	t.Parallel()

	t.Run("always-failing handler ends in dead-letter state with all errors recorded", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "retry_exhaustion_" + t.Name()
		idempotencyToken := "retry-exhaustion-token-1"
		const maxRetry = 3
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ *asynqpg.TaskInfo) error {
				attempt := processed.Add(1)
				return fmt.Errorf("simulated failure on attempt %d", attempt)
			}).Maybe()

		// ConstantRetryPolicy with a tight delay keeps the test under a second.
		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
			RetryPolicy:     &asynqpg.ConstantRetryPolicy{Delay: 30 * time.Millisecond},
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
			return processed.Load() == int32(maxRetry)
		}, 5*time.Second, 50*time.Millisecond, "handler should run exactly maxRetry times")

		// Wait for the batch completer to flush the terminal failure to the DB.
		assert.Eventually(t, func() bool {
			task, getErr := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
			return getErr == nil && task.Status == "failed"
		}, 5*time.Second, 50*time.Millisecond, "task should reach failed status after exhausting retries")

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)

		assert.Equal(t, "failed", task.Status, "exhausted task must be marked failed")
		assert.Equal(t, 0, task.AttemptsLeft, "attempts_left must be 0 so the task is classified as dead-letter, not needs-retry")
		assert.Equal(t, maxRetry, task.AttemptsElapsed, "attempts_elapsed must equal maxRetry once retries are exhausted")
		require.Len(t, task.Messages, maxRetry, "every failed attempt must contribute exactly one message")
		assert.Equal(t, int32(maxRetry), processed.Load(), "handler should run exactly maxRetry times")

		// Each stored message corresponds to its attempt in order.
		for i, msg := range task.Messages {
			expected := fmt.Sprintf("simulated failure on attempt %d", i+1)
			assert.Equal(t, expected, msg, "message %d should record the error from attempt %d", i, i+1)
		}
	})
}

// TestCompletion_BumpsAttemptsElapsed locks down the success-path counterpart
// of TestRetryExhaustion: a task that runs to completion must record the
// attempts that actually executed. Without the CompleteTasks SQL bumping
// attempts_elapsed, a successfully-on-first-try task displays 0 attempts
// even though the handler ran.
func TestCompletion_BumpsAttemptsElapsed(t *testing.T) {
	t.Parallel()

	t.Run("first-try success records exactly one attempt", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "completion_first_try_" + t.Name()
		idempotencyToken := "completion-first-token-1"
		const maxRetry = 3
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ *asynqpg.TaskInfo) error {
				processed.Add(1)
				return nil
			}).Maybe()

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
			return processed.Load() == 1
		}, 2*time.Second, 50*time.Millisecond, "handler should run exactly once")

		// Wait for the batch completer to flush.
		assert.Eventually(t, func() bool {
			task, getErr := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
			return getErr == nil && task.Status == "completed"
		}, 5*time.Second, 50*time.Millisecond, "task should reach completed status")

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)

		assert.Equal(t, "completed", task.Status, "task should be completed")
		assert.Equal(t, 1, task.AttemptsElapsed, "attempts_elapsed must reflect the single successful run")
		assert.Equal(t, maxRetry, task.AttemptsLeft, "attempts_left should be unchanged when the first attempt succeeds")
		assert.Empty(t, task.Messages, "successful run should not append any messages")
	})

	t.Run("success after retries records every executed attempt", func(t *testing.T) {
		t.Parallel()

		db := testutils.SetupTestDatabase(t)

		taskType := "completion_after_retries_" + t.Name()
		idempotencyToken := "completion-after-token-1"
		const maxRetry = 5
		const failAttempts = 2
		var processed atomic.Int32

		handler := mocks.NewTaskHandler(t)
		handler.EXPECT().Handle(mock.Anything, mock.Anything).
			RunAndReturn(func(_ context.Context, _ *asynqpg.TaskInfo) error {
				attempt := processed.Add(1)
				if attempt <= failAttempts {
					return fmt.Errorf("simulated transient failure %d", attempt)
				}
				return nil
			}).Maybe()

		c, err := consumer.New(consumer.Config{
			Pool:            db,
			FetchInterval:   30 * time.Millisecond,
			JanitorInterval: 10 * time.Second,
			RetryPolicy:     &asynqpg.ConstantRetryPolicy{Delay: 30 * time.Millisecond},
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
			return processed.Load() >= int32(failAttempts+1)
		}, 5*time.Second, 50*time.Millisecond, "handler should run until success")

		assert.Eventually(t, func() bool {
			task, getErr := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
			return getErr == nil && task.Status == "completed"
		}, 5*time.Second, 50*time.Millisecond, "task should reach completed status")

		err = c.Stop()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, idempotencyToken)
		require.NoError(t, err)

		const totalAttempts = failAttempts + 1
		assert.Equal(t, "completed", task.Status, "task should be completed")
		assert.Equal(t, totalAttempts, task.AttemptsElapsed, "attempts_elapsed should count both failed retries and the successful run")
		assert.Equal(t, maxRetry-failAttempts, task.AttemptsLeft, "attempts_left should be decremented only by the failed retries")
		require.Len(t, task.Messages, failAttempts, "messages should record every failed attempt but not the successful run")
	})
}

func TestInfo_Lifecycle(t *testing.T) {
	t.Parallel()
	db := testutils.SetupTestDatabase(t)
	c, err := consumer.New(consumer.Config{Pool: db, DisableMaintenance: true, FetchInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	entered, release, draining := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); _ = c.Stop() })
	require.NoError(t, c.RegisterTaskHandler("info.lifecycle", consumer.TaskHandlerFunc(func(ctx context.Context, _ *asynqpg.TaskInfo) error {
		close(entered)
		<-ctx.Done()
		close(draining)
		<-release
		return nil
	}), consumer.WithWorkersCount(1)))
	before := c.Info()
	assert.False(t, before.Running)
	assert.True(t, before.StartedAt.IsZero())
	require.NoError(t, c.Start())
	started := c.Info()
	assert.True(t, started.Running)
	assert.False(t, started.StartedAt.IsZero())
	_, err = db.ExecContext(context.Background(), `INSERT INTO asynqpg_tasks (type, payload, attempts_left, blocked_till) VALUES ('info.lifecycle', '{}', 3, now())`)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not start")
	}
	assert.Equal(t, 1, c.Info().TaskTypes[0].Busy)
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop() }()
	select {
	case <-draining:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not cancel handler context")
	}
	for range 100 {
		assert.True(t, c.Info().Running)
	}
	unblock()
	select {
	case err = <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
	after := c.Info()
	assert.False(t, after.Running)
	assert.Equal(t, started.StartedAt, after.StartedAt)
	assert.Equal(t, 1, after.TaskTypes[0].Workers)
	assert.Zero(t, after.TaskTypes[0].Busy)
}
