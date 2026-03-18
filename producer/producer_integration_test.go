//go:build integration

package producer_test

import (
	"context"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

func TestNew(t *testing.T) {
	t.Run("successfully creates producer with valid config", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)
		require.NotNil(t, p)
	})

	t.Run("returns error when Pool is nil", func(t *testing.T) {
		p, err := producer.New(producer.Config{})
		require.Error(t, err)
		assert.Nil(t, p)

		assert.Contains(t, err.Error(), "database pool is required")
	})
}

func TestEnqueue(t *testing.T) {
	t.Run("successfully enqueues task", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		token := "enqueue-test-" + t.Name()
		_, err = p.Enqueue(t.Context(), &asynqpg.Task{
			Type:             "test-task",
			Payload:          []byte(`{"key": "value"}`),
			IdempotencyToken: ptr.Get(token),
		})
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, "test-task", task.Type)
		assert.Equal(t, []byte(`{"key": "value"}`), task.Payload)
		assert.Equal(t, "pending", task.Status)
	})

	t.Run("returns error when task is nil", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "task cannot be nil")
	})

	t.Run("returns error when task type is empty", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		_, err = p.Enqueue(t.Context(), &asynqpg.Task{Type: ""})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "task type cannot be empty")
	})

	t.Run("applies delay correctly", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		token := "delay-test-" + t.Name()
		delay := 5 * time.Second
		now := time.Now()
		_, err = p.Enqueue(t.Context(), &asynqpg.Task{
			Type:             "delayed-task",
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(token),
			Delay:            delay,
		})
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.True(t, task.BlockedTill.After(now.Add(4*time.Second)))
	})

	t.Run("applies max retry from config", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{
			Pool:            db,
			DefaultMaxRetry: 7,
		})
		require.NoError(t, err)

		token := "retry-test-" + t.Name()
		_, err = p.Enqueue(t.Context(), &asynqpg.Task{
			Type:             "retry-task",
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(token),
		})
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, 7, task.AttemptsLeft)
	})

	t.Run("task max retry overrides config", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{
			Pool:            db,
			DefaultMaxRetry: 7,
		})
		require.NoError(t, err)

		token := "retry-override-" + t.Name()
		_, err = p.Enqueue(t.Context(), &asynqpg.Task{
			Type:             "retry-task",
			Payload:          []byte("{}"),
			IdempotencyToken: ptr.Get(token),
			MaxRetry:         ptr.Get(10),
		})
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, 10, task.AttemptsLeft)
	})
}

func TestEnqueueTx(t *testing.T) {
	t.Run("task created after commit", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		token := "commit-test-" + t.Name()
		_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
			Type:             "test-task",
			Payload:          []byte(`{"tx": "commit"}`),
			IdempotencyToken: ptr.Get(token),
		})
		require.NoError(t, err)

		var countBeforeCommit int
		err = db.Get(&countBeforeCommit, "SELECT COUNT(*) FROM asynqpg_tasks WHERE idempotency_token = $1", token)
		require.NoError(t, err)
		assert.Equal(t, 0, countBeforeCommit)

		err = tx.Commit()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, "test-task", task.Type)
		assert.Equal(t, []byte(`{"tx": "commit"}`), task.Payload)
	})

	t.Run("task not created on rollback", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		token := "rollback-test-" + t.Name()
		_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
			Type:             "test-task",
			Payload:          []byte(`{"tx": "rollback"}`),
			IdempotencyToken: ptr.Get(token),
		})
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM asynqpg_tasks WHERE idempotency_token = $1", token)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("multiple tasks in single transaction", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		tokens := []string{
			"multi-1-" + t.Name(),
			"multi-2-" + t.Name(),
			"multi-3-" + t.Name(),
		}

		for i, token := range tokens {
			_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
				Type:             "multi-task",
				Payload:          []byte(`{"index": ` + string(rune('0'+i)) + `}`),
				IdempotencyToken: ptr.Get(token),
			})
			require.NoError(t, err)
		}

		err = tx.Commit()
		require.NoError(t, err)

		for _, token := range tokens {
			task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
			require.NoError(t, err)
			assert.Equal(t, "multi-task", task.Type)
		}
	})

	t.Run("multiple tasks rollback together", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		tokens := []string{
			"rollback-multi-1-" + t.Name(),
			"rollback-multi-2-" + t.Name(),
		}

		for _, token := range tokens {
			_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
				Type:             "multi-task",
				Payload:          []byte(``),
				IdempotencyToken: ptr.Get(token),
			})
			require.NoError(t, err)
		}

		err = tx.Rollback()
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM asynqpg_tasks WHERE idempotency_token = ANY($1)", pq.Array(tokens))
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("atomicity with business data", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_orders (id SERIAL PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)

		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO test_orders (name) VALUES ('test-order')`)
		require.NoError(t, err)

		token := "atomicity-test-" + t.Name()
		_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
			Type:             "process-order",
			Payload:          []byte(``),
			IdempotencyToken: ptr.Get(token),
		})
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var orderCount int
		err = db.Get(&orderCount, "SELECT COUNT(*) FROM test_orders WHERE name = 'test-order'")
		require.NoError(t, err)
		assert.Equal(t, 0, orderCount)

		var taskCount int
		err = db.Get(&taskCount, "SELECT COUNT(*) FROM asynqpg_tasks WHERE idempotency_token = $1", token)
		require.NoError(t, err)
		assert.Equal(t, 0, taskCount)
	})

	t.Run("atomicity commit with business data", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_orders (id SERIAL PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)

		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO test_orders (name) VALUES ('committed-order')`)
		require.NoError(t, err)

		token := "atomicity-commit-" + t.Name()
		_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
			Type:             "process-order",
			IdempotencyToken: ptr.Get(token),
			Payload:          []byte(``),
		})
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		var orderCount int
		err = db.Get(&orderCount, "SELECT COUNT(*) FROM test_orders WHERE name = 'committed-order'")
		require.NoError(t, err)
		assert.Equal(t, 1, orderCount)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, "process-order", task.Type)
	})

	t.Run("returns error when executor is nil", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		_, err = p.EnqueueTx(context.Background(), nil, &asynqpg.Task{Type: "test"})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "executor cannot be nil")
	})

	t.Run("returns error when task is nil", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = p.EnqueueTx(ctx, tx, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "task cannot be nil")
	})

	t.Run("returns error when task type is empty", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()

		_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{Type: ""})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "task type cannot be empty")
	})

	t.Run("applies delay in transaction", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		token := "tx-delay-" + t.Name()
		delay := 10 * time.Second
		_, err = p.EnqueueTx(ctx, tx, &asynqpg.Task{
			Type:             "delayed-task",
			IdempotencyToken: ptr.Get(token),
			Delay:            delay,
			Payload:          []byte(``),
		})
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.True(t, task.BlockedTill.After(time.Now().Add(9*time.Second)))
	})
}

func TestEnqueueMany(t *testing.T) {
	t.Run("successfully enqueues multiple tasks", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		tasks := []*asynqpg.Task{
			{Type: "batch-task", Payload: []byte(`{"id":1}`)},
			{Type: "batch-task", Payload: []byte(`{"id":2}`)},
			{Type: "batch-task", Payload: []byte(`{"id":3}`)},
		}

		ids, err := p.EnqueueMany(t.Context(), tasks)
		require.NoError(t, err)
		assert.Len(t, ids, 3)
	})

	t.Run("returns empty slice for empty input", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ids, err := p.EnqueueMany(t.Context(), []*asynqpg.Task{})
		require.NoError(t, err)
		assert.Len(t, ids, 0)
	})

	t.Run("handles idempotency - skips duplicates", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		token := "batch-idemp-" + t.Name()
		tasks := []*asynqpg.Task{
			{Type: "batch-task", Payload: []byte(`{}`), IdempotencyToken: ptr.Get(token)},
		}

		// First insert
		ids1, err := p.EnqueueMany(t.Context(), tasks)
		require.NoError(t, err)
		assert.Len(t, ids1, 1)

		// Second insert with same token - should be skipped
		ids2, err := p.EnqueueMany(t.Context(), tasks)
		require.NoError(t, err)
		assert.Len(t, ids2, 0)
	})

	t.Run("validates all tasks - rejects nil task", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		tasks := []*asynqpg.Task{
			{Type: "batch-task", Payload: []byte(`{}`)},
			nil,
			{Type: "batch-task", Payload: []byte(`{}`)},
		}

		_, err = p.EnqueueMany(t.Context(), tasks)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "index 1")
	})

	t.Run("validates all tasks - rejects empty type", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		tasks := []*asynqpg.Task{
			{Type: "batch-task", Payload: []byte(`{}`)},
			{Type: "", Payload: []byte(`{}`)},
		}

		_, err = p.EnqueueMany(t.Context(), tasks)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty type")
	})

	t.Run("validates all tasks - rejects nil payload", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		tasks := []*asynqpg.Task{
			{Type: "batch-task", Payload: []byte(`{}`)},
			{Type: "batch-task", Payload: nil},
		}

		_, err = p.EnqueueMany(t.Context(), tasks)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil payload")
	})

	t.Run("applies delay and max retry", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db, DefaultMaxRetry: 5})
		require.NoError(t, err)

		token := "batch-config-" + t.Name()
		tasks := []*asynqpg.Task{
			{Type: "batch-task", Payload: []byte(`{}`), Delay: 5 * time.Second, IdempotencyToken: ptr.Get(token)},
		}

		_, err = p.EnqueueMany(t.Context(), tasks)
		require.NoError(t, err)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, 5, task.AttemptsLeft)
		assert.True(t, task.BlockedTill.After(time.Now().Add(4*time.Second)))
	})

	t.Run("handles mixed tasks with different configs", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db, DefaultMaxRetry: 3})
		require.NoError(t, err)

		token1 := "batch-mixed-1-" + t.Name()
		token2 := "batch-mixed-2-" + t.Name()
		tasks := []*asynqpg.Task{
			{Type: "type-a", Payload: []byte(`{}`), MaxRetry: ptr.Get(10), IdempotencyToken: ptr.Get(token1)},
			{Type: "type-b", Payload: []byte(`{}`), IdempotencyToken: ptr.Get(token2)}, // Uses default
		}

		_, err = p.EnqueueMany(t.Context(), tasks)
		require.NoError(t, err)

		task1, err := testutils.GetTaskByIdempotencyToken(t, db, token1)
		require.NoError(t, err)
		assert.Equal(t, 10, task1.AttemptsLeft)

		task2, err := testutils.GetTaskByIdempotencyToken(t, db, token2)
		require.NoError(t, err)
		assert.Equal(t, 3, task2.AttemptsLeft)
	})
}

func TestEnqueueManyTx(t *testing.T) {
	t.Run("tasks created after commit", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		tasks := []*asynqpg.Task{
			{Type: "batch-tx-task", Payload: []byte(`{"id":1}`)},
			{Type: "batch-tx-task", Payload: []byte(`{"id":2}`)},
		}

		ids, err := p.EnqueueManyTx(ctx, tx, tasks)
		require.NoError(t, err)
		assert.Len(t, ids, 2)

		// Not visible before commit
		var countBeforeCommit int
		err = db.Get(&countBeforeCommit, "SELECT COUNT(*) FROM asynqpg_tasks WHERE type = 'batch-tx-task'")
		require.NoError(t, err)
		assert.Equal(t, 0, countBeforeCommit)

		err = tx.Commit()
		require.NoError(t, err)

		// Visible after commit
		var countAfterCommit int
		err = db.Get(&countAfterCommit, "SELECT COUNT(*) FROM asynqpg_tasks WHERE type = 'batch-tx-task'")
		require.NoError(t, err)
		assert.Equal(t, 2, countAfterCommit)
	})

	t.Run("tasks not created on rollback", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		token := "batch-rollback-" + t.Name()
		tasks := []*asynqpg.Task{
			{Type: "batch-rollback-task", Payload: []byte(`{}`), IdempotencyToken: ptr.Get(token)},
		}

		_, err = p.EnqueueManyTx(ctx, tx, tasks)
		require.NoError(t, err)

		err = tx.Rollback()
		require.NoError(t, err)

		var count int
		err = db.Get(&count, "SELECT COUNT(*) FROM asynqpg_tasks WHERE idempotency_token = $1", token)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("returns error when executor is nil", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		tasks := []*asynqpg.Task{{Type: "test", Payload: []byte(`{}`)}}
		_, err = p.EnqueueManyTx(context.Background(), nil, tasks)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "executor cannot be nil")
	})

	t.Run("returns empty slice for empty input", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)
		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()

		ids, err := p.EnqueueManyTx(ctx, tx, []*asynqpg.Task{})
		require.NoError(t, err)
		assert.Len(t, ids, 0)
	})

	t.Run("atomicity with business data", func(t *testing.T) {
		db := testutils.SetupTestDatabase(t)

		_, err := db.Exec(`CREATE TABLE IF NOT EXISTS batch_orders (id SERIAL PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)

		p, err := producer.New(producer.Config{Pool: db})
		require.NoError(t, err)

		ctx := context.Background()
		tx, err := db.BeginTxx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.Exec(`INSERT INTO batch_orders (name) VALUES ('batch-order')`)
		require.NoError(t, err)

		token := "batch-atomicity-" + t.Name()
		tasks := []*asynqpg.Task{
			{Type: "process-batch-order", Payload: []byte(`{}`), IdempotencyToken: ptr.Get(token)},
		}

		_, err = p.EnqueueManyTx(ctx, tx, tasks)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		var orderCount int
		err = db.Get(&orderCount, "SELECT COUNT(*) FROM batch_orders WHERE name = 'batch-order'")
		require.NoError(t, err)
		assert.Equal(t, 1, orderCount)

		task, err := testutils.GetTaskByIdempotencyToken(t, db, token)
		require.NoError(t, err)
		assert.Equal(t, "process-batch-order", task.Type)
	})
}
