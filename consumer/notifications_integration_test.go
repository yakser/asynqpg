//go:build integration

package consumer_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

const testTaskTypeNotify = "notify"

type fetchProbe struct {
	asynqpg.NotificationPool
	fetched chan struct{}
}

func (p *fetchProbe) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	err := p.NotificationPool.SelectContext(ctx, dest, query, args...)
	if err == nil && strings.Contains(query, "for no key update skip locked") {
		select {
		case p.fetched <- struct{}{}:
		default:
		}
	}
	return err
}
func waitFetched(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("initial fetch did not finish")
	}
}
func TestConsumerNotify_SparsePickup(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

	probe := &fetchProbe{NotificationPool: testutils.NotificationPool(t, dsn), fetched: make(chan struct{}, 10)}
	c, err := consumer.New(consumer.Config{Pool: probe, DisableMaintenance: true, FetchInterval: 10 * time.Second, Notifications: &consumer.NotificationConfig{}})
	require.NoError(t, err)
	handled := make(chan int64, 1)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(_ context.Context, task *asynqpg.TaskInfo) error { handled <- task.ID; return nil }), consumer.WithWorkersCount(1)))
	require.NoError(t, c.Start())
	t.Cleanup(func() { require.NoError(t, c.Stop()) })
	waitFetched(t, probe.fetched)
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)
	result, err := p.Enqueue(context.Background(), &asynqpg.Task{Type: testTaskTypeNotify, Payload: []byte(`{}`)})
	require.NoError(t, err)
	select {
	case id := <-handled:
		assert.Equal(t, result.ID, id)
	case <-time.After(2 * time.Second):
		t.Fatal("notification did not wake consumer before poll")
	}
}
func TestConsumerNotify_SaturatedBacklog(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

	c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, FetchInterval: 10 * time.Second, Notifications: &consumer.NotificationConfig{}})
	require.NoError(t, err)
	release, entered := make(chan struct{}), make(chan struct{})
	var first, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	handled := make(chan int64, 21)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(_ context.Context, task *asynqpg.TaskInfo) error {
		first.Do(func() { close(entered); <-release })
		handled <- task.ID
		return nil
	}), consumer.WithWorkersCount(1)))
	require.NoError(t, c.Start())
	t.Cleanup(func() { unblock(); require.NoError(t, c.Stop()) })
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)
	_, err = p.Enqueue(context.Background(), &asynqpg.Task{Type: testTaskTypeNotify, Payload: []byte(`{}`)})
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first handler did not start")
	}
	tasks := make([]*asynqpg.Task, 20)
	for i := range tasks {
		tasks[i] = &asynqpg.Task{Type: testTaskTypeNotify, Payload: []byte(`{}`)}
	}
	_, err = p.EnqueueMany(context.Background(), tasks)
	require.NoError(t, err)
	unblock()
	got := map[int64]bool{}
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for range 21 {
		select {
		case id := <-handled:
			assert.False(t, got[id], "duplicate ID")
			got[id] = true
		case <-timeout.C:
			t.Fatalf("only handled %d tasks", len(got))
		}
	}
}
func TestConsumerNotify_StartFailureCleanup(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)
	_, dropErr := db.Exec(`DROP TABLE asynqpg_tasks`)
	require.NoError(t, dropErr)
	c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, Notifications: &consumer.NotificationConfig{}})
	require.NoError(t, err)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { return nil })))
	done := make(chan error, 1)
	go func() { done <- c.Start() }()
	select {
	case err = <-done:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("startup failure deadlocked")
	}
	require.NoError(t, c.Stop())
	assert.False(t, c.Info().Running)
}

func TestConsumerNotify_StartupGap(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

	_, err := db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('notify','{}',3,now())`)
	require.NoError(t, err)
	c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, FetchInterval: 10 * time.Second, Notifications: &consumer.NotificationConfig{}})
	require.NoError(t, err)
	handled := make(chan struct{}, 1)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { handled <- struct{}{}; return nil })))
	require.NoError(t, c.Start())
	t.Cleanup(func() { require.NoError(t, c.Stop()) })
	waitFetched(t, handled)
}
func TestConsumerNotify_ReconnectFallback(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

	c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, FetchInterval: 50 * time.Millisecond, Notifications: &consumer.NotificationConfig{}})
	require.NoError(t, err)
	handled := make(chan struct{}, 2)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { handled <- struct{}{}; return nil })))
	require.NoError(t, c.Start())
	t.Cleanup(func() { require.NoError(t, c.Stop()) })
	var killed bool
	require.NoError(t, db.Get(&killed, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%' AND datname=current_database()`))
	require.True(t, killed)
	_, err = db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('notify','{}',3,now())`)
	require.NoError(t, err)
	waitFetched(t, handled) // No publisher and no listener: safety polling must process it.
	require.Eventually(t, func() bool {
		var n int
		queryErr := db.Get(&n, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%'`)
		return queryErr == nil && n == 1
	}, 5*time.Second, 20*time.Millisecond)
	_, err = db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('notify','{}',3,now())`)
	require.NoError(t, err)
	waitFetched(t, handled)
}
func TestConsumerNotify_TwoConsumers(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

	handled := make(chan int64, 60)
	for range 2 {
		c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, FetchInterval: 10 * time.Second, Notifications: &consumer.NotificationConfig{}})
		require.NoError(t, err)
		require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(_ context.Context, task *asynqpg.TaskInfo) error { handled <- task.ID; return nil }), consumer.WithWorkersCount(2)))
		require.NoError(t, c.Start())
		t.Cleanup(func() { require.NoError(t, c.Stop()) })
	}
	tasks := make([]repository.PushTaskParams, 60)
	for i := range tasks {
		tasks[i] = repository.PushTaskParams{Type: "notify", Payload: []byte(`{}`), AttemptsLeft: 3}
	}
	_, err := repository.NewRepository(db).PushTasks(t.Context(), repository.PushTasksParams{Tasks: tasks})
	require.NoError(t, err)
	got := map[int64]bool{}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for range 60 {
		select {
		case id := <-handled:
			assert.False(t, got[id], "duplicate ID")
			got[id] = true
		case <-deadline.C:
			t.Fatalf("processed only %d tasks", len(got))
		}
	}
	assert.Len(t, got, 60)
}
func TestConsumerNotify_DelayedRetrySnoozeLease(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{true, false} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			t.Parallel()
			db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

			c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, DisableBatchCompleter: !batch, FetchInterval: 50 * time.Millisecond, FetchCooldown: time.Millisecond, Notifications: &consumer.NotificationConfig{}})
			require.NoError(t, err)
			handled := make(chan *asynqpg.TaskInfo, 4)
			require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(_ context.Context, task *asynqpg.TaskInfo) error { handled <- task; return nil }), consumer.WithWorkersCount(4)))
			require.NoError(t, c.Start())
			t.Cleanup(func() { require.NoError(t, c.Stop()) })
			var due time.Time
			require.NoError(t, db.Get(&due, `SELECT now()+interval '300 milliseconds'`))
			for i := range 4 {
				status := "running"
				if i == 0 {
					status = "pending"
				}
				var id int64
				require.NoError(t, db.QueryRowContext(context.Background(), `INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till,status,attempted_at) VALUES ('notify','{}',3,$1,$2::asynqpg_task_status,now()) RETURNING id`, due, status).Scan(&id))
				repo := repository.NewRepository(db)
				switch i {
				case 1:
					require.NoError(t, repo.RetryTask(context.Background(), repository.RetryTaskParams{ID: id, BlockedTill: due, Message: "retry"}))
				case 2:
					require.NoError(t, repo.SnoozeTask(context.Background(), repository.SnoozeTaskParams{ID: id, BlockedTill: due}))
				}
			}
			for range 4 {
				select {
				case task := <-handled:
					require.NotNil(t, task.AttemptedAt)
					assert.False(t, task.AttemptedAt.Before(due))
				case <-time.After(2 * time.Second):
					t.Fatal("due task was stranded")
				}
			}
		})
	}
}
func TestConsumerNotify_Shutdown(t *testing.T) {
	t.Parallel()
	db, dsn := testutils.SetupTestDatabaseWithConnStr(t)

	c, err := consumer.New(consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, Notifications: &consumer.NotificationConfig{}})
	require.NoError(t, err)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { return nil })))
	require.NoError(t, c.Start())
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() { assert.NoError(t, c.Stop()) })
	}
	wg.Wait()
	assert.False(t, c.Info().Running)
	require.Error(t, c.Start())
	// Closing the client socket precedes the server removing pg_stat_activity.
	require.Eventually(t, func() bool {
		var count int
		err := db.Get(&count, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%'`)
		return err == nil && count == 0
	}, time.Second, 10*time.Millisecond)
}
func TestConsumerNotify_PollOnlyCompatibility(t *testing.T) {
	t.Parallel()
	db := testutils.SetupTestDatabase(t) // No optional migration.
	c, err := consumer.New(consumer.Config{Pool: db, DisableMaintenance: true, FetchInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	handled := make(chan struct{}, 1)
	require.NoError(t, c.RegisterTaskHandler(testTaskTypeNotify, consumer.TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { handled <- struct{}{}; return nil })))
	require.NoError(t, c.Start())
	t.Cleanup(func() { require.NoError(t, c.Stop()) })
	_, err = db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('notify','{}',3,now())`)
	require.NoError(t, err)
	waitFetched(t, handled)
	// Closing the client socket precedes the server removing pg_stat_activity.
	require.Eventually(t, func() bool {
		var count int
		err := db.Get(&count, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%'`)
		return err == nil && count == 0
	}, time.Second, 10*time.Millisecond)
}

func TestConsumer_BacklogDrainsInBothModesBeforeFallback(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			t.Parallel()
			db, dsn := testutils.SetupTestDatabaseWithConnStr(t)
			// External SQL deliberately emits no hints. Startup and worker release must drain.
			_, err := db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) SELECT 'backlog','{}',3,now() FROM generate_series(1,20)`)
			require.NoError(t, err)
			cfg := consumer.Config{Pool: testutils.NotificationPool(t, dsn), DisableMaintenance: true, FetchInterval: time.Minute, FetchCooldown: time.Millisecond}
			if enabled {
				cfg.Notifications = &consumer.NotificationConfig{}
			}
			c, err := consumer.New(cfg)
			require.NoError(t, err)
			handled := make(chan struct{}, 20)
			require.NoError(t, c.RegisterTaskHandler("backlog", consumer.TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { handled <- struct{}{}; return nil }), consumer.WithWorkersCount(1)))
			require.NoError(t, c.Start())
			defer c.Stop()
			deadline := time.NewTimer(3 * time.Second)
			defer deadline.Stop()
			for range 20 {
				select {
				case <-handled:
				case <-deadline.C:
					t.Fatal("backlog waited for fallback poll")
				}
			}
		})
	}
}
