//go:build integration

package notify_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

func publisherDB(t *testing.T) (*sqlx.DB, string) {
	t.Helper()
	image := os.Getenv("NOTIFY_TEST_POSTGRES_IMAGE")
	if image == "" {
		image = "postgres:17-alpine"
	}
	db, dsn := testutils.SetupTestDatabaseWithImage(t, image)

	return db, dsn
}

func listenTable(t *testing.T, dsn, table string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close(context.Background())) })
	var oid uint32
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT $1::regclass::oid", table).Scan(&oid))
	_, err = conn.Exec(context.Background(), "LISTEN "+pgx.Identifier{fmt.Sprintf("asynqpg_ready_v1_%d", oid)}.Sanitize())
	require.NoError(t, err)
	return conn
}

func hint(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)
	return n.Payload
}
func noHint(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := conn.WaitForNotification(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestReadyNotifications_CommitRollback(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	tx, err := db.Beginx()
	require.NoError(t, err)
	_, err = repository.NewRepository(db).PushTaskWithTx(t.Context(), tx, &repository.PushTaskParams{Type: "committed", Payload: []byte(`{}`), AttemptsLeft: 3})
	require.NoError(t, err)
	noHint(t, conn)
	require.NoError(t, tx.Commit())
	assert.Equal(t, "committed", hint(t, conn))
	tx, err = db.Beginx()
	require.NoError(t, err)
	_, err = repository.NewRepository(db).PushTaskWithTx(t.Context(), tx, &repository.PushTaskParams{Type: "rolledback", Payload: []byte(`{}`), AttemptsLeft: 3})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	noHint(t, conn)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM asynqpg_tasks WHERE type='rolledback'`))
	assert.Zero(t, count)
}

func TestReadyNotifications_SchemaIsolation(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	_, err := db.Exec(`CREATE SCHEMA other; CREATE TABLE other.asynqpg_tasks (LIKE public.asynqpg_tasks INCLUDING ALL)`)
	require.NoError(t, err)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("search_path", "other,public")
	u.RawQuery = q.Encode()
	other, err := sqlx.Connect("postgres", u.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = other.Close() })

	publicListener := listenTable(t, dsn, "public.asynqpg_tasks")
	otherListener := listenTable(t, dsn, "other.asynqpg_tasks")
	_, err = repository.NewRepository(other).PushTask(t.Context(), &repository.PushTaskParams{Type: "other", Payload: []byte(`{}`), AttemptsLeft: 3})
	require.NoError(t, err)
	assert.Equal(t, "other", hint(t, otherListener))
	noHint(t, publicListener)
}

func TestReadyNotifications_ProducerAPIs(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)
	ctx := context.Background()
	task := func(name string) *asynqpg.Task { return &asynqpg.Task{Type: name, Payload: []byte(`{}`)} }
	_, err = p.Enqueue(ctx, task("single"))
	require.NoError(t, err)
	assert.Equal(t, "single", hint(t, conn))
	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	_, err = p.EnqueueTx(ctx, tx, task("single-tx"))
	require.NoError(t, err)
	noHint(t, conn)
	require.NoError(t, tx.Commit())
	assert.Equal(t, "single-tx", hint(t, conn))
	_, err = p.EnqueueMany(ctx, []*asynqpg.Task{task("batch"), task("batch")})
	require.NoError(t, err)
	assert.Equal(t, "batch", hint(t, conn))
	noHint(t, conn)
	tx, err = db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	_, err = p.EnqueueManyTx(ctx, tx, []*asynqpg.Task{task("batch-tx"), task("batch-tx")})
	require.NoError(t, err)
	noHint(t, conn)
	require.NoError(t, tx.Commit())
	assert.Equal(t, "batch-tx", hint(t, conn))
	noHint(t, conn)
	result, err := p.Enqueue(ctx, task("retry"))
	require.NoError(t, err)
	_ = hint(t, conn)
	_, err = db.Exec(`UPDATE asynqpg_tasks SET status='failed',finalized_at=now() WHERE id=$1`, result.ID)
	require.NoError(t, err)
	c, err := client.New(client.Config{Pool: db})
	require.NoError(t, err)
	_, err = c.RetryTask(ctx, result.ID)
	require.NoError(t, err)
	assert.Equal(t, "retry", hint(t, conn))
}

func TestReadyNotifications_NoDuplicateOrExternalHints(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	repo := repository.NewRepository(db)
	token := "unique"
	task := &repository.PushTaskParams{Type: "dedupe", Payload: []byte(`{}`), AttemptsLeft: 3, IdempotencyToken: &token}
	first, err := repo.PushTask(t.Context(), task)
	require.NoError(t, err)
	require.False(t, first.Duplicate)
	require.Equal(t, "dedupe", hint(t, conn))
	// Wait beyond limiter to prove the SQL no-op predicate, not just suppression.
	time.Sleep(20 * time.Millisecond)
	second, err := repo.PushTask(t.Context(), task)
	require.NoError(t, err)
	require.True(t, second.Duplicate)
	noHint(t, conn)
	_, err = db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('external','{}',3,now())`)
	require.NoError(t, err)
	noHint(t, conn)
}
func TestReadyNotifications_WorkerTransitions(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	repo := repository.NewRepository(db)
	var id int64
	require.NoError(t, db.Get(&id, `INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till,status) VALUES ('transition','{}',3,now(),'running') RETURNING id`))
	count, err := repo.RetryTasksMany(t.Context(), repository.RetryTasksManyParams{IDs: []int64{id}, Generations: []int64{0}, BlockedTills: []time.Time{time.Now()}, Messages: []string{"retry"}})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, "transition", hint(t, conn))
	time.Sleep(20 * time.Millisecond)
	count, err = repo.SnoozeTasksMany(t.Context(), repository.SnoozeTasksManyParams{IDs: []int64{id}, Generations: []int64{0}, BlockedTills: []time.Time{time.Now()}})
	require.NoError(t, err)
	require.Zero(t, count)
	noHint(t, conn)
	_, err = db.Exec(`UPDATE asynqpg_tasks SET status='running' WHERE id=$1`, id)
	require.NoError(t, err)
	require.NoError(t, repo.SnoozeTask(t.Context(), repository.SnoozeTaskParams{ID: id, BlockedTill: time.Now()}))
	require.Equal(t, "transition", hint(t, conn))
}
func TestReadyNotifications_UpgradeRemovesLegacyTriggers(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	testutils.ApplyReadyNotifications(t, db) // Legacy fixture only.
	script, err := os.ReadFile("../../migrations/003_remove_readiness_triggers.sql")
	require.NoError(t, err)
	for range 2 {
		_, err = db.Exec(string(script))
		require.NoError(t, err)
	}
	conn := listenTable(t, dsn, "asynqpg_tasks")
	_, err = db.Exec(`INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('external','{}',3,now())`)
	require.NoError(t, err)
	noHint(t, conn)
	_, err = repository.NewRepository(db).PushTask(t.Context(), &repository.PushTaskParams{Type: "library", Payload: []byte(`{}`), AttemptsLeft: 3})
	require.NoError(t, err)
	require.Equal(t, "library", hint(t, conn))
}
func TestReadyNotifications_PublicationFailureRollsBackMutation(t *testing.T) {
	t.Parallel()
	db, _ := publisherDB(t)
	// PostgreSQL rejects payloads >= 8000 bytes. Widen only this fixture's type column.
	_, err := db.Exec(`ALTER TABLE asynqpg_tasks DROP CONSTRAINT type_length; ALTER TABLE asynqpg_tasks ALTER COLUMN type TYPE text`)
	require.NoError(t, err)
	_, err = repository.NewRepository(db).PushTask(t.Context(), &repository.PushTaskParams{Type: strings.Repeat("x", 8000), Payload: []byte(`{}`), AttemptsLeft: 3})
	require.ErrorContains(t, err, "payload string too long")
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM asynqpg_tasks`))
	require.Zero(t, count)
}

func TestReadyNotifications_RollbackReservationSuppressesNextHint(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	repo := repository.NewRepository(db)
	tx, err := db.Beginx()
	require.NoError(t, err)
	task := &repository.PushTaskParams{Type: "reserved", Payload: []byte(`{}`), AttemptsLeft: 3}
	start := time.Now()
	_, err = repo.PushTaskWithTx(t.Context(), tx, task)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = repo.PushTask(t.Context(), task)
	require.NoError(t, err)
	if time.Since(start) >= 10*time.Millisecond {
		t.Skip("database round trips exceeded limiter window; exact clock boundary covered by unit tests")
	}
	noHint(t, conn)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM asynqpg_tasks`))
	require.Equal(t, 1, count)
	// The reservation expires even after rollback.
	_, err = repo.PushTask(t.Context(), task)
	require.NoError(t, err)
	require.Equal(t, "reserved", hint(t, conn))
}

func TestReadyNotifications_ManualRetryTransactionVisibility(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	var id int64
	require.NoError(t, db.Get(&id, `INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till,status,finalized_at) VALUES ('manual','{}',3,now(),'failed',now()) RETURNING id`))
	repo := repository.NewClientRepository(db)
	tx, err := db.Beginx()
	require.NoError(t, err)
	_, changed, err := repo.RetryTaskByIDWithTx(t.Context(), tx, id)
	require.NoError(t, err)
	require.True(t, changed)
	noHint(t, conn)
	require.NoError(t, tx.Rollback())
	noHint(t, conn)
	tx, err = db.Beginx()
	require.NoError(t, err)
	_, changed, err = repo.RetryTaskByIDWithTx(t.Context(), tx, id)
	require.NoError(t, err)
	require.True(t, changed)
	noHint(t, conn)
	require.NoError(t, tx.Commit())
	require.Equal(t, "manual", hint(t, conn))
	time.Sleep(20 * time.Millisecond)
	_, changed, err = repo.RetryTaskByID(t.Context(), id)
	require.NoError(t, err)
	require.False(t, changed)
	noHint(t, conn)
}

func TestReadyNotifications_BatchRollbackAndDistinctTypes(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	repo := repository.NewRepository(db)
	params := repository.PushTasksParams{Tasks: []repository.PushTaskParams{
		{Type: "a", Payload: []byte(`{}`), AttemptsLeft: 3},
		{Type: "a", Payload: []byte(`{}`), AttemptsLeft: 3},
		{Type: "b", Payload: []byte(`{}`), AttemptsLeft: 3},
	}}
	tx, err := db.Beginx()
	require.NoError(t, err)
	_, err = repo.PushTasksWithTx(t.Context(), tx, params)
	require.NoError(t, err)
	noHint(t, conn)
	require.NoError(t, tx.Rollback())
	noHint(t, conn)
	var count int
	require.NoError(t, db.Get(&count, `SELECT count(*) FROM asynqpg_tasks`))
	require.Zero(t, count)
	_, err = repo.PushTasks(t.Context(), params)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, []string{hint(t, conn), hint(t, conn)})
	noHint(t, conn)
}

func TestReadyNotifications_DirectUnchangedSnoozeHasNoHint(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	conn := listenTable(t, dsn, "asynqpg_tasks")
	var row struct {
		ID  int64     `db:"id"`
		Due time.Time `db:"blocked_till"`
	}
	require.NoError(t, db.Get(&row, `INSERT INTO asynqpg_tasks(type,payload,attempts_left,blocked_till) VALUES ('unchanged','{}',3,now()) RETURNING id,blocked_till`))
	repo := repository.NewRepository(db)
	require.NoError(t, repo.SnoozeTask(t.Context(), repository.SnoozeTaskParams{ID: row.ID, BlockedTill: row.Due}))
	noHint(t, conn)
	require.NoError(t, repo.SnoozeTask(t.Context(), repository.SnoozeTaskParams{ID: row.ID, BlockedTill: row.Due.Add(time.Second)}))
	noHint(t, conn) // An unclaimed pending row is not a worker-owned attempt.
	tasks, err := repo.GetReadyTasks(t.Context(), repository.GetReadyTasksParams{Type: "unchanged", Limit: 1, Delay: time.Minute})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.NoError(t, repo.SnoozeTask(t.Context(), repository.SnoozeTaskParams{ID: row.ID, ClaimGeneration: tasks[0].ClaimGeneration - 1, BlockedTill: row.Due}))
	noHint(t, conn) // A stale generation cannot publish even while the task is running.
	require.NoError(t, repo.SnoozeTask(t.Context(), repository.SnoozeTaskParams{ID: row.ID, ClaimGeneration: tasks[0].ClaimGeneration, BlockedTill: row.Due.Add(time.Second)}))
	require.Equal(t, "unchanged", hint(t, conn))
}
