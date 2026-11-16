//go:build integration

package notify_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/notify"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

func receiveReady(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("listener did not become ready")
	}
}
func TestListener_ReconnectAndRecreatedTable(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	ready := make(chan struct{}, 4)
	types := make(chan string, 4)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	metrics, err := asynqpg.NewMetrics(mp)
	require.NoError(t, err)
	l, err := notify.New(notify.Config{Pool: testutils.NotificationPool(t, dsn), Metrics: metrics, OnReady: func() { ready <- struct{}{} }, OnType: func(value string) bool { types <- value; return true }})
	require.NoError(t, err)
	require.NoError(t, l.Start(context.Background()))
	t.Cleanup(l.Stop)
	receiveReady(t, ready)
	_, err = repository.NewRepository(db).PushTask(t.Context(), &repository.PushTaskParams{Type: "first", Payload: []byte(`{}`), AttemptsLeft: 3})
	require.NoError(t, err)
	select {
	case value := <-types:
		assert.Equal(t, "first", value)
	case <-time.After(2 * time.Second):
		t.Fatal("missing notification")
	}
	// Recreating the table changes its OID, hence its notification channel.
	_, err = db.Exec(`CREATE TABLE replacement (LIKE asynqpg_tasks INCLUDING ALL); ALTER SEQUENCE asynqpg_tasks_id_seq OWNED BY NONE; DROP TABLE asynqpg_tasks; ALTER TABLE replacement RENAME TO asynqpg_tasks`)
	require.NoError(t, err)

	var killed bool
	require.NoError(t, db.Get(&killed, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%' AND datname=current_database()`))
	assert.True(t, killed)
	receiveReady(t, ready)
	_, err = repository.NewRepository(db).PushTask(t.Context(), &repository.PushTaskParams{Type: "second", Payload: []byte(`{}`), AttemptsLeft: 3})
	require.NoError(t, err)
	select {
	case value := <-types:
		assert.Equal(t, "second", value)
	case <-time.After(2 * time.Second):
		t.Fatal("missing reconnected notification")
	}
	l.Stop()
	l.Stop()
	var got metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &got))
	values := map[string]int64{}
	for _, scope := range got.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, point := range sum.DataPoints {
					values[m.Name] += point.Value
				}
			}
		}
	}
	assert.Equal(t, int64(1), values["asynqpg.notifications.reconnects"])
	assert.Zero(t, values["asynqpg.notifications.connected"])
	require.Eventually(t, func() bool {
		var count int
		err := db.Get(&count, `SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%'`)
		return err == nil && count == 0
	}, time.Second, 10*time.Millisecond)
}
func TestListener_NoOptionalMigration(t *testing.T) {
	t.Parallel()
	_, dsn := testutils.SetupTestDatabaseWithConnStr(t)
	l, err := notify.New(notify.Config{Pool: testutils.NotificationPool(t, dsn), OnType: func(string) bool { return true }, OnReady: func() {}})
	require.NoError(t, err)
	require.NoError(t, l.Start(t.Context()))
	l.Stop()
}
func TestListener_SameNamedDifferentDatabases(t *testing.T) {
	t.Parallel()
	db, _ := publisherDB(t)
	other, dsn := publisherDB(t)
	var firstName, secondName string
	require.NoError(t, db.Get(&firstName, "SELECT current_database()"))
	require.NoError(t, other.Get(&secondName, "SELECT current_database()"))
	require.Equal(t, firstName, secondName)
	l, err := notify.New(notify.Config{Pool: &misroutedPool{Pool: db, listener: testutils.NotificationPool(t, dsn)}, OnType: func(string) bool { return true }, OnReady: func() { t.Error("unexpected readiness") }})
	require.NoError(t, err)
	require.ErrorIs(t, l.Start(t.Context()), asynqpg.ErrNotificationRouting)
	l.Stop()
	// Socket closure precedes removal of the server backend from pg_stat_activity.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		var sessions int
		require.NoError(collect, other.Get(&sessions, "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%'"))
		require.Zero(collect, sessions)
	}, 5*time.Second, 20*time.Millisecond)
}

type misroutedPool struct {
	asynqpg.Pool
	listener asynqpg.NotificationPool
}

func (p *misroutedPool) OpenNotificationConnection(ctx context.Context) (asynqpg.NotificationConnection, error) {
	return p.listener.OpenNotificationConnection(ctx)
}

func TestListener_CustomSchemaDifferentSearchPath(t *testing.T) {
	t.Parallel()
	db, dsn := publisherDB(t)
	db.SetMaxOpenConns(1)
	_, err := db.Exec(`CREATE SCHEMA queue_custom; CREATE TABLE queue_custom.asynqpg_tasks (LIKE public.asynqpg_tasks INCLUDING ALL); SET search_path=queue_custom,pg_catalog`)
	require.NoError(t, err)

	u, err := url.Parse(dsn)
	require.NoError(t, err)
	query := u.Query()
	query.Set("search_path", "queue_custom,pg_catalog")
	u.RawQuery = query.Encode()
	pool := &misroutedPool{Pool: testutils.NotificationPool(t, u.String()), listener: testutils.NotificationPool(t, dsn)}
	ready := make(chan struct{}, 1)
	l, err := notify.New(notify.Config{Pool: pool, OnType: func(string) bool { return true }, OnReady: func() { ready <- struct{}{} }})
	require.NoError(t, err)
	require.NoError(t, l.Start(t.Context()))
	defer l.Stop()
	receiveReady(t, ready)
}
