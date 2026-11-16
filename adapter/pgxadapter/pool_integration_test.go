//go:build integration

package pgxadapter_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/adapter/pgxadapter"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

func TestPoolOwnershipAndNotifications(t *testing.T) {
	t.Parallel()
	_, dsn := testutils.SetupTestDatabaseWithConnStr(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	var closed atomic.Int32
	cfg.BeforeClose = func(*pgx.Conn) { closed.Add(1) }
	native, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	defer native.Close()
	pool, err := pgxadapter.New(native)
	require.NoError(t, err)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := pool.OpenNotificationConnection(ctx)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	require.NoError(t, conn.Listen(ctx, `test"quoted`))
	_, err = pool.ExecContext(ctx, "SELECT pg_notify($1,$2)", `test"quoted`, "payload")
	require.NoError(t, err)
	got, err := conn.WaitForNotification(ctx)
	require.NoError(t, err)
	require.Equal(t, asynqpg.Notification{Channel: `test"quoted`, Payload: "payload"}, got)
	require.NoError(t, conn.Unlisten(ctx, got.Channel))
	p, err := producer.New(producer.Config{Pool: pool})
	require.NoError(t, err)
	tx, err := pool.BeginTxx(ctx, nil)
	require.NoError(t, err)
	_, err = p.EnqueueManyTx(ctx, tx, []*asynqpg.Task{{Type: "adapter", Payload: []byte(`{}`)}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	var count int
	require.NoError(t, pool.GetContext(ctx, &count, "SELECT count(*) FROM asynqpg_tasks"))
	require.Zero(t, count)
	tx, err = pool.BeginTxx(ctx, nil)
	require.NoError(t, err)
	_, err = p.EnqueueManyTx(ctx, tx, []*asynqpg.Task{{Type: "adapter", Payload: []byte(`{}`)}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.NoError(t, pool.GetContext(ctx, &count, "SELECT count(*) FROM asynqpg_tasks"))
	require.Equal(t, 1, count)
	before := closed.Load()
	require.NoError(t, conn.Close(ctx))
	require.NoError(t, conn.Close(ctx))
	require.Equal(t, before+1, closed.Load())
	require.NoError(t, pool.Close())
	require.NoError(t, pool.Close())
	require.NoError(t, native.Ping(ctx))
	_, err = pool.OpenNotificationConnection(ctx)
	require.ErrorIs(t, err, pgxadapter.ErrClosed)
}

func TestPoolCloseAcquisitionRace(t *testing.T) {
	t.Parallel()
	_, dsn := testutils.SetupTestDatabaseWithConnStr(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	native, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	defer native.Close()
	for range 10 {
		pool, err := pgxadapter.New(native)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		done := make(chan error, 1)
		go func() {
			conn, openErr := pool.OpenNotificationConnection(ctx)
			if openErr == nil {
				openErr = conn.Close(context.Background())
			}
			done <- openErr
		}()
		require.NoError(t, pool.Close())
		err = <-done
		if err != nil {
			require.ErrorIs(t, err, pgxadapter.ErrClosed)
		}
		cancel()
	}
	require.NoError(t, native.Ping(t.Context()))
	var listeners int
	require.NoError(t, native.QueryRow(t.Context(), "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'asynqpg-listener-%'").Scan(&listeners))
	require.Zero(t, listeners)
}

func TestCloseDuringAcquireClosesDetachedConnection(t *testing.T) {
	t.Parallel()
	_, dsn := testutils.SetupTestDatabaseWithConnStr(t)
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 1
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var closed atomic.Int32
	cfg.PrepareConn = func(ctx context.Context, _ *pgx.Conn) (bool, error) {
		if first.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return false, ctx.Err()
			}
		}
		return true, nil
	}
	cfg.BeforeClose = func(*pgx.Conn) { closed.Add(1) }
	native, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	defer native.Close()
	pool, err := pgxadapter.New(native)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		conn, openErr := pool.OpenNotificationConnection(ctx)
		if conn != nil {
			_ = conn.Close(context.Background())
		}
		done <- openErr
	}()
	<-entered
	require.NoError(t, pool.Close())
	close(release)
	require.ErrorIs(t, <-done, pgxadapter.ErrClosed)
	require.Equal(t, int32(1), closed.Load())
	require.NoError(t, native.Ping(ctx))
}

func TestBeforeConnectNotificationCompatibility(t *testing.T) {
	t.Parallel()
	for _, custom := range []bool{false, true} {
		t.Run(fmt.Sprintf("custom=%v", custom), func(t *testing.T) {
			t.Parallel()
			_, dsn := testutils.SetupTestDatabaseWithConnStr(t)
			cfg, err := pgxpool.ParseConfig(dsn)
			require.NoError(t, err)
			var called atomic.Int32
			cfg.BeforeConnect = func(_ context.Context, cfg *pgx.ConnConfig) error {
				if custom {
					cfg.OnNotification = func(*pgconn.PgConn, *pgconn.Notification) { called.Add(1) }
				}
				return nil
			}
			native, err := pgxpool.NewWithConfig(t.Context(), cfg)
			require.NoError(t, err)
			defer native.Close()
			pool, err := pgxadapter.New(native)
			require.NoError(t, err)
			defer pool.Close()
			conn, err := pool.OpenNotificationConnection(t.Context())
			require.NoError(t, err)
			defer conn.Close(context.Background())
			require.NoError(t, conn.Listen(t.Context(), "hook_compat"))
			_, err = pool.ExecContext(t.Context(), "SELECT pg_notify('hook_compat','ok')")
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			got, err := conn.WaitForNotification(ctx)
			if custom {
				require.ErrorIs(t, err, asynqpg.ErrNotificationsUnsupported)
				require.Equal(t, int32(1), called.Load())
			} else {
				require.NoError(t, err)
				require.Equal(t, "ok", got.Payload)
			}
		})
	}
}
