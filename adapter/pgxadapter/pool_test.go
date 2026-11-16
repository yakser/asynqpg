package pgxadapter_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/adapter/pgxadapter"
)

func TestNilPool(t *testing.T) {
	t.Parallel()
	_, err := pgxadapter.New(nil)
	require.ErrorIs(t, err, pgxadapter.ErrInvalidConfig)
}

var _ asynqpg.NotificationPool = (*pgxadapter.Pool)(nil)

func TestCustomNotificationHandlerRejectedBeforeAcquire(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://localhost/unused")
	require.NoError(t, err)
	cfg.ConnConfig.OnNotification = func(*pgconn.PgConn, *pgconn.Notification) {}
	native, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	defer native.Close()
	_, err = pgxadapter.New(native)
	require.ErrorIs(t, err, pgxadapter.ErrNotificationHandlerUnsupported)
	require.Zero(t, native.Stat().TotalConns())
}

func TestBeforeConnectAcceptedWithoutInvokingOrReplacingHook(t *testing.T) {
	t.Parallel()
	cfg, err := pgxpool.ParseConfig("postgres://localhost/unused")
	require.NoError(t, err)
	cfg.BeforeConnect = func(context.Context, *pgx.ConnConfig) error {
		t.Error("hook must not be called by adapter validation")
		return nil
	}
	native, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	defer native.Close()
	pool, err := pgxadapter.New(native)
	require.NoError(t, err)
	defer pool.Close()
	require.NotNil(t, native.Config().BeforeConnect)
	require.Zero(t, native.Stat().TotalConns())
}
