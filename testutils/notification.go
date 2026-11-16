package testutils

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/adapter/pgxadapter"
)

// NotificationPool connects a test adapter to the fixture database.
func NotificationPool(t testing.TB, dsn string) *pgxadapter.Pool {
	t.Helper()
	native, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(native.Close)
	pool, err := pgxadapter.New(native)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	return pool
}
