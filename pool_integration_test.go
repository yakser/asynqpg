//go:build integration
// +build integration

package asynqpg_test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
	"github.com/yakser/asynqpg/producer"
	"github.com/yakser/asynqpg/testutils"
)

func TestPoolInterfaceSatisfaction(t *testing.T) {
	// Compile-time checks (also in pool_adapter.go, repeated here for clarity).
	var _ asynqpg.Pool = (*sqlx.DB)(nil)
	var _ asynqpg.Querier = (*sqlx.Tx)(nil)
}

func TestWrapStdDB_ProducerAndClient(t *testing.T) {
	_, connStr := testutils.SetupTestDatabaseWithConnStr(t)

	// Open a raw *sql.DB using lib/pq driver.
	stdDB, err := sql.Open("postgres", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { stdDB.Close() })

	require.NoError(t, stdDB.Ping())

	// Wrap it into asynqpg.Pool.
	pool := asynqpg.WrapStdDB(stdDB, "postgres")

	// Create producer using wrapped pool.
	p, err := producer.New(producer.Config{Pool: pool})
	require.NoError(t, err)

	ctx := context.Background()

	// Enqueue a task.
	task := asynqpg.NewTask("wrap-test", []byte(`{"wrapped":true}`), asynqpg.WithMaxRetry(2))
	_, err = p.Enqueue(ctx, task)
	require.NoError(t, err)

	// Create client using the same wrapped pool.
	c, err := client.New(client.Config{Pool: pool})
	require.NoError(t, err)

	// List tasks and verify.
	result, err := c.ListTasks(ctx, nil)
	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)

	assert.Equal(t, "wrap-test", result.Tasks[0].Type)
	assert.Equal(t, asynqpg.TaskStatusPending, result.Tasks[0].Status)
	assert.Equal(t, []byte(`{"wrapped":true}`), result.Tasks[0].Payload)
}

func TestWrapStdDB_PgxDriver(t *testing.T) {
	_, connStr := testutils.SetupTestDatabaseWithConnStr(t)

	// Open a raw *sql.DB using pgx/stdlib driver.
	stdDB, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { stdDB.Close() })

	require.NoError(t, stdDB.Ping())

	// Wrap into pool.
	pool := asynqpg.WrapStdDB(stdDB, "pgx")

	// Producer with pgx driver.
	p, err := producer.New(producer.Config{Pool: pool})
	require.NoError(t, err)

	ctx := context.Background()

	task := asynqpg.NewTask("pgx-test", []byte(`{"driver":"pgx"}`), asynqpg.WithMaxRetry(1))
	_, err = p.Enqueue(ctx, task)
	require.NoError(t, err)

	// Client with pgx driver.
	c, err := client.New(client.Config{Pool: pool})
	require.NoError(t, err)

	result, err := c.ListTasks(ctx, nil)
	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)
	assert.Equal(t, "pgx-test", result.Tasks[0].Type)
}

func TestIndependentPools_ProducerAndClient(t *testing.T) {
	// Each component gets its own connection pool to the same database.
	// Verifies that producer and client can work with separate pools.
	db1, connStr := testutils.SetupTestDatabaseWithConnStr(t)

	// Second pool to the same database.
	db2, err := sqlx.Connect("postgres", connStr)
	require.NoError(t, err)
	db2.SetMaxOpenConns(5)
	t.Cleanup(func() { db2.Close() })

	// Producer uses pool 1.
	p, err := producer.New(producer.Config{Pool: db1})
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		task := asynqpg.NewTask("multi-pool-test", []byte(`{"i":1}`), asynqpg.WithMaxRetry(1))
		_, err = p.Enqueue(ctx, task)
		require.NoError(t, err)
	}

	// Client uses pool 2 (separate connection pool).
	c, err := client.New(client.Config{Pool: db2})
	require.NoError(t, err)

	result, err := c.ListTasks(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, 5, result.Total)
	assert.Len(t, result.Tasks, 5)

	// Verify individual task via client on pool 2.
	info, err := c.GetTask(ctx, result.Tasks[0].ID)
	require.NoError(t, err)
	assert.Equal(t, "multi-pool-test", info.Type)
	assert.Equal(t, asynqpg.TaskStatusPending, info.Status)
}

func TestMixedDriverPools(t *testing.T) {
	// Producer uses lib/pq (via sqlx), Client uses pgx/stdlib (via WrapStdDB).
	// Verifies cross-driver compatibility.
	db, connStr := testutils.SetupTestDatabaseWithConnStr(t)

	// Producer with lib/pq pool.
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)

	ctx := context.Background()

	task := asynqpg.NewTask("cross-driver", []byte(`{"cross":true}`), asynqpg.WithMaxRetry(3))
	_, err = p.Enqueue(ctx, task)
	require.NoError(t, err)

	// Client with pgx driver via WrapStdDB.
	pgxDB, err := sql.Open("pgx", connStr)
	require.NoError(t, err)
	t.Cleanup(func() { pgxDB.Close() })

	pgxPool := asynqpg.WrapStdDB(pgxDB, "pgx")

	c, err := client.New(client.Config{Pool: pgxPool})
	require.NoError(t, err)

	result, err := c.ListTasks(ctx, nil)
	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)

	assert.Equal(t, "cross-driver", result.Tasks[0].Type)
	assert.Equal(t, asynqpg.TaskStatusPending, result.Tasks[0].Status)
	assert.Equal(t, []byte(`{"cross":true}`), result.Tasks[0].Payload)
}
