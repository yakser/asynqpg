// Package testutils provides test helpers for asynqpg integration tests.
// This package is for testing only and should not be considered stable.
package testutils

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// todo: run integration tests with different postgres images?
	postgresImage    = "postgres:17-alpine"
	postgresUser     = "test"
	postgresPassword = "test"
	postgresDB       = "testdb"
)

// SetupTestDatabase creates a PostgreSQL test container and returns a connected *sqlx.DB.
func SetupTestDatabase(t *testing.T) *sqlx.DB {
	t.Helper()
	db, _ := SetupTestDatabaseWithConnStr(t)
	return db
}

// SetupTestDatabaseWithConnStr returns both the sqlx.DB and the raw connection string.
// Useful for tests that need to open additional connections (e.g., via sql.Open or pgx).
func SetupTestDatabaseWithConnStr(t *testing.T) (*sqlx.DB, string) {
	t.Helper()
	image := os.Getenv("ASYNQPG_TEST_POSTGRES_IMAGE")
	if image == "" {
		image = postgresImage
	}
	return setupDatabase(t, 10, image)
}

func setupDatabase(tb testing.TB, maxOpenConns int, image string) (*sqlx.DB, string) {
	tb.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		image,
		postgres.WithDatabase(postgresDB),
		postgres.WithUsername(postgresUser),
		postgres.WithPassword(postgresPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
			wait.ForListeningPort("5432/tcp"),
		),
	)
	require.NoError(tb, err, "failed to start postgres container")

	tb.Cleanup(func() {
		if terminateErr := pgContainer.Terminate(ctx); terminateErr != nil {
			tb.Logf("failed to terminate postgres container: %v", terminateErr)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(tb, err, "failed to get connection string")

	db, err := sqlx.Connect("postgres", connStr)
	require.NoError(tb, err, "failed to connect to test database")

	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxOpenConns / 2)
	db.SetConnMaxLifetime(time.Minute)

	applyMigrations(tb, db)

	tb.Cleanup(func() {
		err := db.Close()
		if err != nil {
			tb.Logf("failed to close database connection: %v", err)
		}
	})

	return db, connStr
}

// Task is a simplified representation of an asynqpg task for test assertions.
type Task struct {
	ID               int64          `db:"id"`
	Type             string         `db:"type"`
	IdempotencyToken *string        `db:"idempotency_token"`
	Payload          []byte         `db:"payload"`
	Status           string         `db:"status"`
	Messages         pq.StringArray `db:"messages"`
	BlockedTill      time.Time      `db:"blocked_till"`
	AttemptsLeft     int            `db:"attempts_left"`
	AttemptsElapsed  int            `db:"attempts_elapsed"`
}

// GetTaskByIdempotencyToken retrieves a task by its idempotency token for test assertions.
func GetTaskByIdempotencyToken(t *testing.T, db *sqlx.DB, token string) (*Task, error) {
	t.Helper()

	var task Task
	err := db.Get(&task, "SELECT id, type, idempotency_token, payload, status, messages, blocked_till, attempts_left, attempts_elapsed FROM asynqpg_tasks WHERE idempotency_token = $1", token)
	if err != nil {
		return nil, err
	}

	return &task, nil
}

func applyMigrations(tb testing.TB, db *sqlx.DB) {
	tb.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("failed to get current file path")
	}

	projectRoot := filepath.Dir(filepath.Dir(filename))
	migrationsDir := filepath.Join(projectRoot, "migrations")

	migrationFiles := []string{
		"001_initial.sql",
		"002_claim_generation.sql",
	}

	for _, file := range migrationFiles {
		migrationPath := filepath.Join(migrationsDir, file)
		migrationSQL, err := os.ReadFile(migrationPath)
		require.NoError(tb, err, "failed to read migration file %s", migrationPath)

		_, err = db.Exec(string(migrationSQL))
		require.NoError(tb, err, "failed to apply migration %s", file)
	}
}

// SetupTestDatabaseWithImage creates a test database using an explicit PostgreSQL image.
func SetupTestDatabaseWithImage(t *testing.T, image string) (*sqlx.DB, string) {
	t.Helper()
	return setupDatabase(t, 10, image)
}

// ApplyReadyNotifications installs LEGACY triggers for upgrade tests only.
// Normal notification tests and production deployments need no trigger installation.
func ApplyReadyNotifications(t testing.TB, db *sqlx.DB) {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	script, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "..", "migrations", "optional", "001_ready_notifications.up.sql"))
	require.NoError(t, err)
	tx, err := db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var schema string
	require.NoError(t, tx.Get(&schema, `SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = 'asynqpg_tasks'::regclass`))
	_, err = tx.Exec("SET LOCAL search_path TO " + pq.QuoteIdentifier(schema) + ", pg_catalog")
	require.NoError(t, err)
	_, err = tx.Exec(string(script))
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}
