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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
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
	return setupDatabase(t, 10)
}

func setupDatabase(tb testing.TB, maxOpenConns int) (*sqlx.DB, string) {
	tb.Helper()

	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		postgresImage,
		postgres.WithDatabase(postgresDB),
		postgres.WithUsername(postgresUser),
		postgres.WithPassword(postgresPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		tb.Fatalf("failed to start postgres container: %v", err)
	}

	tb.Cleanup(func() {
		if terminateErr := pgContainer.Terminate(ctx); terminateErr != nil {
			tb.Logf("failed to terminate postgres container: %v", terminateErr)
		}
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		tb.Fatalf("failed to get connection string: %v", err)
	}

	db, err := sqlx.Connect("postgres", connStr)
	if err != nil {
		tb.Fatalf("failed to connect to test database: %v", err)
	}

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
	}

	for _, file := range migrationFiles {
		migrationPath := filepath.Join(migrationsDir, file)
		migrationSQL, err := os.ReadFile(migrationPath)
		if err != nil {
			tb.Fatalf("failed to read migration file %s: %v", migrationPath, err)
		}

		_, err = db.Exec(string(migrationSQL))
		if err != nil {
			tb.Fatalf("failed to apply migration %s: %v", file, err)
		}
	}
}
