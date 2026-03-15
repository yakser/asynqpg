package testutils

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	// register postgres driver
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yakser/asynqpg/internal/repository"
)

const (
	postgresImage    = "postgres:17-alpine"
	postgresUser     = "test"
	postgresPassword = "test"
	postgresDB       = "testdb"
)

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

// DBOption configures test database setup.
type DBOption func(*dbConfig)

type dbConfig struct {
	maxOpenConns int
}

// WithMaxOpenConns sets the maximum number of open connections for the test database pool.
func WithMaxOpenConns(n int) DBOption {
	return func(c *dbConfig) {
		c.maxOpenConns = n
	}
}

// SetupBenchDatabase creates a test database suitable for benchmarks.
// Accepts testing.TB so it works with both *testing.T and *testing.B.
func SetupBenchDatabase(tb testing.TB, opts ...DBOption) (*sqlx.DB, string) {
	tb.Helper()

	cfg := &dbConfig{maxOpenConns: 50}
	for _, opt := range opts {
		opt(cfg)
	}

	return setupDatabase(tb, cfg.maxOpenConns)
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

func applyMigrations(tb testing.TB, db *sqlx.DB) {
	tb.Helper()

	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("failed to get current file path")
	}

	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(filename))))
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

func GetTaskByIdempotencyToken(t *testing.T, db *sqlx.DB, token string) (*repository.Task, error) {
	t.Helper()

	var task repository.Task
	err := db.Get(&task, "SELECT id, type, idempotency_token, payload, status, messages, blocked_till, attempts_left, attempts_elapsed FROM asynqpg_tasks WHERE idempotency_token = $1", token)
	if err != nil {
		return nil, err
	}

	return &task, nil
}
