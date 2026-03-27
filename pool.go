package asynqpg

import (
	"context"
	"database/sql"
)

// Querier can execute queries and scan results into structs.
// Both database connection pools and transactions satisfy this interface.
// *sqlx.DB and *sqlx.Tx implement it natively.
//
//go:generate go tool mockery --case underscore --with-expecter --name Querier
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Tx represents a database transaction.
// *sql.Tx and *sqlx.Tx satisfy this interface natively.
//
//go:generate go tool mockery --case underscore --with-expecter --name Tx
type Tx interface {
	Querier
	Commit() error
	Rollback() error
}

// Pool represents a database connection pool.
// *sqlx.DB satisfies this interface natively.
//
//go:generate go tool mockery --case underscore --with-expecter --name Pool
type Pool interface {
	Querier
	PingContext(ctx context.Context) error
}
