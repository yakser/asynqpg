package asynqpg

import (
	"context"
	"database/sql"
)

// Querier can execute queries and scan results into structs.
// Both database connection pools and transactions satisfy this interface.
// *sqlx.DB and *sqlx.Tx implement it natively.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Pool represents a database connection pool.
// *sqlx.DB satisfies this interface natively.
type Pool interface {
	Querier
	PingContext(ctx context.Context) error
}
