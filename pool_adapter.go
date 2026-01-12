package asynqpg

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

// Compile-time interface satisfaction checks.
var (
	_ Pool    = (*sqlx.DB)(nil)
	_ Querier = (*sqlx.Tx)(nil)
)

// WrapStdDB wraps a standard *sql.DB into a Pool.
// It uses sqlx internally for struct scanning (SelectContext, GetContext).
// The driverName parameter should match the driver used to open the database
// (e.g., "postgres", "pgx").
func WrapStdDB(db *sql.DB, driverName string) Pool {
	return sqlx.NewDb(db, driverName)
}
