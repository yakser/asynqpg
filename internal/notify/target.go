// Package notify receives readiness hints on a dedicated PostgreSQL session.
package notify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yakser/asynqpg"
)

var (
	ErrUnavailable  = errors.New("notification listener unavailable")
	ErrNotInstalled = errors.New("task table is not installed")
)

type Target struct {
	Database string `db:"database"`
	Schema   string `db:"schema"`
	Channel  string `db:"-"`
	TableOID uint32 `db:"table_oid"`
}

func ResolveTarget(ctx context.Context, pool asynqpg.Pool) (Target, error) {
	const query = `SELECT current_database() AS database, n.nspname AS schema, c.oid::bigint AS table_oid
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass('asynqpg_tasks')`
	var got Target
	if err := pool.GetContext(ctx, &got, query); err != nil {
		if ctx.Err() != nil {
			return Target{}, ctx.Err()
		}
		if errors.Is(err, sql.ErrNoRows) {
			return Target{}, ErrNotInstalled
		}
		return Target{}, ErrUnavailable
	}
	got.Channel = fmt.Sprintf("asynqpg_ready_v1_%d", got.TableOID)
	return got, nil
}
