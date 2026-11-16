// Package pgxadapter adapts a caller-owned pgx pool to asynqpg's SQL and notification interfaces.
package pgxadapter

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"

	"github.com/yakser/asynqpg"
)

var (
	ErrInvalidConfig = errors.New("native pool is required")
	// ErrNotificationHandlerUnsupported rejects custom OnNotification and BeforeConnect hooks.
	// The latter can replace buffering on individual connection configurations.
	ErrNotificationHandlerUnsupported = fmt.Errorf("pgx OnNotification replaces required notification buffering: %w", asynqpg.ErrNotificationsUnsupported)
	ErrUnavailable                    = errors.New("notification connection unavailable")
	ErrClosed                         = errors.New("pool adapter is closed")
)

// Pool borrows a native pool; Close never closes that native pool.
type Pool struct {
	native   *pgxpool.Pool
	db       *sqlx.DB
	mu       sync.Mutex
	closed   bool
	closeErr error
}

// New wraps native without opening a connection or taking ownership of it.
// Pools with OnNotification or BeforeConnect hooks are unsupported; callbacks are never replaced.
func New(native *pgxpool.Pool) (*Pool, error) {
	if native == nil {
		return nil, ErrInvalidConfig
	}
	if native.Config().ConnConfig.OnNotification != nil {
		return nil, ErrNotificationHandlerUnsupported
	}
	return &Pool{native: native, db: sqlx.NewDb(stdlib.OpenDBFromPool(native), "pgx")}, nil
}

// Close closes the SQL bridge. Stop consumers first, then close the adapter and native pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		p.closeErr = p.db.Close()
	}
	return p.closeErr
}
func (p *Pool) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return p.db.ExecContext(ctx, q, args...)
}
func (p *Pool) SelectContext(ctx context.Context, dest any, q string, args ...any) error {
	return p.db.SelectContext(ctx, dest, q, args...)
}
func (p *Pool) GetContext(ctx context.Context, dest any, q string, args ...any) error {
	return p.db.GetContext(ctx, dest, q, args...)
}
func (p *Pool) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return p.db.QueryContext(ctx, q, args...)
}
func (p *Pool) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	return p.db.QueryRowContext(ctx, q, args...)
}
func (p *Pool) PingContext(ctx context.Context) error { return p.db.PingContext(ctx) }
func (p *Pool) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	return p.db.BeginTxx(ctx, opts)
}

// OpenNotificationConnection transfers a detached session to the caller.
func (p *Pool) OpenNotificationConnection(ctx context.Context) (asynqpg.NotificationConnection, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	lease, err := p.native.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire notification session: %w", safeError(ctx, err))
	}
	conn := &notificationConnection{conn: lease.Hijack(), beforeClose: p.native.Config().BeforeClose}
	success := false
	defer func() {
		if !success {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = conn.Close(cleanup)
		}
	}()
	if conn.conn.PgConn().TxStatus() != 'I' {
		return nil, fmt.Errorf("session transaction state: %w", ErrUnavailable)
	}
	if _, err = conn.conn.Exec(ctx, "UNLISTEN *"); err != nil {
		return nil, fmt.Errorf("reset subscriptions: %w", safeError(ctx, err))
	}
	// A distinctive label allows monitoring/reconnect tests without changing pool config.
	if _, err = conn.conn.Exec(ctx, "SELECT set_config('application_name', $1, false)", fmt.Sprintf("asynqpg-listener-%d", conn.conn.PgConn().PID())); err != nil {
		return nil, fmt.Errorf("set listener label: %w", safeError(ctx, err))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	success = true
	return conn, nil
}
func safeError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrUnavailable
}
