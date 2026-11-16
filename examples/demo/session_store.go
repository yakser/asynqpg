package main

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jmoiron/sqlx"

	"github.com/yakser/asynqpg/ui"
	"github.com/yakser/asynqpg/ui/uiauth"
)

//go:embed sessions.sql
var sessionsSQL string

const sessionCleanupInterval = 60 * time.Second

// ApplySessionsMigration creates the asynqpg_demo_sessions table if missing.
// Safe to call on every startup – the embedded SQL uses IF NOT EXISTS.
func ApplySessionsMigration(ctx context.Context, db *sqlx.DB) error {
	if _, err := db.ExecContext(ctx, sessionsSQL); err != nil {
		return fmt.Errorf("apply sessions migration: %w", err)
	}
	return nil
}

// PostgresSessionStore is a ui.SessionStore implementation backed by PostgreSQL.
// Sessions survive demo restarts and can be shared across multiple replicas.
type PostgresSessionStore struct {
	db     *sqlx.DB
	logger *slog.Logger
	cancel context.CancelFunc
	done   chan struct{}
}

var _ ui.SessionStore = (*PostgresSessionStore)(nil)

// NewPostgresSessionStore starts a background cleanup goroutine bound to ctx
// and returns a ready-to-use store. Call Close to stop the goroutine.
func NewPostgresSessionStore(ctx context.Context, db *sqlx.DB, logger *slog.Logger) *PostgresSessionStore {
	cleanupCtx, cancel := context.WithCancel(ctx)
	s := &PostgresSessionStore{
		db:     db,
		logger: logger,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go s.cleanupLoop(cleanupCtx)
	return s
}

type sessionRow struct {
	Token         string    `db:"token"`
	UserID        string    `db:"user_id"`
	UserProvider  string    `db:"user_provider"`
	UserName      string    `db:"user_name"`
	UserEmail     string    `db:"user_email"`
	UserAvatarURL string    `db:"user_avatar_url"`
	CreatedAt     time.Time `db:"created_at"`
	ExpiresAt     time.Time `db:"expires_at"`
}

func (r sessionRow) toSession() *uiauth.Session {
	return &uiauth.Session{
		Token: r.Token,
		User: uiauth.User{
			ID:        r.UserID,
			Provider:  r.UserProvider,
			Name:      r.UserName,
			AvatarURL: r.UserAvatarURL,
			Email:     r.UserEmail,
		},
		CreatedAt: r.CreatedAt,
		ExpiresAt: r.ExpiresAt,
	}
}

// Get retrieves a session by token. Returns uiauth.ErrSessionNotFound if not found or expired.
func (s *PostgresSessionStore) Get(ctx context.Context, token string) (*uiauth.Session, error) {
	var row sessionRow
	err := s.db.GetContext(ctx, &row, `
		SELECT token, user_id, user_provider, user_name, user_email, user_avatar_url, created_at, expires_at
		FROM asynqpg_demo_sessions
		WHERE token = $1
	`, token)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, uiauth.ErrSessionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}

	if time.Now().After(row.ExpiresAt) {
		if _, delErr := s.db.ExecContext(ctx, `DELETE FROM asynqpg_demo_sessions WHERE token = $1`, token); delErr != nil {
			s.logger.Warn("delete expired session", "error", delErr)
		}
		return nil, uiauth.ErrSessionNotFound
	}

	return row.toSession(), nil
}

// Save upserts a session keyed by Token.
func (s *PostgresSessionStore) Save(ctx context.Context, session *uiauth.Session) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO asynqpg_demo_sessions
			(token, user_id, user_provider, user_name, user_email, user_avatar_url, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (token) DO UPDATE SET
			user_id         = EXCLUDED.user_id,
			user_provider   = EXCLUDED.user_provider,
			user_name       = EXCLUDED.user_name,
			user_email      = EXCLUDED.user_email,
			user_avatar_url = EXCLUDED.user_avatar_url,
			created_at      = EXCLUDED.created_at,
			expires_at      = EXCLUDED.expires_at
	`,
		session.Token,
		session.User.ID,
		session.User.Provider,
		session.User.Name,
		session.User.Email,
		session.User.AvatarURL,
		session.CreatedAt,
		session.ExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("save session: %w", err)
	}
	return nil
}

// Delete removes a session by token. No error if the token doesn't exist.
func (s *PostgresSessionStore) Delete(ctx context.Context, token string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM asynqpg_demo_sessions WHERE token = $1`, token); err != nil {
		return fmt.Errorf("delete session: %w", err)
	}
	return nil
}

// Close stops the background cleanup goroutine. Safe to call multiple times.
func (s *PostgresSessionStore) Close() {
	s.cancel()
	<-s.done
}

func (s *PostgresSessionStore) cleanupLoop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(sessionCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.db.ExecContext(ctx, `DELETE FROM asynqpg_demo_sessions WHERE expires_at < NOW()`); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				s.logger.Error("cleanup expired sessions", "error", err)
			}
		}
	}
}
