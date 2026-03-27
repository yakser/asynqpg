package ui

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/yakser/asynqpg/ui/uiauth"
)

//go:generate go tool mockery --case underscore --with-expecter --exported --name AuthProvider

// AuthProvider defines the interface for OAuth/SSO authentication providers.
// Implementations handle the specific OAuth flow for a given identity provider.
type AuthProvider interface {
	// ID returns a unique identifier for this provider (e.g. "github", "google", "okta").
	ID() string
	// DisplayName returns a human-readable name shown on the login button.
	DisplayName() string
	// IconURL returns an optional URL/path for the provider icon. Empty string means no icon.
	IconURL() string
	// BeginAuth starts the OAuth flow by redirecting the user to the provider's authorization page.
	// callbackURL is the full URL the provider should redirect back to after authorization.
	// state is the CSRF token that must be included in the authorization request.
	BeginAuth(w http.ResponseWriter, r *http.Request, callbackURL string, state string)
	// CompleteAuth handles the OAuth callback. It exchanges the authorization code for tokens,
	// fetches user information, and returns the authenticated user.
	CompleteAuth(w http.ResponseWriter, r *http.Request) (*uiauth.User, error)
}

//go:generate go tool mockery --case underscore --with-expecter --exported --name SessionStore

// SessionStore manages user sessions. The default in-memory implementation
// is suitable for single-server deployments. For multi-server setups,
// provide a shared store (e.g. PostgreSQL, Redis).
type SessionStore interface {
	Get(ctx context.Context, token string) (*uiauth.Session, error)
	Save(ctx context.Context, session *uiauth.Session) error
	Delete(ctx context.Context, token string) error
}

// contextKey is an unexported type for context keys in this package.
type contextKey int

const ctxKeyUser contextKey = iota

// UserFromContext returns the authenticated user from the request context,
// or nil if the request is not authenticated.
func UserFromContext(ctx context.Context) *uiauth.User {
	u, _ := ctx.Value(ctxKeyUser).(*uiauth.User)
	return u
}

func withUser(ctx context.Context, user *uiauth.User) context.Context {
	return context.WithValue(ctx, ctxKeyUser, user)
}

// MemorySessionStore is an in-memory SessionStore implementation.
// Sessions are lost on server restart. For production multi-server
// deployments, use a shared store.
type MemorySessionStore struct {
	sessions sync.Map
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewMemorySessionStore creates a new in-memory session store with
// a background cleanup goroutine that removes expired sessions.
func NewMemorySessionStore() *MemorySessionStore {
	ctx, cancel := context.WithCancel(context.Background())
	s := &MemorySessionStore{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go s.cleanupLoop(ctx)
	return s
}

func (s *MemorySessionStore) cleanupLoop(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.removeExpired()
		}
	}
}

func (s *MemorySessionStore) removeExpired() {
	now := time.Now()
	s.sessions.Range(func(key, value any) bool {
		sess, ok := value.(*uiauth.Session)
		if ok && now.After(sess.ExpiresAt) {
			s.sessions.Delete(key)
		}
		return true
	})
}

// Get retrieves a session by token. Returns ErrSessionNotFound if not found or expired.
func (s *MemorySessionStore) Get(_ context.Context, token string) (*uiauth.Session, error) {
	val, ok := s.sessions.Load(token)
	if !ok {
		return nil, uiauth.ErrSessionNotFound
	}

	sess, ok := val.(*uiauth.Session)
	if !ok {
		return nil, uiauth.ErrSessionNotFound
	}

	if time.Now().After(sess.ExpiresAt) {
		s.sessions.Delete(token)
		return nil, uiauth.ErrSessionNotFound
	}

	return sess, nil
}

// Save stores a session. The session's Token field is used as the key.
func (s *MemorySessionStore) Save(_ context.Context, session *uiauth.Session) error {
	s.sessions.Store(session.Token, session)
	return nil
}

// Delete removes a session by token. No error is returned if the token doesn't exist.
func (s *MemorySessionStore) Delete(_ context.Context, token string) error {
	s.sessions.Delete(token)
	return nil
}

// Close stops the background cleanup goroutine. Safe to call multiple times.
func (s *MemorySessionStore) Close() {
	s.cancel()
	<-s.done
}

const sessionTokenBytes = 32

// generateSessionToken creates a cryptographically random session token.
func generateSessionToken() (string, error) {
	b := make([]byte, sessionTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate session token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
