package ui

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/yakser/asynqpg"
)

const defaultSessionMaxAge = 24 * time.Hour

// HandlerOpts configures the UI handler.
type HandlerOpts struct {
	// Pool is a database connection pool (required).
	Pool asynqpg.Pool

	// Prefix is the URL prefix for the handler (e.g. "/asynqpg").
	// Defaults to "/".
	Prefix string

	// Logger is used for logging. If nil, slog.Default() is used.
	Logger *slog.Logger

	// BasicAuth enables optional HTTP Basic Authentication.
	// If nil, no built-in authentication is applied.
	// Mutually exclusive with AuthProviders.
	BasicAuth *BasicAuth

	// AuthProviders configures OAuth/SSO authentication providers.
	// When set, session-based authentication is enabled and a login page is shown.
	// Mutually exclusive with BasicAuth.
	AuthProviders []AuthProvider

	// SessionStore manages user sessions when AuthProviders is set.
	// If nil and AuthProviders is non-empty, an in-memory store is used.
	SessionStore SessionStore

	// SessionMaxAge controls the maximum session duration.
	// Defaults to 24 hours.
	SessionMaxAge time.Duration

	// SecureCookies controls the Secure flag on session cookies.
	// Should be true in production (HTTPS). Defaults to false.
	SecureCookies bool

	// HidePayloadByDefault controls whether task payloads are hidden
	// in list and detail responses. Users can still fetch payloads
	// via the dedicated payload endpoint.
	HidePayloadByDefault bool

	// AllowedOrigins configures CORS allowed origins.
	// If empty, only same-origin requests are allowed.
	AllowedOrigins []string
}

// BasicAuth holds credentials for HTTP Basic Authentication.
type BasicAuth struct {
	Username string
	Password string
}

func (o *HandlerOpts) validate() error {
	if o.Pool == nil {
		return fmt.Errorf("asynqpg/ui: pool is required")
	}

	if o.BasicAuth != nil && len(o.AuthProviders) > 0 {
		return fmt.Errorf("asynqpg/ui: basic auth and OAuth providers are mutually exclusive")
	}

	if o.BasicAuth != nil {
		if o.BasicAuth.Username == "" {
			return fmt.Errorf("asynqpg/ui: basic auth username is required")
		}
		if o.BasicAuth.Password == "" {
			return fmt.Errorf("asynqpg/ui: basic auth password is required")
		}
	}

	if len(o.AuthProviders) > 0 {
		seen := make(map[string]struct{}, len(o.AuthProviders))
		for _, p := range o.AuthProviders {
			id := p.ID()
			if _, ok := seen[id]; ok {
				return fmt.Errorf("asynqpg/ui: duplicate auth provider ID: %s", id)
			}
			seen[id] = struct{}{}
		}
	}

	return nil
}

func (o *HandlerOpts) setDefaults() {
	if o.Prefix == "" {
		o.Prefix = "/"
	}

	if o.Logger == nil {
		o.Logger = slog.Default()
	}

	if len(o.AuthProviders) > 0 {
		if o.SessionStore == nil {
			o.SessionStore = NewMemorySessionStore()
		}
		if o.SessionMaxAge <= 0 {
			o.SessionMaxAge = defaultSessionMaxAge
		}
	}
}

// authMode returns the authentication mode string for the config API.
func (o *HandlerOpts) authMode() string {
	if len(o.AuthProviders) > 0 {
		return "oauth"
	}
	if o.BasicAuth != nil {
		return "basic"
	}
	return "none"
}
