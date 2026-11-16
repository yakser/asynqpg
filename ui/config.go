package ui

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/yakser/asynqpg"
)

const defaultSessionMaxAge = 24 * time.Hour

// Maintenance defaults. These mirror the consumer's effective defaults
// (see consumer.Config) so the preview matches the real Rescuer/Cleaner when
// HandlerOpts.Maintenance is left unset.
const (
	defaultMaintStuckThreshold     = 5 * time.Minute
	defaultMaintCompletedRetention = 24 * time.Hour
	defaultMaintFailedRetention    = 7 * 24 * time.Hour
	defaultMaintCancelledRetention = 24 * time.Hour
	defaultMaintInterval           = 30 * time.Second
)

//go:generate go tool mockery --case underscore --with-expecter --exported --name pool
type pool = asynqpg.Pool

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

	// ConsumerInfoFunc optionally returns live snapshots of the consumer
	// instances running in the same process as this UI handler. It powers the
	// Workers page. When nil (e.g. a standalone dashboard that only holds a DB
	// pool), the Workers page shows an empty state. The function is called on
	// each request, so it always reflects current worker utilization.
	//
	// Note: this only ever reflects consumers attached in-process; it does not
	// discover consumers running on other nodes.
	ConsumerInfoFunc func() []asynqpg.ConsumerInfo

	// Maintenance supplies the dashboard's assumed maintenance settings. It powers the
	// Maintenance page: the values are displayed as-is and used to preview what
	// the Rescuer and Cleaner would act on right now (read-only count queries).
	//
	// When nil, library defaults are used. Keep these in sync with the
	// consumer.Config used to run maintenance, otherwise the preview will not
	// match what the leader actually does.
	Maintenance *MaintenanceConfig

	// Version is the version string returned by /api/config and shown in
	// the dashboard. When empty, it is auto-detected from runtime build
	// info (debug.ReadBuildInfo); the detected value is the asynqpg/ui
	// module version of the binary that imported this package, or
	// "(devel)" for local/replaced builds. Set this explicitly when you
	// build with linker flags such as `-ldflags '-X my/pkg.Version=...'`
	// and want that value surfaced in the dashboard.
	Version string
}

// BasicAuth holds credentials for HTTP Basic Authentication.
type BasicAuth struct {
	Username string
	Password string
}

// MaintenanceConfig supplies assumed maintenance settings for display and
// preview on the Maintenance page.
type MaintenanceConfig struct {
	// StuckThreshold is the duration after which a running task is considered
	// stuck and eligible for rescue (the Rescuer's RescueAfter). Default: 5m.
	StuckThreshold time.Duration
	// CompletedRetention is how long completed tasks are kept. Default: 24h.
	CompletedRetention time.Duration
	// FailedRetention is how long failed tasks are kept. Default: 7d.
	FailedRetention time.Duration
	// CancelledRetention is how long cancelled tasks are kept. Default: 24h.
	CancelledRetention time.Duration
	// Interval is how often the Rescuer and Cleaner run. Display-only. Default: 30s.
	Interval time.Duration
}

// effectiveMaintenance returns the maintenance settings with defaults applied.
func (o *HandlerOpts) effectiveMaintenance() MaintenanceConfig {
	var cfg MaintenanceConfig
	if o.Maintenance != nil {
		cfg = *o.Maintenance
	}
	if cfg.StuckThreshold <= 0 {
		cfg.StuckThreshold = defaultMaintStuckThreshold
	}
	if cfg.CompletedRetention <= 0 {
		cfg.CompletedRetention = defaultMaintCompletedRetention
	}
	if cfg.FailedRetention <= 0 {
		cfg.FailedRetention = defaultMaintFailedRetention
	}
	if cfg.CancelledRetention <= 0 {
		cfg.CancelledRetention = defaultMaintCancelledRetention
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultMaintInterval
	}
	return cfg
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

	if o.Version == "" {
		o.Version = detectVersion()
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
