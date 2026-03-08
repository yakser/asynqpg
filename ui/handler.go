package ui

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
)

// handler holds all dependencies for the UI HTTP handler.
type handler struct {
	pool         asynqpg.Pool
	client       *client.Client
	repo         *repository
	logger       *slog.Logger
	opts         HandlerOpts
	sessionStore SessionStore
}

// NewHandler creates an http.Handler that serves the REST API and embedded SPA.
// The handler should be mounted at the configured prefix in the user's HTTP mux.
//
// Example:
//
//	h, err := ui.NewHandler(ui.HandlerOpts{
//	    Pool:   db,
//	    Prefix: "/asynqpg",
//	})
//	mux.Handle("/asynqpg/", h)
func NewHandler(opts HandlerOpts) (http.Handler, error) {
	opts.setDefaults()

	if err := opts.validate(); err != nil {
		return nil, err
	}

	cl, err := client.New(client.Config{
		Pool:   opts.Pool,
		Logger: opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("asynqpg/ui: create client: %w", err)
	}

	h := &handler{
		pool:         opts.Pool,
		client:       cl,
		repo:         newRepository(opts.Pool),
		logger:       opts.Logger,
		opts:         opts,
		sessionStore: opts.SessionStore,
	}

	mux := http.NewServeMux()
	h.registerRoutes(mux)

	var root http.Handler = mux

	if len(opts.AllowedOrigins) > 0 {
		root = corsMiddleware(opts.AllowedOrigins)(root)
	}

	// BasicAuth wraps the entire handler (including SPA) so the browser
	// shows its native auth dialog for all resources.
	if opts.BasicAuth != nil {
		root = basicAuthMiddleware(opts.BasicAuth.Username, opts.BasicAuth.Password)(root)
	}

	if opts.Prefix != "/" {
		root = http.StripPrefix(strings.TrimRight(opts.Prefix, "/"), root)
	}

	return root, nil
}

func (h *handler) registerRoutes(mux *http.ServeMux) {
	// Public routes (no auth required).
	mux.HandleFunc("GET /api/health", h.handleHealth)
	mux.HandleFunc("GET /api/config", h.handleConfig)

	if h.oauthEnabled() {
		mux.HandleFunc("GET /api/auth/providers", h.handleAuthProviders)
		mux.HandleFunc("GET /api/auth/login/{provider}", h.handleAuthLogin)
		mux.HandleFunc("GET /api/auth/callback/{provider}", h.handleAuthCallback)
	}

	// Protected API routes.
	protected := http.NewServeMux()
	protected.HandleFunc("GET /api/stats", h.handleStats)
	protected.HandleFunc("GET /api/task-types", h.handleTaskTypes)
	protected.HandleFunc("GET /api/tasks", h.handleListTasks)
	protected.HandleFunc("GET /api/tasks/{id}", h.handleGetTask)
	protected.HandleFunc("GET /api/tasks/{id}/payload", h.handleGetTaskPayload)
	protected.HandleFunc("POST /api/tasks/{id}/retry", h.handleRetryTask)
	protected.HandleFunc("POST /api/tasks/{id}/cancel", h.handleCancelTask)
	protected.HandleFunc("DELETE /api/tasks/{id}", h.handleDeleteTask)
	protected.HandleFunc("POST /api/tasks/bulk/retry", h.handleBulkRetry)
	protected.HandleFunc("POST /api/tasks/bulk/delete", h.handleBulkDelete)

	if h.oauthEnabled() {
		protected.HandleFunc("GET /api/auth/me", h.handleAuthMe)
		protected.HandleFunc("POST /api/auth/logout", h.handleAuthLogout)
	}

	var protectedHandler http.Handler = protected
	if h.oauthEnabled() {
		protectedHandler = sessionAuthMiddleware(h.sessionStore)(protected)
	}
	mux.Handle("/api/", protectedHandler)

	// Embedded SPA frontend (always public – login page is part of the SPA).
	mux.Handle("/", frontendHandler())
}

func (h *handler) oauthEnabled() bool {
	return len(h.opts.AuthProviders) > 0
}

// handleHealth returns the health status of the service.
func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := h.pool.PingContext(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":  "error",
			"message": "database unreachable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// configResponse is returned by the config endpoint for frontend runtime configuration.
type configResponse struct {
	Prefix               string `json:"prefix"`
	HidePayloadByDefault bool   `json:"hide_payload_by_default"`
	Version              string `json:"version"`
	AuthMode             string `json:"auth_mode"`
}

// handleConfig returns the runtime configuration for the SPA frontend.
func (h *handler) handleConfig(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, configResponse{
		Prefix:               h.opts.Prefix,
		HidePayloadByDefault: h.opts.HidePayloadByDefault,
		Version:              version,
		AuthMode:             h.opts.authMode(),
	})
}

const version = "0.1.0"

// parseTaskID extracts and validates the task ID from the URL path.
func parseTaskID(r *http.Request) (int64, error) {
	idStr := r.PathValue("id")
	if idStr == "" {
		return 0, fmt.Errorf("missing task id")
	}

	var id int64
	for _, c := range idStr {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid task id: %s", idStr)
		}
		id = id*10 + int64(c-'0')
	}

	if id <= 0 {
		return 0, fmt.Errorf("invalid task id: %s", idStr)
	}

	return id, nil
}
