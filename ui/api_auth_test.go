package ui

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAuthProvider implements AuthProvider for testing.
type mockAuthProvider struct {
	id          string
	displayName string
	iconURL     string
	beginFn     func(w http.ResponseWriter, r *http.Request, callbackURL string, state string)
	completeFn  func(w http.ResponseWriter, r *http.Request) (*User, error)
}

func (m *mockAuthProvider) ID() string          { return m.id }
func (m *mockAuthProvider) DisplayName() string { return m.displayName }
func (m *mockAuthProvider) IconURL() string     { return m.iconURL }

func (m *mockAuthProvider) BeginAuth(w http.ResponseWriter, r *http.Request, callbackURL string, state string) {
	if m.beginFn != nil {
		m.beginFn(w, r, callbackURL, state)
		return
	}
	http.Redirect(w, r, "https://provider.example.com/auth?state="+state, http.StatusFound)
}

func (m *mockAuthProvider) CompleteAuth(w http.ResponseWriter, r *http.Request) (*User, error) {
	if m.completeFn != nil {
		return m.completeFn(w, r)
	}
	return &User{ID: "123", Provider: m.id, Name: "Test User", AvatarURL: "https://example.com/avatar.png", Email: "test@example.com"}, nil
}

func newTestHandlerWithOAuth(t *testing.T, providers ...AuthProvider) http.Handler {
	t.Helper()
	h, err := NewHandler(HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: providers,
	})
	require.NoError(t, err)
	return h
}

func TestHandleAuthProviders_ReturnsList(t *testing.T) {
	t.Parallel()

	h := newTestHandlerWithOAuth(t,
		&mockAuthProvider{id: "github", displayName: "GitHub", iconURL: "https://github.com/icon.png"},
		&mockAuthProvider{id: "google", displayName: "Google"},
	)

	req := httptest.NewRequest(http.MethodGet, "/api/auth/providers", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	data, ok := resp.Data.([]any)
	require.True(t, ok)
	assert.Len(t, data, 2)

	first := data[0].(map[string]any)
	assert.Equal(t, "github", first["id"])
	assert.Equal(t, "GitHub", first["name"])
	assert.Equal(t, "https://github.com/icon.png", first["icon_url"])
	assert.Equal(t, "/api/auth/login/github", first["login_url"])

	second := data[1].(map[string]any)
	assert.Equal(t, "google", second["id"])
}

func TestHandleAuthProviders_Empty(t *testing.T) {
	t.Parallel()

	// With no providers, the endpoint won't be registered, so we test
	// the handler method directly.
	hObj := &handler{
		opts: HandlerOpts{AuthProviders: nil},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/auth/providers", nil)
	rec := httptest.NewRecorder()
	hObj.handleAuthProviders(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	data, ok := resp.Data.([]any)
	require.True(t, ok)
	assert.Len(t, data, 0)
}

func TestHandleAuthMe_Authenticated(t *testing.T) {
	t.Parallel()

	hObj := &handler{}
	user := &User{ID: "42", Provider: "github", Name: "Alice", AvatarURL: "https://example.com/a.png", Email: "alice@example.com"}

	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	req = req.WithContext(withUser(req.Context(), user))
	rec := httptest.NewRecorder()

	hObj.handleAuthMe(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	data, ok := resp.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "42", data["id"])
	assert.Equal(t, "Alice", data["name"])
	assert.Equal(t, "github", data["provider"])
}

func TestHandleAuthMe_Unauthenticated(t *testing.T) {
	t.Parallel()

	hObj := &handler{}
	req := httptest.NewRequest(http.MethodGet, "/api/auth/me", nil)
	rec := httptest.NewRecorder()

	hObj.handleAuthMe(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHandleAuthLogin_ValidProvider(t *testing.T) {
	t.Parallel()

	var capturedCallbackURL string
	provider := &mockAuthProvider{
		id:          "github",
		displayName: "GitHub",
		beginFn: func(w http.ResponseWriter, r *http.Request, callbackURL string, state string) {
			capturedCallbackURL = callbackURL
			http.Redirect(w, r, "https://github.com/login/oauth/authorize?state="+state, http.StatusFound)
		},
	}

	h := newTestHandlerWithOAuth(t, provider)
	req := httptest.NewRequest(http.MethodGet, "/api/auth/login/github", nil)
	req.Host = "localhost:8080"
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusFound, rec.Code)
	assert.Contains(t, capturedCallbackURL, "/api/auth/callback/github")

	// State cookie should be set.
	cookies := rec.Result().Cookies()
	var stateCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == oauthStateCookieName {
			stateCookie = c
			break
		}
	}
	require.NotNil(t, stateCookie, "state cookie should be set")
	assert.True(t, stateCookie.HttpOnly)
	assert.NotEmpty(t, stateCookie.Value)
}

func TestHandleAuthLogin_UnknownProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandlerWithOAuth(t, &mockAuthProvider{id: "github", displayName: "GitHub"})

	req := httptest.NewRequest(http.MethodGet, "/api/auth/login/unknown", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleAuthCallback_Success(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{
		id:          "github",
		displayName: "GitHub",
		completeFn: func(_ http.ResponseWriter, _ *http.Request) (*User, error) {
			return &User{ID: "123", Provider: "github", Name: "Alice"}, nil
		},
	}

	store := NewMemorySessionStore()
	defer store.Close()

	hObj := &handler{
		opts: HandlerOpts{
			AuthProviders: []AuthProvider{provider},
			SessionStore:  store,
			SessionMaxAge: time.Hour,
			Prefix:        "/",
		},
		sessionStore: store,
		logger:       testLogger(),
	}

	state := "test-state-value"
	req := httptest.NewRequest(http.MethodGet, "/api/auth/callback/github?code=abc&state="+state, nil)
	req.SetPathValue("provider", "github")
	req.AddCookie(&http.Cookie{Name: oauthStateCookieName, Value: state})
	rec := httptest.NewRecorder()

	hObj.handleAuthCallback(rec, req)

	assert.Equal(t, http.StatusFound, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "/dashboard")

	// Session cookie should be set.
	var sessionCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionCookie = c
			break
		}
	}
	require.NotNil(t, sessionCookie, "session cookie should be set")
	assert.True(t, sessionCookie.HttpOnly)
	assert.NotEmpty(t, sessionCookie.Value)
}

func TestHandleAuthCallback_InvalidState(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{id: "github", displayName: "GitHub"}
	hObj := &handler{
		opts:   HandlerOpts{AuthProviders: []AuthProvider{provider}, Prefix: "/"},
		logger: testLogger(),
	}

	req := httptest.NewRequest(http.MethodGet, "/api/auth/callback/github?code=abc&state=wrong", nil)
	req.SetPathValue("provider", "github")
	req.AddCookie(&http.Cookie{Name: oauthStateCookieName, Value: "correct"})
	rec := httptest.NewRecorder()

	hObj.handleAuthCallback(rec, req)

	assert.Equal(t, http.StatusFound, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "/login")
	assert.Contains(t, rec.Header().Get("Location"), "error=invalid_state")
}

func TestHandleAuthCallback_MissingStateCookie(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{id: "github", displayName: "GitHub"}
	hObj := &handler{
		opts:   HandlerOpts{AuthProviders: []AuthProvider{provider}, Prefix: "/"},
		logger: testLogger(),
	}

	req := httptest.NewRequest(http.MethodGet, "/api/auth/callback/github?code=abc&state=something", nil)
	req.SetPathValue("provider", "github")
	rec := httptest.NewRecorder()

	hObj.handleAuthCallback(rec, req)

	assert.Equal(t, http.StatusFound, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "error=invalid_state")
}

func TestHandleAuthCallback_ProviderError(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{
		id:          "github",
		displayName: "GitHub",
		completeFn: func(_ http.ResponseWriter, _ *http.Request) (*User, error) {
			return nil, errors.New("access denied by user")
		},
	}

	hObj := &handler{
		opts:   HandlerOpts{AuthProviders: []AuthProvider{provider}, Prefix: "/"},
		logger: testLogger(),
	}

	state := "valid-state"
	req := httptest.NewRequest(http.MethodGet, "/api/auth/callback/github?code=abc&state="+state, nil)
	req.SetPathValue("provider", "github")
	req.AddCookie(&http.Cookie{Name: oauthStateCookieName, Value: state})
	rec := httptest.NewRecorder()

	hObj.handleAuthCallback(rec, req)

	assert.Equal(t, http.StatusFound, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "error=provider_error")
}

func TestHandleAuthCallback_UnknownProvider(t *testing.T) {
	t.Parallel()

	h := newTestHandlerWithOAuth(t, &mockAuthProvider{id: "github", displayName: "GitHub"})

	req := httptest.NewRequest(http.MethodGet, "/api/auth/callback/unknown?code=abc&state=x", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleAuthLogout_Success(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := t.Context()
	sess := &Session{
		Token:     "logout-token",
		User:      User{ID: "1", Name: "Alice"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}
	require.NoError(t, store.Save(ctx, sess))

	hObj := &handler{
		opts:         HandlerOpts{Prefix: "/"},
		sessionStore: store,
		logger:       testLogger(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "logout-token"})
	rec := httptest.NewRecorder()

	hObj.handleAuthLogout(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Session should be deleted.
	_, err := store.Get(ctx, "logout-token")
	assert.ErrorIs(t, err, ErrSessionNotFound)

	// Cookie should be cleared.
	var cleared *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			cleared = c
			break
		}
	}
	require.NotNil(t, cleared)
	assert.Equal(t, -1, cleared.MaxAge)
}

func TestHandleAuthLogout_NoCookie(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	hObj := &handler{
		opts:         HandlerOpts{Prefix: "/"},
		sessionStore: store,
		logger:       testLogger(),
	}

	req := httptest.NewRequest(http.MethodPost, "/api/auth/logout", nil)
	rec := httptest.NewRecorder()

	hObj.handleAuthLogout(rec, req)

	// Still returns 200 – logout is a no-op if no session.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func testLogger() *slog.Logger {
	return slog.Default()
}
