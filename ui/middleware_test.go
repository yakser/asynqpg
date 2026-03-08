package ui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicAuthMiddleware(t *testing.T) {
	t.Parallel()

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	middleware := basicAuthMiddleware("admin", "secret")
	protected := middleware(okHandler)

	t.Run("valid credentials", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		req.SetBasicAuth("admin", "secret")
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "ok", rec.Body.String())
	})

	t.Run("invalid password", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		req.SetBasicAuth("admin", "wrong")
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Contains(t, rec.Header().Get("WWW-Authenticate"), `Basic realm="asynqpg"`)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "unauthorized", resp.Error.Code)
	})

	t.Run("invalid username", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		req.SetBasicAuth("wrong", "secret")
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("missing credentials", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Contains(t, rec.Header().Get("WWW-Authenticate"), `Basic realm="asynqpg"`)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "unauthorized", resp.Error.Code)
		assert.Contains(t, resp.Error.Message, "authentication required")
	})
}

func TestCORSMiddleware(t *testing.T) {
	t.Parallel()

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := corsMiddleware([]string{"http://localhost:3000", "https://example.com"})
	protected := middleware(okHandler)

	t.Run("allowed origin", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "http://localhost:3000", rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("disallowed origin", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		req.Header.Set("Origin", "http://evil.com")
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("preflight request", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodOptions, "/api/tasks", nil)
		req.Header.Set("Origin", "https://example.com")
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNoContent, rec.Code)
		assert.Equal(t, "https://example.com", rec.Header().Get("Access-Control-Allow-Origin"))
		assert.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "GET")
		assert.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "POST")
		assert.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "DELETE")
	})

	t.Run("no origin header", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		rec := httptest.NewRecorder()

		protected.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
	})
}

func TestSessionAuthMiddleware_ValidSession(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	sess := &Session{
		Token:     "valid-token",
		User:      User{ID: "42", Name: "Alice", Provider: "github"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}
	require.NoError(t, store.Save(context.Background(), sess))

	var capturedUser *User
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedUser = UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	middleware := sessionAuthMiddleware(store)
	protected := middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "valid-token"})
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, capturedUser)
	assert.Equal(t, "42", capturedUser.ID)
	assert.Equal(t, "Alice", capturedUser.Name)
}

func TestSessionAuthMiddleware_NoCookie(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := sessionAuthMiddleware(store)
	protected := middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "unauthorized", resp.Error.Code)
}

func TestSessionAuthMiddleware_InvalidToken(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := sessionAuthMiddleware(store)
	protected := middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "nonexistent-token"})
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestSessionAuthMiddleware_ExpiredSession(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	// Save a session that's already expired but hasn't been cleaned up yet.
	sess := &Session{
		Token:     "expired-token",
		User:      User{ID: "1"},
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}
	// Bypass normal Get (which would reject expired), store directly.
	store.sessions.Store("expired-token", sess)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := sessionAuthMiddleware(store)
	protected := middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "expired-token"})
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestSessionAuthMiddleware_UserInContext(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	sess := &Session{
		Token:     "ctx-test-token",
		User:      User{ID: "99", Name: "Bob", Provider: "google", Email: "bob@example.com"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}
	require.NoError(t, store.Save(context.Background(), sess))

	var gotUser *User
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser = UserFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	middleware := sessionAuthMiddleware(store)
	protected := middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "ctx-test-token"})
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, gotUser)
	assert.Equal(t, "99", gotUser.ID)
	assert.Equal(t, "Bob", gotUser.Name)
	assert.Equal(t, "google", gotUser.Provider)
	assert.Equal(t, "bob@example.com", gotUser.Email)
}

func TestSessionAuthMiddleware_EmptyCookieValue(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := sessionAuthMiddleware(store)
	protected := middleware(inner)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: ""})
	rec := httptest.NewRecorder()

	protected.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}
