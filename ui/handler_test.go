package ui

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHandler(t *testing.T) {
	t.Parallel()

	t.Run("missing pool returns error", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{})

		require.Error(t, err)
		assert.Nil(t, h)
		assert.Contains(t, err.Error(), "pool is required")
	})

	t.Run("valid opts creates handler", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})

		require.NoError(t, err)
		assert.NotNil(t, h)
	})

	t.Run("with basic auth creates handler", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{
			Pool:      &mockPool{},
			BasicAuth: &BasicAuth{Username: "admin", Password: "pass"},
		})

		require.NoError(t, err)
		assert.NotNil(t, h)
	})

	t.Run("with prefix creates handler", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{
			Pool:   &mockPool{},
			Prefix: "/ui",
		})

		require.NoError(t, err)
		assert.NotNil(t, h)
	})
}

func TestHandleHealth(t *testing.T) {
	t.Parallel()

	t.Run("healthy", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, ok := resp.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "ok", data["status"])
	})

	t.Run("unhealthy", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{Pool: &mockPool{pingErr: errors.New("connection refused")}})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, ok := resp.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "error", data["status"])
	})
}

func TestHandleConfig(t *testing.T) {
	t.Parallel()

	t.Run("returns config", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{
			Pool:                 &mockPool{},
			Prefix:               "/asynqpg",
			HidePayloadByDefault: true,
		})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/asynqpg/api/config", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, ok := resp.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "/asynqpg", data["prefix"])
		assert.Equal(t, true, data["hide_payload_by_default"])
		assert.Equal(t, version, data["version"])
	})

	t.Run("default config", func(t *testing.T) {
		t.Parallel()

		h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
		require.NoError(t, err)

		req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, ok := resp.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "/", data["prefix"])
		assert.Equal(t, false, data["hide_payload_by_default"])
	})
}

func TestHandleHealth_WithBasicAuth(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{
		Pool:      &mockPool{},
		BasicAuth: &BasicAuth{Username: "admin", Password: "pass"},
	})
	require.NoError(t, err)

	t.Run("authenticated request succeeds", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		req.SetBasicAuth("admin", "pass")
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("unauthenticated request fails", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}

func TestParseTaskID(t *testing.T) {
	t.Parallel()

	t.Run("valid id", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks/123", nil)
		r.SetPathValue("id", "123")

		id, err := parseTaskID(r)

		require.NoError(t, err)
		assert.Equal(t, int64(123), id)
	})

	t.Run("non-numeric id", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks/abc", nil)
		r.SetPathValue("id", "abc")

		_, err := parseTaskID(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid task id")
	})

	t.Run("zero id", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks/0", nil)
		r.SetPathValue("id", "0")

		_, err := parseTaskID(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid task id")
	})

	t.Run("empty id", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks/", nil)
		r.SetPathValue("id", "")

		_, err := parseTaskID(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing task id")
	})

	t.Run("large id", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks/9999999999", nil)
		r.SetPathValue("id", "9999999999")

		id, err := parseTaskID(r)

		require.NoError(t, err)
		assert.Equal(t, int64(9999999999), id)
	})

	t.Run("negative id", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks/-1", nil)
		r.SetPathValue("id", "-1")

		_, err := parseTaskID(r)

		require.Error(t, err)
	})
}

func TestHandler_OAuth_PublicRoutesAccessible(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{id: "github", displayName: "GitHub"}
	h, err := NewHandler(HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: []AuthProvider{provider},
	})
	require.NoError(t, err)

	t.Run("health is public", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("auth providers is public", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/auth/providers", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandler_OAuth_ProtectedRoutesRequireSession(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{id: "github", displayName: "GitHub"}
	h, err := NewHandler(HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: []AuthProvider{provider},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHandler_OAuth_SPARoutesAccessible(t *testing.T) {
	t.Parallel()

	provider := &mockAuthProvider{id: "github", displayName: "GitHub"}
	h, err := NewHandler(HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: []AuthProvider{provider},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// SPA handler returns 200 (serves index.html or fallback).
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandleConfig_AuthMode_Basic(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{
		Pool:      &mockPool{},
		BasicAuth: &BasicAuth{Username: "admin", Password: "pass"},
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.SetBasicAuth("admin", "pass")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	data, ok := resp.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "basic", data["auth_mode"])
}

func TestHandleConfig_AuthMode_None(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	data, ok := resp.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "none", data["auth_mode"])
}

func TestHandleConfig_AuthMode_OAuth(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	provider := &mockAuthProvider{id: "github", displayName: "GitHub"}
	h, err := NewHandler(HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: []AuthProvider{provider},
		SessionStore:  store,
	})
	require.NoError(t, err)

	sess := &Session{
		Token:     "direct-config-test",
		User:      User{ID: "1", Name: "Test"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}
	require.NoError(t, store.Save(t.Context(), sess))

	req := httptest.NewRequest(http.MethodGet, "/api/config", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "direct-config-test"})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	data, ok := resp.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "oauth", data["auth_mode"])
}
