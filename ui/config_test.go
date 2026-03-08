package ui

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlerOpts_Validate(t *testing.T) {
	t.Parallel()

	t.Run("missing pool", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{}
		err := opts.validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "pool is required")
	})

	t.Run("basic auth missing username", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool:      &mockPool{},
			BasicAuth: &BasicAuth{Password: "pass"},
		}
		err := opts.validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "basic auth username is required")
	})

	t.Run("basic auth missing password", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool:      &mockPool{},
			BasicAuth: &BasicAuth{Username: "user"},
		}
		err := opts.validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "basic auth password is required")
	})

	t.Run("valid opts with basic auth", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool:      &mockPool{},
			BasicAuth: &BasicAuth{Username: "admin", Password: "secret"},
		}
		err := opts.validate()

		require.NoError(t, err)
	})

	t.Run("valid opts without basic auth", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool: &mockPool{},
		}
		err := opts.validate()

		require.NoError(t, err)
	})
}

func TestHandlerOpts_SetDefaults(t *testing.T) {
	t.Parallel()

	t.Run("sets default prefix", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{}
		opts.setDefaults()

		assert.Equal(t, "/", opts.Prefix)
	})

	t.Run("sets default logger", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{}
		opts.setDefaults()

		assert.NotNil(t, opts.Logger)
	})

	t.Run("preserves custom prefix", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{Prefix: "/asynqpg"}
		opts.setDefaults()

		assert.Equal(t, "/asynqpg", opts.Prefix)
	})
}

// stubProvider is a minimal AuthProvider for config validation tests.
type stubProvider struct{ id string }

func (s *stubProvider) ID() string                                                   { return s.id }
func (s *stubProvider) DisplayName() string                                          { return s.id }
func (s *stubProvider) IconURL() string                                              { return "" }
func (s *stubProvider) BeginAuth(http.ResponseWriter, *http.Request, string, string) {}
func (s *stubProvider) CompleteAuth(http.ResponseWriter, *http.Request) (*User, error) {
	return nil, errors.New("not implemented")
}

func TestHandlerOpts_Validate_BasicAuthAndOAuthConflict(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		Pool:          &mockPool{},
		BasicAuth:     &BasicAuth{Username: "admin", Password: "pass"},
		AuthProviders: []AuthProvider{&stubProvider{id: "github"}},
	}
	err := opts.validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestHandlerOpts_Validate_AuthProviders_Valid(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: []AuthProvider{&stubProvider{id: "github"}, &stubProvider{id: "google"}},
	}
	err := opts.validate()

	require.NoError(t, err)
}

func TestHandlerOpts_Validate_DuplicateProviderID(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		Pool:          &mockPool{},
		AuthProviders: []AuthProvider{&stubProvider{id: "github"}, &stubProvider{id: "github"}},
	}
	err := opts.validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate auth provider ID: github")
}

func TestHandlerOpts_SetDefaults_SessionStore(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		AuthProviders: []AuthProvider{&stubProvider{id: "github"}},
	}
	opts.setDefaults()

	require.NotNil(t, opts.SessionStore)
	// Verify it's a MemorySessionStore by checking its type.
	ms, ok := opts.SessionStore.(*MemorySessionStore)
	assert.True(t, ok)
	if ms != nil {
		ms.Close()
	}
}

func TestHandlerOpts_SetDefaults_SessionMaxAge(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		AuthProviders: []AuthProvider{&stubProvider{id: "github"}},
	}
	opts.setDefaults()

	assert.Equal(t, 24*time.Hour, opts.SessionMaxAge)

	// Close the auto-created store.
	if ms, ok := opts.SessionStore.(*MemorySessionStore); ok {
		ms.Close()
	}
}

func TestHandlerOpts_AuthMode(t *testing.T) {
	t.Parallel()

	t.Run("none", func(t *testing.T) {
		t.Parallel()
		opts := HandlerOpts{}
		assert.Equal(t, "none", opts.authMode())
	})

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		opts := HandlerOpts{BasicAuth: &BasicAuth{Username: "a", Password: "b"}}
		assert.Equal(t, "basic", opts.authMode())
	})

	t.Run("oauth", func(t *testing.T) {
		t.Parallel()
		opts := HandlerOpts{AuthProviders: []AuthProvider{&stubProvider{id: "gh"}}}
		assert.Equal(t, "oauth", opts.authMode())
	})
}
