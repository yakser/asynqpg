package ui

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/ui/mocks"
)

func newStubProvider(t *testing.T, id string) *mocks.AuthProvider {
	t.Helper()
	p := mocks.NewAuthProvider(t)
	p.EXPECT().ID().Return(id).Maybe()
	p.EXPECT().DisplayName().Return(id).Maybe()
	p.EXPECT().IconURL().Return("").Maybe()
	p.EXPECT().BeginAuth(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Maybe()
	p.EXPECT().CompleteAuth(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	return p
}

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
			Pool:      mocks.NewPool(t),
			BasicAuth: &BasicAuth{Password: "pass"},
		}
		err := opts.validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "basic auth username is required")
	})

	t.Run("basic auth missing password", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool:      mocks.NewPool(t),
			BasicAuth: &BasicAuth{Username: "user"},
		}
		err := opts.validate()

		require.Error(t, err)
		assert.Contains(t, err.Error(), "basic auth password is required")
	})

	t.Run("valid opts with basic auth", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool:      mocks.NewPool(t),
			BasicAuth: &BasicAuth{Username: "admin", Password: "secret"},
		}
		err := opts.validate()

		require.NoError(t, err)
	})

	t.Run("valid opts without basic auth", func(t *testing.T) {
		t.Parallel()

		opts := HandlerOpts{
			Pool: mocks.NewPool(t),
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

func TestHandlerOpts_Validate_BasicAuthAndOAuthConflict(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		Pool:          mocks.NewPool(t),
		BasicAuth:     &BasicAuth{Username: "admin", Password: "pass"},
		AuthProviders: []AuthProvider{newStubProvider(t, "github")},
	}
	err := opts.validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")
}

func TestHandlerOpts_Validate_AuthProviders_Valid(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		Pool:          mocks.NewPool(t),
		AuthProviders: []AuthProvider{newStubProvider(t, "github"), newStubProvider(t, "google")},
	}
	err := opts.validate()

	require.NoError(t, err)
}

func TestHandlerOpts_Validate_DuplicateProviderID(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		Pool:          mocks.NewPool(t),
		AuthProviders: []AuthProvider{newStubProvider(t, "github"), newStubProvider(t, "github")},
	}
	err := opts.validate()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate auth provider ID: github")
}

func TestHandlerOpts_SetDefaults_SessionStore(t *testing.T) {
	t.Parallel()

	opts := HandlerOpts{
		AuthProviders: []AuthProvider{newStubProvider(t, "github")},
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
		AuthProviders: []AuthProvider{newStubProvider(t, "github")},
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
		opts := HandlerOpts{AuthProviders: []AuthProvider{newStubProvider(t, "gh")}}
		assert.Equal(t, "oauth", opts.authMode())
	})
}
