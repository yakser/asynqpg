package ui

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/ui/uiauth"
)

func TestMemorySessionStore_SaveAndGet(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	sess := &uiauth.Session{
		Token:     "test-token",
		User:      uiauth.User{ID: "1", Name: testUserName, Provider: testProviderGithub},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	require.NoError(t, store.Save(ctx, sess))

	got, err := store.Get(ctx, "test-token")
	require.NoError(t, err)
	assert.Equal(t, "test-token", got.Token)
	assert.Equal(t, testUserName, got.User.Name)
	assert.Equal(t, testProviderGithub, got.User.Provider)
}

func TestMemorySessionStore_GetNotFound(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	_, err := store.Get(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, uiauth.ErrSessionNotFound)
}

func TestMemorySessionStore_GetExpired(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	sess := &uiauth.Session{
		Token:     testExpiredToken,
		User:      uiauth.User{ID: "1", Name: testUserName},
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}

	require.NoError(t, store.Save(ctx, sess))

	_, err := store.Get(ctx, testExpiredToken)
	assert.ErrorIs(t, err, uiauth.ErrSessionNotFound)
}

func TestMemorySessionStore_Delete(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	sess := &uiauth.Session{
		Token:     "to-delete",
		User:      uiauth.User{ID: "1"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	require.NoError(t, store.Save(ctx, sess))

	_, err := store.Get(ctx, "to-delete")
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, "to-delete"))

	_, err = store.Get(ctx, "to-delete")
	assert.ErrorIs(t, err, uiauth.ErrSessionNotFound)
}

func TestMemorySessionStore_DeleteNotFound(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	err := store.Delete(context.Background(), "nonexistent")
	assert.NoError(t, err)
}

func TestMemorySessionStore_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := range 100 {
		wg.Go(func() {
			token := "token-" + time.Now().Format("150405.000000") + "-" + string(rune('A'+i%26))
			sess := &uiauth.Session{
				Token:     token,
				User:      uiauth.User{ID: "u"},
				CreatedAt: time.Now(),
				ExpiresAt: time.Now().Add(time.Hour),
			}
			_ = store.Save(ctx, sess)
			_, _ = store.Get(ctx, token)
			_ = store.Delete(ctx, token)
		})
	}

	wg.Wait()
}

func TestMemorySessionStore_CleanupExpired(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	store := &MemorySessionStore{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go store.cleanupLoop(ctx)

	bgCtx := context.Background()
	sess := &uiauth.Session{
		Token:     "expired-for-cleanup",
		User:      uiauth.User{ID: "1"},
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}
	require.NoError(t, store.Save(bgCtx, sess))

	// Manually trigger cleanup.
	store.removeExpired()

	_, err := store.Get(bgCtx, "expired-for-cleanup")
	assert.ErrorIs(t, err, uiauth.ErrSessionNotFound)

	store.Close()
}

func TestGenerateSessionToken_Length(t *testing.T) {
	t.Parallel()

	token, err := generateSessionToken()
	require.NoError(t, err)
	assert.NotEmpty(t, token)
	// 32 bytes -> 43 chars in base64url (no padding)
	assert.Len(t, token, 43)
}

func TestGenerateSessionToken_Unique(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		token, err := generateSessionToken()
		require.NoError(t, err)
		_, ok := seen[token]
		assert.False(t, ok, "duplicate token generated")
		seen[token] = struct{}{}
	}
}

func TestUserFromContext_WithUser(t *testing.T) {
	t.Parallel()

	user := &uiauth.User{ID: "42", Name: "Bob", Provider: testProviderGithub}
	ctx := withUser(context.Background(), user)

	got := UserFromContext(ctx)
	require.NotNil(t, got)
	assert.Equal(t, "42", got.ID)
	assert.Equal(t, "Bob", got.Name)
}

func TestUserFromContext_WithoutUser(t *testing.T) {
	t.Parallel()

	got := UserFromContext(context.Background())
	assert.Nil(t, got)
}

func TestWithUser_RoundTrip(t *testing.T) {
	t.Parallel()

	user := &uiauth.User{ID: "1", Name: testUserName}
	ctx := withUser(context.Background(), user)
	got := UserFromContext(ctx)

	// Same pointer.
	assert.Same(t, user, got)
}
