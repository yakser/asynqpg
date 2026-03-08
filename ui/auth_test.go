package ui

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemorySessionStore_SaveAndGet(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	sess := &Session{
		Token:     "test-token",
		User:      User{ID: "1", Name: "Alice", Provider: "github"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	require.NoError(t, store.Save(ctx, sess))

	got, err := store.Get(ctx, "test-token")
	require.NoError(t, err)
	assert.Equal(t, "test-token", got.Token)
	assert.Equal(t, "Alice", got.User.Name)
	assert.Equal(t, "github", got.User.Provider)
}

func TestMemorySessionStore_GetNotFound(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	_, err := store.Get(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, ErrSessionNotFound)
}

func TestMemorySessionStore_GetExpired(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	sess := &Session{
		Token:     "expired-token",
		User:      User{ID: "1", Name: "Alice"},
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}

	require.NoError(t, store.Save(ctx, sess))

	_, err := store.Get(ctx, "expired-token")
	assert.ErrorIs(t, err, ErrSessionNotFound)
}

func TestMemorySessionStore_Delete(t *testing.T) {
	t.Parallel()

	store := NewMemorySessionStore()
	defer store.Close()

	ctx := context.Background()
	sess := &Session{
		Token:     "to-delete",
		User:      User{ID: "1"},
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	}

	require.NoError(t, store.Save(ctx, sess))

	_, err := store.Get(ctx, "to-delete")
	require.NoError(t, err)

	require.NoError(t, store.Delete(ctx, "to-delete"))

	_, err = store.Get(ctx, "to-delete")
	assert.ErrorIs(t, err, ErrSessionNotFound)
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

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			token := "token-" + time.Now().Format("150405.000000") + "-" + string(rune('A'+idx%26))
			sess := &Session{
				Token:     token,
				User:      User{ID: "u"},
				CreatedAt: time.Now(),
				ExpiresAt: time.Now().Add(time.Hour),
			}
			_ = store.Save(ctx, sess)
			_, _ = store.Get(ctx, token)
			_ = store.Delete(ctx, token)
		}(i)
	}

	wg.Wait()
}

func TestMemorySessionStore_CleanupExpired(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	store := &MemorySessionStore{
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go store.cleanupLoop(ctx)

	bgCtx := context.Background()
	sess := &Session{
		Token:     "expired-for-cleanup",
		User:      User{ID: "1"},
		CreatedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt: time.Now().Add(-time.Hour),
	}
	require.NoError(t, store.Save(bgCtx, sess))

	// Manually trigger cleanup.
	store.removeExpired()

	_, err := store.Get(bgCtx, "expired-for-cleanup")
	assert.ErrorIs(t, err, ErrSessionNotFound)

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
	for i := 0; i < 1000; i++ {
		token, err := generateSessionToken()
		require.NoError(t, err)
		_, ok := seen[token]
		assert.False(t, ok, "duplicate token generated")
		seen[token] = struct{}{}
	}
}

func TestUserFromContext_WithUser(t *testing.T) {
	t.Parallel()

	user := &User{ID: "42", Name: "Bob", Provider: "github"}
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

	user := &User{ID: "1", Name: "Alice"}
	ctx := withUser(context.Background(), user)
	got := UserFromContext(ctx)

	// Same pointer.
	assert.Same(t, user, got)
}
