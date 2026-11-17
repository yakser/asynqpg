package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/mocks"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotificationConfig_DefaultsValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		cooldown time.Duration
		cfg      *NotificationConfig
		poll     time.Duration
		valid    bool
	}{
		{name: "nil", valid: true},
		{name: "empty", cfg: &NotificationConfig{}, valid: true},
		{name: "defaults", cfg: &NotificationConfig{}, valid: true},
		{name: "negative", cooldown: -1},
		{name: "too short", cooldown: 500 * time.Microsecond},
		{name: "too long", cooldown: time.Second},
		{name: "default capped", poll: time.Millisecond, valid: true},
		{name: "explicit small", cooldown: time.Millisecond, poll: time.Millisecond, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := New(Config{Pool: mocks.NewNotificationPool(t), DisableMaintenance: true, Notifications: tc.cfg, FetchCooldown: tc.cooldown, FetchInterval: tc.poll})
			if !tc.valid {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "secret-marker")
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Stop() })
			expected := tc.cooldown
			if expected == 0 {
				expected = min(10*time.Millisecond, c.fetchInterval)
			}
			assert.Equal(t, expected, c.fetchCooldown)
		})
	}
}

func TestNotificationsUnsupportedPool(t *testing.T) {
	t.Parallel()
	_, err := New(Config{Pool: newMockPool(t), Notifications: &NotificationConfig{}})
	require.ErrorIs(t, err, asynqpg.ErrNotificationsUnsupported)
}

func TestNotificationShutdownBoundsBlockedStartup(t *testing.T) {
	t.Parallel()
	pool := mocks.NewNotificationPool(t)
	entered, release := make(chan struct{}), make(chan struct{})
	pool.EXPECT().GetContext(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(context.Context, any, string, ...any) error {
		close(entered)
		<-release
		return errors.New("blocked query")
	}).Once()
	c, err := New(Config{Pool: pool, Notifications: &NotificationConfig{}, DisableMaintenance: true})
	require.NoError(t, err)
	require.NoError(t, c.RegisterTaskHandler("test", TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { return nil })))
	started := make(chan error, 1)
	defer func() { close(release); <-started; _ = c.Stop() }()
	go func() { started <- c.Start() }()
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- c.Shutdown(20 * time.Millisecond) }()
	select {
	case err := <-stopped:
		require.ErrorContains(t, err, "timeout")
	case <-time.After(300 * time.Millisecond):
		t.Fatal("shutdown deadline excludes startup lock")
	}
}
