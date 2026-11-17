package notify

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"

	"github.com/yakser/asynqpg/mocks"
)

const expectedChannel = "expected"

func TestListener_StartupError(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	l, err := New(Config{Pool: mocks.NewNotificationPool(t), Logger: slog.New(slog.NewTextHandler(&logs, nil)), OnType: func(string) bool { return true }, OnReady: func() {}})
	require.NoError(t, err)
	l.dial = func(context.Context) (connection, error) { return nil, errors.New("secret-marker") }
	// Bypass the DB target resolver to isolate connection failures.
	l.resolve = func(context.Context) (Target, error) {
		return Target{Database: "db", Schema: "public", TableOID: 1, Channel: "asynqpg_ready_v1_1"}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Error(t, l.Start(ctx))
	l.Stop()
	l.Stop()
	assert.NotContains(t, logs.String(), "secret-marker")
}
func TestListener_ReconnectBackoff(t *testing.T) {
	t.Parallel()
	for attempt := range 10 {
		got := reconnectDelay(attempt)
		base := min(100*time.Millisecond*time.Duration(1<<attempt), 5*time.Second)
		assert.GreaterOrEqual(t, got, base)
		assert.LessOrEqual(t, got, base+base/5)
	}
}
func TestListener_CancelBackoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan bool, 1)
		go func() { done <- waitBackoff(ctx, time.Hour) }()
		synctest.Wait()
		cancel()
		assert.False(t, <-done)
	})
}
func TestListener_ConfigValidation(t *testing.T) {
	t.Parallel()
	_, err := New(Config{})
	require.Error(t, err)
}

func TestListener_IdlePingKeepsSession(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		conn := mocks.NewNotificationConnection(t)
		ctx, cancel := context.WithCancel(t.Context())
		conn.EXPECT().WaitForNotification(mock.Anything).RunAndReturn(func(ctx context.Context) (asynqpg.Notification, error) {
			<-ctx.Done()
			return asynqpg.Notification{}, ctx.Err()
		}).Twice()
		conn.EXPECT().Ping(mock.Anything).Return(nil).Once()
		l, err := New(Config{Pool: mocks.NewNotificationPool(t), OnType: func(string) bool { return true }, OnReady: func() {}})
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- l.read(ctx, conn) }()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}
func TestListener_CancelConnect(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		l, err := New(Config{Pool: mocks.NewNotificationPool(t), OnType: func(string) bool { return true }, OnReady: func() { t.Error("unexpected ready") }})
		require.NoError(t, err)
		l.resolve = func(context.Context) (Target, error) { return Target{}, nil }
		entered := make(chan struct{})
		l.dial = func(ctx context.Context) (connection, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }
		done := make(chan error, 1)
		go func() { done <- l.Start(t.Context()) }()
		<-entered
		l.Stop()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}
func TestListener_IgnoredPayload(t *testing.T) {
	t.Parallel()
	conn := mocks.NewNotificationConnection(t)
	calls := 0
	l, err := New(Config{Pool: mocks.NewNotificationPool(t), OnType: func(value string) bool { calls++; return value == "known" }, OnReady: func() {}})
	require.NoError(t, err)
	l.channel = expectedChannel
	conn.EXPECT().WaitForNotification(mock.Anything).Return(asynqpg.Notification{Channel: expectedChannel, Payload: "known"}, nil).Once()
	conn.EXPECT().WaitForNotification(mock.Anything).Return(asynqpg.Notification{Channel: expectedChannel, Payload: "unknown"}, nil).Once()
	conn.EXPECT().WaitForNotification(mock.Anything).Return(asynqpg.Notification{Channel: "wrong", Payload: "secret"}, nil).Once()
	conn.EXPECT().WaitForNotification(mock.Anything).Return(asynqpg.Notification{}, context.Canceled).Once()
	require.Error(t, l.read(context.Background(), conn))
	assert.Equal(t, 2, calls)
}

func TestResolveTarget_UnavailableIsNotMissingMigration(t *testing.T) {
	t.Parallel()
	pool := mocks.NewPool(t)
	pool.EXPECT().GetContext(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("connection failed: secret-marker")).Once()
	_, err := ResolveTarget(t.Context(), pool)
	require.ErrorIs(t, err, ErrUnavailable)
	assert.NotContains(t, err.Error(), "secret-marker")
}

func TestListener_CancelResolve(t *testing.T) {
	t.Parallel()
	l, err := New(Config{Pool: mocks.NewNotificationPool(t), OnType: func(string) bool { return true }, OnReady: func() { t.Error("unexpected ready") }})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	l.resolve = func(context.Context) (Target, error) { cancel(); return Target{}, ErrUnavailable }
	require.ErrorIs(t, l.Start(ctx), context.Canceled)
	l.Stop()
}

func TestListener_CancelTaskSubscription(t *testing.T) {
	t.Parallel()
	pool := mocks.NewNotificationPool(t)
	conn := mocks.NewNotificationConnection(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	l, err := New(Config{Pool: pool, OnType: func(string) bool { return true }, OnReady: func() { t.Error("unexpected ready") }})
	require.NoError(t, err)
	l.resolve = func(context.Context) (Target, error) { return Target{Channel: expectedChannel}, nil }
	pool.EXPECT().OpenNotificationConnection(mock.Anything).Return(conn, nil).Once()
	var probe asynqpg.Notification
	conn.EXPECT().Listen(mock.Anything, mock.MatchedBy(func(s string) bool { return s != expectedChannel })).Return(nil).Once()
	pool.EXPECT().ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, _ string, args ...any) (sql.Result, error) {
		probe = asynqpg.Notification{Channel: args[0].(string), Payload: args[1].(string)}
		return driver.RowsAffected(1), nil
	}).Once()
	conn.EXPECT().WaitForNotification(mock.Anything).RunAndReturn(func(context.Context) (asynqpg.Notification, error) { return probe, nil }).Once()
	conn.EXPECT().Unlisten(mock.Anything, mock.Anything).Return(nil).Once()
	conn.EXPECT().Listen(mock.Anything, expectedChannel).RunAndReturn(func(context.Context, string) error { cancel(); return context.Canceled }).Once()
	conn.EXPECT().Close(mock.Anything).RunAndReturn(func(ctx context.Context) error { require.NoError(t, ctx.Err()); return nil }).Once()
	require.ErrorIs(t, l.Start(ctx), context.Canceled)
	l.Stop()
}
