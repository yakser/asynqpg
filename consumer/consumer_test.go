package consumer

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
)

// mockPool implements asynqpg.Pool for testing.
type mockPool struct{}

func (m *mockPool) PingContext(_ context.Context) error { return nil }

func (m *mockPool) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return mockResult{}, nil
}

func (m *mockPool) SelectContext(_ context.Context, _ any, _ string, _ ...any) error { return nil }
func (m *mockPool) GetContext(_ context.Context, _ any, _ string, _ ...any) error    { return nil }

func (m *mockPool) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil //nolint:nilnil
}

func (m *mockPool) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row { return nil }

// mockResult implements sql.Result for testing.
type mockResult struct{}

func (r mockResult) LastInsertId() (int64, error) { return 0, nil }
func (r mockResult) RowsAffected() (int64, error) { return 0, nil }

// noopHandler implements TaskHandler with a no-op Handle method.
type noopHandler struct{}

func (h *noopHandler) Handle(_ context.Context, _ *asynqpg.TaskInfo) error { return nil }

func newTestConsumer(t *testing.T) *Consumer {
	t.Helper()

	c, err := New(Config{
		Pool:               &mockPool{},
		DisableMaintenance: true,
	})
	require.NoError(t, err)

	return c
}

func TestNew_NilPool(t *testing.T) {
	t.Parallel()

	c, err := New(Config{})

	assert.Nil(t, c)
	assert.EqualError(t, err, "database pool is required")
}

func TestNew_DefaultRetryPolicy(t *testing.T) {
	t.Parallel()

	c, err := New(Config{
		Pool:               &mockPool{},
		DisableMaintenance: true,
	})

	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestRegisterTaskHandler_Success(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)

	err := c.RegisterTaskHandler("test:task", &noopHandler{})

	assert.NoError(t, err)
}

func TestRegisterTaskHandler_Duplicate(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	_ = c.RegisterTaskHandler("test:task", &noopHandler{})

	err := c.RegisterTaskHandler("test:task", &noopHandler{})

	assert.ErrorIs(t, err, ErrTaskHandlerAlreadyRegistered)
}

func TestRegisterTaskHandler_AfterStart(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	_ = c.RegisterTaskHandler("test:task", &noopHandler{})
	err := c.Start()
	require.NoError(t, err)
	defer func() { _ = c.Stop() }()

	err = c.RegisterTaskHandler("another:task", &noopHandler{})

	assert.EqualError(t, err, "cannot register handler after consumer is started")
}

func TestRegisterTaskHandler_WithOptions(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)

	err := c.RegisterTaskHandler("test:task", &noopHandler{},
		WithWorkersCount(5),
		WithMaxAttempts(10),
		WithTimeout(1*time.Minute),
	)

	assert.NoError(t, err)
	assert.Equal(t, 5, c.taskOptions["test:task"].WorkersCount)
	assert.Equal(t, 10, c.taskOptions["test:task"].MaxAttempts)
	assert.Equal(t, 1*time.Minute, c.taskOptions["test:task"].Timeout)
}

func TestStart_NoHandlers(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)

	err := c.Start()

	assert.EqualError(t, err, "no task handlers registered")
}

func TestStart_AlreadyStarted(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	_ = c.RegisterTaskHandler("test:task", &noopHandler{})
	err := c.Start()
	require.NoError(t, err)
	defer func() { _ = c.Stop() }()

	err = c.Start()

	assert.EqualError(t, err, "consumer is already started")
}

func TestUse_BeforeStart(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	mw := func(next TaskHandler) TaskHandler { return next }

	err := c.Use(mw)

	assert.NoError(t, err)
	assert.Len(t, c.globalMiddleware, 1)
}

func TestUse_AfterStart(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	_ = c.RegisterTaskHandler("test:task", &noopHandler{})
	err := c.Start()
	require.NoError(t, err)
	defer func() { _ = c.Stop() }()

	mw := func(next TaskHandler) TaskHandler { return next }
	err = c.Use(mw)

	assert.EqualError(t, err, "cannot add middleware after consumer is started")
}

func TestUse_NilMiddleware(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)

	err := c.Use(nil, nil)

	assert.NoError(t, err)
	assert.Empty(t, c.globalMiddleware)
}
