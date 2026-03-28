package consumer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/consumer/mocks"
)

// testSQLResult implements sql.Result for testing.
type testSQLResult struct{}

func (r testSQLResult) LastInsertId() (int64, error) { return 0, nil }
func (r testSQLResult) RowsAffected() (int64, error) { return 0, nil }

func newMockPool(t *testing.T) *mocks.Pool {
	t.Helper()

	p := mocks.NewPool(t)
	p.EXPECT().PingContext(mock.Anything).Return(nil).Maybe()
	p.EXPECT().ExecContext(mock.Anything, mock.Anything, mock.Anything).Return(testSQLResult{}, nil).Maybe()
	p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	p.EXPECT().GetContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	p.EXPECT().QueryContext(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	p.EXPECT().QueryRowContext(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	return p
}

func newMockHandler(t *testing.T) *mocks.TaskHandler {
	t.Helper()

	h := mocks.NewTaskHandler(t)
	h.EXPECT().Handle(mock.Anything, mock.Anything).Return(nil).Maybe()

	return h
}

func newTestConsumer(t *testing.T) *Consumer {
	t.Helper()

	p := newMockPool(t)

	c, err := New(Config{
		Pool:               p,
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
		Pool:               newMockPool(t),
		DisableMaintenance: true,
	})

	require.NoError(t, err)
	assert.NotNil(t, c)
}

func TestRegisterTaskHandler_Success(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)

	err := c.RegisterTaskHandler("test:task", newMockHandler(t))

	assert.NoError(t, err)
}

func TestRegisterTaskHandler_Duplicate(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	_ = c.RegisterTaskHandler("test:task", newMockHandler(t))

	err := c.RegisterTaskHandler("test:task", newMockHandler(t))

	assert.ErrorIs(t, err, ErrTaskHandlerAlreadyRegistered)
}

func TestRegisterTaskHandler_AfterStart(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)
	_ = c.RegisterTaskHandler("test:task", newMockHandler(t))
	err := c.Start()
	require.NoError(t, err)
	defer func() { _ = c.Stop() }()

	err = c.RegisterTaskHandler("another:task", newMockHandler(t))

	assert.EqualError(t, err, "cannot register handler after consumer is started")
}

func TestRegisterTaskHandler_WithOptions(t *testing.T) {
	t.Parallel()

	c := newTestConsumer(t)

	err := c.RegisterTaskHandler("test:task", newMockHandler(t),
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
	_ = c.RegisterTaskHandler("test:task", newMockHandler(t))
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
	_ = c.RegisterTaskHandler("test:task", newMockHandler(t))
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
