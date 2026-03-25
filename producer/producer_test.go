package producer

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/internal/repository"
)

type mockProducerRepo struct {
	pushTaskCalls    []*repository.PushTaskParams
	pushTaskResult   repository.PushTaskResult
	pushTaskErr      error
	pushTxCalls      []*repository.PushTaskParams
	pushTxResult     repository.PushTaskResult
	pushTxErr        error
	pushManyCalls    []repository.PushTasksParams
	pushManyResult   []repository.PushTaskResult
	pushManyErr      error
	pushManyTxCalls  []repository.PushTasksParams
	pushManyTxResult []repository.PushTaskResult
	pushManyTxErr    error
}

func (m *mockProducerRepo) PushTask(_ context.Context, task *repository.PushTaskParams) (repository.PushTaskResult, error) {
	m.pushTaskCalls = append(m.pushTaskCalls, task)
	if m.pushTaskResult.ID == 0 && m.pushTaskErr == nil {
		return repository.PushTaskResult{ID: int64(len(m.pushTaskCalls))}, nil
	}
	return m.pushTaskResult, m.pushTaskErr
}

func (m *mockProducerRepo) PushTaskWithTx(_ context.Context, _ asynqpg.Tx, task *repository.PushTaskParams) (repository.PushTaskResult, error) {
	m.pushTxCalls = append(m.pushTxCalls, task)
	if m.pushTxResult.ID == 0 && m.pushTxErr == nil {
		return repository.PushTaskResult{ID: int64(len(m.pushTxCalls))}, nil
	}
	return m.pushTxResult, m.pushTxErr
}

func (m *mockProducerRepo) PushTasks(_ context.Context, params repository.PushTasksParams) ([]repository.PushTaskResult, error) {
	m.pushManyCalls = append(m.pushManyCalls, params)
	return m.pushManyResult, m.pushManyErr
}

func (m *mockProducerRepo) PushTasksWithTx(_ context.Context, _ asynqpg.Tx, params repository.PushTasksParams) ([]repository.PushTaskResult, error) {
	m.pushManyTxCalls = append(m.pushManyTxCalls, params)
	return m.pushManyTxResult, m.pushManyTxErr
}

type mockTx struct{}

func (m *mockTx) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil //nolint:nilnil
}
func (m *mockTx) SelectContext(_ context.Context, _ any, _ string, _ ...any) error {
	return nil
}
func (m *mockTx) GetContext(_ context.Context, _ any, _ string, _ ...any) error {
	return nil
}
func (m *mockTx) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil //nolint:nilnil
}
func (m *mockTx) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}
func (m *mockTx) Commit() error   { return nil }
func (m *mockTx) Rollback() error { return nil }

// newTestProducer creates a Producer with a mock repo.
func newTestProducer(repo *mockProducerRepo) *Producer {
	m, _ := asynqpg.NewMetrics(nil)
	p := &Producer{
		repo:            repo,
		defaultMaxRetry: 3,
		metrics:         m,
		tracer:          asynqpg.NewTracer(nil),
	}
	p.setDefaults()
	return p
}

func TestEnqueue_Success(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	result, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "email.send",
		Payload: []byte(`{"to":"user@example.com"}`),
	})
	require.NoError(t, err)
	require.NotZero(t, result.ID)
	require.False(t, result.Duplicate)

	require.Len(t, repo.pushTaskCalls, 1)
	require.Equal(t, "email.send", repo.pushTaskCalls[0].Type)
	require.Equal(t, 3, repo.pushTaskCalls[0].AttemptsLeft)
}

func TestEnqueue_NilTask(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	_, err := p.Enqueue(context.Background(), nil)
	require.Error(t, err)
}

func TestEnqueue_EmptyType(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueue_NilPayload(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: nil,
	})
	require.Error(t, err)
}

func TestEnqueue_WithDelay(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
		Delay:   5 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, repo.pushTaskCalls[0].Delay.Duration())
}

func TestEnqueue_WithProcessAt(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	processAt := time.Now().Add(10 * time.Second)
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:      "test",
		Payload:   []byte(`{}`),
		ProcessAt: processAt,
	})
	require.NoError(t, err)

	delay := repo.pushTaskCalls[0].Delay.Duration()
	// Should be approximately 10 seconds
	require.InDelta(t, 10*time.Second, delay, float64(time.Second))
}

func TestEnqueue_ProcessAtInPast(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:      "test",
		Payload:   []byte(`{}`),
		ProcessAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), repo.pushTaskCalls[0].Delay.Duration())
}

func TestEnqueue_CustomMaxRetry(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:     "test",
		Payload:  []byte(`{}`),
		MaxRetry: ptr.Get(10),
	})
	require.NoError(t, err)
	require.Equal(t, 10, repo.pushTaskCalls[0].AttemptsLeft)
}

func TestEnqueue_DefaultMaxRetry(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.Equal(t, 3, repo.pushTaskCalls[0].AttemptsLeft)
}

func TestEnqueue_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{pushTaskErr: errors.New("db error")}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueue_IdempotencyToken(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	token := "unique-token"
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             "test",
		Payload:          []byte(`{}`),
		IdempotencyToken: &token,
	})
	require.NoError(t, err)
	require.NotNil(t, repo.pushTaskCalls[0].IdempotencyToken)
	require.Equal(t, "unique-token", *repo.pushTaskCalls[0].IdempotencyToken)
}

func TestEnqueue_Duplicate(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{
		pushTaskResult: repository.PushTaskResult{ID: 42, Duplicate: true},
	}
	p := newTestProducer(repo)

	result, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.Equal(t, int64(42), result.ID)
	require.True(t, result.Duplicate)
}

func TestEnqueueTx_Success(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.EnqueueTx(context.Background(), &mockTx{}, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.Len(t, repo.pushTxCalls, 1)
}

func TestEnqueueTx_NilTx(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), nil, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueueTx_NilTask(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), &mockTx{}, nil)
	require.Error(t, err)
}

func TestEnqueueTx_EmptyType(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), &mockTx{}, &asynqpg.Task{
		Type:    "",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueueTx_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{pushTxErr: errors.New("tx error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueTx(context.Background(), &mockTx{}, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueueMany_Success(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{
		pushManyResult: []repository.PushTaskResult{
			{ID: 1}, {ID: 2}, {ID: 3},
		},
	}
	p := newTestProducer(repo)

	tasks := []*asynqpg.Task{
		{Type: "email", Payload: []byte(`{"id":1}`)},
		{Type: "email", Payload: []byte(`{"id":2}`)},
		{Type: "sms", Payload: []byte(`{"id":3}`)},
	}

	result, err := p.EnqueueMany(context.Background(), tasks)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)
	require.Len(t, repo.pushManyCalls, 1)
	require.Len(t, repo.pushManyCalls[0].Tasks, 3)
}

func TestEnqueueMany_Empty(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	result, err := p.EnqueueMany(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, result.Results)
}

func TestEnqueueMany_NilTask(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "ok", Payload: []byte(`{}`)},
		nil,
	})
	require.Error(t, err)
}

func TestEnqueueMany_EmptyType(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "", Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestEnqueueMany_NilPayload(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "test", Payload: nil},
	})
	require.Error(t, err)
}

func TestEnqueueMany_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{pushManyErr: errors.New("batch error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestEnqueueManyTx_Success(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{
		pushManyTxResult: []repository.PushTaskResult{
			{ID: 1}, {ID: 2},
		},
	}
	p := newTestProducer(repo)

	result, err := p.EnqueueManyTx(context.Background(), &mockTx{}, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{"id":1}`)},
		{Type: "test", Payload: []byte(`{"id":2}`)},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 2)
	require.Len(t, repo.pushManyTxCalls, 1)
}

func TestEnqueueManyTx_NilTx(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueManyTx(context.Background(), nil, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestEnqueueManyTx_Empty(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	result, err := p.EnqueueManyTx(context.Background(), &mockTx{}, nil)
	require.NoError(t, err)
	require.Empty(t, result.Results)
}

func TestEnqueueManyTx_Validation(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueManyTx(context.Background(), &mockTx{}, []*asynqpg.Task{
		nil,
	})
	require.Error(t, err)
}

func TestEnqueueManyTx_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockProducerRepo{pushManyTxErr: errors.New("tx batch error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueManyTx(context.Background(), &mockTx{}, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestNew_NilPool(t *testing.T) {
	t.Parallel()

	_, err := New(Config{Pool: nil})
	require.Error(t, err)
}

func TestSetDefaults(t *testing.T) {
	t.Parallel()

	p := &Producer{}
	p.setDefaults()

	require.Equal(t, 3, p.defaultMaxRetry)
	require.NotNil(t, p.logger)
}

func TestCalculateDelay_NoDelay(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{Type: "test", Payload: []byte(`{}`)}

	delay := p.calculateDelay(task)
	require.Equal(t, time.Duration(0), delay)
}

func TestCalculateDelay_WithDelay(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{Type: "test", Payload: []byte(`{}`), Delay: 5 * time.Second}

	delay := p.calculateDelay(task)
	require.Equal(t, 5*time.Second, delay)
}

func TestCalculateDelay_ProcessAtOverridesDelay(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{
		Type:      "test",
		Payload:   []byte(`{}`),
		Delay:     time.Hour,
		ProcessAt: time.Now().Add(10 * time.Second),
	}

	delay := p.calculateDelay(task)
	require.InDelta(t, 10*time.Second, delay, float64(time.Second))
}

func TestCalculateMaxRetry_Custom(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{MaxRetry: ptr.Get(7)}

	result := p.calculateMaxRetry(task)
	require.Equal(t, 7, result)
}

func TestCalculateMaxRetry_Default(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{}

	result := p.calculateMaxRetry(task)
	require.Equal(t, 3, result)
}

func TestCalculateMaxRetry_ExplicitZero(t *testing.T) {
	t.Parallel()

	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{MaxRetry: ptr.Get(0)}

	result := p.calculateMaxRetry(task)
	require.Equal(t, 0, result)
}
