package producer

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/internal/repository"
)

// --- Mock Repo ---

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

// --- Mock Tx (satisfies asynqpg.Tx) ---

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

// --- Enqueue Tests ---

func TestEnqueue_Success(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	result, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "email.send",
		Payload: []byte(`{"to":"user@example.com"}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID == 0 {
		t.Fatal("expected non-zero task ID")
	}
	if result.Duplicate {
		t.Fatal("expected Duplicate to be false for new task")
	}

	if len(repo.pushTaskCalls) != 1 {
		t.Fatalf("expected 1 PushTask call, got %d", len(repo.pushTaskCalls))
	}
	if repo.pushTaskCalls[0].Type != "email.send" {
		t.Fatalf("expected type %q, got %q", "email.send", repo.pushTaskCalls[0].Type)
	}
	if repo.pushTaskCalls[0].AttemptsLeft != 3 {
		t.Fatalf("expected default MaxRetry 3, got %d", repo.pushTaskCalls[0].AttemptsLeft)
	}
}

func TestEnqueue_NilTask(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.Enqueue(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for nil task")
	}
}

func TestEnqueue_EmptyType(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "",
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for empty type")
	}
}

func TestEnqueue_NilPayload(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: nil,
	})
	if err == nil {
		t.Fatal("expected error for nil payload")
	}
}

func TestEnqueue_WithDelay(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
		Delay:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if repo.pushTaskCalls[0].Delay.Duration() != 5*time.Second {
		t.Fatalf("expected 5s delay, got %v", repo.pushTaskCalls[0].Delay.Duration())
	}
}

func TestEnqueue_WithProcessAt(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	processAt := time.Now().Add(10 * time.Second)
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:      "test",
		Payload:   []byte(`{}`),
		ProcessAt: processAt,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	delay := repo.pushTaskCalls[0].Delay.Duration()
	// Should be approximately 10 seconds
	if delay < 9*time.Second || delay > 11*time.Second {
		t.Fatalf("expected delay ~10s, got %v", delay)
	}
}

func TestEnqueue_ProcessAtInPast(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:      "test",
		Payload:   []byte(`{}`),
		ProcessAt: time.Now().Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	delay := repo.pushTaskCalls[0].Delay.Duration()
	if delay != 0 {
		t.Fatalf("expected delay clamped to 0, got %v", delay)
	}
}

func TestEnqueue_CustomMaxRetry(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:     "test",
		Payload:  []byte(`{}`),
		MaxRetry: ptr.Get(10),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if repo.pushTaskCalls[0].AttemptsLeft != 10 {
		t.Fatalf("expected MaxRetry 10, got %d", repo.pushTaskCalls[0].AttemptsLeft)
	}
}

func TestEnqueue_DefaultMaxRetry(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if repo.pushTaskCalls[0].AttemptsLeft != 3 {
		t.Fatalf("expected default MaxRetry 3, got %d", repo.pushTaskCalls[0].AttemptsLeft)
	}
}

func TestEnqueue_RepoError(t *testing.T) {
	repo := &mockProducerRepo{pushTaskErr: errors.New("db error")}
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestEnqueue_IdempotencyToken(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	token := "unique-token"
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             "test",
		Payload:          []byte(`{}`),
		IdempotencyToken: &token,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if repo.pushTaskCalls[0].IdempotencyToken == nil {
		t.Fatal("expected idempotency token to be set")
	}
	if *repo.pushTaskCalls[0].IdempotencyToken != "unique-token" {
		t.Fatalf("expected token %q, got %q", "unique-token", *repo.pushTaskCalls[0].IdempotencyToken)
	}
}

func TestEnqueue_Duplicate(t *testing.T) {
	repo := &mockProducerRepo{
		pushTaskResult: repository.PushTaskResult{ID: 42, Duplicate: true},
	}
	p := newTestProducer(repo)

	result, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ID != 42 {
		t.Fatalf("expected ID 42, got %d", result.ID)
	}
	if !result.Duplicate {
		t.Fatal("expected Duplicate to be true")
	}
}

// --- EnqueueTx Tests ---

func TestEnqueueTx_Success(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.EnqueueTx(context.Background(), &mockTx{}, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.pushTxCalls) != 1 {
		t.Fatalf("expected 1 PushTaskWithTx call, got %d", len(repo.pushTxCalls))
	}
}

func TestEnqueueTx_NilTx(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), nil, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for nil tx")
	}
}

func TestEnqueueTx_NilTask(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), &mockTx{}, nil)
	if err == nil {
		t.Fatal("expected error for nil task")
	}
}

func TestEnqueueTx_EmptyType(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), &mockTx{}, &asynqpg.Task{
		Type:    "",
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for empty type")
	}
}

func TestEnqueueTx_RepoError(t *testing.T) {
	repo := &mockProducerRepo{pushTxErr: errors.New("tx error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueTx(context.Background(), &mockTx{}, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- EnqueueMany Tests ---

func TestEnqueueMany_Success(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(result.Results))
	}

	if len(repo.pushManyCalls) != 1 {
		t.Fatalf("expected 1 PushTasks call, got %d", len(repo.pushManyCalls))
	}
	if len(repo.pushManyCalls[0].Tasks) != 3 {
		t.Fatalf("expected 3 tasks in batch, got %d", len(repo.pushManyCalls[0].Tasks))
	}
}

func TestEnqueueMany_Empty(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	result, err := p.EnqueueMany(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Results) != 0 {
		t.Fatalf("expected empty results, got %d", len(result.Results))
	}
}

func TestEnqueueMany_NilTask(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "ok", Payload: []byte(`{}`)},
		nil,
	})
	if err == nil {
		t.Fatal("expected error for nil task in batch")
	}
}

func TestEnqueueMany_EmptyType(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "", Payload: []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error for empty type in batch")
	}
}

func TestEnqueueMany_NilPayload(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "test", Payload: nil},
	})
	if err == nil {
		t.Fatal("expected error for nil payload in batch")
	}
}

func TestEnqueueMany_RepoError(t *testing.T) {
	repo := &mockProducerRepo{pushManyErr: errors.New("batch error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- EnqueueManyTx Tests ---

func TestEnqueueManyTx_Success(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result.Results))
	}

	if len(repo.pushManyTxCalls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(repo.pushManyTxCalls))
	}
}

func TestEnqueueManyTx_NilTx(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueManyTx(context.Background(), nil, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error for nil tx")
	}
}

func TestEnqueueManyTx_Empty(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	result, err := p.EnqueueManyTx(context.Background(), &mockTx{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Results) != 0 {
		t.Fatalf("expected empty results, got %d", len(result.Results))
	}
}

func TestEnqueueManyTx_Validation(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueManyTx(context.Background(), &mockTx{}, []*asynqpg.Task{
		nil,
	})
	if err == nil {
		t.Fatal("expected error for nil task")
	}
}

func TestEnqueueManyTx_RepoError(t *testing.T) {
	repo := &mockProducerRepo{pushManyTxErr: errors.New("tx batch error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueManyTx(context.Background(), &mockTx{}, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- New Tests ---

func TestNew_NilPool(t *testing.T) {
	_, err := New(Config{Pool: nil})
	if err == nil {
		t.Fatal("expected error for nil pool")
	}
}

func TestSetDefaults(t *testing.T) {
	p := &Producer{}
	p.setDefaults()

	if p.defaultMaxRetry != 3 {
		t.Fatalf("expected default MaxRetry 3, got %d", p.defaultMaxRetry)
	}
	if p.logger == nil {
		t.Fatal("expected logger to be set")
	}
}

func TestCalculateDelay_NoDelay(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{Type: "test", Payload: []byte(`{}`)}

	delay := p.calculateDelay(task)
	if delay != 0 {
		t.Fatalf("expected 0 delay, got %v", delay)
	}
}

func TestCalculateDelay_WithDelay(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{Type: "test", Payload: []byte(`{}`), Delay: 5 * time.Second}

	delay := p.calculateDelay(task)
	if delay != 5*time.Second {
		t.Fatalf("expected 5s, got %v", delay)
	}
}

func TestCalculateDelay_ProcessAtOverridesDelay(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{
		Type:      "test",
		Payload:   []byte(`{}`),
		Delay:     time.Hour,
		ProcessAt: time.Now().Add(10 * time.Second),
	}

	delay := p.calculateDelay(task)
	if delay < 9*time.Second || delay > 11*time.Second {
		t.Fatalf("expected ~10s, got %v", delay)
	}
}

func TestCalculateMaxRetry_Custom(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{MaxRetry: ptr.Get(7)}

	result := p.calculateMaxRetry(task)
	if result != 7 {
		t.Fatalf("expected 7, got %d", result)
	}
}

func TestCalculateMaxRetry_Default(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{}

	result := p.calculateMaxRetry(task)
	if result != 3 {
		t.Fatalf("expected default 3, got %d", result)
	}
}

func TestCalculateMaxRetry_ExplicitZero(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	task := &asynqpg.Task{MaxRetry: ptr.Get(0)}

	result := p.calculateMaxRetry(task)
	if result != 0 {
		t.Fatalf("expected 0 (no retries), got %d", result)
	}
}
