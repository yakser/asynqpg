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
	pushTaskErr      error
	pushTxCalls      []*repository.PushTaskParams
	pushTxErr        error
	pushManyCalls    []repository.PushTasksParams
	pushManyResult   []int64
	pushManyErr      error
	pushManyTxCalls  []repository.PushTasksParams
	pushManyTxResult []int64
	pushManyTxErr    error
}

func (m *mockProducerRepo) PushTask(_ context.Context, task *repository.PushTaskParams) (int64, error) {
	m.pushTaskCalls = append(m.pushTaskCalls, task)
	return int64(len(m.pushTaskCalls)), m.pushTaskErr
}

func (m *mockProducerRepo) PushTaskWithExecutor(_ context.Context, _ asynqpg.Querier, task *repository.PushTaskParams) (int64, error) {
	m.pushTxCalls = append(m.pushTxCalls, task)
	return int64(len(m.pushTxCalls)), m.pushTxErr
}

func (m *mockProducerRepo) PushTasks(_ context.Context, params repository.PushTasksParams) ([]int64, error) {
	m.pushManyCalls = append(m.pushManyCalls, params)
	return m.pushManyResult, m.pushManyErr
}

func (m *mockProducerRepo) PushTasksWithExecutor(_ context.Context, _ asynqpg.Querier, params repository.PushTasksParams) ([]int64, error) {
	m.pushManyTxCalls = append(m.pushManyTxCalls, params)
	return m.pushManyTxResult, m.pushManyTxErr
}

// --- Mock Executor (satisfies asynqpg.Querier) ---

type mockExecutor struct{}

func (m *mockExecutor) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, nil //nolint:nilnil
}
func (m *mockExecutor) SelectContext(_ context.Context, _ any, _ string, _ ...any) error {
	return nil
}
func (m *mockExecutor) GetContext(_ context.Context, _ any, _ string, _ ...any) error {
	return nil
}
func (m *mockExecutor) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil //nolint:nilnil
}
func (m *mockExecutor) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

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

	id, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "email.send",
		Payload: []byte(`{"to":"user@example.com"}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero task ID")
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

// --- EnqueueTx Tests ---

func TestEnqueueTx_Success(t *testing.T) {
	repo := &mockProducerRepo{}
	p := newTestProducer(repo)

	_, err := p.EnqueueTx(context.Background(), &mockExecutor{}, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.pushTxCalls) != 1 {
		t.Fatalf("expected 1 PushTaskWithExecutor call, got %d", len(repo.pushTxCalls))
	}
}

func TestEnqueueTx_NilExecutor(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), nil, &asynqpg.Task{
		Type:    "test",
		Payload: []byte(`{}`),
	})
	if err == nil {
		t.Fatal("expected error for nil executor")
	}
}

func TestEnqueueTx_NilTask(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), &mockExecutor{}, nil)
	if err == nil {
		t.Fatal("expected error for nil task")
	}
}

func TestEnqueueTx_EmptyType(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})
	_, err := p.EnqueueTx(context.Background(), &mockExecutor{}, &asynqpg.Task{
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

	_, err := p.EnqueueTx(context.Background(), &mockExecutor{}, &asynqpg.Task{
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
		pushManyResult: []int64{1, 2, 3},
	}
	p := newTestProducer(repo)

	tasks := []*asynqpg.Task{
		{Type: "email", Payload: []byte(`{"id":1}`)},
		{Type: "email", Payload: []byte(`{"id":2}`)},
		{Type: "sms", Payload: []byte(`{"id":3}`)},
	}

	ids, err := p.EnqueueMany(context.Background(), tasks)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
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

	ids, err := p.EnqueueMany(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty slice, got %d IDs", len(ids))
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
		pushManyTxResult: []int64{1, 2},
	}
	p := newTestProducer(repo)

	ids, err := p.EnqueueManyTx(context.Background(), &mockExecutor{}, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{"id":1}`)},
		{Type: "test", Payload: []byte(`{"id":2}`)},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 IDs, got %d", len(ids))
	}

	if len(repo.pushManyTxCalls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(repo.pushManyTxCalls))
	}
}

func TestEnqueueManyTx_NilExecutor(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueManyTx(context.Background(), nil, []*asynqpg.Task{
		{Type: "test", Payload: []byte(`{}`)},
	})
	if err == nil {
		t.Fatal("expected error for nil executor")
	}
}

func TestEnqueueManyTx_Empty(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	ids, err := p.EnqueueManyTx(context.Background(), &mockExecutor{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected empty slice, got %d IDs", len(ids))
	}
}

func TestEnqueueManyTx_Validation(t *testing.T) {
	p := newTestProducer(&mockProducerRepo{})

	_, err := p.EnqueueManyTx(context.Background(), &mockExecutor{}, []*asynqpg.Task{
		nil,
	})
	if err == nil {
		t.Fatal("expected error for nil task")
	}
}

func TestEnqueueManyTx_RepoError(t *testing.T) {
	repo := &mockProducerRepo{pushManyTxErr: errors.New("tx batch error")}
	p := newTestProducer(repo)

	_, err := p.EnqueueManyTx(context.Background(), &mockExecutor{}, []*asynqpg.Task{
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
