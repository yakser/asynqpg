package producer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/internal/repository"
	rootmocks "github.com/yakser/asynqpg/mocks"
	"github.com/yakser/asynqpg/producer/mocks"
)

const taskTypeTest = "test"

// newTestProducer creates a Producer with a mock repo.
func newTestProducer(repo producerRepo) *Producer {
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

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			require.Equal(t, "email.send", task.Type)
			require.Equal(t, 3, task.AttemptsLeft)
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	result, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "email.send",
		Payload: []byte(`{"to":"user@example.com"}`),
	})
	require.NoError(t, err)
	require.NotZero(t, result.ID)
	require.False(t, result.Duplicate)
}

func TestEnqueue_NilTask(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), nil)
	require.Error(t, err)
}

func TestEnqueue_EmptyType(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    "",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueue_NilPayload(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: nil,
	})
	require.Error(t, err)
}

func TestEnqueue_WithDelay(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			require.Equal(t, 5*time.Second, task.Delay.Duration())
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
		Delay:   5 * time.Second,
	})
	require.NoError(t, err)
}

func TestEnqueue_WithProcessAt(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			delay := task.Delay.Duration()
			// Should be approximately 10 seconds
			require.InDelta(t, 10*time.Second, delay, float64(time.Second))
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	processAt := time.Now().Add(10 * time.Second)
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:      taskTypeTest,
		Payload:   []byte(`{}`),
		ProcessAt: processAt,
	})
	require.NoError(t, err)
}

func TestEnqueue_ProcessAtInPast(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			require.Equal(t, time.Duration(0), task.Delay.Duration())
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:      taskTypeTest,
		Payload:   []byte(`{}`),
		ProcessAt: time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
}

func TestEnqueue_CustomMaxRetry(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			require.Equal(t, 10, task.AttemptsLeft)
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:     taskTypeTest,
		Payload:  []byte(`{}`),
		MaxRetry: ptr.Get(10),
	})
	require.NoError(t, err)
}

func TestEnqueue_DefaultMaxRetry(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			require.Equal(t, 3, task.AttemptsLeft)
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
}

func TestEnqueue_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Return(repository.PushTaskResult{}, errors.New("db error")).Once()
	p := newTestProducer(repo)

	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueue_IdempotencyToken(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, task *repository.PushTaskParams) {
			require.NotNil(t, task.IdempotencyToken)
			require.Equal(t, "unique-token", *task.IdempotencyToken)
		}).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)

	token := "unique-token"
	_, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             taskTypeTest,
		Payload:          []byte(`{}`),
		IdempotencyToken: &token,
	})
	require.NoError(t, err)
}

func TestEnqueue_Duplicate(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTask(mock.Anything, mock.Anything).
		Return(repository.PushTaskResult{ID: 42, Duplicate: true}, nil).Once()
	p := newTestProducer(repo)

	result, err := p.Enqueue(context.Background(), &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
	require.Equal(t, int64(42), result.ID)
	require.True(t, result.Duplicate)
}

func TestEnqueueTx_Success(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTaskWithTx(mock.Anything, mock.Anything, mock.Anything).
		Return(repository.PushTaskResult{ID: 1}, nil).Once()
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	_, err := p.EnqueueTx(context.Background(), tx, &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
	})
	require.NoError(t, err)
}

func TestEnqueueTx_NilTx(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.EnqueueTx(context.Background(), nil, &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueueTx_NilTask(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	_, err := p.EnqueueTx(context.Background(), tx, nil)
	require.Error(t, err)
}

func TestEnqueueTx_EmptyType(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	_, err := p.EnqueueTx(context.Background(), tx, &asynqpg.Task{
		Type:    "",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueueTx_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTaskWithTx(mock.Anything, mock.Anything, mock.Anything).
		Return(repository.PushTaskResult{}, errors.New("tx error")).Once()
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	_, err := p.EnqueueTx(context.Background(), tx, &asynqpg.Task{
		Type:    taskTypeTest,
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
}

func TestEnqueueMany_Success(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTasks(mock.Anything, mock.Anything).
		Run(func(_ context.Context, params repository.PushTasksParams) {
			require.Len(t, params.Tasks, 3)
		}).
		Return([]repository.PushTaskResult{
			{ID: 1}, {ID: 2}, {ID: 3},
		}, nil).Once()
	p := newTestProducer(repo)

	tasks := []*asynqpg.Task{
		{Type: "email", Payload: []byte(`{"id":1}`)},
		{Type: "email", Payload: []byte(`{"id":2}`)},
		{Type: "sms", Payload: []byte(`{"id":3}`)},
	}

	result, err := p.EnqueueMany(context.Background(), tasks)
	require.NoError(t, err)
	require.Len(t, result.Results, 3)
}

func TestEnqueueMany_Empty(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	result, err := p.EnqueueMany(context.Background(), nil)
	require.NoError(t, err)
	require.Empty(t, result.Results)
}

func TestEnqueueMany_NilTask(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "ok", Payload: []byte(`{}`)},
		nil,
	})
	require.Error(t, err)
}

func TestEnqueueMany_EmptyType(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: "", Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestEnqueueMany_NilPayload(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: taskTypeTest, Payload: nil},
	})
	require.Error(t, err)
}

func TestEnqueueMany_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTasks(mock.Anything, mock.Anything).
		Return(nil, errors.New("batch error")).Once()
	p := newTestProducer(repo)

	_, err := p.EnqueueMany(context.Background(), []*asynqpg.Task{
		{Type: taskTypeTest, Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestEnqueueManyTx_Success(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTasksWithTx(mock.Anything, mock.Anything, mock.Anything).
		Return([]repository.PushTaskResult{
			{ID: 1}, {ID: 2},
		}, nil).Once()
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	result, err := p.EnqueueManyTx(context.Background(), tx, []*asynqpg.Task{
		{Type: taskTypeTest, Payload: []byte(`{"id":1}`)},
		{Type: taskTypeTest, Payload: []byte(`{"id":2}`)},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 2)
}

func TestEnqueueManyTx_NilTx(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)

	_, err := p.EnqueueManyTx(context.Background(), nil, []*asynqpg.Task{
		{Type: taskTypeTest, Payload: []byte(`{}`)},
	})
	require.Error(t, err)
}

func TestEnqueueManyTx_Empty(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	result, err := p.EnqueueManyTx(context.Background(), tx, nil)
	require.NoError(t, err)
	require.Empty(t, result.Results)
}

func TestEnqueueManyTx_Validation(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	_, err := p.EnqueueManyTx(context.Background(), tx, []*asynqpg.Task{
		nil,
	})
	require.Error(t, err)
}

func TestEnqueueManyTx_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	repo.EXPECT().PushTasksWithTx(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("tx batch error")).Once()
	p := newTestProducer(repo)
	tx := rootmocks.NewTx(t)

	_, err := p.EnqueueManyTx(context.Background(), tx, []*asynqpg.Task{
		{Type: taskTypeTest, Payload: []byte(`{}`)},
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

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	task := &asynqpg.Task{Type: taskTypeTest, Payload: []byte(`{}`)}

	delay := p.calculateDelay(task)
	require.Equal(t, time.Duration(0), delay)
}

func TestCalculateDelay_WithDelay(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	task := &asynqpg.Task{Type: taskTypeTest, Payload: []byte(`{}`), Delay: 5 * time.Second}

	delay := p.calculateDelay(task)
	require.Equal(t, 5*time.Second, delay)
}

func TestCalculateDelay_ProcessAtOverridesDelay(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	task := &asynqpg.Task{
		Type:      taskTypeTest,
		Payload:   []byte(`{}`),
		Delay:     time.Hour,
		ProcessAt: time.Now().Add(10 * time.Second),
	}

	delay := p.calculateDelay(task)
	require.InDelta(t, 10*time.Second, delay, float64(time.Second))
}

func TestCalculateMaxRetry_Custom(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	task := &asynqpg.Task{MaxRetry: ptr.Get(7)}

	result := p.calculateMaxRetry(task)
	require.Equal(t, 7, result)
}

func TestCalculateMaxRetry_Default(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	task := &asynqpg.Task{}

	result := p.calculateMaxRetry(task)
	require.Equal(t, 3, result)
}

func TestCalculateMaxRetry_ExplicitZero(t *testing.T) {
	t.Parallel()

	repo := mocks.NewProducerRepo(t)
	p := newTestProducer(repo)
	task := &asynqpg.Task{MaxRetry: ptr.Get(0)}

	result := p.calculateMaxRetry(task)
	require.Equal(t, 0, result)
}
