package client

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/repository"
)

// --- Mock repository ---

type mockRepo struct {
	getTaskByIDFn    func(ctx context.Context, id int64) (*repository.FullTask, error)
	listTasksFn      func(ctx context.Context, params repository.ListTasksParams) (*repository.ListTasksResult, error)
	cancelTaskByIDFn func(ctx context.Context, id int64) (*repository.FullTask, bool, error)
	retryTaskByIDFn  func(ctx context.Context, id int64) (*repository.FullTask, bool, error)
	deleteTaskByIDFn func(ctx context.Context, id int64) (*repository.FullTask, bool, error)
}

func (m *mockRepo) GetTaskByID(ctx context.Context, id int64) (*repository.FullTask, error) {
	return m.getTaskByIDFn(ctx, id)
}

func (m *mockRepo) GetTaskByIDWithExecutor(_ context.Context, _ asynqpg.Querier, _ int64) (*repository.FullTask, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *mockRepo) ListTasks(ctx context.Context, params repository.ListTasksParams) (*repository.ListTasksResult, error) {
	return m.listTasksFn(ctx, params)
}

func (m *mockRepo) ListTasksWithExecutor(_ context.Context, _ asynqpg.Querier, _ repository.ListTasksParams) (*repository.ListTasksResult, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

func (m *mockRepo) CancelTaskByID(ctx context.Context, id int64) (*repository.FullTask, bool, error) {
	return m.cancelTaskByIDFn(ctx, id)
}

func (m *mockRepo) CancelTaskByIDWithExecutor(_ context.Context, _ asynqpg.Querier, _ int64) (*repository.FullTask, bool, error) {
	return nil, false, fmt.Errorf("not implemented in mock")
}

func (m *mockRepo) RetryTaskByID(ctx context.Context, id int64) (*repository.FullTask, bool, error) {
	return m.retryTaskByIDFn(ctx, id)
}

func (m *mockRepo) RetryTaskByIDWithExecutor(_ context.Context, _ asynqpg.Querier, _ int64) (*repository.FullTask, bool, error) {
	return nil, false, fmt.Errorf("not implemented in mock")
}

func (m *mockRepo) DeleteTaskByID(ctx context.Context, id int64) (*repository.FullTask, bool, error) {
	return m.deleteTaskByIDFn(ctx, id)
}

func (m *mockRepo) DeleteTaskByIDWithExecutor(_ context.Context, _ asynqpg.Querier, _ int64) (*repository.FullTask, bool, error) {
	return nil, false, fmt.Errorf("not implemented in mock")
}

// --- Helpers ---

func newTestFullTask(id int64, status string) *repository.FullTask {
	now := time.Now()
	return &repository.FullTask{
		ID:              id,
		Type:            "test-type",
		Payload:         []byte(`{"key":"value"}`),
		Status:          status,
		Messages:        pq.StringArray{},
		BlockedTill:     now,
		AttemptsLeft:    3,
		AttemptsElapsed: 0,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func newFinalizedTask(id int64, status string) *repository.FullTask {
	t := newTestFullTask(id, status)
	now := time.Now()
	t.FinalizedAt = &now
	return t
}

// --- GetTask unit tests ---

func TestUnitGetTask_Success(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		getTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, error) {
			return newTestFullTask(id, "pending"), nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.GetTask(context.Background(), 42)

	require.NoError(t, err)
	assert.Equal(t, int64(42), got.ID)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Equal(t, "test-type", got.Type)
	assert.Equal(t, []byte(`{"key":"value"}`), got.Payload)
}

func TestUnitGetTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		getTaskByIDFn: func(_ context.Context, _ int64) (*repository.FullTask, error) {
			return nil, fmt.Errorf("get task by id: %w", sql.ErrNoRows)
		},
	}
	c := newWithRepo(repo)

	_, err := c.GetTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

func TestUnitGetTask_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		getTaskByIDFn: func(_ context.Context, _ int64) (*repository.FullTask, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	c := newWithRepo(repo)

	_, err := c.GetTask(context.Background(), 1)

	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrTaskNotFound)
	assert.Contains(t, err.Error(), "connection refused")
}

// --- CancelTask unit tests ---

func TestUnitCancelTask_Updated(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		cancelTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			task := newFinalizedTask(id, "cancelled")
			return task, true, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
	assert.NotNil(t, got.FinalizedAt)
}

func TestUnitCancelTask_RunningSuccess(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		cancelTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			task := newFinalizedTask(id, "cancelled")
			return task, true, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
	assert.NotNil(t, got.FinalizedAt)
}

func TestUnitCancelTask_CompletedError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		cancelTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newFinalizedTask(id, "completed"), false, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskAlreadyFinalized)
	assert.NotNil(t, got)
}

func TestUnitCancelTask_AlreadyCancelled(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		cancelTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newFinalizedTask(id, "cancelled"), false, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
}

func TestUnitCancelTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		cancelTaskByIDFn: func(_ context.Context, _ int64) (*repository.FullTask, bool, error) {
			return nil, false, fmt.Errorf("cancel task: %w", sql.ErrNoRows)
		},
	}
	c := newWithRepo(repo)

	_, err := c.CancelTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- RetryTask unit tests ---

func TestUnitRetryTask_Updated(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		retryTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			task := newTestFullTask(id, "pending")
			task.AttemptsLeft = 1
			return task, true, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Equal(t, 1, got.AttemptsLeft)
}

func TestUnitRetryTask_PendingError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		retryTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newTestFullTask(id, "pending"), false, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskAlreadyAvailable)
	assert.NotNil(t, got)
}

func TestUnitRetryTask_RunningError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		retryTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newTestFullTask(id, "running"), false, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskRunning)
	assert.NotNil(t, got)
}

func TestUnitRetryTask_CompletedError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		retryTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newFinalizedTask(id, "completed"), false, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskAlreadyFinalized)
	assert.NotNil(t, got)
}

func TestUnitRetryTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		retryTaskByIDFn: func(_ context.Context, _ int64) (*repository.FullTask, bool, error) {
			return nil, false, fmt.Errorf("retry task: %w", sql.ErrNoRows)
		},
	}
	c := newWithRepo(repo)

	_, err := c.RetryTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- DeleteTask unit tests ---

func TestUnitDeleteTask_Deleted(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		deleteTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newTestFullTask(id, "pending"), true, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.DeleteTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, int64(10), got.ID)
}

func TestUnitDeleteTask_RunningError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		deleteTaskByIDFn: func(_ context.Context, id int64) (*repository.FullTask, bool, error) {
			return newTestFullTask(id, "running"), false, nil
		},
	}
	c := newWithRepo(repo)

	got, err := c.DeleteTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskRunning)
	assert.NotNil(t, got)
}

func TestUnitDeleteTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		deleteTaskByIDFn: func(_ context.Context, _ int64) (*repository.FullTask, bool, error) {
			return nil, false, fmt.Errorf("delete task: %w", sql.ErrNoRows)
		},
	}
	c := newWithRepo(repo)

	_, err := c.DeleteTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- ListTasks unit tests ---

func TestUnitListTasks_Success(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		listTasksFn: func(_ context.Context, params repository.ListTasksParams) (*repository.ListTasksResult, error) {
			assert.Equal(t, 50, params.Limit)
			assert.Equal(t, []string{"failed"}, params.Statuses)
			return &repository.ListTasksResult{
				Tasks: []repository.FullTask{
					*newFinalizedTask(1, "failed"),
					*newFinalizedTask(2, "failed"),
				},
				Total: 2,
			}, nil
		},
	}
	c := newWithRepo(repo)

	result, err := c.ListTasks(context.Background(),
		NewListParams().States(asynqpg.TaskStatusFailed).Limit(50))

	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	assert.Len(t, result.Tasks, 2)
	assert.Equal(t, asynqpg.TaskStatusFailed, result.Tasks[0].Status)
}

func TestUnitListTasks_NilParams(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		listTasksFn: func(_ context.Context, params repository.ListTasksParams) (*repository.ListTasksResult, error) {
			assert.Equal(t, defaultLimit, params.Limit)
			assert.Equal(t, "id", params.OrderBy)
			assert.Equal(t, "ASC", params.OrderDir)
			return &repository.ListTasksResult{Tasks: []repository.FullTask{}, Total: 0}, nil
		},
	}
	c := newWithRepo(repo)

	result, err := c.ListTasks(context.Background(), nil)

	require.NoError(t, err)
	assert.Equal(t, 0, result.Total)
	assert.Empty(t, result.Tasks)
}

func TestUnitListTasks_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockRepo{
		listTasksFn: func(_ context.Context, _ repository.ListTasksParams) (*repository.ListTasksResult, error) {
			return nil, fmt.Errorf("database timeout")
		},
	}
	c := newWithRepo(repo)

	_, err := c.ListTasks(context.Background(), NewListParams())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database timeout")
}
