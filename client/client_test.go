package client

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client/mocks"
	"github.com/yakser/asynqpg/internal/repository"
)

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

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().GetTaskByID(mock.Anything, int64(42)).
		Return(newTestFullTask(42, "pending"), nil).Once()
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

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().GetTaskByID(mock.Anything, int64(999)).
		Return(nil, fmt.Errorf("get task by id: %w", sql.ErrNoRows)).Once()
	c := newWithRepo(repo)

	_, err := c.GetTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

func TestUnitGetTask_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().GetTaskByID(mock.Anything, int64(1)).
		Return(nil, fmt.Errorf("connection refused")).Once()
	c := newWithRepo(repo)

	_, err := c.GetTask(context.Background(), 1)

	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrTaskNotFound)
	assert.Contains(t, err.Error(), "connection refused")
}

// --- CancelTask unit tests ---

func TestUnitCancelTask_Updated(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().CancelTaskByID(mock.Anything, int64(10)).
		Return(newFinalizedTask(10, "cancelled"), true, nil).Once()
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
	assert.NotNil(t, got.FinalizedAt)
}

func TestUnitCancelTask_RunningSuccess(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().CancelTaskByID(mock.Anything, int64(10)).
		Return(newFinalizedTask(10, "cancelled"), true, nil).Once()
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
	assert.NotNil(t, got.FinalizedAt)
}

func TestUnitCancelTask_CompletedError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().CancelTaskByID(mock.Anything, int64(10)).
		Return(newFinalizedTask(10, "completed"), false, nil).Once()
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskAlreadyFinalized)
	assert.NotNil(t, got)
}

func TestUnitCancelTask_AlreadyCancelled(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().CancelTaskByID(mock.Anything, int64(10)).
		Return(newFinalizedTask(10, "cancelled"), false, nil).Once()
	c := newWithRepo(repo)

	got, err := c.CancelTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusCancelled, got.Status)
}

func TestUnitCancelTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().CancelTaskByID(mock.Anything, int64(999)).
		Return(nil, false, fmt.Errorf("cancel task: %w", sql.ErrNoRows)).Once()
	c := newWithRepo(repo)

	_, err := c.CancelTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- RetryTask unit tests ---

func TestUnitRetryTask_Updated(t *testing.T) {
	t.Parallel()

	task := newTestFullTask(10, "pending")
	task.AttemptsLeft = 1

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().RetryTaskByID(mock.Anything, int64(10)).
		Return(task, true, nil).Once()
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, asynqpg.TaskStatusPending, got.Status)
	assert.Equal(t, 1, got.AttemptsLeft)
}

func TestUnitRetryTask_PendingError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().RetryTaskByID(mock.Anything, int64(10)).
		Return(newTestFullTask(10, "pending"), false, nil).Once()
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskAlreadyAvailable)
	assert.NotNil(t, got)
}

func TestUnitRetryTask_RunningError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().RetryTaskByID(mock.Anything, int64(10)).
		Return(newTestFullTask(10, "running"), false, nil).Once()
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskRunning)
	assert.NotNil(t, got)
}

func TestUnitRetryTask_CompletedError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().RetryTaskByID(mock.Anything, int64(10)).
		Return(newFinalizedTask(10, "completed"), false, nil).Once()
	c := newWithRepo(repo)

	got, err := c.RetryTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskAlreadyFinalized)
	assert.NotNil(t, got)
}

func TestUnitRetryTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().RetryTaskByID(mock.Anything, int64(999)).
		Return(nil, false, fmt.Errorf("retry task: %w", sql.ErrNoRows)).Once()
	c := newWithRepo(repo)

	_, err := c.RetryTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- DeleteTask unit tests ---

func TestUnitDeleteTask_Deleted(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().DeleteTaskByID(mock.Anything, int64(10)).
		Return(newTestFullTask(10, "pending"), true, nil).Once()
	c := newWithRepo(repo)

	got, err := c.DeleteTask(context.Background(), 10)

	require.NoError(t, err)
	assert.Equal(t, int64(10), got.ID)
}

func TestUnitDeleteTask_RunningError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().DeleteTaskByID(mock.Anything, int64(10)).
		Return(newTestFullTask(10, "running"), false, nil).Once()
	c := newWithRepo(repo)

	got, err := c.DeleteTask(context.Background(), 10)

	assert.ErrorIs(t, err, ErrTaskRunning)
	assert.NotNil(t, got)
}

func TestUnitDeleteTask_NotFound(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().DeleteTaskByID(mock.Anything, int64(999)).
		Return(nil, false, fmt.Errorf("delete task: %w", sql.ErrNoRows)).Once()
	c := newWithRepo(repo)

	_, err := c.DeleteTask(context.Background(), 999)

	assert.ErrorIs(t, err, ErrTaskNotFound)
}

// --- ListTasks unit tests ---

func TestUnitListTasks_Success(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().ListTasks(mock.Anything, mock.Anything).
		Run(func(_ context.Context, params repository.ListTasksParams) {
			assert.Equal(t, 50, params.Limit)
			assert.Equal(t, []string{"failed"}, params.Statuses)
		}).
		Return(&repository.ListTasksResult{
			Tasks: []repository.FullTask{
				*newFinalizedTask(1, "failed"),
				*newFinalizedTask(2, "failed"),
			},
			Total: 2,
		}, nil).Once()
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

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().ListTasks(mock.Anything, mock.Anything).
		Run(func(_ context.Context, params repository.ListTasksParams) {
			assert.Equal(t, defaultLimit, params.Limit)
			assert.Equal(t, "id", params.OrderBy)
			assert.Equal(t, "ASC", params.OrderDir)
		}).
		Return(&repository.ListTasksResult{Tasks: []repository.FullTask{}, Total: 0}, nil).Once()
	c := newWithRepo(repo)

	result, err := c.ListTasks(context.Background(), nil)

	require.NoError(t, err)
	assert.Equal(t, 0, result.Total)
	assert.Empty(t, result.Tasks)
}

func TestUnitListTasks_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewTaskRepository(t)
	repo.EXPECT().ListTasks(mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("database timeout")).Once()
	c := newWithRepo(repo)

	_, err := c.ListTasks(context.Background(), NewListParams())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database timeout")
}
