package asynqpg

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrSkipRetry(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "skip retry for the task", ErrSkipRetry.Error())
}

func TestErrSkipRetry_ErrorsIs(t *testing.T) {
	t.Parallel()

	assert.True(t, errors.Is(ErrSkipRetry, ErrSkipRetry))
}

func TestErrSkipRetry_WrappedErrorsIs(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("invalid payload: %w", ErrSkipRetry)

	assert.True(t, errors.Is(wrapped, ErrSkipRetry))
}

func TestErrSkipRetry_UnrelatedError(t *testing.T) {
	t.Parallel()

	unrelated := errors.New("some other error")

	assert.False(t, errors.Is(unrelated, ErrSkipRetry))
}

func TestTaskSnooze(t *testing.T) {
	t.Parallel()

	err := TaskSnooze(5 * time.Second)

	var snoozeErr *TaskSnoozeError
	require.True(t, errors.As(err, &snoozeErr))
	assert.Equal(t, 5*time.Second, snoozeErr.Duration)
	assert.Equal(t, "task snoozed for 5s", err.Error())
}

func TestTaskSnooze_ErrorsAs(t *testing.T) {
	t.Parallel()

	err := TaskSnooze(10 * time.Second)

	var snoozeErr *TaskSnoozeError
	assert.True(t, errors.As(err, &snoozeErr))
	assert.Equal(t, 10*time.Second, snoozeErr.Duration)
}

func TestTaskSnooze_Wrapped(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("not ready yet: %w", TaskSnooze(30*time.Second))

	var snoozeErr *TaskSnoozeError
	require.True(t, errors.As(wrapped, &snoozeErr))
	assert.Equal(t, 30*time.Second, snoozeErr.Duration)
}

func TestTaskSnooze_Is(t *testing.T) {
	t.Parallel()

	err1 := TaskSnooze(1 * time.Second)
	err2 := TaskSnooze(5 * time.Minute)

	assert.True(t, errors.Is(err1, &TaskSnoozeError{}))
	assert.True(t, errors.Is(err2, &TaskSnoozeError{}))

	unrelated := errors.New("some other error")
	assert.False(t, errors.Is(unrelated, &TaskSnoozeError{}))
}

func TestTaskSnooze_ZeroDuration(t *testing.T) {
	t.Parallel()

	err := TaskSnooze(0)

	var snoozeErr *TaskSnoozeError
	require.True(t, errors.As(err, &snoozeErr))
	assert.Equal(t, time.Duration(0), snoozeErr.Duration)
}

func TestTaskSnooze_NegativeDuration_Panics(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() {
		TaskSnooze(-1 * time.Second)
	})
}

func TestTaskSnoozeWithError(t *testing.T) {
	t.Parallel()

	err := TaskSnoozeWithError(5 * time.Second)

	var snoozeErr *TaskSnoozeWithErrError
	require.True(t, errors.As(err, &snoozeErr))
	assert.Equal(t, 5*time.Second, snoozeErr.Duration)
	assert.Equal(t, "task snoozed with error for 5s", err.Error())
}

func TestTaskSnoozeWithError_ErrorsAs(t *testing.T) {
	t.Parallel()

	err := TaskSnoozeWithError(10 * time.Second)

	var snoozeErr *TaskSnoozeWithErrError
	assert.True(t, errors.As(err, &snoozeErr))
	assert.Equal(t, 10*time.Second, snoozeErr.Duration)
}

func TestTaskSnoozeWithError_Wrapped(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("api unavailable: %w", TaskSnoozeWithError(1*time.Minute))

	var snoozeErr *TaskSnoozeWithErrError
	require.True(t, errors.As(wrapped, &snoozeErr))
	assert.Equal(t, 1*time.Minute, snoozeErr.Duration)
}

func TestTaskSnoozeWithError_Is(t *testing.T) {
	t.Parallel()

	err1 := TaskSnoozeWithError(1 * time.Second)
	err2 := TaskSnoozeWithError(5 * time.Minute)

	assert.True(t, errors.Is(err1, &TaskSnoozeWithErrError{}))
	assert.True(t, errors.Is(err2, &TaskSnoozeWithErrError{}))

	unrelated := errors.New("some other error")
	assert.False(t, errors.Is(unrelated, &TaskSnoozeWithErrError{}))
}

func TestTaskSnoozeWithError_ZeroDuration(t *testing.T) {
	t.Parallel()

	err := TaskSnoozeWithError(0)

	var snoozeErr *TaskSnoozeWithErrError
	require.True(t, errors.As(err, &snoozeErr))
	assert.Equal(t, time.Duration(0), snoozeErr.Duration)
}

func TestTaskSnoozeWithError_NegativeDuration_Panics(t *testing.T) {
	t.Parallel()

	assert.Panics(t, func() {
		TaskSnoozeWithError(-1 * time.Second)
	})
}

func TestErrorHandlerFunc(t *testing.T) {
	t.Parallel()

	var gotCtx context.Context
	var gotTask *TaskInfo
	var gotErr error

	handler := ErrorHandlerFunc(func(ctx context.Context, task *TaskInfo, err error) {
		gotCtx = ctx
		gotTask = task
		gotErr = err
	})

	ctx := context.Background()
	task := &TaskInfo{ID: 42, Type: "test"}
	err := fmt.Errorf("permanent failure")

	handler.HandleError(ctx, task, err)

	assert.Equal(t, ctx, gotCtx)
	assert.Equal(t, task, gotTask)
	assert.Equal(t, err, gotErr)
}

func TestErrorHandlerFunc_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ErrorHandler = ErrorHandlerFunc(func(ctx context.Context, task *TaskInfo, err error) {})
}

func TestTaskSnooze_NotConfusedWithTaskSnoozeWithError(t *testing.T) {
	t.Parallel()

	snoozeErr := TaskSnooze(1 * time.Second)
	snoozeWithErrErr := TaskSnoozeWithError(1 * time.Second)

	var s *TaskSnoozeError
	var sw *TaskSnoozeWithErrError

	assert.True(t, errors.As(snoozeErr, &s))
	assert.False(t, errors.As(snoozeErr, &sw))

	assert.False(t, errors.As(snoozeWithErrErr, &s))
	assert.True(t, errors.As(snoozeWithErrErr, &sw))
}
