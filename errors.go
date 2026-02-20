package asynqpg

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrorHandler is called when a task fails permanently (exhausted all retries)
// or encounters an unrecoverable error (ErrSkipRetry, panic).
// Implementations can use this for alerting, dead letter queue routing,
// or external error tracking (e.g., Sentry, PagerDuty).
type ErrorHandler interface {
	HandleError(ctx context.Context, task *TaskInfo, err error)
}

// ErrorHandlerFunc is an adapter to allow ordinary functions to be used as ErrorHandler.
type ErrorHandlerFunc func(ctx context.Context, task *TaskInfo, err error)

// HandleError calls f(ctx, task, err).
func (f ErrorHandlerFunc) HandleError(ctx context.Context, task *TaskInfo, err error) {
	f(ctx, task, err)
}

// ErrSkipRetry is a sentinel error that handlers can return to indicate
// the task should not be retried and should immediately be marked as failed.
// This is useful for non-retryable errors such as invalid payloads or
// business logic rejections.
//
// Usage:
//
//	func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
//	    if invalidPayload(task.Payload) {
//	        return fmt.Errorf("bad payload: %w", asynqpg.ErrSkipRetry)
//	    }
//	    // ...
//	}
var ErrSkipRetry = errors.New("skip retry for the task")

// TaskSnoozeError is returned by TaskSnooze. Detected via errors.As.
// When a handler returns this error, the task is rescheduled after Duration
// without counting it as an attempt – attempts_left and attempts_elapsed remain unchanged.
type TaskSnoozeError struct {
	Duration time.Duration
}

func (e *TaskSnoozeError) Error() string {
	return fmt.Sprintf("task snoozed for %s", e.Duration)
}

func (e *TaskSnoozeError) Is(target error) bool {
	_, ok := target.(*TaskSnoozeError)
	return ok
}

// TaskSnooze returns an error that reschedules the task after the given duration
// without counting it as an attempt. The task's attempts_left and attempts_elapsed
// remain unchanged. Panics if duration < 0.
//
// Usage:
//
//	func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
//	    if !isReady() {
//	        return asynqpg.TaskSnooze(30 * time.Second)
//	    }
//	    // ...
//	}
func TaskSnooze(d time.Duration) error {
	if d < 0 {
		panic("asynqpg.TaskSnooze: duration must not be negative")
	}
	return &TaskSnoozeError{Duration: d}
}

// TaskSnoozeWithErrError is returned by TaskSnoozeWithError. Detected via errors.As.
// When a handler returns this error, the task is rescheduled after Duration,
// counting it as a failed attempt – attempts_left is decremented and the error message is stored.
type TaskSnoozeWithErrError struct {
	Duration time.Duration
}

func (e *TaskSnoozeWithErrError) Error() string {
	return fmt.Sprintf("task snoozed with error for %s", e.Duration)
}

func (e *TaskSnoozeWithErrError) Is(target error) bool {
	_, ok := target.(*TaskSnoozeWithErrError)
	return ok
}

// TaskSnoozeWithError returns an error that reschedules the task after the given duration,
// counting it as a failed attempt. The error message is stored and attempts_left is decremented.
// If no attempts are left, the task is failed instead of snoozed.
// Panics if duration < 0.
//
// Usage:
//
//	func (h *MyHandler) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
//	    if err := callExternalAPI(); err != nil {
//	        return fmt.Errorf("api unavailable: %w", asynqpg.TaskSnoozeWithError(1 * time.Minute))
//	    }
//	    // ...
//	}
func TaskSnoozeWithError(d time.Duration) error {
	if d < 0 {
		panic("asynqpg.TaskSnoozeWithError: duration must not be negative")
	}
	return &TaskSnoozeWithErrError{Duration: d}
}
