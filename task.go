package asynqpg

import (
	"time"
)

// Task represents a unit of work to be enqueued.
// Use NewTask to create a task for enqueueing via Producer.
// Handlers receive *TaskInfo which contains runtime fields like ID, attempt info, etc.
type Task struct {
	Type             string
	Payload          []byte
	IdempotencyToken *string
	Delay            time.Duration
	MaxRetry         *int
	ProcessAt        time.Time
}

// NewTask creates a new task with the given type and payload.
func NewTask(taskType string, payload []byte, opts ...TaskOption) *Task {
	t := &Task{
		Type:    taskType,
		Payload: payload,
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// TaskOption configures a Task.
type TaskOption func(*Task)

// WithMaxRetry sets the maximum number of retries for the task.
func WithMaxRetry(n int) TaskOption {
	return func(t *Task) {
		t.MaxRetry = &n
	}
}

// WithDelay sets the delay before the task becomes available for processing.
func WithDelay(d time.Duration) TaskOption {
	return func(t *Task) {
		t.Delay = d
	}
}

// WithIdempotencyToken sets the idempotency token for the task.
func WithIdempotencyToken(token string) TaskOption {
	return func(t *Task) {
		t.IdempotencyToken = &token
	}
}
