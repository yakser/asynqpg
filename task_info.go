package asynqpg

import "time"

// TaskInfo represents a task fetched from the database for processing.
// Handlers receive *TaskInfo with all runtime information.
// Unlike Task (used for enqueueing), TaskInfo includes database-assigned fields.
type TaskInfo struct {
	ID               int64
	Type             string
	Payload          []byte
	IdempotencyToken *string
	AttemptsLeft     int
	AttemptsElapsed  int
	CreatedAt        time.Time
	AttemptedAt      *time.Time
	Messages         []string
}

// TaskStatus represents the current status of a task in the database.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// IsFinalized returns true if the task is in a terminal state.
func (s TaskStatus) IsFinalized() bool {
	return s == TaskStatusCompleted || s == TaskStatusFailed || s == TaskStatusCancelled
}
