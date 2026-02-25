package client

import (
	"time"

	"github.com/yakser/asynqpg"
)

// TaskInfo represents the full state of a task as stored in the database.
// It is returned by Client methods for task inspection and management.
type TaskInfo struct {
	ID               int64
	Type             string
	Payload          []byte
	Status           asynqpg.TaskStatus
	IdempotencyToken *string
	Messages         []string
	BlockedTill      time.Time
	AttemptsLeft     int
	AttemptsElapsed  int
	CreatedAt        time.Time
	UpdatedAt        time.Time
	FinalizedAt      *time.Time
	AttemptedAt      *time.Time
}
