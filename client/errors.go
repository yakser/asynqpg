package client

import "errors"

var (
	// ErrTaskNotFound is returned when the requested task does not exist.
	ErrTaskNotFound = errors.New("task not found")

	// ErrTaskRunning is returned when an operation cannot be performed
	// because the task is currently being processed.
	ErrTaskRunning = errors.New("task is currently running")

	// ErrTaskAlreadyFinalized is returned when an operation cannot be performed
	// because the task is already in a terminal state (completed).
	ErrTaskAlreadyFinalized = errors.New("task is already in a terminal state")

	// ErrTaskAlreadyAvailable is returned when trying to retry a task
	// that is already in a pending state.
	ErrTaskAlreadyAvailable = errors.New("task is already available for processing")
)
