package asynqpg

import (
	"context"
	"time"
)

type taskMetadataCtxKey struct{}

// TaskMetadata holds metadata about the task currently being processed.
// It is available inside handler contexts via GetTaskMetadata.
type TaskMetadata struct {
	ID         int64
	RetryCount int
	MaxRetry   int
	CreatedAt  time.Time
}

// WithTaskMetadata returns a new context with task metadata injected.
// Used internally by the consumer when invoking handlers.
// Can also be used in tests to create contexts for handler testing.
func WithTaskMetadata(ctx context.Context, meta TaskMetadata) context.Context {
	return context.WithValue(ctx, taskMetadataCtxKey{}, meta)
}

// GetTaskMetadata extracts the task metadata from the context.
// Returns the metadata and true if present, or zero value and false otherwise.
func GetTaskMetadata(ctx context.Context) (TaskMetadata, bool) {
	m, ok := ctx.Value(taskMetadataCtxKey{}).(TaskMetadata)
	return m, ok
}

// GetTaskID extracts the task's database ID from the context.
// Returns (0, false) if the context does not contain task metadata.
func GetTaskID(ctx context.Context) (int64, bool) {
	m, ok := GetTaskMetadata(ctx)
	if !ok {
		return 0, false
	}
	return m.ID, true
}

// GetRetryCount extracts the number of attempts already elapsed from the context.
// Returns (0, false) if the context does not contain task metadata.
func GetRetryCount(ctx context.Context) (int, bool) {
	m, ok := GetTaskMetadata(ctx)
	if !ok {
		return 0, false
	}
	return m.RetryCount, true
}

// GetMaxRetry extracts the total max retry count from the context.
// Returns (0, false) if the context does not contain task metadata.
func GetMaxRetry(ctx context.Context) (int, bool) {
	m, ok := GetTaskMetadata(ctx)
	if !ok {
		return 0, false
	}
	return m.MaxRetry, true
}

// GetCreatedAt extracts the task creation time from the context.
// Returns (zero time, false) if the context does not contain task metadata.
func GetCreatedAt(ctx context.Context) (time.Time, bool) {
	m, ok := GetTaskMetadata(ctx)
	if !ok {
		return time.Time{}, false
	}
	return m.CreatedAt, true
}
