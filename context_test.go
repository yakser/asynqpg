package asynqpg

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetTaskMetadata(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	ctx := WithTaskMetadata(context.Background(), TaskMetadata{ID: 42, RetryCount: 2, MaxRetry: 5, CreatedAt: createdAt})

	got, ok := GetTaskMetadata(ctx)

	assert.True(t, ok)
	assert.Equal(t, TaskMetadata{ID: 42, RetryCount: 2, MaxRetry: 5, CreatedAt: createdAt}, got)
}

func TestGetTaskMetadata_BackgroundContext(t *testing.T) {
	t.Parallel()

	got, ok := GetTaskMetadata(context.Background())

	assert.False(t, ok)
	assert.Equal(t, TaskMetadata{}, got)
}

func TestGetTaskID(t *testing.T) {
	t.Parallel()

	ctx := WithTaskMetadata(context.Background(), TaskMetadata{ID: 42, RetryCount: 2, MaxRetry: 5})

	got, ok := GetTaskID(ctx)

	assert.True(t, ok)
	assert.Equal(t, int64(42), got)
}

func TestGetRetryCount(t *testing.T) {
	t.Parallel()

	ctx := WithTaskMetadata(context.Background(), TaskMetadata{ID: 42, RetryCount: 2, MaxRetry: 5})

	got, ok := GetRetryCount(ctx)

	assert.True(t, ok)
	assert.Equal(t, 2, got)
}

func TestGetMaxRetry(t *testing.T) {
	t.Parallel()

	ctx := WithTaskMetadata(context.Background(), TaskMetadata{ID: 42, RetryCount: 2, MaxRetry: 5})

	got, ok := GetMaxRetry(ctx)

	assert.True(t, ok)
	assert.Equal(t, 5, got)
}

func TestGetCreatedAt(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2026, 3, 1, 12, 30, 0, 0, time.UTC)
	ctx := WithTaskMetadata(context.Background(), TaskMetadata{ID: 42, RetryCount: 2, MaxRetry: 5, CreatedAt: createdAt})

	got, ok := GetCreatedAt(ctx)

	assert.True(t, ok)
	assert.Equal(t, createdAt, got)
}

func TestGetCreatedAt_BackgroundContext(t *testing.T) {
	t.Parallel()

	got, ok := GetCreatedAt(context.Background())

	assert.False(t, ok)
	assert.True(t, got.IsZero())
}

func TestGetTaskID_BackgroundContext(t *testing.T) {
	t.Parallel()

	got, ok := GetTaskID(context.Background())

	assert.False(t, ok)
	assert.Equal(t, int64(0), got)
}

func TestGetRetryCount_BackgroundContext(t *testing.T) {
	t.Parallel()

	got, ok := GetRetryCount(context.Background())

	assert.False(t, ok)
	assert.Equal(t, 0, got)
}

func TestGetMaxRetry_BackgroundContext(t *testing.T) {
	t.Parallel()

	got, ok := GetMaxRetry(context.Background())

	assert.False(t, ok)
	assert.Equal(t, 0, got)
}

func TestWithTaskMetadata_PreservesParentValues(t *testing.T) {
	t.Parallel()

	type parentKey struct{}
	parentCtx := context.WithValue(context.Background(), parentKey{}, "parent-value")

	ctx := WithTaskMetadata(parentCtx, TaskMetadata{ID: 99, RetryCount: 1, MaxRetry: 3})

	got, ok := GetTaskID(ctx)
	assert.True(t, ok)
	assert.Equal(t, int64(99), got)

	parentVal := ctx.Value(parentKey{})
	assert.Equal(t, "parent-value", parentVal)
}

func TestGetTaskMetadata_ZeroValues(t *testing.T) {
	t.Parallel()

	ctx := WithTaskMetadata(context.Background(), TaskMetadata{})

	id, ok := GetTaskID(ctx)
	assert.True(t, ok)
	assert.Equal(t, int64(0), id)

	retryCount, ok := GetRetryCount(ctx)
	assert.True(t, ok)
	assert.Equal(t, 0, retryCount)

	maxRetry, ok := GetMaxRetry(ctx)
	assert.True(t, ok)
	assert.Equal(t, 0, maxRetry)

	createdAt, ok := GetCreatedAt(ctx)
	assert.True(t, ok)
	assert.True(t, createdAt.IsZero())
}
