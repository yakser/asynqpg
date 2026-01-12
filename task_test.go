package asynqpg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTask_BasicConstruction(t *testing.T) {
	t.Parallel()

	wantType := "email:send"
	wantPayload := []byte(`{"to":"user@example.com"}`)

	got := NewTask(wantType, wantPayload)

	assert.Equal(t, wantType, got.Type)
	assert.Equal(t, wantPayload, got.Payload)
}

func TestNewTask_NoOptions(t *testing.T) {
	t.Parallel()

	got := NewTask("task:noop", nil)

	assert.Nil(t, got.MaxRetry)
	assert.Nil(t, got.IdempotencyToken)
	assert.Equal(t, time.Duration(0), got.Delay)
}

func TestNewTask_WithMaxRetry(t *testing.T) {
	t.Parallel()

	want := 5

	got := NewTask("task:retry", nil, WithMaxRetry(want))

	require.NotNil(t, got.MaxRetry)
	assert.Equal(t, want, *got.MaxRetry)
}

func TestNewTask_WithMaxRetryZero(t *testing.T) {
	t.Parallel()

	want := 0

	got := NewTask("task:no-retry", nil, WithMaxRetry(want))

	require.NotNil(t, got.MaxRetry)
	assert.Equal(t, want, *got.MaxRetry)
}

func TestNewTask_WithDelay(t *testing.T) {
	t.Parallel()

	want := 30 * time.Second

	got := NewTask("task:delayed", nil, WithDelay(want))

	assert.Equal(t, want, got.Delay)
}

func TestNewTask_WithIdempotencyToken(t *testing.T) {
	t.Parallel()

	want := "unique-token-123"

	got := NewTask("task:idempotent", nil, WithIdempotencyToken(want))

	require.NotNil(t, got.IdempotencyToken)
	assert.Equal(t, want, *got.IdempotencyToken)
}

func TestNewTask_MultipleOptions(t *testing.T) {
	t.Parallel()

	wantType := "email:send"
	wantPayload := []byte(`{"to":"user@example.com"}`)
	wantMaxRetry := 3
	wantDelay := 10 * time.Second
	wantToken := "token-abc"

	got := NewTask(
		wantType,
		wantPayload,
		WithMaxRetry(wantMaxRetry),
		WithDelay(wantDelay),
		WithIdempotencyToken(wantToken),
	)

	assert.Equal(t, wantType, got.Type)
	assert.Equal(t, wantPayload, got.Payload)
	require.NotNil(t, got.MaxRetry)
	assert.Equal(t, wantMaxRetry, *got.MaxRetry)
	assert.Equal(t, wantDelay, got.Delay)
	require.NotNil(t, got.IdempotencyToken)
	assert.Equal(t, wantToken, *got.IdempotencyToken)
}

func TestNewTask_OptionOrderIndependence(t *testing.T) {
	t.Parallel()

	taskType := "task:order"
	payload := []byte(`{"key":"value"}`)
	maxRetry := 2
	delay := 5 * time.Minute
	token := "order-token"

	gotA := NewTask(
		taskType, payload,
		WithMaxRetry(maxRetry),
		WithDelay(delay),
		WithIdempotencyToken(token),
	)

	gotB := NewTask(
		taskType, payload,
		WithIdempotencyToken(token),
		WithMaxRetry(maxRetry),
		WithDelay(delay),
	)

	gotC := NewTask(
		taskType, payload,
		WithDelay(delay),
		WithIdempotencyToken(token),
		WithMaxRetry(maxRetry),
	)

	assert.Equal(t, gotA.Type, gotB.Type)
	assert.Equal(t, gotA.Type, gotC.Type)
	assert.Equal(t, gotA.Payload, gotB.Payload)
	assert.Equal(t, gotA.Payload, gotC.Payload)
	assert.Equal(t, gotA.Delay, gotB.Delay)
	assert.Equal(t, gotA.Delay, gotC.Delay)
	require.NotNil(t, gotA.MaxRetry)
	require.NotNil(t, gotB.MaxRetry)
	require.NotNil(t, gotC.MaxRetry)
	assert.Equal(t, *gotA.MaxRetry, *gotB.MaxRetry)
	assert.Equal(t, *gotA.MaxRetry, *gotC.MaxRetry)
	require.NotNil(t, gotA.IdempotencyToken)
	require.NotNil(t, gotB.IdempotencyToken)
	require.NotNil(t, gotC.IdempotencyToken)
	assert.Equal(t, *gotA.IdempotencyToken, *gotB.IdempotencyToken)
	assert.Equal(t, *gotA.IdempotencyToken, *gotC.IdempotencyToken)
}
