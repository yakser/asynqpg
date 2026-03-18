package asynqpg_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yakser/asynqpg"
)

func ExampleNewTask() {
	task := asynqpg.NewTask("email:send", []byte(`{"to":"user@example.com"}`),
		asynqpg.WithMaxRetry(5),
		asynqpg.WithDelay(10*time.Second),
		asynqpg.WithIdempotencyToken("unique-token"),
	)

	fmt.Println(task.Type)
	// Output: email:send
}

func ExampleNewTask_processAt() {
	task := asynqpg.NewTask("report:generate", []byte(`{"id":1}`))
	task.ProcessAt = time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

	fmt.Println(task.ProcessAt.Format(time.RFC3339))
	// Output: 2026-01-01T09:00:00Z
}

func ExampleWithTaskMetadata() {
	ctx := asynqpg.WithTaskMetadata(context.Background(), asynqpg.TaskMetadata{
		ID:         42,
		RetryCount: 0,
		MaxRetry:   3,
		CreatedAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	meta, ok := asynqpg.GetTaskMetadata(ctx)
	fmt.Println(ok, meta.ID)
	// Output: true 42
}

func ExampleGetTaskID() {
	ctx := asynqpg.WithTaskMetadata(context.Background(), asynqpg.TaskMetadata{
		ID: 99,
	})

	id, ok := asynqpg.GetTaskID(ctx)
	fmt.Println(ok, id)
	// Output: true 99
}

func ExampleDefaultRetryPolicy() {
	policy := &asynqpg.DefaultRetryPolicy{MaxRetryDelay: 24 * time.Hour}

	delay := policy.NextRetry(1)
	fmt.Println(delay > 0)
	// Output: true
}

func ExampleConstantRetryPolicy() {
	policy := &asynqpg.ConstantRetryPolicy{Delay: 5 * time.Second}

	delay := policy.NextRetry(1)
	fmt.Println(delay)
	// Output: 5s
}

func ExampleErrSkipRetry() {
	err := fmt.Errorf("bad payload: %w", asynqpg.ErrSkipRetry)

	fmt.Println(errors.Is(err, asynqpg.ErrSkipRetry))
	// Output: true
}

func ExampleTaskSnooze() {
	err := asynqpg.TaskSnooze(30 * time.Second)

	var snoozeErr *asynqpg.TaskSnoozeError
	fmt.Println(errors.As(err, &snoozeErr))
	fmt.Println(snoozeErr.Duration)
	// Output:
	// true
	// 30s
}

func ExampleTaskSnoozeWithError() {
	err := fmt.Errorf("api unavailable: %w", asynqpg.TaskSnoozeWithError(1*time.Minute))

	var snoozeErr *asynqpg.TaskSnoozeWithErrError
	fmt.Println(errors.As(err, &snoozeErr))
	fmt.Println(snoozeErr.Duration)
	// Output:
	// true
	// 1m0s
}
