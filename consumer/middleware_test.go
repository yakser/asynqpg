package consumer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
)

type contextKey string

func orderTrackingMiddleware(name string, order *[]string) MiddlewareFunc {
	return func(next TaskHandler) TaskHandler {
		return TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			*order = append(*order, name+"_before")
			err := next.Handle(ctx, task)
			*order = append(*order, name+"_after")
			return err
		})
	}
}

func TestBuildHandlerChain(t *testing.T) {
	t.Parallel()

	t.Run("no_middleware", func(t *testing.T) {
		t.Parallel()

		called := false
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			called = true
			return nil
		})

		got := buildHandlerChain(handler, nil, nil)

		err := got.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("global_only", func(t *testing.T) {
		t.Parallel()

		var order []string
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			order = append(order, "handler")
			return nil
		})

		got := buildHandlerChain(handler, []MiddlewareFunc{orderTrackingMiddleware("global", &order)}, nil)

		err := got.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)
		assert.Equal(t, []string{"global_before", "handler", "global_after"}, order)
	})

	t.Run("per_task_only", func(t *testing.T) {
		t.Parallel()

		var order []string
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			order = append(order, "handler")
			return nil
		})

		got := buildHandlerChain(handler, nil, []MiddlewareFunc{orderTrackingMiddleware("task", &order)})

		err := got.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)
		assert.Equal(t, []string{"task_before", "handler", "task_after"}, order)
	})

	t.Run("global_and_per_task_order", func(t *testing.T) {
		t.Parallel()

		var order []string
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			order = append(order, "handler")
			return nil
		})

		got := buildHandlerChain(
			handler,
			[]MiddlewareFunc{orderTrackingMiddleware("global", &order)},
			[]MiddlewareFunc{orderTrackingMiddleware("task", &order)},
		)

		err := got.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)

		want := []string{"global_before", "task_before", "handler", "task_after", "global_after"}
		assert.Equal(t, want, order)
	})

	t.Run("multiple_global_order", func(t *testing.T) {
		t.Parallel()

		var order []string
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			order = append(order, "handler")
			return nil
		})

		got := buildHandlerChain(
			handler,
			[]MiddlewareFunc{
				orderTrackingMiddleware("mw1", &order),
				orderTrackingMiddleware("mw2", &order),
				orderTrackingMiddleware("mw3", &order),
			},
			nil,
		)

		err := got.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)

		want := []string{"mw1_before", "mw2_before", "mw3_before", "handler", "mw3_after", "mw2_after", "mw1_after"}
		assert.Equal(t, want, order)
	})

	t.Run("context_modification", func(t *testing.T) {
		t.Parallel()

		key := contextKey("test-key")
		mw := func(next TaskHandler) TaskHandler {
			return TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				return next.Handle(context.WithValue(ctx, key, "injected"), task)
			})
		}

		var gotValue any
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			gotValue = ctx.Value(key)
			return nil
		})

		wrapped := buildHandlerChain(handler, []MiddlewareFunc{mw}, nil)

		err := wrapped.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)
		assert.Equal(t, "injected", gotValue)
	})

	t.Run("short_circuit", func(t *testing.T) {
		t.Parallel()

		handlerCalled := false
		shortCircuitErr := fmt.Errorf("short-circuited")

		mw := func(next TaskHandler) TaskHandler {
			return TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				return shortCircuitErr
			})
		}

		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			handlerCalled = true
			return nil
		})

		wrapped := buildHandlerChain(handler, []MiddlewareFunc{mw}, nil)

		err := wrapped.Handle(context.Background(), &asynqpg.TaskInfo{})
		assert.ErrorIs(t, err, shortCircuitErr)
		assert.False(t, handlerCalled)
	})

	t.Run("error_transformation", func(t *testing.T) {
		t.Parallel()

		handlerErr := fmt.Errorf("handler error")

		mw := func(next TaskHandler) TaskHandler {
			return TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
				err := next.Handle(ctx, task)
				if err != nil {
					return fmt.Errorf("wrapped: %w", err)
				}
				return nil
			})
		}

		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			return handlerErr
		})

		wrapped := buildHandlerChain(handler, []MiddlewareFunc{mw}, nil)

		err := wrapped.Handle(context.Background(), &asynqpg.TaskInfo{})
		assert.ErrorIs(t, err, handlerErr)
		assert.Contains(t, err.Error(), "wrapped:")
	})

	t.Run("nil_middleware_skipped", func(t *testing.T) {
		t.Parallel()

		called := false
		handler := TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
			called = true
			return nil
		})

		wrapped := buildHandlerChain(handler, []MiddlewareFunc{nil}, []MiddlewareFunc{nil})

		err := wrapped.Handle(context.Background(), &asynqpg.TaskInfo{})
		require.NoError(t, err)
		assert.True(t, called)
	})
}

func TestUse(t *testing.T) {
	t.Parallel()

	t.Run("before_start", func(t *testing.T) {
		t.Parallel()

		c := &Consumer{}
		mw := func(next TaskHandler) TaskHandler { return next }

		err := c.Use(mw)

		require.NoError(t, err)
		assert.Len(t, c.globalMiddleware, 1)
	})

	t.Run("after_start", func(t *testing.T) {
		t.Parallel()

		c := &Consumer{started: true}
		mw := func(next TaskHandler) TaskHandler { return next }

		err := c.Use(mw)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot add middleware after consumer is started")
	})

	t.Run("multiple_calls_append", func(t *testing.T) {
		t.Parallel()

		c := &Consumer{}
		mw1 := func(next TaskHandler) TaskHandler { return next }
		mw2 := func(next TaskHandler) TaskHandler { return next }

		err := c.Use(mw1)
		require.NoError(t, err)

		err = c.Use(mw2)
		require.NoError(t, err)

		assert.Len(t, c.globalMiddleware, 2)
	})

	t.Run("nil_middleware_filtered", func(t *testing.T) {
		t.Parallel()

		c := &Consumer{}
		mw := func(next TaskHandler) TaskHandler { return next }

		err := c.Use(nil, mw, nil)
		require.NoError(t, err)

		assert.Len(t, c.globalMiddleware, 1)
	})
}

func TestWithMiddleware(t *testing.T) {
	t.Parallel()

	t.Run("stored_in_options", func(t *testing.T) {
		t.Parallel()

		mw1 := func(next TaskHandler) TaskHandler { return next }
		mw2 := func(next TaskHandler) TaskHandler { return next }

		opts := &TaskTypeOptions{}
		WithMiddleware(mw1, mw2)(opts)

		assert.Len(t, opts.Middleware, 2)
	})
}
