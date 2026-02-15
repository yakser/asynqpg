package consumer

import (
	"context"

	"github.com/yakser/asynqpg"
)

// TaskHandlerFunc is an adapter to allow the use of ordinary functions as TaskHandler.
// Similar to http.HandlerFunc.
type TaskHandlerFunc func(ctx context.Context, task *asynqpg.TaskInfo) error

func (f TaskHandlerFunc) Handle(ctx context.Context, task *asynqpg.TaskInfo) error {
	return f(ctx, task)
}
