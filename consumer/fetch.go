package consumer

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/fetcher"
	"github.com/yakser/asynqpg/internal/repository"
)

type fetchRunner interface {
	Start(context.Context) error
	Stop()
}

func (c *Consumer) createFetcher(taskType string) (fetchRunner, error) {
	handler := buildHandlerChain(c.handlers[taskType], c.globalMiddleware, c.taskOptions[taskType].Middleware)
	wp := c.wp[taskType]
	options := c.taskOptions[taskType]
	fetch := func(ctx context.Context, reason fetcher.Reason) (fetcher.Result, error) {
		free := wp.FreeWorkers()
		if free == 0 {
			return fetcher.Result{}, nil
		}
		attrs := metric.WithAttributes(asynqpg.AttrTaskType.String(taskType), attribute.String("reason", string(reason)))
		c.metrics.ConsumerFetches.Add(ctx, 1, attrs)
		tasks, err := c.repo.GetReadyTasks(ctx, repository.GetReadyTasksParams{Type: taskType, Limit: free, Delay: options.Timeout + lockMargin})
		if err != nil {
			return fetcher.Result{}, fmt.Errorf("get ready tasks: %w", err)
		}
		if len(tasks) == 0 {
			c.metrics.ConsumerEmptyFetches.Add(ctx, 1, attrs)
		}
		for _, task := range tasks {
			if err := wp.Submit(func() { c.processTask(handler, &task, options.Timeout) }); err != nil {
				return fetcher.Result{}, fmt.Errorf("submit task: %w", err)
			}
		}
		return fetcher.Result{Fetched: len(tasks), Limit: free}, nil
	}
	logger := c.logger.With("component", "fetcher", "task_type", taskType)
	runner, err := fetcher.New(fetcher.Config{PollInterval: c.fetchInterval, Cooldown: c.fetchCooldown, Fetch: fetch, CapacityAvailable: wp.CapacityAvailable(), Logger: logger})
	if err != nil {
		return nil, err
	}
	c.notifyFetchers[taskType] = runner
	return runner, nil
}
