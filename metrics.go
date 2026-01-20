package asynqpg

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const meterName = "asynqpg"

// Metrics holds all OpenTelemetry metric instruments for asynqpg.
// When no MeterProvider is configured, all instruments are noop (zero overhead).
type Metrics struct {
	TasksEnqueued   metric.Int64Counter
	TasksProcessed  metric.Int64Counter
	TasksErrors     metric.Int64Counter
	TaskDuration    metric.Float64Histogram
	EnqueueDuration metric.Float64Histogram
	TasksInFlight   metric.Int64UpDownCounter
}

// NewMetrics creates metric instruments from the given MeterProvider.
// If mp is nil, the global OTel MeterProvider is used.
func NewMetrics(mp metric.MeterProvider) (*Metrics, error) {
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(meterName)

	tasksEnqueued, err := meter.Int64Counter("asynqpg.tasks.enqueued",
		metric.WithDescription("Number of tasks enqueued"),
	)
	if err != nil {
		return nil, err
	}

	tasksProcessed, err := meter.Int64Counter("asynqpg.tasks.processed",
		metric.WithDescription("Number of tasks that finished processing"),
	)
	if err != nil {
		return nil, err
	}

	tasksErrors, err := meter.Int64Counter("asynqpg.tasks.errors",
		metric.WithDescription("Number of task processing or enqueue errors"),
	)
	if err != nil {
		return nil, err
	}

	taskDuration, err := meter.Float64Histogram("asynqpg.task.duration",
		metric.WithDescription("Task handler execution duration"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	enqueueDuration, err := meter.Float64Histogram("asynqpg.task.enqueue_duration",
		metric.WithDescription("Time to insert task into database"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	tasksInFlight, err := meter.Int64UpDownCounter("asynqpg.tasks.in_flight",
		metric.WithDescription("Number of tasks currently being executed by workers"),
	)
	if err != nil {
		return nil, err
	}

	m := &Metrics{
		TasksEnqueued:   tasksEnqueued,
		TasksProcessed:  tasksProcessed,
		TasksErrors:     tasksErrors,
		TaskDuration:    taskDuration,
		EnqueueDuration: enqueueDuration,
		TasksInFlight:   tasksInFlight,
	}
	m.init()
	return m, nil
}

// init records zero values for all counters so that they appear in Prometheus
// from the very first scrape. Without this, rate() returns nothing until the
// counter is first incremented, because there's no baseline data point.
func (m *Metrics) init() {
	ctx := context.Background()
	m.TasksEnqueued.Add(ctx, 0)
	m.TasksProcessed.Add(ctx, 0)
	m.TasksErrors.Add(ctx, 0)
	m.TasksInFlight.Add(ctx, 0)
}
