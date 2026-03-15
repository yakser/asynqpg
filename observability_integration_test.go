//go:build integration

package asynqpg_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
	"github.com/yakser/asynqpg/consumer"
	"github.com/yakser/asynqpg/internal/lib/ptr"
	"github.com/yakser/asynqpg/internal/lib/testutils"
	"github.com/yakser/asynqpg/producer"
)

func TestObservability_EnqueueMetrics(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	p, err := producer.New(producer.Config{
		Pool:          db,
		MeterProvider: mp,
	})
	require.NoError(t, err)

	ctx := context.Background()

	_, err = p.Enqueue(ctx, &asynqpg.Task{
		Type:             "email",
		Payload:          []byte(`{"to":"user@example.com"}`),
		IdempotencyToken: ptr.Get("obs-enqueue-1"),
	})
	require.NoError(t, err)

	_, err = p.Enqueue(ctx, &asynqpg.Task{
		Type:             "email",
		Payload:          []byte(`{"to":"user2@example.com"}`),
		IdempotencyToken: ptr.Get("obs-enqueue-2"),
	})
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	enqueuedCount := findCounterValue(t, rm, "asynqpg.tasks.enqueued")
	assert.Equal(t, int64(2), enqueuedCount, "should have 2 enqueued tasks")

	enqueueDuration := findHistogramCount(t, rm, "asynqpg.task.enqueue_duration")
	assert.Equal(t, uint64(2), enqueueDuration, "should have 2 enqueue duration records")
}

func TestObservability_EnqueueManyMetrics(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	p, err := producer.New(producer.Config{
		Pool:          db,
		MeterProvider: mp,
	})
	require.NoError(t, err)

	ctx := context.Background()

	tasks := []*asynqpg.Task{
		{Type: "email", Payload: []byte(`{}`), IdempotencyToken: ptr.Get("obs-many-1")},
		{Type: "email", Payload: []byte(`{}`), IdempotencyToken: ptr.Get("obs-many-2")},
		{Type: "sms", Payload: []byte(`{}`), IdempotencyToken: ptr.Get("obs-many-3")},
	}
	_, err = p.EnqueueMany(ctx, tasks)
	require.NoError(t, err)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	enqueuedCount := findCounterValue(t, rm, "asynqpg.tasks.enqueued")
	assert.Equal(t, int64(3), enqueuedCount, "should have 3 enqueued tasks total")
}

func TestObservability_EnqueueTracing(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	p, err := producer.New(producer.Config{
		Pool:           db,
		TracerProvider: tp,
	})
	require.NoError(t, err)

	_, err = p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             "email",
		Payload:          []byte(`{}`),
		IdempotencyToken: ptr.Get("obs-trace-enq-1"),
	})
	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)

	span := spans[0]
	assert.Equal(t, "asynqpg.enqueue", span.Name())
	assertSpanHasAttribute(t, span, "task_type", "email")
}

func TestObservability_EnqueueManyTracing(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	p, err := producer.New(producer.Config{
		Pool:           db,
		TracerProvider: tp,
	})
	require.NoError(t, err)

	tasks := []*asynqpg.Task{
		{Type: "email", Payload: []byte(`{}`), IdempotencyToken: ptr.Get("obs-trace-many-1")},
		{Type: "sms", Payload: []byte(`{}`), IdempotencyToken: ptr.Get("obs-trace-many-2")},
	}
	_, err = p.EnqueueMany(context.Background(), tasks)
	require.NoError(t, err)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, "asynqpg.enqueue_many", spans[0].Name())
}

func TestObservability_ProcessMetricsAndTracing(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	// Enqueue a task
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)

	_, err = p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             "obs_test_task",
		Payload:          []byte(`{"data":"test"}`),
		IdempotencyToken: ptr.Get("obs-process-1"),
	})
	require.NoError(t, err)

	// Process with instrumented consumer
	processed := make(chan struct{})
	c, err := consumer.New(consumer.Config{
		Pool:               db,
		MeterProvider:      mp,
		TracerProvider:     tp,
		DisableMaintenance: true,
		FetchInterval:      100 * time.Millisecond,
	})
	require.NoError(t, err)

	err = c.RegisterTaskHandler("obs_test_task", consumer.TaskHandlerFunc(func(ctx context.Context, task *asynqpg.TaskInfo) error {
		close(processed)
		return nil
	}))
	require.NoError(t, err)

	require.NoError(t, c.Start())
	defer c.Stop()

	select {
	case <-processed:
	case <-time.After(10 * time.Second):
		t.Fatal("task not processed within timeout")
	}

	// Give metrics time to be recorded
	time.Sleep(100 * time.Millisecond)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(context.Background(), &rm)
	require.NoError(t, err)

	processedCount := findCounterValue(t, rm, "asynqpg.tasks.processed")
	assert.Equal(t, int64(1), processedCount)

	durationCount := findHistogramCount(t, rm, "asynqpg.task.duration")
	assert.Equal(t, uint64(1), durationCount)

	// Check tracing
	spans := sr.Ended()
	var processSpan sdktrace.ReadOnlySpan
	for i := range spans {
		if spans[i].Name() == "asynqpg.process" {
			processSpan = spans[i]
			break
		}
	}
	require.NotNil(t, processSpan, "should have asynqpg.process span")
	assertSpanHasAttribute(t, processSpan, "task_type", "obs_test_task")
}

func TestObservability_ClientTracing(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	// Enqueue a task first
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)

	_, err = p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             "client_trace_task",
		Payload:          []byte(`{}`),
		IdempotencyToken: ptr.Get("obs-client-trace-1"),
	})
	require.NoError(t, err)

	// Create instrumented client
	cl, err := client.New(client.Config{
		Pool:           db,
		TracerProvider: tp,
	})
	require.NoError(t, err)

	// List tasks
	_, err = cl.ListTasks(context.Background(), nil)
	require.NoError(t, err)

	spans := sr.Ended()
	var listSpan sdktrace.ReadOnlySpan
	for i := range spans {
		if spans[i].Name() == "asynqpg.list_tasks" {
			listSpan = spans[i]
			break
		}
	}
	require.NotNil(t, listSpan, "should have asynqpg.list_tasks span")
}

func TestObservability_NoopWhenNoProvider(t *testing.T) {
	db := testutils.SetupTestDatabase(t)

	// Create producer without any providers - should work with noop
	p, err := producer.New(producer.Config{Pool: db})
	require.NoError(t, err)

	_, err = p.Enqueue(context.Background(), &asynqpg.Task{
		Type:             "noop_test",
		Payload:          []byte(`{}`),
		IdempotencyToken: ptr.Get("obs-noop-1"),
	})
	require.NoError(t, err)
}

// --- Helpers ---

func findCounterValue(t *testing.T, rm metricdata.ResourceMetrics, name string) int64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
					var total int64
					for _, dp := range sum.DataPoints {
						total += dp.Value
					}
					return total
				}
			}
		}
	}
	return 0
}

func findHistogramCount(t *testing.T, rm metricdata.ResourceMetrics, name string) uint64 {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok {
					var total uint64
					for _, dp := range hist.DataPoints {
						total += dp.Count
					}
					return total
				}
			}
		}
	}
	return 0
}

func assertSpanHasAttribute(t *testing.T, span sdktrace.ReadOnlySpan, key, value string) {
	t.Helper()
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			assert.Equal(t, value, attr.Value.Emit())
			return
		}
	}
	t.Errorf("span %q missing attribute %q=%q, has: %v", span.Name(), key, value, span.Attributes())
}
