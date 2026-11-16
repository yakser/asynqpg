package asynqpg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewMetrics_CreatesAllInstruments(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	m, err := NewMetrics(mp)
	require.NoError(t, err)
	require.NotNil(t, m)

	assert.NotNil(t, m.TasksEnqueued)
	assert.NotNil(t, m.TasksProcessed)
	assert.NotNil(t, m.TasksErrors)
	assert.NotNil(t, m.TaskDuration)
	assert.NotNil(t, m.EnqueueDuration)
	assert.NotNil(t, m.TasksInFlight)
}

func TestNewMetrics_NilProvider_UsesGlobal(t *testing.T) {
	t.Parallel()

	m, err := NewMetrics(nil)
	require.NoError(t, err)
	require.NotNil(t, m)
}

func TestMetrics_RecordAndRead(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	m, err := NewMetrics(mp)
	require.NoError(t, err)

	ctx := context.Background()

	m.TasksEnqueued.Add(ctx, 5)
	m.TasksProcessed.Add(ctx, 3)
	m.TasksErrors.Add(ctx, 1)
	m.TaskDuration.Record(ctx, 1.5)
	m.EnqueueDuration.Record(ctx, 0.01)
	m.TasksInFlight.Add(ctx, 2)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	require.NotEmpty(t, rm.ScopeMetrics)

	metricNames := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			metricNames[metric.Name] = true
		}
	}

	assert.True(t, metricNames["asynqpg.tasks.enqueued"], "tasks.enqueued metric missing")
	assert.True(t, metricNames["asynqpg.tasks.processed"], "tasks.processed metric missing")
	assert.True(t, metricNames["asynqpg.tasks.errors"], "tasks.errors metric missing")
	assert.True(t, metricNames["asynqpg.task.duration"], "task.duration metric missing")
	assert.True(t, metricNames["asynqpg.task.enqueue_duration"], "task.enqueue_duration metric missing")
	assert.True(t, metricNames["asynqpg.tasks.in_flight"], "tasks.in_flight metric missing")
}

func TestMetrics_CounterValues(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	m, err := NewMetrics(mp)
	require.NoError(t, err)

	ctx := context.Background()

	m.TasksEnqueued.Add(ctx, 10)
	m.TasksEnqueued.Add(ctx, 5)

	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	require.NoError(t, err)

	for _, sm := range rm.ScopeMetrics {
		for _, metric := range sm.Metrics {
			if metric.Name == "asynqpg.tasks.enqueued" {
				sum, ok := metric.Data.(metricdata.Sum[int64])
				require.True(t, ok, "expected Sum[int64] for counter")
				require.Len(t, sum.DataPoints, 1)
				assert.Equal(t, int64(15), sum.DataPoints[0].Value)
				return
			}
		}
	}
	t.Fatal("asynqpg.tasks.enqueued metric not found")
}

func TestMetrics_Notifications(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	m, err := NewMetrics(mp)
	require.NoError(t, err)
	ctx := context.Background()
	m.NotificationConnections.Add(ctx, 1)
	m.NotificationsReceived.Add(ctx, 3)
	m.NotificationsIgnored.Add(ctx, 1)
	m.NotificationReconnects.Add(ctx, 1)
	m.ConsumerFetches.Add(ctx, 2)
	m.ConsumerEmptyFetches.Add(ctx, 1)
	m.NotificationConnections.Add(ctx, -1)
	var got metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &got))
	want := map[string]int64{"asynqpg.notifications.connected": 0, "asynqpg.notifications.received": 3, "asynqpg.notifications.ignored": 1, "asynqpg.notifications.reconnects": 1, "asynqpg.consumer.fetches": 2, "asynqpg.consumer.empty_fetches": 1}
	for _, scope := range got.ScopeMetrics {
		for _, m := range scope.Metrics {
			if value, ok := want[m.Name]; ok {
				sum := m.Data.(metricdata.Sum[int64])
				require.Len(t, sum.DataPoints, 1)
				assert.Equal(t, value, sum.DataPoints[0].Value)
				delete(want, m.Name)
			}
		}
	}
	assert.Empty(t, want)
}
