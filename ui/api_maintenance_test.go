package ui

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/ui/mocks"
)

// fillMaintenancePreview populates the destination of a GetContext call based on
// its concrete type, simulating the three preview queries.
func fillMaintenancePreview(_ context.Context, dest any, _ string, _ ...any) {
	switch d := dest.(type) {
	case *RescuePreview:
		*d = RescuePreview{Total: 5, Retriable: 3, Discardable: 2}
	case *CleanupPreview:
		*d = CleanupPreview{Completed: 4, Failed: 1, Cancelled: 2}
	case *QueueHealth:
		ms := 12000.0
		*d = QueueHealth{OldestPendingMs: &ms, CompletedLast15m: 7, FailedLast15m: 1}
	}
}

// expectPreviewQueries registers GetContext expectations for all three preview
// queries, keyed by argument arity (queue health: 0 args, rescue: 1, cleanup: 3).
func expectPreviewQueries(p *mocks.Pool, run func(context.Context, any, string, ...any)) {
	rescue := p.EXPECT().GetContext(mock.Anything, mock.AnythingOfType("*ui.RescuePreview"), mock.Anything, mock.Anything).Run(run).Return(nil).Once()
	cleanup := p.EXPECT().GetContext(mock.Anything, mock.AnythingOfType("*ui.CleanupPreview"), mock.Anything, mock.Anything, mock.Anything, mock.Anything).Run(run).Return(nil).Once()
	health := p.EXPECT().GetContext(mock.Anything, mock.AnythingOfType("*ui.QueueHealth"), mock.Anything).Run(run).Return(nil).Once()
	mock.InOrder(rescue, cleanup, health)
}

func decodeMaintenance(t *testing.T, rec *httptest.ResponseRecorder) maintenanceResponse {
	t.Helper()

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Nil(t, resp.Error)

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var got maintenanceResponse
	require.NoError(t, json.Unmarshal(data, &got))
	return got
}

func TestHandleClusterMaintenance(t *testing.T) {
	t.Parallel()

	t.Run("returns preview with configured thresholds", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		expectPreviewQueries(p, fillMaintenancePreview)

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts: HandlerOpts{
				Pool: p,
				Maintenance: &MaintenanceConfig{
					StuckThreshold:     2 * time.Minute,
					CompletedRetention: time.Hour,
					FailedRetention:    2 * time.Hour,
					CancelledRetention: time.Hour,
					Interval:           15 * time.Second,
				},
			},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/maintenance", nil)
		rec := httptest.NewRecorder()

		h.handleClusterMaintenance(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		got := decodeMaintenance(t, rec)

		assert.Equal(t, "configured", got.Config.Source)
		assert.Equal(t, int64(120000), got.Config.StuckThresholdMs)
		assert.Equal(t, int64(3600000), got.Config.CompletedRetentionMs)
		assert.Equal(t, int64(7200000), got.Config.FailedRetentionMs)
		assert.Equal(t, int64(3600000), got.Config.CancelledRetentionMs)
		assert.Equal(t, int64(15000), got.Config.IntervalMs)

		assert.Equal(t, int64(5), got.Rescuable.Total)
		assert.Equal(t, int64(3), got.Rescuable.Retriable)
		assert.Equal(t, int64(2), got.Rescuable.Discardable)

		assert.Equal(t, int64(7), got.Cleanable.Total) // 4 + 1 + 2
		assert.Equal(t, int64(4), got.Cleanable.Completed)
		assert.Equal(t, int64(1), got.Cleanable.Failed)
		assert.Equal(t, int64(2), got.Cleanable.Cancelled)

		require.NotNil(t, got.QueueHealth.OldestPendingMs)
		assert.Equal(t, int64(12000), *got.QueueHealth.OldestPendingMs)
		assert.Equal(t, int64(7), got.QueueHealth.CompletedLast15m)
		assert.Equal(t, int64(1), got.QueueHealth.FailedLast15m)
	})

	t.Run("applies defaults when maintenance config is nil", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		// No Run: destinations keep their zero values.
		expectPreviewQueries(p, func(context.Context, any, string, ...any) {})

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/maintenance", nil)
		rec := httptest.NewRecorder()

		h.handleClusterMaintenance(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		got := decodeMaintenance(t, rec)

		assert.Equal(t, "defaults", got.Config.Source)
		assert.Equal(t, int64(300000), got.Config.StuckThresholdMs)       // 5m
		assert.Equal(t, int64(86400000), got.Config.CompletedRetentionMs) // 24h
		assert.Equal(t, int64(604800000), got.Config.FailedRetentionMs)   // 7d
		assert.Equal(t, int64(86400000), got.Config.CancelledRetentionMs) // 24h
		assert.Equal(t, int64(30000), got.Config.IntervalMs)              // 30s

		assert.Equal(t, int64(0), got.Rescuable.Total)
		assert.Equal(t, int64(0), got.Cleanable.Total)
		assert.Nil(t, got.QueueHealth.OldestPendingMs)
	})

	t.Run("partial configuration uses defaults for missing fields", func(t *testing.T) {
		t.Parallel()
		p := mocks.NewPool(t)
		expectPreviewQueries(p, fillMaintenancePreview)
		h := &handler{repo: newRepository(p), logger: slog.Default(), opts: HandlerOpts{Maintenance: &MaintenanceConfig{StuckThreshold: 2 * time.Minute}}}
		rec := httptest.NewRecorder()
		h.handleClusterMaintenance(rec, httptest.NewRequest(http.MethodGet, "/api/cluster/maintenance", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		got := decodeMaintenance(t, rec)
		assert.Equal(t, "configured", got.Config.Source)
		assert.Equal(t, int64(120000), got.Config.StuckThresholdMs)
		assert.Equal(t, int64(86400000), got.Config.CompletedRetentionMs)
		assert.Equal(t, int64(604800000), got.Config.FailedRetentionMs)
		assert.Equal(t, int64(86400000), got.Config.CancelledRetentionMs)
		assert.Equal(t, int64(30000), got.Config.IntervalMs)
	})
	for failure, name := range []string{"rescue", "cleanup", "health"} {
		t.Run(name+" error", func(t *testing.T) {
			t.Parallel()
			p := mocks.NewPool(t)
			destinations := []string{"*ui.RescuePreview", "*ui.CleanupPreview", "*ui.QueueHealth"}
			arities := []int{1, 3, 0}
			calls := make([]*mock.Call, 0, failure+1)
			for i := 0; i <= failure; i++ {
				args := []any{mock.Anything, mock.AnythingOfType(destinations[i]), mock.Anything}
				for range arities[i] {
					args = append(args, mock.Anything)
				}
				var err error
				if i == failure {
					err = errors.New("boom")
				}
				calls = append(calls, p.On("GetContext", args...).Return(err).Once())
			}
			mock.InOrder(calls...)
			h := &handler{repo: newRepository(p), logger: slog.New(slog.DiscardHandler)}
			rec := httptest.NewRecorder()
			h.handleClusterMaintenance(rec, httptest.NewRequest(http.MethodGet, "/api/cluster/maintenance", nil))
			assert.Equal(t, http.StatusInternalServerError, rec.Code)
			var response apiResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
			assert.NotNil(t, response.Error)
			assert.Nil(t, response.Data)
		})
	}
}
