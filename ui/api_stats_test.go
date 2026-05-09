package ui

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/ui/mocks"
)

func TestHandleStats(t *testing.T) {
	t.Parallel()

	t.Run("aggregates correctly", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, dest any, _ string, _ ...any) {
				stats, ok := dest.(*[]TaskTypeStat)
				if !ok {
					return
				}
				*stats = []TaskTypeStat{
					{Type: testTaskTypeEmailSend, Status: statusPending, Count: 10},
					{Type: testTaskTypeEmailSend, Status: statusFailed, Count: 5},
					{Type: testTaskTypeEmailSend, Status: statusCompleted, Count: 100},
					{Type: testTaskTypeReportGen, Status: statusPending, Count: 3},
					{Type: testTaskTypeReportGen, Status: statusRunning, Count: 1},
				}
			}).
			Return(nil).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		rec := httptest.NewRecorder()

		h.handleStats(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Nil(t, resp.Error)

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var stats statsResponse
		require.NoError(t, json.Unmarshal(data, &stats))

		assert.Equal(t, int64(119), stats.Total)
		assert.Equal(t, int64(13), stats.ByStatus[statusPending])
		assert.Equal(t, int64(1), stats.ByStatus[statusRunning])
		assert.Equal(t, int64(100), stats.ByStatus[statusCompleted])
		assert.Equal(t, int64(5), stats.ByStatus[statusFailed])
		assert.Equal(t, int64(0), stats.ByStatus[statusCancelled])
		assert.Len(t, stats.ByType, 2)
	})

	t.Run("empty database", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
		rec := httptest.NewRecorder()

		h.handleStats(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var stats statsResponse
		require.NoError(t, json.Unmarshal(data, &stats))

		assert.Equal(t, int64(0), stats.Total)
		assert.Empty(t, stats.ByType)
	})
}

func TestHandleTaskTypes(t *testing.T) {
	t.Parallel()

	t.Run("returns types", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, dest any, _ string, _ ...any) {
				types, ok := dest.(*[]string)
				if !ok {
					return
				}
				*types = []string{testTaskTypeEmailSend, "notification.push", testTaskTypeReportGen}
			}).
			Return(nil).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/task-types", nil)
		rec := httptest.NewRecorder()

		h.handleTaskTypes(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, ok := resp.Data.([]any)
		require.True(t, ok)
		assert.Len(t, data, 3)
		assert.Equal(t, testTaskTypeEmailSend, data[0])
	})

	t.Run("empty returns empty array", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/task-types", nil)
		rec := httptest.NewRecorder()

		h.handleTaskTypes(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, ok := resp.Data.([]any)
		require.True(t, ok)
		assert.Empty(t, data)
	})
}
