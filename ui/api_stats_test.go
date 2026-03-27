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
			Run(func(_ context.Context, dest interface{}, _ string, _ ...interface{}) {
				stats, ok := dest.(*[]TaskTypeStat)
				if !ok {
					return
				}
				*stats = []TaskTypeStat{
					{Type: "email.send", Status: "pending", Count: 10},
					{Type: "email.send", Status: "failed", Count: 5},
					{Type: "email.send", Status: "completed", Count: 100},
					{Type: "report.gen", Status: "pending", Count: 3},
					{Type: "report.gen", Status: "running", Count: 1},
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
		assert.Equal(t, int64(13), stats.ByStatus["pending"])
		assert.Equal(t, int64(1), stats.ByStatus["running"])
		assert.Equal(t, int64(100), stats.ByStatus["completed"])
		assert.Equal(t, int64(5), stats.ByStatus["failed"])
		assert.Equal(t, int64(0), stats.ByStatus["cancelled"])
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
			Run(func(_ context.Context, dest interface{}, _ string, _ ...interface{}) {
				types, ok := dest.(*[]string)
				if !ok {
					return
				}
				*types = []string{"email.send", "notification.push", "report.gen"}
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
		assert.Equal(t, "email.send", data[0])
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
