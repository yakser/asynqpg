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

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/ui/mocks"
)

func TestHandleClusterLeader(t *testing.T) {
	t.Parallel()

	t.Run("returns leader info", func(t *testing.T) {
		t.Parallel()

		electedAt := time.Now().Add(-30 * time.Second).UTC()
		expiresAt := time.Now().Add(30 * time.Second).UTC()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, dest any, _ string, _ ...any) {
				rows, ok := dest.(*[]LeaderRow)
				if !ok {
					return
				}
				*rows = []LeaderRow{
					{
						Name:      "default",
						LeaderID:  "consumer-prod-3",
						ElectedAt: electedAt,
						ExpiresAt: expiresAt,
					},
				}
			}).
			Return(nil).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
		rec := httptest.NewRecorder()

		h.handleClusterLeader(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Nil(t, resp.Error)

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var leader leaderResponse
		require.NoError(t, json.Unmarshal(data, &leader))

		assert.Equal(t, "consumer-prod-3", leader.LeaderID)
		require.NotNil(t, leader.ElectedAt)
		require.NotNil(t, leader.ExpiresAt)
		assert.GreaterOrEqual(t, leader.LeaseTTLSeconds, int64(0))
		assert.LessOrEqual(t, leader.LeaseTTLSeconds, int64(31))
	})

	t.Run("no leader returns empty response", func(t *testing.T) {
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

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
		rec := httptest.NewRecorder()

		h.handleClusterLeader(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var leader leaderResponse
		require.NoError(t, json.Unmarshal(data, &leader))

		assert.Empty(t, leader.LeaderID)
		assert.Nil(t, leader.ElectedAt)
		assert.Nil(t, leader.ExpiresAt)
		assert.Equal(t, int64(0), leader.LeaseTTLSeconds)
	})

	t.Run("expired lease clamps ttl to zero", func(t *testing.T) {
		t.Parallel()

		electedAt := time.Now().Add(-2 * time.Minute).UTC()
		expiresAt := time.Now().Add(-30 * time.Second).UTC()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, dest any, _ string, _ ...any) {
				rows, ok := dest.(*[]LeaderRow)
				if !ok {
					return
				}
				*rows = []LeaderRow{
					{Name: "default", LeaderID: "stale", ElectedAt: electedAt, ExpiresAt: expiresAt},
				}
			}).
			Return(nil).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.Default(),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
		rec := httptest.NewRecorder()

		h.handleClusterLeader(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var leader leaderResponse
		require.NoError(t, json.Unmarshal(data, &leader))

		assert.Equal(t, "stale", leader.LeaderID)
		assert.Equal(t, int64(0), leader.LeaseTTLSeconds)
	})

	t.Run("repository error", func(t *testing.T) {
		t.Parallel()

		p := mocks.NewPool(t)
		p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything).
			Return(errors.New("boom")).Maybe()

		h := &handler{
			pool:   p,
			repo:   newRepository(p),
			logger: slog.New(slog.DiscardHandler),
			opts:   HandlerOpts{Pool: p},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
		rec := httptest.NewRecorder()

		h.handleClusterLeader(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestHandleClusterConsumers(t *testing.T) {
	t.Parallel()

	t.Run("returns consumer snapshots", func(t *testing.T) {
		t.Parallel()

		startedAt := time.Now().Add(-5 * time.Minute).UTC()
		h := &handler{
			logger: slog.Default(),
			opts: HandlerOpts{
				ConsumerInfoFunc: func() []asynqpg.ConsumerInfo {
					return []asynqpg.ConsumerInfo{
						{
							ID:                 "consumer-1",
							Running:            true,
							IsLeader:           true,
							MaintenanceEnabled: true,
							StartedAt:          startedAt,
							FetchInterval:      200 * time.Millisecond,
							JanitorInterval:    30 * time.Second,
							StuckThreshold:     2 * time.Minute,
							ShutdownTimeout:    30 * time.Second,
							TaskTypes: []asynqpg.ConsumerTaskTypeInfo{
								{Type: "email.send", Workers: 5, Busy: 2, MaxAttempts: 3, Timeout: 30 * time.Second},
							},
						},
					}
				},
			},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/consumers", nil)
		rec := httptest.NewRecorder()

		h.handleClusterConsumers(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Nil(t, resp.Error)

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var got []consumerResponse
		require.NoError(t, json.Unmarshal(data, &got))

		require.Len(t, got, 1)
		c := got[0]
		assert.Equal(t, "consumer-1", c.ID)
		assert.True(t, c.Running)
		assert.True(t, c.IsLeader)
		assert.True(t, c.MaintenanceEnabled)
		require.NotNil(t, c.StartedAt)
		assert.Equal(t, int64(200), c.FetchIntervalMs)
		assert.Equal(t, int64(120000), c.StuckThresholdMs)
		require.Len(t, c.TaskTypes, 1)
		assert.Equal(t, "email.send", c.TaskTypes[0].Type)
		assert.Equal(t, 5, c.TaskTypes[0].Workers)
		assert.Equal(t, 2, c.TaskTypes[0].Busy)
		assert.Equal(t, 3, c.TaskTypes[0].MaxAttempts)
		assert.Equal(t, int64(30000), c.TaskTypes[0].TimeoutMs)
	})

	t.Run("nil provider returns empty list", func(t *testing.T) {
		t.Parallel()

		h := &handler{
			logger: slog.Default(),
			opts:   HandlerOpts{},
		}

		req := httptest.NewRequest(http.MethodGet, "/api/cluster/consumers", nil)
		rec := httptest.NewRecorder()

		h.handleClusterConsumers(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		require.Nil(t, resp.Error)

		data, err := json.Marshal(resp.Data)
		require.NoError(t, err)

		var got []consumerResponse
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Empty(t, got)
	})
}

func TestHandleClusterConsumers_Stopped(t *testing.T) {
	t.Parallel()
	started := time.Now().UTC()
	h := &handler{opts: HandlerOpts{ConsumerInfoFunc: func() []asynqpg.ConsumerInfo {
		return []asynqpg.ConsumerInfo{{ID: "stopped", StartedAt: started, Running: false}}
	}}}
	rec := httptest.NewRecorder()
	h.handleClusterConsumers(rec, httptest.NewRequest(http.MethodGet, "/api/cluster/consumers", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var got struct {
		Data []consumerResponse `json:"data"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	require.Len(t, got.Data, 1)
	assert.False(t, got.Data[0].Running)
	require.NotNil(t, got.Data[0].StartedAt)
	assert.Equal(t, started, *got.Data[0].StartedAt)
}
