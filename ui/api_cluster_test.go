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
