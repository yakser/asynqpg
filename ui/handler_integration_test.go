//go:build integration

package ui

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T, db *sqlx.DB) *handler {
	t.Helper()

	return &handler{
		pool:   db,
		repo:   newRepository(db),
		logger: slog.New(slog.NewTextHandler(testWriter{t}, nil)),
		opts:   HandlerOpts{Pool: db},
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// upsertLeader inserts/updates the asynqpg_leader row used by handleClusterLeader.
func upsertLeader(t *testing.T, db *sqlx.DB, leaderID string, electedAt, expiresAt time.Time) {
	t.Helper()

	_, err := db.ExecContext(context.Background(),
		`INSERT INTO asynqpg_leader (name, leader_id, elected_at, expires_at)
		 VALUES ('default', $1, $2, $3)
		 ON CONFLICT (name) DO UPDATE SET
			 leader_id = EXCLUDED.leader_id,
			 elected_at = EXCLUDED.elected_at,
			 expires_at = EXCLUDED.expires_at`,
		leaderID, electedAt, expiresAt,
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(),
			`DELETE FROM asynqpg_leader WHERE name = 'default'`)
	})
}

func TestHandleClusterLeader_Integration_NoLeader(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	h := newTestHandler(t, db)

	// Make sure no row exists in this isolated schema.
	_, err := db.ExecContext(context.Background(), `DELETE FROM asynqpg_leader WHERE name = 'default'`)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
	rec := httptest.NewRecorder()

	h.handleClusterLeader(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Nil(t, resp.Error)

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var leader leaderResponse
	require.NoError(t, json.Unmarshal(data, &leader))

	assert.Empty(t, leader.LeaderID)
	assert.Nil(t, leader.ElectedAt)
	assert.Nil(t, leader.ExpiresAt)
	assert.Equal(t, int64(0), leader.LeaseTTLSeconds)
}

func TestHandleClusterLeader_Integration_ActiveLeader(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	h := newTestHandler(t, db)

	electedAt := time.Now().Add(-10 * time.Second).UTC()
	expiresAt := time.Now().Add(45 * time.Second).UTC()
	upsertLeader(t, db, "consumer-active-1", electedAt, expiresAt)

	req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
	rec := httptest.NewRecorder()

	h.handleClusterLeader(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Nil(t, resp.Error)

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var leader leaderResponse
	require.NoError(t, json.Unmarshal(data, &leader))

	assert.Equal(t, "consumer-active-1", leader.LeaderID)
	require.NotNil(t, leader.ElectedAt)
	require.NotNil(t, leader.ExpiresAt)
	assert.WithinDuration(t, electedAt, *leader.ElectedAt, time.Second)
	assert.WithinDuration(t, expiresAt, *leader.ExpiresAt, time.Second)
	assert.Greater(t, leader.LeaseTTLSeconds, int64(0))
	assert.LessOrEqual(t, leader.LeaseTTLSeconds, int64(46))
}

func TestHandleClusterLeader_Integration_ExpiredLeaseClampsTTL(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	h := newTestHandler(t, db)

	electedAt := time.Now().Add(-2 * time.Minute).UTC()
	expiresAt := time.Now().Add(-30 * time.Second).UTC()
	upsertLeader(t, db, "consumer-stale", electedAt, expiresAt)

	req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
	rec := httptest.NewRecorder()

	h.handleClusterLeader(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var leader leaderResponse
	require.NoError(t, json.Unmarshal(data, &leader))

	assert.Equal(t, "consumer-stale", leader.LeaderID)
	assert.Equal(t, int64(0), leader.LeaseTTLSeconds, "TTL should clamp to zero for expired leases")
}

func TestHandleClusterLeader_Integration_RoutingViaNewHandler(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	upsertLeader(t, db,
		"consumer-routed",
		time.Now().Add(-5*time.Second).UTC(),
		time.Now().Add(20*time.Second).UTC(),
	)

	srv, err := NewHandler(HandlerOpts{Pool: db})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/cluster/leader", nil)
	rec := httptest.NewRecorder()

	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Nil(t, resp.Error)

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var leader leaderResponse
	require.NoError(t, json.Unmarshal(data, &leader))

	assert.Equal(t, "consumer-routed", leader.LeaderID)
}

// --- /api/tasks?idempotency_token={has,none} ---

func TestHandleListTasks_IdempotencyTokenFilter_Has(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	h := newTestHandler(t, db)

	insertPendingTaskWithToken(t, db, "idem-http-has", "tok-1")
	insertPendingTaskWithToken(t, db, "idem-http-has", "tok-2")
	insertPendingTask(t, db, "idem-http-has", []byte(`{}`))

	req := httptest.NewRequest(http.MethodGet,
		"/api/tasks?type=idem-http-has&idempotency_token=has&limit=100", nil)
	rec := httptest.NewRecorder()

	h.handleListTasks(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Nil(t, resp.Error)

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var page TaskListResult
	require.NoError(t, json.Unmarshal(data, &page))

	assert.Equal(t, 2, page.Total)
	require.Len(t, page.Tasks, 2)
	for _, task := range page.Tasks {
		require.NotNil(t, task.IdempotencyToken,
			"idempotency_token=has must only return tasks with a token")
	}
}

func TestHandleListTasks_IdempotencyTokenFilter_None(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	h := newTestHandler(t, db)

	insertPendingTaskWithToken(t, db, "idem-http-none", "tok-1")
	insertPendingTask(t, db, "idem-http-none", []byte(`{}`))
	insertPendingTask(t, db, "idem-http-none", []byte(`{}`))

	req := httptest.NewRequest(http.MethodGet,
		"/api/tasks?type=idem-http-none&idempotency_token=none&limit=100", nil)
	rec := httptest.NewRecorder()

	h.handleListTasks(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	data, err := json.Marshal(resp.Data)
	require.NoError(t, err)

	var page TaskListResult
	require.NoError(t, json.Unmarshal(data, &page))

	assert.Equal(t, 2, page.Total)
	require.Len(t, page.Tasks, 2)
	for _, task := range page.Tasks {
		assert.Nil(t, task.IdempotencyToken,
			"idempotency_token=none must only return tasks without a token")
	}
}

func TestHandleListTasks_IdempotencyTokenFilter_RejectsInvalidValue(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	h := newTestHandler(t, db)

	req := httptest.NewRequest(http.MethodGet,
		"/api/tasks?idempotency_token=bogus&limit=100", nil)
	rec := httptest.NewRecorder()

	h.handleListTasks(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.NotNil(t, resp.Error)
	assert.Contains(t, resp.Error.Message, "idempotency_token")
}
