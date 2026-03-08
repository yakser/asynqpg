package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/client"
)

func TestParseListParams(t *testing.T) {
	t.Parallel()

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks", nil)

		params, err := parseListParams(r)

		require.NoError(t, err)
		assert.Equal(t, 100, params.Limit)
		assert.Equal(t, 0, params.Offset)
		assert.Equal(t, "id", params.OrderBy)
		assert.Equal(t, "ASC", params.OrderDir)
		assert.Empty(t, params.Statuses)
		assert.Empty(t, params.Types)
		assert.Empty(t, params.IDs)
	})

	t.Run("all params", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?status=failed,pending&type=email.send&limit=50&offset=10&order_by=created_at&order=desc&id=1,2,3", nil)

		params, err := parseListParams(r)

		require.NoError(t, err)
		assert.Equal(t, []string{"failed", "pending"}, params.Statuses)
		assert.Equal(t, []string{"email.send"}, params.Types)
		assert.Equal(t, []int64{1, 2, 3}, params.IDs)
		assert.Equal(t, 50, params.Limit)
		assert.Equal(t, 10, params.Offset)
		assert.Equal(t, "created_at", params.OrderBy)
		assert.Equal(t, "DESC", params.OrderDir)
	})

	t.Run("invalid status", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?status=invalid", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid status")
	})

	t.Run("invalid limit", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?limit=abc", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "limit must be a positive integer")
	})

	t.Run("limit zero", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?limit=0", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
	})

	t.Run("limit clamped to max", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?limit=99999", nil)

		params, err := parseListParams(r)

		require.NoError(t, err)
		assert.Equal(t, 10000, params.Limit)
	})

	t.Run("negative offset", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?offset=-1", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "offset must be a non-negative integer")
	})

	t.Run("invalid order_by", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?order_by=payload", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid order_by field")
	})

	t.Run("invalid order direction", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?order=RANDOM", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "order must be ASC or DESC")
	})

	t.Run("invalid task id in filter", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?id=abc", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid task id")
	})

	t.Run("negative task id in filter", func(t *testing.T) {
		t.Parallel()

		r := httptest.NewRequest(http.MethodGet, "/api/tasks?id=-5", nil)

		_, err := parseListParams(r)

		require.Error(t, err)
	})
}

func TestTaskInfoToDetailResponse(t *testing.T) {
	t.Parallel()

	now := time.Now()
	token := "test-token"

	info := &client.TaskInfo{
		ID:               42,
		Type:             "email.send",
		Payload:          []byte(`{"to":"user@example.com"}`),
		Status:           asynqpg.TaskStatusFailed,
		IdempotencyToken: &token,
		Messages:         []string{"connection timeout", "retry failed"},
		BlockedTill:      now,
		AttemptsLeft:     0,
		AttemptsElapsed:  3,
		CreatedAt:        now.Add(-time.Hour),
		UpdatedAt:        now,
		FinalizedAt:      &now,
		AttemptedAt:      &now,
	}

	t.Run("with payload visible", func(t *testing.T) {
		t.Parallel()

		resp := taskInfoToDetailResponse(info, false)

		assert.Equal(t, int64(42), resp.ID)
		assert.Equal(t, "email.send", resp.Type)
		assert.Equal(t, "failed", resp.Status)
		require.NotNil(t, resp.Payload)
		assert.Equal(t, `{"to":"user@example.com"}`, *resp.Payload)
		assert.Equal(t, len(info.Payload), resp.PayloadSize)
		assert.Equal(t, &token, resp.IdempotencyToken)
		assert.Equal(t, []string{"connection timeout", "retry failed"}, resp.Messages)
		assert.Equal(t, 0, resp.AttemptsLeft)
		assert.Equal(t, 3, resp.AttemptsElapsed)
	})

	t.Run("with payload hidden", func(t *testing.T) {
		t.Parallel()

		resp := taskInfoToDetailResponse(info, true)

		assert.Nil(t, resp.Payload)
		assert.Equal(t, len(info.Payload), resp.PayloadSize)
	})

	t.Run("nil messages become empty slice", func(t *testing.T) {
		t.Parallel()

		infoNoMsg := &client.TaskInfo{
			ID:      1,
			Type:    "test",
			Status:  asynqpg.TaskStatusPending,
			Payload: nil,
		}

		resp := taskInfoToDetailResponse(infoNoMsg, false)

		assert.NotNil(t, resp.Messages)
		assert.Empty(t, resp.Messages)
	})

	t.Run("empty payload not exposed", func(t *testing.T) {
		t.Parallel()

		infoEmpty := &client.TaskInfo{
			ID:      1,
			Type:    "test",
			Status:  asynqpg.TaskStatusPending,
			Payload: []byte{},
		}

		resp := taskInfoToDetailResponse(infoEmpty, false)

		assert.Nil(t, resp.Payload)
		assert.Equal(t, 0, resp.PayloadSize)
	})
}

func TestHandleGetTask_InvalidID(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/abc", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "invalid_id", resp.Error.Code)
}

func TestHandleGetTaskPayload_InvalidID(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/api/tasks/xyz/payload", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleListTasks_InvalidParams(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	t.Run("invalid status", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/tasks?status=bogus", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "invalid_params", resp.Error.Code)
	})

	t.Run("invalid limit", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/tasks?limit=-5", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid order_by", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequest(http.MethodGet, "/api/tasks?order_by=payload", nil)
		rec := httptest.NewRecorder()

		h.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
