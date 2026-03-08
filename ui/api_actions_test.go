package ui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/client"
)

func TestWriteClientError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantCode   int
		wantErrKey string
	}{
		{
			name:       "task not found",
			err:        client.ErrTaskNotFound,
			wantCode:   http.StatusNotFound,
			wantErrKey: "task_not_found",
		},
		{
			name:       "task running",
			err:        client.ErrTaskRunning,
			wantCode:   http.StatusConflict,
			wantErrKey: "task_running",
		},
		{
			name:       "task already finalized",
			err:        client.ErrTaskAlreadyFinalized,
			wantCode:   http.StatusConflict,
			wantErrKey: "task_already_finalized",
		},
		{
			name:       "task already available",
			err:        client.ErrTaskAlreadyAvailable,
			wantCode:   http.StatusConflict,
			wantErrKey: "task_already_available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			writeClientError(rec, tt.err, "test")

			assert.Equal(t, tt.wantCode, rec.Code)

			var resp apiResponse
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
			assert.Equal(t, tt.wantErrKey, resp.Error.Code)
		})
	}
}

func TestHandleRetryTask_InvalidID(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/abc/retry", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "invalid_id", resp.Error.Code)
}

func TestHandleCancelTask_InvalidID(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/0/cancel", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleDeleteTask_InvalidID(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodDelete, "/api/tasks/notanumber", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleBulkRetry_InvalidBody(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/bulk/retry", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = 8
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp apiResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "invalid_body", resp.Error.Code)
}

func TestHandleBulkDelete_InvalidBody(t *testing.T) {
	t.Parallel()

	h, err := NewHandler(HandlerOpts{Pool: &mockPool{}})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/bulk/delete", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = 8
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
