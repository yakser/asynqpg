package ui

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	t.Run("writes data", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		writeJSON(rec, 200, map[string]string{"status": "ok"})

		assert.Equal(t, 200, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Nil(t, resp.Error)

		data, ok := resp.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "ok", data["status"])
	})

	t.Run("writes nil data", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		writeJSON(rec, 200, nil)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Nil(t, resp.Data)
		assert.Nil(t, resp.Error)
	})
}

func TestWriteError(t *testing.T) {
	t.Parallel()

	t.Run("writes error", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		writeError(rec, 404, "task_not_found", "Task 123 not found")

		assert.Equal(t, 404, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Nil(t, resp.Data)
		require.NotNil(t, resp.Error)
		assert.Equal(t, "task_not_found", resp.Error.Code)
		assert.Equal(t, "Task 123 not found", resp.Error.Message)
	})

	t.Run("writes 500 error", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		writeError(rec, 500, "internal_error", "something went wrong")

		assert.Equal(t, 500, rec.Code)

		var resp apiResponse
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
		assert.Equal(t, "internal_error", resp.Error.Code)
	})
}
