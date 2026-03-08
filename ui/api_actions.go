package ui

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/yakser/asynqpg/client"
)

func (h *handler) handleRetryTask(w http.ResponseWriter, r *http.Request) {
	id, err := parseTaskID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
		return
	}

	info, err := h.client.RetryTask(r.Context(), id)
	if err != nil {
		writeClientError(w, err, "retry")
		return
	}

	writeJSON(w, http.StatusOK, taskInfoToDetailResponse(info, false))
}

func (h *handler) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id, err := parseTaskID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
		return
	}

	info, err := h.client.CancelTask(r.Context(), id)
	if err != nil {
		writeClientError(w, err, "cancel")
		return
	}

	writeJSON(w, http.StatusOK, taskInfoToDetailResponse(info, false))
}

func (h *handler) handleDeleteTask(w http.ResponseWriter, r *http.Request) {
	id, err := parseTaskID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
		return
	}

	info, err := h.client.DeleteTask(r.Context(), id)
	if err != nil {
		writeClientError(w, err, "delete")
		return
	}

	writeJSON(w, http.StatusOK, taskInfoToDetailResponse(info, false))
}

// writeClientError maps client package errors to appropriate HTTP responses.
func writeClientError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, client.ErrTaskNotFound):
		writeError(w, http.StatusNotFound, "task_not_found", "task not found")
	case errors.Is(err, client.ErrTaskRunning):
		writeError(w, http.StatusConflict, "task_running", "task is currently running")
	case errors.Is(err, client.ErrTaskAlreadyFinalized):
		writeError(w, http.StatusConflict, "task_already_finalized", "task is already in a terminal state")
	case errors.Is(err, client.ErrTaskAlreadyAvailable):
		writeError(w, http.StatusConflict, "task_already_available", "task is already available for processing")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to "+action+" task")
	}
}

// bulkRequest is the request body for bulk operations.
type bulkRequest struct {
	Type *string `json:"type"`
}

// bulkResponse is the response for bulk operations.
type bulkResponse struct {
	Affected int64 `json:"affected"`
}

func (h *handler) handleBulkRetry(w http.ResponseWriter, r *http.Request) {
	var req bulkRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", "invalid request body")
			return
		}
	}

	affected, err := h.repo.BulkRetryFailed(r.Context(), req.Type)
	if err != nil {
		h.logger.Error("failed to bulk retry tasks", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to bulk retry tasks")
		return
	}

	writeJSON(w, http.StatusOK, bulkResponse{Affected: affected})
}

func (h *handler) handleBulkDelete(w http.ResponseWriter, r *http.Request) {
	var req bulkRequest
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_body", "invalid request body")
			return
		}
	}

	affected, err := h.repo.BulkDeleteFailed(r.Context(), req.Type)
	if err != nil {
		h.logger.Error("failed to bulk delete tasks", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to bulk delete tasks")
		return
	}

	writeJSON(w, http.StatusOK, bulkResponse{Affected: affected})
}
