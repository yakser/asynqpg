package ui

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yakser/asynqpg/client"
)

const (
	orderASC  = "ASC"
	orderDESC = "DESC"
)

var validStatuses = map[string]bool{
	"pending":   true,
	"running":   true,
	"completed": true,
	"failed":    true,
	"cancelled": true,
}

var validOrderFields = map[string]bool{
	"id":           true,
	"created_at":   true,
	"updated_at":   true,
	"blocked_till": true,
}

func (h *handler) handleListTasks(w http.ResponseWriter, r *http.Request) {
	params, err := parseListParams(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_params", err.Error())
		return
	}

	result, err := h.repo.ListTasks(r.Context(), *params)
	if err != nil {
		h.logger.Error("failed to list tasks", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list tasks")
		return
	}

	writeJSON(w, http.StatusOK, result)
}

// taskDetailResponse is the API response for the get task endpoint.
type taskDetailResponse struct {
	ID               int64      `json:"id"`
	Type             string     `json:"type"`
	Status           string     `json:"status"`
	Payload          *string    `json:"payload"`
	PayloadSize      int        `json:"payload_size"`
	IdempotencyToken *string    `json:"idempotency_token"`
	Messages         []string   `json:"messages"`
	BlockedTill      time.Time  `json:"blocked_till"`
	AttemptsLeft     int        `json:"attempts_left"`
	AttemptsElapsed  int        `json:"attempts_elapsed"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	FinalizedAt      *time.Time `json:"finalized_at"`
	AttemptedAt      *time.Time `json:"attempted_at"`
}

func taskInfoToDetailResponse(info *client.TaskInfo, hidePayload bool) taskDetailResponse {
	resp := taskDetailResponse{
		ID:               info.ID,
		Type:             info.Type,
		Status:           string(info.Status),
		PayloadSize:      len(info.Payload),
		IdempotencyToken: info.IdempotencyToken,
		Messages:         info.Messages,
		BlockedTill:      info.BlockedTill,
		AttemptsLeft:     info.AttemptsLeft,
		AttemptsElapsed:  info.AttemptsElapsed,
		CreatedAt:        info.CreatedAt,
		UpdatedAt:        info.UpdatedAt,
		FinalizedAt:      info.FinalizedAt,
		AttemptedAt:      info.AttemptedAt,
	}

	if resp.Messages == nil {
		resp.Messages = []string{}
	}

	if !hidePayload && len(info.Payload) > 0 {
		s := string(info.Payload)
		resp.Payload = &s
	}

	return resp
}

func (h *handler) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id, err := parseTaskID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
		return
	}

	info, err := h.client.GetTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, client.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task_not_found", "task not found")
			return
		}
		h.logger.Error("failed to get task", "error", err, "task_id", id)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to get task")
		return
	}

	writeJSON(w, http.StatusOK, taskInfoToDetailResponse(info, h.opts.HidePayloadByDefault))
}

func (h *handler) handleGetTaskPayload(w http.ResponseWriter, r *http.Request) {
	id, err := parseTaskID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
		return
	}

	info, err := h.client.GetTask(r.Context(), id)
	if err != nil {
		if errors.Is(err, client.ErrTaskNotFound) {
			writeError(w, http.StatusNotFound, "task_not_found", "task not found")
			return
		}
		h.logger.Error("failed to get task payload", "error", err, "task_id", id)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to get task payload")
		return
	}

	if len(info.Payload) == 0 {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		return
	}

	if json.Valid(info.Payload) {
		w.Header().Set("Content-Type", "application/json")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(info.Payload)
}

func parseListParams(r *http.Request) (*ListTasksParams, error) {
	params := &ListTasksParams{
		Limit:    100,
		OrderBy:  "id",
		OrderDir: orderASC,
	}

	if v := r.URL.Query().Get("limit"); v != "" {
		limit, err := strconv.Atoi(v)
		if err != nil || limit < 1 {
			return nil, errors.New("limit must be a positive integer")
		}
		if limit > 10000 {
			limit = 10000
		}
		params.Limit = limit
	}

	if v := r.URL.Query().Get("offset"); v != "" {
		offset, err := strconv.Atoi(v)
		if err != nil || offset < 0 {
			return nil, errors.New("offset must be a non-negative integer")
		}
		params.Offset = offset
	}

	if v := r.URL.Query().Get("status"); v != "" {
		statuses := strings.Split(v, ",")
		for _, s := range statuses {
			s = strings.TrimSpace(s)
			if !validStatuses[s] {
				return nil, errors.New("invalid status: " + s)
			}
			params.Statuses = append(params.Statuses, s)
		}
	}

	if v := r.URL.Query().Get("type"); v != "" {
		params.Types = strings.Split(v, ",")
		for i, t := range params.Types {
			params.Types[i] = strings.TrimSpace(t)
		}
	}

	if v := r.URL.Query().Get("id"); v != "" {
		idStrs := strings.Split(v, ",")
		for _, idStr := range idStrs {
			id, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64)
			if err != nil || id <= 0 {
				return nil, errors.New("invalid task id: " + idStr)
			}
			params.IDs = append(params.IDs, id)
		}
	}

	if v := r.URL.Query().Get("order_by"); v != "" {
		if !validOrderFields[v] {
			return nil, errors.New("invalid order_by field: " + v)
		}
		params.OrderBy = v
	}

	if v := r.URL.Query().Get("order"); v != "" {
		upper := strings.ToUpper(v)
		if upper != orderASC && upper != orderDESC {
			return nil, errors.New("order must be ASC or DESC")
		}
		params.OrderDir = upper
	}

	if v := r.URL.Query().Get("created_after"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, errors.New("created_after must be a valid RFC3339 timestamp")
		}
		params.CreatedAfter = &t
	}

	if v := r.URL.Query().Get("created_before"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, errors.New("created_before must be a valid RFC3339 timestamp")
		}
		params.CreatedBefore = &t
	}

	return params, nil
}
