package ui

import (
	"net/http"
)

// statsResponse is the API response for the stats endpoint.
type statsResponse struct {
	Total    int64                   `json:"total"`
	ByStatus map[string]int64        `json:"by_status"`
	ByType   []taskTypeStatsResponse `json:"by_type"`
}

type taskTypeStatsResponse struct {
	Type     string           `json:"type"`
	Total    int64            `json:"total"`
	ByStatus map[string]int64 `json:"by_status"`
}

func (h *handler) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.repo.GetTaskTypeStats(r.Context())
	if err != nil {
		h.logger.Error("failed to get stats", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to get stats")
		return
	}

	byStatus := map[string]int64{
		"pending":   0,
		"running":   0,
		"completed": 0,
		"failed":    0,
		"cancelled": 0,
	}

	typeMap := make(map[string]*taskTypeStatsResponse)
	var total int64

	for _, s := range stats {
		total += s.Count
		byStatus[s.Status] += s.Count

		ts, ok := typeMap[s.Type]
		if !ok {
			ts = &taskTypeStatsResponse{
				Type: s.Type,
				ByStatus: map[string]int64{
					"pending":   0,
					"running":   0,
					"completed": 0,
					"failed":    0,
					"cancelled": 0,
				},
			}
			typeMap[s.Type] = ts
		}
		ts.ByStatus[s.Status] += s.Count
		ts.Total += s.Count
	}

	byType := make([]taskTypeStatsResponse, 0, len(typeMap))
	for _, ts := range typeMap {
		byType = append(byType, *ts)
	}

	writeJSON(w, http.StatusOK, statsResponse{
		Total:    total,
		ByStatus: byStatus,
		ByType:   byType,
	})
}

func (h *handler) handleTaskTypes(w http.ResponseWriter, r *http.Request) {
	types, err := h.repo.GetDistinctTaskTypes(r.Context())
	if err != nil {
		h.logger.Error("failed to get task types", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to get task types")
		return
	}

	if types == nil {
		types = []string{}
	}

	writeJSON(w, http.StatusOK, types)
}
