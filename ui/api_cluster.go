package ui

import (
	"errors"
	"net/http"
	"time"
)

// leaderResponse is the API response for the cluster leader endpoint.
// When no leader has been elected yet, all fields are zero/empty.
type leaderResponse struct {
	LeaderID        string     `json:"leader_id"`
	ElectedAt       *time.Time `json:"elected_at"`
	ExpiresAt       *time.Time `json:"expires_at"`
	LeaseTTLSeconds int64      `json:"lease_ttl_seconds"`
}

func (h *handler) handleClusterLeader(w http.ResponseWriter, r *http.Request) {
	row, err := h.repo.GetLeader(r.Context())
	if errors.Is(err, ErrNoLeader) {
		writeJSON(w, http.StatusOK, leaderResponse{})
		return
	}
	if err != nil {
		h.logger.Error("failed to get leader", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to get leader")
		return
	}

	ttl := max(int64(time.Until(row.ExpiresAt).Seconds()), 0)

	writeJSON(w, http.StatusOK, leaderResponse{
		LeaderID:        row.LeaderID,
		ElectedAt:       &row.ElectedAt,
		ExpiresAt:       &row.ExpiresAt,
		LeaseTTLSeconds: ttl,
	})
}
