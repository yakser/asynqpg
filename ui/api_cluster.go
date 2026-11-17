package ui

import (
	"errors"
	"net/http"
	"time"

	"github.com/yakser/asynqpg"
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

// consumerResponse is a single consumer instance in the cluster consumers endpoint.
type consumerResponse struct {
	Running            bool                       `json:"running"`
	ID                 string                     `json:"id"`
	IsLeader           bool                       `json:"is_leader"`
	MaintenanceEnabled bool                       `json:"maintenance_enabled"`
	StartedAt          *time.Time                 `json:"started_at"`
	FetchIntervalMs    int64                      `json:"fetch_interval_ms"`
	JanitorIntervalMs  int64                      `json:"janitor_interval_ms"`
	StuckThresholdMs   int64                      `json:"stuck_threshold_ms"`
	ShutdownTimeoutMs  int64                      `json:"shutdown_timeout_ms"`
	TaskTypes          []consumerTaskTypeResponse `json:"task_types"`
}

// consumerTaskTypeResponse describes one registered task type and its worker pool.
type consumerTaskTypeResponse struct {
	Type        string `json:"type"`
	Workers     int    `json:"workers"`
	Busy        int    `json:"busy"`
	MaxAttempts int    `json:"max_attempts"`
	TimeoutMs   int64  `json:"timeout_ms"`
}

// handleClusterConsumers returns live snapshots of the consumer instances running
// in the same process as this handler. When no consumer is attached (e.g. a
// standalone dashboard), it returns an empty list.
func (h *handler) handleClusterConsumers(w http.ResponseWriter, _ *http.Request) {
	var infos []asynqpg.ConsumerInfo
	if h.opts.ConsumerInfoFunc != nil {
		infos = h.opts.ConsumerInfoFunc()
	}

	resp := make([]consumerResponse, 0, len(infos))
	for _, info := range infos {
		taskTypes := make([]consumerTaskTypeResponse, 0, len(info.TaskTypes))
		for _, tt := range info.TaskTypes {
			taskTypes = append(taskTypes, consumerTaskTypeResponse{
				Type:        tt.Type,
				Workers:     tt.Workers,
				Busy:        tt.Busy,
				MaxAttempts: tt.MaxAttempts,
				TimeoutMs:   tt.Timeout.Milliseconds(),
			})
		}

		var startedAt *time.Time
		if !info.StartedAt.IsZero() {
			started := info.StartedAt
			startedAt = &started
		}

		resp = append(resp, consumerResponse{
			ID:                 info.ID,
			Running:            info.Running,
			IsLeader:           info.IsLeader,
			MaintenanceEnabled: info.MaintenanceEnabled,
			StartedAt:          startedAt,
			FetchIntervalMs:    info.FetchInterval.Milliseconds(),
			JanitorIntervalMs:  info.JanitorInterval.Milliseconds(),
			StuckThresholdMs:   info.StuckThreshold.Milliseconds(),
			ShutdownTimeoutMs:  info.ShutdownTimeout.Milliseconds(),
			TaskTypes:          taskTypes,
		})
	}

	writeJSON(w, http.StatusOK, resp)
}
