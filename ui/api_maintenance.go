package ui

import (
	"net/http"
	"time"
)

// maintenanceConfigResponse describes the dashboard's preview assumptions.
type maintenanceConfigResponse struct {
	Source               string `json:"source"`
	StuckThresholdMs     int64  `json:"stuck_threshold_ms"`
	CompletedRetentionMs int64  `json:"completed_retention_ms"`
	FailedRetentionMs    int64  `json:"failed_retention_ms"`
	CancelledRetentionMs int64  `json:"cancelled_retention_ms"`
	IntervalMs           int64  `json:"interval_ms"`
}

// rescuePreviewResponse reports what the Rescuer would act on right now.
type rescuePreviewResponse struct {
	Total       int64 `json:"total"`
	Retriable   int64 `json:"retriable"`
	Discardable int64 `json:"discardable"`
}

// cleanupPreviewResponse reports what the Cleaner would delete right now.
type cleanupPreviewResponse struct {
	Total     int64 `json:"total"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Cancelled int64 `json:"cancelled"`
}

// queueHealthResponse holds general queue-health metrics.
type queueHealthResponse struct {
	OldestPendingMs  *int64 `json:"oldest_pending_ms"`
	CompletedLast15m int64  `json:"completed_last_15m"`
	FailedLast15m    int64  `json:"failed_last_15m"`
}

// maintenanceResponse is the API response for the cluster maintenance endpoint.
type maintenanceResponse struct {
	Config      maintenanceConfigResponse `json:"config"`
	Rescuable   rescuePreviewResponse     `json:"rescuable"`
	Cleanable   cleanupPreviewResponse    `json:"cleanable"`
	QueueHealth queueHealthResponse       `json:"queue_health"`
}

// handleClusterMaintenance returns the dashboard's preview configuration plus a
// read-only preview of what the Rescuer and Cleaner would act on right now. The
// preview is derived purely from the task table, so it works on any instance
// regardless of which one holds leadership.
func (h *handler) handleClusterMaintenance(w http.ResponseWriter, r *http.Request) {
	cfg := h.opts.effectiveMaintenance()
	source := "defaults"
	if h.opts.Maintenance != nil {
		source = "configured"
	}
	now := time.Now()
	ctx := r.Context()

	rescuable, err := h.repo.PreviewRescue(ctx, now.Add(-cfg.StuckThreshold))
	if err != nil {
		h.logger.Error("failed to preview rescue", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to preview maintenance")
		return
	}

	cleanable, err := h.repo.PreviewCleanup(ctx,
		now.Add(-cfg.CompletedRetention),
		now.Add(-cfg.FailedRetention),
		now.Add(-cfg.CancelledRetention),
	)
	if err != nil {
		h.logger.Error("failed to preview cleanup", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to preview maintenance")
		return
	}

	health, err := h.repo.GetQueueHealth(ctx)
	if err != nil {
		h.logger.Error("failed to get queue health", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to preview maintenance")
		return
	}

	var oldestPendingMs *int64
	if health.OldestPendingMs != nil {
		ms := int64(*health.OldestPendingMs)
		oldestPendingMs = &ms
	}

	writeJSON(w, http.StatusOK, maintenanceResponse{
		Config: maintenanceConfigResponse{
			Source:               source,
			StuckThresholdMs:     cfg.StuckThreshold.Milliseconds(),
			CompletedRetentionMs: cfg.CompletedRetention.Milliseconds(),
			FailedRetentionMs:    cfg.FailedRetention.Milliseconds(),
			CancelledRetentionMs: cfg.CancelledRetention.Milliseconds(),
			IntervalMs:           cfg.Interval.Milliseconds(),
		},
		Rescuable: rescuePreviewResponse(rescuable),
		Cleanable: cleanupPreviewResponse(cleanable),
		QueueHealth: queueHealthResponse{
			OldestPendingMs:  oldestPendingMs,
			CompletedLast15m: health.CompletedLast15m,
			FailedLast15m:    health.FailedLast15m,
		},
	})
}
