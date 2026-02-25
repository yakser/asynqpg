package client

import (
	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/repository"
)

func fullTaskToInfo(t *repository.FullTask) *TaskInfo {
	return &TaskInfo{
		ID:               t.ID,
		Type:             t.Type,
		Payload:          t.Payload,
		Status:           asynqpg.TaskStatus(t.Status),
		IdempotencyToken: t.IdempotencyToken,
		Messages:         t.Messages,
		BlockedTill:      t.BlockedTill,
		AttemptsLeft:     t.AttemptsLeft,
		AttemptsElapsed:  t.AttemptsElapsed,
		CreatedAt:        t.CreatedAt,
		UpdatedAt:        t.UpdatedAt,
		FinalizedAt:      t.FinalizedAt,
		AttemptedAt:      t.AttemptedAt,
	}
}

func repoResultToListResult(r *repository.ListTasksResult) *ListResult {
	tasks := make([]*TaskInfo, len(r.Tasks))
	for i := range r.Tasks {
		tasks[i] = fullTaskToInfo(&r.Tasks[i])
	}
	return &ListResult{
		Tasks: tasks,
		Total: r.Total,
	}
}
