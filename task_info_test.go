package asynqpg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnitTaskStatus_IsFinalized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status   TaskStatus
		expected bool
	}{
		{TaskStatusPending, false},
		{TaskStatusRunning, false},
		{TaskStatusCompleted, true},
		{TaskStatusFailed, true},
		{TaskStatusCancelled, true},
		{TaskStatus("unknown"), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.status.IsFinalized())
		})
	}
}
