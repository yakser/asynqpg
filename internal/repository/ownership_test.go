package repository

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOwnership_RequiresGenerationForEveryBatchEntry(t *testing.T) {
	t.Parallel()
	// A nil querier proves invalid inputs are rejected before any SQL executes.
	repo := NewRepository(nil)
	ctx := context.Background()
	for _, generations := range [][]int64{nil, {1, 2}} {
		_, err := repo.CompleteTasksMany(ctx, CompleteTasksManyParams{IDs: []int64{1}, Generations: generations})
		require.Error(t, err)
		_, err = repo.FailTasksMany(ctx, FailTasksManyParams{IDs: []int64{1}, Generations: generations, Messages: []string{"failure"}})
		require.Error(t, err)
		_, err = repo.RetryTasksMany(ctx, RetryTasksManyParams{IDs: []int64{1}, Generations: generations, Messages: []string{"failure"}, BlockedTills: []time.Time{time.Now()}})
		require.Error(t, err)
		_, err = repo.SnoozeTasksMany(ctx, SnoozeTasksManyParams{IDs: []int64{1}, Generations: generations, BlockedTills: []time.Time{time.Now()}})
		require.Error(t, err)
		require.Error(t, repo.CompleteTasks(ctx, []int64{1}, generations))
		require.Error(t, repo.FailTasks(ctx, []int64{1}, generations, "failure"))
	}
}
