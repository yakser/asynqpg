//go:build integration

package ui

import (
	"context"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRepo(t *testing.T) (*repository, *sqlx.DB) {
	t.Helper()

	database := setupTestDB(t)
	uiRepo := newRepository(database)

	return uiRepo, database
}

// --- GetTaskTypeStats ---

func TestGetTaskTypeStats_EmptyDatabase(t *testing.T) {
	t.Parallel()

	uiRepo, _ := setupRepo(t)
	ctx := context.Background()

	stats, err := uiRepo.GetTaskTypeStats(ctx)

	require.NoError(t, err)
	assert.Empty(t, stats)
}

func TestGetTaskTypeStats_MultipleTypesAndStatuses(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create failed/completed BEFORE pending to avoid GetReadyTasks interference
	insertFailedTask(t, db, "stats-email")
	insertCompletedTask(t, db, "stats-report")
	insertPendingTask(t, db, "stats-email", []byte(`{}`))
	insertPendingTask(t, db, "stats-email", []byte(`{}`))

	stats, err := uiRepo.GetTaskTypeStats(ctx)

	require.NoError(t, err)
	require.NotEmpty(t, stats)

	// Build lookup map
	lookup := make(map[string]map[string]int64)
	for _, s := range stats {
		if lookup[s.Type] == nil {
			lookup[s.Type] = make(map[string]int64)
		}
		lookup[s.Type][s.Status] = s.Count
	}

	assert.Equal(t, int64(2), lookup["stats-email"]["pending"])
	assert.Equal(t, int64(1), lookup["stats-email"]["failed"])
	assert.Equal(t, int64(1), lookup["stats-report"]["completed"])
}

func TestGetTaskTypeStats_SingleTypeAllStatuses(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()
	taskType := "stats-all-statuses"

	// Create tasks that go through GetReadyTasks BEFORE the pending task
	insertFailedTask(t, db, taskType)
	insertCompletedTask(t, db, taskType)
	insertRunningTask(t, db, taskType)
	insertPendingTask(t, db, taskType, []byte(`{}`))

	stats, err := uiRepo.GetTaskTypeStats(ctx)

	require.NoError(t, err)

	lookup := make(map[string]int64)
	for _, s := range stats {
		if s.Type == taskType {
			lookup[s.Status] = s.Count
		}
	}

	assert.Equal(t, int64(1), lookup["pending"])
	assert.Equal(t, int64(1), lookup["running"])
	assert.Equal(t, int64(1), lookup["failed"])
	assert.Equal(t, int64(1), lookup["completed"])
}

// --- GetDistinctTaskTypes ---

func TestGetDistinctTaskTypes_EmptyDatabase(t *testing.T) {
	t.Parallel()

	uiRepo, _ := setupRepo(t)
	ctx := context.Background()

	types, err := uiRepo.GetDistinctTaskTypes(ctx)

	require.NoError(t, err)
	assert.Empty(t, types)
}

func TestGetDistinctTaskTypes_ReturnsUniqueSorted(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertPendingTask(t, db, "types-charlie", []byte(`{}`))
	insertPendingTask(t, db, "types-alpha", []byte(`{}`))
	insertPendingTask(t, db, "types-bravo", []byte(`{}`))
	insertPendingTask(t, db, "types-alpha", []byte(`{}`)) // duplicate

	types, err := uiRepo.GetDistinctTaskTypes(ctx)

	require.NoError(t, err)
	assert.Equal(t, []string{"types-alpha", "types-bravo", "types-charlie"}, types)
}

func TestGetDistinctTaskTypes_CacheTTL(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertPendingTask(t, db, "cache-test-type", []byte(`{}`))

	// First call – hits DB
	types1, err := uiRepo.GetDistinctTaskTypes(ctx)
	require.NoError(t, err)
	assert.Contains(t, types1, "cache-test-type")

	// Add another type
	insertPendingTask(t, db, "cache-test-type-new", []byte(`{}`))

	// Second call – should return cached data (no "cache-test-type-new")
	types2, err := uiRepo.GetDistinctTaskTypes(ctx)
	require.NoError(t, err)
	assert.NotContains(t, types2, "cache-test-type-new")
	assert.Equal(t, types1, types2)
}

// --- ListTasks ---

func TestListTasks_NoFilters(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	payload := []byte(`{"key":"value","nested":{"a":1}}`)
	insertPendingTask(t, db, "list-no-filter", payload)
	insertPendingTask(t, db, "list-no-filter", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	assert.Len(t, result.Tasks, 2)

	// Verify payload_size is correct (payload is NOT returned, but size is)
	found := false
	for _, task := range result.Tasks {
		if task.PayloadSize == int64(len(payload)) {
			found = true
			break
		}
	}
	assert.True(t, found, "expected to find task with correct payload_size")
}

func TestListTasks_FilterByStatus(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create failed/completed BEFORE pending to avoid GetReadyTasks interference
	insertFailedTask(t, db, "list-status")
	insertCompletedTask(t, db, "list-status")
	insertPendingTask(t, db, "list-status", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Statuses: []string{"failed"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
	assert.Len(t, result.Tasks, 1)
	assert.Equal(t, "failed", result.Tasks[0].Status)
}

func TestListTasks_FilterByMultipleStatuses(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create failed/completed BEFORE pending to avoid GetReadyTasks interference
	insertFailedTask(t, db, "list-multi-status")
	insertCompletedTask(t, db, "list-multi-status")
	insertPendingTask(t, db, "list-multi-status", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Statuses: []string{"pending", "failed"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	for _, task := range result.Tasks {
		assert.Contains(t, []string{"pending", "failed"}, task.Status)
	}
}

func TestListTasks_FilterByType(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertPendingTask(t, db, "list-type-a", []byte(`{}`))
	insertPendingTask(t, db, "list-type-a", []byte(`{}`))
	insertPendingTask(t, db, "list-type-b", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-type-a"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	for _, task := range result.Tasks {
		assert.Equal(t, "list-type-a", task.Type)
	}
}

func TestListTasks_FilterByIDs(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	id1 := insertPendingTask(t, db, "list-ids", []byte(`{}`))
	insertPendingTask(t, db, "list-ids", []byte(`{}`))
	id3 := insertPendingTask(t, db, "list-ids", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		IDs:      []int64{id1, id3},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
	assert.Len(t, result.Tasks, 2)
	assert.Equal(t, id1, result.Tasks[0].ID)
	assert.Equal(t, id3, result.Tasks[1].ID)
}

func TestListTasks_Pagination(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		insertPendingTask(t, db, "list-page", []byte(`{}`))
	}

	page1, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-page"},
		Limit:    2,
		Offset:   0,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Len(t, page1.Tasks, 2)
	assert.Equal(t, 5, page1.Total)

	page2, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-page"},
		Limit:    2,
		Offset:   2,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Len(t, page2.Tasks, 2)
	assert.Equal(t, 5, page2.Total)

	// Different tasks
	assert.NotEqual(t, page1.Tasks[0].ID, page2.Tasks[0].ID)

	// Last page
	page3, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-page"},
		Limit:    2,
		Offset:   4,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Len(t, page3.Tasks, 1)
	assert.Equal(t, 5, page3.Total)
}

func TestListTasks_SortOrderDESC(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertPendingTask(t, db, "list-sort", []byte(`{}`))
	insertPendingTask(t, db, "list-sort", []byte(`{}`))
	insertPendingTask(t, db, "list-sort", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-sort"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "DESC",
	})

	require.NoError(t, err)
	require.GreaterOrEqual(t, len(result.Tasks), 3)

	for i := 1; i < len(result.Tasks); i++ {
		assert.Greater(t, result.Tasks[i-1].ID, result.Tasks[i].ID)
	}
}

func TestListTasks_SortByCreatedAt(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertPendingTask(t, db, "list-sort-created", []byte(`{}`))
	insertPendingTask(t, db, "list-sort-created", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-sort-created"},
		Limit:    100,
		OrderBy:  "created_at",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	require.Len(t, result.Tasks, 2)
	assert.True(t, !result.Tasks[0].CreatedAt.After(result.Tasks[1].CreatedAt))
}

func TestListTasks_CombinedFilters(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create failed task BEFORE pending to avoid GetReadyTasks picking up the wrong task
	insertFailedTask(t, db, "list-combo-a")
	insertPendingTask(t, db, "list-combo-a", []byte(`{}`))
	insertPendingTask(t, db, "list-combo-b", []byte(`{}`))

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-combo-a"},
		Statuses: []string{"pending"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
	assert.Equal(t, "list-combo-a", result.Tasks[0].Type)
	assert.Equal(t, "pending", result.Tasks[0].Status)
}

func TestListTasks_EmptyResult(t *testing.T) {
	t.Parallel()

	uiRepo, _ := setupRepo(t)
	ctx := context.Background()

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"nonexistent-type-xyz-12345"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 0, result.Total)
	assert.NotNil(t, result.Tasks)
	assert.Empty(t, result.Tasks)
}

func TestListTasks_LargePayloadSize(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	largePayload := make([]byte, 100*1024) // 100KB
	for i := range largePayload {
		largePayload[i] = 'A'
	}
	insertPendingTask(t, db, "list-large-payload", largePayload)

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-large-payload"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)
	assert.Equal(t, int64(100*1024), result.Tasks[0].PayloadSize)
}

func TestListTasks_InvalidOrderByDefaultsToID(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertPendingTask(t, db, "list-invalid-order", []byte(`{}`))

	// The repository code defaults unknown order_by to "id"
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"list-invalid-order"},
		Limit:    100,
		OrderBy:  "something_unknown",
		OrderDir: "ASC",
	})

	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
}

// --- BulkRetryFailed ---

func TestBulkRetryFailed_AllFailed(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	id1 := insertFailedTask(t, db, "bulk-retry-all")
	id2 := insertFailedTask(t, db, "bulk-retry-all")
	id3 := insertFailedTask(t, db, "bulk-retry-all")

	affected, err := uiRepo.BulkRetryFailed(ctx, nil)

	require.NoError(t, err)
	assert.Equal(t, int64(3), affected)

	// Verify tasks are now pending
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		IDs:      []int64{id1, id2, id3},
		Limit:    10,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	for _, task := range result.Tasks {
		assert.Equal(t, "pending", task.Status)
	}
}

func TestBulkRetryFailed_FilterByType(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertFailedTask(t, db, "bulk-retry-type-a")
	insertFailedTask(t, db, "bulk-retry-type-a")
	id3 := insertFailedTask(t, db, "bulk-retry-type-b")

	filterType := "bulk-retry-type-a"
	affected, err := uiRepo.BulkRetryFailed(ctx, &filterType)

	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)

	// type-b should still be failed
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		IDs:      []int64{id3},
		Limit:    10,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)
	assert.Equal(t, "failed", result.Tasks[0].Status)
}

func TestBulkRetryFailed_NoMatchingTasks(t *testing.T) {
	t.Parallel()

	uiRepo, _ := setupRepo(t)
	ctx := context.Background()

	affected, err := uiRepo.BulkRetryFailed(ctx, nil)

	require.NoError(t, err)
	assert.Equal(t, int64(0), affected)
}

func TestBulkRetryFailed_SkipsNonFailedTasks(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create tasks that go through GetReadyTasks BEFORE the pending task
	// to avoid GetReadyTasks picking up the wrong pending task.
	failedID := insertFailedTask(t, db, "bulk-retry-skip")
	completedID := insertCompletedTask(t, db, "bulk-retry-skip")
	runningID := insertRunningTask(t, db, "bulk-retry-skip")
	pendingID := insertPendingTask(t, db, "bulk-retry-skip", []byte(`{}`))

	filterType := "bulk-retry-skip"
	affected, err := uiRepo.BulkRetryFailed(ctx, &filterType)

	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Verify only the failed task changed
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		IDs:      []int64{pendingID, runningID, completedID, failedID},
		Limit:    10,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)

	statusMap := make(map[int64]string)
	for _, task := range result.Tasks {
		statusMap[task.ID] = task.Status
	}
	assert.Equal(t, "pending", statusMap[pendingID])
	assert.Equal(t, "running", statusMap[runningID])
	assert.Equal(t, "completed", statusMap[completedID])
	assert.Equal(t, "pending", statusMap[failedID]) // was failed, now pending
}

func TestBulkRetryFailed_SetsAttemptsLeftToOneWhenZero(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	id := insertFailedTaskWithZeroAttempts(t, db, "bulk-retry-zero-attempts")

	affected, err := uiRepo.BulkRetryFailed(ctx, nil)

	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		IDs:      []int64{id},
		Limit:    1,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	require.Len(t, result.Tasks, 1)
	assert.Equal(t, "pending", result.Tasks[0].Status)
	assert.Equal(t, 1, result.Tasks[0].AttemptsLeft)
}

func TestBulkRetryFailed_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create 20 failed tasks
	for i := 0; i < 20; i++ {
		insertFailedTask(t, db, "bulk-retry-concurrent")
	}

	// Run 4 concurrent bulk retries
	var wg sync.WaitGroup
	var totalAffected int64
	var mu sync.Mutex

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			filterType := "bulk-retry-concurrent"
			affected, err := uiRepo.BulkRetryFailed(ctx, &filterType)
			require.NoError(t, err)

			mu.Lock()
			totalAffected += affected
			mu.Unlock()
		}()
	}

	wg.Wait()

	// Total affected across all goroutines should be exactly 20
	assert.Equal(t, int64(20), totalAffected)

	// All tasks should be pending now
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"bulk-retry-concurrent"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	for _, task := range result.Tasks {
		assert.Equal(t, "pending", task.Status)
	}
}

// --- BulkDeleteFailed ---

func TestBulkDeleteFailed_AllFailed(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertFailedTask(t, db, "bulk-delete-all")
	insertFailedTask(t, db, "bulk-delete-all")
	insertFailedTask(t, db, "bulk-delete-all")

	affected, err := uiRepo.BulkDeleteFailed(ctx, nil)

	require.NoError(t, err)
	assert.Equal(t, int64(3), affected)

	// Verify tasks are gone
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"bulk-delete-all"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.Total)
}

func TestBulkDeleteFailed_FilterByType(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	insertFailedTask(t, db, "bulk-del-type-a")
	insertFailedTask(t, db, "bulk-del-type-a")
	insertFailedTask(t, db, "bulk-del-type-b")

	filterType := "bulk-del-type-a"
	affected, err := uiRepo.BulkDeleteFailed(ctx, &filterType)

	require.NoError(t, err)
	assert.Equal(t, int64(2), affected)

	// type-b should still exist
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"bulk-del-type-b"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, result.Total)
	assert.Equal(t, "failed", result.Tasks[0].Status)
}

func TestBulkDeleteFailed_NoMatchingTasks(t *testing.T) {
	t.Parallel()

	uiRepo, _ := setupRepo(t)
	ctx := context.Background()

	affected, err := uiRepo.BulkDeleteFailed(ctx, nil)

	require.NoError(t, err)
	assert.Equal(t, int64(0), affected)
}

func TestBulkDeleteFailed_SkipsNonFailedTasks(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create failed/completed BEFORE pending to avoid GetReadyTasks interference
	insertFailedTask(t, db, "bulk-del-skip")
	completedID := insertCompletedTask(t, db, "bulk-del-skip")
	pendingID := insertPendingTask(t, db, "bulk-del-skip", []byte(`{}`))

	filterType := "bulk-del-skip"
	affected, err := uiRepo.BulkDeleteFailed(ctx, &filterType)

	require.NoError(t, err)
	assert.Equal(t, int64(1), affected)

	// Non-failed tasks should still exist
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		IDs:      []int64{pendingID, completedID},
		Limit:    10,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Equal(t, 2, result.Total)
}

func TestBulkDeleteFailed_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	uiRepo, db := setupRepo(t)
	ctx := context.Background()

	// Create 20 failed tasks
	for i := 0; i < 20; i++ {
		insertFailedTask(t, db, "bulk-del-concurrent")
	}

	// Run 4 concurrent bulk deletes
	var wg sync.WaitGroup
	var totalAffected int64
	var mu sync.Mutex

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			filterType := "bulk-del-concurrent"
			affected, err := uiRepo.BulkDeleteFailed(ctx, &filterType)
			require.NoError(t, err)

			mu.Lock()
			totalAffected += affected
			mu.Unlock()
		}()
	}

	wg.Wait()

	// Total affected across all goroutines should be exactly 20
	assert.Equal(t, int64(20), totalAffected)

	// All tasks should be gone
	result, err := uiRepo.ListTasks(ctx, ListTasksParams{
		Types:    []string{"bulk-del-concurrent"},
		Limit:    100,
		OrderBy:  "id",
		OrderDir: "ASC",
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.Total)
}
