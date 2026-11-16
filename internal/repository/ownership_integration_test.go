//go:build integration

//nolint:paralleltest,goconst // Outcome matrices share database state; explicit scenario literals improve readability.
package repository_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

// Every outcome uses the same fixture and assertions in both execution modes.
func applyOutcome(ctx context.Context, repo *repository.Repository, kind string, batch bool, id, generation int64) (int, error) {
	ids, generations := []int64{id}, []int64{generation}
	due := time.Now().Add(time.Hour)
	if batch {
		switch kind {
		case "complete":
			return repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: ids, Generations: generations})
		case "fail":
			return repo.FailTasksMany(ctx, repository.FailTasksManyParams{IDs: ids, Generations: generations, Messages: []string{"failure"}})
		case "retry":
			return repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{IDs: ids, Generations: generations, Messages: []string{"failure"}, BlockedTills: []time.Time{due}})
		case "snooze":
			return repo.SnoozeTasksMany(ctx, repository.SnoozeTasksManyParams{IDs: ids, Generations: generations, BlockedTills: []time.Time{due}})
		}
	}
	switch kind {
	case "complete":
		return -1, repo.CompleteTasks(ctx, ids, generations)
	case "fail":
		return -1, repo.FailTasks(ctx, ids, generations, "failure")
	case "retry":
		return -1, repo.RetryTask(ctx, repository.RetryTaskParams{ID: id, ClaimGeneration: generation, Message: "failure", BlockedTill: due})
	default:
		return -1, repo.SnoozeTask(ctx, repository.SnoozeTaskParams{ID: id, ClaimGeneration: generation, BlockedTill: due})
	}
}

func claimOwnership(t *testing.T, repo *repository.Repository, taskType string) repository.ReadyTask {
	t.Helper()
	tasks, err := repo.GetReadyTasks(context.Background(), repository.GetReadyTasksParams{Type: taskType, Limit: 1, Delay: time.Hour})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	return tasks[0]
}

func rowSnapshot(t *testing.T, database *sqlx.DB, id int64) string {
	t.Helper()
	var snapshot string
	require.NoError(t, database.Get(&snapshot, `SELECT row_to_json(t)::text FROM asynqpg_tasks t WHERE id=$1`, id))
	return snapshot
}

func TestOwnership_AllOutcomes(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	for _, kind := range []string{"complete", "fail", "retry", "snooze"} {
		for _, batch := range []bool{false, true} {
			for _, state := range []string{"reclaimed", "cancelled", "completed", "failed", "pending"} {
				t.Run(fmt.Sprintf("%s/batch=%v/%s", kind, batch, state), func(t *testing.T) {
					taskType := t.Name()
					result, err := repo.PushTask(context.Background(), &repository.PushTaskParams{Type: taskType, Payload: []byte("{}"), AttemptsLeft: 3})
					require.NoError(t, err)
					a := claimOwnership(t, repo, taskType)
					require.EqualValues(t, 1, a.ClaimGeneration)
					current := a
					if state == "reclaimed" {
						_, err = database.Exec(`UPDATE asynqpg_tasks SET blocked_till=now()-interval '1 second' WHERE id=$1`, result.ID)
						require.NoError(t, err)
						current = claimOwnership(t, repo, taskType)
						require.EqualValues(t, 2, current.ClaimGeneration)
					} else {
						_, err = database.Exec(`UPDATE asynqpg_tasks SET status=$2::asynqpg_task_status, finalized_at=CASE WHEN $2='pending' THEN NULL ELSE now() END WHERE id=$1`, result.ID, state)
						require.NoError(t, err)
					}
					before := rowSnapshot(t, database, a.ID)
					count, err := applyOutcome(context.Background(), repo, kind, batch, a.ID, a.ClaimGeneration)
					require.NoError(t, err)
					if batch {
						require.Zero(t, count)
					}
					require.Equal(t, before, rowSnapshot(t, database, a.ID), "stale outcome must leave every column intact")
					if state != "reclaimed" {
						return
					}
					count, err = applyOutcome(context.Background(), repo, kind, batch, current.ID, current.ClaimGeneration)
					require.NoError(t, err)
					if batch {
						require.Equal(t, 1, count)
					}
					var got struct {
						Status   string
						Left     int `db:"attempts_left"`
						Elapsed  int `db:"attempts_elapsed"`
						Messages int
					}
					require.NoError(t, database.Get(&got, `SELECT status,attempts_left,attempts_elapsed,cardinality(messages) AS messages FROM asynqpg_tasks WHERE id=$1`, a.ID))
					expectedStatus := map[string]string{"complete": "completed", "fail": "failed", "retry": "pending", "snooze": "pending"}[kind]
					require.Equal(t, expectedStatus, got.Status)
					if kind == "snooze" {
						require.Equal(t, 3, got.Left)
						require.Zero(t, got.Elapsed)
					} else {
						require.Equal(t, 1, got.Elapsed)
					}
					if kind == "retry" {
						require.Equal(t, 2, got.Left)
						require.Equal(t, 1, got.Messages)
					}
					if kind == "fail" {
						require.Zero(t, got.Left)
						require.Equal(t, 1, got.Messages)
					}
					// Re-delivery after the successful outcome is benign too.
					before = rowSnapshot(t, database, a.ID)
					_, err = applyOutcome(context.Background(), repo, kind, batch, current.ID, current.ClaimGeneration)
					require.NoError(t, err)
					require.Equal(t, before, rowSnapshot(t, database, a.ID))
				})
			}
		}
	}
}

func TestOwnership_MixedGenerationBatches(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	repo := repository.NewRepository(database)
	ctx := context.Background()
	for _, kind := range []string{"complete", "fail", "retry", "snooze"} {
		t.Run(kind, func(t *testing.T) {
			var ids, gens []int64
			for i := range 3 {
				typ := fmt.Sprintf("%s-%d", kind, i)
				_, err := repo.PushTask(ctx, &repository.PushTaskParams{Type: typ, Payload: []byte("{}"), AttemptsLeft: 3})
				require.NoError(t, err)
				a := claimOwnership(t, repo, typ)
				if i < 2 {
					_, err = database.Exec(`UPDATE asynqpg_tasks SET blocked_till=now()-interval '1 second' WHERE id=$1`, a.ID)
					require.NoError(t, err)
					b := claimOwnership(t, repo, typ)
					if i == 1 {
						a = b
					}
				}
				ids = append(ids, a.ID)
				gens = append(gens, a.ClaimGeneration)
			}
			// Include old and current generation for the SAME ID in one statement.
			ids = append(ids, ids[0])
			gens = append(gens, 2)
			times := []time.Time{time.Now(), time.Now(), time.Now(), time.Now()}
			msgs := []string{"stale", "current", "first", "current"}
			var n int
			var err error
			switch kind {
			case "complete":
				n, err = repo.CompleteTasksMany(ctx, repository.CompleteTasksManyParams{IDs: ids, Generations: gens})
			case "fail":
				n, err = repo.FailTasksMany(ctx, repository.FailTasksManyParams{IDs: ids, Generations: gens, Messages: msgs})
			case "retry":
				n, err = repo.RetryTasksMany(ctx, repository.RetryTasksManyParams{IDs: ids, Generations: gens, Messages: msgs, BlockedTills: times})
			case "snooze":
				n, err = repo.SnoozeTasksMany(ctx, repository.SnoozeTasksManyParams{IDs: ids, Generations: gens, BlockedTills: times})
			}
			require.NoError(t, err)
			require.Equal(t, 3, n)
			if kind == "fail" || kind == "retry" {
				var msg string
				require.NoError(t, database.Get(&msg, `SELECT messages[1] FROM asynqpg_tasks WHERE id=$1`, ids[0]))
				require.Equal(t, "current", msg)
			}
		})
	}
}

func TestOwnership_AdditiveMigrationAndTransaction(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	ctx := context.Background()
	_, err := database.Exec(`ALTER TABLE asynqpg_tasks DROP COLUMN claim_generation`)
	require.NoError(t, err)
	repo := repository.NewRepository(database)
	result, err := repo.PushTask(ctx, &repository.PushTaskParams{Type: "migration", Payload: []byte("{}"), AttemptsLeft: 3})
	require.NoError(t, err)
	script, err := os.ReadFile("../../migrations/002_claim_generation.sql")
	require.NoError(t, err)
	_, err = database.Exec(string(script))
	require.NoError(t, err)
	var generation int64
	require.NoError(t, database.Get(&generation, `SELECT claim_generation FROM asynqpg_tasks WHERE id=$1`, result.ID))
	require.Zero(t, generation)
	tx, err := database.BeginTxx(ctx, nil)
	require.NoError(t, err)
	transactional := repository.NewRepository(tx)
	a := claimOwnership(t, transactional, "migration")
	require.EqualValues(t, 1, a.ClaimGeneration)
	require.NoError(t, transactional.CompleteTasks(ctx, []int64{a.ID}, []int64{a.ClaimGeneration}))
	require.NoError(t, tx.Rollback())
	require.NoError(t, database.Get(&generation, `SELECT claim_generation FROM asynqpg_tasks WHERE id=$1`, result.ID))
	require.Zero(t, generation)
	tx, err = database.BeginTxx(ctx, nil)
	require.NoError(t, err)
	transactional = repository.NewRepository(tx)
	a = claimOwnership(t, transactional, "migration")
	require.NoError(t, transactional.CompleteTasks(ctx, []int64{a.ID}, []int64{a.ClaimGeneration}))
	require.NoError(t, tx.Commit())
	var status string
	require.NoError(t, database.Get(&status, `SELECT status FROM asynqpg_tasks WHERE id=$1`, a.ID))
	require.Equal(t, "completed", status)
}
