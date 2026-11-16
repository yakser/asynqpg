//go:build integration

//nolint:paralleltest,goconst // Outcome matrices share database state; explicit scenario literals improve readability.
package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/completer"
	"github.com/yakser/asynqpg/internal/repository"
	"github.com/yakser/asynqpg/testutils"
)

func TestOwnership_CancellationSurvivesHandlerAndShutdown(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	for _, batch := range []bool{false, true} {
		for _, shutdown := range []bool{false, true} {
			for _, handlerErr := range []error{nil, context.Canceled} {
				t.Run(fmt.Sprintf("batch=%v/shutdown=%v/error=%v", batch, shutdown, handlerErr), func(t *testing.T) {
					c, err := New(Config{Pool: database, DisableMaintenance: true, DisableBatchCompleter: !batch})
					require.NoError(t, err)
					defer c.cancel()
					if batch {
						bc := completer.NewBatchCompleter(c.repo, completer.Config{FlushInterval: time.Hour})
						require.NoError(t, bc.Start(context.Background()))
						c.completer = bc
						defer bc.Stop()
					}
					result, err := c.repo.PushTask(context.Background(), &repository.PushTaskParams{Type: "ownership", Payload: []byte("{}"), AttemptsLeft: 3})
					require.NoError(t, err)
					tasks, claimErr := c.repo.GetReadyTasks(context.Background(), repository.GetReadyTasksParams{Type: "ownership", Limit: 1, Delay: time.Hour})
					require.NoError(t, claimErr)
					require.Len(t, tasks, 1)
					started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
					go func() {
						defer close(done)
						c.processTask(TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { close(started); <-release; return handlerErr }), &tasks[0], time.Minute)
					}()
					<-started
					_, _, err = repository.NewClientRepository(database).CancelTaskByID(context.Background(), result.ID)
					require.NoError(t, err)
					if shutdown {
						c.cancel()
					}
					close(release)
					<-done
					if batch {
						c.completer.Stop()
					}
					var got struct {
						Status     string
						Elapsed    int   `db:"attempts_elapsed"`
						Generation int64 `db:"claim_generation"`
					}
					require.NoError(t, database.Get(&got, `SELECT status,attempts_elapsed,claim_generation FROM asynqpg_tasks WHERE id=$1`, result.ID))
					require.Equal(t, "cancelled", got.Status)
					require.Zero(t, got.Elapsed)
					require.EqualValues(t, 1, got.Generation)
				})
			}
		}
	}
}

func TestOwnership_ConsumerCarriesReclaimedGeneration(t *testing.T) {
	database := testutils.SetupTestDatabase(t)
	outcomes := []struct {
		name    string
		err     error
		status  string
		elapsed int
	}{
		{"complete", nil, "completed", 1},
		{"fail", asynqpg.ErrSkipRetry, "failed", 1},
		{"retry", fmt.Errorf("transient"), "pending", 1},
		{"snooze", asynqpg.TaskSnooze(time.Hour), "pending", 0},
		{"snooze-with-error", asynqpg.TaskSnoozeWithError(time.Hour), "pending", 1},
	}
	for _, batch := range []bool{false, true} {
		for _, outcome := range outcomes {
			t.Run(fmt.Sprintf("%s/batch=%v", outcome.name, batch), func(t *testing.T) {
				c, err := New(Config{Pool: database, DisableMaintenance: true, DisableBatchCompleter: !batch})
				require.NoError(t, err)
				defer c.cancel()
				_, err = c.repo.PushTask(context.Background(), &repository.PushTaskParams{Type: t.Name(), Payload: []byte("{}"), AttemptsLeft: 3})
				require.NoError(t, err)
				claim := func() repository.ReadyTask {
					tasks, claimErr := c.repo.GetReadyTasks(context.Background(), repository.GetReadyTasksParams{Type: t.Name(), Limit: 1, Delay: time.Hour})
					require.NoError(t, claimErr)
					require.Len(t, tasks, 1)
					return tasks[0]
				}
				a := claim()
				_, err = database.Exec(`UPDATE asynqpg_tasks SET blocked_till=now()-interval '1 second' WHERE id=$1`, a.ID)
				require.NoError(t, err)
				b := claim()
				require.EqualValues(t, 2, b.ClaimGeneration)
				process := func(task *repository.ReadyTask) {
					if batch {
						bc := completer.NewBatchCompleter(c.repo, completer.Config{FlushInterval: time.Hour})
						require.NoError(t, bc.Start(context.Background()))
						c.completer = bc
						defer bc.Stop()
					}
					c.processTask(TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { return outcome.err }), task, time.Minute)
				}
				process(&a)
				var status string
				require.NoError(t, database.Get(&status, `SELECT status FROM asynqpg_tasks WHERE id=$1`, a.ID))
				require.Equal(t, "running", status)
				process(&b)
				var elapsed int
				require.NoError(t, database.Get(&status, `SELECT status FROM asynqpg_tasks WHERE id=$1`, b.ID))
				require.Equal(t, outcome.status, status)
				require.NoError(t, database.Get(&elapsed, `SELECT attempts_elapsed FROM asynqpg_tasks WHERE id=$1`, b.ID))
				require.Equal(t, outcome.elapsed, elapsed)
			})
		}
	}
}
