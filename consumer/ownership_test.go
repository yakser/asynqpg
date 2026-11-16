package consumer

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/consumer/mocks"
	"github.com/yakser/asynqpg/internal/repository"
)

func TestOwnership_CancellationSnapshotDoesNotCancelNewAttempt(t *testing.T) {
	t.Parallel()
	p := mocks.NewPool(t)
	c := &Consumer{repo: repository.NewRepository(p), logger: slog.Default()}
	a, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	b, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	c.activeTaskCancels.Store(claimKey{1, 1}, cancelA)
	p.EXPECT().SelectContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, dest interface{}, _ string, _ ...interface{}) {
			// The query observed cancellation of A, but B starts before it returns.
			c.activeTaskCancels.Store(claimKey{1, 2}, cancelB)
			*dest.(*[]int64) = []int64{1}
		}).Return(nil).Once()
	require.NoError(t, c.checkCancelledTasks(context.Background()))
	require.ErrorIs(t, a.Err(), context.Canceled)
	require.NoError(t, b.Err())
	_, ok := c.activeTaskCancels.Load(claimKey{1, 2})
	require.True(t, ok)
}

func TestOwnership_OldHandlerCleanupKeepsNewAttempt(t *testing.T) {
	t.Parallel()
	p := newMockPool(t)
	p.EXPECT().ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(testSQLResult{}, nil).Twice()
	c, err := New(Config{Pool: p, DisableMaintenance: true, DisableBatchCompleter: true})
	require.NoError(t, err)
	defer c.cancel()
	startedA, startedB := make(chan struct{}), make(chan struct{})
	releaseA, releaseB := make(chan struct{}), make(chan struct{})
	doneA, doneB := make(chan struct{}), make(chan struct{})
	var bctx context.Context
	go func() {
		defer close(doneA)
		c.processTask(TaskHandlerFunc(func(context.Context, *asynqpg.TaskInfo) error { close(startedA); <-releaseA; return nil }), &repository.ReadyTask{ID: 1, ClaimGeneration: 1}, time.Minute)
	}()
	<-startedA
	go func() {
		defer close(doneB)
		c.processTask(TaskHandlerFunc(func(ctx context.Context, _ *asynqpg.TaskInfo) error {
			bctx = ctx
			close(startedB)
			<-releaseB
			return nil
		}), &repository.ReadyTask{ID: 1, ClaimGeneration: 2}, time.Minute)
	}()
	<-startedB
	close(releaseA)
	<-doneA
	value, ok := c.activeTaskCancels.Load(claimKey{1, 2})
	require.True(t, ok)
	require.NoError(t, bctx.Err())
	value.(context.CancelFunc)()
	require.ErrorIs(t, bctx.Err(), context.Canceled)
	close(releaseB)
	<-doneB
}
