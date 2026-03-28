package maintenance

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg"
	"github.com/yakser/asynqpg/internal/maintenance/mocks"
	"github.com/yakser/asynqpg/internal/repository"
)

func TestCleanerConfig_SetDefaults(t *testing.T) {
	t.Parallel()

	cfg := CleanerConfig{}
	cfg.setDefaults()

	require.Equal(t, defaultCompletedRetention, cfg.CompletedRetention)
	require.Equal(t, defaultFailedRetention, cfg.FailedRetention)
	require.Equal(t, defaultCancelledRetention, cfg.CancelledRetention)
	require.Equal(t, defaultCleanerInterval, cfg.Interval)
	require.Equal(t, defaultCleanerBatchSize, cfg.BatchSize)
	require.NotNil(t, cfg.Logger)
}

func TestCleanerConfig_SetDefaults_CustomValues(t *testing.T) {
	t.Parallel()

	cfg := CleanerConfig{
		CompletedRetention: 2 * time.Hour,
		FailedRetention:    48 * time.Hour,
		CancelledRetention: 4 * time.Hour,
		Interval:           time.Minute,
		BatchSize:          500,
	}
	cfg.setDefaults()

	require.Equal(t, 2*time.Hour, cfg.CompletedRetention)
	require.Equal(t, 48*time.Hour, cfg.FailedRetention)
	require.Equal(t, 500, cfg.BatchSize)
}

func TestCleaner_Name(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	c := NewCleaner(repo, CleanerConfig{})

	require.Equal(t, "cleaner", c.Name())
}

func TestCleaner_RunOnce_NoTasks(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(0, nil).Once()
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())

	require.NoError(t, err)
}

func TestCleaner_RunOnce_SingleBatch(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(50, nil).Once()
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())

	require.NoError(t, err)
}

func TestCleaner_RunOnce_MultipleBatches(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(100, nil).Once()
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(30, nil).Once()
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())

	require.NoError(t, err)
}

func TestCleaner_RunOnce_RepoError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(0, errors.New("db error")).Once()
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())

	require.Error(t, err)
}

func TestCleaner_RetentionParams(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)

	var capturedParams repository.DeleteOldTasksParams
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).
		Run(func(_ context.Context, params repository.DeleteOldTasksParams) {
			capturedParams = params
		}).
		Return(0, nil).Once()

	c := NewCleaner(repo, CleanerConfig{
		CompletedRetention: time.Hour,
		FailedRetention:    24 * time.Hour,
		CancelledRetention: 2 * time.Hour,
		BatchSize:          500,
	})

	before := time.Now()
	err := c.runOnce(context.Background())

	require.NoError(t, err)
	require.Equal(t, 500, capturedParams.Limit)

	// CompletedBefore should be approximately now - 1 hour
	expectedCompleted := before.Add(-time.Hour)
	require.WithinDuration(t, expectedCompleted, capturedParams.CompletedBefore, time.Second)

	expectedFailed := before.Add(-24 * time.Hour)
	require.WithinDuration(t, expectedFailed, capturedParams.FailedBefore, time.Second)
}

func TestCleaner_StartStop(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(0, nil).Maybe()
	c := NewCleaner(repo, CleanerConfig{Interval: 50 * time.Millisecond})

	require.NoError(t, c.Start(context.Background()))

	time.Sleep(100 * time.Millisecond)
	c.Stop()
}

func TestCleaner_Start_Idempotent(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	repo.EXPECT().DeleteOldTasks(mock.Anything, mock.Anything).Return(0, nil).Maybe()
	c := NewCleaner(repo, CleanerConfig{Interval: time.Hour})

	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	require.NoError(t, c.Start(context.Background()))
}

func TestCleaner_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	repo := mocks.NewCleanerRepo(t)
	c := NewCleaner(repo, CleanerConfig{})

	// Should not panic
	c.Stop()
}

func TestRescuerConfig_SetDefaults(t *testing.T) {
	t.Parallel()

	cfg := RescuerConfig{}
	cfg.setDefaults()

	require.Equal(t, defaultRescueAfter, cfg.RescueAfter)
	require.Equal(t, defaultRescueInterval, cfg.Interval)
	require.Equal(t, defaultRescueBatchSize, cfg.BatchSize)
	require.NotNil(t, cfg.RetryPolicy)
	require.NotNil(t, cfg.Logger)
}

func TestRescuerConfig_SetDefaults_NegativeValues(t *testing.T) {
	t.Parallel()

	cfg := RescuerConfig{
		RescueAfter: -1,
		Interval:    -1,
		BatchSize:   -1,
	}
	cfg.setDefaults()

	require.Equal(t, defaultRescueAfter, cfg.RescueAfter)
	require.Equal(t, defaultRescueInterval, cfg.Interval)
	require.Equal(t, defaultRescueBatchSize, cfg.BatchSize)
}

func TestRescuer_Name(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	r := NewRescuer(repo, RescuerConfig{})

	require.Equal(t, "rescuer", r.Name())
}

func TestRescuer_RunOnce_NoStuckTasks(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return(nil, nil).Once()
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())

	require.NoError(t, err)
}

func TestRescuer_RunOnce_RetryTasks(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 1, Type: "email", AttemptsLeft: 2, AttemptsElapsed: 1},
		{ID: 2, Type: "email", AttemptsLeft: 1, AttemptsElapsed: 2},
	}, nil).Once()

	var retryParams []repository.RetryTaskParams
	repo.EXPECT().RetryTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, params repository.RetryTaskParams) {
			retryParams = append(retryParams, params)
		}).
		Return(nil).Times(2)

	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &asynqpg.ConstantRetryPolicy{Delay: 5 * time.Second},
	})

	err := r.runOnce(context.Background())

	require.NoError(t, err)
	require.Len(t, retryParams, 2)
	require.Equal(t, int64(1), retryParams[0].ID)
	require.Equal(t, int64(2), retryParams[1].ID)
	require.Equal(t, "Stuck task rescued by Rescuer", retryParams[0].Message)
}

func TestRescuer_RunOnce_DiscardTasks(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 10, Type: "email", AttemptsLeft: 0, AttemptsElapsed: 3},
		{ID: 11, Type: "sms", AttemptsLeft: 0, AttemptsElapsed: 5},
	}, nil).Once()

	type failCall struct {
		ids     []int64
		message string
	}
	var failCalls []failCall
	repo.EXPECT().FailTasks(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, ids []int64, message string) {
			failCalls = append(failCalls, failCall{ids: ids, message: message})
		}).
		Return(nil).Times(2)

	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())

	require.NoError(t, err)
	require.Len(t, failCalls, 2)
	require.Equal(t, int64(10), failCalls[0].ids[0])
	require.Equal(t, int64(11), failCalls[1].ids[0])
}

func TestRescuer_RunOnce_MixedTasks(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 1, Type: "email", AttemptsLeft: 2, AttemptsElapsed: 1}, // retry
		{ID: 2, Type: "sms", AttemptsLeft: 0, AttemptsElapsed: 3},   // discard
		{ID: 3, Type: "push", AttemptsLeft: 1, AttemptsElapsed: 2},  // retry
	}, nil).Once()

	repo.EXPECT().RetryTask(mock.Anything, mock.Anything).Return(nil).Times(2)
	repo.EXPECT().FailTasks(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()

	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &asynqpg.ConstantRetryPolicy{Delay: time.Second},
	})

	err := r.runOnce(context.Background())

	require.NoError(t, err)
}

func TestRescuer_RunOnce_MultipleBatches(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 1, AttemptsLeft: 1, AttemptsElapsed: 0},
		{ID: 2, AttemptsLeft: 1, AttemptsElapsed: 0},
	}, nil).Once()
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 3, AttemptsLeft: 1, AttemptsElapsed: 0},
	}, nil).Once()

	repo.EXPECT().RetryTask(mock.Anything, mock.Anything).Return(nil).Times(3)

	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   2, // batch of 2 -- first batch is full, triggers second fetch
		RetryPolicy: &asynqpg.ConstantRetryPolicy{Delay: time.Second},
	})

	err := r.runOnce(context.Background())

	require.NoError(t, err)
}

func TestRescuer_RunOnce_GetStuckError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return(nil, errors.New("db error")).Once()
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())

	require.Error(t, err)
}

func TestRescuer_RunOnce_RetryError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 1, AttemptsLeft: 1, AttemptsElapsed: 0},
	}, nil).Once()
	repo.EXPECT().RetryTask(mock.Anything, mock.Anything).Return(errors.New("retry failed")).Once()

	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &asynqpg.ConstantRetryPolicy{Delay: time.Second},
	})

	err := r.runOnce(context.Background())

	require.Error(t, err)
}

func TestRescuer_RunOnce_FailError(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 1, AttemptsLeft: 0, AttemptsElapsed: 3},
	}, nil).Once()
	repo.EXPECT().FailTasks(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("fail failed")).Once()

	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())

	require.Error(t, err)
}

func TestRescuer_RetryPolicy_Applied(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return([]repository.StuckTask{
		{ID: 1, AttemptsLeft: 2, AttemptsElapsed: 3},
	}, nil).Once()

	var capturedParams repository.RetryTaskParams
	repo.EXPECT().RetryTask(mock.Anything, mock.Anything).
		Run(func(_ context.Context, params repository.RetryTaskParams) {
			capturedParams = params
		}).
		Return(nil).Once()

	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &asynqpg.ConstantRetryPolicy{Delay: 10 * time.Second},
	})

	before := time.Now()
	err := r.runOnce(context.Background())

	require.NoError(t, err)

	// BlockedTill should be approximately now + 10 seconds
	expectedBlockedTill := before.Add(10 * time.Second)
	require.WithinDuration(t, expectedBlockedTill, capturedParams.BlockedTill, time.Second)
}

func TestRescuer_StartStop(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	r := NewRescuer(repo, RescuerConfig{Interval: 50 * time.Millisecond})

	require.NoError(t, r.Start(context.Background()))

	time.Sleep(100 * time.Millisecond)
	r.Stop()
}

func TestRescuer_Start_Idempotent(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	repo.EXPECT().GetStuckTasks(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	r := NewRescuer(repo, RescuerConfig{Interval: time.Hour})

	require.NoError(t, r.Start(context.Background()))
	defer r.Stop()

	require.NoError(t, r.Start(context.Background()))
}

func TestRescuer_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	repo := mocks.NewRescuerRepo(t)
	r := NewRescuer(repo, RescuerConfig{})

	// Should not panic
	r.Stop()
}

func TestMaintainer_NewMaintainer_NilLogger(t *testing.T) {
	t.Parallel()

	m := NewMaintainer(nil)
	require.NotNil(t, m.logger)
}

func TestMaintainer_Start_StartsAllServices(t *testing.T) {
	t.Parallel()

	var started1, started2 atomic.Bool

	svc1 := mocks.NewService(t)
	svc1.EXPECT().Name().Return("svc1").Maybe()
	svc1.EXPECT().Start(mock.Anything).Run(func(_ context.Context) {
		started1.Store(true)
	}).Return(nil).Maybe()
	svc1.EXPECT().Stop().Maybe()

	svc2 := mocks.NewService(t)
	svc2.EXPECT().Name().Return("svc2").Maybe()
	svc2.EXPECT().Start(mock.Anything).Run(func(_ context.Context) {
		started2.Store(true)
	}).Return(nil).Maybe()
	svc2.EXPECT().Stop().Maybe()

	m := NewMaintainer(nil, svc1, svc2)

	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	require.Eventually(t, func() bool {
		return started1.Load() && started2.Load()
	}, 2*time.Second, 10*time.Millisecond)
}

func TestMaintainer_Stop_StopsAllServices(t *testing.T) {
	t.Parallel()

	var stopped1, stopped2 atomic.Bool
	var started1, started2 atomic.Bool

	svc1 := mocks.NewService(t)
	svc1.EXPECT().Name().Return("svc1").Maybe()
	svc1.EXPECT().Start(mock.Anything).Run(func(_ context.Context) {
		started1.Store(true)
	}).Return(nil).Maybe()
	svc1.EXPECT().Stop().Run(func() {
		stopped1.Store(true)
	}).Maybe()

	svc2 := mocks.NewService(t)
	svc2.EXPECT().Name().Return("svc2").Maybe()
	svc2.EXPECT().Start(mock.Anything).Run(func(_ context.Context) {
		started2.Store(true)
	}).Return(nil).Maybe()
	svc2.EXPECT().Stop().Run(func() {
		stopped2.Store(true)
	}).Maybe()

	m := NewMaintainer(nil, svc1, svc2)

	require.NoError(t, m.Start(context.Background()))

	require.Eventually(t, func() bool {
		return started1.Load() && started2.Load()
	}, 2*time.Second, 10*time.Millisecond)
	m.Stop()

	require.True(t, stopped1.Load())
	require.True(t, stopped2.Load())
}

func TestMaintainer_Start_Idempotent(t *testing.T) {
	t.Parallel()

	svc := mocks.NewService(t)
	svc.EXPECT().Name().Return("svc").Maybe()
	svc.EXPECT().Start(mock.Anything).Return(nil).Maybe()
	svc.EXPECT().Stop().Maybe()

	m := NewMaintainer(nil, svc)

	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	require.NoError(t, m.Start(context.Background()))
}

func TestMaintainer_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	m := NewMaintainer(nil)
	// Should not panic
	m.Stop()
}

func TestMaintainer_IsStarted(t *testing.T) {
	t.Parallel()

	svc := mocks.NewService(t)
	svc.EXPECT().Name().Return("svc").Maybe()
	svc.EXPECT().Start(mock.Anything).Return(nil).Maybe()
	svc.EXPECT().Stop().Maybe()

	m := NewMaintainer(nil, svc)

	require.False(t, m.IsStarted())

	require.NoError(t, m.Start(context.Background()))

	require.True(t, m.IsStarted())

	m.Stop()

	require.False(t, m.IsStarted())
}

func TestMaintainer_ServiceStartError(t *testing.T) {
	t.Parallel()

	var started1, started2 atomic.Bool

	svc1 := mocks.NewService(t)
	svc1.EXPECT().Name().Return("failing").Maybe()
	svc1.EXPECT().Start(mock.Anything).Run(func(_ context.Context) {
		started1.Store(true)
	}).Return(errors.New("start failed")).Maybe()
	svc1.EXPECT().Stop().Maybe()

	svc2 := mocks.NewService(t)
	svc2.EXPECT().Name().Return("working").Maybe()
	svc2.EXPECT().Start(mock.Anything).Run(func(_ context.Context) {
		started2.Store(true)
	}).Return(nil).Maybe()
	svc2.EXPECT().Stop().Maybe()

	m := NewMaintainer(nil, svc1, svc2)

	require.NoError(t, m.Start(context.Background()))

	require.Eventually(t, func() bool {
		return started1.Load() && started2.Load()
	}, 2*time.Second, 10*time.Millisecond)
	m.Stop()
}

func TestMaintainer_NoServices(t *testing.T) {
	t.Parallel()

	m := NewMaintainer(nil)

	require.NoError(t, m.Start(context.Background()))

	m.Stop()
}
