package maintenance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/repository"
)

type mockCleanerRepo struct {
	mu      sync.Mutex
	calls   []repository.DeleteOldTasksParams
	results []deleteResult
}

type deleteResult struct {
	count int
	err   error
}

func (m *mockCleanerRepo) DeleteOldTasks(_ context.Context, params repository.DeleteOldTasksParams) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls = append(m.calls, params)
	idx := len(m.calls) - 1
	if idx >= len(m.results) {
		idx = len(m.results) - 1
	}
	if idx < 0 {
		return 0, nil
	}
	return m.results[idx].count, m.results[idx].err
}

func (m *mockCleanerRepo) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockCleanerRepo) getCall(i int) repository.DeleteOldTasksParams {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[i]
}

type mockRescuerRepo struct {
	mu             sync.Mutex
	stuckResults   []stuckResult
	stuckCallCount int
	retryParams    []repository.RetryTaskParams
	retryErr       error
	failCalls      []failCall
	failErr        error
}

type stuckResult struct {
	tasks []repository.StuckTask
	err   error
}

type failCall struct {
	ids     []int64
	message string
}

func (m *mockRescuerRepo) GetStuckTasks(_ context.Context, _ repository.GetStuckTasksParams) ([]repository.StuckTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := m.stuckCallCount
	m.stuckCallCount++
	if idx >= len(m.stuckResults) {
		return nil, nil
	}
	return m.stuckResults[idx].tasks, m.stuckResults[idx].err
}

func (m *mockRescuerRepo) RetryTask(_ context.Context, params repository.RetryTaskParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retryParams = append(m.retryParams, params)
	return m.retryErr
}

func (m *mockRescuerRepo) FailTasks(_ context.Context, ids []int64, message string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCalls = append(m.failCalls, failCall{ids: ids, message: message})
	return m.failErr
}

type constantRetryPolicy struct {
	delay time.Duration
}

func (p *constantRetryPolicy) NextRetry(_ int) time.Duration {
	return p.delay
}

type mockService struct {
	name     string
	started  atomic.Bool
	stopped  atomic.Bool
	startErr error
}

func (s *mockService) Start(_ context.Context) error {
	s.started.Store(true)
	return s.startErr
}

func (s *mockService) Stop() {
	s.stopped.Store(true)
}

func (s *mockService) Name() string {
	return s.name
}

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

	c := NewCleaner(&mockCleanerRepo{}, CleanerConfig{})
	require.Equal(t, "cleaner", c.Name())
}

func TestCleaner_RunOnce_NoTasks(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, repo.callCount())
}

func TestCleaner_RunOnce_SingleBatch(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 50, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, repo.callCount())
}

func TestCleaner_RunOnce_MultipleBatches(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{
			{count: 100, err: nil}, // full batch – continue
			{count: 30, err: nil},  // partial batch – stop
		},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, repo.callCount())
}

func TestCleaner_RunOnce_RepoError(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: errors.New("db error")}},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	require.Error(t, err)
}

func TestCleaner_RetentionParams(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{
		CompletedRetention: time.Hour,
		FailedRetention:    24 * time.Hour,
		CancelledRetention: 2 * time.Hour,
		BatchSize:          500,
	})

	before := time.Now()
	err := c.runOnce(context.Background())
	require.NoError(t, err)

	params := repo.getCall(0)
	require.Equal(t, 500, params.Limit)

	// CompletedBefore should be approximately now - 1 hour
	expectedCompleted := before.Add(-time.Hour)
	require.WithinDuration(t, expectedCompleted, params.CompletedBefore, time.Second)

	expectedFailed := before.Add(-24 * time.Hour)
	require.WithinDuration(t, expectedFailed, params.FailedBefore, time.Second)
}

func TestCleaner_StartStop(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{Interval: 50 * time.Millisecond})

	require.NoError(t, c.Start(context.Background()))

	time.Sleep(100 * time.Millisecond)
	c.Stop()
}

func TestCleaner_Start_Idempotent(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{Interval: time.Hour})

	require.NoError(t, c.Start(context.Background()))
	defer c.Stop()

	require.NoError(t, c.Start(context.Background()))
}

func TestCleaner_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	repo := &mockCleanerRepo{}
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

	r := NewRescuer(&mockRescuerRepo{}, RescuerConfig{})
	require.Equal(t, "rescuer", r.Name())
}

func TestRescuer_RunOnce_NoStuckTasks(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{{tasks: nil, err: nil}},
	}
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())
	require.NoError(t, err)
}

func TestRescuer_RunOnce_RetryTasks(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 1, Type: "email", AttemptsLeft: 2, AttemptsElapsed: 1},
				{ID: 2, Type: "email", AttemptsLeft: 1, AttemptsElapsed: 2},
			}},
		},
	}
	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &constantRetryPolicy{delay: 5 * time.Second},
	})

	err := r.runOnce(context.Background())
	require.NoError(t, err)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	require.Len(t, repo.retryParams, 2)
	require.Equal(t, int64(1), repo.retryParams[0].ID)
	require.Equal(t, int64(2), repo.retryParams[1].ID)
	require.Equal(t, "Stuck task rescued by Rescuer", repo.retryParams[0].Message)
}

func TestRescuer_RunOnce_DiscardTasks(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 10, Type: "email", AttemptsLeft: 0, AttemptsElapsed: 3},
				{ID: 11, Type: "sms", AttemptsLeft: 0, AttemptsElapsed: 5},
			}},
		},
	}
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())
	require.NoError(t, err)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	require.Len(t, repo.failCalls, 2)
	require.Equal(t, int64(10), repo.failCalls[0].ids[0])
	require.Equal(t, int64(11), repo.failCalls[1].ids[0])
}

func TestRescuer_RunOnce_MixedTasks(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 1, Type: "email", AttemptsLeft: 2, AttemptsElapsed: 1}, // retry
				{ID: 2, Type: "sms", AttemptsLeft: 0, AttemptsElapsed: 3},   // discard
				{ID: 3, Type: "push", AttemptsLeft: 1, AttemptsElapsed: 2},  // retry
			}},
		},
	}
	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &constantRetryPolicy{delay: time.Second},
	})

	err := r.runOnce(context.Background())
	require.NoError(t, err)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	require.Len(t, repo.retryParams, 2)
	require.Len(t, repo.failCalls, 1)
}

func TestRescuer_RunOnce_MultipleBatches(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 1, AttemptsLeft: 1, AttemptsElapsed: 0},
				{ID: 2, AttemptsLeft: 1, AttemptsElapsed: 0},
			}},
			{tasks: []repository.StuckTask{
				{ID: 3, AttemptsLeft: 1, AttemptsElapsed: 0},
			}},
		},
	}
	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   2, // batch of 2 – first batch is full, triggers second fetch
		RetryPolicy: &constantRetryPolicy{delay: time.Second},
	})

	err := r.runOnce(context.Background())
	require.NoError(t, err)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	require.Equal(t, 2, repo.stuckCallCount)
	require.Len(t, repo.retryParams, 3)
}

func TestRescuer_RunOnce_GetStuckError(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: nil, err: errors.New("db error")},
		},
	}
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())
	require.Error(t, err)
}

func TestRescuer_RunOnce_RetryError(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 1, AttemptsLeft: 1, AttemptsElapsed: 0},
			}},
		},
		retryErr: errors.New("retry failed"),
	}
	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &constantRetryPolicy{delay: time.Second},
	})

	err := r.runOnce(context.Background())
	require.Error(t, err)
}

func TestRescuer_RunOnce_FailError(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 1, AttemptsLeft: 0, AttemptsElapsed: 3},
			}},
		},
		failErr: errors.New("fail failed"),
	}
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())
	require.Error(t, err)
}

func TestRescuer_RetryPolicy_Applied(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: []repository.StuckTask{
				{ID: 1, AttemptsLeft: 2, AttemptsElapsed: 3},
			}},
		},
	}
	r := NewRescuer(repo, RescuerConfig{
		BatchSize:   100,
		RetryPolicy: &constantRetryPolicy{delay: 10 * time.Second},
	})

	before := time.Now()
	err := r.runOnce(context.Background())
	require.NoError(t, err)

	repo.mu.Lock()
	defer repo.mu.Unlock()

	require.Len(t, repo.retryParams, 1)

	// BlockedTill should be approximately now + 10 seconds
	expectedBlockedTill := before.Add(10 * time.Second)
	require.WithinDuration(t, expectedBlockedTill, repo.retryParams[0].BlockedTill, time.Second)
}

func TestRescuer_StartStop(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{{tasks: nil}},
	}
	r := NewRescuer(repo, RescuerConfig{Interval: 50 * time.Millisecond})

	require.NoError(t, r.Start(context.Background()))

	time.Sleep(100 * time.Millisecond)
	r.Stop()
}

func TestRescuer_Start_Idempotent(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{{tasks: nil}},
	}
	r := NewRescuer(repo, RescuerConfig{Interval: time.Hour})

	require.NoError(t, r.Start(context.Background()))
	defer r.Stop()

	require.NoError(t, r.Start(context.Background()))
}

func TestRescuer_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	repo := &mockRescuerRepo{}
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

	svc1 := &mockService{name: "svc1"}
	svc2 := &mockService{name: "svc2"}
	m := NewMaintainer(nil, svc1, svc2)

	require.NoError(t, m.Start(context.Background()))
	defer m.Stop()

	// Wait for goroutines to start services
	time.Sleep(50 * time.Millisecond)

	require.True(t, svc1.started.Load())
	require.True(t, svc2.started.Load())
}

func TestMaintainer_Stop_StopsAllServices(t *testing.T) {
	t.Parallel()

	svc1 := &mockService{name: "svc1"}
	svc2 := &mockService{name: "svc2"}
	m := NewMaintainer(nil, svc1, svc2)

	require.NoError(t, m.Start(context.Background()))

	time.Sleep(50 * time.Millisecond)
	m.Stop()

	require.True(t, svc1.stopped.Load())
	require.True(t, svc2.stopped.Load())
}

func TestMaintainer_Start_Idempotent(t *testing.T) {
	t.Parallel()

	svc := &mockService{name: "svc"}
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

	m := NewMaintainer(nil, &mockService{name: "svc"})

	require.False(t, m.IsStarted())

	require.NoError(t, m.Start(context.Background()))

	require.True(t, m.IsStarted())

	m.Stop()

	require.False(t, m.IsStarted())
}

func TestMaintainer_ServiceStartError(t *testing.T) {
	t.Parallel()

	svc1 := &mockService{name: "failing", startErr: errors.New("start failed")}
	svc2 := &mockService{name: "working"}
	m := NewMaintainer(nil, svc1, svc2)

	require.NoError(t, m.Start(context.Background()))

	time.Sleep(50 * time.Millisecond)
	m.Stop()

	// Both services should have been attempted
	require.True(t, svc1.started.Load())
	require.True(t, svc2.started.Load())
}

func TestMaintainer_NoServices(t *testing.T) {
	t.Parallel()

	m := NewMaintainer(nil)

	require.NoError(t, m.Start(context.Background()))

	m.Stop()
}
