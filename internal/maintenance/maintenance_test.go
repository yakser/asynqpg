package maintenance

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yakser/asynqpg/internal/repository"
)

// --- Mock Cleaner Repo ---

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

// --- Mock Rescuer Repo ---

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

// --- Mock Retry Policy ---

type constantRetryPolicy struct {
	delay time.Duration
}

func (p *constantRetryPolicy) NextRetry(_ int) time.Duration {
	return p.delay
}

// --- Mock Service ---

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

// ========== Cleaner Config Tests ==========

func TestCleanerConfig_SetDefaults(t *testing.T) {
	cfg := CleanerConfig{}
	cfg.setDefaults()

	if cfg.CompletedRetention != defaultCompletedRetention {
		t.Fatalf("expected CompletedRetention %v, got %v", defaultCompletedRetention, cfg.CompletedRetention)
	}
	if cfg.FailedRetention != defaultFailedRetention {
		t.Fatalf("expected FailedRetention %v, got %v", defaultFailedRetention, cfg.FailedRetention)
	}
	if cfg.CancelledRetention != defaultCancelledRetention {
		t.Fatalf("expected CancelledRetention %v, got %v", defaultCancelledRetention, cfg.CancelledRetention)
	}
	if cfg.Interval != defaultCleanerInterval {
		t.Fatalf("expected Interval %v, got %v", defaultCleanerInterval, cfg.Interval)
	}
	if cfg.BatchSize != defaultCleanerBatchSize {
		t.Fatalf("expected BatchSize %d, got %d", defaultCleanerBatchSize, cfg.BatchSize)
	}
	if cfg.Logger == nil {
		t.Fatal("expected Logger to be set")
	}
}

func TestCleanerConfig_SetDefaults_CustomValues(t *testing.T) {
	cfg := CleanerConfig{
		CompletedRetention: 2 * time.Hour,
		FailedRetention:    48 * time.Hour,
		CancelledRetention: 4 * time.Hour,
		Interval:           time.Minute,
		BatchSize:          500,
	}
	cfg.setDefaults()

	if cfg.CompletedRetention != 2*time.Hour {
		t.Fatalf("expected 2h, got %v", cfg.CompletedRetention)
	}
	if cfg.FailedRetention != 48*time.Hour {
		t.Fatalf("expected 48h, got %v", cfg.FailedRetention)
	}
	if cfg.BatchSize != 500 {
		t.Fatalf("expected 500, got %d", cfg.BatchSize)
	}
}

// ========== Cleaner Tests ==========

func TestCleaner_Name(t *testing.T) {
	c := NewCleaner(&mockCleanerRepo{}, CleanerConfig{})
	if c.Name() != "cleaner" {
		t.Fatalf("expected %q, got %q", "cleaner", c.Name())
	}
}

func TestCleaner_RunOnce_NoTasks(t *testing.T) {
	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.callCount() != 1 {
		t.Fatalf("expected 1 call, got %d", repo.callCount())
	}
}

func TestCleaner_RunOnce_SingleBatch(t *testing.T) {
	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 50, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.callCount() != 1 {
		t.Fatalf("expected 1 call (50 < 100 batch), got %d", repo.callCount())
	}
}

func TestCleaner_RunOnce_MultipleBatches(t *testing.T) {
	repo := &mockCleanerRepo{
		results: []deleteResult{
			{count: 100, err: nil}, // full batch – continue
			{count: 30, err: nil},  // partial batch – stop
		},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.callCount() != 2 {
		t.Fatalf("expected 2 calls, got %d", repo.callCount())
	}
}

func TestCleaner_RunOnce_RepoError(t *testing.T) {
	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: errors.New("db error")}},
	}
	c := NewCleaner(repo, CleanerConfig{BatchSize: 100})

	err := c.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestCleaner_RetentionParams(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	params := repo.getCall(0)
	if params.Limit != 500 {
		t.Fatalf("expected Limit 500, got %d", params.Limit)
	}

	// CompletedBefore should be approximately now - 1 hour
	expectedCompleted := before.Add(-time.Hour)
	if params.CompletedBefore.Before(expectedCompleted.Add(-time.Second)) ||
		params.CompletedBefore.After(expectedCompleted.Add(time.Second)) {
		t.Fatalf("CompletedBefore out of range: expected ~%v, got %v", expectedCompleted, params.CompletedBefore)
	}

	expectedFailed := before.Add(-24 * time.Hour)
	if params.FailedBefore.Before(expectedFailed.Add(-time.Second)) ||
		params.FailedBefore.After(expectedFailed.Add(time.Second)) {
		t.Fatalf("FailedBefore out of range: expected ~%v, got %v", expectedFailed, params.FailedBefore)
	}
}

func TestCleaner_StartStop(t *testing.T) {
	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{Interval: 50 * time.Millisecond})

	err := c.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	c.Stop()
}

func TestCleaner_Start_Idempotent(t *testing.T) {
	repo := &mockCleanerRepo{
		results: []deleteResult{{count: 0, err: nil}},
	}
	c := NewCleaner(repo, CleanerConfig{Interval: time.Hour})

	err := c.Start(context.Background())
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	defer c.Stop()

	err = c.Start(context.Background())
	if err != nil {
		t.Fatalf("second start should return nil, got: %v", err)
	}
}

func TestCleaner_Stop_NotStarted(t *testing.T) {
	repo := &mockCleanerRepo{}
	c := NewCleaner(repo, CleanerConfig{})
	// Should not panic
	c.Stop()
}

// ========== Rescuer Config Tests ==========

func TestRescuerConfig_SetDefaults(t *testing.T) {
	cfg := RescuerConfig{}
	cfg.setDefaults()

	if cfg.RescueAfter != defaultRescueAfter {
		t.Fatalf("expected RescueAfter %v, got %v", defaultRescueAfter, cfg.RescueAfter)
	}
	if cfg.Interval != defaultRescueInterval {
		t.Fatalf("expected Interval %v, got %v", defaultRescueInterval, cfg.Interval)
	}
	if cfg.BatchSize != defaultRescueBatchSize {
		t.Fatalf("expected BatchSize %d, got %d", defaultRescueBatchSize, cfg.BatchSize)
	}
	if cfg.RetryPolicy == nil {
		t.Fatal("expected RetryPolicy to be set")
	}
	if cfg.Logger == nil {
		t.Fatal("expected Logger to be set")
	}
}

func TestRescuerConfig_SetDefaults_NegativeValues(t *testing.T) {
	cfg := RescuerConfig{
		RescueAfter: -1,
		Interval:    -1,
		BatchSize:   -1,
	}
	cfg.setDefaults()

	if cfg.RescueAfter != defaultRescueAfter {
		t.Fatalf("expected default RescueAfter, got %v", cfg.RescueAfter)
	}
	if cfg.Interval != defaultRescueInterval {
		t.Fatalf("expected default Interval, got %v", cfg.Interval)
	}
	if cfg.BatchSize != defaultRescueBatchSize {
		t.Fatalf("expected default BatchSize, got %d", cfg.BatchSize)
	}
}

// ========== Rescuer Tests ==========

func TestRescuer_Name(t *testing.T) {
	r := NewRescuer(&mockRescuerRepo{}, RescuerConfig{})
	if r.Name() != "rescuer" {
		t.Fatalf("expected %q, got %q", "rescuer", r.Name())
	}
}

func TestRescuer_RunOnce_NoStuckTasks(t *testing.T) {
	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{{tasks: nil, err: nil}},
	}
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRescuer_RunOnce_RetryTasks(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.retryParams) != 2 {
		t.Fatalf("expected 2 retry calls, got %d", len(repo.retryParams))
	}
	if repo.retryParams[0].ID != 1 {
		t.Fatalf("expected retry task ID 1, got %d", repo.retryParams[0].ID)
	}
	if repo.retryParams[1].ID != 2 {
		t.Fatalf("expected retry task ID 2, got %d", repo.retryParams[1].ID)
	}
	if repo.retryParams[0].Message != "Stuck task rescued by Rescuer" {
		t.Fatalf("unexpected message: %q", repo.retryParams[0].Message)
	}
}

func TestRescuer_RunOnce_DiscardTasks(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.failCalls) != 2 {
		t.Fatalf("expected 2 fail calls, got %d", len(repo.failCalls))
	}
	if repo.failCalls[0].ids[0] != 10 {
		t.Fatalf("expected fail task ID 10, got %d", repo.failCalls[0].ids[0])
	}
	if repo.failCalls[1].ids[0] != 11 {
		t.Fatalf("expected fail task ID 11, got %d", repo.failCalls[1].ids[0])
	}
}

func TestRescuer_RunOnce_MixedTasks(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.retryParams) != 2 {
		t.Fatalf("expected 2 retries, got %d", len(repo.retryParams))
	}
	if len(repo.failCalls) != 1 {
		t.Fatalf("expected 1 discard, got %d", len(repo.failCalls))
	}
}

func TestRescuer_RunOnce_MultipleBatches(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if repo.stuckCallCount != 2 {
		t.Fatalf("expected 2 GetStuckTasks calls, got %d", repo.stuckCallCount)
	}
	if len(repo.retryParams) != 3 {
		t.Fatalf("expected 3 retries total, got %d", len(repo.retryParams))
	}
}

func TestRescuer_RunOnce_GetStuckError(t *testing.T) {
	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{
			{tasks: nil, err: errors.New("db error")},
		},
	}
	r := NewRescuer(repo, RescuerConfig{BatchSize: 100})

	err := r.runOnce(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRescuer_RunOnce_RetryError(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected error from RetryTask")
	}
}

func TestRescuer_RunOnce_FailError(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected error from FailTasks")
	}
}

func TestRescuer_RetryPolicy_Applied(t *testing.T) {
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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if len(repo.retryParams) != 1 {
		t.Fatalf("expected 1 retry call, got %d", len(repo.retryParams))
	}

	// BlockedTill should be approximately now + 10 seconds
	expectedBlockedTill := before.Add(10 * time.Second)
	diff := repo.retryParams[0].BlockedTill.Sub(expectedBlockedTill)
	if diff < -time.Second || diff > time.Second {
		t.Fatalf("BlockedTill out of range: expected ~%v, got %v", expectedBlockedTill, repo.retryParams[0].BlockedTill)
	}
}

func TestRescuer_StartStop(t *testing.T) {
	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{{tasks: nil}},
	}
	r := NewRescuer(repo, RescuerConfig{Interval: 50 * time.Millisecond})

	err := r.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	r.Stop()
}

func TestRescuer_Start_Idempotent(t *testing.T) {
	repo := &mockRescuerRepo{
		stuckResults: []stuckResult{{tasks: nil}},
	}
	r := NewRescuer(repo, RescuerConfig{Interval: time.Hour})

	err := r.Start(context.Background())
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	defer r.Stop()

	err = r.Start(context.Background())
	if err != nil {
		t.Fatalf("second start should return nil, got: %v", err)
	}
}

func TestRescuer_Stop_NotStarted(t *testing.T) {
	repo := &mockRescuerRepo{}
	r := NewRescuer(repo, RescuerConfig{})
	// Should not panic
	r.Stop()
}

// ========== Maintainer Tests ==========

func TestMaintainer_NewMaintainer_NilLogger(t *testing.T) {
	m := NewMaintainer(nil)
	if m.logger == nil {
		t.Fatal("expected default logger")
	}
}

func TestMaintainer_Start_StartsAllServices(t *testing.T) {
	svc1 := &mockService{name: "svc1"}
	svc2 := &mockService{name: "svc2"}
	m := NewMaintainer(nil, svc1, svc2)

	err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer m.Stop()

	// Wait for goroutines to start services
	time.Sleep(50 * time.Millisecond)

	if !svc1.started.Load() {
		t.Fatal("expected svc1 to be started")
	}
	if !svc2.started.Load() {
		t.Fatal("expected svc2 to be started")
	}
}

func TestMaintainer_Stop_StopsAllServices(t *testing.T) {
	svc1 := &mockService{name: "svc1"}
	svc2 := &mockService{name: "svc2"}
	m := NewMaintainer(nil, svc1, svc2)

	err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	m.Stop()

	if !svc1.stopped.Load() {
		t.Fatal("expected svc1 to be stopped")
	}
	if !svc2.stopped.Load() {
		t.Fatal("expected svc2 to be stopped")
	}
}

func TestMaintainer_Start_Idempotent(t *testing.T) {
	svc := &mockService{name: "svc"}
	m := NewMaintainer(nil, svc)

	err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	defer m.Stop()

	err = m.Start(context.Background())
	if err != nil {
		t.Fatalf("second start should return nil, got: %v", err)
	}
}

func TestMaintainer_Stop_NotStarted(t *testing.T) {
	m := NewMaintainer(nil)
	// Should not panic
	m.Stop()
}

func TestMaintainer_IsStarted(t *testing.T) {
	m := NewMaintainer(nil, &mockService{name: "svc"})

	if m.IsStarted() {
		t.Fatal("expected not started initially")
	}

	err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !m.IsStarted() {
		t.Fatal("expected started after Start()")
	}

	m.Stop()

	if m.IsStarted() {
		t.Fatal("expected not started after Stop()")
	}
}

func TestMaintainer_ServiceStartError(t *testing.T) {
	svc1 := &mockService{name: "failing", startErr: errors.New("start failed")}
	svc2 := &mockService{name: "working"}
	m := NewMaintainer(nil, svc1, svc2)

	err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("maintainer start should not fail: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	m.Stop()

	// Both services should have been attempted
	if !svc1.started.Load() {
		t.Fatal("expected failing service Start() to be called")
	}
	if !svc2.started.Load() {
		t.Fatal("expected working service to be started")
	}
}

func TestMaintainer_NoServices(t *testing.T) {
	m := NewMaintainer(nil)

	err := m.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	m.Stop()
}
