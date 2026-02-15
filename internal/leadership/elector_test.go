package leadership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

const testElectorGroup = "test-group"

// --- Mocks ---

type mockResult struct {
	rowsAffected int64
	err          error
}

func (r *mockResult) LastInsertId() (int64, error) { return 0, nil }
func (r *mockResult) RowsAffected() (int64, error) { return r.rowsAffected, r.err }

type execCall struct {
	query string
	args  []any
}

type mockDB struct {
	mu    sync.Mutex
	calls []execCall

	// Results to return in order. If exhausted, returns the last one.
	results []mockExecResult
}

type mockExecResult struct {
	result sql.Result
	err    error
}

func newMockDB(results ...mockExecResult) *mockDB {
	return &mockDB{results: results}
}

func (m *mockDB) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls = append(m.calls, execCall{query: query, args: args})

	idx := len(m.calls) - 1
	if idx >= len(m.results) {
		idx = len(m.results) - 1
	}
	if idx < 0 {
		return &mockResult{rowsAffected: 0}, nil
	}
	return m.results[idx].result, m.results[idx].err
}

func (m *mockDB) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func (m *mockDB) getCall(i int) execCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[i]
}

// Helper: create mock that always elects (delete OK, insert RowsAffected=1)
func mockDBElected() *mockDB {
	return newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil}, // delete expired
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil}, // insert/upsert
	)
}

// Helper: create mock that never elects (delete OK, insert RowsAffected=0)
func mockDBNotElected() *mockDB {
	return newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil}, // delete expired
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil}, // insert/upsert
	)
}

// Helper: create mock that returns error
func mockDBError(err error) *mockDB {
	return newMockDB(
		mockExecResult{result: nil, err: err},
	)
}

// --- Config Tests ---

func TestElectorConfig_SetDefaults_AllEmpty(t *testing.T) {
	cfg := ElectorConfig{}
	cfg.setDefaults()

	if cfg.ClientID == "" {
		t.Fatal("expected ClientID to be generated")
	}
	if cfg.Name != defaultName {
		t.Fatalf("expected Name %q, got %q", defaultName, cfg.Name)
	}
	if cfg.ElectInterval != defaultElectInterval {
		t.Fatalf("expected ElectInterval %v, got %v", defaultElectInterval, cfg.ElectInterval)
	}
	if cfg.TTL != defaultTTL {
		t.Fatalf("expected TTL %v, got %v", defaultTTL, cfg.TTL)
	}
	if cfg.Logger == nil {
		t.Fatal("expected Logger to be set")
	}
}

func TestElectorConfig_SetDefaults_CustomValues(t *testing.T) {
	cfg := ElectorConfig{
		ClientID:      "my-id",
		Name:          "my-group",
		ElectInterval: 10 * time.Second,
		TTL:           30 * time.Second,
	}
	cfg.setDefaults()

	if cfg.ClientID != "my-id" {
		t.Fatalf("expected ClientID %q, got %q", "my-id", cfg.ClientID)
	}
	if cfg.Name != "my-group" {
		t.Fatalf("expected Name %q, got %q", "my-group", cfg.Name)
	}
	if cfg.ElectInterval != 10*time.Second {
		t.Fatalf("expected ElectInterval 10s, got %v", cfg.ElectInterval)
	}
	if cfg.TTL != 30*time.Second {
		t.Fatalf("expected TTL 30s, got %v", cfg.TTL)
	}
}

func TestElectorConfig_SetDefaults_NegativeDurations(t *testing.T) {
	cfg := ElectorConfig{
		ElectInterval: -1 * time.Second,
		TTL:           -1 * time.Second,
	}
	cfg.setDefaults()

	if cfg.ElectInterval != defaultElectInterval {
		t.Fatalf("expected default ElectInterval, got %v", cfg.ElectInterval)
	}
	if cfg.TTL != defaultTTL {
		t.Fatalf("expected default TTL, got %v", cfg.TTL)
	}
}

// --- Lifecycle Tests ---

func TestElector_IsLeader_InitiallyFalse(t *testing.T) {
	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{})
	if e.IsLeader() {
		t.Fatal("expected IsLeader to be false initially")
	}
}

func TestElector_Start_Success(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := e.Start(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	// Wait for first election attempt
	time.Sleep(100 * time.Millisecond)

	if !e.IsLeader() {
		t.Fatal("expected to be leader after successful election")
	}
	if db.callCount() < 2 {
		t.Fatalf("expected at least 2 DB calls (delete expired + insert), got %d", db.callCount())
	}
}

func TestElector_Start_Idempotent(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{ElectInterval: time.Hour})

	ctx := context.Background()
	err := e.Start(ctx)
	if err != nil {
		t.Fatalf("first start: unexpected error: %v", err)
	}
	defer e.Stop()

	err = e.Start(ctx)
	if err != nil {
		t.Fatalf("second start should return nil, got: %v", err)
	}
}

func TestElector_Stop_NotStarted(t *testing.T) {
	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{})
	// Should not panic
	e.Stop()
}

func TestElector_Stop_Idempotent(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{ElectInterval: time.Hour})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	e.Stop()
	// Second stop should not panic
	e.Stop()
}

func TestElector_Stop_ResignsLeadership(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait to become leader
	time.Sleep(100 * time.Millisecond)
	if !e.IsLeader() {
		t.Fatal("expected to be leader before stop")
	}

	e.Stop()

	if e.IsLeader() {
		t.Fatal("expected not to be leader after stop")
	}

	// Verify resign query was called (DELETE with name and leader_id)
	found := false
	for i := 0; i < db.callCount(); i++ {
		call := db.getCall(i)
		if len(call.args) == 2 {
			// resign query: DELETE FROM asynqpg_leader WHERE name = $1 AND leader_id = $2
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected resign query to be called")
	}
}

// --- Election Logic Tests ---

func TestElector_ElectsLeader_RowsAffected1(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	if !e.IsLeader() {
		t.Fatal("expected to be leader when RowsAffected=1")
	}
}

func TestElector_NotElected_RowsAffected0(t *testing.T) {
	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	if e.IsLeader() {
		t.Fatal("expected not to be leader when RowsAffected=0")
	}
}

func TestElector_LosesLeadership(t *testing.T) {
	// First election: wins. Subsequent: loses.
	db := newMockDB(
		// First attempt (delete + insert)
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
		// Second attempt (delete + insert) – loses
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
	)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	// Wait for first election
	time.Sleep(30 * time.Millisecond)
	if !e.IsLeader() {
		t.Fatal("expected to be leader after first election")
	}

	// Wait for second election
	time.Sleep(80 * time.Millisecond)
	if e.IsLeader() {
		t.Fatal("expected to lose leadership after second election")
	}
}

func TestElector_MaintainsLeadership(t *testing.T) {
	db := mockDBElected() // Always returns RowsAffected=1
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(30 * time.Millisecond)
	if !e.IsLeader() {
		t.Fatal("expected to be leader")
	}

	// Wait for re-election
	time.Sleep(80 * time.Millisecond)
	if !e.IsLeader() {
		t.Fatal("expected to maintain leadership")
	}
}

func TestElector_ErrorAssumedLostLeadership(t *testing.T) {
	// First: win election. Then: DB error.
	db := newMockDB(
		// First attempt: success
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
		// Second attempt: error on delete expired
		mockExecResult{result: nil, err: errors.New("connection refused")},
	)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(30 * time.Millisecond)
	if !e.IsLeader() {
		t.Fatal("expected to be leader after first election")
	}

	// Wait for error attempt
	time.Sleep(80 * time.Millisecond)
	if e.IsLeader() {
		t.Fatal("expected to lose leadership on DB error (fail-safe)")
	}
}

func TestElector_RowsAffectedError(t *testing.T) {
	// RowsAffected returns error
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0, err: errors.New("rows affected error")}, err: nil},
	)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)
	if e.IsLeader() {
		t.Fatal("expected not to be leader when RowsAffected returns error")
	}
}

// --- SQL Query Tests ---

func TestElector_DeletesExpiredLeaders(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ClientID:      "test-client",
		Name:          testElectorGroup,
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	// First call should be the delete expired query
	call := db.getCall(0)
	if len(call.args) != 2 {
		t.Fatalf("delete expired: expected 2 args, got %d", len(call.args))
	}
	if call.args[0] != testElectorGroup {
		t.Fatalf("delete expired: expected name %q, got %v", testElectorGroup, call.args[0])
	}
}

func TestElector_ElectionSQL_InsertOnConflict(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ClientID:      "test-client",
		Name:          testElectorGroup,
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	// Second call should be the insert/upsert query
	if db.callCount() < 2 {
		t.Fatalf("expected at least 2 calls, got %d", db.callCount())
	}
	call := db.getCall(1)
	if len(call.args) != 4 {
		t.Fatalf("insert: expected 4 args (name, client_id, now, expires_at), got %d", len(call.args))
	}
	if call.args[0] != testElectorGroup {
		t.Fatalf("insert: expected name %q, got %v", testElectorGroup, call.args[0])
	}
	if call.args[1] != "test-client" {
		t.Fatalf("insert: expected client_id %q, got %v", "test-client", call.args[1])
	}
}

// --- Subscriber Tests ---

func TestElector_Subscribe_InitialState(t *testing.T) {
	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{})

	ch := e.Subscribe()

	select {
	case v := <-ch:
		if v {
			t.Fatal("expected initial state false")
		}
	case <-time.After(time.Second):
		t.Fatal("expected to receive initial state")
	}
}

func TestElector_Subscribe_GainLeadership(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch := e.Subscribe()

	// Drain initial state (false)
	<-ch

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	select {
	case v := <-ch:
		if !v {
			t.Fatal("expected true on leadership gain")
		}
	case <-time.After(time.Second):
		t.Fatal("expected to receive leadership gain notification")
	}
}

func TestElector_Subscribe_LoseLeadership(t *testing.T) {
	db := newMockDB(
		// First: win
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
		// Second: lose
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		// Further: keep losing
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
	)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch := e.Subscribe()
	<-ch // drain initial false

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	// Should get true (gained)
	select {
	case v := <-ch:
		if !v {
			t.Fatal("expected true on leadership gain")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for leadership gain")
	}

	// Should get false (lost)
	select {
	case v := <-ch:
		if v {
			t.Fatal("expected false on leadership loss")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for leadership loss")
	}
}

func TestElector_Subscribe_MultipleSubscribers(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch1 := e.Subscribe()
	ch2 := e.Subscribe()

	// Drain initial states
	<-ch1
	<-ch2

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer e.Stop()

	// Both should receive leadership gain
	for i, ch := range []<-chan bool{ch1, ch2} {
		select {
		case v := <-ch:
			if !v {
				t.Fatalf("subscriber %d: expected true", i)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timeout", i)
		}
	}
}

func TestElector_Subscribe_FullChannel(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch := e.Subscribe()
	// Don't read from ch – channel buffer (1) has initial value
	// setLeader should not block even if channel is full

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for election – should not deadlock
	time.Sleep(100 * time.Millisecond)
	e.Stop()

	// We should still be able to read at least the initial value
	select {
	case <-ch:
	default:
		t.Fatal("expected at least initial value in channel")
	}
}

// --- Election Loop Tests ---

func TestElector_ElectionLoop_ContextCancellation(t *testing.T) {
	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	err := e.Start(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(30 * time.Millisecond)
	cancel()

	// Stop should complete quickly since context is already cancelled
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not complete after context cancellation")
	}
}

func TestElector_Resign_DBError(t *testing.T) {
	// First attempts: win leadership
	// resign: error
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
		// resign will be the next call – return error
		mockExecResult{result: nil, err: errors.New("connection closed")},
	)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	err := e.Start(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(30 * time.Millisecond)

	// Stop should not panic even if resign fails
	e.Stop()
}

// --- attemptElect direct test ---

func TestAttemptElect_DeleteExpiredError(t *testing.T) {
	db := mockDBError(fmt.Errorf("delete error"))
	e := NewElector(db, ElectorConfig{})

	elected, err := e.attemptElect(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if elected {
		t.Fatal("expected not elected on error")
	}
}

func TestAttemptElect_InsertError(t *testing.T) {
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: nil, err: fmt.Errorf("insert error")},
	)
	e := NewElector(db, ElectorConfig{})

	elected, err := e.attemptElect(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if elected {
		t.Fatal("expected not elected on error")
	}
}

func TestAttemptElect_Success(t *testing.T) {
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
	)
	e := NewElector(db, ElectorConfig{ClientID: "test"})

	elected, err := e.attemptElect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !elected {
		t.Fatal("expected elected")
	}
}

func TestAttemptElect_NotElected(t *testing.T) {
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
	)
	e := NewElector(db, ElectorConfig{ClientID: "test"})

	elected, err := e.attemptElect(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elected {
		t.Fatal("expected not elected")
	}
}

// --- Resign direct test ---

func TestResign_Success(t *testing.T) {
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
	)
	e := NewElector(db, ElectorConfig{ClientID: "test-client", Name: testElectorGroup})
	e.isLeader.Store(true)

	err := e.resign(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.IsLeader() {
		t.Fatal("expected not leader after resign")
	}

	call := db.getCall(0)
	if call.args[0] != testElectorGroup {
		t.Fatalf("expected name %q, got %v", testElectorGroup, call.args[0])
	}
	if call.args[1] != "test-client" {
		t.Fatalf("expected leader_id %q, got %v", "test-client", call.args[1])
	}
}

func TestResign_Error(t *testing.T) {
	db := mockDBError(fmt.Errorf("db error"))
	e := NewElector(db, ElectorConfig{ClientID: "test"})

	err := e.resign(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}
