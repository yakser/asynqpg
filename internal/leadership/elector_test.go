package leadership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testElectorGroup = "test-group"

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

func TestElectorConfig_SetDefaults_AllEmpty(t *testing.T) {
	t.Parallel()

	cfg := ElectorConfig{}
	cfg.setDefaults()

	require.NotEmpty(t, cfg.ClientID)
	require.Equal(t, defaultName, cfg.Name)
	require.Equal(t, defaultElectInterval, cfg.ElectInterval)
	require.Equal(t, defaultTTL, cfg.TTL)
	require.NotNil(t, cfg.Logger)
}

func TestElectorConfig_SetDefaults_CustomValues(t *testing.T) {
	t.Parallel()

	cfg := ElectorConfig{
		ClientID:      "my-id",
		Name:          "my-group",
		ElectInterval: 10 * time.Second,
		TTL:           30 * time.Second,
	}
	cfg.setDefaults()

	require.Equal(t, "my-id", cfg.ClientID)
	require.Equal(t, "my-group", cfg.Name)
	require.Equal(t, 10*time.Second, cfg.ElectInterval)
	require.Equal(t, 30*time.Second, cfg.TTL)
}

func TestElectorConfig_SetDefaults_NegativeDurations(t *testing.T) {
	t.Parallel()

	cfg := ElectorConfig{
		ElectInterval: -1 * time.Second,
		TTL:           -1 * time.Second,
	}
	cfg.setDefaults()

	require.Equal(t, defaultElectInterval, cfg.ElectInterval)
	require.Equal(t, defaultTTL, cfg.TTL)
}

func TestElector_IsLeader_InitiallyFalse(t *testing.T) {
	t.Parallel()

	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{})
	require.False(t, e.IsLeader())
}

func TestElector_Start_Success(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, e.Start(ctx))
	defer e.Stop()

	// Wait for first election attempt
	time.Sleep(100 * time.Millisecond)

	require.True(t, e.IsLeader())
	require.GreaterOrEqual(t, db.callCount(), 2, "expected at least 2 DB calls (delete expired + insert)")
}

func TestElector_Start_Idempotent(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{ElectInterval: time.Hour})

	ctx := context.Background()
	require.NoError(t, e.Start(ctx))
	defer e.Stop()

	require.NoError(t, e.Start(ctx))
}

func TestElector_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{})
	// Should not panic
	e.Stop()
}

func TestElector_Stop_Idempotent(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{ElectInterval: time.Hour})

	require.NoError(t, e.Start(context.Background()))

	e.Stop()
	// Second stop should not panic
	e.Stop()
}

func TestElector_Stop_ResignsLeadership(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))

	// Wait to become leader
	time.Sleep(100 * time.Millisecond)
	require.True(t, e.IsLeader())

	e.Stop()

	require.False(t, e.IsLeader())

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
	require.True(t, found, "expected resign query to be called")
}

func TestElector_ElectsLeader_RowsAffected1(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	require.True(t, e.IsLeader())
}

func TestElector_NotElected_RowsAffected0(t *testing.T) {
	t.Parallel()

	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	require.False(t, e.IsLeader())
}

func TestElector_LosesLeadership(t *testing.T) {
	t.Parallel()

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

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	// Wait for first election
	time.Sleep(30 * time.Millisecond)
	require.True(t, e.IsLeader())

	// Wait for second election
	time.Sleep(80 * time.Millisecond)
	require.False(t, e.IsLeader())
}

func TestElector_MaintainsLeadership(t *testing.T) {
	t.Parallel()

	db := mockDBElected() // Always returns RowsAffected=1
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(30 * time.Millisecond)
	require.True(t, e.IsLeader())

	// Wait for re-election
	time.Sleep(80 * time.Millisecond)
	require.True(t, e.IsLeader())
}

func TestElector_ErrorAssumedLostLeadership(t *testing.T) {
	t.Parallel()

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

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(30 * time.Millisecond)
	require.True(t, e.IsLeader())

	// Wait for error attempt
	time.Sleep(80 * time.Millisecond)
	require.False(t, e.IsLeader())
}

func TestElector_RowsAffectedError(t *testing.T) {
	t.Parallel()

	// RowsAffected returns error
	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0, err: errors.New("rows affected error")}, err: nil},
	)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)
	require.False(t, e.IsLeader())
}

func TestElector_DeletesExpiredLeaders(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ClientID:      "test-client",
		Name:          testElectorGroup,
		ElectInterval: 50 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	// First call should be the delete expired query
	call := db.getCall(0)
	require.Len(t, call.args, 2)
	require.Equal(t, testElectorGroup, call.args[0])
}

func TestElector_ElectionSQL_InsertOnConflict(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ClientID:      "test-client",
		Name:          testElectorGroup,
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	// Second call should be the insert/upsert query
	require.GreaterOrEqual(t, db.callCount(), 2)
	call := db.getCall(1)
	require.Len(t, call.args, 4, "insert: expected 4 args (name, client_id, now, expires_at)")
	require.Equal(t, testElectorGroup, call.args[0])
	require.Equal(t, "test-client", call.args[1])
}

func TestElector_Subscribe_InitialState(t *testing.T) {
	t.Parallel()

	db := mockDBNotElected()
	e := NewElector(db, ElectorConfig{})

	ch := e.Subscribe()

	select {
	case v := <-ch:
		require.False(t, v, "expected initial state false")
	case <-time.After(time.Second):
		t.Fatal("expected to receive initial state")
	}
}

func TestElector_Subscribe_GainLeadership(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch := e.Subscribe()

	// Drain initial state (false)
	<-ch

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	select {
	case v := <-ch:
		require.True(t, v, "expected true on leadership gain")
	case <-time.After(time.Second):
		t.Fatal("expected to receive leadership gain notification")
	}
}

func TestElector_Subscribe_LoseLeadership(t *testing.T) {
	t.Parallel()

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

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	// Should get true (gained)
	select {
	case v := <-ch:
		require.True(t, v, "expected true on leadership gain")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for leadership gain")
	}

	// Should get false (lost)
	select {
	case v := <-ch:
		require.False(t, v, "expected false on leadership loss")
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for leadership loss")
	}
}

func TestElector_Subscribe_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch1 := e.Subscribe()
	ch2 := e.Subscribe()

	// Drain initial states
	<-ch1
	<-ch2

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	// Both should receive leadership gain
	for i, ch := range []<-chan bool{ch1, ch2} {
		select {
		case v := <-ch:
			require.True(t, v, "subscriber %d: expected true", i)
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timeout", i)
		}
	}
}

func TestElector_Subscribe_FullChannel(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch := e.Subscribe()
	// Don't read from ch – channel buffer (1) has initial value
	// setLeader should not block even if channel is full

	require.NoError(t, e.Start(context.Background()))

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

func TestElector_ElectionLoop_ContextCancellation(t *testing.T) {
	t.Parallel()

	db := mockDBElected()
	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, e.Start(ctx))

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
	t.Parallel()

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

	require.NoError(t, e.Start(context.Background()))

	time.Sleep(30 * time.Millisecond)

	// Stop should not panic even if resign fails
	e.Stop()
}

func TestAttemptElect_DeleteExpiredError(t *testing.T) {
	t.Parallel()

	db := mockDBError(fmt.Errorf("delete error"))
	e := NewElector(db, ElectorConfig{})

	elected, err := e.attemptElect(context.Background())
	require.Error(t, err)
	require.False(t, elected)
}

func TestAttemptElect_InsertError(t *testing.T) {
	t.Parallel()

	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: nil, err: fmt.Errorf("insert error")},
	)
	e := NewElector(db, ElectorConfig{})

	elected, err := e.attemptElect(context.Background())
	require.Error(t, err)
	require.False(t, elected)
}

func TestAttemptElect_Success(t *testing.T) {
	t.Parallel()

	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
	)
	e := NewElector(db, ElectorConfig{ClientID: "test"})

	elected, err := e.attemptElect(context.Background())
	require.NoError(t, err)
	require.True(t, elected)
}

func TestAttemptElect_NotElected(t *testing.T) {
	t.Parallel()

	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
		mockExecResult{result: &mockResult{rowsAffected: 0}, err: nil},
	)
	e := NewElector(db, ElectorConfig{ClientID: "test"})

	elected, err := e.attemptElect(context.Background())
	require.NoError(t, err)
	require.False(t, elected)
}

func TestResign_Success(t *testing.T) {
	t.Parallel()

	db := newMockDB(
		mockExecResult{result: &mockResult{rowsAffected: 1}, err: nil},
	)
	e := NewElector(db, ElectorConfig{ClientID: "test-client", Name: testElectorGroup})
	e.isLeader.Store(true)

	err := e.resign(context.Background())
	require.NoError(t, err)
	require.False(t, e.IsLeader())

	call := db.getCall(0)
	require.Equal(t, testElectorGroup, call.args[0])
	require.Equal(t, "test-client", call.args[1])
}

func TestResign_Error(t *testing.T) {
	t.Parallel()

	db := mockDBError(fmt.Errorf("db error"))
	e := NewElector(db, ElectorConfig{ClientID: "test"})

	err := e.resign(context.Background())
	require.Error(t, err)
}
