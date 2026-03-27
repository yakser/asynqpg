package leadership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/yakser/asynqpg/internal/leadership/mocks"
)

const testElectorGroup = "test-group"

// testSQLResult implements sql.Result for use in mock return values.
type testSQLResult struct {
	rowsAffected int64
	err          error
}

func (r *testSQLResult) LastInsertId() (int64, error) { return 0, nil }
func (r *testSQLResult) RowsAffected() (int64, error) { return r.rowsAffected, r.err }

// mockElected sets up the mock so that every election succeeds:
// delete expired (4 args) returns 0 rows, insert/upsert (6 args) returns 1 row.
func mockElected(db *mocks.DbExecer) {
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 1}, nil).Maybe()
}

// mockNotElected sets up the mock so that every election fails:
// delete expired (4 args) returns 0 rows, insert/upsert (6 args) returns 0 rows.
func mockNotElected(db *mocks.DbExecer) {
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
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

	db := mocks.NewDbExecer(t)
	e := NewElector(db, ElectorConfig{})

	require.False(t, e.IsLeader())
}

func TestElector_Start_Success(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	mockElected(db)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, e.Start(ctx))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	require.True(t, e.IsLeader())
}

func TestElector_Start_Idempotent(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	mockElected(db)

	e := NewElector(db, ElectorConfig{ElectInterval: time.Hour})

	ctx := context.Background()
	require.NoError(t, e.Start(ctx))
	defer e.Stop()

	require.NoError(t, e.Start(ctx))
}

func TestElector_Stop_NotStarted(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	e := NewElector(db, ElectorConfig{})

	// Should not panic
	e.Stop()
}

func TestElector_Stop_Idempotent(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	mockElected(db)

	e := NewElector(db, ElectorConfig{ElectInterval: time.Hour})

	require.NoError(t, e.Start(context.Background()))

	e.Stop()
	// Second stop should not panic
	e.Stop()
}

func TestElector_Stop_ResignsLeadership(t *testing.T) {
	t.Parallel()

	var queries sync.Map
	var queryIdx atomic.Int64

	db := mocks.NewDbExecer(t)
	// 4-arg calls: delete expired + resign
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, query string, _ ...any) {
			idx := queryIdx.Add(1)
			queries.Store(idx, query)
		}).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	// 6-arg calls: insert/upsert
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 1}, nil).Maybe()

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))

	time.Sleep(100 * time.Millisecond)
	require.True(t, e.IsLeader())

	e.Stop()

	require.False(t, e.IsLeader())

	// Verify that a resign query (DELETE ... WHERE name = $1 AND leader_id = $2) was called.
	found := false
	queries.Range(func(_, value any) bool {
		q := value.(string)
		if strings.Contains(q, "leader_id") && strings.Contains(q, "DELETE") {
			found = true
			return false
		}
		return true
	})
	require.True(t, found, "expected resign query to be called")
}

func TestElector_ElectsLeader_RowsAffected1(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	mockElected(db)

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

	db := mocks.NewDbExecer(t)
	mockNotElected(db)

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

	// Use a counter to change insert/upsert behavior over time.
	var insertCallCount atomic.Int64

	db := mocks.NewDbExecer(t)
	// 4-arg calls (delete expired + resign): always succeed with 0 rows.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	// 6-arg calls (insert/upsert): first call elected, subsequent not.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ ...any) (sql.Result, error) {
			n := insertCallCount.Add(1)
			if n == 1 {
				return &testSQLResult{rowsAffected: 1}, nil
			}
			return &testSQLResult{rowsAffected: 0}, nil
		}).Maybe()

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

	db := mocks.NewDbExecer(t)
	mockElected(db)

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

	// Use a counter: first delete succeeds, first insert wins, second delete errors.
	var fourArgCallCount atomic.Int64

	db := mocks.NewDbExecer(t)
	// 4-arg calls (delete expired): first succeeds, then errors.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ ...any) (sql.Result, error) {
			n := fourArgCallCount.Add(1)
			if n == 1 {
				return &testSQLResult{rowsAffected: 0}, nil
			}
			// Resign also matches 4-arg; returning error is fine for resign too.
			return nil, errors.New("connection refused")
		}).Maybe()
	// 6-arg calls (insert/upsert): always elected.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 1}, nil).Maybe()

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

	db := mocks.NewDbExecer(t)
	// 4-arg calls (delete expired): succeed.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	// 6-arg calls (insert/upsert): result.RowsAffected() returns error.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0, err: errors.New("rows affected error")}, nil).Maybe()

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

	var capturedName string
	var mu sync.Mutex

	db := mocks.NewDbExecer(t)
	// 4-arg calls (delete expired): capture the name arg.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			if capturedName == "" && len(args) >= 1 {
				if name, ok := args[0].(string); ok {
					capturedName = name
				}
			}
		}).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	// 6-arg calls (insert/upsert).
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 1}, nil).Maybe()

	e := NewElector(db, ElectorConfig{
		ClientID:      "test-client",
		Name:          testElectorGroup,
		ElectInterval: 50 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	require.Equal(t, testElectorGroup, capturedName)
	mu.Unlock()
}

func TestElector_ElectionSQL_InsertOnConflict(t *testing.T) {
	t.Parallel()

	var capturedInsertArgs []any
	var mu sync.Mutex

	db := mocks.NewDbExecer(t)
	// 4-arg calls (delete expired).
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	// 6-arg calls (insert/upsert): capture args.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			if capturedInsertArgs == nil {
				capturedInsertArgs = args
			}
		}).
		Return(&testSQLResult{rowsAffected: 1}, nil).Maybe()

	e := NewElector(db, ElectorConfig{
		ClientID:      "test-client",
		Name:          testElectorGroup,
		ElectInterval: 50 * time.Millisecond,
		TTL:           150 * time.Millisecond,
	})

	require.NoError(t, e.Start(context.Background()))
	defer e.Stop()

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	require.Len(t, capturedInsertArgs, 4, "insert: expected 4 args (name, client_id, now, expires_at)")
	require.Equal(t, testElectorGroup, capturedInsertArgs[0])
	require.Equal(t, "test-client", capturedInsertArgs[1])
	mu.Unlock()
}

func TestElector_Subscribe_InitialState(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
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

	db := mocks.NewDbExecer(t)
	mockElected(db)

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

	var insertCallCount atomic.Int64

	db := mocks.NewDbExecer(t)
	// 4-arg calls (delete expired + resign): always succeed.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Maybe()
	// 6-arg calls (insert/upsert): first elected, then not.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ ...any) (sql.Result, error) {
			n := insertCallCount.Add(1)
			if n == 1 {
				return &testSQLResult{rowsAffected: 1}, nil
			}
			return &testSQLResult{rowsAffected: 0}, nil
		}).Maybe()

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

	db := mocks.NewDbExecer(t)
	mockElected(db)

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

	db := mocks.NewDbExecer(t)
	mockElected(db)

	e := NewElector(db, ElectorConfig{
		ElectInterval: 50 * time.Millisecond,
	})

	ch := e.Subscribe()
	// Don't read from ch -- channel buffer (1) has initial value
	// setLeader should not block even if channel is full

	require.NoError(t, e.Start(context.Background()))

	// Wait for election -- should not deadlock
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

	db := mocks.NewDbExecer(t)
	mockElected(db)

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

	var fourArgCallCount atomic.Int64

	db := mocks.NewDbExecer(t)
	// 4-arg calls: first delete succeeds, then resign returns error.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ string, _ ...any) (sql.Result, error) {
			n := fourArgCallCount.Add(1)
			if n == 1 {
				return &testSQLResult{rowsAffected: 0}, nil
			}
			return nil, errors.New("connection closed")
		}).Maybe()
	// 6-arg calls: elected.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 1}, nil).Maybe()

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

	db := mocks.NewDbExecer(t)
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("delete error")).Once()

	e := NewElector(db, ElectorConfig{})

	elected, err := e.attemptElect(context.Background())

	require.Error(t, err)
	require.False(t, elected)
}

func TestAttemptElect_InsertError(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	// Delete expired succeeds.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Once()
	// Insert fails.
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("insert error")).Once()

	e := NewElector(db, ElectorConfig{})

	elected, err := e.attemptElect(context.Background())

	require.Error(t, err)
	require.False(t, elected)
}

func TestAttemptElect_Success(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Once()
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 1}, nil).Once()

	e := NewElector(db, ElectorConfig{ClientID: "test"})

	elected, err := e.attemptElect(context.Background())

	require.NoError(t, err)
	require.True(t, elected)
}

func TestAttemptElect_NotElected(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Once()
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&testSQLResult{rowsAffected: 0}, nil).Once()

	e := NewElector(db, ElectorConfig{ClientID: "test"})

	elected, err := e.attemptElect(context.Background())

	require.NoError(t, err)
	require.False(t, elected)
}

func TestResign_Success(t *testing.T) {
	t.Parallel()

	var capturedArgs []any
	var mu sync.Mutex

	db := mocks.NewDbExecer(t)
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ string, args ...any) {
			mu.Lock()
			capturedArgs = args
			mu.Unlock()
		}).
		Return(&testSQLResult{rowsAffected: 1}, nil).Once()

	e := NewElector(db, ElectorConfig{ClientID: "test-client", Name: testElectorGroup})
	e.isLeader.Store(true)

	err := e.resign(context.Background())

	require.NoError(t, err)
	require.False(t, e.IsLeader())

	mu.Lock()
	require.Equal(t, testElectorGroup, capturedArgs[0])
	require.Equal(t, "test-client", capturedArgs[1])
	mu.Unlock()
}

func TestResign_Error(t *testing.T) {
	t.Parallel()

	db := mocks.NewDbExecer(t)
	db.EXPECT().
		ExecContext(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, fmt.Errorf("db error")).Once()

	e := NewElector(db, ElectorConfig{ClientID: "test"})

	err := e.resign(context.Background())

	require.Error(t, err)
}
