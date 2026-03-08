package ui

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// mockPool implements asynqpg.Pool for testing.
type mockPool struct {
	pingErr error

	// For ExecContext mock
	execResult sql.Result
	execErr    error

	// For SelectContext mock
	selectFn func(dest any) error

	// For GetContext mock
	getFn func(dest any) error
}

func (m *mockPool) PingContext(_ context.Context) error {
	return m.pingErr
}

func (m *mockPool) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	if m.execResult != nil || m.execErr != nil {
		return m.execResult, m.execErr
	}
	return mockResult{}, nil
}

func (m *mockPool) SelectContext(_ context.Context, dest any, _ string, _ ...any) error {
	if m.selectFn != nil {
		return m.selectFn(dest)
	}
	return nil
}

func (m *mockPool) GetContext(_ context.Context, dest any, _ string, _ ...any) error {
	if m.getFn != nil {
		return m.getFn(dest)
	}
	return nil
}

func (m *mockPool) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, nil //nolint:nilnil
}

func (m *mockPool) QueryRowContext(_ context.Context, _ string, _ ...any) *sql.Row {
	return nil
}

// mockResult implements sql.Result for testing.
type mockResult struct {
	lastID   int64
	affected int64
}

func (r mockResult) LastInsertId() (int64, error) { return r.lastID, nil }
func (r mockResult) RowsAffected() (int64, error) { return r.affected, nil }

// mockExecResult wraps a rows-affected value and optional error.
type mockExecResult struct {
	affected    int64
	affectedErr error
}

func (r mockExecResult) LastInsertId() (int64, error) { return 0, nil }

func (r mockExecResult) RowsAffected() (int64, error) {
	return r.affected, r.affectedErr
}

// Ensure mockExecResult satisfies driver.Result at compile time.
var _ driver.Result = mockExecResult{}
