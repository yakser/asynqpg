package ui

import (
	"database/sql/driver"
)

// testExecResult wraps a rows-affected value and optional error.
type testExecResult struct {
	affected    int64
	affectedErr error
}

func (r testExecResult) LastInsertId() (int64, error) { return 0, nil }

func (r testExecResult) RowsAffected() (int64, error) {
	return r.affected, r.affectedErr
}

// Ensure testExecResult satisfies driver.Result at compile time.
var _ driver.Result = testExecResult{}
