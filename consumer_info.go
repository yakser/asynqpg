package asynqpg

import "time"

// ConsumerInfo is a read-only snapshot of a running consumer's configuration and
// live worker utilization. It is produced in-process (the consumer reads its own
// state) and is intended for inspection surfaces such as the web dashboard.
type ConsumerInfo struct {
	// ID is the unique identifier of the consumer instance (its leader-election ClientID).
	ID string
	// Running is true after Start succeeds until graceful shutdown completes.
	Running bool
	// IsLeader reports whether this instance currently holds leadership.
	// Always false when maintenance is disabled.
	IsLeader bool
	// MaintenanceEnabled reports whether this instance participates in leader election
	// and runs maintenance services (rescuer, cleaner) when elected.
	MaintenanceEnabled bool
	// StartedAt is when the consumer was started. Zero if not started yet.
	StartedAt time.Time

	FetchInterval   time.Duration
	JanitorInterval time.Duration
	StuckThreshold  time.Duration
	ShutdownTimeout time.Duration

	// TaskTypes lists the registered task types and their worker pools.
	TaskTypes []ConsumerTaskTypeInfo
}

// ConsumerTaskTypeInfo describes a single registered task type and its worker pool.
type ConsumerTaskTypeInfo struct {
	Type string
	// Workers is the configured size of the worker pool for this task type.
	Workers int
	// Busy is the number of workers currently processing a task (live snapshot).
	Busy int
	// MaxAttempts is the default attempt count applied when enqueueing is not explicit.
	MaxAttempts int
	// Timeout is the per-task execution timeout for this task type.
	Timeout time.Duration
}
