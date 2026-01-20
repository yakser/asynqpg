package asynqpg

import "go.opentelemetry.io/otel/attribute"

// Attribute keys used across metrics and traces.
var (
	AttrTaskType  = attribute.Key("task_type")
	AttrStatus    = attribute.Key("status")
	AttrErrorType = attribute.Key("error_type")
)

// Bounded attribute values for status.
const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusRetried   = "retried"
	StatusSnoozed   = "snoozed"
)

// Bounded attribute values for error_type.
const (
	ErrorTypeHandler = "handler_error"
	ErrorTypeDB      = "db_error"
)
