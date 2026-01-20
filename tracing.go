package asynqpg

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "asynqpg"

// NewTracer creates a tracer from the given TracerProvider.
// If tp is nil, the global OTel TracerProvider is used.
func NewTracer(tp trace.TracerProvider) trace.Tracer {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return tp.Tracer(tracerName)
}
