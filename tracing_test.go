package asynqpg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestNewTracer_WithProvider(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer tp.Shutdown(context.TODO())

	tracer := NewTracer(tp)
	require.NotNil(t, tracer)
}

func TestNewTracer_NilProvider_UsesGlobal(t *testing.T) {
	tracer := NewTracer(nil)
	assert.NotNil(t, tracer)
}
