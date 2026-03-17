package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	defaultOTelEndpoint   = "localhost:4317"
	defaultMetricInterval = 5 * time.Second
)

// otelProviders holds initialized OTel SDK providers.
type otelProviders struct {
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *metric.MeterProvider
}

// Shutdown gracefully shuts down both providers.
func (p *otelProviders) Shutdown(ctx context.Context) {
	if p.TracerProvider != nil {
		_ = p.TracerProvider.Shutdown(ctx)
	}
	if p.MeterProvider != nil {
		_ = p.MeterProvider.Shutdown(ctx)
	}
}

// otelInit creates and registers OTel TracerProvider and MeterProvider
// configured to export via OTLP gRPC.
//
// Endpoint is read from OTEL_EXPORTER_OTLP_ENDPOINT env var,
// defaulting to localhost:4317.
func otelInit(ctx context.Context, serviceName string) (*otelProviders, error) {
	endpoint := defaultOTelEndpoint
	if v := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); v != "" {
		endpoint = v
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource: %w", err)
	}

	traceExp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	metricExp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(endpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create metric exporter: %w", err)
	}

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExp, metric.WithInterval(defaultMetricInterval))),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return &otelProviders{
		TracerProvider: tp,
		MeterProvider:  mp,
	}, nil
}
