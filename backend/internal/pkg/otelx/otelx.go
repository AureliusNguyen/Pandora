// Package otelx wires the OpenTelemetry SDK: a Prometheus-backed meter
// provider on a private registry and an optional OTLP/HTTP trace exporter.
package otelx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Options configures Setup.
type Options struct {
	ServiceName string
	// OTLPEndpoint is the OTEL_EXPORTER_OTLP_ENDPOINT base URL
	// (e.g. http://otel-collector:4318). Empty disables trace export.
	OTLPEndpoint string
}

// Telemetry holds the providers and the registry served on /metrics.
type Telemetry struct {
	Registry       *prometheus.Registry
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
}

// Setup builds providers, installs them as otel globals and returns them.
func Setup(ctx context.Context, opts Options) (*Telemetry, error) {
	if opts.ServiceName == "" {
		return nil, errors.New("otelx: ServiceName is required")
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", opts.ServiceName)),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("otelx: resource: %w", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	promExp, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("otelx: prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(promExp), sdkmetric.WithResource(res))

	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if opts.OTLPEndpoint != "" {
		url := strings.TrimRight(opts.OTLPEndpoint, "/") + "/v1/traces"
		exp, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(url))
		if err != nil {
			return nil, errors.Join(fmt.Errorf("otelx: otlp exporter: %w", err), mp.Shutdown(ctx))
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return &Telemetry{Registry: reg, TracerProvider: tp, MeterProvider: mp}, nil
}

// Shutdown flushes and stops both providers; bounded by ctx.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	return errors.Join(t.TracerProvider.Shutdown(ctx), t.MeterProvider.Shutdown(ctx))
}
