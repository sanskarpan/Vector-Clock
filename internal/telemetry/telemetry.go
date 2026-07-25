// Package telemetry configures OpenTelemetry tracing for the Vector Clock
// Lab backend.
//
// Init builds a TracerProvider with one of three exporters:
//
//   - "stdout": writes JSON-encoded spans to stderr (useful for local dev).
//   - "otlp":   ships spans to an OTLP/HTTP collector (e.g. Jaeger, Tempo).
//   - "none":   installs a no-op provider so tracing has zero overhead.
//
// The "none" path is the default and MUST stay zero-cost. Spans obtained
// from the package Tracer are no-op spans when no SDK is configured; the
// otel package guarantees that creating-and-ending a no-op span is a
// constant-time operation that does not allocate.
package telemetry

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Config is the configuration for Init.
//
// Exporter selects the span sink: "stdout" | "otlp" | "none". Empty is
// treated as "none". OTLPEndpoint is only used when Exporter == "otlp".
// Insecure disables TLS for the OTLP client. SampleRatio is clamped to
// [0, 1]; 0 disables sampling, 1 samples every root span.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Exporter       string
	OTLPEndpoint   string
	SampleRatio    float64
	Insecure       bool
}

// TracerName is the instrumentation scope used by application code.
const TracerName = "github.com/DistributedClocks/vectorclock-system"

// Init configures the global TracerProvider and W3C TraceContext +
// Baggage propagator. Returns a shutdown function the caller must defer.
//
// The shutdown function always succeeds as a no-op when Exporter == "none".
// It is safe to call shutdown more than once (subsequent calls are no-ops).
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	exporter := strings.ToLower(strings.TrimSpace(cfg.Exporter))
	if exporter == "" {
		exporter = "none"
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "vectorclock-server"
	}
	if cfg.SampleRatio < 0 {
		cfg.SampleRatio = 0
	}
	if cfg.SampleRatio > 1 {
		cfg.SampleRatio = 1
	}

	// Register the W3C TraceContext + Baggage propagator globally so any
	// out-of-band code that uses otel.GetTextMapPropagator() picks it up.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	switch exporter {
	case "none":
		// Install a no-op provider. Spans are still creatable but
		// discarded without allocation; propagator is already set so
		// traceparent extraction from inbound requests still works.
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	case "stdout", "otlp":
		// fall through to the real provider
	default:
		return nil, fmt.Errorf("telemetry: unknown exporter %q (want stdout|otlp|none)", cfg.Exporter)
	}

	res, err := buildResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("telemetry: build resource: %w", err)
	}

	spanExporter, closer, err := buildExporter(ctx, exporter, cfg)
	if err != nil {
		return nil, fmt.Errorf("telemetry: build exporter: %w", err)
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(spanExporter,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(512),
		),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)

	shutdown := func(ctx context.Context) error {
		// Shutdown the SDK first so the in-flight queue is flushed
		// through our exporter; then close the exporter.
		shutdownErr := tp.Shutdown(ctx)
		if closer != nil {
			if cerr := closer.Close(); cerr != nil && shutdownErr == nil {
				shutdownErr = cerr
			}
		}
		return shutdownErr
	}
	return shutdown, nil
}

// buildResource attaches service.* attributes + an instance id. The
// instance id is a random UUID per process so multiple pods / replicas
// are distinguishable in trace UIs. The base resource from resource.New
// is merged in so SDK-detected attrs (telemetry.sdk.*) are also present.
func buildResource(cfg Config) (*resource.Resource, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	// resource.Default() picks up OTEL_RESOURCE_ATTRIBUTES + SDK attrs.
	base := resource.Default()
	svc := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.ServiceInstanceID(uuid.NewString()),
		semconv.HostName(host),
	)
	return resource.Merge(base, svc)
}

// buildExporter returns a span exporter and an optional closer. The
// closer is nil for stdout; otlptracehttp's underlying client implements
// io.Closer and is returned so shutdown can drain the HTTP client.
func buildExporter(ctx context.Context, kind string, cfg Config) (sdktrace.SpanExporter, io.Closer, error) {
	switch kind {
	case "stdout":
		exp, err := stdouttrace.New(
			stdouttrace.WithWriter(os.Stderr),
			stdouttrace.WithPrettyPrint(),
		)
		if err != nil {
			return nil, nil, err
		}
		return exp, nil, nil
	case "otlp":
		opts := []otlptracehttp.Option{
			otlptracehttp.WithEndpoint(cfg.OTLPEndpoint),
		}
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
		client := otlptracehttp.NewClient(opts...)
		exp, err := otlptrace.New(ctx, client)
		if err != nil {
			return nil, nil, err
		}
		// otlptracehttp's Client implements io.Closer; cast through a
		// runtime interface check to avoid leaking internal types.
		closer, _ := any(client).(io.Closer)
		return exp, closer, nil
	}
	return nil, nil, fmt.Errorf("telemetry: unreachable exporter %q", kind)
}

// Tracer returns a tracer scoped to this package's instrumentation.
// Safe to call before Init: returns a no-op tracer when no provider
// has been set, so production code can do its work without an init
// check.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}
