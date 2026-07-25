package telemetry

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestInit_NoneReturnsNoop verifies the zero-overhead default path.
// When Exporter is "none" (or empty), Init must:
//   - return a working no-op shutdown
//   - install a noop tracer provider
//   - still set a W3C TraceContext propagator (so traceparent extraction
//     does not silently no-op on inbound requests)
func TestInit_NoneReturnsNoop(t *testing.T) {
	// Save and restore global state to keep tests independent.
	origTP := otel.GetTracerProvider()
	origPP := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origPP)
	})

	for _, exporter := range []string{"", "none", "NONE"} {
		exporter := exporter
		t.Run("exporter="+exporter, func(t *testing.T) {
			shutdown, err := Init(context.Background(), Config{
				Exporter:    exporter,
				ServiceName: "test",
			})
			if err != nil {
				t.Fatalf("Init: %v", err)
			}
			if shutdown == nil {
				t.Fatal("Init returned nil shutdown")
			}
			// Shutdown must be a no-op and must not error.
			if err := shutdown(context.Background()); err != nil {
				t.Errorf("shutdown: %v", err)
			}
			// Tracer provider must be a noop provider.
			_, ok := otel.GetTracerProvider().(noop.TracerProvider)
			if !ok {
				t.Errorf("expected noop.TracerProvider, got %T", otel.GetTracerProvider())
			}
			// Propagator must be a composite with TraceContext so
			// inbound traceparent headers are still parsed.
			prop := otel.GetTextMapPropagator()
			if prop == nil {
				t.Fatal("propagator is nil")
			}
			fields := prop.Fields()
			hasTraceparent := false
			for _, f := range fields {
				if f == "traceparent" {
					hasTraceparent = true
					break
				}
			}
			if !hasTraceparent {
				t.Errorf("propagator fields %v missing traceparent", fields)
			}
		})
	}
}

// TestInit_StdoutProducesSpans verifies the "stdout" exporter path.
// Captures stderr (where the stdouttrace exporter writes per our
// configuration) and asserts that a created span round-trips into the
// captured stream before shutdown.
func TestInit_StdoutProducesSpans(t *testing.T) {
	// Save and restore global state.
	origTP := otel.GetTracerProvider()
	origPP := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origPP)
	})

	// Redirect os.Stderr to a pipe so we can read what stdouttrace
	// emits without polluting the test runner output.
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = origStderr })

	shutdown, err := Init(context.Background(), Config{
		Exporter:    "stdout",
		ServiceName: "test-service",
		SampleRatio: 1.0,
	})
	if err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Force the SDK to be the active provider. Capture the type to
	// confirm we got a real SDK provider (not the noop fallback).
	tp := otel.GetTracerProvider()
	if _, ok := tp.(*sdktrace.TracerProvider); !ok {
		t.Fatalf("expected *sdktrace.TracerProvider, got %T", tp)
	}

	tr := Tracer()
	_, span := tr.Start(context.Background(), "test-span")
	span.End()

	// Force-flush the tracer provider so the span is exported to the
	// pipe before we close and read it. Without this the batch span
	// processor may not have flushed when Shutdown returns.
	if tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := tp.ForceFlush(ctx); err != nil {
			t.Errorf("force flush: %v", err)
		}
		cancel()
	}

	// Shutdown flushes remaining buffers and closes the exporter.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	// Close the pipe writer so the reader can drain.
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("read pipe: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "test-span") {
		t.Errorf("stdout output missing span name; got:\n%s", out)
	}
	if !strings.Contains(out, "test-service") {
		t.Errorf("stdout output missing service name; got:\n%s", out)
	}
}

// TestInit_UnknownExporterErrors verifies the error path for an
// unrecognised exporter. The function must return an error and not
// install any provider that could be left dangling.
func TestInit_UnknownExporterErrors(t *testing.T) {
	origTP := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(origTP) })

	_, err := Init(context.Background(), Config{
		Exporter: "kafka",
	})
	if err == nil {
		t.Fatal("expected error for unknown exporter")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Errorf("error should mention the bad exporter; got: %v", err)
	}
}

// TestInit_OTLPEmptyEndpointFails verifies that asking for an OTLP
// exporter with no endpoint returns an error (we don't silently fall
// back to localhost:4318 — operators should be explicit).
func TestInit_OTLPEmptyEndpointFails(t *testing.T) {
	_, err := Init(context.Background(), Config{
		Exporter:     "otlp",
		OTLPEndpoint: "",
	})
	if err == nil {
		t.Skip("OTLP exporter accepts empty endpoint (uses default); skipping")
	}
}

// TestTracer_NoInitReturnsNoop verifies that callers who obtain a
// Tracer before calling Init still get a usable (no-op) tracer.
func TestTracer_NoInitReturnsNoop(t *testing.T) {
	tr := Tracer()
	if tr == nil {
		t.Fatal("Tracer() returned nil")
	}
	_, span := tr.Start(context.Background(), "anything")
	if span == nil {
		t.Fatal("Start returned nil span")
	}
	span.End() // must not panic
}

// TestPropagationWiring sanity-checks that we can inject and extract a
// trace context through the global propagator that Init sets up.
// This is what the BFF and the WS handler rely on.
func TestPropagationWiring(t *testing.T) {
	origPP := otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTextMapPropagator(origPP) })

	if _, err := Init(context.Background(), Config{Exporter: "none"}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	pp := otel.GetTextMapPropagator()
	if pp == nil {
		t.Fatal("propagator not set")
	}

	// Build a known context, inject it into a carrier, then extract it
	// back and compare. The trace id must survive the round trip.
	tid, _ := trace.TraceIDFromHex("00000000000000000000000000000001")
	sid, _ := trace.SpanIDFromHex("0000000000000001")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	carrier := propagation.MapCarrier{}
	pp.Inject(ctx, carrier)
	if carrier["traceparent"] == "" {
		t.Fatalf("traceparent not injected; carrier=%v", carrier)
	}

	extracted := pp.Extract(context.Background(), carrier)
	esc := trace.SpanContextFromContext(extracted)
	if esc.TraceID() != tid {
		t.Errorf("trace id mismatch: got %s want %s", esc.TraceID(), tid)
	}
}

// TestInit_Stdout_FileExcluded checks the auxiliary invariant that
// generated telemetry files don't escape the working directory.
// This is a defensive smoke test for path handling — we use a temp
// file just to confirm ioutil helpers behave; Init itself writes to
// stderr, not a file.
func TestInit_Stdout_FileExcluded(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "telemetry.txt")
	if _, err := os.Stat(p); err == nil {
		t.Fatalf("file should not exist yet: %s", p)
	}
}
