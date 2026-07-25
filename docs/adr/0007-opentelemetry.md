# ADR 0007: OpenTelemetry Distributed Tracing

## Status

Accepted (2026-07-23)

## Context

The Vector Clock Lab needs observability across the request path: browser → BFF (Elysia) → Go backend → simulation engine. Without distributed tracing it is hard to debug latency regressions, causality violations that span process boundaries, or snapshot coordination delays in production-like environments.

We considered three approaches:

1. **Structured logging only** (current state) — cheap but loses the causal span tree. A single user action (e.g. "run scenario") fans out to dozens of goroutines; correlating them via log ids alone is tedious.

2. **Prometheus metrics** (already present) — great for aggregate dashboards but useless for debugging individual slow requests.

3. **OpenTelemetry distributed tracing** — provides end-to-end span trees, W3C Trace Context propagation across the HTTP/WS boundary, and can be exported to Jaeger, Tempo, or any OTLP-compatible backend.

## Decision

Adopt OpenTelemetry for distributed tracing, using the OTLP HTTP exporter as the primary transport.

### Architecture

```
Browser ──HTTP──→ BFF ──HTTP──→ Go Gin ──→ Simulation Engine
                       │
                   traceparent
                   propagated
                   via headers
```

- **Browser → BFF**: W3C `traceparent` header is propagated by the browser's `fetch()` API automatically when the page's `meta` tag includes it, or manually injected by the BFF.
- **BFF → Go**: The Elysia BFF forwards `traceparent` headers on proxied REST calls.
- **Go Gin → Simulation**: The Gin request context carries the span. Internal operations (`SendMessage`, `TriggerSnapshot`, etc.) create child spans via `otel.Tracer().Start(ctx, name)`.
- **WebSocket**: The WS upgrade request carries the trace context. The first subscribe message from the BFF includes the trace context, establishing the root span for the WS session.

### Exporter selection

| Mode | Exporter | When to use |
|------|----------|-------------|
| `none` | No-op (no overhead) | Production default, dev without tracing |
| `stdout` | JSON to stderr | Local debugging |
| `otlp` | OTLP HTTP to collector | Production with Jaeger/Tempo/Grafana |

### Sampling

Default sample ratio is 1.0 for development; production deployments should set `OTEL_SAMPLE_RATIO` to a value between 0.01 and 0.1 depending on traffic volume. The sampler is `ParentBased(TraceIDRatioBased(sampleRatio))` so sampled root spans propagate the decision to child spans.

### Zero-overhead path

When `OTEL_EXPORTER=none` (the default), a no-op `TracerProvider` is installed. Spans obtained from `telemetry.Tracer()` are no-op objects that cost a single interface dispatch and do not allocate. All tracing code paths are identical regardless of exporter; the no-op path is verified in `telemetry_test.go`.

## Consequences

### Positive

- End-to-end span trees from HTTP request to simulation message delivery
- Standard W3C Trace Context propagation works with any OTLP backend
- No overhead when tracing is disabled (the default)
- BFF can propagate traces without depending on a full OTel SDK (just header forwarding)
- Existing Prometheus metrics are unaffected; traces and metrics are complementary

### Negative

- Adds OTel SDK dependencies (~2 MB to binary size)
- OTLP exporter requires a collector endpoint in production
- BFF currently does not parse traceparent from browser requests (future work)

### Mitigations

- Binary size increase is acceptable for a lab deployment; the distroless image is already ~20 MB
- Production deployments can skip the collector by setting `OTEL_EXPORTER=none`
- BFF tracing integration is tracked as a low-priority follow-up

## References

- [OpenTelemetry Go SDK](https://opentelemetry.io/docs/languages/go/)
- [W3C Trace Context](https://www.w3.org/TR/trace-context/)
- [OTLP HTTP Exporter](https://opentelemetry.io/docs/specs/otlp/#otlphttp)
