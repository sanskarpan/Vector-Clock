# ADR 0005: Prometheus metrics over custom format

## Status
Accepted.

## Context
The `/metrics` endpoint previously returned a small JSON document with just
the event bus drop count. Operators had no visibility into request rate,
error rate, latency, or resource utilisation.

## Decision
Expose a Prometheus-compatible `/metrics` endpoint with:
- `vc_http_requests_total` (counter, by method/route/status)
- `vc_http_request_duration_seconds` (histogram)
- `vc_http_in_flight_requests` (gauge)
- `vc_http_errors_total` (counter)
- `vc_ws_clients_connected` (gauge)
- `vc_ws_messages_sent_total`, `vc_ws_events_dropped_total` (counters)
- `vc_panics_recovered_total` (counter)
- Standard `go_*` and `process_*` collectors.

## Consequences
- Operators can build dashboards and alerts in Grafana.
- Format is stable, scrape-config compatible.
- Tests can assert specific metric names.

## Alternatives considered
- **OpenTelemetry / OTLP**: Considered — overkill for a single-service lab.
  Will revisit if we add distributed tracing.
- **StatsD**: Rejected — less standard, more vendor lock-in.
- **JSON custom format**: Rejected — operator friction.