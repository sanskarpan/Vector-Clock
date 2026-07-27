# Architecture Decision Records

This directory contains the Architecture Decision Records (ADRs) for the Vector Clock Lab. Each ADR documents a significant design decision, its context, and the reasoning behind it.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0001](0001-vector-clocks-default.md) | Vector clocks as the default causal ordering primitive | Accepted |
| [ADR-0002](0002-in-process-transport.md) | In-process transport vs. real network | Accepted |
| [ADR-0003](0003-websocket-origin-allowlist.md) | Configurable WebSocket origin allowlist | Accepted |
| [ADR-0004](0004-http-server-timeouts.md) | HTTP server timeouts | Accepted |
| [ADR-0005](0005-prometheus-metrics.md) | Prometheus metrics over custom format | Accepted |
| [ADR-0006](0006-tls-termination.md) | TLS termination at the application | Accepted |
| [ADR-0007](0007-opentelemetry.md) | OpenTelemetry distributed tracing | Accepted |

## Format

Each ADR follows the [Michael Nygard format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions):

- **Status** — Proposed / Accepted / Deprecated / Superseded
- **Context** — What situation forced a decision?
- **Decision** — What was decided?
- **Consequences** — What are the results of the decision?
- **Alternatives considered** — What else was evaluated?

## Adding a new ADR

1. Copy the format from an existing ADR.
2. Name it `00NN-short-title.md` (next sequential number).
3. Add it to the index above.
4. Reference it from the relevant code or documentation.
