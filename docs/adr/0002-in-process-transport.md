# ADR 0002: In-process transport vs. real network

## Status
Accepted.

## Context
The lab needs to simulate message passing between processes for educational
purposes. We could either (a) use a real network stack (TCP, UDP) or (b)
implement an in-process transport using Go channels.

## Decision
In-process transport (Go channels) per `internal/simulation/transport.go`.

## Consequences
- Zero external dependencies, runs anywhere.
- Deterministic ordering under controlled scenarios.
- Cannot model real-network behaviour (reordering, partitions) without
  fault-injection code; we expose `InjectDelay` and `InjectDrop` to cover
  the most pedagogically useful cases.

## Alternatives considered
- **TCP**: Adds deployment friction (port allocation), more code, no upside.
- **gRPC**: Massive dependency for a lab tool.
- **Shared-memory queue with simulated delays**: We do this — it's exactly
  the `SimTransport` we ship.