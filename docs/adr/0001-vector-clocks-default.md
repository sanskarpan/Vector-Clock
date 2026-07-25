# ADR 0001: Vector clocks as the default causal ordering primitive

## Status
Accepted.

## Context
The system needs to track causal ordering between events across multiple
processes. Lamport scalar clocks provide a total order but not enough
information to detect concurrent events; vector clocks are the standard
trade-off between Lamport's compactness and matrix clocks' expressiveness.

## Decision
Vector clocks are the default. Lamport clocks are available for compatibility
and matrix clocks for advanced garbage-collection analysis.

## Consequences
- Detects concurrent events correctly (Lamport cannot).
- Memory cost is O(P) per event where P is the number of processes.
- Matrix clocks' GC capability is documented but not enforced by default.

## Alternatives considered
- **Lamport only**: Rejected — cannot answer "are A and B concurrent?" queries.
- **Matrix only**: Rejected — 2× memory cost with no benefit for our scale.
- **Hybrid (Lamport + vector)**: Considered — adds complexity without
  enabling new features.