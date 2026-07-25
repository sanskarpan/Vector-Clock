# ADR 0003: Configurable WebSocket origin allowlist

## Status
Accepted.

## Context
Pre-hardening, the WebSocket upgrader accepted connections from any origin
(`CheckOrigin` returned `true`). This is a CSRF vector — a malicious page
on a different origin could open a WS to the lab and drive scenarios or
exfiltrate state.

## Decision
Make the origin allowlist configurable via the `VC_ALLOWED_ORIGINS` env var.
Empty = same-origin only (the safe default).

## Consequences
- Operators must explicitly opt in to cross-origin access.
- Same default (empty) blocks all cross-origin connections — operators
  configuring dev fronts must set the env var.
- Tests cover both the default-deny and allowlist paths.

## Alternatives considered
- **Hard-code `localhost:3001`**: Rejected — would prevent any other deployment.
- **Echo back any origin (CORS spec violation)**: Rejected — same risk as the
  pre-hardening state.
- **Auth header-based auth instead of origin**: Considered — would require
  a session store and credential lifecycle. Out of scope for a lab tool.