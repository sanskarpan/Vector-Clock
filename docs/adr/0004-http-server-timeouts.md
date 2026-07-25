# ADR 0004: HTTP server timeouts

## Status
Accepted.

## Context
The HTTP server was started with `ListenAndServe` and a bare `http.Server`
struct that set only `ReadTimeout` and `WriteTimeout`. Slowloris attacks
exploit the absence of `ReadHeaderTimeout`; idle connections waste file
descriptors without `IdleTimeout`.

## Decision
Set all four production-relevant timeouts on every `http.Server`:
`ReadHeaderTimeout=5s`, `ReadTimeout=15s`, `WriteTimeout=15s`,
`IdleTimeout=120s`.

## Consequences
- Slowloris is mitigated.
- Idle keep-alive connections are recycled every 2 minutes.
- Long-running requests (none in the current API) would need an explicit
  context-aware handler.

## Alternatives considered
- **No timeouts**: Rejected — known vulnerability.
- **Single 30s timeout for everything**: Rejected — write-blocking legitimately
  takes longer than read-header parsing.