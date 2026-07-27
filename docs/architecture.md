# System Architecture

The Vector Clock Lab is a two-tier system: a Go backend that runs the distributed simulation and serves an HTTP + WebSocket API, and a Bun/TypeScript frontend that visualises it. This page covers the package structure, concurrency model, event bus, and WebSocket protocol.

---

## Package structure

```
vectorclock-system/
├── cmd/server/         # main() — wires gateway, simulation, config
├── gateway/            # Gin HTTP + WebSocket server
│   ├── server.go       # ListenAndServe, routes, middleware
│   ├── handlers.go     # REST handlers (/api/v1/…)
│   ├── websocket.go    # WS upgrader, broadcast loop, CheckOrigin
│   ├── auth/           # Bearer token middleware
│   ├── metrics.go      # Prometheus text-format /metrics
│   └── tlsconfig/      # Hot-reload TLS
├── internal/
│   ├── simulation/     # orchestrates N processes
│   │   ├── simulation.go   # Simulation.SpawnProcess / SendMessage / RunScenario
│   │   └── transport.go    # SimTransport: per-channel buffered Go channels
│   ├── process/        # single process: clocks, hold-back queue, KV store
│   │   └── process.go
│   ├── clock/
│   │   ├── lamport/    # scalar clock
│   │   ├── vector/     # vector clock + compare/diff
│   │   ├── matrix/     # matrix clock (MC1–MC4)
│   │   ├── version/    # version vectors
│   │   └── dvv/        # dotted version vectors
│   ├── causality/      # happened-before graph, BFS causal path
│   ├── conflict/       # multi-version KV store, LWW/FWW/keep-all
│   ├── snapshot/       # Chandy-Lamport coordinator
│   ├── events/         # EventBus pub/sub, event type constants
│   └── telemetry/      # OpenTelemetry setup
├── frontend/
│   ├── server/bff.ts   # Bun + Elysia BFF on :3001
│   └── src/
│       ├── components/ # SpaceTimeDiagram, ClockInspector, …
│       └── stores/     # nanostores (reactive state)
├── test/
│   ├── integration/    # integration_test.go
│   ├── e2e/            # e2e_test.go (HTTP + WS against live server)
│   ├── k6/             # load + chaos scenarios (k6)
│   └── playwright/     # 22 browser tests
├── deploy/
│   ├── k8s/            # Kubernetes manifests
│   └── …
├── docs/               # this site
├── Dockerfile
├── docker-compose.yml
├── Makefile
└── config.yaml
```

---

## Component responsibilities

### `gateway/`

The HTTP/WebSocket server. Responsibilities:

- **Auth middleware** (`gateway/auth/`) — validates `Authorization: Bearer <token>` against `VC_API_TOKENS`. Health (`/healthz`, `/readyz`) and metrics (`/metrics`) are exempt.
- **REST handlers** — CRUD for processes, messages, scenarios, KV store, and snapshot initiation.
- **WebSocket broadcaster** (`gateway/websocket.go`) — upgrades connections, runs a fan-out loop reading from the `EventBus`, serialises events as JSON, and writes to every connected client. `CheckOrigin` enforces the `VC_ALLOWED_ORIGINS` allowlist.
- **Prometheus metrics** (`gateway/metrics.go`) — custom text-format exporter: request counters, latency histograms, WS client gauge, panic counter.
- **TLS** (`gateway/tlsconfig/`) — hot-reload cert via atomic pointer; mTLS when client CA is configured.

### `internal/simulation/`

Orchestrates the distributed simulation. Responsibilities:

- **`Simulation`** — owns the process registry and the snapshot coordinator. Exposes `SpawnProcess`, `KillProcess`, `SendMessage`, `BroadcastMessage`, `RunScenario`, `TriggerSnapshot`.
- **`SimTransport`** — one buffered Go channel per directed (from, to) pair. The `forward` goroutine per channel looks up the delivery function dynamically on each message (so killing a process doesn't leave stale closures).
- **Scenarios** — stateless functions `func(s *Simulation) error` registered by name. They call the public Simulation API.

### `internal/process/`

A single simulated process. Responsibilities:

- Maintains a logical clock (type is runtime-selectable: lamport / vector / matrix).
- Runs a `handleMessage` goroutine that: verifies causal ordering (hold-back queue if `DeliveryMode == Causal`), applies the clock receive rule, delivers or holds the message.
- Captures local state atomically via `snapshotLocked()` while `p.mu` is held, and calls `OnMarker` to notify the snapshot coordinator — the key to the race-free snapshot/send fix.
- Publishes events to the `EventBus` on every clock tick, send, receive, and marker.

### `internal/snapshot/`

The `SnapshotCoordinator` implements Chandy-Lamport:

- **`RegisterProcess`** / **`DeregisterProcess`** — maintains the live process list; `RegisterProcess` updates all existing processes' peer lists.
- **`InitiateSnapshot`** — captures initiator's local state *before* acquiring `ps.mu` (deadlock prevention), transitions initiator to `Initiator` role, and sends markers on all outgoing channels.
- **`OnMarkerReceived`** — accepts a `capturedState interface{}` pre-captured by the caller (the process) while it holds its own lock, then transitions the receiving process to `Participant` or closes the in-transit recording. Calls `checkFinalized` when all expected markers have been received.

### `internal/events/`

A lightweight pub/sub `EventBus`. Subscribers get a buffered channel; the bus fan-outs events via `select default` (non-blocking). The gateway WebSocket broadcaster subscribes and fans out to all WS clients.

!!! warning "EventBus channel pattern"
    Never `range` over an EventBus channel — the bus doesn't close subscriber channels on `Unsubscribe`. Use `select { case e, ok := <-ch: ...; case <-quit: return }`.

---

## Concurrency model

```
main goroutine
  └── gateway HTTP server goroutine
        └── per-WS-connection read/write goroutines
              └── EventBus subscription
  └── Simulation goroutines
        └── per-Process handleMessage goroutine
        └── per-Channel forward goroutine (SimTransport)
        └── SnapshotCoordinator (stateless dispatch, no goroutines)
```

**Lock ordering** (must never be violated):

```
gateway.broadcastMu  (never nested)
Simulation.mu        (held briefly for process registry reads)
SnapshotCoordinator.mu  → ProcessSnapState.mu
Process.mu              (never held while acquiring ps.mu)
```

The snapshot ABBA deadlock that was fixed:
- `handleMessage` held `p.mu.Lock()` → called `OnMarkerReceived` → tried `ps.mu.Lock()`
- `InitiateSnapshot` held `ps.mu.Lock()` → called `onCaptureState` → `p.Snapshot()` → tried `p.mu.RLock()`

Fix: `InitiateSnapshot` calls `onCaptureState` *before* `ps.mu.Lock()`. `handleMessage` captures state via `snapshotLocked()` while holding `p.mu`, then passes it to `OnMarkerReceived` as a value.

---

## WebSocket protocol

The gateway upgrades GET `/ws` to a WebSocket connection. All messages from server to client are JSON. The client sends no messages (read-only subscription).

### Event envelope

```json
{
  "type": "clock_tick",
  "ts":   "2026-07-27T11:00:00Z",
  "data": { ... }
}
```

### Event types

| Type | Trigger | Data fields |
|------|---------|-------------|
| `clock_tick` | Internal event on a process | `pid`, `clock` |
| `message_sent` | Process sends a message | `from`, `to`, `payload`, `clock` |
| `message_delivered` | Message delivered (no hold-back delay) | `from`, `to`, `payload`, `clock` |
| `message_held` | Message held back in BSS queue | `from`, `to`, `blocked_by` |
| `message_dropped` | Transport drops message (fault injection) | `from`, `to` |
| `snapshot_marker_sent` | Marker forwarded on a channel | `pid`, `snapshot_id`, `to` |
| `snapshot_marker_received` | Marker received | `pid`, `snapshot_id`, `from` |
| `snapshot_finalized` | Snapshot complete | `snapshot_id`, `state` |
| `process_spawned` | New process registered | `pid`, `clock_type` |
| `process_killed` | Process removed | `pid` |
| `scenario_started` | Scenario run begins | `name` |
| `scenario_finished` | Scenario run ends | `name`, `duration_ms` |
| `kv_write` | KV store write | `key`, `value`, `version` |
| `kv_conflict` | Concurrent write detected | `key`, `versions` |

---

## Data flow: message send

```
REST POST /api/v1/messages
  → gateway handler calls Simulation.SendMessage(from, to, payload)
  → Simulation looks up from-process, calls p.Send(to, payload)
  → Process.Send: increments clock, enqueues message to SimTransport channel
  → EventBus: publishes message_sent event
  → SimTransport.forward goroutine: looks up deliverFn for `to`, calls deliverFn(msg)
  → to-Process.handleMessage: checks causal order
      if causal delivery required and not yet deliverable: enqueue hold-back
      else: apply clock receive rule, deliver, emit message_delivered
  → EventBus: publishes message_delivered (or message_held)
  → WS broadcaster: fans out to all clients
  → Browser: D3 diagram renders new arc on space-time diagram
```
