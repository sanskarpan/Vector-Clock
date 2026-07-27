# Vector Clock Lab

> An interactive laboratory for exploring distributed-system time: Lamport scalar clocks, vector clocks, matrix clocks, version vectors, dotted version vectors, causal delivery (BSS hold-back queues), the Chandy-Lamport global snapshot algorithm, and multi-version conflict detection — all rendered live in a browser.

---

## What this lab does

The Vector Clock Lab simulates a small distributed system — N processes exchanging messages over FIFO in-process channels — and exposes every internal event through a WebSocket stream that drives a D3-powered space-time diagram in the browser.

You can:

- **Spawn and kill processes** and watch their Lamport, vector, or matrix clocks tick in real time.
- **Inject network faults** — delay, drop, reorder, or partition specific channels.
- **Run pre-built scenarios** that demonstrate key distributed systems properties (causal violations, concurrent writes, global snapshots, conflict resolution).
- **Trigger a Chandy-Lamport global snapshot** and inspect the captured process state plus all in-transit messages at the moment of the consistent cut.
- **Read and write a causal KV store** that detects concurrent writes via version vectors and resolves conflicts with pluggable strategies (LWW, FWW, keep-all, merge).

---

## Implemented algorithms

| Concept | Paper | Package |
|---------|-------|---------|
| Scalar logical clocks | Lamport 1978 | `internal/clock/lamport` |
| Happened-before relation (→) | Lamport 1978 | `internal/causality` |
| Vector clocks | Fidge 1988 / Mattern 1989 | `internal/clock/vector` |
| Partial order detection | Charron-Bost 1991 | `internal/causality` |
| Matrix clocks | Kshemkalyani-Singhal 1992 | `internal/clock/matrix` |
| Version vectors | Parker et al. 1983 | `internal/clock/version` |
| Dotted version vectors | Preguiça et al. 2010 | `internal/clock/dvv` |
| BSS causal broadcast | Birman-Schiper-Stephenson 1987 | `internal/process` |
| Global snapshot | Chandy-Lamport 1985 | `internal/snapshot` |
| Causal KV store | Ahamad et al. 1995 | `internal/conflict` |

---

## Architecture at a glance

```
Browser / curl
     │ HTTP + WebSocket
┌────▼──────────────────────────────┐
│  gateway/  (Gin HTTP + WS server) │
│  /api/v1/…  /ws  /metrics        │
└────┬──────────────────────────────┘
     │
┌────▼──────────────────────────────┐
│  internal/simulation              │
│  ┌──────────────────────────────┐ │
│  │  N × Process                 │ │
│  │  ┌──────────┐ ┌───────────┐  │ │
│  │  │ clock/   │ │ snapshot  │  │ │
│  │  │ vector   │ │ coord.    │  │ │
│  │  └──────────┘ └───────────┘  │ │
│  └──────────────────────────────┘ │
│  SimTransport   EventBus          │
└───────────────────────────────────┘
     │
┌────▼──────────────────────────────┐
│  frontend/server/ (Bun + Elysia)  │
│  BFF on :3001 — REST proxy + WS   │
└────┬──────────────────────────────┘
     │
┌────▼──────────────────────────────┐
│  Browser  (Bun + TypeScript + D3) │
│  SpaceTimeDiagram  ClockInspector │
│  SnapshotViewer   ConflictDash    │
└───────────────────────────────────┘
```

The Go backend runs on `:8080`. The Bun BFF runs on `:3001` and proxies REST calls (forwarding `Authorization` headers) and WebSocket connections to the backend. All internal clock state changes, message deliveries, marker events, and KV writes are published on the `EventBus` and fanned out to every connected WebSocket client.

---

## Start here

**Theory**

- [Core concepts](concepts.md) — logical time, happened-before, causality, consistent cuts
- [Clock algorithms](algorithms.md) — Lamport, vector, matrix, DVV, version vectors — theory and API
- [Global snapshots](snapshot.md) — Chandy-Lamport 1985, marker propagation, the consistent cut
- [Causal delivery](causal-delivery.md) — BSS hold-back queues, `BlockedBy` analysis
- [Conflict detection](conflict.md) — version vectors, LWW, FWW, keep-all

**Implementation**

- [System architecture](architecture.md) — packages, concurrency model, event bus, WS protocol
- [REST & WebSocket API](api.md) — every endpoint and message type
- [Configuration reference](configuration.md) — `config.yaml` and all env vars
- [Pre-built scenarios](scenarios.md) — 8 scenarios and what each demonstrates
- [Frontend components](frontend.md) — SpaceTimeDiagram, ClockInspector, and more

**Operations**

- [Deployment guide](deployment.md) — Docker Compose, Kubernetes, TLS, Prometheus
- [Testing strategy](testing.md) — unit, integration, E2E, K6, Playwright

**Cookbook**

- [Run your first scenario](cookbook/basic-scenario.md)
- [Chandy-Lamport snapshot walkthrough](cookbook/snapshot-walkthrough.md)
- [Fault injection — delay, drop, partition](cookbook/fault-injection.md)
- [Causal delivery with hold-back queues](cookbook/causal-delivery.md)
- [Conflict detection with version vectors](cookbook/conflict-resolution.md)

---

## Quickstart

```bash
# Clone and run with Docker Compose
git clone https://github.com/sanskarpan/Vector-Clock.git
cd Vector-Clock
docker compose up -d

# Liveness check
curl http://localhost:8080/healthz   # {"status":"ok"}

# Spawn three processes and send a message
curl -X POST http://localhost:8080/api/v1/processes -d '{"id":"P1","clock_type":"vector"}'
curl -X POST http://localhost:8080/api/v1/processes -d '{"id":"P2","clock_type":"vector"}'
curl -X POST http://localhost:8080/api/v1/messages \
     -d '{"from":"P1","to":"P2","payload":"hello"}'

# Run the pre-built 3-process snapshot scenario
curl -X POST http://localhost:8080/api/v1/scenarios/Snapshot3P/run

# Open the frontend
open http://localhost:3001
```

---

## Paper references

| Paper | What it enables |
|-------|----------------|
| Lamport, L. (1978). *Time, clocks, and the ordering of events in a distributed system.* CACM. | Happened-before relation, Lamport scalar clocks |
| Fidge, C. (1988). *Timestamps in message-passing systems.* Proc. 11th Australian CS Conf. | Vector clocks |
| Mattern, F. (1989). *Virtual time and global states of distributed systems.* Parallel and Distributed Algorithms. | Vector clocks, consistent global cuts |
| Kshemkalyani, A. & Singhal, M. (1992). *Efficient detection of message causality.* IEEE TPDS. | Matrix clocks (MC1–MC4 rules) |
| Chandy, K.M. & Lamport, L. (1985). *Distributed snapshots: Determining global states of distributed systems.* ACM TOCS. | Global snapshot algorithm |
| Birman, K., Schiper, A. & Stephenson, P. (1987). *Lightweight causal and atomic group multicast.* ACM TOCS. | BSS causal delivery |
| Parker, D. et al. (1983). *Detection of mutual inconsistency in distributed systems.* IEEE TSE. | Version vectors |
| Preguiça, N. et al. (2010). *A dotted version vector: Managing causality in distributed key-value stores.* SRDS. | Dotted version vectors |

> This site is generated with [MkDocs](https://www.mkdocs.org/) using the [Material theme](https://squidfunk.github.io/mkdocs-material/).
> Every page is a file in `docs/` — edit it on GitHub.
