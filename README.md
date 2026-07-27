# Vector Clock Lab

[![Docs](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://sanskarpan.github.io/Vector-Clock/)
[![CI](https://github.com/sanskarpan/Vector-Clock/actions/workflows/ci.yml/badge.svg)](https://github.com/sanskarpan/Vector-Clock/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> An interactive laboratory for exploring distributed-system time: Lamport
> scalar clocks, vector clocks, matrix clocks, version vectors, dotted version
> vectors, causal delivery (BSS hold-back queues), the Chandy-Lamport global
> snapshot algorithm, and multi-version conflict detection.

**[Full documentation →](https://sanskarpan.github.io/Vector-Clock/)**

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Quickstart](#quickstart)
5. [Configuration reference](#configuration-reference)
6. [HTTP + WebSocket API](#http--websocket-api)
7. [Running tests](#running-tests)
8. [Building and running in Docker](#building-and-running-in-docker)
9. [TLS termination](#tls-termination)
10. [Operations runbook](docs/RUNBOOK.md)
11. [Architecture decisions](docs/adr/)
12. [Contributing](#contributing)
13. [License](#license)

---

## What it does

The Vector Clock Lab simulates a small distributed system in a single Go
process and exposes it as an HTTP + WebSocket API. You can:

- Spawn processes (P1, P2, ...) and watch their clocks tick.
- Inject network faults (delay, drop) on specific channels.
- Run pre-built scenarios (BasicLamport, ConcurrentWrites, CausalViolation,
  CausalDelivery, Snapshot3P).
- Issue a Chandy-Lamport global snapshot and inspect the captured state +
  in-transit messages.
- Read and write to a multi-version KV store that detects causal conflicts
  and resolves them with last-writer-wins / first-writer-wins / keep-all.

The frontend (Bun + TypeScript) visualises clocks, the causal graph, the
space-time diagram, and the snapshot inspector.

## Features

- **Clock types**: Lamport scalar, vector, matrix, dotted version vectors
- **Causal delivery** via BSS hold-back queues with `BlockedBy` analysis
- **Concurrent snapshot initiators** (per CHECKLIST Phase 7)
- **Multi-version conflict detection** with `LastWriterWins`/`FirstWriterWins`/custom resolvers
- **Transport fault injection**: delay, drop, reorder, partition
- **Anti-entropy sync** between replicas
- **OpenTelemetry tracing** (OTLP/stdout)
- **Rate limiting** per-IP (token bucket, 100 req/min default)
- **Pre-built scenarios**: BasicLamport, CausalViolation, CausalDelivery, Snapshot3P, ConcurrentWrites, FalseConflict, MatrixGC, PartitionAndHeal

## Architecture

```
                          ┌─────────────────────────────┐
                          │        Browser / curl       │
                          └──────────┬──────────────────┘
                                     │ HTTP + WS
                          ┌──────────▼──────────────────┐
                          │   gateway/  (Gin server)    │
                          │   /api/v1/...   /ws   /m... │
                          └──────────┬──────────────────┘
                                     │
                          ┌──────────▼──────────────────┐
                          │   internal/simulation       │
                          │   ┌─────────────────────┐   │
                          │   │  N × Process        │   │
                          │   │  ┌──────────────┐   │   │
                          │   │  │ clock/vector │   │   │
                          │   │  │ clock/matrix │   │   │
                          │   │  │ clock/lamport│   │   │
                          │   │  └──────────────┘   │   │
                          │   └─────────────────────┘   │
                          │   SimTransport (in-proc)    │
                          │   EventBus (pub/sub)        │
                          │   SnapshotCoordinator       │
                          └──────────┬──────────────────┘
                                     │
                          ┌──────────▼──────────────────┐
                          │   internal/conflict  (KV)   │
                          │   internal/causality (graph)│
                          └─────────────────────────────┘
```

## Prerequisites

| Tool   | Version          | Notes                                  |
| ------ | ---------------- | -------------------------------------- |
| Go     | 1.25.0 or newer  | Required by go.mod                     |
| Bun    | 1.1+             | Frontend only; not needed for backend   |
| Docker | 24+              | Optional; for container deployment     |

Verify with:

```sh
go version       # go1.25.0 or newer
bun --version    # 1.1 or newer (frontend only)
docker --version # optional
```

## Quickstart

Five commands to a running server:

```sh
git clone <repo>
cd Vector-Clock
go mod download
cp config.yaml config.local.yaml       # optional: tweak defaults
go run ./cmd/server
# Server listening on http://localhost:8080
```

Verify:

```sh
curl http://localhost:8080/healthz     # → {"status":"ok",...}
curl http://localhost:8080/readyz      # → {"ready":true}
curl http://localhost:8080/metrics | head
curl http://localhost:8080/api/v1/simulation/state | jq .
```

Frontend (optional):

```sh
cd frontend
bun install
bun run dev
# Open http://localhost:3001

Run a pre-built scenario (server already running):

```sh
curl -X POST http://localhost:8080/api/v1/scenarios/BasicLamport/run
```

#### Frontend UI components

The frontend is organized into several panels:

- **Space-Time Diagram**: event visualization, consistent cut lines, message transit animation, timeline scrubber
- **Clock Inspector**: per-process clock display, matrix heatmap, concurrent analysis
- **Delivery Monitor**: hold-back queue visualization, delivery timeline per process, causal/immediate mode toggle
- **Conflict Dashboard**: version DAG, VV/DVV false conflict toggle, resolution log
- **Snapshot Viewer**: 3-phase display, verification button
- **Scenario Panel**: narration, auto-animate, custom scenario builder

## Configuration reference

All configuration is read from `config.yaml` (or `CONFIG_PATH` if set) plus
environment variables. **Environment variables override the config file.**

### File: `config.yaml`

| Key                          | Type   | Default      | Description                                |
| ---------------------------- | ------ | ------------ | ------------------------------------------ |
| `server.port`                | int    | 8080         | TCP port the HTTP server binds to          |
| `server.ws_buffer`           | int    | 256          | (Reserved) per-client WS send buffer       |
| `server.rate_limit_per_ip`   | float  | 1.67         | Per-IP rate limit (req/s); 0 = disabled   |
| `server.rate_limit_burst`    | float  | 10           | Token bucket burst size                   |
| `simulation.initial_processes` | int  | 3            | Number of processes spawned at startup     |
| `simulation.clock_type`      | string | `vector`     | `lamport` \| `vector` \| `matrix`          |
| `simulation.delivery_mode`   | string | `causal`     | `immediate` \| `causal` \| `total_order`   |
| `simulation.channels`        | string | `full_mesh`  | `full_mesh` (other values log + fall back) |
| `logging.level`              | string | `info`       | `debug` \| `info` \| `warn` \| `error`      |
| `logging.format`             | string | `json`       | `json` \| `console`                        |

### Environment variables

| Variable                    | Type        | Default          | Required | Description                                          |
| --------------------------- | ----------- | ---------------- | -------- | ---------------------------------------------------- |
| `PORT`                      | int         | 8080             | no       | Override listening port                              |
| `CONFIG_PATH`               | string      | `config.yaml`    | no       | Path to YAML config file                             |
| `VC_ALLOWED_ORIGINS`        | string list | (empty)          | no       | Comma-separated origin allowlist for WS + CORS       |
| `VC_TLS_CERT_FILE`          | string      | (empty)          | no       | PEM cert path. Setting this enables HTTPS            |
| `VC_TLS_KEY_FILE`           | string      | (empty)          | no       | PEM private key path (required when cert is set)     |
| `VC_TLS_CLIENT_CA_FILE`     | string      | (empty)          | no       | PEM CA bundle path; enables mTLS when set            |
| `VC_TLS_RELOAD_INTERVAL`    | duration    | (empty)          | no       | e.g. `5m`; poll interval for cert hot-reload          |
| `LOGGING_LEVEL`             | string      | (from config)    | no       | Log level override                                   |
| `LOGGING_FORMAT`            | string      | (from config)    | no       | Log format override                                  |

`PORT` must be in the range 1..65535. `VC_ALLOWED_ORIGINS` empty = same-origin
only (recommended).

### TLS termination

The server can terminate TLS itself or run behind a TLS-terminating
load balancer. To terminate TLS at the application, set
`VC_TLS_CERT_FILE` and `VC_TLS_KEY_FILE` to PEM-encoded files. When
set, the server listens on the same port for HTTPS instead of HTTP.
Plain HTTP is no longer served on that port — put a load balancer in
front to redirect HTTP → HTTPS, or accept that clients must use the
`https://` scheme.

Cipher suites and protocol versions match Mozilla's "intermediate"
profile: TLS 1.2 minimum, modern AEAD suites only, ALPN `h2` /
`http/1.1` so HTTP/2 is negotiated by default. `SSLv3`, `TLS 1.0`, and
`TLS 1.1` are rejected.

To enable mutual TLS, set `VC_TLS_CLIENT_CA_FILE` to a PEM bundle of
the CA(s) you want to trust for client certificates. The server will
require a valid client cert on every connection (mTLS).

#### Hot reload (cert-manager / Let's Encrypt)

When `VC_TLS_RELOAD_INTERVAL` is set to a non-zero duration (e.g. `5m`,
`1h`), a background goroutine polls the cert + key files. If either
file's mtime or size has changed, the cert is re-parsed and
atomically swapped in. In-flight connections continue with the old
cert; new handshakes use the new one. This is the recommended
configuration for cert-manager + Let's Encrypt, where the cert is
renewed on disk every 60–90 days and the server must pick up the new
cert without a restart.

```sh
# Example: 5-minute reload poll.
VC_TLS_CERT_FILE=/etc/vectorclock/tls.crt \
VC_TLS_KEY_FILE=/etc/vectorclock/tls.key \
VC_TLS_RELOAD_INTERVAL=5m \
./server
```

#### Generating a self-signed cert for testing

Use the helper bundled with the tlsconfig package (no openssl
dependency required):

```go
import "github.com/DistributedClocks/vectorclock-system/gateway/tlsconfig"

pair, _ := tlsconfig.GenerateSelfSignedCert(
    "localhost",
    []string{"localhost"},
    []net.IP{net.ParseIP("127.0.0.1")},
)
os.WriteFile("cert.pem", pair.CertPEM, 0o600)
os.WriteFile("key.pem",  pair.KeyPEM,  0o600)
```

Or with `openssl`:

```sh
openssl req -x509 -newkey ed25519 -nodes -days 365 \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout key.pem -out cert.pem
```

Note: macOS's built-in `curl` (SecureTransport) does not currently
support ed25519 in TLS certs, so use an ECDSA or RSA cert for manual
smoke tests on macOS. The server itself accepts any signature
algorithm.

#### Production: cert-manager + Let's Encrypt

A typical Kubernetes setup uses cert-manager to issue a Let's Encrypt
cert and writes it to a `Secret` that the deployment mounts as a
volume. The server's reload goroutine picks up the renewed cert
without a pod restart.

See `deploy/k8s/manifests.yaml` for an annotated example, including
the `cert-manager.io/issuer` annotation and the `Secret`/`VolumeMount`
wiring.

### OpenTelemetry

| Variable                       | Type   | Default              | Description                              |
| ------------------------------ | ------ | -------------------- | ---------------------------------------- |
| `OTEL_EXPORTER`                | string | `none`               | Values: `otlp`, `stdout`, `none`         |
| `OTEL_SERVICE_NAME`            | string | `vectorclock-server` | Service name for traces                  |
| `OTEL_SAMPLE_RATIO`            | float  | `1.0`                | Trace sampling ratio (0.0 to 1.0)        |
| `OTEL_INSECURE`                | bool   | `false`              | Skip TLS for OTLP exporter               |
| `OTEL_EXPORTER_OTLP_ENDPOINT`  | string | `localhost:4318`     | OTLP exporter endpoint                   |

## HTTP + WebSocket API

Base URL: `http://localhost:8080/api/v1`.

### Operational

- `GET /healthz` — liveness (always 200 if process running)
- `GET /readyz` — readiness (503 during shutdown)
- `GET /metrics` — Prometheus text exposition format

### Simulation

- `POST /simulation/start` — `{ processCount, clockType, deliveryMode }`
- `POST /simulation/reset` — body optional, same fields
- `GET  /simulation/state` — current snapshot of all processes

### Processes

- `POST   /processes` — `{ id, clockType?, deliveryMode? }` (201 Created)
- `DELETE /processes/:id`
- `GET    /processes/:id`
- `POST   /processes/:id/event` — tick internal event
- `POST   /processes/:id/snapshot` — initiate Chandy-Lamport snapshot

### Messages

- `POST /messages` — `{ from, to, data }`
- `POST /broadcast` — `{ from, data }`

### Snapshots

- `GET /snapshots/:id`
- `GET /snapshots/:id/verify`

### Causality

- `GET /causality/happened-before?a=<id>&b=<id>`

### Faults

- `POST   /faults/delay` — `{ from, to, delayMs }` (max 600000ms)
- `POST   /faults/drop` — `{ from, to }` (next message on that channel)
- `DELETE /faults` — clear all injected faults

### KV store (causal conflict detection)

- `POST /kv/:key` — `{ value: base64, authorId, contextVc: {pid: count} }`
- `GET  /kv/:key` — list sibling versions
- `POST /kv/:key/resolve` — `{ strategy: "last_writer_wins" | "first_writer_wins" | "keep_all" }`

### Scenarios

- `GET  /scenarios`
- `POST /scenarios/:name/run` — async; emits `scenario_step` events

### WebSocket

- `GET /ws` — bidirectional event stream. Client messages: `{ action, types }` (parsed but filter not yet applied).

All errors are JSON: `{ "error": "message" }`. Errors never leak internal
paths or goroutine stacks.

## Running tests

```sh
make test              # all tests, fast
make test-race         # with race detector (~3x slower)
make test-coverage     # generates coverage.html, enforces 70% min
make fuzz              # 5s per fuzz target
make bench             # benchmarks
```

Individual packages:

```sh
go test ./internal/clock/vector/...
go test ./internal/simulation/... -race
go test -run TestCausalDelivery_HoldsOutOfOrder ./internal/process/...
```

## Building and running in Docker

```sh
# Build and run locally
docker compose up --build

# Or build a release image
docker build -t vectorclock/server:1.0.0 .
docker run --rm -p 8080:8080 \
    -e PORT=8080 \
    -e VC_ALLOWED_ORIGINS=https://app.example.com \
    vectorclock/server:1.0.0
```

The image is `distroless/static:nonroot` — no shell, runs as UID 65532.
`HEALTHCHECK` is intentionally NONE because the distroless image lacks `wget`
or `curl`; use Kubernetes' `httpGet` probe against `/healthz`.

## Contributing

1. Fork → branch named `feat/<short-name>` or `fix/<short-name>`.
2. Run `make lint test test-race` locally before pushing.
3. Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/).
4. Open a PR; CI must pass (lint, test, race, coverage, vuln, build).
5. Coverage must remain ≥ 70%.

Branch names: `feat/...`, `fix/...`, `docs/...`, `refactor/...`, `test/...`.

## License

MIT — see [LICENSE](LICENSE).