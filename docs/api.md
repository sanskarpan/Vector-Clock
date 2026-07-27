# REST & WebSocket API

The backend serves on `:8080`. All REST endpoints are under `/api/v1/`. Authentication is optional — set `VC_API_TOKENS` to enable Bearer-token auth on non-health endpoints.

---

## Authentication

When `VC_API_TOKENS` is set (comma-separated `name:token` pairs), all non-exempt endpoints require:

```
Authorization: Bearer <token>
```

Exempt (no auth required): `/healthz`, `/readyz`, `/metrics`.

The WebSocket endpoint `/ws` **is** protected when auth is enabled. Pass the token as a query parameter or header during the upgrade:

```bash
wscat -c "ws://localhost:8080/ws" -H "Authorization: Bearer mytoken"
```

---

## Health endpoints

### `GET /healthz`

Liveness probe. Returns `200` when the process is alive.

```json
{"status":"ok"}
```

### `GET /readyz`

Readiness probe. Returns `200` when the simulation is initialised, `503` otherwise.

```json
{"status":"ok"}
```

---

## Metrics

### `GET /metrics`

Prometheus text-format metrics. Scraped by Prometheus; view in Grafana.

Key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `vc_http_requests_total` | counter | HTTP requests by method, route, status |
| `vc_http_request_duration_seconds` | histogram | Request latency |
| `vc_http_in_flight_requests` | gauge | Currently active HTTP requests |
| `vc_http_errors_total` | counter | HTTP 4xx/5xx responses |
| `vc_ws_clients_connected` | gauge | Active WebSocket clients |
| `vc_ws_messages_sent_total` | counter | WS events broadcast |
| `vc_ws_events_dropped_total` | counter | WS events dropped (slow client) |
| `vc_panics_recovered_total` | counter | Recovered panics in handlers |

---

## Processes

### `POST /api/v1/processes`

Spawn a new process.

**Request body**:
```json
{
  "id": "P1",
  "clock_type": "vector"
}
```

`clock_type`: `lamport` | `vector` | `matrix` (default: `vector`)

**Response** `201`:
```json
{
  "id": "P1",
  "clock_type": "vector",
  "peers": ["P2", "P3"]
}
```

**Errors**: `400` if `id` already exists; `422` if `clock_type` is invalid.

---

### `GET /api/v1/processes`

List all live processes.

**Response** `200`:
```json
[
  {"id": "P1", "clock_type": "vector", "clock": {"P1":3,"P2":1,"P3":0}},
  {"id": "P2", "clock_type": "vector", "clock": {"P1":1,"P2":4,"P3":2}}
]
```

---

### `GET /api/v1/processes/:id`

Get a single process state.

**Response** `200`:
```json
{
  "id": "P1",
  "clock_type": "vector",
  "clock": {"P1":3,"P2":1,"P3":0},
  "hold_back_queue": [],
  "peers": ["P2","P3"]
}
```

**Errors**: `404` if process not found.

---

### `DELETE /api/v1/processes/:id`

Kill a process. Deregisters it from the snapshot coordinator and removes it from the transport.

**Response** `204` (no body).

---

## Messages

### `POST /api/v1/messages`

Send a message between processes.

**Request body**:
```json
{
  "from":    "P1",
  "to":      "P2",
  "payload": "hello world"
}
```

**Response** `202`:
```json
{
  "queued": true,
  "from": "P1",
  "to":   "P2"
}
```

The message is asynchronously delivered — subscribe to the WebSocket to observe delivery events.

---

### `POST /api/v1/broadcast`

Broadcast a message from one process to all peers.

**Request body**:
```json
{"from": "P1", "payload": "broadcast message"}
```

**Response** `202`.

---

## Scenarios

### `GET /api/v1/scenarios`

List available scenarios.

**Response** `200`:
```json
[
  "BasicLamport",
  "CausalViolation",
  "CausalDelivery",
  "Snapshot3P",
  "ConcurrentWrites",
  "FalseConflict",
  "MatrixGC",
  "PartitionAndHeal"
]
```

---

### `POST /api/v1/scenarios/:name/run`

Run a named scenario. Resets the simulation state, spawns the required processes, and executes the scenario script.

**Response** `200`:
```json
{
  "scenario":    "Snapshot3P",
  "duration_ms": 312
}
```

**Errors**: `404` if scenario not found; `409` if another scenario is running.

---

## Snapshots

### `POST /api/v1/snapshots`

Initiate a Chandy-Lamport global snapshot.

**Request body**:
```json
{"initiator": "P1"}
```

**Response** `202`:
```json
{"snapshot_id": "snap-1722074919"}
```

Subscribe to WebSocket to receive `snapshot_finalized` when complete.

---

### `GET /api/v1/snapshots`

List completed snapshots.

**Response** `200`:
```json
[
  {
    "id": "snap-1722074919",
    "initiated_by": "P1",
    "finalized_at": "2026-07-27T11:00:05Z",
    "process_states": {
      "P1": {"clock": {"P1":3,"P2":1}, "kv": {}},
      "P2": {"clock": {"P1":2,"P2":4}, "kv": {}},
      "P3": {"clock": {"P1":1,"P2":3}, "kv": {}}
    },
    "channel_states": {
      "P1→P2": [{"payload":"m1","clock":{"P1":2}}],
      "P2→P3": []
    }
  }
]
```

---

### `GET /api/v1/snapshots/:id`

Get a specific snapshot.

---

## Fault injection

### `POST /api/v1/channels/:from/:to/delay`

Inject a fixed delay on a channel.

**Request body**:
```json
{"delay_ms": 200}
```

**Response** `200`.

---

### `POST /api/v1/channels/:from/:to/drop`

Set the drop probability on a channel.

**Request body**:
```json
{"probability": 0.3}
```

---

### `POST /api/v1/channels/:from/:to/reset`

Reset all fault injection on a channel.

---

### `POST /api/v1/partition`

Create a network partition between two sets of processes.

**Request body**:
```json
{
  "side_a": ["P1","P2"],
  "side_b": ["P3","P4"]
}
```

---

### `DELETE /api/v1/partition`

Heal the current partition.

---

## KV store

### `PUT /api/v1/kv/:key`

Write a value. The server assigns a version using the writing process's vector clock.

**Request body**:
```json
{
  "value":    "hello",
  "writer":   "P1"
}
```

**Response** `200`:
```json
{
  "key":     "x",
  "value":   "hello",
  "version": {"P1":3,"P2":0}
}
```

---

### `GET /api/v1/kv/:key`

Read all versions of a key (may be multiple if conflicts exist).

**Response** `200`:
```json
{
  "key": "x",
  "versions": [
    {"value":"hello","version":{"P1":3,"P2":0},"writer":"P1"},
    {"value":"world","version":{"P1":0,"P2":2},"writer":"P2"}
  ],
  "conflict": true
}
```

---

## Configuration

### `GET /api/v1/config`

Return the running configuration (sanitised — tokens redacted).

### `PATCH /api/v1/config`

Apply partial configuration update at runtime (limited fields).

---

## WebSocket

### `GET /ws`  →  WebSocket upgrade

Subscribe to the real-time event stream. Receives all events as JSON objects — see [Architecture: WebSocket protocol](architecture.md#websocket-protocol) for the full event catalogue.

```bash
# Using wscat
wscat -c ws://localhost:8080/ws

# Using websocat
websocat ws://localhost:8080/ws
```

Example stream:

```json
{"type":"process_spawned","ts":"2026-07-27T11:00:00Z","data":{"pid":"P1","clock_type":"vector"}}
{"type":"clock_tick","ts":"2026-07-27T11:00:01Z","data":{"pid":"P1","clock":{"P1":1,"P2":0,"P3":0}}}
{"type":"message_sent","ts":"2026-07-27T11:00:02Z","data":{"from":"P1","to":"P2","payload":"hello","clock":{"P1":2,"P2":0}}}
{"type":"message_delivered","ts":"2026-07-27T11:00:02Z","data":{"from":"P1","to":"P2","payload":"hello","clock":{"P1":2,"P2":1}}}
```
