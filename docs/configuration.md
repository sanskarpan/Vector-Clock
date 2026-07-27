# Configuration Reference

The Vector Clock Lab is configured via `config.yaml` (path set by `CONFIG_PATH` env var, default `./config.yaml`) and environment variable overrides. Environment variables take precedence over `config.yaml` values.

---

## config.yaml reference

```yaml
server:
  port: 8080          # HTTP/WS listen port
  ws_buffer: 256      # WebSocket outbound channel buffer per client

simulation:
  initial_processes: 3     # processes to spawn on startup
  clock_type: vector       # lamport | vector | matrix
  delivery_mode: causal    # immediate | causal | total_order
  channels: full_mesh      # full_mesh | ring | custom

timing:
  internal_event_interval: 0       # 0 = no automatic ticks; duration string e.g. "100ms"
  message_transit_delay: 50ms      # simulated network latency per message
  reorder_probability: 0.0         # fraction of messages reordered (0.0–1.0)
  drop_probability: 0.0            # fraction of messages dropped (0.0–1.0)

snapshot:
  fifo_channels: true   # enforce FIFO (required for Chandy-Lamport correctness)

kv:
  conflict_strategy: keep_all    # lww | first_writer | merge | keep_all

frontend:
  port: 3001                      # BFF listen port
  go_backend: http://localhost:8080  # backend URL for BFF proxy

logging:
  level: info     # debug | info | warn | error
  format: json    # json | text
```

---

## Environment variables

All env vars override the corresponding `config.yaml` field.

### Server

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP/WS listen port (overrides `server.port`) |
| `CONFIG_PATH` | `./config.yaml` | Path to config.yaml |
| `VC_API_TOKENS` | _(unset)_ | Comma-separated `name:token` pairs. When set, auth is required on non-exempt endpoints. Example: `admin:secret123,reader:readonly456` |
| `VC_ALLOWED_ORIGINS` | _(unset)_ | Comma-separated allowed WebSocket origins. Empty = same-origin only. Example: `http://localhost:3001,https://myapp.example.com` |

### TLS

| Variable | Default | Description |
|----------|---------|-------------|
| `VC_TLS_CERT_FILE` | _(unset)_ | Path to TLS certificate PEM. When set, TLS is enabled. |
| `VC_TLS_KEY_FILE` | _(unset)_ | Path to TLS private key PEM. Required when cert is set. |
| `VC_TLS_CLIENT_CA_FILE` | _(unset)_ | Path to CA bundle for mTLS. When set, client certificate is required. |
| `VC_TLS_RELOAD_INTERVAL` | _(unset)_ | Duration string (e.g. `5m`). Hot-reload cert from disk at this interval. |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `LOGGING_LEVEL` | `info` | `debug` | `info` | `warn` | `error` |
| `LOGGING_FORMAT` | `json` | `json` | `text` |

### OpenTelemetry

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER` | `none` | `none` | `stdout` | `otlp` |
| `OTEL_ENDPOINT` | `http://localhost:4318` | OTLP HTTP collector endpoint |
| `OTEL_SERVICE_NAME` | `vectorclock-lab` | Service name in traces |
| `OTEL_SAMPLE_RATIO` | `1.0` | Trace sampling ratio (0.0–1.0). Use `0.1` in production. |

---

## Configuration profiles

### Development (defaults)

```yaml
simulation:
  initial_processes: 3
  clock_type: vector
  delivery_mode: causal
timing:
  message_transit_delay: 50ms
logging:
  level: debug
  format: text
```

### Production

```yaml
simulation:
  initial_processes: 5
  clock_type: vector
  delivery_mode: causal
timing:
  message_transit_delay: 10ms
logging:
  level: info
  format: json
```

```bash
# Production env overrides
export VC_API_TOKENS="admin:$(openssl rand -hex 32)"
export VC_ALLOWED_ORIGINS="https://vectorclock.example.com"
export VC_TLS_CERT_FILE=/etc/certs/tls.crt
export VC_TLS_KEY_FILE=/etc/certs/tls.key
export VC_TLS_RELOAD_INTERVAL=5m
export OTEL_EXPORTER=otlp
export OTEL_ENDPOINT=http://otel-collector:4318
export OTEL_SAMPLE_RATIO=0.05
```

### Teaching / classroom (no auth, verbose logging)

```yaml
simulation:
  initial_processes: 2
  clock_type: lamport
  delivery_mode: immediate
timing:
  message_transit_delay: 200ms  # slow enough to observe events
logging:
  level: debug
  format: text
```

---

## Conflict resolution strategies

| Strategy | Behaviour |
|----------|-----------|
| `lww` | Last-writer-wins: the write with the higher Lamport timestamp wins. |
| `first_writer` | First-writer-wins: earlier timestamp wins. |
| `merge` | Merge all concurrent values into a single combined value (type-specific). |
| `keep_all` | Retain all concurrent versions (default). Returns multiple values on read. |

---

## Delivery modes

| Mode | Behaviour |
|------|-----------|
| `immediate` | Messages are delivered as soon as they arrive, regardless of causal order. |
| `causal` | BSS hold-back queues enforce causal ordering. Messages held until all causal dependencies are satisfied. |
| `total_order` | Not yet implemented (reserved for future work). |

---

## Rate limiting

The gateway applies token-bucket rate limiting per source IP:

- Default: 100 requests/minute, burst of 20.
- Not configurable via `config.yaml` in the current release; adjust in `gateway/server.go`.
