# Deployment Guide

The Vector Clock Lab is packaged as a single Go binary + a static frontend bundle. This guide covers Docker Compose, Kubernetes, TLS, Prometheus, and OpenTelemetry.

---

## Quick start — Docker Compose

```bash
git clone https://github.com/sanskarpan/Vector-Clock.git
cd Vector-Clock
docker compose up -d

# Verify
curl http://localhost:8080/healthz   # {"status":"ok"}
open http://localhost:3001
```

`docker-compose.yml` starts two services:
- `server` — Go backend on `:8080`
- `frontend` — Bun BFF on `:3001`

---

## Docker Compose with TLS + auth

```yaml
# docker-compose.prod.yml
services:
  server:
    image: ghcr.io/sanskarpan/vector-clock:latest
    ports:
      - "8443:8443"
    environment:
      PORT: 8443
      VC_TLS_CERT_FILE: /certs/tls.crt
      VC_TLS_KEY_FILE:  /certs/tls.key
      VC_TLS_RELOAD_INTERVAL: 5m
      VC_API_TOKENS: "admin:$ADMIN_TOKEN"
      VC_ALLOWED_ORIGINS: "https://vectorclock.example.com"
      OTEL_EXPORTER: otlp
      OTEL_ENDPOINT: http://otel-collector:4318
    volumes:
      - ./certs:/certs:ro
      - ./config.yaml:/app/config.yaml:ro
```

```bash
ADMIN_TOKEN=$(openssl rand -hex 32) docker compose -f docker-compose.prod.yml up -d
```

---

## Building the image

```bash
docker build -t vectorclock/server:latest .
```

The `Dockerfile` uses a multi-stage build:
1. `golang:1.22-alpine` — builds the Go binary.
2. `gcr.io/distroless/static` — runtime image (no shell, no package manager).

The binary is statically linked (`CGO_ENABLED=0`), so the distroless image works without any glibc.

---

## Kubernetes

The `deploy/k8s/` directory contains production-ready manifests:

```
deploy/k8s/
├── namespace.yaml
├── deployment.yaml       # 2 replicas, readiness/liveness probes
├── service.yaml          # ClusterIP :8080
├── ingress.yaml          # nginx-ingress, WebSocket annotation
├── hpa.yaml              # HorizontalPodAutoscaler (2–10 replicas)
├── pdb.yaml              # PodDisruptionBudget (minAvailable: 1)
├── configmap.yaml        # config.yaml as a ConfigMap
└── secret.yaml.template  # TLS cert + API token secret template
```

### Deploy

```bash
# Create namespace and secrets
kubectl apply -f deploy/k8s/namespace.yaml
kubectl create secret generic vectorclock-tls \
  --from-file=tls.crt=./certs/tls.crt \
  --from-file=tls.key=./certs/tls.key \
  -n vectorclock

kubectl create secret generic vectorclock-tokens \
  --from-literal=tokens="admin:$(openssl rand -hex 32)" \
  -n vectorclock

# Apply all manifests
kubectl apply -f deploy/k8s/

# Check rollout
kubectl rollout status deployment/vectorclock-server -n vectorclock
kubectl get pods -n vectorclock
```

### Ingress (nginx)

WebSocket requires special annotations:

```yaml
annotations:
  nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
  nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
  nginx.ingress.kubernetes.io/configuration-snippet: |
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
```

### Scaling

The backend is stateless (no persistent storage). Scale horizontally:

```bash
kubectl scale deployment vectorclock-server --replicas=4 -n vectorclock
```

!!! note "Snapshot coordinator is per-process"
    The `SnapshotCoordinator` is in-process and not shared across replicas. In a multi-replica deployment, snapshot state is local to each pod. If you need cross-replica snapshot coordination, pin WebSocket clients to a single pod (session affinity).

---

## TLS configuration

TLS is enabled when `VC_TLS_CERT_FILE` is set. The server uses Mozilla Intermediate defaults:

- Minimum TLS version: 1.2
- AEAD cipher suites only (ChaCha20-Poly1305, AES-GCM)
- ALPN: `h2`, `http/1.1`

### Hot reload

Set `VC_TLS_RELOAD_INTERVAL` (e.g. `5m`) to reload the certificate from disk at that interval. The reload is atomic — in-flight connections are not interrupted. This allows cert-manager renewals with zero downtime.

```bash
# Check TLS and cert expiry
openssl s_client -connect localhost:8443 -servername localhost < /dev/null 2>&1 | grep "Not After"
```

### mTLS

Set `VC_TLS_CLIENT_CA_FILE` to require client certificates. Useful for service-to-service auth in internal deployments.

---

## Prometheus metrics

The `/metrics` endpoint serves Prometheus text-format metrics. Scrape it from Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: vectorclock
    static_configs:
      - targets: ['vectorclock-server:8080']
```

**Key dashboards** (Grafana):

| Panel | Query |
|-------|-------|
| Request rate | `rate(vc_http_requests_total[5m])` |
| Error rate | `rate(vc_http_errors_total[5m])` |
| P99 latency | `histogram_quantile(0.99, rate(vc_http_request_duration_seconds_bucket[5m]))` |
| Active WS clients | `vc_ws_clients_connected` |
| Dropped WS events | `rate(vc_ws_events_dropped_total[5m])` |
| Recovered panics | `increase(vc_panics_recovered_total[1h])` |

---

## OpenTelemetry tracing

Set `OTEL_EXPORTER=otlp` to send traces to a Jaeger/Tempo/Grafana collector:

```bash
OTEL_EXPORTER=otlp \
OTEL_ENDPOINT=http://otel-collector:4318 \
OTEL_SERVICE_NAME=vectorclock-lab \
OTEL_SAMPLE_RATIO=0.1 \
./bin/vectorclock-server
```

### Trace coverage

- Every HTTP request: method, route, status, duration.
- WebSocket connections: connect, disconnect, events dropped.
- Simulation operations: `SpawnProcess`, `SendMessage`, `TriggerSnapshot`.
- Snapshot lifecycle: initiate → markers → finalise.

Set `OTEL_EXPORTER=stdout` for local debugging (JSON to stderr).

---

## Health checks

```bash
# Liveness (process alive)
curl http://localhost:8080/healthz      # 200 {"status":"ok"}

# Readiness (simulation initialised)
curl http://localhost:8080/readyz       # 200 {"status":"ok"} or 503
```

Kubernetes probes:
```yaml
livenessProbe:
  httpGet:
    path: /healthz
    port: 8080
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /readyz
    port: 8080
  initialDelaySeconds: 3
  periodSeconds: 5
```

---

## Environment reference (quick card)

```bash
# Minimal production env
export PORT=8080
export VC_API_TOKENS="admin:$(openssl rand -hex 32)"
export VC_ALLOWED_ORIGINS="https://your-frontend.example.com"
export VC_TLS_CERT_FILE=/certs/tls.crt
export VC_TLS_KEY_FILE=/certs/tls.key
export VC_TLS_RELOAD_INTERVAL=5m
export LOGGING_LEVEL=info
export LOGGING_FORMAT=json
export OTEL_EXPORTER=otlp
export OTEL_ENDPOINT=http://otel-collector:4318
export OTEL_SAMPLE_RATIO=0.05
```

For the full variable reference, see [Configuration](configuration.md).

---

## Operations runbook

See [Operations Runbook](RUNBOOK.md) for deployment, rollback, and the top 5 failure modes.
