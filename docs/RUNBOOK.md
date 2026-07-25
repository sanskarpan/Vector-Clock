# Operations Runbook

This runbook covers deployment, rollback, and the top 5 most likely production
failure modes for the Vector Clock Lab backend.

## Deployment

### Prerequisites

- Container image built and pushed to your registry (default: `vectorclock/server`).
- Kubernetes 1.27+ or Docker Compose 2.20+.
- An ingress controller that supports WebSocket upgrade (nginx-ingress, Envoy,
  ALB).

### Rolling out

**Kubernetes** (recommended):

```sh
kubectl apply -f deploy/
kubectl rollout status deployment/vectorclock-server
kubectl get pods -l app=vectorclock-server
```

**Docker Compose**:

```sh
docker compose pull
docker compose up -d
docker compose logs -f server
```

### Configuration injection

| Concern            | Method                                       |
| ------------------ | -------------------------------------------- |
| Listen port        | `PORT` env var                               |
| Allowed origins    | `VC_ALLOWED_ORIGINS` env var                 |
| Initial processes  | `config.yaml` mounted at `$CONFIG_PATH`      |
| Logging level      | `LOGGING_LEVEL` env var or `config.yaml`     |
| Logging format     | `LOGGING_FORMAT` env var or `config.yaml`    |

### Smoke test after deploy

```sh
# Liveness
curl -fsS https://server.example.com/healthz
# Readiness
curl -fsS https://server.example.com/readyz
# Metrics endpoint returns Prometheus text
curl -fsS https://server.example.com/metrics | head
# Simulation is responsive
curl -fsS https://server.example.com/api/v1/simulation/state | jq '.processes | length'
```

If any check fails, roll back (next section).

## Rollback

### Kubernetes

```sh
kubectl rollout undo deployment/vectorclock-server
kubectl rollout status deployment/vectorclock-server
```

### Docker Compose

```sh
# Re-deploy the previous image
docker compose down
docker compose pull <previous-tag>
docker compose up -d
```

### Verify rollback

```sh
curl -fsS https://server.example.com/healthz
docker logs --tail 50 vectorclock-server | grep "shutdown\|startup"
```

If a partial deploy is stuck in CrashLoopBackOff, scale to zero first:

```sh
kubectl scale deployment/vectorclock-server --replicas=0
```

## Top 5 failure modes

### 1. WebSocket clients stuck / accumulating

**Symptoms**: `/metrics` shows `vc_ws_clients_connected > 1000` or growing
without bound. CPU on server steady, memory growing.

**Likely cause**: Network path is dropping pings (firewall NAT timeout < 30s)
or clients are not responding to pongs. Connections not detected as dead
because the WS read deadline is not firing.

**Diagnosis**:

```sh
# Check WS client gauge
curl -fsS server:8080/metrics | grep vc_ws_clients_connected
# Look for recent reconnects in logs
kubectl logs -l app=vectorclock-server --tail 200 | grep "ws upgrade\|ws close"
```

**Mitigation**:

1. Increase `WSPongDeadline` (default 60s) and `WSPingInterval` (default 30s)
   to values below the upstream NAT idle timeout.
2. Restart the server to drop all clients cleanly:
   `kubectl rollout restart deployment/vectorclock-server`.
3. Investigate the network path between client and server.

### 2. Event bus dropping events under load

**Symptoms**: `/metrics` shows `vc_ws_events_dropped_total` or the
`vc_event_bus_publish_drops` counter incrementing. Clients see gaps in the
event stream.

**Likely cause**: Publish rate exceeds the bus's internal buffer (4096 events)
or individual subscribers can't keep up.

**Diagnosis**:

```sh
curl -fsS server:8080/metrics | grep -E 'drop|publish'
```

**Mitigation**:

1. Reduce event-generation rate in the calling scenario.
2. Bump the bus publish buffer (`internal/events/bus.go` constant).
3. Add additional subscribers to spread the load.

### 3. Goroutine leak after repeated `POST /simulation/reset`

**Symptoms**: `go_goroutines` growing over time after several resets.
Eventually OOM-killed.

**Likely cause**: Pre-hardening version had setSim() that did not release the
old bus subscription or transport. **This should no longer occur** (P5 fix).
If you observe it, the deployed image is pre-hardening — rebuild from main.

**Diagnosis**:

```sh
curl -fsS server:8080/metrics | grep go_goroutines
```

**Mitigation**: Redeploy from a current image. If urgent, restart the pod.

### 4. Snapshot never completes

**Symptoms**: `POST /processes/:id/snapshot` returns a snapshot ID, but
`GET /snapshots/:id/verify` returns `consistent: false` or the snapshot never
appears in `/api/v1/simulation/state`.

**Likely cause**: A process was killed after the snapshot started, leaving
markers undelivered. The snapshot coordinator is still waiting for the missing
process.

**Diagnosis**:

```sh
# Check for orphaned snapshots
curl -fsS server:8080/api/v1/simulation/state | jq .
# Check logs for "marker received" lines
kubectl logs -l app=vectorclock-server --tail 200 | grep marker
```

**Mitigation**: Reset the simulation (`POST /simulation/reset`). The orphaned
snapshot will be GC'd when the simulation is reinitialised.

### 5. Cross-origin WebSocket rejected

**Symptoms**: Browser console shows `WebSocket connection to 'wss://...' failed`
with status 403. Direct `curl` connections work fine.

**Likely cause**: `VC_ALLOWED_ORIGINS` does not include the page's origin.

**Diagnosis**:

```sh
# Check what origins are configured
kubectl exec -it deploy/vectorclock-server -- printenv VC_ALLOWED_ORIGINS
```

**Mitigation**:

```sh
kubectl set env deployment/vectorclock-server \
    VC_ALLOWED_ORIGINS=https://app.example.com,https://staging.example.com
kubectl rollout restart deployment/vectorclock-server
```

## On-call escalation

| Severity            | Response time | Escalation          |
| ------------------- | ------------- | ------------------- |
| S1 (total outage)   | 15 min        | Primary → Secondary → Manager |
| S2 (degraded)       | 1 hour        | Primary → Secondary |
| S3 (single feature) | Next business | Primary             |

### Contacts

- **Primary on-call**: pager rotation, see PagerDuty schedule "vc-backend".
- **Secondary on-call**: backup rotation.
- **Engineering manager**: see team page.

### Comms

- Status page: https://status.example.com
- Incident channel: #incidents-vectorclock (Slack)
- Customer comms template: `docs/comm-templates/incident.md`