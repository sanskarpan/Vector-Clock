# k6 Load Tests

## Prerequisites
- k6 installed (https://k6.io/docs/getting-started/installation/)
- Server running on localhost:8080

## Run tests

```bash
# Smoke test (1 VU, 30s) — quick sanity check
k6 run --vus 1 --duration 30s test/k6/scenarios.js

# Load test (50 VUs, 2min) — sustained traffic
k6 run --vus 50 --duration 2m test/k6/scenarios.js

# Stress test (ramp to 200 VUs, 3min) — find breaking point
k6 run --stage 1m:50,1m:100,1m:200 --duration 3m test/k6/scenarios.js

# Soak test (30 VUs, 5min) — detect memory leaks
k6 run --vus 30 --duration 5m test/k6/scenarios.js

# Chaos / fault injection test
k6 run test/k6/chaos.js
```

## Options

```bash
# Override server URL
BASE_URL=http://localhost:9090 k6 run test/k6/scenarios.js

# Custom VUs and duration
k6 run --vus 100 --duration 5m test/k6/scenarios.js

# Output results to JSON
k6 run --out json=results.json test/k6/scenarios.js

# HTML report (requires xk6-output-plugin or k6 v51+)
k6 run --out html=report.html test/k6/scenarios.js
```

## Test scenarios

| Test       | VUs  | Duration | Purpose                     |
|------------|------|----------|-----------------------------|
| Smoke      | 1    | 30s      | Verify basic functionality  |
| Load       | 50   | 2m       | Sustained normal traffic    |
| Stress     | 200  | 3m       | Find breaking point         |
| Soak       | 30   | 5m       | Detect memory/resource leaks|
| Chaos      | 10-20| 2m       | Fault injection resilience  |

## Metrics tracked

- `events_per_second` — rate of message send events
- `snapshot_completion_rate` — rate of successful snapshot triggers
- `msg_send_duration` — trend of message send latency
- `scenario_duration` — trend of scenario execution time
- `fault_recovery_rate` — system stays healthy during faults
- `process_respawn_rate` — successful process respawns after kills
- `rate_limit_observed` — rate limiting seen during burst traffic

## Architecture

The test suite exercises all major API endpoints:
- `/healthz` — liveness probe
- `/api/v1/simulation/state` — simulation state
- `/api/v1/processes` — process lifecycle (spawn, get, kill)
- `/api/v1/messages` — inter-process messaging
- `/api/v1/scenarios/:name/run` — pre-built scenarios
- `/api/v1/processes/:id/snapshot` — Chandy-Lamport snapshots
- `/api/v1/faults/*` — fault injection (delay, drop)
