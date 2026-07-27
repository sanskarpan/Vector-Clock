# Testing Strategy

The Vector Clock Lab uses a four-tier test pyramid: unit tests per package, integration tests for the full simulation, E2E tests against the live HTTP server, and K6 load/chaos tests. All tiers pass with `-race` enabled.

---

## Test pyramid

```
               ┌─────────────────────┐
               │   Playwright (22)   │  Browser — component rendering
               └─────────────────────┘
              ┌───────────────────────┐
              │   K6 load + chaos     │  Performance, fault injection
              └───────────────────────┘
             ┌─────────────────────────┐
             │   E2E (test/e2e/)       │  Live server, HTTP + WebSocket
             └─────────────────────────┘
            ┌───────────────────────────┐
            │   Integration            │  Simulation engine + scenarios
            └───────────────────────────┘
           ┌─────────────────────────────┐
           │   Unit (per package)        │  Clock logic, causality, snapshot
           └─────────────────────────────┘
```

---

## Running all tests

```bash
# All Go tests, race detector, 2-minute timeout
make test-race

# Equivalent
go test ./... -race -count=1 -timeout=180s

# With coverage report
make test-coverage    # fails if total < 60%
```

---

## Unit tests

Each internal package has its own `*_test.go` file. Key packages:

### `internal/clock/`

- `lamport/clock_test.go` — send/receive/tick rules, ordering invariants.
- `vector/clock_test.go`, `compare_test.go`, `diff_test.go` — comparison operators, strong clock condition, fuzz tests.
- `vector/fuzz_test.go` — Go fuzzer (random vector pairs, checks `A→B ⟺ V(A)<V(B)`).
- `matrix/clock_test.go` — MC1–MC4 rules, step-3 third-party merge.
- `version/` and `dvv/` — dominance, conflict detection, dot merge.

### `internal/causality/`

- `graph_test.go` — BFS causal path, transitivity, visited-set bound.

### `internal/snapshot/`

- `snapshot_test.go` — 3-process coordinator, concurrent initiators, marker forwarding, all 6 `OnMarkerReceived` call sites.

### `internal/process/`

- `process_test.go` — hold-back queue flush, `snapshotLocked` correctness.

### Run a single package

```bash
go test ./internal/clock/vector/... -v -race
go test ./internal/snapshot/... -v -race -run TestConcurrentInitiators
```

---

## Integration tests

**File**: `test/integration/integration_test.go`

Tests the full simulation engine without HTTP:

```go
func TestIntegration_Snapshot3P(t *testing.T) {
    sim := simulation.New(config.Default())
    sim.SpawnProcess("P1", "vector")
    sim.SpawnProcess("P2", "vector")
    sim.SpawnProcess("P3", "vector")

    // Run scenario
    err := sim.RunScenario("Snapshot3P")
    require.NoError(t, err)

    // Verify snapshot finalized
    snaps := sim.Snapshots()
    require.Len(t, snaps, 1)
    require.True(t, snaps[0].IsConsistent())
}
```

Covers: all 8 scenarios, causal delivery hold-back, conflict detection, KV store strategies.

```bash
go test ./test/integration/... -v -race -timeout=120s
```

---

## E2E tests

**File**: `test/e2e/e2e_test.go`

Tests the live HTTP + WebSocket server. The test starts the server binary, runs requests, and validates responses.

### Key helpers

```go
// authDialWS dials the WebSocket with a Bearer token header
func authDialWS(t *testing.T, url, token string) *websocket.Conn {
    header := http.Header{}
    header.Set("Authorization", "Bearer "+token)
    conn, _, err := websocket.DefaultDialer.Dial(url, header)
    require.NoError(t, err)
    return conn
}
```

### Test cases

| Test | What it validates |
|------|------------------|
| `TestE2E_HealthEndpoints` | `/healthz`, `/readyz` return 200 |
| `TestE2E_SpawnAndMessage` | Full message flow via REST + WS subscription |
| `TestE2E_Snapshot` | Snapshot initiation, finalization, consistent cut |
| `TestE2E_FaultInjection` | Drop probability, delay injection, channel reset |
| `TestE2E_CausalDelivery` | Hold-back queue events fire in correct order |
| `TestE2E_ConflictDetection` | Concurrent KV writes produce `kv_conflict` event |
| `TestE2E_Auth` | 401 without token, 200 with valid Bearer token |
| `TestE2E_CustomConfig` | Server respects `config.yaml` overrides |
| `TestE2E_WebSocketOrigin` | Empty origin rejected when allowlist configured |

```bash
# Run with auth enabled
VC_API_TOKENS="test:e2e-secret" go test ./test/e2e/... -v -timeout=180s
```

---

## K6 load and chaos tests

**Directory**: `test/k6/`

[k6](https://k6.io) scripts exercise the system under load and chaos conditions.

### Scenarios

| Script | What it does |
|--------|-------------|
| `scenarios.js` | Ramps to 50 VUs over 2 minutes; verifies P99 latency < 200 ms |
| `chaos.js` | Injects faults (drop, delay) while maintaining 20 VUs; validates the system recovers |

```bash
# Install k6
brew install k6

# Run load test against local server
k6 run test/k6/scenarios.js --env BASE_URL=http://localhost:8080

# Run chaos test
k6 run test/k6/chaos.js --env BASE_URL=http://localhost:8080
```

### Performance targets

| Metric | Target |
|--------|--------|
| REST P99 latency | < 50 ms |
| WS event fan-out (10 clients) | < 5 ms |
| Snapshot (3 processes) | < 500 ms end-to-end |
| Throughput | 500 req/s sustained on 2 CPU |

---

## Playwright browser tests

**Directory**: `test/playwright/`

22 browser tests covering all 6 frontend components.

### Setup

```bash
cd frontend
bun install
bunx playwright install chromium
```

### Running

```bash
bunx playwright test                     # all 22 tests (headless)
bunx playwright test --headed            # with browser window
bunx playwright test --ui                # interactive UI mode
bunx playwright test ClockInspector      # single component
```

### What's tested

| Test group | Coverage |
|-----------|---------|
| SpaceTimeDiagram | Event rendering, causal arrows, scrubber, live mode, cut overlay |
| ClockInspector | Clock display for lamport/vector/matrix, hold-back queue |
| CausalDelivery | Hold-back queue bar chart, held/delivered events |
| SnapshotViewer | Snapshot list, process states, channel states |
| ConflictDash | Conflict detection display, version vector comparison |
| ScenarioPanel | Cards, run button, live indicator, history |

### Setup helper

```typescript
// test/playwright/setup.ts
async function setupTestPage(page: Page, events: WSEvent[]) {
    // Mocks the WebSocket connection and API calls
    await page.route('/api/v1/**', mockAPIHandler)
    await page.addInitScript(() => {
        window.__mockEvents = events
    })
    await page.goto('http://localhost:3001')
}
```

---

## CI integration

The GitHub Actions workflow (`.github/workflows/ci.yml`) runs:

1. `go build ./...` — compile check
2. `go vet ./...` + `golangci-lint` — static analysis
3. `go test ./... -race -timeout=120s` — unit + integration
4. `go test ./test/e2e/... -timeout=180s` (with `VC_API_TOKENS`) — E2E
5. Playwright tests — frontend browser suite (separate `frontend` job)

See [Deployment → CI](deployment.md) for the full workflow configuration.
