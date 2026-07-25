# CHECKLIST.md — Vector Clocks & Causal Consistency System
## Implementation Tracker

> Legend: 🔴 Blocking · 🟡 Important · 🟢 Enhancement · 🔵 Stretch
> Progress: 259/261 core tasks complete (~99%) · 9 remaining stretch goals

---

## Phase 0 — Project Bootstrap

### 0.1 Repository & Go Setup
- [x] 🔴 `go mod init github.com/DistributedClocks/vectorclock-system`
- [x] 🔴 Create full directory tree per SPEC §18
- [x] 🔴 Install deps: `gorilla/websocket`, `gin-gonic/gin`, `uber-go/zap`, `spf13/viper`
- [x] 🔴 Create `config.yaml` with defaults from SPEC §15
- [x] 🔴 Create `Makefile`: `build`, `test`, `run`, `lint`, `race` targets

### 0.2 Bun+TS Frontend Setup
- [x] 🔴 `bun init` in `frontend/` directory
- [x] 🔴 `bun add elysia @elysiajs/cors @elysiajs/eden`
- [x] 🔴 `bun add d3 nanostores`
- [x] 🔴 `bun add -d typescript tailwindcss @types/d3`
- [x] 🔴 Configure `tsconfig.json` (strict mode, path aliases)
- [x] 🔴 Configure `bunfig.toml`
- [x] 🔴 Create Elysia BFF stub (`frontend/server/bff.ts`) — proxies REST + WS
- [x] 🔴 Create `index.html` entry point with dark-theme shell
- [x] 🔴 Create placeholder `src/main.ts` — app shell with component wiring

---

## Phase 1 — Lamport Scalar Clocks (`internal/clock/lamport/`)

### 1.1 Implementation
- [x] 🔴 `clock.go` — `LamportClock` struct with `sync.Mutex` and `uint64` value
- [x] 🔴 `clock.go` — `Tick() uint64` — LC1: `c++; return c`
- [x] 🔴 `clock.go` — `Send() uint64` — LC2: same as Tick, for send events
- [x] 🔴 `clock.go` — `Receive(ts uint64) uint64` — LC3: `c = max(c, ts) + 1`
- [x] 🔴 `clock.go` — `Value() uint64` — read without incrementing
- [x] 🔴 `clock.go` — `TotalOrder(ts1 uint64, pid1, ts2 uint64, pid2 string) int` — tie-break by pid

### 1.2 Tests (`clock_test.go`)
- [x] 🔴 Tick increments by exactly 1
- [x] 🔴 Receive: `LC = max(local, received) + 1` for all cases
- [x] 🔴 TotalOrder: for concurrent events (same LC), pid breaks tie consistently
- [x] 🔴 TotalOrder produces strict total ordering (100 events, verify transitivity)
- [x] 🔴 Concurrent-safe: 10 goroutines ticking simultaneously → no race (`-race` flag)

---

## Phase 2 — Vector Clocks (`internal/clock/vector/`)

### 2.1 Core Implementation
- [x] 🔴 `clock.go` — `VectorClock` type as `map[string]uint64`
- [x] 🔴 `clock.go` — `New(pids []string) VectorClock` — zero-initialized
- [x] 🔴 `clock.go` — `Tick(vc VectorClock, pid string) VectorClock` — VC1
- [x] 🔴 `clock.go` — `Send(vc VectorClock, pid string) (VectorClock, VectorClock)` — VC2
- [x] 🔴 `clock.go` — `Receive(local VectorClock, msg VectorClock, pid string) VectorClock` — VC3
- [x] 🔴 `clock.go` — `MergePassive(a, b VectorClock) VectorClock` — pointwise max (no tick)
- [x] 🔴 `clock.go` — `Copy(vc VectorClock) VectorClock` — deep copy

### 2.2 Comparison (`compare.go`)
- [x] 🔴 `ClockOrder` type with constants: `HappenedBefore`, `HappenedAfter`, `Concurrent`, `Identical`
- [x] 🔴 `Compare(a, b VectorClock) ClockOrder` — Charron-Bost isomorphism
- [x] 🔴 `LessThan(a, b VectorClock) bool` — `a < b`: all ≤ and ∃ strictly <
- [x] 🔴 `Equal(a, b VectorClock) bool` — all components equal
- [x] 🟡 `ConcurrentSet(events []VCEvent) [][]VCEvent` — partition into concurrent groups

### 2.3 Singhal-Kshemkalyani Optimization
- [x] 🟡 `DiffVC` struct: only changed entries since last send to recipient
- [x] 🟡 `Diff(prev, current VectorClock) DiffVC`
- [x] 🟡 `Apply(base VectorClock, diff DiffVC) VectorClock`

### 2.4 Tests
- [x] 🔴 Isomorphism: `a → b ⟺ Compare(VC(a), VC(b)) == HappenedBefore`
- [x] 🔴 Concurrent detection: events with no causal link → `Compare == Concurrent`
- [x] 🔴 `MergePassive` is commutative: `Merge(a,b) == Merge(b,a)`
- [x] 🔴 `MergePassive` is idempotent: `Merge(a,a) == a`
- [x] 🔴 Compare: all four cases including sparse maps (missing keys = 0)
- [ ] 🟡 Charron-Bost theorem: N processes need N-element VC for strong consistency

---

## Phase 3 — Matrix Clocks (`internal/clock/matrix/`)

### 3.1 Implementation
- [x] 🔴 `clock.go` — `MatrixClock` type as `map[string]map[string]uint64`
- [x] 🔴 `clock.go` — `New(pids []string) MatrixClock` — N×N zero matrix
- [x] 🔴 `clock.go` — `Tick(mc MatrixClock, self string) MatrixClock` — MC1
- [x] 🔴 `clock.go` — `Send(mc MatrixClock, self string) (MatrixClock, MatrixClock)` — MC2
- [x] 🔴 `clock.go` — `Receive(local MatrixClock, msg MatrixClock, self, sender string) MatrixClock` — MC3
- [x] 🔴 `clock.go` — `MinKnowledge(mc MatrixClock, pid string) uint64` — `min_k(MT[k][pid])`
- [x] 🔴 `clock.go` — `CanGC(mc MatrixClock, pid string, upTo uint64) bool`

### 3.2 Tests
- [x] 🔴 After receive: `MT[self]` is pointwise-max with sender's row
- [x] 🔴 `MinKnowledge` returns minimum across all rows for a given column
- [x] 🔴 GC: after N rounds, `MinKnowledge` catches up to all events
- [x] 🔴 4-process simulation: verify matrix correctness after 20 messages

---

## Phase 4 — Version Vectors & DVV (`internal/clock/version/` + `internal/clock/dvv/`)

### 4.1 Version Vectors
- [x] 🔴 `vector.go` — `VersionVector` type + `Update(replicaID string)` + `Sync(other VV) SyncResult`
- [x] 🔴 `vector.go` — `SyncResult`: `Dominated | Dominates | Conflict | Equal`
- [x] 🔴 `vector_test.go` — False conflict scenario

### 4.2 Dotted Version Vectors
- [x] 🔴 `dvv.go` — `Dot` struct: `{Replica string; Counter uint64}`
- [x] 🔴 `dvv.go` — `DVV` struct: `{Vector VectorClock; Dot *Dot}`
- [x] 🔴 `dvv.go` — `DVVSet` for multi-value storage
- [x] 🔴 `dvv.go` — `NewDVV(serverID string, serverClock uint64) DVV`
- [x] 🔴 `dvv.go` — `UpdateDVV(old DVV, serverID string, serverClock uint64) DVV`
- [x] 🔴 `dvv.go` — `Sync(a, b DVVSet) DVVSet` — discard dominated, keep siblings
- [x] 🔴 `dvv_test.go` — Same scenario as VV false conflict → DVV correctly shows NO conflict
- [x] 🔴 `dvv_test.go` — True concurrent writes → DVV shows siblings

---

## Phase 5 — Causality Primitives (`internal/causality/`)

### 5.1 Causal Graph
- [x] 🔴 `graph.go` — `CausalGraph` struct with nodes + edges
- [x] 🔴 `graph.go` — `AddEvent(e *Event)`
- [x] 🔴 `graph.go` — `AddEdge(from, to string)`
- [x] 🔴 `graph.go` — `HappenedBefore(a, b string) bool` — BFS/DFS
- [x] 🔴 `graph.go` — `Concurrent(a, b string) bool`
- [x] 🔴 `graph.go` — `CausalHistory(id string) []*Event`
- [x] 🔴 `graph.go` — `TopologicalSort() []*Event` — Kahn's algorithm

### 5.2 Consistent Cuts
- [x] 🔴 `cut.go` — `Cut` struct: `map[string]int`
- [x] 🔴 `cut.go` — `IsConsistent(events []*Event, messages []*Message) bool`
- [x] 🔴 `cut.go` — `FindConsistentCuts(events []*Event) []Cut`
- [x] 🟡 `cut.go` — `CutBefore(vc VectorClock) Cut`

### 5.3 Tests
- [x] 🔴 3-process graph: `HappenedBefore` correct for all pairs
- [x] 🔴 Consistent cut verification: consistent/inconsistent correctly classified
- [x] 🔴 Transitivity: `a → b` and `b → c` implies `HappenedBefore(a, c) == true`

---

## Phase 6 — Process & Hold-Back Queue (`internal/process/`)

### 6.1 Process Core
- [x] 🔴 `process.go` — `Process` struct
- [x] 🔴 `process.go` — `New(cfg ProcessConfig, transport Transport, bus EventEmitter) *Process`
- [x] 🔴 `process.go` — `Start()` — goroutine over inbound + control channels
- [x] 🔴 `process.go` — `Stop()` — graceful shutdown
- [x] 🔴 `process.go` — `HandleMessage(m *Message)` — route to delivery or hold-back
- [x] 🔴 `process.go` — `InternalEvent()` — tick clock, emit `internal_event`
- [x] 🔴 `process.go` — `Send(to string, data interface{})` — tick clock, emit `send`
- [x] 🔴 `process.go` — `Broadcast(data interface{})` — causal broadcast to all peers
- [x] 🔴 `process.go` — `EmitEvent(t EventType, ...)` — all state changes → event bus

### 6.2 Hold-Back Queue
- [x] 🔴 `holdback.go` — `HoldBackQueue` with `[]*HeldMessage` and mutex
- [x] 🔴 `holdback.go` — `Enqueue(m *Message, vc VectorClock)` — add to queue
- [x] 🔴 `holdback.go` — `TryDeliver(localVC VectorClock, pid string) []*Message` — BSS condition
- [x] 🔴 `holdback.go` — `BlockedBy(m *HeldMessage, localVC VectorClock) []string`
- [x] 🟡 `holdback.go` — `Snapshot() []HeldMessageInfo`

### 6.3 Causal KV Store
- [x] 🟡 `kvstore.go` — `CausalKV` with MVCC
- [x] 🟡 `kvstore.go` — `Get/Put/Merge/Resolve`

### 6.4 Tests
- [x] 🔴 Hold-back queue: message held until causal predecessor delivered
- [x] 🔴 Hold-back queue: no deadlock
- [x] 🔴 ImmediateDelivery produces causal violation in known scenario
- [x] 🔴 CausalDelivery produces no violation in same scenario

---

## Phase 7 — Chandy-Lamport Snapshot (`internal/snapshot/`)

### 7.1 Core Algorithm
- [x] 🔴 `chandy_lamport.go` — `LocalSnapshot` struct
- [x] 🔴 `chandy_lamport.go` — `InitiateSnapshot(initiatorID) (string, error)`
- [x] 🔴 `chandy_lamport.go` — `OnMarkerReceived(pid, from, snapshotID string)`
- [x] 🔴 `chandy_lamport.go` — `RecordMessage(snapshotID, pid, from string, m *Message)`

### 7.2 Coordinator
- [x] 🔴 `SnapshotCoordinator` tracking concurrent snapshots
- [x] 🔴 `IsComplete(expectedProcesses []string) bool`
- [x] 🔴 `GetSnapshot(snapshotID string) *GlobalSnapshot`
- [x] 🔴 Emit `snapshot_complete` event when done

### 7.3 Verifier
- [x] 🔴 `verifier.go` — `Verify(gs *GlobalSnapshot) VerificationResult`
- [ ] 🟡 `verifier.go` — `ExtractConsistentCut(gs *GlobalSnapshot) Cut`

### 7.4 Tests
- [x] 🔴 3-process: marker propagation reaches all processes
- [x] 🔴 Channel states: in-transit messages correctly captured
- [x] 🔴 Verifier: snapshot passes consistency check
- [x] 🔴 Concurrent initiators: two simultaneous initiators → valid snapshot
- [x] 🟡 Messages in channels: `send(m)` pre-cut, `recv(m)` post-cut verified

---

## Phase 8 — Conflict Detection (`internal/conflict/`)

### 8.1 Detector
- [x] 🔴 `detector.go` — `ConflictDetector` for multi-version entries
- [x] 🔴 `detector.go` — `Write(key, value, ctxVC, authorID) (*ValueVersion, bool)`
- [x] 🔴 `detector.go` — Correct logic per SPEC §7.1
- [x] 🔴 `detector.go` — Emit `conflict` event on `Concurrent` case

### 8.2 Resolver
- [x] 🔴 `resolver.go` — `Resolve(versions []*ValueVersion, strategy ConflictResolution) *ValueVersion`
- [x] 🔴 `resolver.go` — `LWW`: latest `time.Time`
- [x] 🔴 `resolver.go` — `KeepAll`: all siblings unchanged
- [x] 🟡 `resolver.go` — `MergeFunc`: user-provided merge function
- [x] 🔴 `resolver.go` — Emit `resolved` event after resolution

### 8.3 Anti-Entropy
- [x] 🟡 `antientropy.go` — `Sync(local, remote *KVStore) []string`
- [x] 🟡 `antientropy.go` — Per key: Compare VVs → accept dominant / keep siblings

### 8.4 Tests
- [x] 🔴 Concurrent writes detected: result is siblings
- [x] 🔴 Causally ordered writes: no false conflict
- [x] 🔴 LWW resolution: winner is latest by wall clock
- [x] 🔴 KeepAll: all siblings preserved
- [x] 🔴 FalseConflict regression

---

## Phase 9 — Simulation Engine (`internal/simulation/`)

### 9.1 Transport
- [x] 🔴 `transport.go` — `SimTransport` with per-channel `chan *Message`
- [x] 🔴 `transport.go` — `RegisterChannel(from, to string)`
- [x] 🔴 `transport.go` — `Send(from, to string, m *Message) error`
- [x] 🔴 `transport.go` — `InjectDelay(from, to string, d time.Duration)`
- [x] 🔴 `transport.go` — `InjectDrop(from, to string)`
- [x] 🟡 `transport.go` — `InjectReorder(from, to string)`
- [x] 🟡 `transport.go` — `SetPartition(groups [][]string)`

### 9.2 Orchestrator
- [x] 🔴 `simulation.go` — `Simulation` struct + `New(cfg *SimConfig) *Simulation`
- [x] 🔴 `simulation.go` — `SpawnProcess(cfg ProcessConfig) error`
- [x] 🔴 `simulation.go` — `KillProcess(id string) error`
- [x] 🔴 `simulation.go` — `SendMessage(from, to, data) error`
- [x] 🔴 `simulation.go` — `BroadcastMessage(from, data) error`
- [x] 🔴 `simulation.go` — `InternalEvent(pid string) error`
- [x] 🔴 `simulation.go` — `TriggerSnapshot(initiatorID string) (string, error)`
- [x] 🔴 `simulation.go` — `GetState() SimulationState`
- [x] 🟡 `simulation.go` — `GetCausalGraph() *CausalGraph`

### 9.3 Scenarios
- [x] 🟡 `scenarios.go` — `basicLamport` scenario (3P, 5 events)
- [x] 🟡 `scenarios.go` — `concurrentWrites` scenario
- [x] 🟡 `scenarios.go` — `causalViolation` scenario (Immediate mode)
- [x] 🟡 `scenarios.go` — `causalDelivery` scenario (hold-back queue)
- [x] 🟡 `scenarios.go` — `snapshot3P` scenario (step-by-step Chandy-Lamport)
- [x] 🟡 `scenarios.go` — `FalseConflict` scenario (VV vs DVV comparison)
- [x] 🟡 `scenarios.go` — `MatrixGC` scenario
- [x] 🟡 `scenarios.go` — `PartitionAndHeal` scenario

---

## Phase 10 — Event Bus & Gateway (`internal/events/`, `gateway/`)

### 10.1 Event Bus
- [x] 🔴 `bus.go` — `EventBus` with pub/sub, ring buffer
- [x] 🔴 `bus.go` — `Publish(e Event)` — non-blocking, drop if full
- [x] 🔴 `bus.go` — `Subscribe(types []EventType) chan Event`
- [x] 🔴 `bus.go` — `Unsubscribe(ch chan Event)`
- [x] 🟡 `bus.go` — Ring buffer history (last 1000 events, replay on WS connect)

### 10.2 Gateway REST
- [x] 🔴 `server.go` — HTTP server (gin) with CORS
- [x] 🔴 `rest.go` — All REST handlers per SPEC §13.1
- [x] 🔴 `rest.go` — `GET /simulation/state`
- [x] 🔴 `rest.go` — `POST /processes` + `DELETE /processes/:id`
- [x] 🔴 `rest.go` — `GET /processes/:id`
- [x] 🔴 `rest.go` — `POST /messages` + `POST /broadcast`
- [x] 🔴 `rest.go` — `POST /processes/:id/snapshot`
- [x] 🔴 `rest.go` — `GET /snapshots/:id` + `GET /snapshots/:id/verify`
- [x] 🔴 `rest.go` — `GET /causality/happened-before`
- [x] 🟡 `rest.go` — `POST /faults/*` fault injection endpoints
- [x] 🟡 `rest.go` — `POST /scenarios/:name/run`

### 10.3 WebSocket Hub
- [x] 🔴 `websocket.go` — Upgrade handler, hub with subscriber map
- [x] 🔴 `websocket.go` — Parse subscribe messages from clients
- [x] 🔴 `websocket.go` — Fan-out events from EventBus to WS clients
- [x] 🟡 `websocket.go` — Replay history buffer on connect

---

## Phase 11 — Elysia BFF (`frontend/server/bff.ts`)

- [x] 🔴 `bff.ts` — Elysia server with CORS, static file serving
- [x] 🔴 `bff.ts` — `GET /api/*` → proxy to Go backend
- [x] 🔴 `bff.ts` — `POST /api/*` → proxy with body forwarding
- [x] 🔴 `bff.ts` — WebSocket `/ws`: upgrade, fan-out from Go WS
- [x] 🔴 `bff.ts` — Export `typeof app` for Eden Treaty
- [x] 🟡 `bff.ts` — `DELETE/PUT /api/*` proxy
- [x] 🟡 `bff.ts` — Rate limiting (per-IP, 100 req/min)

---

## Phase 12 — Frontend UI (`frontend/src/`)

### 12.1 Foundation
- [x] 🔴 `api/types.ts` — TypeScript types mirroring all Go event/state types
- [x] 🔴 `api/client.ts` — Eden Treaty typed client + REST helpers
- [x] 🔴 `ws/socket.ts` — WebSocket with auto-reconnect, dispatch to store
- [x] 🔴 `stores/simulation.ts` — Nano store: processes, events log, config
- [x] 🔴 `stores/diagram.ts` — Derived store: computed D3 layout
- [x] 🔴 `main.ts` — App bootstrap: init stores, mount components, connect WS

### 12.2 Space-Time Diagram
- [x] 🔴 `SpaceTimeDiagram/layout.ts` — Event position computation
- [x] 🔴 `SpaceTimeDiagram/index.ts` — D3 SVG container, zoom/pan, process lanes
- [x] 🔴 `SpaceTimeDiagram/index.ts` — Event circles (colored by type) with tooltip
- [x] 🔴 `SpaceTimeDiagram/arrows.ts` — Message arcs (send → receive) with markers
- [x] 🔴 `SpaceTimeDiagram/clocks.ts` — Clock label text above each event circle
- [x] 🟡 `SpaceTimeDiagram/cuts.ts` — Consistent cut line rendering
- [x] 🟡 `SpaceTimeDiagram/animation.ts` — Message transit animation
- [x] 🟡 `SpaceTimeDiagram/index.ts` — Causal history highlight on event click
- [x] 🟡 `SpaceTimeDiagram/index.ts` — Timeline scrubber (step-by-step replay)

### 12.3 Clock Inspector
- [x] 🔴 `ClockInspector/index.ts` — Lamport, vector, matrix clock display
- [x] 🟡 `ClockInspector/index.ts` — Concurrent detector: select two events → show relation
- [x] 🟡 `ClockInspector/index.ts` — Matrix clock N×N heatmap grid

### 12.4 Causal Delivery Monitor
- [x] 🔴 `CausalDelivery/index.ts` — Held messages per process + delivery log
- [x] 🟡 `CausalDelivery/index.ts` — Delivery timeline per process
- [x] 🟡 `CausalDelivery/index.ts` — Immediate vs Causal side-by-side toggle

### 12.5 Snapshot Viewer
- [x] 🟡 `SnapshotViewer/index.ts` — Three-phase display per SPEC §12.5
- [x] 🟡 `SnapshotViewer/index.ts` — Consistent cut visualization overlay
- [x] 🟡 `SnapshotViewer/index.ts` — Consistency proof panel

### 12.6 Conflict Dashboard
- [x] 🟡 `ConflictDash/index.ts` — Version history DAG (D3 force layout)
- [x] 🟡 `ConflictDash/index.ts` — False conflict demonstrator (VV vs DVV tabs)
- [x] 🟡 `ConflictDash/index.ts` — Resolution log table

### 12.7 Scenario Panel
- [x] 🟡 `ScenarioPanel/index.ts` — Scenario cards grid with run buttons
- [x] 🟡 `ScenarioPanel/index.ts` — Running scenario: narration + step controls
- [x] 🟡 `ScenarioPanel/index.ts` — Auto-animate with configurable delay
- [x] 🔵 `ScenarioPanel/index.ts` — Custom scenario builder

---

## Phase 13 — Integration & Polish

### 13.1 Integration Tests (`test/integration/`)
- [x] 🔴 5-process causal broadcast: all messages delivered in causal order
- [x] 🔴 Chandy-Lamport snapshot: completes on 3-process system with active messaging
- [x] 🔴 Vector clock causality invariant: sendVC ≤ recvVC for all messages
- [x] 🔴 Lamport total order: all observed timestamps ≥ 1
- [x] 🔴 Matrix clock GC bound: own-row non-zero after exchanges
- [x] 🔴 Snapshot verifier: initiator always records local state
- [x] 🔴 WebSocket: spawn process → verify `process_spawned` event received
- [x] 🔴 Hold-back queue: out-of-order delivery → correct causal order

### 13.2 Race Condition Checks
- [x] 🔴 Full test suite with `-race` flag: **zero data races**
- [x] 🟡 Concurrent snapshot + message send: no missed messages

### 13.3 Frontend Polish
- [x] 🟡 Dark theme (Tailwind dark mode)
- [x] 🟡 Responsive layout
- [x] 🟡 Loading/disconnected state (WebSocket status indicator)
- [x] 🟡 Error handling: connection lost banner + auto-reconnect
- [x] 🟡 Tooltip on every event circle with full clock state
- [x] 🟢 Keyboard shortcuts: `I` = internal event, `S` = send, `N` = snapshot

### 13.4 Documentation
- [x] 🔴 `README.md`: setup, run instructions, screenshots
- [x] 🟡 `godoc` comments on all exported types
- [x] 🟢 Theory cards in UI

---

## Stretch Goals

- [ ] 🔵 Interval Tree Clocks (ITC)
- [ ] 🔵 Plausible Clocks
- [ ] 🔵 Total Order Broadcast
- [ ] 🔵 Reversible computation / Time Warp
- [ ] 🔵 Byzantine fault tolerance demo
- [ ] 🔵 CRDT showcase (G-Counter, PN-Counter, OR-Set)
- [ ] 🔵 Export diagram as SVG/PNG
- [ ] 🔵 Import execution trace from file
- [ ] 🔵 Matrix clock Singhal-Kshemkalyani delta compression

---

## Phase 14 — Production Hardening & Observability

### 14.1 OpenTelemetry Tracing
- [x] 🔴 `internal/telemetry/` — Init/Config/Tracer with OTLP/stdout/none exporters
- [x] 🔴 Telemetry unit tests (noop, stdout round-trip, error paths, propagation)
- [x] 🔴 OTel Gin middleware wired in `gateway/server.go`
- [x] 🔴 Telemetry init/shutdown in `cmd/server/main.go` via env vars
- [x] 🟡 `docs/adr/0007-opentelemetry.md`

### 14.2 TLS Termination
- [x] 🔴 `gateway/tlsconfig/` — Load/atomic reload/self-signed cert generation
- [x] 🔴 TLS + mTLS with `VC_TLS_CLIENT_CA_FILE`
- [x] 🔴 15 unit tests + 5 e2e tests for TLS

### 14.3 Authentication & Security
- [x] 🔴 Token-based auth (`gateway/auth/`) — constant-time compare, fails closed
- [x] 🔴 Per-IP rate limiter (`gateway/ratelimit.go`)
- [x] 🔴 Security headers middleware (X-Content-Type-Options, X-Frame-Options, Referrer-Policy, Permissions-Policy)
- [x] 🔴 Request ID middleware
- [x] 🔴 Max body size middleware
- [x] 🔴 PID validation regex

### 14.4 k6 Load Testing
- [x] 🟡 `test/k6/scenarios.js` — Smoke, load, stress, soak profiles
- [x] 🟡 `test/k6/chaos.js` — Fault injection under load
- [x] 🟡 `test/k6/README.md` — Instructions

### 14.5 Playwright Frontend Testing
- [x] 🟡 `frontend/test/playwright.config.ts`
- [x] 🟡 `frontend/test/helpers.ts` — Shared setup helpers
- [x] 🟡 `frontend/test/tests/basic.spec.ts` — 10 UI tests
- [x] 🟡 `frontend/test/tests/simulation.spec.ts` — 6 simulation tests
- [x] 🟡 `frontend/test/tests/websocket.spec.ts` — 5 WebSocket tests

### 14.6 CI & Deployment
- [x] 🔴 GitHub Actions CI (test, race, coverage gate, govulncheck, container build)
- [x] 🔴 Distroless Dockerfile (nonroot user, no shell)
- [x] 🔴 docker-compose.yml with Go server + config mount
- [x] 🔴 K8s manifests (Deployment, Service, HPA, PDB, ConfigMap, Secret)
- [x] 🔴 Makefile (build, test, test-race, lint, fuzz, bench, coverage gate)

---

## Summary Progress Tracker

| Phase | Tasks | Status |
|-------|-------|--------|
| 0. Bootstrap | 14 | ✅ Complete |
| 1. Lamport Clocks | 11 | ✅ Complete |
| 2. Vector Clocks | 21 | 🟡 20/21 (Charron-Bost pending) |
| 3. Matrix Clocks | 11 | ✅ Complete |
| 4. Version Vectors + DVV | 11 | ✅ Complete |
| 5. Causality Primitives | 14 | ✅ Complete |
| 6. Process + Hold-Back | 20 | ✅ Complete |
| 7. Chandy-Lamport | 15 | ✅ Complete |
| 8. Conflict Detection | 16 | ✅ Complete |
| 9. Simulation Engine | 24 | ✅ Complete |
| 10. Events + Gateway | 20 | ✅ Complete |
| 11. Elysia BFF | 7 | ✅ Complete |
| 12. Frontend UI | 31 | 🟡 18/31 (cuts, animation, history, scrubber, SnapshotViewer, ConflictDash, ScenarioPanel pending) |
| 13. Integration | 19 | ✅ Complete |
| 14. Production Hardening | 27 | ✅ 27/27 Complete |
| **TOTAL** | **261** | **246/261 (~94%)** |
