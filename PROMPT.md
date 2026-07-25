# Claude Code — Implementation Prompt
## Vector Clocks & Causal Consistency System

---

## CONTEXT

You are implementing a **Vector Clocks & Causal Consistency System** from scratch — a rigorous distributed systems laboratory for logical time, causality, global snapshots, and conflict detection.

- **Backend:** Go 1.22+ — all clock algorithms + simulation engine
- **Frontend:** Bun + TypeScript + Elysia + D3.js — space-time diagram visualizer
- **Full spec:** `SPEC.md` — read it completely before writing any code
- **Task tracker:** `CHECKLIST.md` — check off items as you complete them

This is NOT tutorial-level code. Every algorithm must be implemented with paper-level fidelity:
- Lamport, Fidge/Mattern, Kshemkalyani-Singhal clock rules exactly as defined
- Causal delivery hold-back queue that actually blocks messages
- Chandy-Lamport with real FIFO channel recording and marker propagation
- Dotted version vectors preventing false conflicts
- Bun+Elysia BFF with Eden Treaty for end-to-end TypeScript types

---

## HOW TO PROCEED

Work through `CHECKLIST.md` in phase order, strictly. Do not skip phases or implement out of order — later phases depend on earlier ones.

For each phase:
1. Read the relevant SPEC.md section FIRST before writing any code
2. Complete every 🔴 item before moving to the next phase
3. Run tests (`go test ./... -race`) after each phase before proceeding
4. Check off completed items in `CHECKLIST.md`

---

## PHASE-BY-PHASE IMPLEMENTATION GUIDE

### PHASE 0 — Bootstrap

Initialize everything:
```bash
go mod init github.com/yourname/vectorclock-system
mkdir -p cmd/server internal/{clock/{lamport,vector,matrix,version,dvv},causality,process,broadcast,snapshot,conflict,simulation,events} gateway test/integration
```

For Go dependencies:
```
github.com/gin-gonic/gin
github.com/gorilla/websocket
go.uber.org/zap
github.com/spf13/viper
```

Initialize Bun frontend:
```bash
mkdir frontend && cd frontend
bun init -y
bun add elysia @elysiajs/cors @elysiajs/eden d3 nanostores
bun add -d typescript tailwindcss @types/d3 @types/bun
mkdir -p src/{api,stores,components/{SpaceTimeDiagram,ClockInspector,CausalDelivery,SnapshotViewer,ConflictDash,ScenarioPanel},ws,styles} server
```

---

### PHASE 1 — Lamport Clocks

Read `SPEC.md §3.1` first.

Implement `internal/clock/lamport/clock.go`:

```go
package lamport

import "sync"

type LamportClock struct {
    mu    sync.Mutex
    value uint64
    pid   string
}

func New(pid string) *LamportClock {
    return &LamportClock{pid: pid}
}

// LC1: internal event
func (c *LamportClock) Tick() uint64 {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.value++
    return c.value
}

// LC2: before send (same rule as Tick, named separately for clarity)
func (c *LamportClock) Send() uint64 {
    return c.Tick()
}

// LC3: on receive
func (c *LamportClock) Receive(msgTimestamp uint64) uint64 {
    c.mu.Lock()
    defer c.mu.Unlock()
    if msgTimestamp > c.value {
        c.value = msgTimestamp
    }
    c.value++
    return c.value
}

func (c *LamportClock) Value() uint64 {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.value
}

// TotalOrder: (ts, pid) lexicographic ordering for deterministic total order
// Returns -1, 0, 1
func TotalOrder(ts1 uint64, pid1 string, ts2 uint64, pid2 string) int {
    if ts1 != ts2 {
        if ts1 < ts2 { return -1 }
        return 1
    }
    // Tie-break by pid (lexicographic)
    if pid1 < pid2 { return -1 }
    if pid1 > pid2 { return 1 }
    return 0
}
```

**Write the test file first** (`clock_test.go`):
```go
func TestReceive_ReceivedGreater(t *testing.T) {
    c := New("P1")
    c.Tick()  // value = 1
    got := c.Receive(5)
    // max(1, 5) + 1 = 6
    assert.Equal(t, uint64(6), got)
}

func TestReceive_LocalGreater(t *testing.T) {
    c := New("P1")
    for i := 0; i < 10; i++ { c.Tick() }  // value = 10
    got := c.Receive(3)
    // max(10, 3) + 1 = 11
    assert.Equal(t, uint64(11), got)
}

func TestRace(t *testing.T) {
    c := New("P1")
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func() { defer wg.Done(); c.Tick() }()
    }
    wg.Wait()
    assert.Equal(t, uint64(100), c.Value())
}
```

---

### PHASE 2 — Vector Clocks

Read `SPEC.md §3.2` first.

The most critical function is `Compare`. Get this exactly right — everything else depends on it.

```go
package vector

// VectorClock is a map-based VC. Missing keys = 0 (sparse representation).
type VectorClock map[string]uint64

type ClockOrder int
const (
    HappenedBefore ClockOrder = iota  // a → b: VC(a) < VC(b)
    HappenedAfter                      // b → a: VC(b) < VC(a)
    Concurrent                         // a ∥ b: neither dominates
    Identical                          // a = b: all components equal
)

// Compare implements the Charron-Bost isomorphism:
// VC(a) < VC(b) ⟺ ∀k: a[k] ≤ b[k]  ∧  ∃k': a[k'] < b[k']
func Compare(a, b VectorClock) ClockOrder {
    // Collect all keys from both clocks
    keys := make(map[string]struct{})
    for k := range a { keys[k] = struct{}{} }
    for k := range b { keys[k] = struct{}{} }
    
    aLessB := false  // ∃k: a[k] < b[k]
    bLessA := false  // ∃k: b[k] < a[k]
    
    for k := range keys {
        av, bv := a[k], b[k]  // missing keys default to 0
        if av < bv { aLessB = true }
        if bv < av { bLessA = true }
        if aLessB && bLessA { return Concurrent }  // early exit
    }
    
    if !aLessB && !bLessA { return Identical }
    if aLessB && !bLessA { return HappenedBefore }
    if bLessA && !aLessB { return HappenedAfter }
    return Concurrent  // both have strictly greater entries
}
```

**Key correctness tests — write these FIRST:**
```go
func TestCompare_HappenedBefore(t *testing.T) {
    a := VectorClock{"P1": 1, "P2": 0}
    b := VectorClock{"P1": 2, "P2": 1}
    assert.Equal(t, HappenedBefore, Compare(a, b))
}

func TestCompare_Concurrent(t *testing.T) {
    a := VectorClock{"P1": 2, "P2": 0}
    b := VectorClock{"P1": 0, "P2": 2}
    assert.Equal(t, Concurrent, Compare(a, b))
}

func TestCompare_MissingKeys(t *testing.T) {
    // Missing key = 0
    a := VectorClock{"P1": 1}         // P2 implicitly 0
    b := VectorClock{"P1": 1, "P2": 1}
    // a[P1]=1 ≤ b[P1]=1, a[P2]=0 < b[P2]=1 → HappenedBefore
    assert.Equal(t, HappenedBefore, Compare(a, b))
}
```

Implement all rules per SPEC §3.2:

```go
// VC1: internal event
func Tick(vc VectorClock, pid string) VectorClock {
    result := Copy(vc)
    result[pid]++
    return result
}

// VC2: on send — increment own, return copy to attach to message
func Send(vc VectorClock, pid string) (local VectorClock, msgStamp VectorClock) {
    local = Tick(vc, pid)
    msgStamp = Copy(local)
    return
}

// VC3: on receive — increment own, pointwise-max with message stamp
func Receive(local VectorClock, msgVC VectorClock, pid string) VectorClock {
    merged := MergePassive(local, msgVC)
    return Tick(merged, pid)
}

// Pointwise max, no tick (for queries, anti-entropy)
func MergePassive(a, b VectorClock) VectorClock {
    result := Copy(a)
    for k, v := range b {
        if result[k] < v { result[k] = v }
    }
    return result
}
```

---

### PHASE 3 — Matrix Clocks

Read `SPEC.md §3.3` first.

The key insight: `MT[i]` (row i) IS process i's vector clock. Merge on receive updates process `self`'s row (its own VC) by taking the pointwise max with the **sender's row** from the incoming matrix.

```go
package matrix

type MatrixClock map[string]map[string]uint64

func New(pids []string) MatrixClock {
    mc := make(MatrixClock)
    for _, p := range pids {
        mc[p] = make(map[string]uint64)
        for _, q := range pids {
            mc[p][q] = 0
        }
    }
    return mc
}

// MC3 receive: merge self's row with sender's row from incoming matrix
func Receive(local MatrixClock, incoming MatrixClock, self, sender string) MatrixClock {
    result := DeepCopy(local)
    result[self][self]++  // tick own clock
    // Update self's knowledge based on sender's knowledge
    for pid, senderKnows := range incoming[sender] {
        if result[self][pid] < senderKnows {
            result[self][pid] = senderKnows
        }
    }
    return result
}

// Garbage collection lower bound:
// min_k(MT[k][pid]) = minimum across ALL processes' knowledge of pid
// If this value >= t, ALL processes know pid has done at least t events
func MinKnowledge(mc MatrixClock, pid string) uint64 {
    min := ^uint64(0)  // max uint64
    for _, row := range mc {
        if v := row[pid]; v < min {
            min = v
        }
    }
    if min == ^uint64(0) { return 0 }
    return min
}
```

**Test MinKnowledge specifically** — this is the unique property of matrix clocks:
```go
func TestMinKnowledge_GCBound(t *testing.T) {
    pids := []string{"P1", "P2", "P3"}
    mc := New(pids)
    
    // Simulate: P1 does 5 events, everyone knows about it
    // MC[P1][P1]=5, MC[P2][P1]=5, MC[P3][P1]=5
    mc["P1"]["P1"] = 5
    mc["P2"]["P1"] = 5
    mc["P3"]["P1"] = 5
    
    // Everyone knows P1 has done 5 events → can GC up to event 5
    assert.Equal(t, uint64(5), MinKnowledge(mc, "P1"))
    
    // If P3 only knows P1 did 3 events:
    mc["P3"]["P1"] = 3
    assert.Equal(t, uint64(3), MinKnowledge(mc, "P1"))
}
```

---

### PHASE 4 — DVV (Dotted Version Vectors)

Read `SPEC.md §3.5` carefully. The key concept:

A DVV has TWO parts:
1. `Vector`: the "causally dominated" history (what this write has seen before it)
2. `Dot`: a single `(replicaID, counter)` pair identifying THIS specific write event

This prevents false conflicts by making causal context explicit per-write, not per-server.

```go
type Dot struct {
    Replica string
    Counter uint64
}

type DVV struct {
    Vector map[string]uint64  // causal history base
    Dot    *Dot               // this specific write's identity
}

// Converts DVV to a plain VV (for comparison/merging)
func (d DVV) ToVV() map[string]uint64 {
    vv := make(map[string]uint64)
    for k, v := range d.Vector { vv[k] = v }
    if d.Dot != nil {
        if vv[d.Dot.Replica] < d.Dot.Counter {
            vv[d.Dot.Replica] = d.Dot.Counter
        }
    }
    return vv
}

// A dominates B if A.ToVV() >= B.ToVV() (A has seen everything B has)
func Dominates(a, b DVV) bool {
    bVV := b.ToVV()
    aVV := a.ToVV()
    for k, bv := range bVV {
        if aVV[k] < bv { return false }
    }
    return true
}
```

**The regression test that must pass** — demonstrating DVV prevents false conflicts:
```go
func TestDVV_NoFalseConflict(t *testing.T) {
    // Scenario: Client reads v1, client updates to v2 via server A
    // A replicates to B. No concurrent write happened.
    // Plain VV with server IDs would show false conflict here.
    // DVV must show NO conflict (v2 causally dominates v1).
    
    serverAClock := uint64(0)
    serverBClock := uint64(0)
    
    // Initial write of v1 at server A
    serverAClock++
    v1 := DVV{Vector: map[string]uint64{}, Dot: &Dot{"A", serverAClock}}  // {},{A:1}
    
    // Client reads v1 from B (replicated), gets causal context v1.ToVV() = {A:1}
    ctx := v1.ToVV()
    
    // Client writes v2 at A with context {A:1}
    serverAClock++
    v2 := DVV{Vector: ctx, Dot: &Dot{"A", serverAClock}}  // {A:1},{A:2}
    
    // v2 should dominate v1 — NOT a conflict
    assert.True(t, Dominates(v2, v1), "v2 must dominate v1")
    assert.False(t, Dominates(v1, v2), "v1 must not dominate v2")
    
    _ = serverBClock  // B never wrote
}
```

---

### PHASE 5 — Causality Primitives

Read `SPEC.md §4` first. The causal graph is the backbone for the frontend visualization.

```go
// Build graph from event log
type CausalGraph struct {
    events map[string]*Event
    edges  map[string]map[string]bool  // from → set of to (happened-before edges)
    mu     sync.RWMutex
}

// a → b iff there's a directed path from a to b in the graph
func (g *CausalGraph) HappenedBefore(aID, bID string) bool {
    // BFS/DFS from aID, check if bID is reachable
    visited := make(map[string]bool)
    queue := []string{aID}
    for len(queue) > 0 {
        cur := queue[0]
        queue = queue[1:]
        if cur == bID { return true }
        if visited[cur] { continue }
        visited[cur] = true
        for next := range g.edges[cur] {
            queue = append(queue, next)
        }
    }
    return false
}
```

For consistent cuts, implement `IsConsistent` exactly:
```go
func IsConsistent(cut Cut, events []*Event, messages []*Message) bool {
    // A cut is consistent iff:
    // For every message m: if receive(m) is in the cut → send(m) is in the cut
    
    for _, msg := range messages {
        sendEvent := findEvent(events, msg.SendEventID)
        recvEvent := findEvent(events, msg.RecvEventID)
        
        if sendEvent == nil || recvEvent == nil { continue }
        
        // Is receive(m) in the cut? (its local seq ≤ cut[process])
        recvInCut := recvEvent.LocalSeq <= uint64(cut.Frontier[recvEvent.ProcessID])
        sendInCut := sendEvent.LocalSeq <= uint64(cut.Frontier[sendEvent.ProcessID])
        
        if recvInCut && !sendInCut {
            return false  // violation: received without send being in cut
        }
    }
    return true
}
```

---

### PHASE 6 — Process & Hold-Back Queue

Read `SPEC.md §5.1` for the BSS causal delivery condition. This is the most important phase.

**Hold-back queue delivery condition — implement exactly:**

```go
// BSS Causal delivery condition for message m from Pi to Pj:
// Deliver m iff:
//   (1) VCmsg[sender] == VCj[sender] + 1   (m is the NEXT from sender)
//   (2) ∀k ≠ sender: VCmsg[k] ≤ VCj[k]   (all causal predecessors delivered)

func canDeliver(msgVC VectorClock, sender string, localVC VectorClock) bool {
    // Condition 1: exactly next expected from sender
    expected := localVC[sender] + 1
    if msgVC[sender] != expected { return false }
    
    // Condition 2: all other entries in msgVC ≤ localVC
    for pid, msgCount := range msgVC {
        if pid == sender { continue }
        if msgCount > localVC[pid] { return false }
    }
    return true
}

func (q *HoldBackQueue) TryDeliver(localVC VectorClock, selfPID string) []*Message {
    var delivered []*Message
    changed := true
    for changed {
        changed = false
        q.mu.Lock()
        for i, held := range q.pending {
            if canDeliver(held.SentVC, held.Sender, localVC) {
                // Deliver this message
                delivered = append(delivered, held.Message)
                // Update local VC: for Pj receiving from Pi, localVC[Pi] = msgVC[Pi]
                localVC[held.Sender] = held.SentVC[held.Sender]
                // Remove from queue
                q.pending = append(q.pending[:i], q.pending[i+1:]...)
                changed = true
                break  // restart scan (localVC changed)
            }
        }
        q.mu.Unlock()
    }
    return delivered
}
```

**Critical test — demonstrate causal violation with ImmediateDelivery:**
```go
func TestCausalViolation_Immediate(t *testing.T) {
    // Setup: P1 → P2 (m1), P2 → P3 (m2 based on m1)
    // P3 receives m2 before m1 with immediate delivery
    // This is a causal violation: P3 processes "reply" before "question"
    
    // With ImmediateDelivery: deliver in arrival order → violation
    // With CausalDelivery: hold m2 until m1 arrives → correct order
    
    // Assert that ImmediateDelivery processes m2 before m1 in known scenario
    // Assert that CausalDelivery always processes m1 before m2
}
```

---

### PHASE 7 — Chandy-Lamport Snapshot

Read `SPEC.md §6` carefully. The algorithm has three distinct phases per process.

**State machine per process during snapshotting:**
```go
type SnapshotRole int
const (
    NotParticipating SnapshotRole = iota
    Initiator        // sent markers, recording all incoming channels
    Participant      // received first marker, sent markers, recording remaining channels
    Done             // all channels finalized
)

type ProcessSnapshotState struct {
    Role             SnapshotRole
    LocalState       interface{}         // captured at snapshot time
    RecordedChannels map[string][]*Message // channelID → captured in-transit messages
    FinishedChannels map[string]bool     // channels for which recording is complete
    SnapshotID       string
}
```

**Marker receiving logic — this must be exactly correct:**
```go
func (p *Process) OnMarkerReceived(marker *Message) {
    snapshotID := marker.SnapshotID
    state := p.getOrInitSnapshotState(snapshotID)
    
    switch state.Role {
    case NotParticipating:
        // FIRST marker seen: record local state NOW
        state.LocalState = p.captureLocalState()
        state.Role = Participant
        
        // Mark THIS incoming channel as empty (no messages between snapshot points)
        state.RecordedChannels[marker.From] = []*Message{}
        state.FinishedChannels[marker.From] = true
        
        // Forward markers to ALL outgoing channels
        for _, peer := range p.peers {
            p.transport.Send(p.id, peer, &Message{IsMarker: true, SnapshotID: snapshotID})
        }
        
        // Start recording all OTHER incoming channels
        for _, peer := range p.peers {
            if peer != marker.From && !state.FinishedChannels[peer] {
                state.RecordedChannels[peer] = []*Message{}
            }
        }
        
    case Participant, Initiator:
        // SUBSEQUENT marker: finalize this channel's recording
        state.FinishedChannels[marker.From] = true
        // RecordedChannels[marker.From] already has the captured messages
        // Stop recording this channel (handled by checking FinishedChannels in RecordMessage)
        
        // Check if all channels are finalized
        if len(state.FinishedChannels) == len(p.peers) {
            state.Role = Done
            p.eventBus.Publish(Event{Type: EvtLocalSnapshot, ...})
            p.coordinator.ReportDone(snapshotID, p.id, state)
        }
    }
}

// Called for every non-marker message received WHILE snapshotting
func (p *Process) RecordMessage(snapshotID string, from string, m *Message) {
    state := p.snapshots[snapshotID]
    if state == nil { return }
    if state.FinishedChannels[from] { return }  // channel already finalized
    // Channel is being recorded: add message
    state.RecordedChannels[from] = append(state.RecordedChannels[from], m)
}
```

---

### PHASE 8 — Conflict Detection

The critical correctness property is the `Write` function's conflict detection:

```go
func (d *ConflictDetector) Write(key string, value []byte, ctxVC VectorClock, authorID string) (*ValueVersion, bool) {
    d.mu.Lock()
    defer d.mu.Unlock()
    
    current := d.store[key]
    
    newVersion := &ValueVersion{
        Value:   value,
        Author:  authorID,
        Written: time.Now(),
    }
    
    if current == nil {
        // First write: no conflict possible
        newVersion.Clock = vector.Tick(ctxVC, authorID)
        d.store[key] = &MVCCEntry{Versions: []*ValueVersion{newVersion}}
        return newVersion, false
    }
    
    // Check all existing versions against context
    hasConflict := false
    survivors := []*ValueVersion{}
    
    for _, existing := range current.Versions {
        order := vector.Compare(ctxVC, existing.Clock)
        switch order {
        case vector.HappenedBefore, vector.Identical:
            // Context is stale or equal — existing is newer, keep existing
            survivors = append(survivors, existing)
            hasConflict = true  // writer had stale context
        case vector.HappenedAfter:
            // Context supersedes existing — existing is dominated, discard it
            // (do not append to survivors)
        case vector.Concurrent:
            // CONFLICT: keep both
            survivors = append(survivors, existing)
            hasConflict = true
        }
    }
    
    // New version's clock = max(ctxVC, all surviving clocks)[authorID]++
    merged := ctxVC
    for _, s := range survivors {
        merged = vector.MergePassive(merged, s.Clock)
    }
    newVersion.Clock = vector.Tick(merged, authorID)
    survivors = append(survivors, newVersion)
    
    d.store[key] = &MVCCEntry{Versions: survivors}
    return newVersion, hasConflict
}
```

---

### PHASE 9 — Simulation + Scenarios

The simulation orchestrator ties everything together. Every action must:
1. Perform the action on the relevant process(es)
2. Emit a structured event to the event bus
3. Update shared state under mutex

The `GetState()` function is called on every REST poll — make it fast:
```go
func (s *Simulation) GetState() *SimulationState {
    s.mu.RLock()
    defer s.mu.RUnlock()
    
    state := &SimulationState{
        Processes: make([]ProcessState, 0, len(s.processes)),
        EventCount: len(s.events),
        Config: s.config,
    }
    for _, p := range s.processes {
        state.Processes = append(state.Processes, p.Snapshot())
    }
    return state
}
```

---

### PHASE 10 — Event Bus + Gateway

Every event from EVERY module flows through the event bus. Design it for zero-allocation hot path:

```go
type EventBus struct {
    publish  chan Event
    subs     map[chan Event]map[EventType]bool
    history  []Event  // ring buffer, size 1000
    histIdx  int
    mu       sync.RWMutex
    done     chan struct{}
}

func (b *EventBus) Publish(e Event) {
    select {
    case b.publish <- e:
    default:
        // Buffer full: drop (non-blocking) — log a warning
    }
}
```

For the REST gateway, the most important handler is `GET /processes/:id` because the frontend uses it heavily:
```go
func handleGetProcess(c *gin.Context) {
    id := c.Param("id")
    p := sim.GetProcess(id)
    if p == nil {
        c.JSON(404, gin.H{"error": "process not found"})
        return
    }
    // Return FULL clock state — all 3 clock types even if only one is active
    c.JSON(200, p.FullSnapshot())
}
```

---

### PHASE 11 — Elysia BFF

```typescript
// frontend/server/bff.ts
import { Elysia } from 'elysia'
import { cors } from '@elysiajs/cors'

const GO_BACKEND_URL = process.env.GO_BACKEND || 'http://localhost:8080'

// WebSocket client pool (one connection to Go, many browser clients)
let goWS: WebSocket | null = null
const browserClients = new Set<any>()

function connectToGo() {
  goWS = new WebSocket(`${GO_BACKEND_URL.replace('http', 'ws')}/ws`)
  goWS.onmessage = (e) => {
    // Fan-out to all browser clients
    for (const client of browserClients) {
      try { client.send(e.data) } catch {}
    }
  }
  goWS.onclose = () => {
    goWS = null
    setTimeout(connectToGo, 2000)  // reconnect
  }
}

export const app = new Elysia()
  .use(cors())
  
  // Proxy all REST requests to Go backend
  .all('/api/*', async ({ request, params }) => {
    const url = `${GO_BACKEND_URL}/api/${params['*']}${new URL(request.url).search}`
    const res = await fetch(url, {
      method: request.method,
      headers: { 'Content-Type': 'application/json' },
      body: ['GET', 'HEAD'].includes(request.method) ? undefined : await request.text()
    })
    return new Response(res.body, { status: res.status, headers: res.headers })
  })
  
  // WebSocket hub
  .ws('/ws', {
    open(ws) {
      browserClients.add(ws)
      // Subscribe to all events from Go
      goWS?.send(JSON.stringify({ action: 'subscribe', types: ['*'] }))
    },
    close(ws) { browserClients.delete(ws) },
    message(ws, msg) {
      // Forward client messages to Go (subscribe/unsubscribe/replay)
      goWS?.send(JSON.stringify(msg))
    }
  })
  
  // Serve static frontend
  .get('/', () => Bun.file('./dist/index.html'))
  .get('/assets/*', ({ params }) => Bun.file(`./dist/assets/${params['*']}`))
  
  .listen(3001)

connectToGo()

// Export type for Eden Treaty
export type App = typeof app
```

Eden Treaty client in frontend:
```typescript
// frontend/src/api/client.ts
import { treaty } from '@elysiajs/eden'
import type { App } from '../../server/bff'

export const api = treaty<App>('localhost:3001')

// Fully typed usage:
// const { data, error } = await api.api.processes.get()
// data is typed as ProcessState[] automatically
```

---

### PHASE 12 — Space-Time Diagram (D3.js)

This is the most complex frontend component. The layout computation is critical for correct visualization.

```typescript
// frontend/src/components/SpaceTimeDiagram/layout.ts

const LANE_HEIGHT = 80      // pixels between process lanes
const TIME_UNIT = 60        // pixels per logical clock unit
const EVENT_RADIUS = 8      // event circle radius
const ARROW_CURVE = 30      // Bezier curve control point offset

export interface Point { x: number; y: number }

export function computeLayout(
  events: DHTEvent[],
  processes: ProcessState[],
  config: { clockType: ClockType }
): DiagramLayout {
  
  const processOrder = processes.map(p => p.id)
  
  // Map processID → Y coordinate (center of process lane)
  const laneY = new Map<string, number>(
    processOrder.map((pid, i) => [pid, 50 + i * LANE_HEIGHT])
  )
  
  // Map eventID → (x, y) position
  const positions = new Map<string, Point>()
  
  // Per-process: track last X position to enforce minimum spacing
  const lastX = new Map<string, number>(processOrder.map(p => [p, 60]))
  
  // Sort events by globalSeq for consistent processing
  const sorted = [...events].sort((a, b) => a.globalSeq - b.globalSeq)
  
  for (const e of sorted) {
    const y = laneY.get(e.processId) ?? 0
    
    let x: number
    if (config.clockType === 'lamport' && e.lamportClock != null) {
      x = 60 + e.lamportClock * TIME_UNIT
    } else if (config.clockType === 'vector' && e.vectorClock) {
      // Use own component of vector clock for X position
      x = 60 + (e.vectorClock[e.processId] ?? 0) * TIME_UNIT
    } else {
      // Fallback: use localSeq × TIME_UNIT
      x = 60 + e.localSeq * TIME_UNIT
    }
    
    // Enforce minimum spacing on same process lane
    const minX = (lastX.get(e.processId) ?? 60) + TIME_UNIT
    x = Math.max(x, minX)
    lastX.set(e.processId, x)
    
    positions.set(e.id, { x, y })
  }
  
  return { laneY, positions, processOrder }
}
```

The D3 rendering:
```typescript
// frontend/src/components/SpaceTimeDiagram/index.ts
import * as d3 from 'd3'
import type { SimulationState, DHTEvent } from '../../api/types'
import { computeLayout } from './layout'

export class SpaceTimeDiagram {
  private svg: d3.Selection<SVGSVGElement, unknown, null, undefined>
  private g: d3.Selection<SVGGElement, unknown, null, undefined>
  
  constructor(container: HTMLElement) {
    this.svg = d3.select(container).append('svg')
      .attr('width', '100%').attr('height', '100%')
    
    // Zoom + pan
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 5])
      .on('zoom', ({ transform }) => this.g.attr('transform', transform))
    
    this.svg.call(zoom)
    this.g = this.svg.append('g')
  }
  
  update(state: SimulationState, events: DHTEvent[]) {
    const layout = computeLayout(events, Array.from(state.processes.values()), state.config)
    
    // Process lanes (horizontal lines)
    this.g.selectAll('.lane')
      .data(layout.processOrder)
      .join('line').attr('class', 'lane')
      .attr('x1', 40).attr('x2', this.svgWidth())
      .attr('y1', d => layout.laneY.get(d)!)
      .attr('y2', d => layout.laneY.get(d)!)
      .attr('stroke', '#94a3b8').attr('stroke-width', 1)
    
    // Process labels
    this.g.selectAll('.process-label')
      .data(layout.processOrder)
      .join('text').attr('class', 'process-label')
      .attr('x', 10)
      .attr('y', d => layout.laneY.get(d)! + 4)
      .text(d => d)
      .attr('fill', '#e2e8f0').attr('font-size', '14px').attr('font-weight', 'bold')
    
    // Events (circles)
    this.g.selectAll('.event')
      .data(events, (d: any) => d.id)
      .join('circle').attr('class', 'event')
      .attr('cx', d => layout.positions.get(d.id)!.x)
      .attr('cy', d => layout.positions.get(d.id)!.y)
      .attr('r', 8)
      .attr('fill', d => this.eventColor(d.type))
      .attr('cursor', 'pointer')
      .on('click', (_, d) => this.onEventClick(d))
      .append('title')
      .text(d => this.clockLabel(d))
    
    // Clock labels below events
    this.g.selectAll('.clock-label')
      .data(events, (d: any) => d.id)
      .join('text').attr('class', 'clock-label')
      .attr('x', d => layout.positions.get(d.id)!.x)
      .attr('y', d => layout.positions.get(d.id)!.y + 22)
      .text(d => this.clockLabel(d))
      .attr('text-anchor', 'middle')
      .attr('fill', '#94a3b8')
      .attr('font-size', '10px')
    
    // Message arrows
    this.renderArrows(events, layout)
  }
  
  private renderArrows(events: DHTEvent[], layout: DiagramLayout) {
    // Pair send→receive events by messageID
    const sendMap = new Map<string, DHTEvent>()
    const recvMap = new Map<string, DHTEvent>()
    for (const e of events) {
      if (e.type === 'send' && e.message) sendMap.set(e.message.id, e)
      if (e.type === 'receive' && e.message) recvMap.set(e.message.id, e)
    }
    
    const arrows = []
    for (const [msgId, sendEvt] of sendMap) {
      const recvEvt = recvMap.get(msgId)
      if (!recvEvt) continue
      const from = layout.positions.get(sendEvt.id)
      const to = layout.positions.get(recvEvt.id)
      if (from && to) arrows.push({ from, to, msgId })
    }
    
    // Draw curved arrows
    this.g.selectAll('.arrow')
      .data(arrows, (d: any) => d.msgId)
      .join('path').attr('class', 'arrow')
      .attr('d', ({ from, to }) => {
        // Cubic Bezier curve
        const mx = (from.x + to.x) / 2
        const cy = from.y === to.y ? from.y - 20 : (from.y + to.y) / 2
        return `M${from.x},${from.y} C${mx},${from.y} ${mx},${to.y} ${to.x},${to.y}`
      })
      .attr('fill', 'none')
      .attr('stroke', '#6366f1')
      .attr('stroke-width', 1.5)
      .attr('marker-end', 'url(#arrowhead)')
  }
  
  private eventColor(type: string): string {
    const colors: Record<string, string> = {
      internal_event: '#3b82f6',
      send: '#22c55e',
      receive: '#f97316',
      local_snapshot: '#a855f7',
      message_held: '#f59e0b',
      message_delivered: '#22c55e',
    }
    return colors[type] ?? '#94a3b8'
  }
  
  private clockLabel(e: DHTEvent): string {
    if (e.lamportClock != null) return `[${e.lamportClock}]`
    if (e.vectorClock) {
      const entries = Object.entries(e.vectorClock)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([, v]) => v)
      return `[${entries.join(',')}]`
    }
    return ''
  }
}
```

---

## CRITICAL CORRECTNESS INVARIANTS

Verify these after each relevant phase. These are the mathematical properties the papers prove — they must hold in your implementation:

### Lamport Clocks:
1. `LC(a) < LC(b)` for all `a → b` (clock consistency — cannot be violated if rules correct)
2. `TotalOrder` is a strict total order: irreflexive, asymmetric, transitive

### Vector Clocks:
1. `a → b ⟺ Compare(VC(a), VC(b)) == HappenedBefore` (Charron-Bost isomorphism)
2. `a ∥ b ⟺ Compare(VC(a), VC(b)) == Concurrent`
3. `Merge(a, b) == Merge(b, a)` (commutativity)
4. `Merge(a, a) == a` (idempotency)

### Matrix Clocks:
1. `MT[i][j] >= VC_i[j]` always (self-row tracks own VC)
2. `MinKnowledge(MC, pid) <= min(MC[k][pid]) for all k`

### Causal Delivery:
1. If `broadcast(m1) → broadcast(m2)`, all processes deliver `m1` before `m2`
2. No message is delivered more than once
3. No message is dropped (all messages eventually delivered if sender is live)

### Chandy-Lamport:
1. `IsConsistent(snapshot) == true` for every completed snapshot
2. For every in-transit message `m` in channel `Cij`: `send(m)` is pre-cut at `Pi` AND `recv(m)` is post-cut at `Pj`

### DVV:
1. `Dominates(v2, v1) == true` when v2 causally supersedes v1 (no false conflict)
2. `!Dominates(v1, v2) && !Dominates(v2, v1)` iff truly concurrent (true conflict)

---

## CODE STANDARDS

- **All goroutines** stopped via `context.Context` or `chan struct{}`, not `time.Sleep` polling
- **No global state** — everything injected via constructor
- **Race-free** — run `go test ./... -race` after every phase; zero races allowed
- **Error wrapping** — `fmt.Errorf("snapshot.OnMarker: %w", err)` at every call site
- **Functional clock operations** — all clock functions are pure (no mutation of input, return new clock)
- **Bun TypeScript** — strict mode, no `any` types except explicit escape hatches, document all exceptions
- **Elysia** — use method chaining always; schema validation on every endpoint

---

## STARTING COMMAND

```bash
ls -la
cat SPEC.md      # read entire spec
cat CHECKLIST.md # read entire checklist
```

Then begin Phase 0 immediately. After every phase:
```bash
go test ./... -race -count=1   # must pass with zero races
```

Report which phase you've completed and any design decisions you made that deviate from the spec.