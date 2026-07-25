# SPEC.md — Vector Clocks & Causal Consistency System
## Logical Time, Causality, Conflict Detection & Global Snapshots — From Scratch in Go

> **Backend:** Go 1.22+ · **Frontend:** Bun + TypeScript + Elysia (BFF) + Vanilla TS + D3.js  
> **Version:** 1.0.0

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture](#2-architecture)
3. [Clock Algorithms — Theory & Implementation](#3-clock-algorithms)
   - 3.1 Lamport Scalar Clocks
   - 3.2 Vector Clocks (Fidge/Mattern)
   - 3.3 Matrix Clocks
   - 3.4 Version Vectors
   - 3.5 Dotted Version Vectors
4. [Causal Ordering Primitives](#4-causal-ordering-primitives)
5. [Causal Broadcast & Delivery](#5-causal-broadcast--delivery)
6. [Global Snapshot (Chandy-Lamport)](#6-global-snapshot-chandy-lamport)
7. [Conflict Detection & Resolution](#7-conflict-detection--resolution)
8. [Causal KV Store](#8-causal-kv-store)
9. [Simulation Engine](#9-simulation-engine)
10. [Event System](#10-event-system)
11. [Go Backend Architecture](#11-go-backend-architecture)
12. [Bun+TS Frontend Architecture](#12-bunts-frontend-architecture)
    - 12.1 Space-Time Diagram Visualizer
    - 12.2 Clock State Inspector
    - 12.3 Causal Delivery Monitor
    - 12.4 Snapshot Viewer
    - 12.5 Conflict Dashboard
    - 12.6 Scenario Playground
13. [API Specification](#13-api-specification)
14. [Data Models](#14-data-models)
15. [Configuration Reference](#15-configuration-reference)
16. [Testing Strategy](#16-testing-strategy)
17. [Performance Targets](#17-performance-targets)
18. [File Structure](#18-file-structure)

---

## 1. System Overview

### 1.1 Purpose

A fully rigorous, interactively visualized distributed systems laboratory for **logical time and causality**. The Go backend simulates N processes communicating asynchronously; every process runs its own clock algorithm (Lamport, Vector, or Matrix), sends messages with piggybacked timestamps, enforces causal delivery via hold-back queues, and participates in Chandy-Lamport global snapshots on demand.

The Bun+TypeScript frontend renders the execution in real time as a **space-time diagram** — the canonical notation from Lamport's 1978 paper — with live clock state overlays, causal arrow annotations, consistent-cut visualization, and conflict detection dashboards.

### 1.2 Core Concepts Implemented

| Concept | Paper Reference | Module |
|---------|-----------------|--------|
| Scalar logical clocks | Lamport 1978 | `internal/clock/lamport` |
| Happened-before relation (→) | Lamport 1978 | `internal/causality` |
| Vector clocks | Fidge 1988 / Mattern 1989 | `internal/clock/vector` |
| Partial order detection | Charron-Bost 1991 | `internal/causality` |
| Matrix clocks | Kshemkalyani-Singhal 1992 | `internal/clock/matrix` |
| Version vectors | Parker et al. 1983 | `internal/clock/version` |
| Dotted version vectors | Preguiça et al. 2012 | `internal/clock/dvv` |
| Causal broadcast | Raynal 1991 | `internal/broadcast` |
| Causal delivery (hold-back queue) | Birman-Schiper-Stephenson | `internal/delivery` |
| Chandy-Lamport snapshot | Chandy-Lamport 1985 | `internal/snapshot` |
| Consistent cut | Mattern 1987 | `internal/snapshot` |
| Conflict detection (Before/After/Concurrent) | Parker 1983 | `internal/conflict` |
| Causal KV store | Dynamo-inspired | `internal/kvstore` |

### 1.3 What Makes This Non-Trivial

- Processes run as concurrent goroutines with real channel-based message passing
- Hold-back queues enforce **actual** causal delivery — messages genuinely blocked until causal predecessors arrive
- Matrix clocks emit `min_k(MT[k][j])` — enabling per-entry garbage collection
- Chandy-Lamport implemented with real FIFO channel recording and marker propagation
- Dotted version vectors prevent false conflicts that plain version vectors produce
- All clock states are streamed to frontend in real time; frontend replay traces the space-time diagram from event log

---

## 2. Architecture

### 2.1 Component Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                   Bun + TypeScript Frontend                       │
│  ┌─────────────────┐ ┌──────────────┐ ┌──────────────────────┐   │
│  │ Space-Time Diag │ │ Clock State  │ │ Conflict Dashboard   │   │
│  │ (D3.js SVG)     │ │ Inspector    │ │ (DVV / Version Vecs) │   │
│  └────────┬────────┘ └──────┬───────┘ └──────────┬───────────┘   │
│           └──────────────── ┼ ──────────────────-┘               │
│                     WebSocket + Fetch API                         │
│                                                                   │
│  ┌─────────────────────────────────────────────┐                 │
│  │  Elysia BFF (Bun, port 3001)                │                 │
│  │  REST proxy + WebSocket fan-out             │                 │
│  └──────────────────────┬──────────────────────┘                 │
└─────────────────────────┼────────────────────────────────────────┘
                          │ HTTP / WebSocket
┌─────────────────────────▼────────────────────────────────────────┐
│                    Go Simulation Engine (port 8080)               │
│                                                                   │
│  ┌──────────────────┐   ┌────────────────────┐                   │
│  │  Process Manager │   │  Event Bus          │                   │
│  │  (Goroutines)    │   │  (chan Event)        │                   │
│  └────────┬─────────┘   └────────┬────────────┘                  │
│           │                      │                                │
│  ┌────────▼──────────────────────▼────────────────────────────┐  │
│  │              Simulation Process (×N goroutines)             │  │
│  │                                                             │  │
│  │  ┌────────────┐ ┌──────────────┐ ┌──────────────────────┐  │  │
│  │  │ Clock      │ │ Causal       │ │ Snapshot             │  │  │
│  │  │ Engine     │ │ Delivery     │ │ Participant           │  │  │
│  │  │ (Lamport / │ │ (hold-back   │ │ (marker send/recv)   │  │  │
│  │  │  Vector /  │ │  queue)      │ │                      │  │  │
│  │  │  Matrix)   │ └──────────────┘ └──────────────────────┘  │  │
│  │  └────────────┘                                             │  │
│  │  ┌─────────────────────────────────────────────────────┐   │  │
│  │  │  Message Channels  (buffered chan Message per pair)  │   │  │
│  │  └─────────────────────────────────────────────────────┘   │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 Process Internal Architecture

```
┌───────────────────────────────────────────────────────────┐
│                  Simulation Process Pi                     │
│                                                            │
│  State:                                                    │
│    processID    string                                     │
│    clockType    ClockType  // Lamport | Vector | Matrix    │
│    lamportClock uint64                                     │
│    vectorClock  map[string]uint64  // pid → counter        │
│    matrixClock  map[string]map[string]uint64               │
│    localState   interface{}  // app-level state            │
│                                                            │
│  Channels:                                                 │
│    inbound     chan Message  // from all peers             │
│    outbound    map[string]chan Message  // to each peer    │
│    control     chan ControlCmd  // spawn/kill/snapshot     │
│                                                            │
│  Components:                                               │
│    HoldBackQueue  []HeldMessage   // for causal delivery   │
│    SnapshotState  *LocalSnapshot  // during Chandy-Lamport │
│    KVStore        *CausalKV       // optional              │
│    EventEmitter   func(Event)     // → global event bus   │
└───────────────────────────────────────────────────────────┘
```

---

## 3. Clock Algorithms

### 3.1 Lamport Scalar Clocks

**Definition:** A single integer counter per process tracking event count with cross-process synchronization.

**Rules (exactly as in Lamport 1978):**
```
LC1 (Internal Event):  Ci = Ci + 1
LC2 (Send):           Ci = Ci + 1; attach Ci to message
LC3 (Receive):        Ci = max(Ci, Cmsg) + 1
```

**Properties:**
- If `a → b` then `LC(a) < LC(b)` (clock consistency condition — one direction only)
- Converse is NOT guaranteed: `LC(a) < LC(b)` does NOT imply `a → b`
- Cannot detect concurrency — `LC(a) < LC(b)` is ambiguous

**Go implementation:**
```go
type LamportClock struct {
    mu    sync.Mutex
    value uint64
    pid   string
}

func (c *LamportClock) Tick() uint64           // LC1: internal event
func (c *LamportClock) Send() uint64           // LC2: increment then return
func (c *LamportClock) Receive(ts uint64) uint64  // LC3: max+1

// Total ordering with tie-break:
func TotalOrder(ts1 uint64, pid1 string, ts2 uint64, pid2 string) int
// Compares (ts, pid) lexicographically for deterministic total order
```

**Singhal-Kshemkalyani optimization (efficient vector transmission):**  
Instead of sending full vector on every message, send only the entries that changed since the last message to that specific recipient. The differential is `{(i, vc[i]) : vc[i] changed since last send to recipient}`. This reduces message size from O(N) to O(changed entries).

### 3.2 Vector Clocks (Fidge/Mattern)

**Definition:** N-element vector where `VC[i]` = number of events process `i` has performed that process `j` knows about.

**Rules:**
```
VC1 (Internal):  VC[self]++
VC2 (Send):      VC[self]++; attach copy of VC to message
VC3 (Receive):   VC[self]++; for each k: VC[k] = max(VC[k], VCmsg[k])
```

**Causality detection (strong consistency property):**
```
VC(a) < VC(b)  ⟺  ∀k: VC(a)[k] ≤ VC(b)[k]  ∧  ∃k': VC(a)[k'] < VC(b)[k']
VC(a) = VC(b)  ⟺  ∀k: VC(a)[k] = VC(b)[k]
a ∥ b          ⟺  VC(a) ≱ VC(b)  ∧  VC(b) ≱ VC(a)  (neither dominates)
a → b          ⟺  VC(a) < VC(b)   (isomorphism theorem — Charron-Bost)
```

**Go implementation:**
```go
type VectorClock map[string]uint64

type ClockOrder int
const (
    HappenedBefore  ClockOrder = iota // a → b
    HappenedAfter                     // b → a  
    Concurrent                        // a ∥ b
    Identical                         // a = b
)

func NewVC(pids []string) VectorClock
func (vc VectorClock) Tick(pid string) VectorClock          // VC1
func (vc VectorClock) IncrementAndCopy(pid string) VectorClock // VC2: for send
func (vc VectorClock) Merge(other VectorClock, pid string) VectorClock // VC3: on receive

// Core comparison — implements Charron-Bost isomorphism
func Compare(a, b VectorClock) ClockOrder

// Merge without ticking (for VC state queries)
func MergePassive(a, b VectorClock) VectorClock  // pointwise max, no increment

// Causal history: all events that must have happened before this timestamp
func CausalHistory(vc VectorClock) string  // human-readable

// Dotted vector clock (for data versioning, see §3.5)
type DottedVC struct {
    Clock VectorClock
    Dot   Dot  // (pid, counter) of the latest event
}
```

**Charron-Bost theorem:** For strong consistency, vector clocks must be of size ≥ N (number of processes). Any smaller representation loses causal information.

### 3.3 Matrix Clocks

**Definition:** N×N matrix where `MT[i][j]` = process `i`'s knowledge of how many events process `j` has experienced. The `i`-th row is process `i`'s own vector clock. The `j`-th column of row `k` tracks what process `k` believes about process `j`.

**Rules:**
```
MC1 (Internal):  MT[self][self]++
MC2 (Send):      MT[self][self]++; attach full matrix MT to message  
MC3 (Receive):   MT[self][self]++
                 for each row k:
                   for each col j: MT[self][j] = max(MT[self][j], MTmsg[k][j])
                 // Simpler: MT[self] = pointwise_max(MT[self], MTmsg[sender])
```

**Key property:** `min_k(MT[i][k])` gives a lower bound on what ALL processes know about process `i`. This enables garbage collection:

> If `min_k(MT[self][k]) >= t`, then process `self` knows that ALL other processes know about at least `t` events from `self`. Events with timestamp ≤ t can safely be garbage collected from operation logs.

**Go implementation:**
```go
type MatrixClock map[string]map[string]uint64  // MT[pid1][pid2] = value

func NewMC(pids []string) MatrixClock
func (mc MatrixClock) Tick(self string) MatrixClock
func (mc MatrixClock) Send(self string) (MatrixClock, MatrixClock) // (updated_local, copy_to_send)
func (mc MatrixClock) Receive(self, sender string, recv MatrixClock) MatrixClock

// Garbage collection lower bound
func MinKnowledge(mc MatrixClock, pid string) uint64  // min_k(MT[k][pid])

// "Pi knows that Pj knows about at least t events from Pk"
func KnowledgeOf(mc MatrixClock, pi, pj, pk string) uint64
```

### 3.4 Version Vectors

Version vectors differ from vector clocks in their **update rules**: they tag data items (not events) and track which replica most recently modified each data item.

```
VV update rules:
  LOCAL_WRITE(replica r, data item d):
    VV(d)[r]++
    
  SYNC(replica r, replica s, data item d):
    if VV_r(d) < VV_s(d): r accepts s's version
    if VV_s(d) < VV_r(d): s accepts r's version
    if VV_r(d) ∥ VV_s(d): conflict — both versions kept (siblings)
```

**False conflict problem with Server-ID version vectors:**

```
Timeline showing false conflict:
  1. Client writes v1 at server A → VV_A = {A:1}
  2. A replicates to B → VV_B = {A:1}
  3. Client reads from B (gets v1 with {A:1})
  4. Client writes v2 at A (context={A:1}) → VV_A = {A:2}
  5. A replicates to B → VV_B = {A:2}
  
  No conflict: v2 causally supersedes v1 (A:2 > A:1) ✓

  But with a different write sequence:
  1. Client writes v1 at A → VV_A = {A:1}
  2. Network partition
  3. Client writes v2 at A → VV_A = {A:2}
  4. Different client writes v3 at B → VV_B = {B:1}
  5. Partition heals: VV_A={A:2}, VV_B={B:1}
  → CONFLICT: A:2 ∥ B:1 → siblings {v2, v3} ← TRUE conflict ✓
```

### 3.5 Dotted Version Vectors (DVV)

DVVs solve the **false conflict** problem in systems where multiple clients write to the same key via different servers.

**Structure:**
```go
type Dot struct {
    Replica string
    Counter uint64
}

type DVV struct {
    Vector  map[string]uint64  // causally dominated history
    Dot     *Dot               // the single event this DVV represents
}
// A DVVSet for a key holds multiple DVVs (sibling values under conflict)
type DVVSet struct {
    Siblings []DVVEntry  // each entry is (DVV, value)
}
```

**Operations:**
```
NEW(server_id, value):
    dot = {server_id, server_clock[server_id]+1}
    server_clock[server_id]++
    return DVV{vector={}, dot=dot}

UPDATE(old_dvv, server_id, value):
    // old_dvv is what the client last read (causal context)
    new_counter = server_clock[server_id]+1
    server_clock[server_id]++
    // new dot supersedes old_dvv's causality
    new_dvv = DVV{vector=old_dvv.to_vv(), dot={server_id, new_counter}}
    return new_dvv

SYNC(dvv_a, dvv_b):
    // Discard values whose DVV is dominated
    // Keep only concurrent values as siblings
```

**Why DVVs prevent false conflicts:**  
Each write's causal context is captured in the Dot, which allows distinguishing "write that saw v1 and replaced it" from "write that happened concurrently with v1".

---

## 4. Causal Ordering Primitives

### 4.1 Happened-Before Relation (→)

Lamport's definition:
```
a → b  iff:
  (1) a and b are in the same process and a comes before b, OR
  (2) a is a send event and b is the corresponding receive event, OR  
  (3) ∃c such that a → c → b  (transitivity)
```

**Go representation:**
```go
type Event struct {
    ID        string
    ProcessID string
    EventType EventType  // Internal | Send | Receive
    Timestamp interface{} // LamportClock | VectorClock | MatrixClock
    MessageID string      // for Send/Receive pairing
    Data      interface{}
}

type CausalGraph struct {
    Events map[string]*Event
    Edges  map[string][]string  // event_id → []caused_event_ids
}

func (g *CausalGraph) HappenedBefore(a, b string) bool  // DFS/BFS on graph
func (g *CausalGraph) Concurrent(a, b string) bool
func (g *CausalGraph) CausalHistory(id string) []*Event  // all ancestors
func (g *CausalGraph) ConsistentCut(frontier map[string]int) bool
```

### 4.2 Consistent Cut

A cut assigns each process a "snapshot point" (index in its event sequence). A cut is consistent iff:

> For every message m: if `receive(m)` is in the cut, then `send(m)` is also in the cut.

Equivalently: no arrow from "past" (pre-cut) in one process to "future" (post-cut) in another crosses the cut in the "wrong" direction (receive before send).

```go
type Cut struct {
    Frontier map[string]int  // processID → last included event index
}

func (c *Cut) IsConsistent(events []Event, channels map[string][]Message) bool
func FindConsistentCuts(events []Event) []Cut  // enumerate all consistent cuts
```

### 4.3 Space-Time Diagram Model

```go
// The data model behind the visualization
type SpaceTimeDiagram struct {
    Processes []ProcessLine
    Messages  []MessageArrow
    Cuts      []Cut
}

type ProcessLine struct {
    ProcessID string
    Events    []PlottedEvent  // sorted by local clock
}

type PlottedEvent struct {
    Event     *Event
    X         float64  // logical time axis
    Y         float64  // process axis
    ClockLabel string  // formatted clock value for overlay
}

type MessageArrow struct {
    SendEvent    string  // event ID
    ReceiveEvent string  // event ID
    IsDelayed    bool    // true if causal delivery held this back
}
```

---

## 5. Causal Broadcast & Delivery

### 5.1 Causal Broadcast Protocol

Causal broadcast ensures that if `broadcast(m1) → broadcast(m2)`, then every process delivers `m1` before `m2`.

**Algorithm (Birman-Schiper-Stephenson 1987):**
```
On BROADCAST(m) at process Pi:
  VCi[i]++
  send(m, VCi) to all processes

On RECEIVE(m, VCmsg) at process Pj from Pi:
  // Causal delivery condition:
  hold m until:
    (1) VCmsg[i] == VCj[i] + 1   (m is the next expected from Pi)
    (2) ∀k≠i: VCmsg[k] <= VCj[k] (all causal predecessors already delivered)
  
  When condition met:
    deliver m to application
    VCj[i] = VCmsg[i]
    // (do NOT increment VCj[j] on receive — only on send/internal)
```

**Hold-Back Queue:**
```go
type HeldMessage struct {
    Message   *Message
    VCStamp   VectorClock
    Timestamp time.Time  // for timeout/debugging
    BlockedBy []string   // which processes we're waiting on
}

type HoldBackQueue struct {
    mu      sync.Mutex
    pending []*HeldMessage
}

// Called on every state change (new deliver OR new message added)
func (q *HoldBackQueue) TryDeliver(localVC VectorClock, pid string) []*Message
```

### 5.2 Causal Delivery Guarantees

Three properties that causal broadcast provides:
1. **Validity**: if Pi broadcasts m, Pi eventually delivers m
2. **Agreement**: if a correct process delivers m, all correct processes eventually deliver m
3. **Causal ordering**: if `broadcast(m1) → broadcast(m2)`, every process delivers m1 before m2

**Violation demonstration mode:**
The simulation can optionally **disable causal delivery** (deliver messages immediately in arrival order) to show violations — where a process receives "reply to a message it hasn't seen yet", demonstrating why causal ordering matters.

```go
type DeliveryMode int
const (
    ImmediateDelivery DeliveryMode = iota  // raw arrival order (shows violations)
    CausalDelivery                          // hold-back queue enforced
    TotalOrderDelivery                      // Lamport total order (Zookeeper-style)
)
```

---

## 6. Global Snapshot (Chandy-Lamport)

### 6.1 Algorithm Specification

**Assumptions:**
- FIFO channels (messages not overtaken)
- Reliable message delivery
- Connected process graph

**Algorithm:**

```
INITIATE_SNAPSHOT at process Pi:
  1. Record Pi's local state Si
  2. Send MARKER on all outgoing channels
  3. Begin recording all incoming channels Cji (start empty recording)

On RECEIVE(MARKER) at process Pj from channel Cij:
  If Pj has NOT yet recorded its state:
    // First marker Pj sees
    1. Record Pj's local state Sj
    2. Send MARKER on ALL outgoing channels from Pj
    3. Mark channel Cij state as empty  (no messages between snapshot points)
    4. Begin recording all OTHER incoming channels Cki (k≠i)
  Else:
    // Pj already snapshotted — finalize this channel
    1. Channel Cij state = messages received on Cij since Pj recorded its state
       (these messages were in transit when Pj snapshotted)
    2. Stop recording Cij

SNAPSHOT COMPLETE when:
  All processes have recorded state AND all channels have been recorded
  Global state = ∪{Si} ∪ ∪{channel_states}
```

**Go implementation:**
```go
type LocalSnapshot struct {
    ProcessID   string
    LocalState  interface{}       // process state at snapshot time
    VectorClock VectorClock       // VC at time of snapshot
    ChannelStates map[string][]*Message // channelID → in-transit messages
    RecordingChans map[string]bool      // which channels we're recording
    Finalized   bool
}

type GlobalSnapshot struct {
    SnapshotID    string
    InitiatorID   string
    StartTime     time.Time
    LocalStates   map[string]*LocalSnapshot
    Complete      bool
    ConsistentCut Cut  // computed after completion
}

type SnapshotCoordinator struct {
    snapshots map[string]*GlobalSnapshot
    mu        sync.RWMutex
}
```

### 6.2 Consistent Cut Verification

After a Chandy-Lamport snapshot completes, verify consistency:

```
For each message m in any channel_state(Cij):
  ASSERT: send(m) is in Pi's recorded state
  ASSERT: receive(m) is NOT in Pj's recorded state
  (m was sent before Pi's snapshot point, but arrived after Pj's snapshot point)
```

### 6.3 Properties of Recorded Global State

The recorded state `S*` is a valid global state: it corresponds to a state the system could have been in, even if no process was actually in state `S*` at the same physical instant.

The algorithm also guarantees that `S*` is **reachable** from the initial state and the **final state** is reachable from `S*`.

---

## 7. Conflict Detection & Resolution

### 7.1 Conflict Detection via Vector Clocks

```go
type Conflict struct {
    Key      string
    Siblings []*ValueVersion  // concurrent versions
    Detected time.Time
}

type ValueVersion struct {
    Value   []byte
    Clock   VectorClock  // or DVV
    Author  string       // which process wrote this
    Written time.Time
}

type ConflictResolution int
const (
    LastWriterWins ConflictResolution = iota  // by wall clock Timestamp
    FirstWriterWins
    MergeFunc                                  // user-provided merge function
    KeepAll                                    // return all siblings (CouchDB style)
    ManualResolution                           // flag for user intervention
)
```

**Conflict detection algorithm:**
```
On WRITE(key, value, context_vc) at replica R:
  current = R.get(key)
  if current == nil:
    store value with VCnew = {R: 1}
    return
  
  order = Compare(context_vc, current.Clock)
  switch order:
    case HappenedBefore:
      // client wrote based on stale read — overwrite
      new_vc = Increment(Merge(context_vc, current.Clock), R.ID)
      store value with new_vc
    case Identical, HappenedAfter:
      // client had latest version — overwrite
      new_vc = Increment(context_vc, R.ID)
      store value with new_vc
    case Concurrent:
      // CONFLICT: keep both as siblings
      emit ConflictDetected event
      store value WITH current as sibling
      new_vc = Increment(Merge(context_vc, current.Clock), R.ID)
```

### 7.2 Anti-Entropy (Version Vector Sync)

```
SYNC(replica A, replica B) for key K:
  vv_A = A.vc(K)
  vv_B = B.vc(K)
  
  Compare(vv_A, vv_B):
    HappenedBefore → B's version dominates → A accepts B's value, VV
    HappenedAfter  → A's version dominates → B accepts A's value, VV
    Identical      → No action needed
    Concurrent     → Both keep siblings until resolved
    
  After sync: A and B have consistent view of conflicts for K
```

---

## 8. Causal KV Store

### 8.1 Overview

An in-memory key-value store where every value is tagged with a causal context (vector clock), reads return the context, and writes include the context to establish causality.

```go
type CausalKV struct {
    mu      sync.RWMutex
    store   map[string]*MVCCEntry  // key → multi-version entry
    clock   VectorClock            // this replica's current VC
    replicaID string
}

type MVCCEntry struct {
    Key      string
    Versions []*ValueVersion  // siblings if concurrent writes exist
}

// Client reads — returns value + causal context token
func (kv *CausalKV) Get(key string) ([]byte, VectorClock, error)

// Client writes — must include context from prior read
func (kv *CausalKV) Put(key string, value []byte, context VectorClock) (VectorClock, error)

// Internal replication — merge incoming version
func (kv *CausalKV) Merge(key string, version *ValueVersion) error

// Conflict resolution — apply strategy + emit event
func (kv *CausalKV) Resolve(key string, strategy ConflictResolution) error

// Sync with peer replica
func (kv *CausalKV) AntiEntropy(peer *CausalKV) ([]string, error)  // returns resolved keys
```

### 8.2 MVCC (Multi-Version Concurrency Control)

Each key maintains all concurrent versions (siblings). Version `a` is discarded if any other version `b` causally dominates it (`a → b`). Only truly concurrent versions are kept.

```
Invariant: ∀ versions V1, V2 stored for key K:
  Compare(V1.Clock, V2.Clock) == Concurrent
  (if one dominated the other, the dominated one would have been discarded)
```

---

## 9. Simulation Engine

### 9.1 Process Lifecycle

```go
type ProcessConfig struct {
    ID        string
    ClockType ClockType      // Lamport | Vector | Matrix
    Delivery  DeliveryMode   // Immediate | Causal | TotalOrder
    Channels  []string       // connected peer process IDs
    KVEnabled bool           // whether this process has a causal KV store
}

type Simulation struct {
    processes   map[string]*Process
    transport   *SimTransport
    eventBus    *EventBus
    snapshots   *SnapshotCoordinator
    config      *SimConfig
    mu          sync.RWMutex
}

// Control commands
func (s *Simulation) SpawnProcess(cfg ProcessConfig) error
func (s *Simulation) KillProcess(id string) error
func (s *Simulation) SendMessage(from, to string, data interface{}) error
func (s *Simulation) BroadcastMessage(from string, data interface{}) error
func (s *Simulation) InternalEvent(pid string) error  // trigger Tick
func (s *Simulation) TriggerSnapshot(initiatorID string) (string, error)
func (s *Simulation) InjectDelay(from, to string, d time.Duration) error
func (s *Simulation) InjectReorder(from, to string) error  // swap next 2 messages
func (s *Simulation) InjectDrop(from, to string) error     // drop next message
```

### 9.2 Scenarios

Pre-built scenarios that demonstrate key concepts:

| Scenario | What it Shows |
|----------|--------------|
| `BasicLamport` | 3 processes, 5 events — shows LC total order |
| `ConcurrentWrites` | 2 processes write concurrently — VC detects ∥ |
| `CausalViolation` | Disabled delivery — shows out-of-order receive |
| `CausalDelivery` | Same as above with hold-back queue — correct ordering |
| `FalseConflict` | Plain VV shows false conflict; DVV shows it's not a conflict |
| `Snapshot3P` | 3-process Chandy-Lamport — marker propagation animated step by step |
| `MatrixGC` | 4 processes — shows min_k(MT[k][j]) enabling log truncation |
| `PartitionAndHeal` | Network partition + reconciliation with sibling resolution |
| `TotalOrderBroadcast` | Lamport total order algorithm with tie-breaking |

```go
type Scenario struct {
    Name        string
    Description string
    Steps       []ScenarioStep
}

type ScenarioStep struct {
    Delay   time.Duration  // pause before this step
    Action  func(*Simulation) error
    Narrate string         // text shown in UI overlay
}
```

### 9.3 Message Transport

```go
type SimTransport struct {
    channels  map[ChannelKey]chan *Message  // buffered FIFO channels
    mu        sync.RWMutex
    
    // Fault injection
    delays    map[ChannelKey]time.Duration
    dropNext  map[ChannelKey]bool
    reorderQ  map[ChannelKey][]*Message  // for reorder injection
    
    // FIFO guarantee enforcement
    seqNums   map[ChannelKey]uint64  // per-channel sequence numbers
}

type ChannelKey struct {
    From, To string
}
```

---

## 10. Event System

Every state change emits a structured event. The frontend reconstructs the full space-time diagram from the event log.

```go
type EventType string
const (
    EvtInternalEvent   EventType = "internal_event"
    EvtSend            EventType = "send"
    EvtReceive         EventType = "receive"
    EvtHeld            EventType = "message_held"      // added to hold-back queue
    EvtDelivered       EventType = "message_delivered"  // released from hold-back
    EvtClockUpdate     EventType = "clock_update"
    EvtSnapshotStart   EventType = "snapshot_start"
    EvtMarkerSent      EventType = "marker_sent"
    EvtMarkerReceived  EventType = "marker_received"
    EvtLocalSnapshot   EventType = "local_snapshot"
    EvtSnapshotComplete EventType = "snapshot_complete"
    EvtConflict        EventType = "conflict"
    EvtResolved        EventType = "resolved"
    EvtProcessSpawned  EventType = "process_spawned"
    EvtProcessKilled   EventType = "process_killed"
    EvtScenarioStep    EventType = "scenario_step"
)

type Event struct {
    ID        string          `json:"id"`
    Type      EventType       `json:"type"`
    ProcessID string          `json:"processId"`
    Timestamp time.Time       `json:"timestamp"`
    
    // Clock state at time of event
    LamportClock  *uint64                       `json:"lamportClock,omitempty"`
    VectorClock   map[string]uint64             `json:"vectorClock,omitempty"`
    MatrixClock   map[string]map[string]uint64  `json:"matrixClock,omitempty"`
    
    // Message details (for Send/Receive/Held/Delivered)
    Message *MessagePayload `json:"message,omitempty"`
    
    // Snapshot details
    Snapshot *SnapshotPayload `json:"snapshot,omitempty"`
    
    // Conflict details
    Conflict *ConflictPayload `json:"conflict,omitempty"`
    
    // Narration (for scenarios)
    Narration string `json:"narration,omitempty"`
    
    // Sequence number (for diagram x-axis positioning)
    LocalSeq  uint64 `json:"localSeq"`   // per-process event index
    GlobalSeq uint64 `json:"globalSeq"`  // monotonic global counter
}
```

---

## 11. Go Backend Architecture

### 11.1 Package Structure

```
vectorclock-system/
├── cmd/server/main.go          # HTTP server + simulation bootstrap
├── internal/
│   ├── clock/
│   │   ├── lamport/            # LamportClock struct + rules
│   │   ├── vector/             # VectorClock + Compare
│   │   ├── matrix/             # MatrixClock + min_k
│   │   ├── version/            # VersionVector
│   │   └── dvv/                # DottedVersionVector + DVVSet
│   ├── causality/
│   │   ├── graph.go            # CausalGraph: edges, happened-before query
│   │   ├── cut.go              # Cut + IsConsistent + FindConsistentCuts
│   │   └── order.go            # Compare, TotalOrder, ConcurrentSet
│   ├── process/
│   │   ├── process.go          # Process goroutine + state machine
│   │   ├── holdback.go         # HoldBackQueue + TryDeliver
│   │   └── kvstore.go          # CausalKV embedded in process
│   ├── broadcast/
│   │   ├── causal.go           # BSS causal broadcast
│   │   └── total_order.go      # Lamport total order broadcast
│   ├── snapshot/
│   │   ├── chandy_lamport.go   # Algorithm implementation
│   │   ├── coordinator.go      # Snapshot lifecycle management
│   │   └── verifier.go         # Consistency check post-snapshot
│   ├── conflict/
│   │   ├── detector.go         # Multi-version conflict detection
│   │   ├── resolver.go         # LWW, Merge, KeepAll strategies
│   │   └── antientropy.go      # VV-based sync
│   ├── simulation/
│   │   ├── simulation.go       # Top-level Simulation orchestrator
│   │   ├── transport.go        # SimTransport FIFO channels + fault injection
│   │   └── scenarios.go        # Canned scenario definitions
│   └── events/
│       ├── bus.go              # EventBus pub/sub
│       └── types.go            # All event struct definitions
├── gateway/
│   ├── server.go               # HTTP server setup
│   ├── rest.go                 # REST handlers
│   └── websocket.go            # WebSocket hub
└── frontend/                   # Bun+TS project (see §12)
```

### 11.2 Process State Machine

```
States: Idle → Running → Snapshotting → Dead

Transitions:
  Idle → Running:       SpawnProcess command
  Running → Snapshotting: InitiateSnapshot or ReceiveMarker
  Snapshotting → Running: AllChannelsRecorded
  Running → Dead:       KillProcess command or crash simulation
  
Per state behaviors:
  Running:     Process messages from inbound channel; execute clock rules
  Snapshotting: Record incoming messages; forward markers; wait for all channels
  Dead:         Close all channels; remove from transport registry
```

---

## 12. Bun+TS Frontend Architecture

### 12.1 Technology Choices

| Layer | Technology | Reason |
|-------|------------|--------|
| Runtime & package manager | Bun 1.1+ | Native TS, fast I/O, built-in bundler |
| BFF / API server | Elysia | Bun-native, end-to-end type safety via Eden Treaty, WebSocket built-in |
| UI rendering | Vanilla TypeScript + D3.js | Space-time diagrams require fine-grained SVG control |
| Styling | TailwindCSS (via Bun PostCSS) | Utility-first, works with Bun |
| State management | Nano Stores (`nanostores`) | 1KB, framework-agnostic, reactive |
| Charts | D3.js v7 | SVG diagram for space-time, force graphs for causal DAG |
| Icons | Lucide (vanilla) | Tree-shakable icon set |

**Project layout:**
```
frontend/
├── bun.lockb
├── package.json
├── tsconfig.json
├── index.html               # Entry point
├── src/
│   ├── main.ts              # App bootstrap
│   ├── api/
│   │   ├── client.ts        # Elysia Eden Treaty typed client
│   │   └── types.ts         # Shared API types
│   ├── stores/
│   │   ├── simulation.ts    # Nano store: processes, events, config
│   │   ├── diagram.ts       # Nano store: computed space-time layout
│   │   └── snapshot.ts      # Nano store: active snapshots
│   ├── components/
│   │   ├── SpaceTimeDiagram/
│   │   │   ├── index.ts     # D3 diagram controller
│   │   │   ├── layout.ts    # Event position computation
│   │   │   ├── arrows.ts    # Message arc rendering
│   │   │   ├── clocks.ts    # Clock label overlays
│   │   │   ├── cuts.ts      # Consistent cut line rendering
│   │   │   └── animation.ts # Message transit animation
│   │   ├── ClockInspector/
│   │   ├── CausalDelivery/
│   │   ├── SnapshotViewer/
│   │   ├── ConflictDash/
│   │   └── ScenarioPanel/
│   ├── ws/
│   │   └── socket.ts        # WebSocket connection + event dispatch
│   └── styles/
│       └── tailwind.css
└── server/
    └── bff.ts               # Elysia BFF server (REST proxy + WS hub)
```

### 12.2 Space-Time Diagram Visualizer

This is the core visualization — a faithful implementation of the standard distributed systems notation.

**Layout rules:**
```typescript
// X-axis: logical time (Lamport timestamp or vector clock index)
// Y-axis: process lanes (one per process, evenly spaced)

interface DiagramLayout {
  processLanes: Map<string, number>  // processID → Y coordinate
  eventPositions: Map<string, Point> // eventID → (x, y)
  arrowPaths: MessageArrow[]
  cutLines: CutLine[]
}

// X-position computation:
// For Lamport: x = event.lamportClock * TIME_UNIT
// For Vector: x = event.vectorClock[processID] * TIME_UNIT (own component only)
// Spacing rule: events on same process maintain minimum horizontal gap
function computeLayout(events: Event[], config: DiagramConfig): DiagramLayout
```

**SVG elements:**
```
Process lines:  horizontal SVG lines, one per process
                Label at left: "P1", "P2", etc.

Events:         SVG circle (r=8), colored by type:
                  Internal: blue filled
                  Send:     green filled  
                  Receive:  orange filled
                  Snapshot: purple filled (star shape)
                  
                On hover: tooltip showing full clock state
                
Clock labels:   Text below each event
                  Lamport: single number "[5]"
                  Vector:  "[1,2,0]" or sparse "{P1:1, P2:2}"
                  Matrix:  small matrix grid (rendered as nested text)

Message arrows: SVG curved arc from send-event to receive-event
                  Normal:  solid arrow, grey
                  Held:    dashed orange (in hold-back queue)
                  Released: animated dotted → solid on delivery
                  Violated: red arrow (causal violation in Immediate mode)

Snapshot markers: vertical dashed line at each process's snapshot point
                  When all processes snapshotted: horizontal gray band = consistent cut

Conflict markers: red exclamation badge on events producing concurrent versions
```

**Interaction:**
```
Click event circle:  → opens Clock Inspector panel, highlights causal history
                       (all ancestor events highlighted in blue)
Click message arrow: → opens message detail panel (timestamp, VCsend vs VCrecv)
Click consistent cut: → opens Snapshot Viewer panel
Hover process line:  → highlight all events for this process
Drag event:          → reorder local events for "what-if" analysis  
Zoom/pan:            → standard D3 zoom behavior
Timeline scrubber:   → replay simulation step-by-step, pause at any event
```

### 12.3 Clock State Inspector

```
Panel layout:
  Process selector tabs (P1, P2, P3...)
  
  For selected process at selected event:
  
  Lamport tab:
    Current value: [big number display]
    Comparison: select any other event → show LC(a) vs LC(b), can infer relation?
    
  Vector Clock tab:
    Table: process | this VC component | other event's VC component | ≤?
    Summary: "P1[2,1,0] < P2[2,2,1]? YES → P1's event HAPPENED BEFORE P2's"
    Concurrent detector: select two events → show side-by-side VC comparison
    
  Matrix Clock tab:
    N×N grid rendered as colored heatmap
      - Cell (i,j): MT[i][j] value
      - Row i = process Pi's full view of the world
      - Min column: show min_k(MT[k][j]) in footer of each column
      - Highlight: cells that increased since last event
    
  Version Vector tab (KV operations only):
    Show VV per key in the store
    Sibling indicator: red badge if key has concurrent versions
```

### 12.4 Causal Delivery Monitor

```
Three-column layout:

Column 1 — Message Log:
  All messages with: from | to | sent_vc | status
  Status badges: IN_TRANSIT | HELD | DELIVERED | DROPPED
  Color: green (delivered in causal order) / orange (held) / red (violation)

Column 2 — Hold-Back Queue:
  Per process: list of currently held messages
  For each held message:
    "Waiting for: P2's event #3 (currently at #1)"
    Visual: dependency graph showing what's blocking delivery
    
Column 3 — Delivery Timeline:
  Per process: timeline of delivery events
  Show gaps where causal delay occurred
  Overlay: "Would have violated causality if delivered immediately"
  
Toggle: Compare Immediate vs Causal delivery side-by-side
```

### 12.5 Snapshot Viewer

```
Layout: 3-phase display (Initiation → Propagation → Complete)

Phase 1 — Initiation:
  Highlight initiator process
  Show its local state at snapshot time
  Show marker messages being sent

Phase 2 — Propagation:
  Animated marker propagation across process graph
  Each process: state captured indicator (clock icon with checkmark)
  Channel recording status: recording / finalized / not started
  In-transit messages in channels shown as dots moving along arrows

Phase 3 — Complete:
  Consistent cut line drawn across space-time diagram
  Global state summary: { P1: {state}, P2: {state}, ... }
  Channel states: { C12: [m1, m2], C21: [], ... }  (in-transit messages)
  
Consistency proof panel:
  For each channel message: "send(m) is PRE-CUT at P1 ✓; recv(m) is POST-CUT at P2 ✓"
  Green checkmarks = consistent; Red cross = inconsistent (should never appear)
```

### 12.6 Conflict Dashboard

```
Three sections:

Section 1 — Version History per Key:
  Select key from dropdown
  Show causal DAG of all writes:
    Nodes = (value, VC, author)
    Edges = causal dependencies
    Concurrent nodes at same horizontal level (side-by-side)
    Red background = unresolved conflict
    Green background = resolved

Section 2 — False Conflict Demonstrator:
  Two tabs: Version Vectors | Dotted Version Vectors
  Same scenario played in both modes
  VV tab: "FALSE CONFLICT detected (red)" with explanation
  DVV tab: "No conflict — causally ordered (green)" with explanation
  Step-by-step comparison of the causality tracking difference

Section 3 — Resolution Log:
  All resolved conflicts: key | strategy | winner | loser | timestamp
  Filter by resolution strategy
  "Replay" button: animate the conflict → resolution in space-time diagram
```

### 12.7 Scenario Playground

```
Scenario cards:
  Each card: name | description | concept demonstrated | [Run] button
  
Running scenario:
  Narration panel (left): step-by-step text explaining what's happening
  Space-time diagram (right): auto-animates with 1s delay between steps
  Step controls: [Prev] [Pause/Play] [Next] [Reset]
  Concepts panel (bottom): highlighted theory box for current step
  
Custom scenario builder:
  Timeline editor: drag events onto process lanes
  Message connector: draw arrows between events (automatically sets timestamps)
  Export: generate scenario JSON for sharing
  Import: paste scenario JSON
```

---

## 13. API Specification

### 13.1 REST Endpoints

```
Base: http://localhost:8080/api/v1

# Simulation Control
POST  /simulation/start         Body: {processCount, clockType, deliveryMode}
POST  /simulation/reset
GET   /simulation/state         Full snapshot: all processes, events, clocks

# Process Operations  
POST  /processes                Spawn  Body: {id, clockType, deliveryMode, channels}
DELETE /processes/:id           Kill
GET   /processes/:id            State snapshot (clock, queue, KV store)
POST  /processes/:id/event      Trigger internal event
POST  /processes/:id/snapshot   Initiate Chandy-Lamport from this process

# Message Operations
POST  /messages                 Send  Body: {from, to, data}
POST  /broadcast                Causal broadcast  Body: {from, data}

# KV Store Operations (per-process)
GET   /processes/:id/kv/:key    Read with causal context
POST  /processes/:id/kv         Write  Body: {key, value, context_vc}
GET   /processes/:id/kv/:key/versions  All versions (siblings)
POST  /processes/:id/kv/:key/resolve   Body: {strategy}

# Snapshot Operations
GET   /snapshots                List all snapshots
GET   /snapshots/:id            Full snapshot state
GET   /snapshots/:id/verify     Consistency proof

# Causal Graph
GET   /causality/graph          Full event graph (nodes + edges)
GET   /causality/happened-before?a=eventID&b=eventID
GET   /causality/concurrent?a=eventID&b=eventID
GET   /causality/cuts           All consistent cuts

# Fault Injection
POST  /faults/delay             Body: {from, to, delayMs}
POST  /faults/drop              Body: {from, to}
POST  /faults/reorder           Body: {from, to}
POST  /faults/partition         Body: {groups: [[p1, p2], [p3, p4]]}
POST  /faults/heal

# Scenarios
GET   /scenarios                List
POST  /scenarios/:name/run
GET   /scenarios/:name/steps    Pre-computed step plan

# Metrics
GET   /metrics/causal-depth     Distribution of causal chain lengths
GET   /metrics/conflict-rate    Conflicts per time window
GET   /metrics/delivery-delay   Avg time messages wait in hold-back queue
```

### 13.2 WebSocket Protocol

```
Endpoint: ws://localhost:8080/ws  (Go) or ws://localhost:3001/ws  (Elysia BFF)

Client → Server:
  { "action": "subscribe", "types": ["*"] }  // or specific event types
  { "action": "unsubscribe", "types": ["clock_update"] }
  { "action": "replay", "from": 0, "to": 100 }  // replay events [0,100]

Server → Client (event stream, one JSON object per line):
  { "type": "internal_event", "processId": "P1", "localSeq": 3,
    "vectorClock": {"P1": 3, "P2": 1}, "timestamp": "..." }
  
  { "type": "send", "processId": "P1", "localSeq": 4,
    "message": {"id": "m1", "to": "P2", "data": "..."},
    "vectorClock": {"P1": 4, "P2": 1} }
    
  { "type": "message_held", "processId": "P2",
    "message": {"id": "m3"}, "blockedBy": ["m2"],
    "holdBackQueueSize": 1 }
    
  { "type": "snapshot_complete", "snapshotId": "s1",
    "globalState": {...}, "consistentCut": {...} }

  { "type": "conflict", "key": "user:42",
    "siblings": [{"value":"...", "clock":{...}}, ...] }
```

### 13.3 Elysia BFF (`frontend/server/bff.ts`)

```typescript
import { Elysia, t } from 'elysia'
import { cors } from '@elysiajs/cors'

const GO_BACKEND = 'http://localhost:8080'

const app = new Elysia()
  .use(cors())
  
  // Proxy REST to Go backend
  .get('/api/*', ({ params, query }) =>
    fetch(`${GO_BACKEND}/api/${params['*']}?${new URLSearchParams(query)}`)
      .then(r => r.json())
  )
  .post('/api/*', ({ params, body }) =>
    fetch(`${GO_BACKEND}/api/${params['*']}`, {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify(body)
    }).then(r => r.json())
  )
  
  // WebSocket fan-out hub (multiple browser clients share one WS to Go)
  .ws('/ws', {
    open(ws) { registerClient(ws) },
    close(ws) { removeClient(ws) },
    message(ws, msg) { handleClientMessage(ws, msg) }
  })
  
  // Serve frontend static files
  .get('/', () => Bun.file('./dist/index.html'))
  .get('/assets/*', ({ params }) => Bun.file(`./dist/assets/${params['*']}`))
  
  .listen(3001)

// Eden Treaty typed client (in frontend src/api/client.ts):
// import { treaty } from '@elysiajs/eden'
// const client = treaty<typeof app>('localhost:3001')
// const { data } = await client.api.processes.get()  ← fully typed!
```

---

## 14. Data Models

### 14.1 Core Go Types

```go
// Process state
type ProcessState struct {
    ID           string
    ClockType    ClockType
    Delivery     DeliveryMode
    LamportClock uint64
    VectorClock  map[string]uint64
    MatrixClock  map[string]map[string]uint64
    Peers        []string
    EventCount   int
    HeldMessages int
    KVKeys       []string
    Status       ProcessStatus // Running | Snapshotting | Dead
}

// Message
type Message struct {
    ID        string
    From      string
    To        string
    Data      interface{}
    SentVC    map[string]uint64
    SentLC    uint64
    SentAt    time.Time
    IsMarker  bool    // Chandy-Lamport marker
    SnapshotID string // which snapshot this marker belongs to
}

// Event (emitted to bus, sent over WebSocket)
type Event struct {
    ID           string
    Type         EventType
    ProcessID    string
    Timestamp    time.Time
    LocalSeq     uint64
    GlobalSeq    uint64
    LamportClock *uint64
    VectorClock  map[string]uint64
    MatrixClock  map[string]map[string]uint64
    Message      *MessagePayload
    Snapshot     *SnapshotPayload
    Conflict     *ConflictPayload
    Narration    string
}
```

### 14.2 TypeScript Frontend Types

```typescript
// Mirror of Go event types, auto-generated from OpenAPI or manually synced
export type ClockType = 'lamport' | 'vector' | 'matrix'
export type DeliveryMode = 'immediate' | 'causal' | 'total_order'
export type EventType = 
  | 'internal_event' | 'send' | 'receive'
  | 'message_held' | 'message_delivered'
  | 'clock_update'
  | 'snapshot_start' | 'marker_sent' | 'marker_received'
  | 'local_snapshot' | 'snapshot_complete'
  | 'conflict' | 'resolved'
  | 'process_spawned' | 'process_killed'
  | 'scenario_step'

export type VectorClock = Record<string, number>
export type MatrixClock = Record<string, VectorClock>

export interface DHTEvent {  // renamed to avoid DOM Event collision
  id: string
  type: EventType
  processId: string
  timestamp: string
  localSeq: number
  globalSeq: number
  lamportClock?: number
  vectorClock?: VectorClock
  matrixClock?: MatrixClock
  message?: MessagePayload
  snapshot?: SnapshotPayload
  conflict?: ConflictPayload
  narration?: string
}

// Simulation state (kept in nanostores)
export interface SimulationState {
  processes: Map<string, ProcessState>
  events: DHTEvent[]        // append-only log
  activeSnapshot: string | null
  config: SimConfig
}
```

---

## 15. Configuration Reference

```yaml
# config.yaml
server:
  port: 8080
  ws_buffer: 256          # event buffer per WebSocket connection

simulation:
  initial_processes: 3
  clock_type: vector       # lamport | vector | matrix
  delivery_mode: causal    # immediate | causal | total_order
  channels: full_mesh      # full_mesh | ring | custom
  
timing:
  internal_event_interval: 0     # 0 = manual only
  message_transit_delay: 50ms    # base delay for all messages
  reorder_probability: 0.0       # probability of message reorder
  drop_probability: 0.0          # probability of message drop

snapshot:
  fifo_channels: true            # enforced by transport
  
kv:
  conflict_strategy: keep_all    # lww | first_writer | merge | keep_all

frontend:
  port: 3001                     # Elysia BFF port
  go_backend: http://localhost:8080

logging:
  level: info
  format: json
```

---

## 16. Testing Strategy

### 16.1 Clock Algorithm Tests

```
internal/clock/lamport/   lamport_test.go
  - LC increments on internal event
  - LC increments on send  
  - LC = max(LC, recv_ts) + 1 on receive
  - Total order with tie-breaking is deterministic and consistent
  - 100-event log: TotalOrder gives consistent total ordering

internal/clock/vector/    vector_test.go
  - Isomorphism: a → b ⟺ VC(a) < VC(b)
  - Compare returns Concurrent for truly concurrent events
  - Merge is commutative and idempotent
  - BSS causal delivery: 100 scenarios, never violates causal order
  - Charron-Bost theorem: N-process system requires N-element VC

internal/clock/matrix/    matrix_test.go
  - MT[self] after receive = max(MT[self], MTmsg[sender])
  - min_k(MT[k][j]): correct garbage collection lower bound
  - 4-process ring: after 10 rounds, all processes know all others' counts

internal/clock/dvv/       dvv_test.go
  - DVV prevents false conflicts that plain VV produces (regression test)
  - Sibling detection: concurrent writes correctly detected
  - Dominated version correctly discarded
```

### 16.2 Causal Ordering Tests

```
internal/causality/       causality_test.go
  - CausalGraph.HappenedBefore: transitive closure correct
  - ConsistentCut verification: 10 generated cuts, 5 consistent, 5 not
  - FindConsistentCuts: count matches theoretical bound
  
internal/delivery/        delivery_test.go  
  - HoldBackQueue releases messages in causal order
  - Messages genuinely blocked until causal predecessors delivered
  - No deadlock: if m1 → m2, m2 held until m1 delivered
  - Comparison: ImmediateDelivery produces violation in known scenario
                CausalDelivery produces no violation in same scenario
```

### 16.3 Chandy-Lamport Tests

```
internal/snapshot/        snapshot_test.go
  - 3-process snapshot: verify marker propagation
  - Channel state recording: in-transit messages correctly captured
  - Consistency check: recorded global state passes ConsistentCut.IsConsistent
  - Concurrent initiators: two simultaneous initiators produce valid snapshot
  - Messages in channels: verify send(m) is pre-cut, recv(m) is post-cut
```

### 16.4 Conflict Detection Tests

```
internal/conflict/        conflict_test.go
  - Concurrent writes → siblings created (not overwritten)
  - Causally ordered writes → no conflict (dominated version discarded)
  - LWW resolution: winner has latest timestamp
  - KeepAll: both siblings preserved until explicit resolve
  - AntiEntropy: after sync, replicas agree on conflict state
  - FalseConflict scenario: plain VV shows conflict, DVV does not
```

### 16.5 Integration Tests

```
test/integration/
  - 5-process causal broadcast: all processes deliver in causal order
  - Snapshot during active broadcast: snapshot is consistent
  - Partition + heal: after heal, no violations
  - Matrix clock GC: log truncation does not lose causality info
```

---

## 17. Performance Targets

| Metric | Target |
|--------|--------|
| Event processing throughput | 10,000 events/s per process |
| WebSocket event broadcast latency | < 5ms (Go → Elysia → browser) |
| Causal delivery hold-back check | O(N × Q) where N=processes, Q=queue depth |
| Space-time diagram render (100 events) | < 16ms (60fps) |
| Vector clock comparison | O(N) |
| Matrix clock merge | O(N²) |
| Chandy-Lamport completion | O(E) messages where E=edges in process graph |
| Consistent cut enumeration | < 100ms for up to 10 processes × 20 events |

---

## 18. File Structure

```
vectorclock-system/
│
├── cmd/
│   └── server/
│       └── main.go
│
├── internal/
│   ├── clock/
│   │   ├── lamport/
│   │   │   ├── clock.go
│   │   │   └── clock_test.go
│   │   ├── vector/
│   │   │   ├── clock.go          # VectorClock + Compare + Merge
│   │   │   ├── compare.go        # ClockOrder comparisons
│   │   │   └── clock_test.go
│   │   ├── matrix/
│   │   │   ├── clock.go          # MatrixClock + min_k + GC bound
│   │   │   └── clock_test.go
│   │   ├── version/
│   │   │   ├── vector.go         # VersionVector
│   │   │   └── vector_test.go
│   │   └── dvv/
│   │       ├── dvv.go            # DottedVersionVector + DVVSet
│   │       └── dvv_test.go
│   │
│   ├── causality/
│   │   ├── graph.go
│   │   ├── cut.go
│   │   ├── order.go
│   │   └── causality_test.go
│   │
│   ├── process/
│   │   ├── process.go
│   │   ├── holdback.go
│   │   ├── kvstore.go
│   │   └── process_test.go
│   │
│   ├── broadcast/
│   │   ├── causal.go
│   │   ├── total_order.go
│   │   └── broadcast_test.go
│   │
│   ├── snapshot/
│   │   ├── chandy_lamport.go
│   │   ├── coordinator.go
│   │   ├── verifier.go
│   │   └── snapshot_test.go
│   │
│   ├── conflict/
│   │   ├── detector.go
│   │   ├── resolver.go
│   │   ├── antientropy.go
│   │   └── conflict_test.go
│   │
│   ├── simulation/
│   │   ├── simulation.go
│   │   ├── transport.go
│   │   └── scenarios.go
│   │
│   └── events/
│       ├── bus.go
│       └── types.go
│
├── gateway/
│   ├── server.go
│   ├── rest.go
│   └── websocket.go
│
├── frontend/
│   ├── package.json
│   ├── tsconfig.json
│   ├── bunfig.toml
│   ├── index.html
│   ├── src/
│   │   ├── main.ts
│   │   ├── api/
│   │   │   ├── client.ts        # Eden Treaty typed client
│   │   │   └── types.ts
│   │   ├── stores/
│   │   │   ├── simulation.ts
│   │   │   ├── diagram.ts
│   │   │   └── snapshot.ts
│   │   ├── components/
│   │   │   ├── SpaceTimeDiagram/
│   │   │   │   ├── index.ts
│   │   │   │   ├── layout.ts
│   │   │   │   ├── arrows.ts
│   │   │   │   ├── clocks.ts
│   │   │   │   ├── cuts.ts
│   │   │   │   └── animation.ts
│   │   │   ├── ClockInspector/
│   │   │   │   └── index.ts
│   │   │   ├── CausalDelivery/
│   │   │   │   └── index.ts
│   │   │   ├── SnapshotViewer/
│   │   │   │   └── index.ts
│   │   │   ├── ConflictDash/
│   │   │   │   └── index.ts
│   │   │   └── ScenarioPanel/
│   │   │       └── index.ts
│   │   ├── ws/
│   │   │   └── socket.ts
│   │   └── styles/
│   │       └── tailwind.css
│   └── server/
│       └── bff.ts               # Elysia BFF
│
├── test/
│   └── integration/
│       └── integration_test.go
│
├── config.yaml
├── go.mod
├── go.sum
├── Makefile
├── SPEC.md
├── CHECKLIST.md
└── README.md