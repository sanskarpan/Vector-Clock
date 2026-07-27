# Clock Algorithms

The lab implements five logical-clock types behind a uniform Go interface. This page covers the theory, formal rules, properties, and Go API for each.

---

## The `Clock` interface

All clock types satisfy:

```go
// internal/clock — common interface (simplified)
type Clock interface {
    Tick() Clock             // local event: increment self
    Send() Clock             // before sending: same as Tick
    Receive(other Clock)     // on receive: merge + increment
    HappensBefore(other Clock) bool
    Concurrent(other Clock)    bool
    String() string
}
```

---

## Lamport scalar clocks

**Package**: `internal/clock/lamport`

**Paper**: Lamport, L. (1978). *Time, clocks, and the ordering of events.*

### Rules

| Event | Rule |
|-------|------|
| Local event | `L := L + 1` |
| Send message | `L := L + 1`; attach `L` |
| Receive message | `L := max(L, received) + 1` |

### Properties

- Memory: **O(1)** per process.
- `A → B ⟹ C(A) < C(B)` — necessary condition only.
- `C(A) < C(B)` does **not** imply `A → B` — concurrent events may compare equal or in either direction.
- Cannot distinguish concurrent from causally ordered events.

### Go API

```go
import "github.com/DistributedClocks/vectorclock-system/internal/clock/lamport"

lc := lamport.New()
lc = lc.Tick()           // local event
lc = lc.Send()           // before send

received := lamport.FromValue(42)
lc = lc.Receive(received) // on receipt

fmt.Println(lc.Value())   // current counter
```

---

## Vector clocks

**Package**: `internal/clock/vector`

**Papers**: Fidge 1988; Mattern 1989.

### Rules

Each process `Pᵢ` (of N) maintains `V[1..N]`. Only `V[i]` is incremented locally; all entries may change on receive.

| Event | Rule |
|-------|------|
| Local event | `V[i] := V[i] + 1` |
| Send message | `V[i] := V[i] + 1`; attach full `V` |
| Receive message `V'` | `∀j: V[j] := max(V[j], V'[j])`; then `V[i] := V[i] + 1` |

### Comparison

```
V ≤ W  ⟺  ∀i: V[i] ≤ W[i]
V < W  ⟺  V ≤ W  ∧  ∃i: V[i] < W[i]
```

- `A → B  ⟺  V(A) < V(B)` — **strong clock condition** (both directions).
- `A ∥ B  ⟺  V(A) ≰ V(B)  ∧  V(B) ≰ V(A)`.

### Properties

- Memory: **O(N)** per event.
- Fully characterises the happened-before relation.
- Process must know N in advance (or use dynamic expansion).
- No information about what other processes know.

### Go API

```go
import "github.com/DistributedClocks/vectorclock-system/internal/clock/vector"

vc := vector.New("P1", []string{"P1", "P2", "P3"})
vc = vc.Tick()                    // local event

msg := vc.Send()                  // get snapshot for piggybacking

received := vector.FromMap(map[string]uint64{"P1":1,"P2":2,"P3":0})
vc = vc.Receive(received)

fmt.Println(vc.HappensBefore(received))  // false if concurrent
fmt.Println(vc.Concurrent(received))     // true if neither dominates
```

---

## Matrix clocks

**Package**: `internal/clock/matrix`

**Paper**: Kshemkalyani, A. & Singhal, M. (1992). *Efficient detection of message causality.*

### Structure

Process `Pᵢ` maintains an N×N matrix `M` where:
- `M[i][j]` = what `Pᵢ` knows about `Pⱼ`'s clock.
- `M[i][i]` = `Pᵢ`'s own logical clock.
- `M[i][k]` = when `Pᵢ` last heard that `Pₖ` had seen `Pⱼ`'s clock at value `M[i][k]`.

### MC1–MC4 update rules

| Rule | Trigger | Action |
|------|---------|--------|
| MC1 | Internal event at `Pᵢ` | `M[i][i]++` |
| MC2 | Send from `Pᵢ` to `Pⱼ` | MC1 first; attach full `M` |
| MC3 receive (step 1) | `Pᵢ` receives `M'` from `Pⱼ` | `∀k: M[i][k] := max(M[i][k], M'[j][k])` |
| MC3 receive (step 2) | | `∀k: M[i][k] := max(M[i][k], M'[i][k])` (self-row) |
| MC3 receive (step 3) | | `∀r≠i: ∀k: M[r][k] := max(M[r][k], M'[r][k])` (third-party rows) |
| MC4 | After MC3 | `M[i][i]++` |

!!! warning "Step 3 is critical"
    Omitting step 3 (the third-party row merge) breaks matrix clock correctness — third-party knowledge becomes stale. This was a bug fixed in the production audit (Issue #95).

### Properties

- Memory: **O(N²)** per event.
- Enables **stable message GC**: a message from `Pⱼ` with timestamp `t` is stable at `Pᵢ` when `∀k: M[k][j] ≥ t` — every process has acknowledged seeing `Pⱼ`'s state at least up to `t`.
- No information is lost at the cost of quadratic memory.

### Go API

```go
import "github.com/DistributedClocks/vectorclock-system/internal/clock/matrix"

mc := matrix.New("P1", []string{"P1","P2","P3"})
mc = mc.Tick()

msgClock := mc.Send()

received := matrix.FromMap(/* incoming matrix map */)
mc = mc.Receive(received)  // applies MC3 steps 1–3, then MC4
```

---

## Version vectors

**Package**: `internal/clock/version`

**Paper**: Parker, D. et al. (1983). *Detection of mutual inconsistency in distributed systems.*

### Structure

A version vector `V` is a map from replica IDs to event counts. Unlike process vector clocks (which increment on every event), version vectors increment only when a specific replica generates a new write:

```
V[r] = number of writes by replica r that are in this version's causal history
```

### Comparison and conflict detection

```
V dominates W  ⟺  ∀r: V[r] ≥ W[r]  ∧  ∃r: V[r] > W[r]
V == W         ⟺  ∀r: V[r] = W[r]
V and W conflict  ⟺  neither dominates the other
```

If `V` dominates `W`, `V` is the causal successor — no conflict. If neither dominates, the writes are **concurrent** — a conflict. The KV store must resolve or keep both.

### Go API

```go
import "github.com/DistributedClocks/vectorclock-system/internal/clock/version"

vv1 := version.New()
vv1 = vv1.Increment("r1")  // r1 writes

vv2 := version.New()
vv2 = vv2.Increment("r2")  // r2 writes concurrently

fmt.Println(vv1.Dominates(vv2))   // false — concurrent
fmt.Println(vv1.Concurrent(vv2))  // true
```

---

## Dotted version vectors (DVV)

**Package**: `internal/clock/dvv`

**Paper**: Preguiça, N. et al. (2010). *A dotted version vector.*

### Motivation

Plain version vectors conflate concurrent values from the same replica: if R1 writes `A` and `B` concurrently (from different clients), version vectors cannot distinguish them and may lose one value. DVVs attach a **dot** (replica, counter) to each value, uniquely identifying it.

### Structure

A DVV is a pair `(V, dot)`:
- `V` — a version vector representing the causal history the write is based on.
- `dot` — `(replica, N)` identifying this specific write.

### Merge rule

```
merge((V1, d1), (V2, d2)):
  V := merge(V1, V2)   // element-wise max
  keep d1 if d1 not dominated by V2
  keep d2 if d2 not dominated by V1
  // surviving dots = concurrent values to keep
```

### Go API

```go
import "github.com/DistributedClocks/vectorclock-system/internal/clock/dvv"

d1 := dvv.New("r1", 1, version.FromMap(map[string]uint64{}))
d2 := dvv.New("r2", 1, version.FromMap(map[string]uint64{}))

merged := dvv.Merge(d1, d2)
fmt.Println(merged.ConcurrentDots())  // both dots — concurrent write
```

---

## Algorithm comparison

| | Lamport | Vector | Matrix | Version Vector | DVV |
|---|---------|--------|--------|---------------|-----|
| Memory / event | O(1) | O(N) | O(N²) | O(R) | O(R) |
| Detects concurrency | ❌ | ✅ | ✅ | ✅ | ✅ |
| Tracks all-process knowledge | ❌ | ❌ | ✅ | ❌ | ❌ |
| Stable GC | ❌ | ❌ | ✅ | ❌ | ❌ |
| Tracks individual writes | ❌ | ❌ | ❌ | ❌ | ✅ |
| Use case | Total order / ordering proof | Causality tracking | GC, full knowledge | KV conflict detect | KV per-write conflict |

N = number of processes, R = number of replicas
