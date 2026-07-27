# Conflict Detection & Resolution

The Vector Clock Lab includes a multi-version causal KV store that detects concurrent writes using version vectors and resolves conflicts with pluggable strategies.

---

## What is a conflict?

A conflict occurs when two writes to the same key are **concurrent**: neither write happened-before the other. If `A → B`, then `B` supersedes `A` with no conflict. If `A ∥ B` (concurrent), both writes must be preserved until a resolution policy is applied.

In a distributed system, concurrent writes happen whenever two replicas receive a write without first seeing each other's latest version.

---

## Version vectors for conflict detection

Each value in the KV store carries a **version vector** `V` — one entry per replica that has contributed to this value's causal history:

```
V[r] = number of writes by replica r that are causally before this value
```

### Comparison rules

```
V dominates W   ⟺   ∀r: V[r] ≥ W[r]  ∧  ∃r: V[r] > W[r]
V equal to W    ⟺   ∀r: V[r] = W[r]
V concurrent W  ⟺   ¬(V dominates W)  ∧  ¬(W dominates V)
```

### Write semantics

When process Pᵢ writes `key = value`:
1. Fetch the current stored version vector `V_stored`.
2. Pᵢ's local vector clock must dominate `V_stored` (Pᵢ has seen the latest value).
3. Pᵢ increments its own entry: `V_new[i] = V_stored[i] + 1`.
4. Write `(value, V_new)` to the store.

If Pᵢ has **not** seen `V_stored` (its clock does not dominate `V_stored`), the write is concurrent — a conflict.

---

## Dotted version vectors (DVV)

Plain version vectors cannot distinguish two concurrent writes from the same replica. **Dotted version vectors** attach a **dot** `(replica, N)` to each value, uniquely identifying it:

```
dot = (r, N)  where N is the Nth write by replica r
```

On merge, dots that are not dominated by the other version vector's history are retained as concurrent values. This prevents the "sibling explosion" problem: only genuinely concurrent writes are kept, not all past writes.

---

## Conflict resolution strategies

Configured via `kv.conflict_strategy` in `config.yaml`.

### `keep_all` (default)

Retain all concurrent versions. Reads return all versions; the application must choose.

```json
{
  "key": "x",
  "versions": [
    {"value":"hello","version":{"P1":1,"P2":0},"writer":"P1"},
    {"value":"world","version":{"P1":0,"P2":1},"writer":"P2"}
  ],
  "conflict": true
}
```

**Use when**: the application has domain-specific merge logic (e.g. CRDT-style).

### `lww` — Last Writer Wins

The write with the higher Lamport timestamp wins. Ties are broken by process ID (lexicographic).

```go
// internal/conflict
func LWW(versions []Version) Version {
    sort.Slice(versions, func(i, j int) bool {
        if versions[i].LamportTS != versions[j].LamportTS {
            return versions[i].LamportTS > versions[j].LamportTS
        }
        return versions[i].Writer > versions[j].Writer
    })
    return versions[0]
}
```

**Use when**: last write always wins is acceptable and clocks are roughly synchronised.

**Caveat**: LWW can silently discard writes. Two concurrent writes to the same key — the "loser" is gone forever.

### `first_writer` — First Writer Wins

The write with the lower Lamport timestamp wins. Same tie-breaking as LWW.

**Use when**: protecting "first reservation wins" semantics (e.g. ticket allocation).

### `merge`

Merge all concurrent values into one combined value. The merge function is type-specific — for strings, values are joined with `|`. For numeric counters, values are summed.

**Use when**: values are CRDTs (counters, sets, maps) designed to be merged.

---

## Observing conflicts in the lab

### ConflictDash panel

The frontend's ConflictDash shows:
- All keys with concurrent versions.
- Side-by-side comparison of version vectors.
- Visual dominance indicator: which versions dominate which.
- Active resolution strategy and resolved value (if not `keep_all`).

### WebSocket events

| Event | Fired when |
|-------|-----------|
| `kv_write` | Any write to the KV store |
| `kv_conflict` | A write creates concurrent versions |
| `kv_resolved` | A resolution strategy was applied |

### REST API

```bash
# Write from P1
curl -X PUT http://localhost:8080/api/v1/kv/x \
  -d '{"value":"hello","writer":"P1"}'

# Write from P2 concurrently
curl -X PUT http://localhost:8080/api/v1/kv/x \
  -d '{"value":"world","writer":"P2"}'

# Read — shows conflict
curl http://localhost:8080/api/v1/kv/x
```

---

## The FalseConflict scenario

The `FalseConflict` scenario demonstrates that version vectors correctly identify **non-conflicting** writes even when the values differ:

1. P1 writes `x = "v1"`.
2. P2 reads P1's write (vector clock updated: P2 now knows P1's write).
3. P2 writes `x = "v2"` — P2's version vector dominates P1's.
4. No conflict: P2's write is the causal successor.

This shows version vectors avoid false conflicts that a naive "last write wins by wall clock" approach would get wrong.

---

## Implementation: `internal/conflict/`

```go
// Version represents one version of a KV value
type Version struct {
    Value     string
    VV        map[string]uint64  // version vector
    LamportTS uint64
    Writer    string
    Dot       *Dot               // optional DVV dot
}

// Store — multi-version KV store
type Store struct {
    mu       sync.RWMutex
    data     map[string][]Version
    strategy ResolutionStrategy
}

func (s *Store) Put(key string, v Version) (conflict bool) { … }
func (s *Store) Get(key string) []Version { … }
```

The `Put` method:
1. Checks if any existing version is dominated by `v.VV` (if so, replace it).
2. Checks if `v.VV` is dominated by any existing version (if so, drop — stale write).
3. Otherwise (concurrent): appends; fires `kv_conflict` event; runs resolution strategy if not `keep_all`.

---

## Further reading

- Parker, D. et al. (1983). [Detection of mutual inconsistency in distributed systems.](https://doi.org/10.1109/TSE.1983.236661) *IEEE TSE* 9(3).
- DeCandia, G. et al. (2007). [Dynamo: Amazon's highly available key-value store.](https://dl.acm.org/doi/10.1145/1294261.1294281) *SOSP*.
- Preguiça, N. et al. (2010). [A dotted version vector: Managing causality in distributed key-value stores.](https://arxiv.org/abs/1011.5808) *SRDS*.
- Shapiro, M. et al. (2011). [Conflict-free replicated data types.](https://doi.org/10.1007/978-3-642-24550-3_29) *SSS*.
