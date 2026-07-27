# Core Concepts

This page introduces the fundamental distributed systems concepts the Vector Clock Lab implements. All of the lab's clock algorithms, delivery guarantees, and snapshot mechanisms are built on these primitives.

---

## Physical time is not enough

In a single-machine system, processes share a physical clock, so every event has a wall-clock timestamp that provides a total order. In a distributed system, physical clocks drift and can never be perfectly synchronised. Even a 1 ms skew makes it impossible to reliably determine whether event A at node 1 happened before or after event B at node 2.

Lamport's 1978 insight: **causality, not clock agreement, is what matters**. We want to know which events could have influenced which others — not what the wall clock said.

---

## The happened-before relation (→)

Lamport defines a partial order over events called **happened-before**, written `→`:

1. If A and B are events in the same process and A comes before B, then `A → B`.
2. If A is the send of a message and B is the receipt of that same message, then `A → B`.
3. If `A → B` and `B → C`, then `A → C` (transitivity).

Two events A and B are **concurrent** (written `A ∥ B`) if neither `A → B` nor `B → A`. Concurrent events cannot have influenced each other — there is no causal path between them.

!!! note "Why this matters"
    Concurrent events can be observed in different orders at different nodes with no correctness problem. Events related by `→` must be observed in that order at any node that needs both.

---

## Logical clocks: capturing happened-before

A **logical clock** is a function `C` that assigns a value to each event such that:

```
A → B  ⟹  C(A) < C(B)
```

The converse is not required for Lamport clocks but is required for vector clocks (the *strong clock condition*):

```
C(A) < C(B)  ⟺  A → B          (strong clock condition)
```

### Lamport scalar clocks

The simplest logical clock. Each process maintains a counter:

- **Send**: increment counter, attach to message.
- **Receive**: `counter := max(local, received) + 1`.

Lamport clocks establish the implication `A → B ⟹ C(A) < C(B)` but *not* the converse. Two events with `C(A) < C(B)` may be concurrent.

### Vector clocks

Each process `Pᵢ` maintains a vector `V[1..N]`, one entry per process:

- **Send**: increment `V[i]`, attach full vector.
- **Receive**: `V[j] := max(V[j], received[j])` for all `j`, then `V[i]++`.

Vector clocks satisfy the **strong clock condition**:

```
A → B  ⟺  V(A) < V(B)   (element-wise ≤, at least one strictly <)
```

This means: given two events and their vector timestamps, you can determine whether one caused the other, or whether they are concurrent. Lamport clocks cannot do this.

---

## Causal consistency

A system is **causally consistent** if:

> Any process that sees the effect of event B must also have seen event A, for all A → B.

More concretely: if P1 sends `m1`, P2 receives `m1` and sends `m2`, then any process that receives `m2` must also have received `m1` first.

**Causal delivery** enforces causal consistency at the message layer: a process may not deliver a message until all causally prior messages have been delivered. The lab implements this via BSS hold-back queues — see [Causal Delivery](causal-delivery.md).

---

## Global state and consistent cuts

A **global state** is a snapshot of every process's local state and every channel's in-transit messages at a single logical instant. Collecting global state in a running system is non-trivial because there is no global clock: while we're collecting P1's state, P2 may send a message to P1 that changes P1's state.

A **cut** is a division of the computation into a "past" and a "future" for each process. A cut is **consistent** if: whenever an event `e` is in the past at process Pᵢ, all events that causally preceded `e` at every other process are also in the past.

```
              P1: e1 ──── e2 ──── e3
                             │
              P2: f1 ─── f2 ┘ f3 ── f4
                                    │
              P3: g1 ───────────── g2 ┘ g3
```

The cut `{e2, f2, g2}` is consistent if no message crosses from right (future) to left (past). The Chandy-Lamport algorithm finds such a cut automatically — see [Global Snapshots](snapshot.md).

---

## Causal histories and version vectors

A **causal history** of an event is the set of all events that happened-before it. This is impractical to store directly, but version vectors (one counter per replica) provide a compact encoding: `V[r]` records how many events from replica `r` are in the history.

**Conflict detection**: two values `v1` (with version `V1`) and `v2` (with version `V2`) are concurrent if `V1 ≄ V2` and `V2 ≄ V1` (neither dominates the other). If one dominates, it is the causal successor. Concurrent values must be reconciled — see [Conflict Detection](conflict.md).

---

## Summary table

| Property | Lamport clocks | Vector clocks | Matrix clocks |
|----------|---------------|---------------|---------------|
| Timestamps per process | 1 integer | N integers | N² integers |
| `A→B ⟹ C(A)<C(B)` | ✅ | ✅ | ✅ |
| `C(A)<C(B) ⟹ A→B` | ❌ | ✅ | ✅ |
| Detects concurrent events | ❌ | ✅ | ✅ |
| Knows all processes' knowledge | ❌ | ❌ | ✅ |
| Enables stable message GC | ❌ | ❌ | ✅ |

---

## Further reading

- Lamport, L. (1978). [Time, clocks, and the ordering of events in a distributed system.](https://doi.org/10.1145/359545.359563) *CACM* 21(7).
- Fidge, C. (1988). Timestamps in message-passing systems that preserve the partial ordering. *Proc. 11th Australian CS Conf.*
- Mattern, F. (1989). Virtual time and global states of distributed systems. *Parallel and Distributed Algorithms.*
- Charron-Bost, B. (1991). Concerning the size of logical clocks in distributed systems. *Information Processing Letters* 39(1).
