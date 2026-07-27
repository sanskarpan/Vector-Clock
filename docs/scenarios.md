# Pre-Built Scenarios

The lab ships 8 pre-built scenarios that demonstrate key distributed systems properties. Each scenario resets the simulation to a known state and executes a scripted sequence of events so you can observe the outcome in the space-time diagram and inspector panels.

Run any scenario via the UI's Scenario Panel or the API:

```bash
curl -X POST http://localhost:8080/api/v1/scenarios/Snapshot3P/run
```

---

## BasicLamport

**Demonstrates**: Lamport scalar clocks and the happened-before relation.

**Setup**: 3 processes (P1, P2, P3) with Lamport clock type.

**Script**:
1. P1 sends a message to P2.
2. P2 sends a message to P3.
3. P1 sends a concurrent message directly to P3.

**What to observe**:

- After P2 → P3: P3's clock is `max(P3_local, P2_clock) + 1`.
- The P1 → P3 message arrives with a lower Lamport timestamp than the P2 → P3 message, even though it was sent *after* P2 → P3 was triggered. This illustrates that Lamport clock values don't directly tell you wall-clock order.
- In the space-time diagram, the causal chain P1 → P2 → P3 is visible via arrows; the P1 → P3 arc is concurrent with P2 → P3.

**Key insight**: `C(A) < C(B)` does **not** imply `A → B`. Two events may be concurrent but still have ordered timestamps.

---

## CausalViolation

**Demonstrates**: What happens when causal delivery is **not** enforced.

**Setup**: 3 processes with `delivery_mode: immediate` (no hold-back queues).

**Script**:
1. P1 sends `m1` to P2. P2 applies the message and updates its clock.
2. P2 sends `m2` (which causally depends on `m1`) to P3.
3. P1 also sends `m1'` directly to P3 — but the transport injects a 300 ms delay on P1→P3.
4. With `immediate` delivery, P3 receives `m2` before `m1'`. It sees the effect of `m1` without ever seeing `m1`.

**What to observe**:

- `message_held` is **not** fired (no causal delivery).
- P3's clock jumps over a causal dependency, demonstrating a causal anomaly.
- The ConflictDash shows the broken causal chain.

**Key insight**: Without causal delivery, a process can observe effects before their causes — a consistency violation that causes hard-to-debug distributed bugs.

---

## CausalDelivery

**Demonstrates**: BSS hold-back queues enforcing causal ordering.

**Setup**: 3 processes with `delivery_mode: causal`. Channels P1→P3 have a 200 ms injected delay.

**Script**:
1. P1 sends `m1` to P2 (fast channel).
2. P2 receives `m1`, sends `m2` (causally depends on `m1`) to P3.
3. P1 sends `m1'` directly to P3 via the slow channel.
4. `m2` arrives at P3 before `m1'`. P3 holds `m2` in its hold-back queue.
5. When `m1'` finally arrives, P3 delivers `m1'` then immediately flushes `m2`.

**What to observe**:

- `message_held` event fires for `m2` with `blocked_by: ["m1'"]`.
- The ClockInspector shows the hold-back queue length.
- After `m1'` arrives, both messages are delivered in causal order.

**Key insight**: The BSS condition is checked on every received message — delivery requires that all causally prior messages have already been delivered.

---

## Snapshot3P

**Demonstrates**: The Chandy-Lamport global snapshot algorithm.

**Setup**: 3 processes (P1, P2, P3) with vector clocks, full-mesh channels.

**Script**:
1. A steady stream of messages flows: P1→P2, P2→P3, P3→P1 (forming a ring).
2. While messages are in transit, P1 initiates a global snapshot.
3. P1 captures its local state and sends markers on all outgoing channels (to P2 and P3).
4. P2 and P3 receive markers, capture their own states, and forward markers.
5. Each process records all in-transit messages received after the marker for each incoming channel until it receives that channel's marker.

**What to observe**:

- `snapshot_marker_sent` and `snapshot_marker_received` events.
- The SnapshotViewer shows each process's captured state (vector clock at cut time).
- The channel states show which messages were in-transit during the cut.
- `snapshot_finalized` fires when all processes have received all markers.

**Key insight**: The resulting cut is **consistent** — no process recorded a message send whose receive was not also recorded.

---

## ConcurrentWrites

**Demonstrates**: Version-vector-based conflict detection in the causal KV store.

**Setup**: 3 processes with a KV store, `conflict_strategy: keep_all`.

**Script**:
1. P1 writes `x = "hello"` with its vector clock.
2. P2 (no knowledge of P1's write) writes `x = "world"` concurrently.
3. P3 reads `x` — it sees both versions with `conflict: true`.
4. P1 and P2's version vectors are incomparable (neither dominates), confirming the conflict.

**What to observe**:

- `kv_write` fires twice.
- `kv_conflict` fires when the second write arrives.
- The ConflictDash shows both versions and their version vectors side by side.

**Key insight**: Concurrent writes produce incomparable version vectors. The system cannot automatically choose a winner without application-specific logic.

---

## FalseConflict

**Demonstrates**: Version vectors avoiding false conflicts when one write causally follows another.

**Setup**: 3 processes, `conflict_strategy: lww`.

**Script**:
1. P1 writes `x = "v1"`. P2 receives P1's write (synchronises clocks).
2. P2 writes `x = "v2"` — P2's version vector now dominates P1's.
3. P3 reads `x` — it sees only `"v2"`, no conflict.

**What to observe**:

- Only one version in the KV store.
- P2's version vector `{P1:1, P2:1}` dominates P1's `{P1:1, P2:0}`.

**Key insight**: A later write from a node that has seen the earlier write correctly supersedes it. Version vectors prevent treating causally ordered writes as concurrent.

---

## MatrixGC

**Demonstrates**: Matrix clocks enabling stable-message garbage collection.

**Setup**: 3 processes with `clock_type: matrix`.

**Script**:
1. Steady stream of messages between all three processes.
2. After 10 messages, query the GC stable frontier.
3. The matrix clock's knowledge (`M[k][j]` for all k) identifies messages from each process that every other process has acknowledged.

**What to observe**:

- The ClockInspector shows the full N×N matrix for each process.
- After sufficient message exchanges, the stable frontier advances — messages older than the frontier can be safely GC'd.
- Vector clocks cannot compute this frontier; matrix clocks can.

**Key insight**: `M[i][j] ≥ t` for all `i` means every process knows that process `j` had reached at least time `t` — messages from `j` with timestamp `≤ t` are stable and can be discarded from all in-transit logs.

---

## PartitionAndHeal

**Demonstrates**: Network partition, message loss, and recovery.

**Setup**: 4 processes: `[P1, P2]` and `[P3, P4]`, with the partition API.

**Script**:
1. P1 sends several messages to P3 (all dropped — partition active).
2. P3 sends messages within its partition side (P3↔P4, delivered normally).
3. Partition healed — P1 sends again to P3.
4. P3's clock reflects only the post-heal messages; the dropped messages are gone.

**What to observe**:

- `message_dropped` events during the partition.
- Post-heal messages flow normally.
- The space-time diagram shows the gap in P3's causal history.

**Key insight**: The lab deliberately models a partition with drop (not just delay). Recovery requires the application to handle lost messages — the transport does not retransmit.
