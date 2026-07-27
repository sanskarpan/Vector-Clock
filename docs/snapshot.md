# Global Snapshots — Chandy-Lamport 1985

The Chandy-Lamport algorithm captures a consistent global state of a distributed system while it is running, without stopping any process. The snapshot represents a **consistent cut** — a logical point in time where the system's state is internally coherent.

---

## Why snapshots?

Distributed systems need to checkpoint state for fault recovery, detect stable properties (deadlock, termination), and audit system behaviour. The challenge: collecting every process's state simultaneously is impossible without a global clock. By the time you've collected P1's state, P2 may have already acted on a message from P1.

**Key requirement**: The snapshot must represent a *consistent cut* — no message's receive is recorded without its send also being recorded.

---

## FIFO channels

The algorithm requires **FIFO channels**: messages on each directed channel are delivered in the order they were sent. This is the `SimTransport` invariant — each channel is a buffered Go channel, which is inherently FIFO. The `snapshot.fifo_channels: true` config enforces this.

---

## The algorithm

### Setup

- N processes: P1, P2, …, Pn.
- Directed FIFO channels between every pair.
- Any process can initiate a snapshot at any time.

### Marker propagation

**Step 1** — Initiator (any process Pᵢ can initiate):
1. Record own local state (atomic with `p.mu` held).
2. Send a **marker** message on every outgoing channel.
3. Begin recording incoming messages on every incoming channel.

**Step 2** — Any process Pⱼ that receives a marker on channel Cᵢⱼ for the first time:
1. Record own local state (if not already done — race-free capture while holding `p.mu`).
2. Stop recording incoming messages on Cᵢⱼ (the channel state from Pᵢ is now complete).
3. Send a marker on all outgoing channels.
4. Continue recording incoming messages on all other channels.

**Step 3** — Any process Pⱼ that receives a marker on channel Cₖⱼ (not the first marker):
- Stop recording on Cₖⱼ. The channel state is: all messages received on Cₖⱼ after Pⱼ recorded its state and before this marker.

**Termination**: When every process has received markers on all incoming channels, the snapshot is complete. The `SnapshotCoordinator.checkFinalized` method detects this.

---

## Consistent cut property

The resulting snapshot is a consistent cut because:

1. A process records its state *before* sending any markers.
2. Markers travel on FIFO channels — so any message sent before a marker arrives before the marker.
3. A channel's in-transit messages are exactly those sent before the initiator's marker but received after the receiving process recorded its own state.

**Formal proof sketch**: Suppose event `e` (a send at Pᵢ) is in the cut, but `e'` (the corresponding receive at Pⱼ) is not. Then `e'` occurred after Pⱼ recorded its state. But Pⱼ records incoming messages from `e` to the first marker on that channel — so `e'` is captured in the channel state. No inconsistency.

---

## Concurrent initiators

Multiple processes can initiate snapshots simultaneously. Each snapshot has a unique ID (`snap-{timestamp}`). The `SnapshotCoordinator` tracks per-process, per-snapshot state independently. A process in one snapshot receiving markers from a second snapshot starts participating in both — they are orthogonal and do not interfere.

---

## Implementation details

### The ABBA deadlock (and fix)

The original implementation had a deadlock between `InitiateSnapshot` and `handleMessage`:

```
Thread A: handleMessage
  p.mu.Lock()           ← acquires process lock
  calls OnMarkerReceived
    ps.mu.Lock()        ← tries to acquire snapshot state lock

Thread B: InitiateSnapshot
  ps.mu.Lock()          ← acquires snapshot state lock
  calls onCaptureState
    p.Snapshot()
      p.mu.RLock()      ← tries to acquire process lock  ← DEADLOCK
```

**Fix** (Issues #91–#94):

1. `InitiateSnapshot` calls `onCaptureState(initiatorID)` *before* acquiring `ps.mu`.
2. `OnMarkerReceived` accepts a `capturedState interface{}` pre-captured by the caller.
3. `handleMessage` captures state via `snapshotLocked()` while holding `p.mu`, then passes it to `OnMarkerReceived`.

This breaks the ABBA cycle: `handleMessage` never acquires `ps.mu`, and `InitiateSnapshot` never calls `onCaptureState` while holding `ps.mu`.

### State capture — `snapshotLocked()`

```go
// Called by handleMessage while holding p.mu.
func (p *Process) snapshotLocked() ProcessState {
    // Reads p.clock, p.inbox, p.kvStore without acquiring p.mu
    return ProcessState{
        ID:          p.id,
        Clock:       p.clock.Copy(),
        VectorClock: p.clock.Map(),
        HoldBack:    len(p.holdBack),
    }
}
```

The `Snapshot()` public method acquires `p.mu` then calls `snapshotLocked()` — so external callers get the same correctness without needing to know about the lock.

### `DeregisterProcess`

When `KillProcess` is called, `simulation.go` calls `s.snapshots.DeregisterProcess(id)` before `p.Stop()`. This removes the process from all in-progress snapshots, preventing marker routing to a dead process.

---

## Reading snapshot results

The `snapshot_finalized` WebSocket event and `GET /api/v1/snapshots/:id` return:

```json
{
  "id": "snap-1722074919",
  "initiated_by": "P1",
  "process_states": {
    "P1": {"clock": {"P1":3,"P2":1,"P3":0}},
    "P2": {"clock": {"P1":2,"P2":4,"P3":2}},
    "P3": {"clock": {"P1":1,"P2":3,"P3":5}}
  },
  "channel_states": {
    "P1→P2": [{"payload":"m5","clock":{"P1":3,"P2":1,"P3":0}}],
    "P2→P1": [],
    "P1→P3": [],
    "P3→P1": [{"payload":"m7","clock":{"P1":1,"P2":3,"P3":5}}],
    "P2→P3": [],
    "P3→P2": []
  }
}
```

- `process_states`: each process's local state at the moment it recorded itself.
- `channel_states`: messages in-transit at the cut — sent before the sender's cut but received after the receiver's cut.

The SnapshotViewer component in the frontend renders these as an annotated space-time diagram with the consistent cut overlaid.

---

## Common pitfalls

| Pitfall | Consequence | Fix |
|---------|-------------|-----|
| Non-FIFO channels | Marker may arrive before a message it should precede | Use FIFO channels only (default) |
| Marker not forwarded to all outgoing channels | Some processes never learn about the snapshot | Propagate marker on *all* outgoing channels, including back-to-sender |
| State captured after ps.mu acquired | ABBA deadlock | Capture before lock; pass as parameter |
| Not deregistering killed processes | Markers sent to dead process, snapshot never finalises | Call `DeregisterProcess` on `KillProcess` |

---

## Further reading

- Chandy, K.M. & Lamport, L. (1985). [Distributed snapshots: Determining global states of distributed systems.](https://doi.org/10.1145/214451.214456) *ACM TOCS* 3(1):63–75.
- Mattern, F. (1989). Global quiescence detection based on credit distribution and recovery. *Information Processing Letters* 30(4):195–200.
