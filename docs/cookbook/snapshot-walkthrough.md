# Chandy-Lamport Snapshot Walkthrough

This recipe walks through initiating a global snapshot while messages are in-transit, and reading the resulting consistent cut.

---

## Setup

Start with three processes and a message stream:

```bash
# Spawn processes
for pid in P1 P2 P3; do
  curl -s -X POST http://localhost:8080/api/v1/processes \
    -H "Content-Type: application/json" \
    -d "{\"id\":\"$pid\",\"clock_type\":\"vector\"}" | jq .id
done

# Add a small transit delay to P1→P2 so a message is in-flight during snapshot
curl -s -X POST http://localhost:8080/api/v1/channels/P1/P2/delay \
  -H "Content-Type: application/json" \
  -d '{"delay_ms":300}'
```

---

## Step 1: Start a message in transit

Send a message that will be in-flight while we initiate the snapshot:

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P2","payload":"in-flight"}' | jq .
```

The `message_sent` event fires immediately; `message_delivered` will fire 300 ms later.

---

## Step 2: Initiate a snapshot (before P1→P2 delivers)

Within 300 ms (before the delay expires), initiate a snapshot from P1:

```bash
curl -s -X POST http://localhost:8080/api/v1/snapshots \
  -H "Content-Type: application/json" \
  -d '{"initiator":"P1"}' | jq .
```

```json
{"snapshot_id":"snap-1722074919"}
```

---

## Step 3: Watch the marker propagation

In the WebSocket stream you'll see:

```json
{"type":"snapshot_marker_sent","data":{"pid":"P1","snapshot_id":"snap-1722074919","to":"P2"}}
{"type":"snapshot_marker_sent","data":{"pid":"P1","snapshot_id":"snap-1722074919","to":"P3"}}
{"type":"snapshot_marker_received","data":{"pid":"P2","snapshot_id":"snap-1722074919","from":"P1"}}
{"type":"snapshot_marker_sent","data":{"pid":"P2","snapshot_id":"snap-1722074919","to":"P3"}}
{"type":"snapshot_marker_received","data":{"pid":"P3","snapshot_id":"snap-1722074919","from":"P1"}}
{"type":"snapshot_marker_received","data":{"pid":"P3","snapshot_id":"snap-1722074919","from":"P2"}}
{"type":"snapshot_finalized","data":{"snapshot_id":"snap-1722074919",...}}
```

**What happened**:
1. P1 recorded its own state, sent markers to P2 and P3.
2. P2 received P1's marker, recorded its own state, sent a marker to P3.
3. P3 received both markers (from P1 and from P2) and recorded its state.
4. The snapshot coordinator detected all markers received — snapshot finalised.

---

## Step 4: Read the snapshot

```bash
curl -s http://localhost:8080/api/v1/snapshots/snap-1722074919 | jq .
```

```json
{
  "id": "snap-1722074919",
  "initiated_by": "P1",
  "finalized_at": "2026-07-27T11:00:05Z",
  "process_states": {
    "P1": {"clock": {"P1":2,"P2":0,"P3":0}},
    "P2": {"clock": {"P1":0,"P2":0,"P3":0}},
    "P3": {"clock": {"P1":0,"P2":0,"P3":0}}
  },
  "channel_states": {
    "P1→P2": [{"payload":"in-flight","clock":{"P1":1,"P2":0,"P3":0}}],
    "P2→P1": [],
    "P1→P3": [],
    "P3→P1": [],
    "P2→P3": [],
    "P3→P2": []
  }
}
```

**Interpreting the result**:

- `P1.clock = {P1:2, ...}` — P1 had sent two events (the message + the snapshot initiation) before recording.
- `P2.clock = {P1:0, ...}` — P2 hadn't yet received P1's message when it recorded (message was in-flight).
- `P1→P2 channel state = ["in-flight"]` — the message was in-transit during the cut.

The cut is **consistent**: the `in-flight` message's *send* is in P1's past (P1 recorded after sending), and its *receive* is in P2's future (P2 recorded before receiving). No receive is recorded without a corresponding send.

---

## Step 5: Verify consistency client-side

The SnapshotViewer component validates consistency automatically. For manual verification:

For every message in a channel state `Cᵢⱼ`:
1. The message's vector clock `V_m` must satisfy `V_m ≤ P_i.clock` (send is in Pᵢ's past).
2. `V_m[i] > P_j.clock[i]` (receive would have been in Pⱼ's future).

```python
# Quick Python validation
import json, requests

snap = requests.get('http://localhost:8080/api/v1/snapshots/snap-1722074919').json()

for channel, messages in snap['channel_states'].items():
    sender, receiver = channel.split('→')
    ps = snap['process_states']
    for msg in messages:
        vc = msg['clock']
        # Send in sender's past
        for pid, t in vc.items():
            assert t <= ps[sender]['clock'].get(pid, 0), f"Inconsistent: {channel}"
        # Receive in receiver's future
        assert vc[sender] > ps[receiver]['clock'].get(sender, 0), f"Inconsistent: {channel}"

print("Snapshot is consistent ✓")
```

---

## Run as a pre-built scenario

The `Snapshot3P` scenario automates this walkthrough with precise timing:

```bash
curl -s -X POST http://localhost:8080/api/v1/scenarios/Snapshot3P/run | jq .
```

Open the SnapshotViewer panel in the frontend to see the consistent cut rendered as a dashed line through all three process lanes.
