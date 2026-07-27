# Causal Delivery with Hold-Back Queues

This recipe demonstrates the BSS hold-back queue mechanism: a message that depends on a causal predecessor is held until the predecessor arrives.

---

## Setup

Switch to causal delivery mode before spawning processes. The easiest way is to use the `CausalDelivery` scenario directly, or set `delivery_mode: causal` in `config.yaml` and restart.

```bash
# Run the pre-built scenario (resets simulation with causal mode)
curl -s -X POST http://localhost:8080/api/v1/scenarios/CausalDelivery/run | jq .
```

Or follow the manual steps below to understand each part.

---

## Manual walkthrough

### Step 1: Spawn processes with causal delivery

The server must be configured with `delivery_mode: causal`. Verify:

```bash
curl -s http://localhost:8080/api/v1/config | jq .simulation.delivery_mode
# "causal"
```

Spawn three processes:

```bash
for pid in P1 P2 P3; do
  curl -s -X POST http://localhost:8080/api/v1/processes \
    -H "Content-Type: application/json" \
    -d "{\"id\":\"$pid\",\"clock_type\":\"vector\"}" | jq .id
done
```

### Step 2: Inject delay on P1→P3

The trick: P1 sends to both P2 (fast) and P3 (slow), then P2 sends to P3. We need P2's message to arrive at P3 before P1's direct message.

```bash
curl -s -X POST http://localhost:8080/api/v1/channels/P1/P3/delay \
  -H "Content-Type: application/json" \
  -d '{"delay_ms": 400}'
```

### Step 3: Send m1 from P1 to P2 (fast path)

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P2","payload":"m1"}'
```

P2 receives `m1` immediately.

### Step 4: P2 sends m2 (depends on m1)

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P2","to":"P3","payload":"m2-depends-on-m1"}'
```

P2's clock now contains P1's information: `{P1:1, P2:2, P3:0}`. This is piggybacked on `m2`.

### Step 5: P1 also sends directly to P3 (slow path)

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P3","payload":"m1-direct"}'
```

This message (the causal predecessor of `m2`) travels on the 400 ms delayed channel.

### Step 6: Observe the hold-back

In the WebSocket stream:

```json
{"type":"message_held","data":{
  "pid":"P3",
  "from":"P2",
  "payload":"m2-depends-on-m1",
  "blocked_by":[{"pid":"P1","required":1,"have":0}]
}}
```

**Interpreting `blocked_by`**: P3 needs P1's clock to be at least 1, but P3 only has 0. It's waiting for `m1-direct` to arrive.

Check P3's hold-back queue:

```bash
curl -s http://localhost:8080/api/v1/processes/P3 | jq .hold_back_queue
```

```json
[{"from":"P2","payload":"m2-depends-on-m1","clock":{"P1":1,"P2":2,"P3":0}}]
```

### Step 7: m1-direct arrives — hold-back flushes

After 400 ms, `m1-direct` arrives. In the WebSocket stream:

```json
{"type":"message_delivered","data":{"from":"P1","to":"P3","payload":"m1-direct"}}
{"type":"message_delivered","data":{"from":"P2","to":"P3","payload":"m2-depends-on-m1"}}
```

Both events fire in rapid succession — `m1-direct` is delivered, then the hold-back queue is flushed and `m2` is immediately delivered.

P3's final clock:
```json
{"P1":1,"P2":2,"P3":2}
```

Causal order preserved: `m1` was delivered before `m2`, even though `m2` arrived first.

---

## Observing in the UI

| Panel | What to look for |
|-------|-----------------|
| SpaceTimeDiagram | `m2` arc dashed/pending until `m1-direct` arrives; then both render |
| ClockInspector | P3 hold-back queue depth = 1 during the delay period; drops to 0 after flush |
| CausalDelivery monitor | Bar chart shows P3 holding 1 message; timeline shows held → delivered pair |

---

## What would happen without causal delivery?

Set `delivery_mode: immediate` and repeat. `m2` is delivered to P3 as soon as it arrives — before `m1-direct`. P3 processes the effect (`m2`) without ever seeing the cause (`m1-direct`). This is the `CausalViolation` scenario.
