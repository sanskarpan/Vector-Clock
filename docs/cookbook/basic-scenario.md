# Run Your First Scenario

This recipe walks you through spawning processes, exchanging messages, and running the `BasicLamport` pre-built scenario — the simplest demonstration of logical clocks.

---

## Step 1: Subscribe to the WebSocket stream

In a terminal, open a WebSocket subscription so you can watch events as they fire:

```bash
# Using websocat (brew install websocat)
websocat ws://localhost:8080/ws | jq .

# Or using wscat (npm i -g wscat)
wscat -c ws://localhost:8080/ws
```

Leave this running in the background. Every clock tick, send, and receive will appear here.

---

## Step 2: Spawn three processes

```bash
curl -s -X POST http://localhost:8080/api/v1/processes \
  -H "Content-Type: application/json" \
  -d '{"id":"P1","clock_type":"vector"}' | jq .

curl -s -X POST http://localhost:8080/api/v1/processes \
  -H "Content-Type: application/json" \
  -d '{"id":"P2","clock_type":"vector"}' | jq .

curl -s -X POST http://localhost:8080/api/v1/processes \
  -H "Content-Type: application/json" \
  -d '{"id":"P3","clock_type":"vector"}' | jq .
```

**Expected WebSocket events**:
```json
{"type":"process_spawned","data":{"pid":"P1","clock_type":"vector"}}
{"type":"process_spawned","data":{"pid":"P2","clock_type":"vector"}}
{"type":"process_spawned","data":{"pid":"P3","clock_type":"vector"}}
```

In the UI: three process lanes appear in the SpaceTimeDiagram.

---

## Step 3: Verify initial clock state

```bash
curl -s http://localhost:8080/api/v1/processes | jq '.[].clock'
```

Expected (all zeros):
```json
{"P1":0,"P2":0,"P3":0}
{"P1":0,"P2":0,"P3":0}
{"P1":0,"P2":0,"P3":0}
```

---

## Step 4: Send a message P1 → P2

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P2","payload":"hello"}' | jq .
```

**Expected events**:
```json
{"type":"message_sent",      "data":{"from":"P1","to":"P2","clock":{"P1":1,"P2":0,"P3":0}}}
{"type":"message_delivered", "data":{"from":"P1","to":"P2","clock":{"P1":1,"P2":1,"P3":0}}}
```

**What happened**:
- P1 incremented its clock to `{P1:1, ...}` before sending.
- P2 merged P1's clock and incremented its own: `{P1:1, P2:1}`.
- The arc P1→P2 appears in the SpaceTimeDiagram.

---

## Step 5: Send P2 → P3 (causal chain)

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P2","to":"P3","payload":"forwarded"}' | jq .
```

P3's clock after delivery:
```json
{"P1":1,"P2":2,"P3":1}
```

P3 now knows about P1's original message — via P2's piggybacked vector clock. The transitive causal chain P1→P2→P3 is captured in P3's vector timestamp.

---

## Step 6: Send a concurrent message P1 → P3

Without waiting, send directly from P1 to P3:

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P3","payload":"concurrent"}' | jq .
```

P1's clock at send time: `{P1:2, P2:0, P3:0}` (P1 hasn't seen P2's events).

After P3 delivers this, check P3's clock:

```bash
curl -s http://localhost:8080/api/v1/processes/P3 | jq .clock
```

```json
{"P1":2,"P2":2,"P3":2}
```

P3 merged both delivery clocks. In the SpaceTimeDiagram, the P1→P3 arc is **concurrent** with the P2→P3 arc — they cross without a causal arrow between them.

---

## Step 7: Run the full pre-built scenario

Reset and run `BasicLamport` in one call:

```bash
curl -s -X POST http://localhost:8080/api/v1/scenarios/BasicLamport/run | jq .
```

```json
{"scenario":"BasicLamport","duration_ms":245}
```

The scenario replays the above steps with scripted timing so the space-time diagram fills in cleanly. Use the ScenarioPanel in the UI to run it with one click.

---

## What to observe

| UI panel | What to look for |
|----------|-----------------|
| SpaceTimeDiagram | Causal arcs P1→P2, P2→P3; concurrent P1→P3 |
| ClockInspector | Vector clock values updating per event |
| ScenarioPanel | `BasicLamport` run card with duration |

Try changing `clock_type` to `lamport` when spawning processes and repeat — you'll see scalar integers instead of vectors, and the inspector shows that you cannot determine concurrency from the Lamport values alone.
