# Conflict Detection with Version Vectors

This recipe demonstrates concurrent writes to the causal KV store and the three resolution strategies.

---

## Setup

```bash
# Spawn two processes
curl -s -X POST http://localhost:8080/api/v1/processes \
  -H "Content-Type: application/json" \
  -d '{"id":"P1","clock_type":"vector"}' | jq .id

curl -s -X POST http://localhost:8080/api/v1/processes \
  -H "Content-Type: application/json" \
  -d '{"id":"P2","clock_type":"vector"}' | jq .id
```

---

## Scenario A: Concurrent writes (conflict)

P1 and P2 each write to key `x` without seeing each other's write.

```bash
# P1 writes x = "hello" (P1's clock: {P1:1, P2:0})
curl -s -X PUT http://localhost:8080/api/v1/kv/x \
  -H "Content-Type: application/json" \
  -d '{"value":"hello","writer":"P1"}' | jq .

# P2 writes x = "world" independently (P2's clock: {P1:0, P2:1})
curl -s -X PUT http://localhost:8080/api/v1/kv/x \
  -H "Content-Type: application/json" \
  -d '{"value":"world","writer":"P2"}' | jq .
```

**Expected `kv_conflict` WebSocket event**:
```json
{
  "type": "kv_conflict",
  "data": {
    "key": "x",
    "versions": [
      {"value":"hello","version":{"P1":1,"P2":0},"writer":"P1"},
      {"value":"world","version":{"P1":0,"P2":1},"writer":"P2"}
    ]
  }
}
```

**Read the key**:

```bash
curl -s http://localhost:8080/api/v1/kv/x | jq .
```

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

Two versions, neither dominates the other (`{P1:1,P2:0}` vs `{P1:0,P2:1}`). This is a genuine conflict.

---

## Scenario B: Causal write (no conflict)

P2 reads P1's write (sees P1's version vector) before writing:

```bash
# Simulate P2 syncing by sending P1→P2 message first
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P2","payload":"sync"}'

# Now P2's clock includes P1's state: {P1:1, P2:1}
# P2 writes x = "world-v2" — causally after P1's write
curl -s -X PUT http://localhost:8080/api/v1/kv/x \
  -H "Content-Type: application/json" \
  -d '{"value":"world-v2","writer":"P2"}' | jq .
```

**Read the key**:

```bash
curl -s http://localhost:8080/api/v1/kv/x | jq .
```

```json
{
  "key": "x",
  "versions": [
    {"value":"world-v2","version":{"P1":1,"P2":2},"writer":"P2"}
  ],
  "conflict": false
}
```

Only one version — `world-v2` dominates `hello` (`{P1:1,P2:2}` > `{P1:1,P2:0}`). This is the `FalseConflict` scenario's non-conflict.

---

## Resolution strategies

### Strategy: `keep_all` (default)

Both concurrent versions are kept. Your application decides what to do.

```bash
# config.yaml: kv.conflict_strategy: keep_all
curl -s http://localhost:8080/api/v1/kv/x | jq '.versions | length'
# 2
```

### Strategy: `lww` (Last Writer Wins)

The write with the higher Lamport timestamp wins.

Restart with `kv.conflict_strategy: lww` in `config.yaml`, then repeat Scenario A.

```bash
curl -s http://localhost:8080/api/v1/kv/x | jq .
```

```json
{
  "key": "x",
  "versions": [{"value":"world","version":{"P1":0,"P2":1},"writer":"P2"}],
  "conflict": false
}
```

P2's write wins (higher Lamport timestamp, assuming P2 wrote last). P1's write is silently discarded.

!!! warning "LWW silently discards data"
    LWW should only be used when losing a concurrent write is acceptable — e.g. sensor readings, cached values. Never use LWW for financial data or any value where both concurrent writes matter.

### Strategy: `first_writer`

The write with the lower Lamport timestamp wins.

With `kv.conflict_strategy: first_writer`, P1's write wins (it was written first).

### Strategy: `merge`

Merges concurrent string values with `|`:

```json
{"value":"hello|world","conflict":false}
```

Use this with CRDTs or when the domain supports lossless merge (e.g. sets, counters).

---

## Observing in the UI

| Panel | What to look for |
|-------|-----------------|
| ConflictDash | Key `x` shows 2 versions side-by-side with version vectors |
| ConflictDash | Dominance arrows: neither points at the other (concurrent) |
| SpaceTimeDiagram | Two `kv_write` events on the timeline with no causal arrow between them |

Run the pre-built `ConcurrentWrites` scenario for a scripted version:

```bash
curl -s -X POST http://localhost:8080/api/v1/scenarios/ConcurrentWrites/run | jq .
```
