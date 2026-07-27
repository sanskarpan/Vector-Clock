# Fault Injection — Delay, Drop, Partition

The lab's transport layer supports three fault injection modes: fixed delay, probabilistic drop, and full network partition. This recipe demonstrates each.

---

## Setup

```bash
# Spawn four processes
for pid in P1 P2 P3 P4; do
  curl -s -X POST http://localhost:8080/api/v1/processes \
    -H "Content-Type: application/json" \
    -d "{\"id\":\"$pid\",\"clock_type\":\"vector\"}" | jq -r .id
done
```

---

## 1. Fixed delay

Inject a 500 ms delay on the P1→P3 channel:

```bash
curl -s -X POST http://localhost:8080/api/v1/channels/P1/P3/delay \
  -H "Content-Type: application/json" \
  -d '{"delay_ms": 500}'
```

Send a message:

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P3","payload":"slow"}'
```

Observe: `message_sent` fires immediately; `message_delivered` fires ~500 ms later. In the SpaceTimeDiagram, the causal arc P1→P3 is visibly longer (stretched downward in time) than other arcs.

**Use this to**: demonstrate that vector clocks work correctly even across slow channels, and to create in-transit messages for snapshot scenarios.

---

## 2. Probabilistic message drop

Set a 40% drop probability on the P2→P4 channel:

```bash
curl -s -X POST http://localhost:8080/api/v1/channels/P2/P4/drop \
  -H "Content-Type: application/json" \
  -d '{"probability": 0.4}'
```

Send 10 messages:

```bash
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/api/v1/messages \
    -H "Content-Type: application/json" \
    -d "{\"from\":\"P2\",\"to\":\"P4\",\"payload\":\"msg-$i\"}" &
done
wait
```

Observe in the WebSocket stream: roughly 4 `message_dropped` events and 6 `message_delivered` events. The exact count varies (probabilistic). The SpaceTimeDiagram shows dropped messages as dashed arcs that end mid-air.

**Use this to**: demonstrate that message loss breaks causal delivery guarantees without explicit detection, and to test application-level retry logic.

---

## 3. Network partition

Partition the network: [P1, P2] and [P3, P4] cannot communicate.

```bash
curl -s -X POST http://localhost:8080/api/v1/partition \
  -H "Content-Type: application/json" \
  -d '{"side_a":["P1","P2"],"side_b":["P3","P4"]}'
```

Try sending across the partition:

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P3","payload":"lost"}'
# Observe: message_dropped
```

Send within a partition side (succeeds):

```bash
curl -s -X POST http://localhost:8080/api/v1/messages \
  -H "Content-Type: application/json" \
  -d '{"from":"P1","to":"P2","payload":"ok"}'
# Observe: message_delivered
```

### Heal the partition

```bash
curl -s -X DELETE http://localhost:8080/api/v1/partition
```

After healing, cross-partition messages flow again. Note that messages dropped during the partition are **not retransmitted** — they are gone. This is intentional: the lab models a lossy network, not a reliable one.

---

## 4. Combining delay + drop

Stack fault injection on the same channel:

```bash
# 200ms delay AND 20% drop on P3→P1
curl -s -X POST http://localhost:8080/api/v1/channels/P3/P1/delay \
  -H "Content-Type: application/json" \
  -d '{"delay_ms": 200}'

curl -s -X POST http://localhost:8080/api/v1/channels/P3/P1/drop \
  -H "Content-Type: application/json" \
  -d '{"probability": 0.2}'
```

Messages on P3→P1 now have a 20% chance of being dropped outright; the surviving 80% experience a 200 ms delay.

---

## 5. Resetting fault injection

Reset a specific channel:

```bash
curl -s -X POST http://localhost:8080/api/v1/channels/P1/P3/reset
```

Reset all channels (re-run the scenario or restart):

```bash
curl -s -X POST http://localhost:8080/api/v1/scenarios/BasicLamport/run
# The scenario resets simulation state including all fault injection
```

---

## Pre-built scenario: PartitionAndHeal

The `PartitionAndHeal` scenario automates a partition + recovery sequence:

```bash
curl -s -X POST http://localhost:8080/api/v1/scenarios/PartitionAndHeal/run | jq .
```

Observe in the SpaceTimeDiagram: dropped messages during the partition, then normal flow after healing, and the clear gap in P3/P4's causal history during the partitioned period.
