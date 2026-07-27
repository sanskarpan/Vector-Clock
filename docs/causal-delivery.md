# Causal Delivery

Causal delivery is a communication guarantee that ensures: if event A happened-before event B, and B produced a message `m2`, then any process that delivers `m2` must have already delivered the message `m1` associated with A. In other words, causes arrive before effects.

---

## The problem

Consider three processes P1, P2, P3:

```
P1: ─── send(m1)─────────────────────────────
         │            (direct, fast)
P2: ─── recv(m1) ─── send(m2) ───────────────
                            │   (direct, fast)
P3: ─────────────────────────── recv(m2)? ───
              (m1 via slow channel) ──── recv(m1)
```

If the P1→P3 channel is slow, P3 may receive `m2` (which depends on `m1`) before receiving `m1`. Without causal delivery, P3 processes `m2` without ever seeing the cause it depends on — a **causal anomaly**.

This can surface as: a reply arriving before the question, a "deleted" record reappearing, or a cascade of state changes applied in the wrong order.

---

## The BSS algorithm

The **Birman-Schiper-Stephenson (BSS)** algorithm enforces causal delivery using vector clock timestamps and a **hold-back queue** at each process.

### Delivery condition

Process Pⱼ may deliver a message `m` (with piggybacked vector clock `V_m`, sent by Pᵢ) when:

```
V_m[i]  = V_j[i] + 1   (exactly the next message from sender Pᵢ)
V_m[k]  ≤ V_j[k]       for all k ≠ i   (all other causal dependencies already delivered)
```

If either condition fails, `m` is placed in the **hold-back queue** and re-checked whenever a new message is delivered.

### Hold-back queue flush

After delivering a message (updating Pⱼ's vector clock), Pⱼ scans the hold-back queue for any message now satisfying the delivery condition. This flush is repeated until no more messages can be delivered (a fixed-point iteration).

---

## Implementation in the lab

### `internal/process/process.go` — `handleMessage`

```go
func (p *Process) handleMessage(msg *Message) {
    if msg.IsMarker {
        // ... snapshot path
        return
    }

    if p.deliveryMode == Causal {
        if !p.canDeliver(msg) {
            p.holdBack = append(p.holdBack, msg)
            p.emitEvent(events.EvtMessageHeld, HeldPayload{
                From:      msg.From,
                BlockedBy: p.blockedBy(msg),
            })
            return
        }
    }

    p.deliver(msg)
    p.flushHoldBack()
}
```

### `canDeliver`

```go
func (p *Process) canDeliver(msg *Message) bool {
    vc := msg.Clock  // vector clock piggybacked on message

    // Condition 1: exactly next from sender
    if vc[msg.From] != p.clock.Get(msg.From)+1 {
        return false
    }
    // Condition 2: all other entries ≤ local
    for pid, t := range vc {
        if pid == msg.From { continue }
        if t > p.clock.Get(pid) {
            return false
        }
    }
    return true
}
```

### `blockedBy`

Returns a list of (process, timestamp) pairs that `msg` is waiting for — shown in the `message_held` WebSocket event and the ClockInspector's hold-back queue panel.

---

## Configuration

```yaml
simulation:
  delivery_mode: causal    # immediate | causal
```

Or set per-scenario. The `CausalDelivery` scenario uses a 200 ms injected delay to make the hold-back queue visible in the UI.

---

## Observing causal delivery

### WebSocket events

| Event | Description |
|-------|-------------|
| `message_held` | Message placed in hold-back queue; includes `blocked_by` |
| `message_delivered` | Message flushed from hold-back and delivered |

### ClockInspector panel

Shows:
- Each process's current vector clock.
- Hold-back queue depth and contents.
- `BlockedBy` analysis: which missing messages are blocking delivery.

---

## BSS vs. other causal delivery protocols

| Protocol | Mechanism | Overhead |
|----------|-----------|---------|
| **BSS (this lab)** | Hold-back queue + VC comparison | O(N) timestamps per message |
| **ISIS Virtual Synchrony** | Group membership + view-change protocol | Heavy — full membership protocol |
| **Causal+ (Bolt-on)** | Application-layer shim, per-client dependency tracking | O(clients × keys) |
| **COPS** | Client session tokens + dependency checks at storage nodes | O(dependencies per transaction) |

BSS is the pedagogically clearest protocol because the hold-back condition is a direct implementation of the formal happened-before definition. It is not the most efficient for large N (O(N) timestamp overhead per message) but is exact and easy to reason about.

---

## Common questions

**Q: Why hold the message rather than reject and request a retransmit?**

Hold-back avoids the complexity of retransmit protocols and is appropriate when messages are reliably delivered (no drops). In the lab the transport is lossless by default; the fault-injection API adds explicit drops.

**Q: Can a message be held indefinitely?**

Yes, if a causally prior message is dropped and never delivered. The lab's `ImmediateDelivery` mode and fault injection allow you to demonstrate this deadlock scenario.

**Q: Does BSS work with matrix clocks?**

Yes — the delivery condition is the same; matrix clocks provide strictly more information (they can also GC stable messages). The lab uses vector clocks for BSS by default.

---

## Further reading

- Birman, K., Schiper, A. & Stephenson, P. (1987). [Lightweight causal and atomic group multicast.](https://doi.org/10.1145/128738.128742) *ACM TOCS* 9(3):272–314.
- Ahamad, M. et al. (1995). Causal memory: Definitions, implementation, and programming. *Distributed Computing* 9(1):37–49.
