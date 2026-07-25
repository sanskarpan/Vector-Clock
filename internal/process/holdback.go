// Package process implements distributed system processes with causal delivery.
package process

import (
	"sync"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

// HeldMessage is a message waiting in the hold-back queue.
type HeldMessage struct {
	Msg    *Message
	SentVC vector.VectorClock
}

// HoldBackQueue implements the BSS causal delivery hold-back mechanism.
// Messages are held until the BSS delivery condition is satisfied.
type HoldBackQueue struct {
	mu      sync.Mutex
	pending []*HeldMessage
}

// Enqueue adds a message to the hold-back queue.
func (q *HoldBackQueue) Enqueue(m *Message, sentVC vector.VectorClock) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, &HeldMessage{Msg: m, SentVC: sentVC})
}

// TryDeliver repeatedly scans the queue and delivers any messages whose
// BSS condition is now satisfied. Returns all delivered messages in order.
// localVC is updated in place for each delivery.
//
// Critical invariant: for each delivered message, the receiver's own
// component is incremented (VC3). Skipping this tick would violate Fidge/
// Mattern strong clock-consistency and cause downstream causal deadlocks
// (the receiver would emit future sends with a stale self-counter, and
// other replicas would hold those sends indefinitely under BSS).
func (q *HoldBackQueue) TryDeliver(localVC vector.VectorClock, selfPID string) ([]*Message, vector.VectorClock) {
	var delivered []*Message
	changed := true
	for changed {
		changed = false
		q.mu.Lock()
		for i, held := range q.pending {
			if canDeliver(held.SentVC, held.Msg.From, localVC) {
				delivered = append(delivered, held.Msg)
				localVC = vector.Copy(localVC)
				// VC3: merge the non-sender components (already ≤ local by
				// condition 2), set the sender component to msgVC[sender]
				// (== localVC[sender]+1 by condition 1), and tick self.
				for k, v := range held.SentVC {
					if k == held.Msg.From {
						localVC[k] = v
					} else if v > localVC[k] {
						localVC[k] = v
					}
				}
				localVC[selfPID]++

				// Swap-with-last pop: O(1) remove and releases the tail
				// slot for GC. Unordered but BSS does not require order.
				q.pending[i] = q.pending[len(q.pending)-1]
				q.pending[len(q.pending)-1] = nil
				q.pending = q.pending[:len(q.pending)-1]
				changed = true
				break // restart scan: localVC changed
			}
		}
		q.mu.Unlock()
	}
	return delivered, localVC
}

// Len returns the number of messages currently held.
func (q *HoldBackQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// Snapshot returns a copy of held message info (for API/debug).
func (q *HoldBackQueue) Snapshot() []HeldMessage {
	q.mu.Lock()
	defer q.mu.Unlock()
	result := make([]HeldMessage, len(q.pending))
	for i, h := range q.pending {
		result[i] = *h
	}
	return result
}

// BlockedBy returns the PIDs that are blocking the next delivery for
// each held message. A message is blocked when:
//   - Condition 1 fails: msgVC[sender] > localVC[sender] + 1
//     (more than one message from sender outstanding)
//   - Condition 2 fails: msgVC[pid] > localVC[pid] for some pid ≠ sender
//     (causal predecessor from pid not yet delivered)
func (q *HoldBackQueue) BlockedBy(localVC vector.VectorClock) []string {
	q.mu.Lock()
	defer q.mu.Unlock()

	blockers := make(map[string]struct{})
	for _, held := range q.pending {
		sender := held.Msg.From
		msgVC := held.SentVC

		if msgVC[sender] > localVC[sender]+1 {
			blockers[sender] = struct{}{}
		}
		for pid, msgCount := range msgVC {
			if pid == sender {
				continue
			}
			if msgCount > localVC[pid] {
				blockers[pid] = struct{}{}
			}
		}
	}

	result := make([]string, 0, len(blockers))
	for pid := range blockers {
		result = append(result, pid)
	}
	return result
}

// canDeliver implements the BSS causal delivery condition:
//  1. msgVC[sender] == localVC[sender] + 1  (m is the NEXT expected from sender)
//  2. ∀k ≠ sender: msgVC[k] <= localVC[k]   (all causal predecessors delivered)
func canDeliver(msgVC vector.VectorClock, sender string, localVC vector.VectorClock) bool {
	// Condition 1: exactly next from sender
	if msgVC[sender] != localVC[sender]+1 {
		return false
	}
	// Condition 2: all other entries ≤ local
	for pid, msgCount := range msgVC {
		if pid == sender {
			continue
		}
		if msgCount > localVC[pid] {
			return false
		}
	}
	return true
}
