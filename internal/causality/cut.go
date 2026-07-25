// Package causality provides consistent cut detection and enumeration for
// distributed event traces using vector clocks.
package causality

import "github.com/DistributedClocks/vectorclock-system/internal/clock/vector"

// Cut assigns each process a frontier: the last included local event sequence.
// A cut is consistent iff for every message m:
//
//	if receive(m) is in the cut → send(m) is in the cut.
type Cut struct {
	Frontier map[string]int // processID → last included event local seq (0-based index)
}

// Message represents a sent/received message pair for cut consistency checking.
type Message struct {
	ID          string
	SendEventID string
	RecvEventID string
}

// IsConsistent returns true iff the cut is consistent:
// for every message, if receive(m) is in the cut then send(m) must be too.
func IsConsistent(cut Cut, events []*Event, messages []*Message) bool {
	for _, msg := range messages {
		sendEvent := findEvent(events, msg.SendEventID)
		recvEvent := findEvent(events, msg.RecvEventID)
		if sendEvent == nil || recvEvent == nil {
			continue
		}

		// Is receive(m) in the cut?
		recvCutline, ok := cut.Frontier[recvEvent.ProcessID]
		if !ok {
			continue
		}
		recvInCut := uint64ToIntBounded(recvEvent.LocalSeq) <= recvCutline

		// Is send(m) in the cut?
		sendCutline, ok2 := cut.Frontier[sendEvent.ProcessID]
		sendInCut := ok2 && uint64ToIntBounded(sendEvent.LocalSeq) <= sendCutline

		if recvInCut && !sendInCut {
			return false // violation: received but not sent
		}
	}
	return true
}

// FindConsistentCuts enumerates all consistent cuts for a set of processes and events.
// Each process can have a frontier from 0 (before first event) to len(its events).
// This is exponential but acceptable for small simulations (≤10 processes × 20 events).
func FindConsistentCuts(events []*Event, messages []*Message) []Cut {
	// Group events by process, sorted by LocalSeq
	byProcess := groupByProcess(events)
	pids := keys(byProcess)

	var result []Cut
	// Generate all possible frontier combinations via recursive enumeration
	frontier := make(map[string]int, len(pids))
	enumerateCuts(pids, 0, frontier, byProcess, events, messages, &result)
	return result
}

func enumerateCuts(
	pids []string, idx int,
	frontier map[string]int,
	byProcess map[string][]*Event,
	allEvents []*Event, messages []*Message,
	result *[]Cut,
) {
	if idx == len(pids) {
		cut := Cut{Frontier: copyFrontier(frontier)}
		if IsConsistent(cut, allEvents, messages) {
			*result = append(*result, cut)
		}
		return
	}
	pid := pids[idx]
	maxSeq := len(byProcess[pid])
	for i := 0; i <= maxSeq; i++ {
		if i == 0 {
			frontier[pid] = -1 // before first event
		} else {
			frontier[pid] = uint64ToIntBounded(byProcess[pid][i-1].LocalSeq)
		}
		enumerateCuts(pids, idx+1, frontier, byProcess, allEvents, messages, result)
	}
}

// CutBefore returns the consistent cut that includes all events with
// sequence numbers ≤ the components in vc. Returns the resulting Cut
// where Cut.Frontier[pid] = vc[pid] for each known pid.
func CutBefore(vc vector.VectorClock, events []*Event) Cut {
	frontier := make(map[string]int)
	for _, e := range events {
		seq := int(e.LocalSeq)
		limit := int(vc[e.ProcessID])
		if seq <= limit {
			if seq > frontier[e.ProcessID] {
				frontier[e.ProcessID] = seq
			}
		}
	}
	// Fill in missing PIDs with -1 (before first event)
	byProcess := groupByProcess(events)
	for pid := range byProcess {
		if _, ok := frontier[pid]; !ok {
			frontier[pid] = -1
		}
	}
	cut := Cut{Frontier: frontier}
	return cut
}

// FindMessages pairs send and receive events by shared MessageID and returns
// the resulting message list for cut-consistency checking.
func FindMessages(events []*Event) []*Message {
	var messages []*Message
	for _, e := range events {
		if e.Type == EventSend && e.MessageID != "" {
			// Find matching receive
			for _, recv := range events {
				if recv.Type == EventReceive && recv.MessageID == e.MessageID {
					messages = append(messages, &Message{
						ID:          e.MessageID,
						SendEventID: e.ID,
						RecvEventID: recv.ID,
					})
				}
			}
		}
	}
	return messages
}

// ── helpers ──────────────────────────────────────────────────────────────────

func findEvent(events []*Event, id string) *Event {
	for _, e := range events {
		if e.ID == id {
			return e
		}
	}
	return nil
}

func groupByProcess(events []*Event) map[string][]*Event {
	result := make(map[string][]*Event)
	for _, e := range events {
		result[e.ProcessID] = append(result[e.ProcessID], e)
	}
	return result
}

func keys(m map[string][]*Event) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}

func copyFrontier(f map[string]int) map[string]int {
	result := make(map[string]int, len(f))
	for k, v := range f {
		result[k] = v
	}
	return result
}

// uint64ToIntBounded safely converts a uint64 to int by capping at the
// platform's max int. Event sequence numbers in this system never approach
// this bound, but we cap explicitly so gosec's G115 stays quiet.
func uint64ToIntBounded(v uint64) int {
	const maxInt = int(^uint(0) >> 1)
	if v > uint64(maxInt) {
		return maxInt
	}
	return int(v)
}
