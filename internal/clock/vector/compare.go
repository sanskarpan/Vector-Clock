package vector

import "sort"

// ClockOrder represents the causal relationship between two vector clocks.
type ClockOrder int

const (
	HappenedBefore ClockOrder = iota // a → b: VC(a) < VC(b)
	HappenedAfter                    // b → a: VC(b) < VC(a)
	Concurrent                       // a ∥ b: neither dominates
	Identical                        // a = b: all components equal
)

// Compare implements the Charron-Bost isomorphism:
//
//	VC(a) < VC(b) ⟺ ∀k: a[k] ≤ b[k]  ∧  ∃k': a[k'] < b[k']
//
// Missing keys are treated as 0.
func Compare(a, b VectorClock) ClockOrder {
	// Collect union of keys
	keys := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}

	aLessB := false // ∃k: a[k] < b[k]
	bLessA := false // ∃k: b[k] < a[k]

	for k := range keys {
		av, bv := a[k], b[k] // missing keys default to zero-value (0)
		if av < bv {
			aLessB = true
		}
		if bv < av {
			bLessA = true
		}
		if aLessB && bLessA {
			return Concurrent // early exit: can't be HB either way
		}
	}

	switch {
	case !aLessB && !bLessA:
		return Identical
	case aLessB && !bLessA:
		return HappenedBefore
	case bLessA && !aLessB:
		return HappenedAfter
	default:
		return Concurrent
	}
}

// VCEvent is an event with a vector clock timestamp.
type VCEvent struct {
	ID    string
	Clock VectorClock
}

// ConcurrentSet partitions events into groups of concurrent events.
// Returns [][]VCEvent where each group contains events that are all
// concurrent with each other. Events within a group that are causally
// related end up in separate groups.
func ConcurrentSet(events []VCEvent) [][]VCEvent {
	if len(events) == 0 {
		return nil
	}

	// Sort events by their vector clock (partial order)
	sorted := make([]VCEvent, len(events))
	copy(sorted, events)
	sortEvents(sorted)

	var groups [][]VCEvent
	for _, ev := range sorted {
		placed := false
		for gi, group := range groups {
			concurrentWithAll := true
			for _, gev := range group {
				rel := Compare(ev.Clock, gev.Clock)
				if rel == HappenedBefore || rel == HappenedAfter || rel == Identical {
					concurrentWithAll = false
					break
				}
			}
			if concurrentWithAll {
				groups[gi] = append(group, ev)
				placed = true
				break
			}
		}
		if !placed {
			groups = append(groups, []VCEvent{ev})
		}
	}
	return groups
}

// sortEvents sorts events by the sum of their clock components,
// providing a deterministic total order for the grouping algorithm.
func sortEvents(events []VCEvent) {
	sort.Slice(events, func(i, j int) bool {
		sumI, sumJ := uint64(0), uint64(0)
		for _, v := range events[i].Clock {
			sumI += v
		}
		for _, v := range events[j].Clock {
			sumJ += v
		}
		if sumI != sumJ {
			return sumI < sumJ
		}
		return events[i].ID < events[j].ID
	})
}
