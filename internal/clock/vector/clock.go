// Package vector implements Fidge/Mattern vector clocks with
// the Charron-Bost causality isomorphism.
package vector

// VectorClock is a sparse map-based vector clock. Missing keys are treated as 0.
type VectorClock map[string]uint64

// New creates a zero-initialized vector clock for the given process IDs.
func New(pids []string) VectorClock {
	vc := make(VectorClock, len(pids))
	for _, p := range pids {
		vc[p] = 0
	}
	return vc
}

// Copy returns a deep copy of the vector clock.
func Copy(vc VectorClock) VectorClock {
	result := make(VectorClock, len(vc))
	for k, v := range vc {
		result[k] = v
	}
	return result
}

// Tick increments the clock for pid (VC1: internal event).
// Returns a new clock; the input is not mutated.
func Tick(vc VectorClock, pid string) VectorClock {
	result := Copy(vc)
	result[pid]++
	return result
}

// Send increments own component and returns both the updated local clock
// and a copy to attach to the outgoing message (VC2).
func Send(vc VectorClock, pid string) (local VectorClock, msgStamp VectorClock) {
	local = Tick(vc, pid)
	msgStamp = Copy(local)
	return
}

// Receive updates the clock on message receipt (VC3):
// increment own component, take pointwise max with msgVC.
func Receive(local VectorClock, msgVC VectorClock, pid string) VectorClock {
	merged := MergePassive(local, msgVC)
	return Tick(merged, pid)
}

// MergePassive takes the pointwise maximum without ticking any component.
// Used for anti-entropy, queries, and as a building block for Receive.
func MergePassive(a, b VectorClock) VectorClock {
	result := Copy(a)
	for k, v := range b {
		if result[k] < v {
			result[k] = v
		}
	}
	return result
}

// Equal returns true if all components are equal (both ways).
func Equal(a, b VectorClock) bool {
	return Compare(a, b) == Identical
}

// LessThan returns true iff a < b: all a[k] ≤ b[k] and ∃k: a[k] < b[k].
func LessThan(a, b VectorClock) bool {
	return Compare(a, b) == HappenedBefore
}
