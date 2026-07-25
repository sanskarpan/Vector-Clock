// Package dvv implements Dotted Version Vectors (Preguiça et al. 2012).
// DVVs prevent false conflicts that plain version vectors produce by tracking
// the specific write event (dot) separate from the causal history (vector).
package dvv

// Dot identifies a single write event: (replicaID, counter).
type Dot struct {
	Replica string
	Counter uint64
}

// DVV represents a single value's causal metadata.
// Vector: the causally dominated history (what this write has seen before it).
// Dot: the single write event this DVV represents.
type DVV struct {
	Vector map[string]uint64 // causal history base
	Dot    *Dot              // this specific write's identity (nil for initial writes from context)
}

// DVVEntry pairs a DVV with a value.
type DVVEntry struct {
	DVV   DVV
	Value []byte
}

// DVVSet holds multiple DVV entries for a key (siblings under concurrent writes).
type DVVSet struct {
	Siblings []DVVEntry
}

// NewDVV creates a fresh DVV for a first write at a replica.
func NewDVV(serverID string, serverClock uint64) DVV {
	return DVV{
		Vector: make(map[string]uint64),
		Dot:    &Dot{Replica: serverID, Counter: serverClock},
	}
}

// UpdateDVV creates a DVV for a new write that causally follows old (the client's context).
func UpdateDVV(old DVV, serverID string, serverClock uint64) DVV {
	return DVV{
		Vector: old.ToVV(),
		Dot:    &Dot{Replica: serverID, Counter: serverClock},
	}
}

// ToVV converts a DVV to a plain version vector by folding the dot into the vector.
func (d DVV) ToVV() map[string]uint64 {
	vv := make(map[string]uint64, len(d.Vector)+1)
	for k, v := range d.Vector {
		vv[k] = v
	}
	if d.Dot != nil && vv[d.Dot.Replica] < d.Dot.Counter {
		vv[d.Dot.Replica] = d.Dot.Counter
	}
	return vv
}

// Dominates returns true if a has seen everything b has (a causally supersedes b).
// a dominates b iff a.ToVV()[k] >= b.ToVV()[k] for all k.
func Dominates(a, b DVV) bool {
	bVV := b.ToVV()
	aVV := a.ToVV()
	for k, bv := range bVV {
		if aVV[k] < bv {
			return false
		}
	}
	return true
}

// Sync takes a DVVSet and a new DVV, discards dominated entries, and adds the new one.
// Returns the resulting DVVSet with only concurrent siblings plus the new entry.
func Sync(set DVVSet, incoming DVVEntry) DVVSet {
	var survivors []DVVEntry
	for _, existing := range set.Siblings {
		// Keep existing if it's NOT dominated by incoming
		if !Dominates(incoming.DVV, existing.DVV) {
			survivors = append(survivors, existing)
		}
	}
	// Add incoming if it's not dominated by any survivor
	dominated := false
	for _, s := range survivors {
		if Dominates(s.DVV, incoming.DVV) {
			dominated = true
			break
		}
	}
	if !dominated {
		survivors = append(survivors, incoming)
	}
	return DVVSet{Siblings: survivors}
}
