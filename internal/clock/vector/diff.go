// Package vector provides the VectorClock type and operations to diff two clocks
// and apply a diff onto a base clock for efficient state transfer.
package vector

// DiffVC represents only the entries that changed between two VCs.
type DiffVC struct {
	Entries map[string]uint64
}

// Diff computes the entries in `current` that have higher values than
// in `prev`. Missing keys in prev are treated as 0.
func Diff(prev, current VectorClock) DiffVC {
	entries := make(map[string]uint64)
	keys := make(map[string]struct{}, len(prev)+len(current))
	for k := range prev {
		keys[k] = struct{}{}
	}
	for k := range current {
		keys[k] = struct{}{}
	}
	for k := range keys {
		cv := current[k]
		if cv > prev[k] {
			entries[k] = cv
		}
	}
	return DiffVC{Entries: entries}
}

// Apply merges a DiffVC into the base clock (pointwise max).
func Apply(base VectorClock, diff DiffVC) VectorClock {
	result := Copy(base)
	for k, v := range diff.Entries {
		if v > result[k] {
			result[k] = v
		}
	}
	return result
}
