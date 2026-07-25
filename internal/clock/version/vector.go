// Package version implements plain version vectors (Parker et al. 1983).
// Version vectors tag data items, not events, and track per-replica writes.
// They are deliberately kept separate from event-tracking VectorClocks to
// highlight their distinct semantics (and their false-conflict problem).
package version

// SyncResult classifies the relationship between two version vectors.
type SyncResult int

const (
	Dominated SyncResult = iota // other dominates self: accept other
	Dominates                   // self dominates other: keep self
	Conflict                    // concurrent: siblings
	Equal                       // identical: no action needed
)

// VersionVector maps replicaID → write counter.
type VersionVector map[string]uint64

// New creates a zero-initialized version vector.
func New(replicaIDs []string) VersionVector {
	vv := make(VersionVector, len(replicaIDs))
	for _, r := range replicaIDs {
		vv[r] = 0
	}
	return vv
}

// Copy returns a deep copy.
func Copy(vv VersionVector) VersionVector {
	result := make(VersionVector, len(vv))
	for k, v := range vv {
		result[k] = v
	}
	return result
}

// Update increments the counter for replicaID (a write at that replica).
func (vv VersionVector) Update(replicaID string) VersionVector {
	result := Copy(vv)
	result[replicaID]++
	return result
}

// Sync compares this version vector with other and returns the relationship.
func (vv VersionVector) Sync(other VersionVector) SyncResult {
	keys := make(map[string]struct{}, len(vv)+len(other))
	for k := range vv {
		keys[k] = struct{}{}
	}
	for k := range other {
		keys[k] = struct{}{}
	}

	selfGreater := false
	otherGreater := false

	for k := range keys {
		sv, ov := vv[k], other[k]
		if sv > ov {
			selfGreater = true
		}
		if ov > sv {
			otherGreater = true
		}
		if selfGreater && otherGreater {
			return Conflict
		}
	}

	switch {
	case !selfGreater && !otherGreater:
		return Equal
	case selfGreater:
		return Dominates
	default:
		return Dominated
	}
}
