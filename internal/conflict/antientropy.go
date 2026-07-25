// Package conflict implements anti-entropy synchronization and conflict detection
// for MVCC entries across distributed replicas.
package conflict

import (
	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

// SyncResultItem tracks a single key's sync outcome.
type SyncResultItem struct {
	Key      string
	Merged   bool
	Conflict bool
}

// Sync performs anti-entropy: absorbs entries from remote into the local
// conflict detector. Returns items that were updated or conflicted.
func (d *ConflictDetector) Sync(remote map[string]*MVCCEntry) []SyncResultItem {
	d.mu.Lock()
	defer d.mu.Unlock()

	var result []SyncResultItem

	for key, remoteEntry := range remote {
		if remoteEntry == nil || len(remoteEntry.Versions) == 0 {
			continue
		}

		localEntry, exists := d.store[key]
		if !exists {
			// Absorb remote entry entirely
			entry := &MVCCEntry{
				Key:      key,
				Versions: copyVersions(remoteEntry.Versions),
			}
			d.store[key] = entry
			result = append(result, SyncResultItem{Key: key, Merged: true})
			continue
		}

		changed := false
		hasConflict := false

		for _, rv := range remoteEntry.Versions {
			absorbed := false
			for _, lv := range localEntry.Versions {
				switch vector.Compare(rv.Clock, lv.Clock) {
				case vector.HappenedBefore, vector.Identical:
					// remote is dominated by local — skip
					absorbed = true
				case vector.HappenedAfter:
					// remote dominates this local version — will replace
				case vector.Concurrent:
					// keep both as siblings
				}
				if absorbed {
					break
				}
			}
			if absorbed {
				continue
			}

			// Check if remote dominates ALL local versions
			dominatesAll := true
			allConcurrent := true
			for _, lv := range localEntry.Versions {
				order := vector.Compare(rv.Clock, lv.Clock)
				if order == vector.HappenedBefore || order == vector.Identical {
					dominatesAll = false
					allConcurrent = false
					break
				}
				if order == vector.HappenedAfter {
					allConcurrent = false
				}
			}
			if dominatesAll {
				localEntry.Versions = []*ValueVersion{rv}
				changed = true
				continue
			}

			// Concurrent with at least one local version — add as sibling
			localEntry.Versions = append(localEntry.Versions, rv)
			changed = true
			if !allConcurrent {
				hasConflict = true
			}
		}

		if changed {
			result = append(result, SyncResultItem{
				Key:      key,
				Merged:   true,
				Conflict: hasConflict,
			})
		}
	}

	return result
}

func copyVersions(src []*ValueVersion) []*ValueVersion {
	dst := make([]*ValueVersion, len(src))
	copy(dst, src)
	return dst
}

func init() {
	// Ensure default resolvers are registered
}
