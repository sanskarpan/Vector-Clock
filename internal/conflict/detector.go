// Package conflict implements multi-version conflict detection and resolution
// using vector clock causality (Parker et al. 1983).
package conflict

import (
	"fmt"
	"sync"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
)

// Limits on conflict store growth. The store is a teaching tool so the
// bounds are conservative; production deployments with high write rates
// would tune these via the config layer.
const (
	// MaxKeys is the maximum number of distinct keys retained. Older
	// keys are not auto-evicted today; the bound is a hard cap that
	// returns an error on the next write past the limit.
	MaxKeys = 10_000
	// MaxSiblingsPerKey caps concurrent versions retained per key.
	// Past this, the oldest sibling is dropped.
	MaxSiblingsPerKey = 64
)

// ValueVersion is a single version of a key's value.
type ValueVersion struct {
	Value   []byte
	Clock   vector.VectorClock
	Author  string
	Written time.Time
}

// MVCCEntry stores all concurrent versions (siblings) for a key.
type MVCCEntry struct {
	Key      string
	Versions []*ValueVersion
}

// ConflictDetector is a causally-consistent multi-version store.
type ConflictDetector struct {
	mu    sync.Mutex
	store map[string]*MVCCEntry
	bus   *events.EventBus // optional; may be nil
}

// New creates a ConflictDetector.
func New(bus *events.EventBus) *ConflictDetector {
	return &ConflictDetector{
		store: make(map[string]*MVCCEntry),
		bus:   bus,
	}
}

// Write adds a new version for key. Returns (new version, isConflict).
// ctxVC is the vector clock the writer observed (from a prior read).
// authorID is the replica/process performing the write.
//
// Returns an error when the keyspace is full (see MaxKeys) or when input is
// invalid. Invalid input is the caller's responsibility to filter; this
// function does not sanitise values.
func (d *ConflictDetector) Write(key string, value []byte, ctxVC vector.VectorClock, authorID string) (*ValueVersion, bool, error) {
	if key == "" {
		return nil, false, fmt.Errorf("key required")
	}
	if authorID == "" {
		return nil, false, fmt.Errorf("authorID required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	current, exists := d.store[key]
	if !exists {
		if len(d.store) >= MaxKeys {
			return nil, false, fmt.Errorf("conflict store full (max %d keys)", MaxKeys)
		}
		newVersion := &ValueVersion{
			Value:   value,
			Author:  authorID,
			Written: time.Now(),
			Clock:   vector.Tick(ctxVC, authorID),
		}
		d.store[key] = &MVCCEntry{Key: key, Versions: []*ValueVersion{newVersion}}
		return newVersion, false, nil
	}

	hasConflict := false
	survivors := make([]*ValueVersion, 0, len(current.Versions))
	mergeClock := vector.Copy(ctxVC)

	for _, existing := range current.Versions {
		order := vector.Compare(ctxVC, existing.Clock)
		switch order {
		case vector.HappenedBefore:
			// New write is based on stale state. CRITICAL: do NOT silently
			// rebase — the value was computed against ctxVC, not the
			// absorbed clock. Keep the existing version and surface the
			// situation as a conflict so the caller can resolve explicitly.
			survivors = append(survivors, existing)
			hasConflict = true
		case vector.HappenedAfter, vector.Identical:
			// New write causally supersedes existing — discard existing
		case vector.Concurrent:
			// True conflict: keep both
			survivors = append(survivors, existing)
			mergeClock = vector.MergePassive(mergeClock, existing.Clock)
			hasConflict = true
		}
	}

	newVersion := &ValueVersion{
		Value:   value,
		Author:  authorID,
		Written: time.Now(),
		Clock:   vector.Tick(mergeClock, authorID),
	}
	survivors = append(survivors, newVersion)

	// Enforce MaxSiblingsPerKey by trimming the oldest non-new entry when
	// we exceed the cap. Newest version is always retained.
	if len(survivors) > MaxSiblingsPerKey {
		survivors = trimOldest(survivors, len(survivors)-MaxSiblingsPerKey)
	}

	current.Versions = survivors

	if hasConflict && d.bus != nil {
		d.bus.Publish(events.Event{
			Type:      events.EvtConflict,
			Timestamp: time.Now(),
			Conflict: &events.ConflictPayload{
				Key: key,
			},
		})
	}

	return newVersion, hasConflict, nil
}

// trimOldest removes the n oldest non-newest entries. Used to enforce
// MaxSiblingsPerKey. The newest version is always at the tail of the slice.
func trimOldest(versions []*ValueVersion, n int) []*ValueVersion {
	if n <= 0 || len(versions) <= n {
		return versions
	}
	return versions[n:]
}

// Read returns all current versions (siblings) for a key.
// Returns nil if key does not exist.
func (d *ConflictDetector) Read(key string) []*ValueVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := d.store[key]
	if entry == nil {
		return nil
	}
	result := make([]*ValueVersion, len(entry.Versions))
	copy(result, entry.Versions)
	return result
}

// Resolve applies a conflict resolution strategy to a key.
func (d *ConflictDetector) Resolve(key string, strategy ConflictResolution) *ValueVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	entry := d.store[key]
	if entry == nil || len(entry.Versions) == 0 {
		return nil
	}
	winner := Resolve(entry.Versions, strategy)
	if winner != nil && strategy != KeepAll {
		entry.Versions = []*ValueVersion{winner}
	}
	return winner
}
