package process

import (
	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/conflict"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
)

// CausalKV is a causally-consistent key-value store using MVCC.
type CausalKV struct {
	store *conflict.ConflictDetector
}

// NewCausalKV creates a CausalKV wrapping a ConflictDetector.
func NewCausalKV(bus *events.EventBus) *CausalKV {
	return &CausalKV{store: conflict.New(bus)}
}

// Get returns all current versions (siblings) for a key.
func (kv *CausalKV) Get(key string) []*conflict.ValueVersion {
	return kv.store.Read(key)
}

// Put writes a new version for key. Returns (new version, isConflict, error).
func (kv *CausalKV) Put(key string, value []byte, ctxVC vector.VectorClock, author string) (*conflict.ValueVersion, bool, error) {
	return kv.store.Write(key, value, ctxVC, author)
}

// Resolve applies a conflict resolution strategy to a key.
func (kv *CausalKV) Resolve(key string, strategy conflict.ConflictResolution) *conflict.ValueVersion {
	return kv.store.Resolve(key, strategy)
}

// Merge absorbs entries from a remote map and returns keys that were updated.
func (kv *CausalKV) Merge(remote map[string]*conflict.MVCCEntry) []string {
	items := kv.store.Sync(remote)
	keys := make([]string, 0, len(items))
	for _, item := range items {
		if item.Merged {
			keys = append(keys, item.Key)
		}
	}
	return keys
}
