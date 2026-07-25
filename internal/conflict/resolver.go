// Package conflict implements conflict resolution strategies for concurrent
// value versions in a distributed key-value store.
package conflict

// ConflictResolution selects how siblings are resolved.
type ConflictResolution int

const (
	LastWriterWins ConflictResolution = iota
	FirstWriterWins
	KeepAll
	ManualResolution
	MergeFunc
)

// Resolve applies the resolution strategy and returns the winning version.
// For KeepAll, returns the first sibling (all are preserved in the store).
func Resolve(versions []*ValueVersion, strategy ConflictResolution) *ValueVersion {
	if len(versions) == 0 {
		return nil
	}
	if len(versions) == 1 {
		return versions[0]
	}

	switch strategy {
	case LastWriterWins:
		winner := versions[0]
		for _, v := range versions[1:] {
			if v.Written.After(winner.Written) {
				winner = v
			}
		}
		return winner

	case FirstWriterWins:
		winner := versions[0]
		for _, v := range versions[1:] {
			if v.Written.Before(winner.Written) {
				winner = v
			}
		}
		return winner

	case KeepAll:
		return versions[0] // caller keeps all

	default:
		return versions[len(versions)-1]
	}
}

// ResolveWithFunc resolves versions using a custom merge function.
func ResolveWithFunc(versions []*ValueVersion, fn func([]*ValueVersion) *ValueVersion) *ValueVersion {
	if len(versions) == 0 {
		return nil
	}
	return fn(versions)
}

// customResolvers is a registry of named merge functions.
var customResolvers = make(map[string]func([]*ValueVersion) *ValueVersion)

// RegisterResolver registers a named custom merge function.
func RegisterResolver(name string, fn func([]*ValueVersion) *ValueVersion) {
	customResolvers[name] = fn
}

// GetResolver retrieves a registered resolver by name.
func GetResolver(name string) (func([]*ValueVersion) *ValueVersion, bool) {
	fn, ok := customResolvers[name]
	return fn, ok
}

func init() {
	RegisterResolver("lww", func(versions []*ValueVersion) *ValueVersion {
		return Resolve(versions, LastWriterWins)
	})
	RegisterResolver("fww", func(versions []*ValueVersion) *ValueVersion {
		return Resolve(versions, FirstWriterWins)
	})
	RegisterResolver("keep_all", func(versions []*ValueVersion) *ValueVersion {
		return versions[0]
	})
}
