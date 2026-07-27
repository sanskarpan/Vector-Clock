// Package matrix implements Kshemkalyani-Singhal matrix clocks.
// MT[i][j] = process i's knowledge of how many events process j has done.
// Row i is process i's own vector clock.
package matrix

// MatrixClock is a sparse N×N matrix. MT[pid1][pid2] = value.
// Missing keys are treated as 0.
type MatrixClock map[string]map[string]uint64

// New creates a zero-initialized matrix clock for the given process IDs.
func New(pids []string) MatrixClock {
	mc := make(MatrixClock, len(pids))
	for _, p := range pids {
		mc[p] = make(map[string]uint64, len(pids))
		for _, q := range pids {
			mc[p][q] = 0
		}
	}
	return mc
}

// DeepCopy returns a full deep copy of the matrix clock.
func DeepCopy(mc MatrixClock) MatrixClock {
	result := make(MatrixClock, len(mc))
	for p, row := range mc {
		r := make(map[string]uint64, len(row))
		for q, v := range row {
			r[q] = v
		}
		result[p] = r
	}
	return result
}

// Tick increments MT[self][self] (MC1: internal event).
func Tick(mc MatrixClock, self string) MatrixClock {
	result := DeepCopy(mc)
	if result[self] == nil {
		result[self] = make(map[string]uint64)
	}
	result[self][self]++
	return result
}

// Send increments own clock and returns both updated local and a copy for the message (MC2).
func Send(mc MatrixClock, self string) (local MatrixClock, msgStamp MatrixClock) {
	local = Tick(mc, self)
	msgStamp = DeepCopy(local)
	return
}

// Receive processes an incoming matrix clock (Kshemkalyani-Singhal MC3):
//  1. Increment MT[self][self]
//  2. For self's row: pointwise-max with the sender's row in the incoming matrix
//  3. For every OTHER row k: pointwise-max with incoming[k] so that we
//     propagate the sender's transitive knowledge of all other processes.
//     Without step 3, MinKnowledge always reads stale zeros for non-self rows.
func Receive(local MatrixClock, incoming MatrixClock, self, sender string) MatrixClock {
	result := DeepCopy(local)
	if result[self] == nil {
		result[self] = make(map[string]uint64)
	}
	result[self][self]++ // tick own clock

	// Step 2: merge sender's row into self's row.
	for pid, val := range incoming[sender] {
		if result[self][pid] < val {
			result[self][pid] = val
		}
	}

	// Step 3: propagate sender's knowledge of all other processes.
	for k, incomingRow := range incoming {
		if k == self {
			continue // already handled in step 2
		}
		if result[k] == nil {
			result[k] = make(map[string]uint64)
		}
		for pid, val := range incomingRow {
			if result[k][pid] < val {
				result[k][pid] = val
			}
		}
	}
	return result
}

// MinKnowledge returns min_k(MT[k][pid]) — the lower bound on what ALL
// processes know about pid's event count. Enables safe garbage collection.
func MinKnowledge(mc MatrixClock, pid string) uint64 {
	if len(mc) == 0 {
		return 0
	}
	min := ^uint64(0) // max uint64
	for _, row := range mc {
		v := row[pid] // missing = 0
		if v < min {
			min = v
		}
	}
	if min == ^uint64(0) {
		return 0
	}
	return min
}

// CanGC returns true if it is safe to garbage collect pid's events up to upTo.
// Safe when ALL processes know about at least upTo events from pid.
func CanGC(mc MatrixClock, pid string, upTo uint64) bool {
	return MinKnowledge(mc, pid) >= upTo
}
