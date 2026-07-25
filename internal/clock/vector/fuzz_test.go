package vector_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

// FuzzCompare tests that Compare is well-defined on arbitrary inputs.
// Compare must always return one of the four defined orders and must be
// reflexive (a = a) and antisymmetric (a < b ⟹ b > a).
func FuzzCompare(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(0), uint8(0), "P1", "P2")
	f.Add(uint8(1), uint8(2), uint8(3), uint8(4), "P1", "P2")

	f.Fuzz(func(t *testing.T, a1, a2, b1, b2 uint8, k1, k2 string) {
		a := vector.VectorClock{k1: uint64(a1), k2: uint64(a2)}
		b := vector.VectorClock{k1: uint64(b1), k2: uint64(b2)}

		// Reflexive: a = a
		if vector.Compare(a, a) != vector.Identical {
			t.Fatalf("Compare(a, a) must be Identical, got %v", vector.Compare(a, a))
		}
		// Symmetric: if a happened-before b then b happened-after a
		switch vector.Compare(a, b) {
		case vector.HappenedBefore:
			if vector.Compare(b, a) != vector.HappenedAfter {
				t.Fatalf("not antisymmetric: Compare(a,b)=HB but Compare(b,a)=%v",
					vector.Compare(b, a))
			}
		case vector.HappenedAfter:
			if vector.Compare(b, a) != vector.HappenedBefore {
				t.Fatalf("not antisymmetric: Compare(a,b)=HA but Compare(b,a)=%v",
					vector.Compare(b, a))
			}
		}
		// Result is one of the four defined values.
		ord := vector.Compare(a, b)
		if ord != vector.HappenedBefore && ord != vector.HappenedAfter &&
			ord != vector.Concurrent && ord != vector.Identical {
			t.Fatalf("Compare returned invalid order: %d", ord)
		}
	})
}

// FuzzMerge tests that Merge is commutative and idempotent under fuzzing.
func FuzzMerge(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(0), uint8(0))
	f.Add(uint8(1), uint8(2), uint8(3), uint8(4))

	f.Fuzz(func(t *testing.T, a1, a2, b1, b2 uint8) {
		a := vector.VectorClock{"P1": uint64(a1), "P2": uint64(a2)}
		b := vector.VectorClock{"P1": uint64(b1), "P2": uint64(b2)}

		ab := vector.MergePassive(a, b)
		ba := vector.MergePassive(b, a)
		if vector.Compare(ab, ba) != vector.Identical {
			t.Fatalf("Merge not commutative: ab=%v ba=%v", ab, ba)
		}
		// Idempotent: Merge(a, a) = a
		aa := vector.MergePassive(a, a)
		if vector.Compare(a, aa) != vector.Identical {
			t.Fatalf("Merge not idempotent: a=%v aa=%v", a, aa)
		}
		// Merge never decreases any component.
		if ab["P1"] < a["P1"] || ab["P1"] < b["P1"] {
			t.Fatalf("Merge decreased P1: ab[P1]=%d a[P1]=%d b[P1]=%d",
				ab["P1"], a["P1"], b["P1"])
		}
	})
}

// FuzzTickReceive exercises the VC1/VC3 rules with fuzzed inputs.
func FuzzTickReceive(f *testing.F) {
	f.Add(uint8(1), uint8(2), "P1")
	f.Add(uint8(5), uint8(0), "P2")

	f.Fuzz(func(t *testing.T, initial, msg uint8, pid string) {
		if pid == "" {
			return // empty pid is a degenerate case the API would reject
		}
		local := vector.VectorClock{pid: uint64(initial)}
		incoming := vector.VectorClock{pid: uint64(msg)}

		// Tick should monotonically advance local[pid] by 1.
		ticked := vector.Tick(local, pid)
		if ticked[pid] != local[pid]+1 {
			t.Fatalf("Tick did not advance: local=%v ticked=%v", local, ticked)
		}

		// Receive should never decrease any component.
		received := vector.Receive(local, incoming, pid)
		for k, v := range received {
			if v < local[k] {
				t.Fatalf("Receive decreased %s: local=%d received=%d", k, local[k], v)
			}
			if v < incoming[k] {
				t.Fatalf("Receive decreased %s: incoming=%d received=%d", k, incoming[k], v)
			}
		}
	})
}
