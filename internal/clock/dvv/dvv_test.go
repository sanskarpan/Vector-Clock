package dvv_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/dvv"
)

// TestDVV_NoFalseConflict demonstrates that DVV correctly shows NO conflict
// when a write causally supersedes another (sequential writes via same server).
func TestDVV_NoFalseConflict(t *testing.T) {
	serverAClock := uint64(0)

	// Initial write of v1 at server A
	serverAClock++
	v1 := dvv.NewDVV("A", serverAClock) // {},{A:1}

	// Client reads v1, gets context = v1.ToVV() = {A:1}
	ctx := v1.ToVV()

	// Client writes v2 at A with context {A:1}
	serverAClock++
	v2 := dvv.UpdateDVV(dvv.DVV{Vector: ctx}, "A", serverAClock) // {A:1},{A:2}

	// v2 must dominate v1 (no conflict)
	if !dvv.Dominates(v2, v1) {
		t.Error("DVV: v2 must dominate v1 (no false conflict)")
	}
	if dvv.Dominates(v1, v2) {
		t.Error("DVV: v1 must NOT dominate v2")
	}
}

// TestDVV_TrueConcurrentConflict: concurrent writes at different servers
// produce genuine siblings.
func TestDVV_TrueConcurrentConflict(t *testing.T) {
	// Both A and B receive independent writes with the same initial state (empty)
	v1 := dvv.NewDVV("A", 1) // {},{A:1}
	v2 := dvv.NewDVV("B", 1) // {},{B:1}

	// Neither dominates the other: true conflict
	if dvv.Dominates(v1, v2) {
		t.Error("DVV: v1{A:1} should NOT dominate v2{B:1}")
	}
	if dvv.Dominates(v2, v1) {
		t.Error("DVV: v2{B:1} should NOT dominate v1{A:1}")
	}
}

func TestDVV_Sync_DropsDominated(t *testing.T) {
	v1 := dvv.DVVEntry{DVV: dvv.NewDVV("A", 1), Value: []byte("v1")}
	set := dvv.DVVSet{Siblings: []dvv.DVVEntry{v1}}

	// v2 causally supersedes v1
	v2DVV := dvv.UpdateDVV(dvv.DVV{Vector: v1.DVV.ToVV()}, "A", 2)
	v2 := dvv.DVVEntry{DVV: v2DVV, Value: []byte("v2")}

	result := dvv.Sync(set, v2)
	if len(result.Siblings) != 1 {
		t.Fatalf("Sync: expected 1 sibling (v2), got %d", len(result.Siblings))
	}
	if string(result.Siblings[0].Value) != "v2" {
		t.Error("Sync: expected v2 to survive")
	}
}

func TestDVV_Sync_KeepsSiblings(t *testing.T) {
	v1 := dvv.DVVEntry{DVV: dvv.NewDVV("A", 1), Value: []byte("v1")}
	set := dvv.DVVSet{Siblings: []dvv.DVVEntry{v1}}

	// Concurrent write at B — no causal relationship
	v2 := dvv.DVVEntry{DVV: dvv.NewDVV("B", 1), Value: []byte("v2")}
	result := dvv.Sync(set, v2)

	if len(result.Siblings) != 2 {
		t.Fatalf("Sync: expected 2 siblings, got %d", len(result.Siblings))
	}
}

func TestToVV_IncludesDot(t *testing.T) {
	d := dvv.DVV{
		Vector: map[string]uint64{"A": 2},
		Dot:    &dvv.Dot{Replica: "B", Counter: 5},
	}
	vv := d.ToVV()
	if vv["A"] != 2 {
		t.Fatalf("ToVV: A=%d, want 2", vv["A"])
	}
	if vv["B"] != 5 {
		t.Fatalf("ToVV: B=%d, want 5", vv["B"])
	}
}

func TestToVV_DotOverridesVector(t *testing.T) {
	// If dot.Counter > vector[replica], dot wins
	d := dvv.DVV{
		Vector: map[string]uint64{"A": 1},
		Dot:    &dvv.Dot{Replica: "A", Counter: 3},
	}
	vv := d.ToVV()
	if vv["A"] != 3 {
		t.Fatalf("ToVV: dot should override vector: got %d, want 3", vv["A"])
	}
}
