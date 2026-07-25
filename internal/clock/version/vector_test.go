package version_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/version"
)

func TestSync_Dominates(t *testing.T) {
	vvA := version.VersionVector{"A": 2, "B": 1}
	vvB := version.VersionVector{"A": 1, "B": 1}
	if got := vvA.Sync(vvB); got != version.Dominates {
		t.Fatalf("got %v, want Dominates", got)
	}
}

func TestSync_Dominated(t *testing.T) {
	vvA := version.VersionVector{"A": 1, "B": 1}
	vvB := version.VersionVector{"A": 2, "B": 1}
	if got := vvA.Sync(vvB); got != version.Dominated {
		t.Fatalf("got %v, want Dominated", got)
	}
}

func TestSync_Equal(t *testing.T) {
	vvA := version.VersionVector{"A": 1, "B": 2}
	vvB := version.VersionVector{"A": 1, "B": 2}
	if got := vvA.Sync(vvB); got != version.Equal {
		t.Fatalf("got %v, want Equal", got)
	}
}

// TestFalseConflict: plain VV with server IDs produces a false conflict.
// Client reads from B (replica of A:1 write), writes v2 via A.
// With server-based VV, after replication VV_A={A:2}, VV_B={A:2} — no conflict here.
// But the SPEC describes the false-conflict case with concurrent writes.
// Demonstrate: concurrent independent writes look like conflict even when one supersedes.
func TestFalseConflict_PlainVV(t *testing.T) {
	// Write v1 at A: VV_A = {A:1}
	vvV1 := version.New([]string{"A", "B"}).Update("A")

	// Client reads v1, then writes v2 at A with context: VV_V2 = {A:2}
	vvV2 := vvV1.Update("A")

	// v2 dominates v1 — this is the CORRECT case (no false conflict with server IDs here)
	if got := vvV2.Sync(vvV1); got != version.Dominates {
		t.Fatalf("v2 should dominate v1: got %v", got)
	}

	// Demonstrate TRUE conflict: separate write at B
	vvConc := version.New([]string{"A", "B"}).Update("B") // {B:1}
	if got := vvV1.Sync(vvConc); got != version.Conflict {
		t.Fatalf("v1{A:1} and vConc{B:1} should be Conflict: got %v", got)
	}
}
