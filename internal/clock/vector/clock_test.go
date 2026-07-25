package vector_test

import (
	"math/rand"
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

// ── Compare tests ────────────────────────────────────────────────────────────

func TestCompare_HappenedBefore(t *testing.T) {
	a := vector.VectorClock{"P1": 1, "P2": 0}
	b := vector.VectorClock{"P1": 2, "P2": 1}
	if got := vector.Compare(a, b); got != vector.HappenedBefore {
		t.Fatalf("Compare: got %v, want HappenedBefore", got)
	}
}

func TestCompare_HappenedAfter(t *testing.T) {
	a := vector.VectorClock{"P1": 3, "P2": 2}
	b := vector.VectorClock{"P1": 1, "P2": 1}
	if got := vector.Compare(a, b); got != vector.HappenedAfter {
		t.Fatalf("Compare: got %v, want HappenedAfter", got)
	}
}

func TestCompare_Concurrent(t *testing.T) {
	a := vector.VectorClock{"P1": 2, "P2": 0}
	b := vector.VectorClock{"P1": 0, "P2": 2}
	if got := vector.Compare(a, b); got != vector.Concurrent {
		t.Fatalf("Compare: got %v, want Concurrent", got)
	}
}

func TestCompare_Identical(t *testing.T) {
	a := vector.VectorClock{"P1": 3, "P2": 2}
	b := vector.VectorClock{"P1": 3, "P2": 2}
	if got := vector.Compare(a, b); got != vector.Identical {
		t.Fatalf("Compare: got %v, want Identical", got)
	}
}

func TestCompare_MissingKeys(t *testing.T) {
	// P2 implicitly 0 in a
	a := vector.VectorClock{"P1": 1}
	b := vector.VectorClock{"P1": 1, "P2": 1}
	// a[P2]=0 < b[P2]=1, a[P1]=b[P1]=1 → HappenedBefore
	if got := vector.Compare(a, b); got != vector.HappenedBefore {
		t.Fatalf("MissingKeys: got %v, want HappenedBefore", got)
	}
}

// TestIsomorphism: a → b ⟺ Compare(VC(a), VC(b)) == HappenedBefore
// Simulate 50 scenarios with known causal ordering.
func TestIsomorphism_HappenedBefore(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	// Simulate: start with zero clocks, apply events in causal order
	for trial := 0; trial < 50; trial++ {
		vc := vector.New(pids)

		// Event a: tick P1
		vcA := vector.Tick(vc, "P1")

		// Send from P1: a causes b at P2
		_, stamp := vector.Send(vcA, "P1")
		vcB := vector.Receive(vector.New(pids), stamp, "P2")

		// a → b (a happened before b): vcA should be HappenedBefore vcB
		if vector.Compare(vcA, vcB) != vector.HappenedBefore {
			t.Fatalf("trial %d: expected HappenedBefore, got %v\nvcA=%v\nvcB=%v", trial, vector.Compare(vcA, vcB), vcA, vcB)
		}
	}
}

func TestConcurrent_NoLink(t *testing.T) {
	pids := []string{"P1", "P2"}
	vcP1 := vector.Tick(vector.New(pids), "P1") // P1 event
	vcP2 := vector.Tick(vector.New(pids), "P2") // P2 event (no message between them)
	if vector.Compare(vcP1, vcP2) != vector.Concurrent {
		t.Fatalf("independent events should be Concurrent: vcP1=%v, vcP2=%v", vcP1, vcP2)
	}
}

// TestMergeCommutative: Merge(a,b) == Merge(b,a)
func TestMerge_Commutative(t *testing.T) {
	for i := 0; i < 100; i++ {
		a := randVC([]string{"P1", "P2", "P3"})
		b := randVC([]string{"P1", "P2", "P3"})
		ab := vector.MergePassive(a, b)
		ba := vector.MergePassive(b, a)
		if vector.Compare(ab, ba) != vector.Identical {
			t.Fatalf("Merge not commutative:\na=%v\nb=%v\nab=%v\nba=%v", a, b, ab, ba)
		}
	}
}

// TestMergeIdempotent: Merge(a,a) == a
func TestMerge_Idempotent(t *testing.T) {
	for i := 0; i < 100; i++ {
		a := randVC([]string{"P1", "P2", "P3"})
		aa := vector.MergePassive(a, a)
		if vector.Compare(a, aa) != vector.Identical {
			t.Fatalf("Merge not idempotent:\na=%v\naa=%v", a, aa)
		}
	}
}

func TestTick_IncrementsSelf(t *testing.T) {
	vc := vector.New([]string{"P1", "P2"})
	vc2 := vector.Tick(vc, "P1")
	if vc2["P1"] != 1 {
		t.Fatalf("Tick: P1 should be 1, got %d", vc2["P1"])
	}
	if vc2["P2"] != 0 {
		t.Fatalf("Tick: P2 should be 0, got %d", vc2["P2"])
	}
	// Original unchanged (functional)
	if vc["P1"] != 0 {
		t.Fatal("Tick mutated original clock")
	}
}

func TestReceive_VC3(t *testing.T) {
	// local = {P1:3, P2:1}, msg = {P1:1, P2:5}, pid=P2
	// merge = max(P1:3,1), max(P2:1,5) = {P1:3, P2:5}
	// tick P2 → {P1:3, P2:6}
	local := vector.VectorClock{"P1": 3, "P2": 1}
	msg := vector.VectorClock{"P1": 1, "P2": 5}
	got := vector.Receive(local, msg, "P2")
	if got["P1"] != 3 || got["P2"] != 6 {
		t.Fatalf("Receive: got %v, want {P1:3, P2:6}", got)
	}
}

func TestSend_ReturnsStamp(t *testing.T) {
	vc := vector.VectorClock{"P1": 2, "P2": 1}
	local, stamp := vector.Send(vc, "P1")
	// Both should have P1:3 (incremented)
	if local["P1"] != 3 || stamp["P1"] != 3 {
		t.Fatalf("Send: local=%v, stamp=%v", local, stamp)
	}
	// Mutating stamp should not affect local
	stamp["P2"] = 99
	if local["P2"] == 99 {
		t.Fatal("Send stamp and local share underlying map")
	}
}

func TestCopy_DeepCopy(t *testing.T) {
	a := vector.VectorClock{"P1": 1, "P2": 2}
	b := vector.Copy(a)
	b["P1"] = 99
	if a["P1"] == 99 {
		t.Fatal("Copy does not deep-copy")
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func randVC(pids []string) vector.VectorClock {
	vc := make(vector.VectorClock, len(pids))
	for _, p := range pids {
		vc[p] = uint64(rand.Intn(10))
	}
	return vc
}
