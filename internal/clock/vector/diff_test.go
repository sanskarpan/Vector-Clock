package vector_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

func TestDiff_Basic(t *testing.T) {
	prev := vector.VectorClock{"P1": 1, "P2": 0}
	current := vector.VectorClock{"P1": 2, "P2": 1}
	d := vector.Diff(prev, current)
	if len(d.Entries) != 2 {
		t.Fatalf("expected 2 entries in diff, got %d", len(d.Entries))
	}
	if d.Entries["P1"] != 2 {
		t.Fatalf("expected P1=2, got %d", d.Entries["P1"])
	}
	if d.Entries["P2"] != 1 {
		t.Fatalf("expected P2=1, got %d", d.Entries["P2"])
	}
}

func TestDiff_EmptyWhenNoChange(t *testing.T) {
	prev := vector.VectorClock{"P1": 3, "P2": 5}
	current := vector.VectorClock{"P1": 3, "P2": 5}
	d := vector.Diff(prev, current)
	if len(d.Entries) != 0 {
		t.Fatalf("expected empty diff, got %d entries", len(d.Entries))
	}
}

func TestDiff_ApplyRestores(t *testing.T) {
	prev := vector.VectorClock{"P1": 0, "P2": 0}
	current := vector.VectorClock{"P1": 3, "P2": 2}
	d := vector.Diff(prev, current)
	restored := vector.Apply(prev, d)
	if !vector.Equal(restored, current) {
		t.Fatalf("Apply(Diff(prev, current)) should restore current: restored=%v, want=%v", restored, current)
	}
}

func TestDiff_MissingPrev(t *testing.T) {
	prev := vector.VectorClock{}
	current := vector.VectorClock{"P1": 5}
	d := vector.Diff(prev, current)
	if d.Entries["P1"] != 5 {
		t.Fatalf("missing prev key should be treated as 0, got %d", d.Entries["P1"])
	}
}

func TestDiff_IgnoresLowerValues(t *testing.T) {
	prev := vector.VectorClock{"P1": 5}
	current := vector.VectorClock{"P1": 3}
	d := vector.Diff(prev, current)
	if len(d.Entries) != 0 {
		t.Fatalf("expected empty diff when current < prev, got %d entries", len(d.Entries))
	}
}

func TestApply_NewerBase(t *testing.T) {
	base := vector.VectorClock{"P1": 5, "P2": 1}
	diff := vector.DiffVC{Entries: map[string]uint64{"P1": 3, "P2": 2}}
	applied := vector.Apply(base, diff)
	if applied["P1"] != 5 {
		t.Fatalf("apply with newer base should keep max P1=5, got %d", applied["P1"])
	}
	if applied["P2"] != 2 {
		t.Fatalf("apply should update P2 to 2, got %d", applied["P2"])
	}
}
