package conflict_test

import (
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/conflict"
)

func TestWrite_FirstWrite_NoConflict(t *testing.T) {
	d := conflict.New(nil)
	emptyCtx := vector.VectorClock{}
	v, isConflict, err := d.Write("key1", []byte("v1"), emptyCtx, "A")
	if err != nil {
		t.Fatal(err)
	}
	if isConflict {
		t.Fatal("First write should not be a conflict")
	}
	if string(v.Value) != "v1" {
		t.Fatalf("expected v1, got %s", v.Value)
	}
}

func TestWrite_CausallyOrdered_NoConflict(t *testing.T) {
	d := conflict.New(nil)

	ctx0 := vector.VectorClock{"A": 0}
	v1, _, err := d.Write("key", []byte("v1"), ctx0, "A")
	if err != nil {
		t.Fatal(err)
	}
	v2, isConflict, err := d.Write("key", []byte("v2"), v1.Clock, "A")
	if err != nil {
		t.Fatal(err)
	}
	if isConflict {
		t.Fatal("Causally ordered write should not be a conflict")
	}
	versions := d.Read("key")
	if len(versions) != 1 {
		t.Fatalf("expected 1 version (v2 dominates v1), got %d", len(versions))
	}
	if string(versions[0].Value) != string(v2.Value) {
		t.Fatal("expected v2 to survive")
	}
}

func TestWrite_Concurrent_Conflict(t *testing.T) {
	d := conflict.New(nil)

	ctxA := vector.VectorClock{"A": 1}
	_, _, _ = d.Write("key", []byte("v1"), ctxA, "A")
	ctxB := vector.VectorClock{"B": 1}
	_, isConflict, err := d.Write("key", []byte("v2"), ctxB, "B")
	if err != nil {
		t.Fatal(err)
	}
	if !isConflict {
		t.Fatal("Concurrent writes should produce a conflict")
	}

	versions := d.Read("key")
	if len(versions) != 2 {
		t.Fatalf("expected 2 sibling versions, got %d", len(versions))
	}
}

func TestResolve_LWW(t *testing.T) {
	d := conflict.New(nil)

	ctxA := vector.VectorClock{"A": 1}
	_, _, _ = d.Write("key", []byte("v1"), ctxA, "A")
	time.Sleep(2 * time.Millisecond)
	ctxB := vector.VectorClock{"B": 1}
	_, _, _ = d.Write("key", []byte("v2"), ctxB, "B")

	winner := d.Resolve("key", conflict.LastWriterWins)
	if winner == nil {
		t.Fatal("Resolve returned nil")
	}
	if string(winner.Value) != "v2" {
		t.Fatalf("LWW: expected v2 (newer), got %s", winner.Value)
	}
	versions := d.Read("key")
	if len(versions) != 1 {
		t.Fatalf("LWW: expected 1 version after resolve, got %d", len(versions))
	}
}

func TestResolve_KeepAll(t *testing.T) {
	d := conflict.New(nil)

	_, _, _ = d.Write("key", []byte("v1"), vector.VectorClock{"A": 1}, "A")
	_, _, _ = d.Write("key", []byte("v2"), vector.VectorClock{"B": 1}, "B")

	d.Resolve("key", conflict.KeepAll)
	versions := d.Read("key")
	if len(versions) != 2 {
		t.Fatalf("KeepAll: expected 2 siblings preserved, got %d", len(versions))
	}
}

func TestFalseConflict_NotPresent(t *testing.T) {
	d := conflict.New(nil)

	ctx := vector.VectorClock{"A": 0}
	v1, _, err := d.Write("key", []byte("v1"), ctx, "A")
	if err != nil {
		t.Fatal(err)
	}
	_, isConflict, err := d.Write("key", []byte("v2"), v1.Clock, "A")
	if err != nil {
		t.Fatal(err)
	}
	if isConflict {
		t.Fatal("Sequential writes via same replica must NOT produce false conflict")
	}
}

func TestWrite_RejectsEmptyInputs(t *testing.T) {
	d := conflict.New(nil)
	_, _, err := d.Write("", []byte("v"), vector.VectorClock{}, "A")
	if err == nil {
		t.Error("expected error for empty key")
	}
	_, _, err = d.Write("k", []byte("v"), vector.VectorClock{}, "")
	if err == nil {
		t.Error("expected error for empty authorID")
	}
}

func TestWrite_RespectsMaxSiblings(t *testing.T) {
	d := conflict.New(nil)
	// Spawn more concurrent versions than MaxSiblingsPerKey.
	for i := 0; i < conflict.MaxSiblingsPerKey+10; i++ {
		_, _, err := d.Write("k", []byte{byte(i)}, vector.VectorClock{
			"X": uint64(i),
		}, "X")
		if err != nil {
			t.Fatal(err)
		}
	}
	versions := d.Read("k")
	if len(versions) > conflict.MaxSiblingsPerKey {
		t.Errorf("expected at most %d siblings, got %d",
			conflict.MaxSiblingsPerKey, len(versions))
	}
}
