package vector_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

func TestConcurrentSet_TwoConcurrent(t *testing.T) {
	events := []vector.VCEvent{
		{ID: "a", Clock: vector.VectorClock{"P1": 1, "P2": 0}},
		{ID: "b", Clock: vector.VectorClock{"P1": 0, "P2": 1}},
	}
	groups := vector.ConcurrentSet(events)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if len(groups[0]) != 2 {
		t.Fatalf("expected 2 events in group, got %d", len(groups[0]))
	}
}

func TestConcurrentSet_HappenedBeforeSeparates(t *testing.T) {
	// a → b, but c is concurrent with both
	a := vector.VCEvent{ID: "a", Clock: vector.VectorClock{"P1": 1, "P2": 0, "P3": 0}}
	b := vector.VCEvent{ID: "b", Clock: vector.VectorClock{"P1": 2, "P2": 1, "P3": 0}}
	c := vector.VCEvent{ID: "c", Clock: vector.VectorClock{"P1": 0, "P2": 0, "P3": 1}}

	groups := vector.ConcurrentSet([]vector.VCEvent{a, b, c})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	// a→b so a and b must be in different groups.
	groupOf := make(map[string]int)
	for gi, g := range groups {
		for _, ev := range g {
			groupOf[ev.ID] = gi
		}
	}
	if groupOf["a"] == groupOf["b"] {
		t.Fatal("a and b should be in different groups")
	}
}

func TestConcurrentSet_EmptyInput(t *testing.T) {
	groups := vector.ConcurrentSet(nil)
	if groups != nil {
		t.Fatalf("expected nil for empty input, got %v", groups)
	}
}

func TestConcurrentSet_SingleEvent(t *testing.T) {
	events := []vector.VCEvent{
		{ID: "a", Clock: vector.VectorClock{"P1": 1}},
	}
	groups := vector.ConcurrentSet(events)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if len(groups[0]) != 1 {
		t.Fatalf("expected 1 event in group, got %d", len(groups[0]))
	}
	if groups[0][0].ID != "a" {
		t.Fatalf("expected event 'a', got '%s'", groups[0][0].ID)
	}
}

func TestConcurrentSet_CausallyRelated(t *testing.T) {
	// Two events where one happened before the other
	events := []vector.VCEvent{
		{ID: "a", Clock: vector.VectorClock{"P1": 1, "P2": 0}},
		{ID: "b", Clock: vector.VectorClock{"P1": 2, "P2": 1}},
	}
	groups := vector.ConcurrentSet(events)
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups for causally related events, got %d", len(groups))
	}
}
