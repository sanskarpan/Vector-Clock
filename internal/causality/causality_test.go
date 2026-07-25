package causality_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/causality"
)

// ── CausalGraph tests ────────────────────────────────────────────────────────

func TestHappenedBefore_Direct(t *testing.T) {
	g := causality.NewCausalGraph()
	addEvent(g, "e1", "P1", 1)
	addEvent(g, "e2", "P1", 2)
	g.AddEdge("e1", "e2")

	if !g.HappenedBefore("e1", "e2") {
		t.Fatal("e1 → e2: HappenedBefore should be true")
	}
	if g.HappenedBefore("e2", "e1") {
		t.Fatal("e2 → e1: should not happen before")
	}
}

func TestHappenedBefore_Transitive(t *testing.T) {
	g := causality.NewCausalGraph()
	addEvent(g, "e1", "P1", 1)
	addEvent(g, "e2", "P2", 1)
	addEvent(g, "e3", "P3", 1)
	g.AddEdge("e1", "e2")
	g.AddEdge("e2", "e3")

	if !g.HappenedBefore("e1", "e3") {
		t.Fatal("transitivity: e1 → e2 → e3 implies e1 → e3")
	}
}

func TestConcurrent(t *testing.T) {
	g := causality.NewCausalGraph()
	addEvent(g, "e1", "P1", 1)
	addEvent(g, "e2", "P2", 1)
	// No edges: concurrent

	if !g.Concurrent("e1", "e2") {
		t.Fatal("e1 and e2 with no edges should be Concurrent")
	}
}

func TestHappenedBefore_SameEvent(t *testing.T) {
	g := causality.NewCausalGraph()
	addEvent(g, "e1", "P1", 1)
	if g.HappenedBefore("e1", "e1") {
		t.Fatal("event should not happen before itself (irreflexive)")
	}
}

func TestCausalHistory(t *testing.T) {
	g := causality.NewCausalGraph()
	addEvent(g, "e1", "P1", 1)
	addEvent(g, "e2", "P2", 1)
	addEvent(g, "e3", "P1", 2)
	g.AddEdge("e1", "e2")
	g.AddEdge("e2", "e3")

	hist := g.CausalHistory("e3")
	ids := eventIDs(hist)
	if !contains(ids, "e1") || !contains(ids, "e2") {
		t.Fatalf("CausalHistory(e3) should include e1 and e2, got %v", ids)
	}
}

func TestTopologicalSort(t *testing.T) {
	g := causality.NewCausalGraph()
	addEvent(g, "e1", "P1", 1)
	addEvent(g, "e2", "P2", 1)
	addEvent(g, "e3", "P1", 2)
	g.AddEdge("e1", "e3")
	g.AddEdge("e2", "e3")

	sorted := g.TopologicalSort()
	// e3 must come after both e1 and e2
	pos := func(id string) int {
		for i, e := range sorted {
			if e.ID == id {
				return i
			}
		}
		return -1
	}
	if pos("e3") <= pos("e1") || pos("e3") <= pos("e2") {
		t.Fatal("e3 must come after e1 and e2 in topological order")
	}
}

// ── IsConsistent tests ───────────────────────────────────────────────────────

func TestIsConsistent_ValidCut(t *testing.T) {
	// P1: e1(send) → e2
	// P2: e3(recv)
	events := []*causality.Event{
		{ID: "e1", ProcessID: "P1", LocalSeq: 1, Type: causality.EventSend, MessageID: "m1"},
		{ID: "e2", ProcessID: "P1", LocalSeq: 2},
		{ID: "e3", ProcessID: "P2", LocalSeq: 1, Type: causality.EventReceive, MessageID: "m1"},
	}
	messages := []*causality.Message{
		{ID: "m1", SendEventID: "e1", RecvEventID: "e3"},
	}

	// Cut: P1 includes e1 and e2 (frontier=2), P2 includes e3 (frontier=1)
	// send(m1)=e1 in cut, recv(m1)=e3 in cut → consistent
	cut := causality.Cut{Frontier: map[string]int{"P1": 2, "P2": 1}}
	if !causality.IsConsistent(cut, events, messages) {
		t.Fatal("Cut should be consistent: both send and recv in cut")
	}
}

func TestIsConsistent_InvalidCut(t *testing.T) {
	events := []*causality.Event{
		{ID: "e1", ProcessID: "P1", LocalSeq: 1, Type: causality.EventSend, MessageID: "m1"},
		{ID: "e3", ProcessID: "P2", LocalSeq: 1, Type: causality.EventReceive, MessageID: "m1"},
	}
	messages := []*causality.Message{
		{ID: "m1", SendEventID: "e1", RecvEventID: "e3"},
	}

	// Cut: P1 has NO events (frontier=-1), P2 includes e3
	// recv(m1) in cut but send(m1) NOT in cut → INCONSISTENT
	cut := causality.Cut{Frontier: map[string]int{"P1": -1, "P2": 1}}
	if causality.IsConsistent(cut, events, messages) {
		t.Fatal("Cut should be INCONSISTENT: recv without send in cut")
	}
}

func TestFindConsistentCuts_Count(t *testing.T) {
	// Simple 2-process, 2-event scenario
	// P1: e1(send) → e2
	// P2: e3(recv)
	// m1: e1 → e3
	events := []*causality.Event{
		{ID: "e1", ProcessID: "P1", LocalSeq: 1, Type: causality.EventSend, MessageID: "m1"},
		{ID: "e2", ProcessID: "P1", LocalSeq: 2},
		{ID: "e3", ProcessID: "P2", LocalSeq: 1, Type: causality.EventReceive, MessageID: "m1"},
	}
	messages := []*causality.Message{
		{ID: "m1", SendEventID: "e1", RecvEventID: "e3"},
	}
	cuts := causality.FindConsistentCuts(events, messages)
	// Should find at least some consistent cuts (not zero)
	if len(cuts) == 0 {
		t.Fatal("FindConsistentCuts should find at least 1 consistent cut")
	}
	// All returned cuts should actually be consistent
	for i, c := range cuts {
		if !causality.IsConsistent(c, events, messages) {
			t.Fatalf("Cut %d returned by FindConsistentCuts is not consistent", i)
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func addEvent(g *causality.CausalGraph, id, pid string, seq uint64) {
	g.AddEvent(&causality.Event{ID: id, ProcessID: pid, LocalSeq: seq})
}

func eventIDs(events []*causality.Event) []string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

func contains(ids []string, target string) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}
