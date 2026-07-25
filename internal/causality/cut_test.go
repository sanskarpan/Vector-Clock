package causality_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/causality"
	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
)

func TestCutBefore_Basic(t *testing.T) {
	events := []*causality.Event{
		{ID: "e1", ProcessID: "P1", LocalSeq: 1, Type: causality.EventSend, MessageID: "m1"},
		{ID: "e2", ProcessID: "P2", LocalSeq: 1, Type: causality.EventReceive, MessageID: "m1"},
		{ID: "e3", ProcessID: "P2", LocalSeq: 2},
	}
	vc := vector.VectorClock{"P1": 1, "P2": 2}
	cut := causality.CutBefore(vc, events)

	if cut.Frontier["P1"] != 1 {
		t.Fatalf("expected P1 frontier=1, got %d", cut.Frontier["P1"])
	}
	if cut.Frontier["P2"] != 2 {
		t.Fatalf("expected P2 frontier=2, got %d", cut.Frontier["P2"])
	}

	// Should be consistent (send and receive both in cut)
	if !causality.IsConsistent(cut, events, causality.FindMessages(events)) {
		t.Fatal("cut should be consistent")
	}
}

func TestCutBefore_EmptyVC(t *testing.T) {
	events := []*causality.Event{
		{ID: "e1", ProcessID: "P1", LocalSeq: 1},
		{ID: "e2", ProcessID: "P2", LocalSeq: 1},
	}
	vc := vector.VectorClock{}
	cut := causality.CutBefore(vc, events)

	// Empty VC means everything at 0 → no events included
	if cut.Frontier["P1"] != -1 {
		t.Fatalf("expected P1 frontier=-1, got %d", cut.Frontier["P1"])
	}
	if cut.Frontier["P2"] != -1 {
		t.Fatalf("expected P2 frontier=-1, got %d", cut.Frontier["P2"])
	}
}

func TestCutBefore_PartialCut(t *testing.T) {
	events := []*causality.Event{
		{ID: "e1", ProcessID: "P1", LocalSeq: 1},
		{ID: "e2", ProcessID: "P1", LocalSeq: 2},
		{ID: "e3", ProcessID: "P2", LocalSeq: 1},
	}
	vc := vector.VectorClock{"P1": 1}
	cut := causality.CutBefore(vc, events)

	if cut.Frontier["P1"] != 1 {
		t.Fatalf("expected P1 frontier=1, got %d", cut.Frontier["P1"])
	}
	if cut.Frontier["P2"] != -1 {
		t.Fatalf("expected P2 frontier=-1 (not in VC), got %d", cut.Frontier["P2"])
	}
}
