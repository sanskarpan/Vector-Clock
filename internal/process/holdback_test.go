package process_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/process"
)

func TestHoldBackQueue_OutOfOrderDeliversInCausalOrder(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0}

	m3 := &process.Message{ID: "m3", From: "P1", SentVC: vector.VectorClock{"P1": 3, "P2": 0}}
	m1 := &process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 1, "P2": 0}}
	m2 := &process.Message{ID: "m2", From: "P1", SentVC: vector.VectorClock{"P1": 2, "P2": 0}}

	hbq.Enqueue(m3, m3.SentVC)
	hbq.Enqueue(m1, m1.SentVC)
	hbq.Enqueue(m2, m2.SentVC)

	// TryDeliver loops internally until no more messages are eligible;
	// all three are causally ready (each is the next from P1) so they
	// all deliver in a single call, in causal order m1 → m2 → m3.
	delivered, localVC := hbq.TryDeliver(localVC, "P2")
	if len(delivered) != 3 {
		t.Fatalf("expected 3 deliveries, got %v", msgIDs(delivered))
	}
	if delivered[0].ID != "m1" || delivered[1].ID != "m2" || delivered[2].ID != "m3" {
		t.Fatalf("expected [m1 m2 m3], got %v", msgIDs(delivered))
	}
	if localVC["P1"] != 3 || localVC["P2"] != 3 {
		t.Fatalf("expected localVC {P1:3, P2:3}, got %v", localVC)
	}

	if hbq.Len() != 0 {
		t.Fatalf("expected empty queue, got %d items", hbq.Len())
	}
}

func TestHoldBackQueue_BlockedByCausalPredecessor(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0, "P3": 0}

	msgP3 := &process.Message{ID: "m3", From: "P3", SentVC: vector.VectorClock{"P1": 1, "P2": 0, "P3": 1}}
	hbq.Enqueue(msgP3, msgP3.SentVC)

	msgP1 := &process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 1, "P2": 0, "P3": 0}}
	hbq.Enqueue(msgP1, msgP1.SentVC)

	// m1 delivers (P1:1 == 0+1). This unblocks m3 (P3:1 == 0+1, P1:1 <= 1).
	// Both deliver in a single TryDeliver call in causal order.
	delivered, localVC := hbq.TryDeliver(localVC, "P2")
	if len(delivered) != 2 {
		t.Fatalf("expected 2 deliveries, got %v", msgIDs(delivered))
	}
	if delivered[0].ID != "m1" || delivered[1].ID != "m3" {
		t.Fatalf("expected [m1 m3], got %v", msgIDs(delivered))
	}
	if localVC["P1"] != 1 || localVC["P2"] != 2 || localVC["P3"] != 1 {
		t.Fatalf("expected localVC {P1:1, P2:2, P3:1}, got %v", localVC)
	}

	if hbq.Len() != 0 {
		t.Fatalf("expected empty queue, got %d items", hbq.Len())
	}
}

func TestHoldBackQueue_BlockedByReturnsBlockers(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0, "P3": 0}

	msg := &process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 5, "P2": 0, "P3": 2}}
	hbq.Enqueue(msg, msg.SentVC)

	blockers := hbq.BlockedBy(localVC)
	if len(blockers) != 2 {
		t.Fatalf("expected 2 blockers, got %v", blockers)
	}
	blockerSet := make(map[string]bool)
	for _, b := range blockers {
		blockerSet[b] = true
	}
	if !blockerSet["P1"] {
		t.Fatal("expected P1 to be a blocker")
	}
	if !blockerSet["P3"] {
		t.Fatal("expected P3 to be a blocker")
	}
}

func TestHoldBackQueue_EmptyQueue(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0}

	if hbq.Len() != 0 {
		t.Fatalf("expected Len() == 0, got %d", hbq.Len())
	}

	delivered, newVC := hbq.TryDeliver(localVC, "P2")
	if len(delivered) != 0 {
		t.Fatalf("expected no deliveries, got %v", msgIDs(delivered))
	}
	if newVC["P1"] != 0 || newVC["P2"] != 0 {
		t.Fatalf("expected localVC unchanged by empty TryDeliver, got %v", newVC)
	}

	blockers := hbq.BlockedBy(localVC)
	if len(blockers) != 0 {
		t.Fatalf("expected no blockers, got %v", blockers)
	}
}

func TestHoldBackQueue_MultipleSenders(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0, "P3": 0}

	m4 := &process.Message{ID: "m4", From: "P3", SentVC: vector.VectorClock{"P1": 2, "P2": 0, "P3": 2}}
	m3 := &process.Message{ID: "m3", From: "P1", SentVC: vector.VectorClock{"P1": 2, "P2": 0, "P3": 1}}
	m2 := &process.Message{ID: "m2", From: "P3", SentVC: vector.VectorClock{"P1": 1, "P2": 0, "P3": 1}}
	m1 := &process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 1, "P2": 0, "P3": 0}}

	hbq.Enqueue(m4, m4.SentVC)
	hbq.Enqueue(m3, m3.SentVC)
	hbq.Enqueue(m2, m2.SentVC)
	hbq.Enqueue(m1, m1.SentVC)

	// All four deliver in a single TryDeliver call: m1 → m2 → m3 → m4.
	delivered, localVC := hbq.TryDeliver(localVC, "P2")
	if len(delivered) != 4 {
		t.Fatalf("expected 4 deliveries, got %v", msgIDs(delivered))
	}
	expected := []string{"m1", "m2", "m3", "m4"}
	for i, id := range expected {
		if delivered[i].ID != id {
			t.Fatalf("delivery[%d]: expected %s, got %s", i, id, delivered[i].ID)
		}
	}
	// P2's clock ticks once per delivery (VC3 rule).
	if localVC["P1"] != 2 || localVC["P2"] != 4 || localVC["P3"] != 2 {
		t.Fatalf("expected localVC {P1:2, P2:4, P3:2}, got %v", localVC)
	}

	if hbq.Len() != 0 {
		t.Fatalf("expected empty queue, got %d items", hbq.Len())
	}
}

func msgIDs(msgs []*process.Message) []string {
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	return ids
}
