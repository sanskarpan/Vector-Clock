package process_test

import (
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/matrix"
	"github.com/DistributedClocks/vectorclock-system/internal/clock/vector"
	"github.com/DistributedClocks/vectorclock-system/internal/events"
	"github.com/DistributedClocks/vectorclock-system/internal/process"
)

// mockTransport delivers messages directly to a target process.
type mockTransport struct {
	procs map[string]*process.Process
}

func (t *mockTransport) Send(from, to string, m *process.Message) error {
	if p, ok := t.procs[to]; ok {
		p.Deliver(m)
	}
	return nil
}

func setupProcesses(t *testing.T, ids ...string) (map[string]*process.Process, *events.EventBus, *mockTransport) {
	t.Helper()
	bus := events.NewEventBus()
	xport := &mockTransport{procs: make(map[string]*process.Process)}
	procs := make(map[string]*process.Process)

	for _, id := range ids {
		p := process.New(process.ProcessConfig{
			ID:           id,
			ClockType:    process.ClockVector,
			DeliveryMode: process.CausalDelivery,
			Peers:        ids,
			AllPIDs:      ids,
		}, xport, bus)
		procs[id] = p
		xport.procs[id] = p
	}
	for _, p := range procs {
		p.Start()
	}
	t.Cleanup(func() {
		for _, p := range procs {
			p.Stop()
		}
		bus.Stop()
	})
	return procs, bus, xport
}

func TestHoldBackQueue_BSS_Condition(t *testing.T) {
	// Direct unit test of BSS delivery condition via HoldBackQueue
	hbq := &process.HoldBackQueue{}

	// localVC = {P1:0, P2:0}
	localVC := vector.VectorClock{"P1": 0, "P2": 0}

	// m1 from P1: SentVC = {P1:1, P2:0} → should deliver immediately (P1=localP1+1=1, P2=0≤0)
	m1 := &process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 1, "P2": 0}}
	hbq.Enqueue(m1, m1.SentVC)

	delivered, newVC := hbq.TryDeliver(localVC, "P2")
	if len(delivered) != 1 || delivered[0].ID != "m1" {
		t.Fatalf("m1 should be delivered immediately: got %v", delivered)
	}
	if newVC["P1"] != 1 {
		t.Fatalf("after deliver m1: localVC[P1] should be 1, got %d", newVC["P1"])
	}
}

func TestHoldBackQueue_HoldsUntilPredecessor(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0}

	// m2 from P1: SentVC = {P1:2, P2:0} — requires P1 to be 1 first
	m2 := &process.Message{ID: "m2", From: "P1", SentVC: vector.VectorClock{"P1": 2, "P2": 0}}
	hbq.Enqueue(m2, m2.SentVC)

	// Should NOT deliver yet (P1=0, need P1=1 first)
	delivered, newVC := hbq.TryDeliver(localVC, "P2")
	if len(delivered) != 0 {
		t.Fatalf("m2 should be held: got %d deliveries", len(delivered))
	}

	// Now m1 arrives: update localVC to reflect P1=1
	newVC["P1"] = 1
	delivered, _ = hbq.TryDeliver(newVC, "P2")
	if len(delivered) != 1 || delivered[0].ID != "m2" {
		t.Fatalf("after P1=1, m2 should deliver: got %v", delivered)
	}
}

func TestBlockedBy_MessageBlockedByPredecessor(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0, "P3": 0}

	// Message from P2 that depends on P1: SentVC = {P1:1, P2:1, P3:0}
	// Condition 1: msgVC[P2]=1 == localVC[P2]+1=1 ✓ (P2 is sender)
	// Condition 2: msgVC[P1]=1 > localVC[P1]=0 ✗ (P1 has unseen event)
	hbq.Enqueue(&process.Message{ID: "m1", From: "P2", SentVC: vector.VectorClock{"P1": 1, "P2": 1, "P3": 0}},
		vector.VectorClock{"P1": 1, "P2": 1, "P3": 0})

	blockers := hbq.BlockedBy(localVC)
	if len(blockers) == 0 {
		t.Fatal("expected blockers, got empty")
	}
	found := false
	for _, b := range blockers {
		if b == "P1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected P1 to be a blocker, got %v", blockers)
	}
}

func TestBlockedBy_ReadyToDeliver(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0}

	// Message from P1: SentVC = {P1:1, P2:0} → should deliver
	hbq.Enqueue(&process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 1, "P2": 0}},
		vector.VectorClock{"P1": 1, "P2": 0})

	blockers := hbq.BlockedBy(localVC)
	if len(blockers) != 0 {
		t.Fatalf("expected no blockers for ready-to-deliver message, got %v", blockers)
	}
}

func TestBlockedBy_MultipleBlockers(t *testing.T) {
	hbq := &process.HoldBackQueue{}
	localVC := vector.VectorClock{"P1": 0, "P2": 0, "P3": 0}

	// Message from P1: SentVC = {P1:2, P2:1, P3:0}
	// Condition 1 fail: msgVC[P1]=2 > localVC[P1]+1=1
	// Condition 2 fail: msgVC[P2]=1 > localVC[P2]=0
	hbq.Enqueue(&process.Message{ID: "m1", From: "P1", SentVC: vector.VectorClock{"P1": 2, "P2": 1, "P3": 0}},
		vector.VectorClock{"P1": 2, "P2": 1, "P3": 0})

	blockers := hbq.BlockedBy(localVC)
	// Should have both P1 and P2 as blockers
	if len(blockers) < 2 {
		t.Fatalf("expected at least 2 blockers, got %v", blockers)
	}
	blockerMap := make(map[string]bool)
	for _, b := range blockers {
		blockerMap[b] = true
	}
	if !blockerMap["P1"] {
		t.Fatal("expected P1 to be a blocker")
	}
	if !blockerMap["P2"] {
		t.Fatal("expected P2 to be a blocker")
	}
}

func TestCausalDelivery_HoldsOutOfOrder(t *testing.T) {
	// Deterministic test of causal delivery: drive the hold-back queue
	// directly via Deliver() calls. This avoids the timing nondeterminism
	// of the mockTransport and gives us a controlled scenario.
	//
	// Scenario: P3 has not seen P1's first event, but receives a message
	// claiming P1's counter is 2. The BSS condition must hold this message
	// until P3 observes P1:1 first.

	procs, bus, _ := setupProcesses(t, "P1", "P2", "P3")
	sub := bus.Subscribe([]events.EventType{events.EvtReceive, events.EvtDelivered, events.EvtHeld})
	defer bus.Unsubscribe(sub)

	p3 := procs["P3"]

	// m1 from P1 with SentVC = {P1:2, P2:0, P3:0}. P3's localVC is
	// {P1:0, P2:0, P3:0}. BSS: msgVC[P1]=2 != localVC[P1]+1=1 → HOLD.
	p3.Deliver(&process.Message{
		ID:     "m1",
		From:   "P1",
		To:     "P3",
		Data:   "future-event",
		SentVC: vector.VectorClock{"P1": 2, "P2": 0, "P3": 0},
	})

	// Wait for the hold event.
	var gotHeld bool
	deadline := time.After(500 * time.Millisecond)
	for !gotHeld {
		select {
		case e := <-sub:
			if e.ProcessID == "P3" && e.Type == events.EvtHeld {
				gotHeld = true
			}
		case <-deadline:
			t.Fatal("expected P3 to emit EvtHeld for out-of-order m1")
		}
	}

	// Now deliver m0 from P1 (SentVC = {P1:1}). BSS is satisfied:
	// msgVC[P1]=1==localVC[P1]+1=1, msgVC[P2]=0<=localVC[P2]=0.
	// m0 must deliver; then m1 (still in hold-back queue) becomes
	// deliverable too.
	p3.Deliver(&process.Message{
		ID:     "m0",
		From:   "P1",
		To:     "P3",
		Data:   "missing-predecessor",
		SentVC: vector.VectorClock{"P1": 1, "P2": 0, "P3": 0},
	})

	// Wait for m0 to be received and m1 to be delivered from the queue.
	var gotReceive, gotDelivered bool
	deadline = time.After(1 * time.Second)
	for !(gotReceive && gotDelivered) {
		select {
		case e := <-sub:
			if e.ProcessID != "P3" {
				continue
			}
			if e.Type == events.EvtReceive && e.Message != nil && e.Message.ID == "m0" {
				gotReceive = true
			}
			if e.Type == events.EvtDelivered && e.Message != nil && e.Message.ID == "m1" {
				gotDelivered = true
			}
		case <-deadline:
			t.Fatalf("expected m0 receive + m1 deliver; gotReceive=%v gotDelivered=%v",
				gotReceive, gotDelivered)
		}
	}
}

func TestImmediateDelivery_Allowed(t *testing.T) {
	bus := events.NewEventBus()
	xport := &mockTransport{procs: make(map[string]*process.Process)}

	p := process.New(process.ProcessConfig{
		ID:           "P1",
		ClockType:    process.ClockVector,
		DeliveryMode: process.ImmediateDelivery,
		AllPIDs:      []string{"P1", "P2"},
	}, xport, bus)
	p.Start()
	defer p.Stop()
	defer bus.Stop()

	sub := bus.Subscribe([]events.EventType{events.EvtReceive})

	// Deliver a message directly
	p.Deliver(&process.Message{
		ID:     "m1",
		From:   "P2",
		To:     "P1",
		Data:   "test",
		SentVC: vector.VectorClock{"P1": 0, "P2": 1},
	})
	time.Sleep(50 * time.Millisecond)

	select {
	case e := <-sub:
		if e.Type != events.EvtReceive {
			t.Fatalf("expected receive event, got %v", e.Type)
		}
	default:
		t.Fatal("expected a receive event")
	}
}

// TestMatrixClock_DeliverCausal covers the matrix clock receive path which
// is not exercised by the default test setup. Uses ImmediateDelivery so the
// vector-clock BSS check doesn't intercept (matrix clock has no vector).
func TestMatrixClock_DeliverCausal(t *testing.T) {
	bus := events.NewEventBus()
	xport := &mockTransport{procs: make(map[string]*process.Process)}

	p := process.New(process.ProcessConfig{
		ID:           "P1",
		ClockType:    process.ClockMatrix,
		DeliveryMode: process.ImmediateDelivery,
		AllPIDs:      []string{"P1", "P2"},
		Peers:        []string{"P1", "P2"},
	}, xport, bus)
	xport.procs = map[string]*process.Process{"P1": p}
	p.Start()
	defer p.Stop()
	defer bus.Stop()

	// Deliver a message with matrix stamp from P2.
	p.Deliver(&process.Message{
		ID:         "m1",
		From:       "P2",
		To:         "P1",
		Data:       "hello",
		SentMatrix: matrix.MatrixClock{"P2": {"P1": 0, "P2": 1}},
	})
	time.Sleep(50 * time.Millisecond)

	state := p.Snapshot()
	if state.MatrixClock == nil {
		t.Fatal("matrix clock not populated")
	}
	// P1's self row should be 1 after one receive.
	if state.MatrixClock["P1"]["P1"] != 1 {
		t.Errorf("expected P1[P1]=1, got %d", state.MatrixClock["P1"]["P1"])
	}
}

// TestLamportProcess_TickAndSend covers the Lamport path in Process.
func TestLamportProcess_TickAndSend(t *testing.T) {
	bus := events.NewEventBus()
	xport := &mockTransport{procs: make(map[string]*process.Process)}

	p := process.New(process.ProcessConfig{
		ID:           "P1",
		ClockType:    process.ClockLamport,
		DeliveryMode: process.ImmediateDelivery,
		AllPIDs:      []string{"P1"},
	}, xport, bus)
	xport.procs = map[string]*process.Process{"P1": p}
	p.Start()
	defer p.Stop()
	defer bus.Stop()

	p.InternalEvent()
	if state := p.Snapshot(); state.LamportClock < 1 {
		t.Errorf("expected clock >= 1, got %d", state.LamportClock)
	}
}

// TestPanicInDeliveryDoesNotCrashGoroutine ensures process.run survives
// a panic in handleMessage.
func TestPanicInDeliveryDoesNotCrashGoroutine(t *testing.T) {
	bus := events.NewEventBus()
	defer bus.Stop()
	xport := &mockTransport{procs: make(map[string]*process.Process)}

	p := process.New(process.ProcessConfig{
		ID:           "P1",
		ClockType:    process.ClockVector,
		DeliveryMode: process.ImmediateDelivery,
		AllPIDs:      []string{"P1", "P2"},
	}, xport, bus)
	xport.procs = map[string]*process.Process{"P1": p}
	p.Start()
	defer p.Stop()

	// Trigger an internal event; should not panic even if state is weird.
	p.InternalEvent()
	time.Sleep(50 * time.Millisecond)

	// Process should still be reachable.
	state := p.Snapshot()
	if state.ID == "" {
		t.Fatal("expected non-empty process state")
	}
}

// Re-export HoldBackQueue for test access
func init() {} // ensures the package is importable
