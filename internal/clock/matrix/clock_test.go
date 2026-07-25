package matrix_test

import (
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/matrix"
)

func TestNew_ZeroMatrix(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	mc := matrix.New(pids)
	for _, p := range pids {
		for _, q := range pids {
			if mc[p][q] != 0 {
				t.Fatalf("New: mc[%s][%s] = %d, want 0", p, q, mc[p][q])
			}
		}
	}
}

func TestTick_IncrementsSelf(t *testing.T) {
	mc := matrix.New([]string{"P1", "P2"})
	mc2 := matrix.Tick(mc, "P1")
	if mc2["P1"]["P1"] != 1 {
		t.Fatalf("Tick: MC[P1][P1] = %d, want 1", mc2["P1"]["P1"])
	}
	// Original not mutated
	if mc["P1"]["P1"] != 0 {
		t.Fatal("Tick mutated original matrix")
	}
}

func TestReceive_MC3(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	local := matrix.New(pids)
	// Local P1 has done 3 events
	for i := 0; i < 3; i++ {
		local = matrix.Tick(local, "P1")
	}

	// Incoming from P2: P2 knows P1=2, P2=5, P3=1
	incoming := matrix.New(pids)
	incoming["P2"]["P1"] = 2
	incoming["P2"]["P2"] = 5
	incoming["P2"]["P3"] = 1

	result := matrix.Receive(local, incoming, "P1", "P2")

	// P1's self-counter should increment by 1
	if result["P1"]["P1"] != 4 {
		t.Fatalf("Receive: MC[P1][P1] = %d, want 4", result["P1"]["P1"])
	}
	// P1 learns from P2's view of P2 = 5
	if result["P1"]["P2"] != 5 {
		t.Fatalf("Receive: MC[P1][P2] = %d, want 5", result["P1"]["P2"])
	}
	// P1 learns from P2's view of P3 = 1
	if result["P1"]["P3"] != 1 {
		t.Fatalf("Receive: MC[P1][P3] = %d, want 1", result["P1"]["P3"])
	}
}

func TestReceive_KeepsLocalMax(t *testing.T) {
	pids := []string{"P1", "P2"}
	// Local P1 knows P2=10
	local := matrix.New(pids)
	local["P1"]["P2"] = 10

	// Incoming from P2 says P2=3 (stale)
	incoming := matrix.New(pids)
	incoming["P2"]["P2"] = 3

	result := matrix.Receive(local, incoming, "P1", "P2")
	// Should keep local max: P2=10
	if result["P1"]["P2"] != 10 {
		t.Fatalf("Receive: should keep local max P2=10, got %d", result["P1"]["P2"])
	}
}

func TestMinKnowledge_GCBound(t *testing.T) {
	pids := []string{"P1", "P2", "P3"}
	mc := matrix.New(pids)

	// All processes know P1 did 5 events
	mc["P1"]["P1"] = 5
	mc["P2"]["P1"] = 5
	mc["P3"]["P1"] = 5

	if got := matrix.MinKnowledge(mc, "P1"); got != 5 {
		t.Fatalf("MinKnowledge all=5: got %d, want 5", got)
	}

	// P3 only knows P1 did 3
	mc["P3"]["P1"] = 3
	if got := matrix.MinKnowledge(mc, "P1"); got != 3 {
		t.Fatalf("MinKnowledge with P3=3: got %d, want 3", got)
	}
}

func TestCanGC(t *testing.T) {
	pids := []string{"P1", "P2"}
	mc := matrix.New(pids)
	mc["P1"]["P1"] = 5
	mc["P2"]["P1"] = 5

	if !matrix.CanGC(mc, "P1", 5) {
		t.Fatal("CanGC should be true when all know ≥5")
	}
	if matrix.CanGC(mc, "P1", 6) {
		t.Fatal("CanGC should be false when not all know ≥6")
	}
}

// TestFourProcess_Simulation: 4-process ring, verify MinKnowledge after message passing.
func TestFourProcess_Simulation(t *testing.T) {
	pids := []string{"P1", "P2", "P3", "P4"}
	clocks := map[string]matrix.MatrixClock{
		"P1": matrix.New(pids),
		"P2": matrix.New(pids),
		"P3": matrix.New(pids),
		"P4": matrix.New(pids),
	}

	// Do 5 internal events at P1
	for i := 0; i < 5; i++ {
		clocks["P1"] = matrix.Tick(clocks["P1"], "P1")
	}

	// P1 → P2
	var stamp matrix.MatrixClock
	clocks["P1"], stamp = matrix.Send(clocks["P1"], "P1")
	clocks["P2"] = matrix.Receive(clocks["P2"], stamp, "P2", "P1")

	// P2 → P3
	clocks["P2"], stamp = matrix.Send(clocks["P2"], "P2")
	clocks["P3"] = matrix.Receive(clocks["P3"], stamp, "P3", "P2")

	// P3 → P4
	clocks["P3"], stamp = matrix.Send(clocks["P3"], "P3")
	clocks["P4"] = matrix.Receive(clocks["P4"], stamp, "P4", "P3")

	// P4 knows about P1's events indirectly
	if clocks["P4"]["P4"]["P1"] < 5 {
		t.Fatalf("P4 should know P1 did ≥5 events, got %d", clocks["P4"]["P4"]["P1"])
	}
}

func TestMinKnowledge_EmptyMatrix(t *testing.T) {
	mc := matrix.MatrixClock{}
	if got := matrix.MinKnowledge(mc, "P1"); got != 0 {
		t.Fatalf("Empty matrix MinKnowledge: got %d, want 0", got)
	}
}
