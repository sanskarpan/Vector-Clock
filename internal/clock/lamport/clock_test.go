package lamport_test

import (
	"sort"
	"sync"
	"testing"

	"github.com/DistributedClocks/vectorclock-system/internal/clock/lamport"
)

func TestTick_IncrementsBy1(t *testing.T) {
	c := lamport.New("P1")
	for i := uint64(1); i <= 5; i++ {
		got := c.Tick()
		if got != i {
			t.Fatalf("Tick() = %d, want %d", got, i)
		}
	}
}

func TestSend_SameAsTick(t *testing.T) {
	c := lamport.New("P1")
	c.Tick() // value=1
	got := c.Send()
	if got != 2 {
		t.Fatalf("Send() = %d, want 2", got)
	}
}

func TestReceive_ReceivedGreater(t *testing.T) {
	c := lamport.New("P1")
	c.Tick() // value=1
	got := c.Receive(5)
	// max(1,5)+1 = 6
	if got != 6 {
		t.Fatalf("Receive(5) with local=1: got %d, want 6", got)
	}
}

func TestReceive_LocalGreater(t *testing.T) {
	c := lamport.New("P1")
	for i := 0; i < 10; i++ {
		c.Tick()
	} // value=10
	got := c.Receive(3)
	// max(10,3)+1 = 11
	if got != 11 {
		t.Fatalf("Receive(3) with local=10: got %d, want 11", got)
	}
}

func TestReceive_Equal(t *testing.T) {
	c := lamport.New("P1")
	c.Tick() // value=1
	got := c.Receive(1)
	// max(1,1)+1 = 2
	if got != 2 {
		t.Fatalf("Receive(1) with local=1: got %d, want 2", got)
	}
}

func TestValue_NoIncrement(t *testing.T) {
	c := lamport.New("P1")
	c.Tick()
	c.Tick()
	v1 := c.Value()
	v2 := c.Value()
	if v1 != 2 || v2 != 2 {
		t.Fatalf("Value should not increment: got %d, %d", v1, v2)
	}
}

func TestTotalOrder_DifferentTimestamps(t *testing.T) {
	if lamport.TotalOrder(1, "P1", 2, "P1") != -1 {
		t.Fatal("1 < 2 should return -1")
	}
	if lamport.TotalOrder(5, "P2", 3, "P1") != 1 {
		t.Fatal("5 > 3 should return 1")
	}
}

func TestTotalOrder_TieBreakByPID(t *testing.T) {
	if lamport.TotalOrder(5, "P1", 5, "P2") != -1 {
		t.Fatal("same ts, P1 < P2 should return -1")
	}
	if lamport.TotalOrder(5, "P2", 5, "P1") != 1 {
		t.Fatal("same ts, P2 > P1 should return 1")
	}
	if lamport.TotalOrder(5, "P1", 5, "P1") != 0 {
		t.Fatal("identical should return 0")
	}
}

// TestTotalOrder_Transitivity: sort 100 (ts, pid) pairs and verify consistent ordering.
func TestTotalOrder_Transitivity(t *testing.T) {
	type pair struct {
		ts  uint64
		pid string
	}
	pairs := make([]pair, 100)
	for i := range pairs {
		pairs[i] = pair{uint64(i % 10), "P" + string(rune('0'+i%5))}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		return lamport.TotalOrder(pairs[i].ts, pairs[i].pid, pairs[j].ts, pairs[j].pid) < 0
	})
	// Verify sorted: each element <= next
	for i := 0; i < len(pairs)-1; i++ {
		cmp := lamport.TotalOrder(pairs[i].ts, pairs[i].pid, pairs[i+1].ts, pairs[i+1].pid)
		if cmp > 0 {
			t.Fatalf("sort not stable at index %d: %v > %v", i, pairs[i], pairs[i+1])
		}
	}
}

func TestRace(t *testing.T) {
	c := lamport.New("P1")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Tick()
		}()
	}
	wg.Wait()
	if got := c.Value(); got != 100 {
		t.Fatalf("after 100 concurrent ticks: got %d, want 100", got)
	}
}
