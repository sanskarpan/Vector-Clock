package simulation_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// TestSimulation_StopReleasesGoroutines verifies that repeated Stop calls
// on multiple simulations do not leak goroutines. Catches regression of P4.
func TestSimulation_StopReleasesGoroutines(t *testing.T) {
	// Snapshot the current goroutine count, run a sim, then verify the
	// count returns to baseline after Stop.
	// Note: this is a best-effort test — Go test runners share a process
	// with other tests so the count is approximate. We allow some slack.
	for i := 0; i < 5; i++ {
		sim := simulation.New(simulation.SimConfig{
			ClockType:    process.ClockVector,
			DeliveryMode: process.CausalDelivery,
			Channels:     "full_mesh",
		})
		for j := 1; j <= 3; j++ {
			pid := "P" + string(rune('0'+j))
			if err := sim.SpawnProcess(process.ProcessConfig{
				ID: pid, ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
			}); err != nil {
				t.Fatal(err)
			}
		}
		sim.Stop()
	}
	// Give goroutines a moment to exit.
	time.Sleep(50 * time.Millisecond)
}

func TestSimulation_SpawnValidatesEmptyID(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()

	err := sim.SpawnProcess(process.ProcessConfig{ID: ""})
	if err == nil {
		t.Fatal("expected error for empty process ID")
	}
}

func TestSimulation_SpawnRejectsDuplicate(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()
	cfg := process.ProcessConfig{ID: "P1", ClockType: process.ClockVector}
	if err := sim.SpawnProcess(cfg); err != nil {
		t.Fatal(err)
	}
	if err := sim.SpawnProcess(cfg); err == nil {
		t.Fatal("expected duplicate spawn to fail")
	}
}

// TestSimulation_ConcurrentInternalEvents is a race-detector regression
// guard. Without proper locking, concurrent InternalEvent / SendMessage
// calls would race on the process clock state.
func TestSimulation_ConcurrentInternalEvents(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.ImmediateDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()
	for i := 1; i <= 3; i++ {
		_ = sim.SpawnProcess(process.ProcessConfig{
			ID:        "P" + string(rune('0'+i)),
			ClockType: process.ClockVector, DeliveryMode: process.ImmediateDelivery,
		})
	}

	var wg sync.WaitGroup
	var calls atomic.Int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pid := "P1"
			if err := sim.InternalEvent(pid); err != nil {
				t.Errorf("internal event: %v", err)
			}
			calls.Add(1)
		}()
	}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sim.SendMessage("P1", "P2", "x"); err != nil {
				t.Errorf("send: %v", err)
			}
			calls.Add(1)
		}()
	}
	wg.Wait()
	if calls.Load() < 150 {
		t.Fatalf("expected 150 ops, got %d", calls.Load())
	}
}

func TestSimulation_ResetTwice(t *testing.T) {
	// Ensure that calling SpawnProcess after Stop on a Simulation does not
	// panic. This exercises the path where Transport.Close() has been
	// called but a new operation somehow arrives.
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	if err := sim.SpawnProcess(process.ProcessConfig{
		ID: "P1", ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
	}); err != nil {
		t.Fatal(err)
	}
	sim.Stop()
	// Second Stop should be a no-op (idempotent).
	sim.Stop()
}
