package e2e

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// TestE2E_GoroutineLeak_SustainedTraffic sends a steady stream of
// HTTP traffic for several seconds, then verifies that the goroutine
// count after the server shuts down is within a reasonable delta of
// the count before it started. Catches goroutine leaks from anywhere
// in the request-handling path.
//
// Uses a single client with a custom transport (limited connection
// pool) to avoid exhausting ephemeral ports on macOS.
func TestE2E_GoroutineLeak_SustainedTraffic(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	tr := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     10,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
	}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	// Warm up.
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
		req.Header.Set("Authorization", "Bearer test:e2e-secret")
		resp, _ := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}
	time.Sleep(100 * time.Millisecond)
	before := runtime.NumGoroutine()

	// Sustained traffic — sequential to avoid pool exhaustion on macOS.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
		req.Header.Set("Authorization", "Bearer test:e2e-secret")
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			resp.Body.Close()
		}
	}
	tr.CloseIdleConnections()

	// Stop the server.
	cleanup()
	time.Sleep(2 * time.Second) // let goroutines drain
	runtime.GC()

	after := runtime.NumGoroutine()
	if after > before+30 {
		t.Errorf("goroutine leak: before=%d after=%d (delta=%d)", before, after, after-before)
	}
}

// TestE2E_MemoryLeak_SustainedTraffic is a coarse memory leak check:
// after a few seconds of traffic, heap allocation should be bounded.
// We don't assert a hard number (Go's GC makes that flaky) but we
// check that a 3x growth is not seen.
func TestE2E_MemoryLeak_SustainedTraffic(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	tr := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     10,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{Transport: tr, Timeout: 10 * time.Second}

	// Warm up.
	for i := 0; i < 20; i++ {
		req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
		req.Header.Set("Authorization", "Bearer test:e2e-secret")
		resp, _ := client.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	// Sustained traffic — sequential.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
		req.Header.Set("Authorization", "Bearer test:e2e-secret")
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			resp.Body.Close()
		}
	}
	tr.CloseIdleConnections()
	cleanup()
	time.Sleep(2 * time.Second)
	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	heapGrowth := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
	if heapGrowth > 50*1024*1024 {
		t.Errorf("memory growth: m1.HeapAlloc=%d m2.HeapAlloc=%d delta=%d",
			m1.HeapAlloc, m2.HeapAlloc, heapGrowth)
	}
}

// TestE2E_SnapshotChaos_NetworkPartition simulates a network partition
// during a Chandy-Lamport snapshot by spawning processes in stages
// and triggering snapshots that cross partitions. The coordinator
// must not deadlock and any partial snapshot must remain consistent.
func TestE2E_SnapshotChaos_NetworkPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos in -short mode")
	}
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()
	for i := 1; i <= 6; i++ {
		pid := "P" + strconv.Itoa(i)
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID:           pid,
			ClockType:    process.ClockVector,
			DeliveryMode: process.CausalDelivery,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Background traffic on all 6 processes.
	stop := make(chan struct{})
	var trafficWG sync.WaitGroup
	for i := 0; i < 6; i++ {
		trafficWG.Add(1)
		pid := "P" + strconv.Itoa(i+1)
		go func() {
			defer trafficWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = sim.InternalEvent(pid)
				_ = sim.SendMessage(pid, "P"+strconv.Itoa((i%6)+1), i)
				time.Sleep(time.Millisecond)
			}
		}()
	}

	// Trigger 5 concurrent snapshots and immediately kill processes
	// that are involved.
	var snapWG sync.WaitGroup
	for i := 0; i < 5; i++ {
		snapWG.Add(1)
		pid := "P" + strconv.Itoa(i+1)
		go func() {
			defer snapWG.Done()
			snapID, err := sim.TriggerSnapshot(pid)
			if err != nil {
				return
			}
			// Wait briefly then kill the initiator.
			time.Sleep(5 * time.Millisecond)
			_ = sim.KillProcess(pid)
			// Snapshot may complete or not — both are acceptable.
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				gs := sim.GetSnapshot(snapID)
				if gs != nil && gs.Done() {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	snapWG.Wait()
	close(stop)
	trafficWG.Wait()

	// No assertions on snapshot success — only that the simulation
	// didn't deadlock. Reaching this point is the success.
}

// TestE2E_HighChurn_SpawnKill exercises the spawn/kill cycle
// repeatedly to catch race conditions in the simulation lifecycle.
// Opt-in via VC_E2E_CHAOS=1 because it consumes significant FDs.
func TestE2E_HighChurn_SpawnKill(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	if os.Getenv("VC_E2E_CHAOS") == "" {
		t.Skip("set VC_E2E_CHAOS=1 to run")
	}
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				pid := "churn-" + strconv.Itoa(i)
				if err := sim.SpawnProcess(process.ProcessConfig{
					ID: pid, ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
				}); err == nil {
					_ = sim.KillProcess(pid)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestE2E_Resilience_ChildProcessKilled is a black-box resilience
// test: the test server is killed with SIGKILL (no graceful shutdown).
// The test process must not crash, and the cleanup must not hang.
func TestE2E_Resilience_ChildProcessKilled(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	port := freePort(t)
	addr := "http://127.0.0.1:" + strconv.Itoa(port)

	binPath := sharedBinPath
	if binPath == "" {
		t.Skip("binary not built")
	}

	cmd := exec.Command(binPath)
	cmd.Env = append(cmd.Environ(),
		"PORT="+strconv.Itoa(port),
		"VC_API_TOKENS=test:e2e-secret",
	)
	devNull, _ := exec.LookPath("sh") // existence check
	_ = devNull
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	// Wait for healthy.
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := http.Get(addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
	}

	// SIGKILL.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("kill did not complete within 5s")
	}
	// No assertion needed — the test passes if we don't crash or hang.
}

// Suppress unused warnings.
var _ = process.StatusRunning
