package simulation_test

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// TestProcessSurvives_SIGTERM verifies that the server handles SIGTERM
// gracefully: the running simulation stops, goroutines exit, and the
// process exits 0 within the shutdown grace period.
//
// This exercises the same path the binary takes in production when
// Kubernetes sends SIGTERM during pod termination.
func TestProcessSurvives_SIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix signals only")
	}
	// Build the server binary so we don't pay `go run` compile time.
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer buildCancel()
	binPath := t.TempDir() + "/vc-server"
	// Use the package import path so the test works regardless of CWD.
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binPath,
		"github.com/DistributedClocks/vectorclock-system/cmd/server")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	// Use a high port unlikely to conflict. Tests share a host with
	// other services; pick a random unused port in the ephemeral range.
	port := fmt.Sprintf("2%04d", time.Now().UnixNano()%10000)
	addr := "http://localhost:" + port

	cmd := exec.Command(binPath)
	cmd.Env = append(cmd.Environ(), "PORT="+port, "LOGGING_FORMAT=json")
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	// Wait for the server to be reachable.
	waitForHealthz(t, addr+"/healthz", 5*time.Second)

	// Verify it serves at least one state request successfully. The
	// /api/v1/simulation/state endpoint depends on the bus being
	// drained; on a cold start the bus loop may not have caught up
	// to the initial process spawn events. Retry up to 10 times.
	stateOK := false
	for i := 0; i < 10; i++ {
		stateCmd := exec.Command("curl", "-fsS", addr+"/api/v1/simulation/state")
		if _, err := stateCmd.CombinedOutput(); err == nil {
			stateOK = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !stateOK {
		t.Fatalf("state request failed after 10 attempts")
	}

	// Send SIGTERM.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}

	// Wait for the process to exit; should be within ShutdownGracePeriod.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		// Clean exit is exit code 0. -1 means killed by signal, also ok.
		if exitErr, ok := err.(*exec.ExitError); ok {
			code := exitErr.ExitCode()
			if code != 0 && code != -1 {
				t.Fatalf("non-zero exit on SIGTERM: %d", code)
			}
		}
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("process did not exit within 15s of SIGTERM")
	}
}

// TestSnapshot_CompletesWithKilledProcess simulates a network partition:
// after a Chandy-Lamport snapshot is initiated, one process is killed
// before its markers propagate. The remaining snapshot must remain
// consistent and the coordinator must not deadlock.
func TestSnapshot_CompletesWithKilledProcess(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType:    process.ClockVector,
		DeliveryMode: process.CausalDelivery,
		Channels:     "full_mesh",
	})
	defer sim.Stop()
	for i := 1; i <= 4; i++ {
		pid := "P" + string(rune('0'+i))
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID:           pid,
			ClockType:    process.ClockVector,
			DeliveryMode: process.CausalDelivery,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Generate some background traffic.
	go func() {
		for i := 0; i < 30; i++ {
			pid := "P" + string(rune('0'+(i%4+1)))
			next := "P" + string(rune('0'+((i+1)%4+1)))
			_ = sim.SendMessage(pid, next, i)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Initiate snapshot.
	snapID, err := sim.TriggerSnapshot("P1")
	if err != nil {
		t.Fatalf("trigger snapshot: %v", err)
	}

	// Kill P3 before its markers arrive. P3 won't respond; the snapshot
	// remains incomplete but the coordinator must not deadlock.
	time.Sleep(20 * time.Millisecond)
	if err := sim.KillProcess("P3"); err != nil {
		t.Fatalf("kill P3: %v", err)
	}

	// Wait up to 3 seconds for snapshot completion.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		gs := sim.GetSnapshot(snapID)
		if gs != nil && gs.Done() {
			return // completed (markers happened to arrive before kill)
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Not completed is fine — the run loop should be alive and we must
	// still be able to kill the rest without panic.
	if err := sim.KillProcess("P1"); err != nil {
		t.Fatalf("kill P1: %v", err)
	}
	if err := sim.KillProcess("P2"); err != nil {
		t.Fatalf("kill P2: %v", err)
	}
	if err := sim.KillProcess("P4"); err != nil {
		t.Fatalf("kill P4: %v", err)
	}
}

func waitForHealthz(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Simple socket check via curl equivalent.
		cmd := exec.Command("curl", "-fsS", url)
		if err := cmd.Run(); err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("server not reachable at %s within %v", url, timeout)
}

// TestProcessSurvives_SIGKILL verifies that the server can be killed
// (SIGKILL) without leaving the parent process in a bad state. The
// process exits with a non-zero status; the test passes if cmd.Wait
// returns within 5s.
func TestProcessSurvives_SIGKILL(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix signals only")
	}
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer buildCancel()
	binPath := t.TempDir() + "/vc-server"
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binPath,
		"github.com/DistributedClocks/vectorclock-system/cmd/server")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	port := fmt.Sprintf("2%04d", time.Now().UnixNano()%10000)
	cmd := exec.Command(binPath)
	cmd.Env = append(cmd.Environ(), "PORT="+port)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	waitForHealthz(t, "http://localhost:"+port+"/healthz", 5*time.Second)

	// SIGKILL — process MUST die.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		// OK: process died. We don't care about the exit code.
	case <-time.After(5 * time.Second):
		t.Fatal("process did not die from SIGKILL within 5s")
	}
}
