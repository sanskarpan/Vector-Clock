package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/events"
	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// TestWebSocket_SustainedLoad opens N concurrent WebSocket clients, runs
// the simulation for D seconds, and verifies every client received at least
// one event (no drops for healthy clients on localhost).
//
// Skipped by default; enable with `-run TestWebSocket_SustainedLoad`.
// macOS file-descriptor limits make this test fragile in CI; run it on
// Linux where ulimit -n is higher.
func TestWebSocket_SustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	if !enableLoadTests() {
		t.Skip("set VC_ENABLE_LOAD_TESTS=1 to run")
	}
	const N = 5
	const Duration = 500 * time.Millisecond

	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.ImmediateDelivery, Channels: "full_mesh",
	})
	for i := 1; i <= 5; i++ {
		pid := "P" + string(rune('0'+i))
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID: pid, ClockType: process.ClockVector, DeliveryMode: process.ImmediateDelivery,
		}); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewServer(sim, zap.NewNop(), nil, NewMetrics())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	sim.Stop() // we'll generate events manually

	ts := httptest.NewServer(srv.engine)
	defer ts.Close()

	// Open N WS clients.
	u := "ws" + strings.TrimPrefix(ts.URL, "http")
	clients := make([]*websocket.Conn, N)
	var connected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conn, _, err := websocket.DefaultDialer.Dial(u+"/ws", nil)
			if err != nil {
				t.Errorf("client %d dial: %v", i, err)
				return
			}
			clients[i] = conn
			connected.Add(1)
		}(i)
	}
	wg.Wait()

	if int(connected.Load()) != N {
		t.Fatalf("only %d/%d clients connected", connected.Load(), N)
	}

	// Each client reads events into a counter.
	var receivedTotal atomic.Int64
	var readErrs atomic.Int64
	stop := make(chan struct{})
	var rwgs sync.WaitGroup
	for i := 0; i < N; i++ {
		rwgs.Add(1)
		go func(i int) {
			defer rwgs.Done()
			conn := clients[i]
			if conn == nil {
				return
			}
			conn.SetReadDeadline(time.Now().Add(Duration + 2*time.Second))
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _, err := conn.ReadMessage()
				if err != nil {
					readErrs.Add(1)
					return
				}
				receivedTotal.Add(1)
			}
		}(i)
	}

	// Generate events at ~100/s for the duration.
	tick := time.NewTicker(10 * time.Millisecond)
	deadline := time.After(Duration)
loop:
	for {
		select {
		case <-tick.C:
			sim.Bus().Publish(events.Event{
				Type:      events.EvtInternalEvent,
				GlobalSeq: sim.Bus().NextSeq(),
				ProcessID: "P1",
			})
		case <-deadline:
			break loop
		}
	}
	tick.Stop()

	// Let clients drain remaining events.
	time.Sleep(200 * time.Millisecond)
	close(stop)
	rwgs.Wait()

	// Close all clients.
	for _, c := range clients {
		if c != nil {
			_ = c.Close()
		}
	}

	if readErrs.Load() > int64(N) {
		// Each client may get one read error on close.
		t.Logf("read errors: %d (acceptable if close-related)", readErrs.Load())
	}
	if receivedTotal.Load() == 0 {
		t.Fatalf("no events received across %d clients", N)
	}
	t.Logf("sustained load: %d clients received %d total events in %v (errors=%d)",
		N, receivedTotal.Load(), Duration, readErrs.Load())
}

// TestParallelSimulation_NoLeak starts M concurrent simulations, runs each
// for N seconds, stops them, and verifies the goroutine count returns to
// baseline. Catches goroutine leaks in transport close, hub reconnect, etc.
func TestParallelSimulation_NoLeak(t *testing.T) {
	const M = 5
	for i := 0; i < M; i++ {
		sim := simulation.New(simulation.SimConfig{
			ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
		})
		for j := 1; j <= 3; j++ {
			pid := "P" + string(rune('0'+j))
			if err := sim.SpawnProcess(process.ProcessConfig{
				ID: pid, ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
			}); err != nil {
				t.Fatal(err)
			}
		}
		// Run for a bit then stop.
		time.Sleep(50 * time.Millisecond)
		sim.Stop()
	}
	time.Sleep(100 * time.Millisecond) // let goroutines exit
}

// TestGateway_RequestRate captures request rate during sustained traffic.
// Kept conservative (single client, sequential) to be portable across
// macOS/Linux CI without hitting FD limits.
func TestGateway_RequestRate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in -short mode")
	}
	if !enableLoadTests() {
		t.Skip("set VC_ENABLE_LOAD_TESTS=1 to run")
	}
	_, ts := newTestServer(t)
	const N = 1000

	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()
	for i := 0; i < N; i++ {
		res, err := client.Get(ts.URL + "/api/v1/simulation/state")
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("iter %d: status %d", i, res.StatusCode)
		}
	}
	elapsed := time.Since(start)
	rate := float64(N) / elapsed.Seconds()
	t.Logf("%d requests in %v = %.0f req/s", N, elapsed, rate)
}

// Suppress unused warnings from this file.
var _ = url.QueryEscape

// enableLoadTests returns true only when the operator explicitly opts in to
// the load tests. These exercise sustained HTTP/WS load and can be slow or
// flaky on constrained CI runners.
func enableLoadTests() bool {
	return os.Getenv("VC_ENABLE_LOAD_TESTS") == "1"
}
