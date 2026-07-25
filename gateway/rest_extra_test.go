package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

func TestScenario_List(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/scenarios")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
	body := res.Header.Get("Content-Type")
	if !strings.HasPrefix(body, "application/json") {
		t.Errorf("unexpected content-type: %q", body)
	}
}

func TestScenario_Run_BadName(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Post(ts.URL+"/api/v1/scenarios/no-such-scenario/run", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
}

func TestScenario_Run_RealScenario(t *testing.T) {
	// Use a fresh server so the initial processes match what the scenario
	// expects (P1, P2, P3).
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()
	srv := NewServer(sim, zap.NewNop(), nil, NewMetrics())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	ts := httptest.NewServer(srv.engine)
	defer ts.Close()

	// concurrentWrites scenario requires P1, P2.
	if err := sim.SpawnProcess(process.ProcessConfig{
		ID: "P1", ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
	}); err != nil {
		t.Fatal(err)
	}
	if err := sim.SpawnProcess(process.ProcessConfig{
		ID: "P2", ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
	}); err != nil {
		t.Fatal(err)
	}

	res, err := http.Post(ts.URL+"/api/v1/scenarios/ConcurrentWrites/run", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestFault_InjectAndClear(t *testing.T) {
	_, ts := newTestServer(t)
	// Inject delay.
	body := `{"from":"P1","to":"P2","delayMs":50}`
	res, err := http.Post(ts.URL+"/api/v1/faults/delay", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("delay status %d", res.StatusCode)
	}
	// Drop next.
	body = `{"from":"P1","to":"P2"}`
	res, err = http.Post(ts.URL+"/api/v1/faults/drop", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("drop status %d", res.StatusCode)
	}
	// Clear.
	req, _ := http.NewRequest("DELETE", ts.URL+"/api/v1/faults", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("clear status %d", res.StatusCode)
	}
}

func TestFault_RejectsOutOfRange(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"from":"P1","to":"P2","delayMs":-1}`
	res, err := http.Post(ts.URL+"/api/v1/faults/delay", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestSimulation_Start_RejectsBadCount(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"processCount":1000,"clockType":"vector","deliveryMode":"causal"}`
	res, err := http.Post(ts.URL+"/api/v1/simulation/start", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestSimulation_Start_RejectsBadClock(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"processCount":2,"clockType":"totally-bogus","deliveryMode":"causal"}`
	res, err := http.Post(ts.URL+"/api/v1/simulation/start", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestKV_FullLifecycle(t *testing.T) {
	_, ts := newTestServer(t)
	// Write.
	body := `{"value":"aGVsbG8=","authorId":"P1","contextVc":{}}`
	res, err := http.Post(ts.URL+"/api/v1/kv/test", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("write status %d", res.StatusCode)
	}
	// Read.
	res, err = http.Get(ts.URL + "/api/v1/kv/test")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("read status %d", res.StatusCode)
	}
	// Resolve.
	body = `{"strategy":"last_writer_wins"}`
	res, err = http.Post(ts.URL+"/api/v1/kv/test/resolve", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("resolve status %d", res.StatusCode)
	}
}

func TestKV_ReadNotFound(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/kv/does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
}

func TestHappenedBefore_MissingParams(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/causality/happened-before")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestHappenedBefore_Returns(t *testing.T) {
	// Even with no events, the endpoint should return a structured response.
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/causality/happened-before?a=x&b=y")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestKillProcess_BadID(t *testing.T) {
	_, ts := newTestServer(t)
	// A space in the ID is rejected by the PID regex. The URL must be
	// properly encoded (%20) so the route matches and our validator runs.
	req, _ := http.NewRequest("DELETE", ts.URL+"/api/v1/processes/bad%20id", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestInternalEvent_NotFound(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Post(ts.URL+"/api/v1/processes/does-not-exist/event", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
}

func TestSendMessage_RejectsBadFromTo(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"from":"","to":"P1","data":""}`
	res, err := http.Post(ts.URL+"/api/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
	body = `{"from":"P1","to":"bad/char","data":""}`
	res, err = http.Post(ts.URL+"/api/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestSimulation_Reset(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"processCount":2,"clockType":"vector","deliveryMode":"causal"}`
	res, err := http.Post(ts.URL+"/api/v1/simulation/reset", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestMetricsLegacyEndpoint(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/metrics")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestSnapshotEndpoint_NotFound(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/snapshots/no-such-id")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", res.StatusCode)
	}
}
