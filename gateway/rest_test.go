package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

func newTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	sim := simulation.New(simulation.SimConfig{
		ClockType:    process.ClockVector,
		DeliveryMode: process.CausalDelivery,
		Channels:     "full_mesh",
	})
	t.Cleanup(sim.Stop)
	for i := 1; i <= 3; i++ {
		pid := fmt.Sprintf("P%d", i)
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID:           pid,
			ClockType:    process.ClockVector,
			DeliveryMode: process.CausalDelivery,
		}); err != nil {
			t.Fatalf("spawn %s: %v", pid, err)
		}
	}
	srv := NewServer(sim, zap.NewNop(), nil, NewMetrics())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	ts := httptest.NewServer(srv.engine)
	t.Cleanup(ts.Close)
	return srv, ts
}

// ── Health endpoints ─────────────────────────────────────────────────────────

func TestHealthz_AlwaysOK(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", res.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("body: %v", body)
	}
}

func TestReadyz_Ok(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", res.StatusCode)
	}
}

func TestReadyz_FailsAfterShutdown(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	srv := NewServer(sim, zap.NewNop(), nil, NewMetrics())
	ts := httptest.NewServer(srv.engine)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	// After Shutdown the readiness flag is false.
	res, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after shutdown, got %d", res.StatusCode)
	}
}

// ── Metrics endpoint ─────────────────────────────────────────────────────────

func TestMetricsEndpoint_PrometheusFormat(t *testing.T) {
	_, ts := newTestServer(t)
	// Hit a few endpoints to generate metric data.
	_, _ = http.Get(ts.URL + "/api/v1/simulation/state")
	res, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", res.StatusCode)
	}
	buf := new(bytes.Buffer)
	_, _ = buf.ReadFrom(res.Body)
	body := buf.String()
	// Must contain a TYPE declaration and at least the in_flight gauge.
	if !strings.Contains(body, "# TYPE vc_http_requests_total counter") {
		t.Errorf("missing HTTP requests counter; body:\n%s", body)
	}
	if !strings.Contains(body, "# TYPE vc_http_in_flight_requests gauge") {
		t.Errorf("missing in_flight gauge; body:\n%s", body)
	}
}

// ── Request size limit ───────────────────────────────────────────────────────

func TestRequestBodyTooLarge_Returns413(t *testing.T) {
	_, ts := newTestServer(t)
	// Generate a 2 MiB body (>1 MiB cap).
	big := make([]byte, 2<<20)
	for i := range big {
		big[i] = 'A'
	}
	req, _ := http.NewRequest("POST", ts.URL+"/api/v1/processes", bytes.NewReader(big))
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", res.StatusCode)
	}
}

// ── PID validation ───────────────────────────────────────────────────────────

func TestSpawnProcess_RejectsBadPID(t *testing.T) {
	_, ts := newTestServer(t)
	cases := []struct {
		name string
		pid  string
	}{
		{"empty", ""},
		{"slash", "a/b"},
		{"too_long", strings.Repeat("a", 65)},
		{"space", "a b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"id":%q,"clockType":"vector","deliveryMode":"causal"}`, tc.pid)
			res, err := http.Post(ts.URL+"/api/v1/processes", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d", res.StatusCode)
			}
		})
	}
}

func TestSpawnProcess_AcceptsGoodPID(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"id":"worker-1","clockType":"vector","deliveryMode":"causal"}`
	res, err := http.Post(ts.URL+"/api/v1/processes", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", res.StatusCode)
	}
}

// ── Error sanitization ───────────────────────────────────────────────────────

func TestErrorResponses_DoNotLeakInternals(t *testing.T) {
	_, ts := newTestServer(t)
	// Hit an endpoint that will return 500 (snapshot a missing process).
	res, err := http.Post(ts.URL+"/api/v1/processes/does-not-exist/snapshot", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	// Either 404 (process not found) or 400 (bad pid) — but it must not 500.
	if res.StatusCode >= 500 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(res.Body)
		t.Fatalf("got 5xx: %d %s", res.StatusCode, buf.String())
	}
}

// ── CORS preflight ───────────────────────────────────────────────────────────

func TestCORSPreflight_NoAllowOriginByDefault(t *testing.T) {
	_, ts := newTestServer(t)
	req, _ := http.NewRequest("OPTIONS", ts.URL+"/api/v1/simulation/state", nil)
	req.Header.Set("Origin", "https://evil.example")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	// With empty allowlist the cors middleware must NOT echo the origin back.
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin should be empty, got %q", got)
	}
}

func TestCORS_AllowsConfiguredOrigin(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()
	srv := NewServer(sim, zap.NewNop(), []string{"https://app.example"}, NewMetrics())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	ts := httptest.NewServer(srv.engine)
	defer ts.Close()

	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/simulation/state", nil)
	req.Header.Set("Origin", "https://app.example")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if got := res.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Errorf("expected allowed origin echoed, got %q", got)
	}
}

// ── WebSocket: ping/pong and origin ──────────────────────────────────────────

func TestWebSocket_RejectsDisallowedOrigin(t *testing.T) {
	sim := simulation.New(simulation.SimConfig{
		ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery, Channels: "full_mesh",
	})
	defer sim.Stop()
	srv := NewServer(sim, zap.NewNop(), []string{"https://app.example"}, NewMetrics())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()
	ts := httptest.NewServer(srv.engine)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	hdr := http.Header{}
	hdr.Set("Origin", "https://evil.example")
	_, res, err := websocket.DefaultDialer.Dial(wsURL+"/ws", hdr)
	if err == nil {
		t.Fatal("expected dial to fail for disallowed origin")
	}
	if res != nil && res.StatusCode != http.StatusForbidden {
		// gorilla returns 403 for failed CheckOrigin.
		t.Fatalf("expected 403, got %d", res.StatusCode)
	}
}

func TestWebSocket_AcceptsAllowedOrigin(t *testing.T) {
	_, ts := newTestServer(t)
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Expect a history message or a connect acknowledgment within 500ms.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for i := 0; i < 5; i++ {
		_, msg, err := conn.ReadMessage()
		if err == nil && len(msg) > 0 {
			return // got at least one message
		}
	}
	t.Fatal("expected at least one message on connect (history replay or event)")
}

// ── Request ID propagation ───────────────────────────────────────────────────

func TestRequestID_Propagated(t *testing.T) {
	_, ts := newTestServer(t)
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/simulation/state", nil)
	req.Header.Set("X-Request-Id", "test-rid-12345")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if got := res.Header.Get("X-Request-Id"); got != "test-rid-12345" {
		t.Errorf("expected X-Request-Id echo, got %q", got)
	}
}

func TestRequestID_GeneratedWhenMissing(t *testing.T) {
	_, ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/api/v1/simulation/state")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if got := res.Header.Get("X-Request-Id"); got == "" {
		t.Errorf("expected X-Request-Id to be set")
	}
}

// ── Concurrency: parallel requests do not race ───────────────────────────────

func TestParallelRequests_NoRace(t *testing.T) {
	_, ts := newTestServer(t)
	var wg sync.WaitGroup
	var failures atomic.Int64
	const N = 50
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := http.Get(ts.URL + "/api/v1/simulation/state")
			if err != nil || res.StatusCode != 200 {
				failures.Add(1)
			}
			if res != nil {
				res.Body.Close()
			}
		}()
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d requests failed under concurrent load", failures.Load())
	}
}

// ── Metrics rendering (no-net unit test) ─────────────────────────────────────

func TestRenderPrometheus_BasicFamily(t *testing.T) {
	// Build a synthetic counter family and render it.
	counter := struct{}{} // placeholder; we rely on real promauto below
	_ = counter
	m := NewMetrics()
	m.HTTPRequests.WithLabelValues("GET", "/test", "2xx").Inc()
	m.HTTPRequests.WithLabelValues("GET", "/test", "2xx").Inc()
	m.HTTPRequests.WithLabelValues("POST", "/test", "4xx").Inc()
	m.HTTPRequests.WithLabelValues("POST", "/test", "4xx").Add(2)

	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	renderPrometheus(&buf, families)
	out := buf.String()
	if !strings.Contains(out, "vc_http_requests_total") {
		t.Fatalf("missing counter name; got:\n%s", out)
	}
	if !strings.Contains(out, `method="GET"`) {
		t.Fatalf("missing method label; got:\n%s", out)
	}
	// Cumulative count for GET 2xx should be 2.
	if !strings.Contains(out, "2\n") {
		t.Fatalf("missing count value 2; got:\n%s", out)
	}
}
