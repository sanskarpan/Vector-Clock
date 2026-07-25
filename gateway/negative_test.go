package gateway

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// ── Malformed JSON ───────────────────────────────────────────────────────────

func TestNegative_MalformedJSON_AllEndpoints(t *testing.T) {
	_, ts := newTestServer(t)

	cases := []struct {
		method, path, body string
	}{
		{"POST", "/api/v1/simulation/start", "{not-json"},
		{"POST", "/api/v1/processes", "{"},
		{"POST", "/api/v1/messages", `{"from":`},
		{"POST", "/api/v1/broadcast", `{"data":`},
		{"POST", "/api/v1/kv/test", `{"value":`},
		{"POST", "/api/v1/kv/test/resolve", `{"strategy":`},
		{"POST", "/api/v1/faults/delay", `{`},
		{"POST", "/api/v1/faults/drop", `not even json`},
	}

	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			res, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			if res.StatusCode < 400 || res.StatusCode >= 500 {
				t.Fatalf("expected 4xx, got %d for %s %s", res.StatusCode, tc.method, tc.path)
			}
		})
	}
}

func TestNegative_EmptyJSONBody(t *testing.T) {
	_, ts := newTestServer(t)
	// POST with empty body — handlers should respond with 400 because required
	// fields are missing.
	res, err := http.Post(ts.URL+"/api/v1/processes", "application/json", strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400, got %d", res.StatusCode)
	}
}

func TestNegative_NullValues(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"id":null,"clockType":"vector","deliveryMode":"causal"}`
	res, err := http.Post(ts.URL+"/api/v1/processes", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400 for null ID, got %d", res.StatusCode)
	}
}

// ── Unicode & special characters ────────────────────────────────────────────

func TestNegative_UnicodePID_Rejected(t *testing.T) {
	_, ts := newTestServer(t)
	cases := []string{
		`{"id":"π","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"名前","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"emoji😀","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"with space","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"with\ttab","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"with\nnewline","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"with\"quote","clockType":"vector","deliveryMode":"causal"}`,
		`{"id":"with\\backslash","clockType":"vector","deliveryMode":"causal"}`,
	}
	for _, body := range cases {
		t.Run(body, func(t *testing.T) {
			res, err := http.Post(ts.URL+"/api/v1/processes", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != 400 {
				t.Errorf("expected 400 for unicode PID, got %d", res.StatusCode)
			}
		})
	}
}

func TestNegative_AcceptsValidUnicodeInData(t *testing.T) {
	// Data field is interface{} — unicode is allowed inside.
	_, ts := newTestServer(t)
	body := `{"from":"P1","to":"P2","data":"こんにちは世界"}`
	res, err := http.Post(ts.URL+"/api/v1/messages", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("expected 200 for unicode data, got %d", res.StatusCode)
	}
}

// ── Out-of-range values ─────────────────────────────────────────────────────

func TestNegative_ProcessCount_OutOfRange(t *testing.T) {
	_, ts := newTestServer(t)
	cases := []struct {
		count int
		want  int
	}{
		{0, 400},
		{-1, 400},
		{101, 400},
		{1000, 400},
	}
	for _, tc := range cases {
		body := `{"processCount":` + itoa(tc.count) + `,"clockType":"vector","deliveryMode":"causal"}`
		res, err := http.Post(ts.URL+"/api/v1/simulation/start", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != tc.want {
			t.Errorf("count=%d: expected %d, got %d", tc.count, tc.want, res.StatusCode)
		}
	}
}

func TestNegative_FaultDelay_OutOfRange(t *testing.T) {
	_, ts := newTestServer(t)
	body := `{"from":"P1","to":"P2","delayMs":999999999}`
	res, err := http.Post(ts.URL+"/api/v1/faults/delay", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400 for delayMs > 600000, got %d", res.StatusCode)
	}
}

func TestNegative_BodySize_AtLimit(t *testing.T) {
	// Exactly at the 1 MiB limit should pass; 1 MiB + 1 should fail.
	_, ts := newTestServer(t)

	// At-limit body: a small process payload padded with non-PID field. We
	// use the KV endpoint since its value field can grow arbitrarily up to
	// the MaxJSONBodyBytes cap.
	max := strings.Repeat("a", 1<<20-200) // leave room for JSON envelope
	body := `{"value":"` + max + `","authorId":"P1","contextVc":{}}`
	if len(body) > (1 << 20) {
		t.Skip("body unexpectedly exceeds limit")
	}
	res, err := http.Post(ts.URL+"/api/v1/kv/big", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	// Should succeed (under limit).
	if res.StatusCode != 200 {
		t.Fatalf("at-limit body should succeed, got %d", res.StatusCode)
	}

	// Over-limit: 1 MiB + 1KB body.
	tooBig := strings.Repeat("a", (1<<20)+1024)
	body = `{"value":"` + tooBig + `","authorId":"P1","contextVc":{}}`
	res, err = http.Post(ts.URL+"/api/v1/kv/big", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 413 {
		t.Fatalf("over-limit body should be 413, got %d", res.StatusCode)
	}
}

func TestNegative_KVKey_TooLong(t *testing.T) {
	_, ts := newTestServer(t)
	key := strings.Repeat("a", 257)
	res, err := http.Get(ts.URL + "/api/v1/kv/" + key)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatalf("expected 400 for over-long key, got %d", res.StatusCode)
	}
}

// ── Concurrent + abusive ────────────────────────────────────────────────────

func TestNegative_HighConcurrency_NoRace(t *testing.T) {
	// 200 concurrent requests against multiple endpoints. Catches races
	// in the metrics middleware, hub, and simulation swap.
	_, ts := newTestServer(t)
	endpoints := []string{
		ts.URL + "/healthz",
		ts.URL + "/readyz",
		ts.URL + "/api/v1/simulation/state",
		ts.URL + "/api/v1/scenarios",
		ts.URL + "/metrics",
	}
	const N = 200
	done := make(chan struct{}, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			url := endpoints[i%len(endpoints)]
			res, err := http.Get(url)
			if err == nil {
				res.Body.Close()
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < N; i++ {
		<-done
	}
}

func TestNegative_RapidReset_NoCrash(t *testing.T) {
	// Hammer /simulation/reset to expose goroutine/channel leaks.
	_, ts := newTestServer(t)
	for i := 0; i < 10; i++ {
		body := `{"processCount":2,"clockType":"vector","deliveryMode":"causal"}`
		res, err := http.Post(ts.URL+"/api/v1/simulation/reset", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("iter %d: expected 200, got %d", i, res.StatusCode)
		}
	}
}

func TestNegative_RapidSpawn_NoCrash(t *testing.T) {
	// Spawn many processes with unique IDs to stress the snapshot
	// coordinator and transport goroutines.
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

	// Spawn 20 processes directly via the simulation.
	for i := 0; i < 20; i++ {
		if err := sim.SpawnProcess(process.ProcessConfig{
			ID: "P" + itoa(i+1), ClockType: process.ClockVector, DeliveryMode: process.CausalDelivery,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Sanity: state has 20 processes.
	state := sim.GetState()
	if len(state.Processes) != 20 {
		t.Fatalf("expected 20 processes, got %d", len(state.Processes))
	}
}

// ── Unauthenticated access ───────────────────────────────────────────────────
// The lab has no auth by design, but verify that endpoints are at least
// reachable without crashing on missing/empty headers.

func TestNegative_MissingAuth_NoCrash(t *testing.T) {
	_, ts := newTestServer(t)
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/simulation/state", nil)
	// No Authorization header; no cookies.
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", res.StatusCode)
	}
}

// ── HTTP method routing ─────────────────────────────────────────────────────

func TestNegative_WrongMethods(t *testing.T) {
	_, ts := newTestServer(t)
	// GET on a POST-only endpoint.
	res, err := http.Get(ts.URL + "/api/v1/simulation/start")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 405 && res.StatusCode != 404 {
		t.Fatalf("GET on POST-only endpoint: expected 405/404, got %d", res.StatusCode)
	}

	// DELETE on a GET-only endpoint.
	req, _ := http.NewRequest("DELETE", ts.URL+"/api/v1/simulation/state", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 405 && res.StatusCode != 404 {
		t.Fatalf("DELETE on GET-only endpoint: expected 405/404, got %d", res.StatusCode)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
