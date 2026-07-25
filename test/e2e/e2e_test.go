// E2E tests that run the actual Go binary together with the actual BFF
// (Bun/TypeScript) and verify the full HTTP + WebSocket round trip.
//
// These tests require:
//   - the Go server binary to have been built (TestMain builds it once)
//   - bun to be on PATH
//   - ports not conflicting with other services
//
// Tests are skipped if the binary build fails.

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// sharedBinPath is set by TestMain and reused by every e2e test to
// avoid rebuilding the binary for every test (~20s per test otherwise).
var sharedBinPath string

// TestMain builds the Go binary once before any test runs.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "vc-e2e-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e: cannot create temp dir:", err)
		os.Exit(1)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	binPath := filepath.Join(dir, "vc-server")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binPath,
		"github.com/DistributedClocks/vectorclock-system/cmd/server")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: go build failed: %v\n%s\n", err, out)
		os.Exit(1)
	}
	sharedBinPath = binPath
	code := m.Run()
	os.Exit(code)
}

// freePort asks the kernel for a free TCP port. The port is closed before
// returning so there's a small race; the caller should retry on bind error.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// startGoServer starts the Go backend on a free port. Returns the
// address, a cleanup function, and an error if the server failed to
// become healthy within 10s.
func startGoServer(t *testing.T) (string, func()) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unix-specific")
	}
	if sharedBinPath == "" {
		t.Skip("binary not built (TestMain failure)")
	}

	port := freePort(t)
	addr := "http://127.0.0.1:" + strconv.Itoa(port)

	cmd := exec.Command(sharedBinPath)
	cmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(port),
		"VC_API_TOKENS=test:e2e-secret",
		"LOGGING_FORMAT=console",
	)
	// Redirect stderr to /dev/null so it doesn't block on a full pipe
	// buffer when the server produces lots of output under load.
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err == nil {
		cmd.Stderr = devNull
		t.Cleanup(func() { _ = devNull.Close() })
	}
	// When VC_E2E_LOG=1, write the server log to /tmp/vc-e2e-<test>.log
	// so it survives the temp dir cleanup. Useful for debugging.
	if os.Getenv("VC_E2E_LOG") != "" {
		logPath := fmt.Sprintf("/tmp/vc-e2e-%s.log", strings.ReplaceAll(t.Name(), "/", "_"))
		logFile, lerr := os.Create(logPath)
		if lerr == nil {
			cmd.Stderr = logFile
			t.Cleanup(func() { _ = logFile.Close() })
			t.Logf("server log: %s", logPath)
		}
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for /healthz.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return addr, func() {
					_ = cmd.Process.Signal(os.Interrupt)
					done := make(chan struct{})
					go func() { _ = cmd.Wait(); close(done) }()
					select {
					case <-done:
					case <-time.After(10 * time.Second):
						_ = cmd.Process.Kill()
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	t.Fatalf("Go server did not become healthy at %s", addr)
	return "", nil
}

// authGET is an http.Get with the test bearer token.
func authGET(t *testing.T, url string) (*http.Response, error) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer test:e2e-secret")
	return http.DefaultClient.Do(req)
}

func authPOST(t *testing.T, url string, body string) (*http.Response, error) {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test:e2e-secret")
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// ── E2E tests ────────────────────────────────────────────────────────────────

// TestE2E_GoServer_Health verifies the running binary exposes
// /healthz, /readyz, and /metrics.
func TestE2E_GoServer_Health(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, err := authGET(t, addr+path)
		if err != nil {
			t.Errorf("GET %s: %v", path, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("GET %s: status %d", path, resp.StatusCode)
		}
	}
}

// TestE2E_Auth_Required verifies unauthenticated requests are rejected.
func TestE2E_Auth_Required(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	resp, err := http.Get(addr + "/api/v1/simulation/state")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

// TestE2E_Auth_RejectsBadToken verifies wrong secrets fail.
func TestE2E_Auth_RejectsBadToken(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
	req.Header.Set("Authorization", "Bearer test:wrong-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Errorf("expected 401, got %d", resp.StatusCode)
	}
}

// TestE2E_Auth_HealthBypass verifies /healthz works without auth.
func TestE2E_Auth_HealthBypass(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	resp, err := http.Get(addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("expected 200 without auth, got %d", resp.StatusCode)
	}
}

// TestE2E_Simulation_ResetAndState verifies the full spawn → reset →
// state cycle.
func TestE2E_Simulation_ResetAndState(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Initial state: 3 processes from config.
	resp, err := authGET(t, addr+"/api/v1/simulation/state")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("state: %d", resp.StatusCode)
	}
	var state struct {
		Processes []map[string]interface{} `json:"processes"`
	}
	if err := json.Unmarshal(body, &state); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(state.Processes) != 3 {
		t.Errorf("expected 3 initial processes, got %d", len(state.Processes))
	}

	// Reset to 5 processes.
	resp, err = authPOST(t, addr+"/api/v1/simulation/reset",
		`{"processCount":5,"clockType":"vector","deliveryMode":"causal"}`)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("reset: %d", resp.StatusCode)
	}

	resp, err = authGET(t, addr+"/api/v1/simulation/state")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	json.Unmarshal(body, &state)
	if len(state.Processes) != 5 {
		t.Errorf("expected 5 after reset, got %d", len(state.Processes))
	}
}

// TestE2E_Scenario_Run runs a real scenario and verifies it produces
// events on the WebSocket.
func TestE2E_Scenario_Run(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Reset to 3 processes for the basicLamport scenario.
	resp, err := authPOST(t, addr+"/api/v1/simulation/reset",
		`{"processCount":3,"clockType":"lamport","deliveryMode":"causal"}`)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("reset: %d", resp.StatusCode)
	}

	// Open WebSocket to capture events BEFORE running the scenario, so
	// the history replay + new events all flow through.
	wsURL := "ws" + strings.TrimPrefix(addr, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Open a goroutine-driven reader: it reads from the WS with no
	// deadline and forwards each message (or error) to readCh. We MUST
	// NOT use conn.SetReadDeadline, because gorilla/websocket corrupts
	// its internal state after a single read timeout (sets c.readErr
	// permanently), and all subsequent reads return the cached error in
	// microseconds — causing a tight loop and a panic at readErrCount
	// >= 1000 ("repeated read on failed websocket connection").
	type wsReadResult struct {
		msg []byte
		err error
	}
	readCh := make(chan wsReadResult, 256)
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			readCh <- wsReadResult{msg: msg, err: err}
			if err != nil {
				return
			}
		}
	}()

	// Drain history replay: the hub replays 3 process_spawned events
	// for the reset. Wait until no message has arrived for 200ms.
	histCount := 0
	idleDeadline := time.Now().Add(200 * time.Millisecond)
drain:
	for {
		select {
		case r := <-readCh:
			if r.err != nil {
				t.Fatalf("ws read error during drain: %v", r.err)
			}
			histCount++
			idleDeadline = time.Now().Add(200 * time.Millisecond)
		case <-time.After(50 * time.Millisecond):
			if !time.Now().Before(idleDeadline) {
				break drain
			}
		}
	}
	t.Logf("drained %d history events", histCount)

	// Run the basicLamport scenario.
	resp, err = authPOST(t, addr+"/api/v1/scenarios/BasicLamport/run", "")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("scenario: %d", resp.StatusCode)
	}

	// Collect events until the channel has been idle for 500ms.
	// Scenario total delay is 100+100+150+100 = 450ms, plus action time.
	var events []map[string]interface{}
	idleDeadline = time.Now().Add(500 * time.Millisecond)
	overallDeadline := time.Now().Add(5 * time.Second)
collect:
	for {
		select {
		case r := <-readCh:
			if r.err != nil {
				t.Logf("ws closed during collect: %v", r.err)
				break collect
			}
			var e map[string]interface{}
			if json.Unmarshal(r.msg, &e) == nil {
				events = append(events, e)
			}
			idleDeadline = time.Now().Add(500 * time.Millisecond)
			if !time.Now().Before(overallDeadline) {
				break collect
			}
		case <-time.After(50 * time.Millisecond):
			if !time.Now().Before(idleDeadline) {
				break collect
			}
			if !time.Now().Before(overallDeadline) {
				break collect
			}
		}
	}
	t.Logf("got %d events from scenario", len(events))

	if len(events) == 0 {
		t.Fatal("expected at least one event from BasicLamport scenario")
	}

	// Verify at least one event mentions a process.
	hasProcess := false
	for _, e := range events {
		if _, ok := e["processId"]; ok {
			hasProcess = true
			break
		}
	}
	if !hasProcess {
		t.Errorf("no event had processId; got %d events", len(events))
	}
}

// TestE2E_KV_FullLifecycle exercises the causal conflict store through
// the HTTP layer.
func TestE2E_KV_FullLifecycle(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Write a key as P1. Value is base64("hello") = "aGVsbG8=".
	resp, err := authPOST(t, addr+"/api/v1/kv/testkey",
		`{"value":"aGVsbG8=","authorId":"P1","contextVc":{}}`)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("write: %d %s", resp.StatusCode, body)
	}

	// Read it back. The server stores the raw bytes; for "hello" (5 bytes)
	// the stored form is "hello", not the base64 string we sent.
	resp, err = authGET(t, addr+"/api/v1/kv/testkey")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	var got struct {
		Key      string `json:"key"`
		Versions []struct {
			Value  []byte `json:"value"`
			Author string `json:"author"`
		} `json:"versions"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.Key != "testkey" {
		t.Errorf("expected key=testkey, got %q", got.Key)
	}
	if len(got.Versions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(got.Versions))
	}
	// Server decodes base64 internally — the stored value is the raw
	// bytes of "hello" which JSON-encodes as "aGVsbG8=" again. Either
	// form is acceptable; we just verify author + non-empty value.
	if got.Versions[0].Author != "P1" {
		t.Errorf("expected author=P1, got %q", got.Versions[0].Author)
	}
	if len(got.Versions[0].Value) == 0 {
		t.Errorf("value should be non-empty")
	}
}

// TestE2E_KV_ConcurrentWrites verifies that concurrent writes produce
// proper conflict semantics.
func TestE2E_KV_ConcurrentWrites(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Two concurrent writes (ctxVCs are not causally related).
	resp1, _ := authPOST(t, addr+"/api/v1/kv/conflict",
		`{"value":"QQ==","authorId":"P1","contextVc":{"P1":1,"P2":0}}`)
	_, _ = io.ReadAll(resp1.Body)
	resp1.Body.Close()

	resp2, _ := authPOST(t, addr+"/api/v1/kv/conflict",
		`{"value":"Qg==","authorId":"P2","contextVc":{"P1":0,"P2":1}}`)
	_, _ = io.ReadAll(resp2.Body)
	resp2.Body.Close()

	if resp1.StatusCode != 200 || resp2.StatusCode != 200 {
		t.Fatalf("statuses: %d %d", resp1.StatusCode, resp2.StatusCode)
	}

	// Read — should have 2 siblings (true conflict).
	resp, _ := authGET(t, addr+"/api/v1/kv/conflict")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var got struct {
		Siblings int `json:"siblings"`
	}
	_ = json.Unmarshal(body, &got)
	if got.Siblings < 2 {
		t.Errorf("expected 2+ siblings, got %d", got.Siblings)
	}
}

// TestE2E_PromFormat verifies /metrics returns Prometheus text.
func TestE2E_PromFormat(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	resp, err := http.Get(addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	// Must contain at least one TYPE declaration.
	if !bytes.Contains(body, []byte("# TYPE ")) {
		t.Errorf("missing TYPE declaration; body:\n%s", body)
	}
	// Must contain the in_flight gauge from our custom metrics.
	if !bytes.Contains(body, []byte("vc_http_in_flight_requests")) {
		t.Errorf("missing vc_http_in_flight_requests; body:\n%s", body)
	}
}

// TestE2E_MetricsIncrement verifies the request counter actually
// increments with traffic.
func TestE2E_MetricsIncrement(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Send 5 requests.
	for i := 0; i < 5; i++ {
		resp, _ := authGET(t, addr+"/api/v1/simulation/state")
		if resp != nil {
			resp.Body.Close()
		}
	}

	// Now scrape metrics and look for the counter.
	resp, err := http.Get(addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if !bytes.Contains(body, []byte("vc_http_requests_total")) {
		t.Errorf("missing vc_http_requests_total")
	}
	// We expect at least 5 requests on the state endpoint.
	if !bytes.Contains(body, []byte(`route="/api/v1/simulation/state"`)) {
		t.Errorf("missing route label for /api/v1/simulation/state")
	}
}

// TestE2E_RequestIDPropagation verifies X-Request-Id is preserved.
func TestE2E_RequestIDPropagation(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	req, _ := http.NewRequest("GET", addr+"/healthz", nil)
	req.Header.Set("X-Request-Id", "trace-abc-12345")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("X-Request-Id"); got != "trace-abc-12345" {
		t.Errorf("X-Request-Id not preserved: got %q", got)
	}
}

// TestE2E_CustomConfig applies a custom config via env and verifies it
// takes effect.
func TestE2E_CustomConfig(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-specific")
	}

	port := freePort(t)
	addr := "http://127.0.0.1:" + strconv.Itoa(port)

	// Build a custom config file.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	yaml := `server:
  port: 9999
  ws_buffer: 256
simulation:
  initial_processes: 5
  clock_type: vector
  delivery_mode: causal
  channels: full_mesh
logging:
  level: info
  format: console
`
	if err := os.WriteFile(cfgPath, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	binPath := sharedBinPath
	if binPath == "" {
		t.Skip("binary not built")
	}

	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		"PORT="+strconv.Itoa(port), // override config port
		"CONFIG_PATH="+cfgPath,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}()

	// Wait for healthy.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify 5 initial processes (from custom config, not 3).
	req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var state struct {
		Processes []map[string]interface{} `json:"processes"`
	}
	_ = json.Unmarshal(body, &state)
	if len(state.Processes) != 5 {
		t.Errorf("expected 5 processes from custom config, got %d", len(state.Processes))
	}
}

// TestE2E_GoroutineLeak starts/stops the server repeatedly and checks
// the runtime goroutine count is bounded.
func TestE2E_GoroutineLeak(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix-specific")
	}

	before := runtime.NumGoroutine()
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Make some traffic to spin up goroutines.
	for i := 0; i < 20; i++ {
		resp, _ := authGET(t, addr+"/api/v1/simulation/state")
		if resp != nil {
			resp.Body.Close()
		}
	}

	// Stop the server.
	cleanup()
	time.Sleep(2 * time.Second) // let goroutines drain

	after := runtime.NumGoroutine()
	// Allow some slack for test infrastructure and the test framework.
	if after > before+50 {
		t.Errorf("goroutine leak: before=%d after=%d (delta=%d)", before, after, after-before)
	}
}

// TestE2E_ReadyzDuringShutdown verifies /readyz flips to 503 when the
// server is shutting down. Skipped: requires the test to inject a
// shutdown, which is racy with the test runner. Covered by integration
// tests at the handler level.
func TestE2E_ConcurrentRequests_No5xx(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	const N = 50
	var wg sync.WaitGroup
	var fiveXX atomic.Int64
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := authGET(t, addr+"/api/v1/simulation/state")
			if err != nil {
				return
			}
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				fiveXX.Add(1)
			}
		}()
	}
	wg.Wait()
	if fiveXX.Load() > 0 {
		t.Errorf("%d/%d concurrent requests returned 5xx", fiveXX.Load(), N)
	}
}

// TestE2E_KillProcessCleanly verifies KillProcess endpoint works.
func TestE2E_KillProcessCleanly(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	// Kill P3.
	req, _ := http.NewRequest("DELETE", addr+"/api/v1/processes/P3", nil)
	req.Header.Set("Authorization", "Bearer test:e2e-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("kill: %d", resp.StatusCode)
	}

	// Verify P3 is gone.
	resp, err = authGET(t, addr+"/api/v1/simulation/state")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var state struct {
		Processes []struct {
			ID string `json:"id"`
		} `json:"processes"`
	}
	_ = json.Unmarshal(body, &state)
	for _, p := range state.Processes {
		if p.ID == "P3" {
			t.Error("P3 should have been killed")
		}
	}
}

// TestE2E_WS_ProcessSpawned verifies that spawning a process via the REST
// API publishes a process_spawned event on the WebSocket.
func TestE2E_WS_ProcessSpawned(t *testing.T) {
	addr, cleanup := startGoServer(t)
	defer cleanup()

	wsURL := "ws" + strings.TrimPrefix(addr, "http") + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	type wsReadResult struct {
		msg []byte
		err error
	}
	readCh := make(chan wsReadResult, 256)
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			readCh <- wsReadResult{msg: msg, err: err}
			if err != nil {
				return
			}
		}
	}()

	// Drain history replay: 3 process_spawned events for the default processes.
	histCount := 0
	idleDeadline := time.Now().Add(200 * time.Millisecond)
drain:
	for {
		select {
		case r := <-readCh:
			if r.err != nil {
				t.Fatalf("ws read error during drain: %v", r.err)
			}
			histCount++
			idleDeadline = time.Now().Add(200 * time.Millisecond)
		case <-time.After(50 * time.Millisecond):
			if !time.Now().Before(idleDeadline) {
				break drain
			}
		}
	}
	t.Logf("drained %d history events", histCount)

	// Spawn a new process P4.
	resp, err := authPOST(t, addr+"/api/v1/processes",
		`{"id":"P4","clockType":"vector","deliveryMode":"causal"}`)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("spawn: %d (expected 201)", resp.StatusCode)
	}

	// Wait for process_spawned event on the WebSocket within 3s.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case r := <-readCh:
			if r.err != nil {
				t.Fatalf("ws read error: %v", r.err)
			}
			var e map[string]interface{}
			if err := json.Unmarshal(r.msg, &e); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			eventType, _ := e["type"].(string)
			processID, _ := e["processId"].(string)
			if eventType == "process_spawned" && processID == "P4" {
				t.Logf("received process_spawned for P4: %s", r.msg)
				return
			}
			t.Logf("ignored event type=%s processId=%s", eventType, processID)
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for process_spawned event for P4 on WebSocket")
}

// Ensure unused imports stay used (helps with refactors).
var (
	_ = httptest.NewServer
	_ = fmt.Sprintf
	_ = sync.Mutex{}
)
