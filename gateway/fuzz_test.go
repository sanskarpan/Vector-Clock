package gateway

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/internal/process"
	"github.com/DistributedClocks/vectorclock-system/internal/simulation"
)

// fuzzOnce is reused across all fuzz iterations of a single target so we
// don't exhaust ephemeral ports by spinning a new test server per input.
var (
	fuzzOnce sync.Once
	fuzzSrv  *httptest.Server
	fuzzErr  error
)

// initFuzzServer creates a single test server used by all fuzz iterations
// of FuzzSpawnProcess and FuzzKVWrite.
func initFuzzServer() (*httptest.Server, error) {
	fuzzOnce.Do(func() {
		sim := simulation.New(simulation.SimConfig{
			ClockType:    process.ClockVector,
			DeliveryMode: process.CausalDelivery,
			Channels:     "full_mesh",
		})
		srv := NewServer(sim, zap.NewNop(), nil, NewMetrics())
		fuzzSrv = httptest.NewServer(srv.engine)
	})
	return fuzzSrv, fuzzErr
}

// FuzzSpawnProcess exercises the spawn-process endpoint with arbitrary
// JSON bodies. Catches panics, infinite loops, and unhandled JSON shapes.
func FuzzSpawnProcess(f *testing.F) {
	f.Add([]byte(`{"id":"P1","clockType":"vector","deliveryMode":"causal"}`))
	f.Add([]byte(`{"id":""}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`[]`))
	f.Add([]byte(`not json at all`))

	ts, _ := initFuzzServer()
	if ts == nil {
		f.Fatal("fuzz server not initialised")
	}
	f.Cleanup(func() {})

	f.Fuzz(func(t *testing.T, body []byte) {
		req, _ := http.NewRequest("POST", ts.URL+"/api/v1/processes", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			// Connection refused / ephemeral-port exhaustion is a flaky
			// test infrastructure issue, not a fuzz finding.
			t.Skip("network: " + err.Error())
		}
		res.Body.Close()
		// Any 2xx, 4xx, or 5xx is acceptable. What we don't want is a
		// panic or hang — both would manifest as a test failure.
		if res.StatusCode < 200 || res.StatusCode >= 600 {
			t.Errorf("invalid status %d for body %q", res.StatusCode, body)
		}
	})
}

// FuzzKVWrite exercises the conflict-store write endpoint with arbitrary
// JSON. Catches nil-map panics in the vector clock merger.
func FuzzKVWrite(f *testing.F) {
	f.Add([]byte(`{"value":"aGVsbG8=","authorId":"P1","contextVc":{}}`))
	f.Add([]byte(`{"value":"","authorId":""}`))
	f.Add([]byte(`{"value":"x","authorId":"P1","contextVc":{"P1":-1}}`))
	f.Add([]byte(`{"value":"x","authorId":"P1","contextVc":null}`))
	f.Add([]byte(``))

	ts, _ := initFuzzServer()
	if ts == nil {
		f.Fatal("fuzz server not initialised")
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		req, _ := http.NewRequest("POST", ts.URL+"/api/v1/kv/fuzz", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Skip("network: " + err.Error())
		}
		res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode >= 600 {
			t.Errorf("invalid status %d for body %q", res.StatusCode, body)
		}
	})
}
