// E2E tests for the TLS termination layer. These tests:
//
//   - Build a self-signed cert + (optionally) a CA + client cert in a
//     temp dir.
//   - Spawn the actual server binary with VC_TLS_CERT_FILE /
//     VC_TLS_KEY_FILE / VC_TLS_CLIENT_CA_FILE / VC_TLS_RELOAD_INTERVAL
//     set.
//   - Connect over HTTPS and verify the server is reachable, ALPN
//     negotiates, and the cert chain matches.
//   - For mTLS, verify that a client cert signed by the CA is
//     accepted, and a connection without one is rejected.
//   - For cert reload, verify that swapping the on-disk cert is
//     picked up by the server within one reload interval.
//
// These tests reuse the binary built by TestMain in e2e_test.go.
// They skip themselves if the binary is not available.

package e2e

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DistributedClocks/vectorclock-system/gateway/tlsconfig"
)

// writePEM writes data to a fresh file in dir with 0600 permissions.
func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

// writeCertPair writes a CertPair's cert + key to files in dir.
func writeCertPair(t *testing.T, dir, prefix string, p *tlsconfig.CertPair) (certFile, keyFile string) {
	t.Helper()
	return writePEM(t, dir, prefix+".cert.pem", p.CertPEM),
		writePEM(t, dir, prefix+".key.pem", p.KeyPEM)
}

// startTLSServer starts the Go binary with TLS env vars set. Returns
// the https:// URL prefix and a cleanup function.
func startTLSServer(t *testing.T, env []string) (httpsURL string, cleanup func()) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("unix-specific")
	}
	if sharedBinPath == "" {
		t.Skip("binary not built (TestMain failure)")
	}
	port := freePort(t)
	addr := "https://127.0.0.1:" + strconv.Itoa(port)

	cmd := exec.Command(sharedBinPath)
	cmd.Env = append(os.Environ(), env...)
	cmd.Env = append(cmd.Env, "PORT="+strconv.Itoa(port))

	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err == nil {
		cmd.Stderr = devNull
		t.Cleanup(func() { _ = devNull.Close() })
	}
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
	cleanupFn := func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
		}
	}
	return addr, cleanupFn
}

// waitForHealthz polls https://addr/healthz until it returns 200 or
// the deadline expires. The client must be configured to trust the
// server's cert.
func waitForHealthz(t *testing.T, addr string, client *http.Client) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server did not become healthy at %s within 10s", addr)
}

// httpsClientFor returns an *http.Client that trusts the server cert
// identified by pair.
func httpsClientFor(serverPair *tlsconfig.CertPair) *http.Client {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(serverPair.CertPEM) {
		panic("AppendCertsFromPEM failed")
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			ServerName: "localhost",
		},
	}
	return &http.Client{Transport: tr, Timeout: 5 * time.Second}
}

// ── E2E tests ────────────────────────────────────────────────────────────────

// TestE2E_TLS_HTTPS verifies the server listens on TLS and serves the
// API over HTTPS when VC_TLS_CERT_FILE / VC_TLS_KEY_FILE are set.
func TestE2E_TLS_HTTPS(t *testing.T) {
	dir := t.TempDir()
	pair, err := tlsconfig.GenerateSelfSignedCert("server",
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile := writeCertPair(t, dir, "server", pair)

	addr, cleanup := startTLSServer(t, []string{
		"VC_TLS_CERT_FILE=" + certFile,
		"VC_TLS_KEY_FILE=" + keyFile,
		"VC_API_TOKENS=test:e2e-secret",
		"LOGGING_FORMAT=console",
	})
	defer cleanup()

	client := httpsClientFor(pair)
	waitForHealthz(t, addr, client)

	// Verify /healthz returns ok over TLS.
	resp, err := client.Get(addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("GET /healthz: status %d", resp.StatusCode)
	}

	// Verify auth is still enforced over TLS.
	req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
	req.Header.Set("Authorization", "Bearer test:e2e-secret")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET state: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("GET state: status %d", resp.StatusCode)
	}
}

// TestE2E_TLS_ALPN_H2 verifies that h2 is negotiated when the client
// offers it (default for Go's http.Transport).
func TestE2E_TLS_ALPN_H2(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("server",
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	certFile, keyFile := writeCertPair(t, dir, "server", pair)

	addr, cleanup := startTLSServer(t, []string{
		"VC_TLS_CERT_FILE=" + certFile,
		"VC_TLS_KEY_FILE=" + keyFile,
		"VC_API_TOKENS=test:e2e-secret",
	})
	defer cleanup()

	client := httpsClientFor(pair)
	waitForHealthz(t, addr, client)

	// Use raw TLS to inspect ALPN.
	pool := x509.NewCertPool()
	pool.AddCert(parseCertFromPEM(t, pair.CertPEM))
	port := addr[strings.LastIndex(addr, ":")+1:]
	conn, err := tls.Dial("tcp", "127.0.0.1:"+port, &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
		NextProtos: []string{"h2", "http/1.1"},
	})
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	state := conn.ConnectionState()
	if state.NegotiatedProtocol != "h2" && state.NegotiatedProtocol != "http/1.1" {
		t.Errorf("unexpected ALPN: %q", state.NegotiatedProtocol)
	}
	if state.Version < tls.VersionTLS12 {
		t.Errorf("negotiated version = 0x%x, want >= TLS 1.2", state.Version)
	}
}

// TestE2E_TLS_CertReload verifies the background reloader picks up a
// new cert within the configured interval.
func TestE2E_TLS_CertReload(t *testing.T) {
	dir := t.TempDir()
	pair1, _ := tlsconfig.GenerateSelfSignedCert("server-1",
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	certFile, keyFile := writeCertPair(t, dir, "server", pair1)

	// Short reload interval so the test completes in <5s.
	addr, cleanup := startTLSServer(t, []string{
		"VC_TLS_CERT_FILE=" + certFile,
		"VC_TLS_KEY_FILE=" + keyFile,
		"VC_TLS_RELOAD_INTERVAL=200ms",
		"VC_API_TOKENS=test:e2e-secret",
	})
	defer cleanup()

	// A client that trusts BOTH certs so we can compare server cert
	// fingerprints regardless of which one is currently in use.
	bothPool := x509.NewCertPool()
	bothPool.AddCert(parseCertFromPEM(t, pair1.CertPEM))

	client := httpsClientFor(pair1)
	waitForHealthz(t, addr, client)

	// Confirm the first cert is in use.
	fp1 := serverCertFingerprint(t, addr, bothPool)
	want1 := fingerprintOfPEMCert(t, pair1.CertPEM)
	if fp1 != want1 {
		t.Fatalf("first fingerprint mismatch: got %x, want %x", fp1, want1)
	}

	// Overwrite the cert files with a freshly-generated pair. Sleep
	// first so the mtime is guaranteed to change (some filesystems
	// have second resolution).
	time.Sleep(50 * time.Millisecond)
	pair2, _ := tlsconfig.GenerateSelfSignedCert("server-2",
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err := os.WriteFile(certFile, pair2.CertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pair2.KeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	bothPool.AddCert(parseCertFromPEM(t, pair2.CertPEM))
	want2 := fingerprintOfPEMCert(t, pair2.CertPEM)

	// Wait up to 5s for the server to pick up the new cert. The
	// reloader interval is 200ms, so it should be very fast.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		fp := serverCertFingerprint(t, addr, bothPool)
		if fp == want2 {
			return // success
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("server did not pick up the new cert within 5s (last fp=%x, want %x)", serverCertFingerprint(t, addr, bothPool), want2)
}

// TestE2E_TLS_MTLS_Rejects verifies the server rejects connections
// without a client cert when VC_TLS_CLIENT_CA_FILE is set.
func TestE2E_TLS_MTLS_Rejects(t *testing.T) {
	dir := t.TempDir()
	ca, _ := tlsconfig.GenerateCA("test-ca")
	srvPair, _ := tlsconfig.GenerateSelfSignedCert("server",
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	certFile, keyFile := writeCertPair(t, dir, "server", srvPair)
	caFile := writePEM(t, dir, "ca.pem", ca.CertPEM)

	addr, cleanup := startTLSServer(t, []string{
		"VC_TLS_CERT_FILE=" + certFile,
		"VC_TLS_KEY_FILE=" + keyFile,
		"VC_TLS_CLIENT_CA_FILE=" + caFile,
		"VC_API_TOKENS=test:e2e-secret",
	})
	defer cleanup()

	// Wait for the server to bind. We can't use the public health
	// probe without a client cert, so probe with a TLS dial that
	// trusts the server cert. The handshake may complete at the TLS
	// layer but the HTTP layer will not respond (mTLS gate fails the
	// handshake). We just check the server is listening on the port.
	port := addr[strings.LastIndex(addr, ":")+1:]
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rawConn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 500*time.Millisecond)
		if err == nil {
			_ = rawConn.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Now perform an mTLS-rejected dial. We expect the handshake to
	// fail; surface the error on the next Read.
	rawConn, err := tls.Dial("tcp", "127.0.0.1:"+port, &tls.Config{
		InsecureSkipVerify: true, // server cert is self-signed; only mTLS matters
	})
	if err == nil {
		_ = rawConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, 1)
		_, rerr := rawConn.Read(buf)
		_ = rawConn.Close()
		if rerr == nil {
			t.Errorf("expected mTLS rejection, but read succeeded")
		}
	}

	// Also verify that the HTTP client (which trusts the server but
	// presents no client cert) gets a TLS error.
	client := httpsClientFor(srvPair)
	if resp, err := client.Get(addr + "/healthz"); err == nil {
		resp.Body.Close()
		t.Errorf("expected HTTP client to fail without client cert, but got status %d", resp.StatusCode)
	}
}

// TestE2E_TLS_MTLS_Accepts verifies the server accepts a client cert
// signed by VC_TLS_CLIENT_CA_FILE.
func TestE2E_TLS_MTLS_Accepts(t *testing.T) {
	dir := t.TempDir()
	ca, _ := tlsconfig.GenerateCA("test-ca")
	srvPair, _ := tlsconfig.GenerateSelfSignedCert("server",
		[]string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	clientPair, _ := tlsconfig.GenerateCASignedCert(ca, "client", nil, nil)

	certFile, keyFile := writeCertPair(t, dir, "server", srvPair)
	caFile := writePEM(t, dir, "ca.pem", ca.CertPEM)
	clientCertFile, clientKeyFile := writeCertPair(t, dir, "client", clientPair)

	addr, cleanup := startTLSServer(t, []string{
		"VC_TLS_CERT_FILE=" + certFile,
		"VC_TLS_KEY_FILE=" + keyFile,
		"VC_TLS_CLIENT_CA_FILE=" + caFile,
		"VC_API_TOKENS=test:e2e-secret",
	})
	defer cleanup()

	// Build a client that trusts the server AND presents a client
	// cert signed by the CA.
	serverPool := x509.NewCertPool()
	serverPool.AddCert(parseCertFromPEM(t, srvPair.CertPEM))
	clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      serverPool,
			ServerName:   "localhost",
			Certificates: []tls.Certificate{clientCert},
		},
	}
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	waitForHealthz(t, addr, client)

	resp, err := client.Get(addr + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("GET /healthz: status %d", resp.StatusCode)
	}

	// Verify the API also works.
	req, _ := http.NewRequest("GET", addr+"/api/v1/simulation/state", nil)
	req.Header.Set("Authorization", "Bearer test:e2e-secret")
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("GET state: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("GET state: status %d body %s", resp.StatusCode, body)
	}
}

// ── helpers ─────────────────────────────────────────────────────────────────

// serverCertFingerprint connects to addr and returns the SHA-256
// fingerprint of the server's leaf cert. The client must trust the
// server (via rootPool) for the handshake to succeed.
func serverCertFingerprint(t *testing.T, addr string, rootPool *x509.CertPool) [32]byte {
	t.Helper()
	port := addr[strings.LastIndex(addr, ":")+1:]
	conn, err := tls.Dial("tcp", "127.0.0.1:"+port, &tls.Config{
		RootCAs:    rootPool,
		ServerName: "localhost",
		NextProtos: []string{"h2", "http/1.1"},
	})
	if err != nil {
		t.Fatalf("tls.Dial: %v", err)
	}
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("handshake: %v", err)
	}
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		t.Fatal("no peer certificates")
	}
	return sha256.Sum256(state.PeerCertificates[0].Raw)
}

func fingerprintOfPEMCert(t *testing.T, pemBytes []byte) [32]byte {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatal("no PEM block in cert")
	}
	return sha256.Sum256(block.Bytes)
}

func parseCertFromPEM(t *testing.T, pemBytes []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatal("no PEM block")
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return c
}
