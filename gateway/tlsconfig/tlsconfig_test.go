// Tests for the tlsconfig package. These cover:
//   - Load: a valid cert + key is read and the *tls.Config is wired
//     with Mozilla-intermediate defaults.
//   - Load: missing files / mismatched keys fail loudly.
//   - Reload: swapping the on-disk cert is reflected in GetCertificate.
//   - Reload: no-op when the file is unchanged.
//   - Reload: parse errors preserve the previous cert.
//   - Reload concurrency: under -race, simultaneous Reload + handshake
//     are safe.
//   - mTLS: handshake without a client cert is rejected.
//   - mTLS: handshake with a CA-signed client cert is accepted.
//   - StartReloader: a background tick re-reads the cert.
package tlsconfig_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/DistributedClocks/vectorclock-system/gateway/tlsconfig"
)

// writePEM writes data to a fresh file in dir with 0600 permissions.
// Private keys are sensitive; the cert is also fine to keep private.
func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

// ── Load ─────────────────────────────────────────────────────────────────────

func TestLoad_ValidCert(t *testing.T) {
	dir := t.TempDir()
	pair, err := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg := cfg.TLSConfig()

	if tlsCfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %v, want TLS 1.2", tlsCfg.MinVersion)
	}
	if tlsCfg.GetCertificate == nil {
		t.Fatal("GetCertificate is nil")
	}
	if !contains(tlsCfg.NextProtos, "h2") {
		t.Errorf("NextProtos missing h2: %v", tlsCfg.NextProtos)
	}
	if cfg.ClientAuthEnabled() {
		t.Errorf("ClientAuthEnabled = true with empty ClientCAFile")
	}
}

func TestLoad_MissingCertFile(t *testing.T) {
	_, err := tlsconfig.Load("", "key.pem", "")
	if err == nil {
		t.Fatal("expected error for empty cert file")
	}
}

func TestLoad_MissingKeyFile(t *testing.T) {
	_, err := tlsconfig.Load("cert.pem", "", "")
	if err == nil {
		t.Fatal("expected error for empty key file")
	}
}

func TestLoad_BadCertPath(t *testing.T) {
	_, err := tlsconfig.Load("/no/such/file.pem", "/no/such/key.pem", "")
	if err == nil {
		t.Fatal("expected error for missing files")
	}
}

func TestLoad_BadClientCAPath(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, nil)
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)
	_, err := tlsconfig.Load(certFile, keyFile, "/no/such/ca.pem")
	if err == nil {
		t.Fatal("expected error for missing client CA")
	}
}

// ── Reload ───────────────────────────────────────────────────────────────────

func TestReload_UpdatesCertificate(t *testing.T) {
	dir := t.TempDir()
	pair1, err := tlsconfig.GenerateSelfSignedCert("test-1", []string{"localhost"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	certFile := writePEM(t, dir, "cert.pem", pair1.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair1.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	first, err := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}

	// Ensure mtime changes (some filesystems have second resolution).
	time.Sleep(20 * time.Millisecond)

	pair2, err := tlsconfig.GenerateSelfSignedCert("test-2", []string{"localhost"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pair2.CertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pair2.KeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := cfg.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	second, err := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatal(err)
	}
	if string(first.Certificate[0]) == string(second.Certificate[0]) {
		t.Error("certificate did not change after reload")
	}
}

func TestReload_NoChangeIsNoop(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, nil)
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})

	// Reload again with the same file. Should be a no-op.
	if err := cfg.Reload(); err != nil {
		t.Fatal(err)
	}
	second, _ := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})
	if string(first.Certificate[0]) != string(second.Certificate[0]) {
		t.Error("unexpected cert change on no-op reload")
	}
}

func TestReload_PreservesCertOnParseError(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, nil)
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})

	// Corrupt the key file. Reload must fail and the previous cert
	// must remain available.
	if err := os.WriteFile(keyFile, []byte("not a real key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Reload(); err == nil {
		t.Fatal("expected error from Reload on corrupt key")
	}
	stillThere, err := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate after failed reload: %v", err)
	}
	if string(stillThere.Certificate[0]) != string(first.Certificate[0]) {
		t.Error("previous cert was lost after failed reload")
	}
}

// ── mTLS ─────────────────────────────────────────────────────────────────────

// spinTLSServer starts a tls.Listener on 127.0.0.1 and runs an accept
// loop that performs the TLS handshake on each connection. The
// listener is closed via t.Cleanup. We do NOT wrap the listener in an
// http.Server because that would race our accept loop for connections
// and swallow handshake errors.
func spinTLSServer(t *testing.T, cfg *tlsconfig.Config) int {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg.TLSConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	// Drain the accept loop in the background so the test process
	// doesn't leak goroutines. We don't surface handshake results here
	// — the client-side assertions in each test are the source of
	// truth for mTLS / cert / ALPN behaviour.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				if tlsConn, ok := c.(*tls.Conn); ok {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					_ = tlsConn.HandshakeContext(ctx)
					cancel()
				}
				_ = c.Close()
			}(conn)
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestMTLS_RejectsConnectionWithoutClientCert(t *testing.T) {
	dir := t.TempDir()
	ca, err := tlsconfig.GenerateCA("test-ca")
	if err != nil {
		t.Fatal(err)
	}
	srvPair, err := tlsconfig.GenerateSelfSignedCert("server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	certFile := writePEM(t, dir, "cert.pem", srvPair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", srvPair.KeyPEM)
	caFile := writePEM(t, dir, "ca.pem", ca.CertPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ClientAuthEnabled() {
		t.Fatal("ClientAuthEnabled = false after Load with ClientCAFile")
	}
	port := spinTLSServer(t, cfg)

	// Connect without a client cert. The Go TLS client may return
	// HandshakeComplete=true even when the server sent a
	// "certificate_required" alert (it surfaces on the next Read).
	// We do a Read to trigger the error path.
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
		InsecureSkipVerify: true, // server cert is self-signed; only mTLS matters here
	})
	if err != nil {
		// Most direct path: dial returned the error.
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	_, rerr := conn.Read(buf)
	_ = conn.Close()
	if rerr == nil {
		t.Fatal("expected handshake to fail without client cert (no error from Read)")
	}
}

func TestMTLS_AcceptsCAClientCert(t *testing.T) {
	dir := t.TempDir()
	ca, err := tlsconfig.GenerateCA("test-ca")
	if err != nil {
		t.Fatal(err)
	}
	srvPair, err := tlsconfig.GenerateSelfSignedCert("server", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	clientPair, err := tlsconfig.GenerateCASignedCert(ca, "client", nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	certFile := writePEM(t, dir, "cert.pem", srvPair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", srvPair.KeyPEM)
	caFile := writePEM(t, dir, "ca.pem", ca.CertPEM)
	clientCertFile := writePEM(t, dir, "client-cert.pem", clientPair.CertPEM)
	clientKeyFile := writePEM(t, dir, "client-key.pem", clientPair.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, caFile)
	if err != nil {
		t.Fatal(err)
	}
	port := spinTLSServer(t, cfg)

	// Build a client cert from the PEM files.
	clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	// Trust the server's self-signed cert.
	serverCAPool := x509.NewCertPool()
	if !serverCAPool.AppendCertsFromPEM(srvPair.CertPEM) {
		t.Fatal("AppendCertsFromPEM failed")
	}

	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      serverCAPool,
	})
	if err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	state := conn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		t.Error("server presented no certificates in PeerCertificates")
	}
}

// ── Plain TLS handshake (non-mTLS) ──────────────────────────────────────────

func TestHandshake_PlainTLS(t *testing.T) {
	dir := t.TempDir()
	pair, err := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	port := spinTLSServer(t, cfg)

	// Client trusts the self-signed cert via RootCAs.
	rootPool := x509.NewCertPool()
	if !rootPool.AppendCertsFromPEM(pair.CertPEM) {
		t.Fatal("AppendCertsFromPEM failed")
	}
	conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
		RootCAs:    rootPool,
		ServerName: "localhost",
		NextProtos: []string{"h2", "http/1.1"},
	})
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	defer conn.Close()
	if err := conn.Handshake(); err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	state := conn.ConnectionState()
	if !state.HandshakeComplete {
		t.Error("HandshakeComplete = false after successful Handshake")
	}
	// Negotiated version must be TLS 1.2 or 1.3.
	if state.Version < tls.VersionTLS12 {
		t.Errorf("negotiated version = 0x%x, want >= TLS 1.2", state.Version)
	}
	// ALPN should be one of the configured NextProtos.
	if state.NegotiatedProtocol != "h2" && state.NegotiatedProtocol != "http/1.1" {
		t.Errorf("unexpected ALPN: %q", state.NegotiatedProtocol)
	}
}

// ── StartReloader ────────────────────────────────────────────────────────────

func TestStartReloader_PicksUpFileChanges(t *testing.T) {
	dir := t.TempDir()
	pair1, _ := tlsconfig.GenerateSelfSignedCert("test-1", []string{"localhost"}, nil)
	certFile := writePEM(t, dir, "cert.pem", pair1.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair1.KeyPEM)

	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	first, _ := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.StartReloader(ctx, 20*time.Millisecond, zap.NewNop())

	// Give the reloader one tick to start.
	time.Sleep(50 * time.Millisecond)

	// Overwrite with a new cert. Sleep first so mtime changes.
	time.Sleep(20 * time.Millisecond)
	pair2, _ := tlsconfig.GenerateSelfSignedCert("test-2", []string{"localhost"}, nil)
	if err := os.WriteFile(certFile, pair2.CertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pair2.KeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	// Wait up to 2s for the reloader to pick it up.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cur, _ := cfg.TLSConfig().GetCertificate(&tls.ClientHelloInfo{})
		if string(cur.Certificate[0]) != string(first.Certificate[0]) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("reloader did not pick up new cert within 2s")
}

func TestStartReloader_ZeroIntervalIsNoop(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, nil)
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)
	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	// Should not panic, not start a goroutine, not block.
	cfg.StartReloader(context.Background(), 0, zap.NewNop())
	cfg.StartReloader(context.Background(), -1, zap.NewNop())
}

func TestStartReloader_Idempotent(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, nil)
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)
	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.StartReloader(ctx, time.Hour, zap.NewNop())
	cfg.StartReloader(ctx, time.Hour, zap.NewNop())
	// If StartReloader is correctly idempotent, only one goroutine
	// was started. We don't directly observe goroutine count here, but
	// the absence of a panic / data race is the assertion.
}

// ── Concurrency stress ──────────────────────────────────────────────────────

func TestReload_ConcurrentHandshakesAndReloads(t *testing.T) {
	dir := t.TempDir()
	pair, _ := tlsconfig.GenerateSelfSignedCert("test", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	certFile := writePEM(t, dir, "cert.pem", pair.CertPEM)
	keyFile := writePEM(t, dir, "key.pem", pair.KeyPEM)
	cfg, err := tlsconfig.Load(certFile, keyFile, "")
	if err != nil {
		t.Fatal(err)
	}
	port := spinTLSServer(t, cfg)

	rootPool := x509.NewCertPool()
	if !rootPool.AppendCertsFromPEM(pair.CertPEM) {
		t.Fatal("AppendCertsFromPEM failed")
	}

	var wg sync.WaitGroup
	var stop atomic.Bool
	// 4 reader goroutines hammering the server.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				conn, err := tls.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port), &tls.Config{
					RootCAs:    rootPool,
					ServerName: "localhost",
					NextProtos: []string{"h2", "http/1.1"},
				})
				if err == nil {
					_ = conn.Close()
				}
			}
		}()
	}
	// 1 reloader goroutine swapping the cert on disk.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			time.Sleep(5 * time.Millisecond)
			newPair, _ := tlsconfig.GenerateSelfSignedCert(fmt.Sprintf("c-%d", i), []string{"localhost"}, nil)
			_ = os.WriteFile(certFile, newPair.CertPEM, 0o600)
			_ = os.WriteFile(keyFile, newPair.KeyPEM, 0o600)
			if err := cfg.Reload(); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("reload %d: %v", i, err)
				return
			}
		}
		stop.Store(true)
	}()
	wg.Wait()
}

// ── helpers ─────────────────────────────────────────────────────────────────

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
