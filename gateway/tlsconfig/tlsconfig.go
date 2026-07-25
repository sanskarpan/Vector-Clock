// Package tlsconfig builds a hardened *tls.Config for the Vector Clock
// Lab backend. It is intentionally minimal: one Load call, one Reload
// call, one goroutine for periodic re-reads. Operators wire it into
// http.Server.TLSConfig and forget about it.
//
// Defaults match Mozilla's "intermediate" profile
// (https://wiki.mozilla.org/Security/Server_Side_TLS): TLS 1.2 minimum,
// modern AEAD suites, ALPN h2 / http/1.1 for HTTP/2 negotiation, no
// client certificates by default. Setting ClientCAFile enables mTLS:
// the CA is loaded into a *x509.CertPool and ClientAuth is set to
// RequireAndVerifyClientCert.
package tlsconfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Config owns the certificate + tls.Config used by the server.
//
// Concurrency contract:
//   - cert and lastLoaded are mutated only via atomic.Pointer.
//   - GetCertificate is called by the Go tls package once per handshake
//     and must be safe for concurrent invocation.
//   - Reload may be called from any goroutine; in particular, the
//     reloader goroutine started by StartReloader does so on a timer.
//   - The first Reload is performed by Load, so cfg.cert is always
//     non-nil after a successful Load.
type Config struct {
	certFile     string
	keyFile      string
	clientCAFile string

	cert       atomic.Pointer[tls.Certificate]
	lastLoaded atomic.Pointer[fileFingerprint]
	tlsConfig  *tls.Config

	reloaderOnce sync.Once
}

// fileFingerprint records the (size, mtime) of the cert file so Reload
// can short-circuit when the on-disk cert is unchanged.
type fileFingerprint struct {
	size  int64
	mtime time.Time
}

// Load reads the cert + key from disk and (optionally) the client CA
// pool. The returned Config has already loaded the cert once; callers
// may immediately call TLSConfig().GetCertificate.
//
//	clientCAFile "" → no client cert verification (plain TLS).
//	clientCAFile "ca.pem" → RequireAndVerifyClientCert against the pool.
func Load(certFile, keyFile, clientCAFile string) (*Config, error) {
	if certFile == "" {
		return nil, fmt.Errorf("tlsconfig: cert file is required")
	}
	if keyFile == "" {
		return nil, fmt.Errorf("tlsconfig: key file is required")
	}
	c := &Config{
		certFile:     certFile,
		keyFile:      keyFile,
		clientCAFile: clientCAFile,
		tlsConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			NextProtos: []string{"h2", "http/1.1"},
		},
	}
	if err := c.Reload(); err != nil {
		return nil, err
	}
	if clientCAFile != "" {
		pool, err := loadClientCAPool(clientCAFile)
		if err != nil {
			return nil, err
		}
		c.tlsConfig.ClientCAs = pool
		c.tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	// Set the GetCertificate callback LAST so it always points at a
	// fully-initialised Config.
	c.tlsConfig.GetCertificate = c.getCertificate
	return c, nil
}

// TLSConfig returns the *tls.Config to pass into http.Server.TLSConfig.
// The returned value shares state with the Config (via the
// GetCertificate closure); do not mutate it after handing it to the
// http.Server, and do not let the Config be garbage-collected while the
// server is running.
func (c *Config) TLSConfig() *tls.Config {
	return c.tlsConfig
}

// getCertificate is the tls.Config.GetCertificate callback. It returns
// the latest certificate atomically.
func (c *Config) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	cert := c.cert.Load()
	if cert == nil {
		return nil, fmt.Errorf("tlsconfig: no certificate loaded")
	}
	return cert, nil
}

// Reload re-reads the cert + key files from disk and updates the
// in-memory certificate atomically. If the on-disk file is unchanged
// (by size + mtime fingerprint), Reload is a no-op. If the new cert
// fails to parse, the previously-loaded cert is preserved.
//
// Reload is safe to call from any goroutine. Callers that want
// periodic refresh should use StartReloader.
func (c *Config) Reload() error {
	cert, err := tls.LoadX509KeyPair(c.certFile, c.keyFile)
	if err != nil {
		return fmt.Errorf("tlsconfig: load %s,%s: %w", c.certFile, c.keyFile, err)
	}
	if leaf, perr := x509.ParseCertificate(cert.Certificate[0]); perr == nil {
		// Setting Leaf enables SNI + chain validation optimisations in
		// the Go tls package. We swallow the error because the cert is
		// already validated by LoadX509KeyPair.
		cert.Leaf = leaf
	}
	fp, err := certFingerprint(c.certFile)
	if err != nil {
		return fmt.Errorf("tlsconfig: stat %s: %w", c.certFile, err)
	}
	if prev := c.lastLoaded.Load(); prev != nil && prev.size == fp.size && prev.mtime.Equal(fp.mtime) {
		return nil
	}
	c.cert.Store(&cert)
	c.lastLoaded.Store(&fp)
	return nil
}

// StartReloader launches a background goroutine that calls Reload every
// interval. The goroutine exits when ctx is cancelled. Idempotent:
// subsequent calls with the same Config are no-ops.
//
// A zero or negative interval is treated as "do not start".
func (c *Config) StartReloader(ctx context.Context, interval time.Duration, log *zap.Logger) {
	if interval <= 0 {
		return
	}
	if log == nil {
		log = zap.NewNop()
	}
	c.reloaderOnce.Do(func() {
		go c.reloaderLoop(ctx, interval, log)
	})
}

func (c *Config) reloaderLoop(ctx context.Context, interval time.Duration, log *zap.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Reload(); err != nil {
				log.Warn("tlsconfig: cert reload failed",
					zap.String("cert_file", c.certFile),
					zap.Error(err))
				continue
			}
			log.Debug("tlsconfig: cert reloaded", zap.String("cert_file", c.certFile))
		}
	}
}

// CertFile returns the configured cert path. Useful for tests + logging.
func (c *Config) CertFile() string { return c.certFile }

// ClientAuthEnabled reports whether mTLS is configured.
func (c *Config) ClientAuthEnabled() bool {
	return c.tlsConfig.ClientAuth == tls.RequireAndVerifyClientCert ||
		c.tlsConfig.ClientAuth == tls.RequestClientCert ||
		c.tlsConfig.ClientAuth == tls.VerifyClientCertIfGiven
}

// loadClientCAPool reads a PEM bundle from disk and returns it as a
// *x509.CertPool suitable for tls.Config.ClientCAs.
func loadClientCAPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- operator-controlled path
	if err != nil {
		return nil, fmt.Errorf("tlsconfig: read client CA %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("tlsconfig: no certificates in client CA file %s", path)
	}
	return pool, nil
}

// certFingerprint returns the (size, mtime) of the file at path. Used
// by Reload to detect on-disk changes without parsing the cert.
func certFingerprint(path string) (fileFingerprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return fileFingerprint{}, err
	}
	return fileFingerprint{size: info.Size(), mtime: info.ModTime()}, nil
}
