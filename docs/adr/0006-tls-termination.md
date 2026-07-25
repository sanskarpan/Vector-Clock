# ADR 0006: TLS termination at the application

## Status
Accepted.

## Context
Production deployments of the lab backend need HTTPS. The simplest
patterns are: (1) terminate TLS at a load balancer / ingress in front
of the app, or (2) terminate TLS inside the Go process. The lab's
container image is distroless, so adding an `nginx` sidecar is a heavy
change. The Go stdlib ships `crypto/tls` and `crypto/x509` with no
extra dependencies. Let's Encrypt + cert-manager is the most common
production cert source and it writes renewed certs to a `Secret` on a
60-90 day cycle — the app must pick up the renewed cert without a
pod restart.

## Decision
Terminate TLS in the Go process via a new `gateway/tlsconfig` package
that:

- Loads the cert + key from disk via `tls.LoadX509KeyPair`.
- Optionally loads a client CA bundle to enable mTLS.
- Returns a `*tls.Config` with Mozilla-intermediate defaults
  (`MinVersion = TLS 1.2`, modern AEAD suites, ALPN `h2`/`http/1.1`).
- Hot-reloads the cert from disk on a timer when
  `VC_TLS_RELOAD_INTERVAL > 0`. The reload is atomic via
  `atomic.Pointer[tls.Certificate]` and uses a (size, mtime)
  fingerprint to short-circuit no-op reloads.

The server is configured by four new env vars:

- `VC_TLS_CERT_FILE` — required to enable TLS.
- `VC_TLS_KEY_FILE` — required when cert is set.
- `VC_TLS_CLIENT_CA_FILE` — optional; enables mTLS.
- `VC_TLS_RELOAD_INTERVAL` — optional; e.g. `5m`.

When `VC_TLS_CERT_FILE` is empty the server runs plain HTTP (the
existing default), so existing deployments are unaffected.

## Consequences
- The distroless image continues to ship without a shell or extra
  binaries; the cert is mounted as a Kubernetes `Secret`.
- Hot-reload means cert-manager renewals happen with zero downtime
  and no pod restart.
- mTLS is opt-in but trivially enabled by mounting a CA bundle.
- The e2e test suite now exercises HTTPS, ALPN, mTLS, and cert
  reload against the actual binary.
- Plain HTTP is no longer served on the same port when TLS is
  enabled. HTTP→HTTPS redirect is the responsibility of the LB /
  ingress / BFF, not the app.

## Alternatives considered
- **TLS at the ingress only**: Simpler, but every deployment topology
  (NodePort, `kubectl port-forward`, dev loopback) would need a
  separate LB to test. The cost of in-process TLS is small.
- **`autocert.Manager` for Let's Encrypt**: Requires the process to
  serve the ACME challenge on port 80, which conflicts with the
  distroless / non-root posture. cert-manager + a mounted `Secret` is
  the standard k8s pattern.
- **`caddyserver/certmagic`**: Excellent library, but adds a heavy
  dependency for what is fundamentally `crypto/tls` + a 30-line
  reload loop. The Go stdlib is sufficient.
