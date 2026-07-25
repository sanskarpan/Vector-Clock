// gen-test-cert generates a self-signed ed25519 TLS cert + key for
// local development and smoke-testing. For production use cert-manager
// + Let's Encrypt, not this script.
//
// Usage:
//
//	go run ./scripts/gen-test-cert -out /tmp/cert.pem -key /tmp/key.pem
//	# OR (writes both files next to each other):
//	go run ./scripts/gen-test-cert -out /tmp/vc
//	# → /tmp/vc.cert.pem and /tmp/vc.key.pem
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/DistributedClocks/vectorclock-system/gateway/tlsconfig"
)

func main() {
	out := flag.String("out", "cert", "Output prefix; files are <out>.cert.pem and <out>.key.pem. Use -cert / -key to override individually.")
	certFlag := flag.String("cert", "", "Path to write the cert PEM (overrides -out for the cert file)")
	keyFlag := flag.String("key", "", "Path to write the key PEM (overrides -out for the key file)")
	cn := flag.String("cn", "localhost", "Common Name for the cert subject")
	dns := flag.String("dns", "localhost", "Comma-separated DNS SANs")
	ips := flag.String("ip", "127.0.0.1", "Comma-separated IP SANs")
	flag.Parse()

	dnsList := splitNonEmpty(*dns)
	ipList := []net.IP{}
	for _, s := range splitNonEmpty(*ips) {
		if ip := net.ParseIP(s); ip != nil {
			ipList = append(ipList, ip)
		} else {
			log.Fatalf("invalid IP %q", s)
		}
	}

	pair, err := tlsconfig.GenerateSelfSignedCert(*cn, dnsList, ipList)
	if err != nil {
		log.Fatalf("generate cert: %v", err)
	}

	certPath := *certFlag
	keyPath := *keyFlag
	if certPath == "" {
		certPath = *out + ".cert.pem"
	}
	if keyPath == "" {
		keyPath = *out + ".key.pem"
	}
	if err := os.WriteFile(certPath, pair.CertPEM, 0o600); err != nil {
		log.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pair.KeyPEM, 0o600); err != nil {
		log.Fatalf("write key: %v", err)
	}
	fmt.Printf("wrote %s and %s\n", certPath, keyPath)
}

func splitNonEmpty(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
