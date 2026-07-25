// Cert generation helpers. These are exported because they are useful
// to operators (for spinning up a quick dev / staging server with a
// throwaway self-signed cert) and to tests (for generating fixture
// material on the fly). The same code path is used in both cases so
// what tests cover is what production tooling uses.
//
// Certs are ed25519 (small, fast, no curve choice to second-guess).
// Validity is one year. None of this is suitable for production — for
// production use cert-manager / Let's Encrypt / a real CA.
package tlsconfig

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"time"
)

// CertPair is a PEM-encoded certificate + private key.
type CertPair struct {
	CertPEM []byte
	KeyPEM  []byte
}

// GenerateSelfSignedCert creates a self-signed ed25519 certificate
// with the given common name and SANs. Valid for 365 days. The
// returned pair is suitable for tls.LoadX509KeyPair.
func GenerateSelfSignedCert(commonName string, dnsSANs []string, ipSANs []net.IP) (*CertPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("gencert: generate key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("gencert: serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Vector Clock Lab (generated)"},
		},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsSANs,
		IPAddresses:           ipSANs,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("gencert: create certificate: %w", err)
	}
	return encodePair(derBytes, priv)
}

// GenerateCA creates a self-signed ed25519 CA certificate. Valid for
// 365 days. The CA has IsCA=true and may be used to sign client / server
// certs via GenerateCASignedCert.
func GenerateCA(commonName string) (*CertPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("gencert: generate CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("gencert: CA serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Vector Clock Lab (generated CA)"},
		},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("gencert: create CA: %w", err)
	}
	return encodePair(derBytes, priv)
}

// GenerateCASignedCert creates a leaf certificate signed by ca. The
// leaf is suitable for client auth (ExtKeyUsage = ClientAuth) by
// default.
func GenerateCASignedCert(ca *CertPair, commonName string, dnsSANs []string, ipSANs []net.IP) (*CertPair, error) {
	if ca == nil {
		return nil, errors.New("gencert: ca is nil")
	}
	caCert, caKey, err := parsePair(ca)
	if err != nil {
		return nil, err
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("gencert: generate leaf key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("gencert: leaf serial: %w", err)
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Vector Clock Lab (generated leaf)"},
		},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              dnsSANs,
		IPAddresses:           ipSANs,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, pub, caKey)
	if err != nil {
		return nil, fmt.Errorf("gencert: create leaf: %w", err)
	}
	return encodePair(derBytes, priv)
}

// encodePair PEM-encodes a DER cert and a private key. Uses PKCS#8 for
// the key (Go's preferred format).
func encodePair(certDER []byte, priv any) (*CertPair, error) {
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("gencert: marshal key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return &CertPair{CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// parsePair decodes a CertPair's PEM blocks. Used by GenerateCASignedCert.
func parsePair(p *CertPair) (*x509.Certificate, any, error) {
	cb, _ := pem.Decode(p.CertPEM)
	if cb == nil {
		return nil, nil, errors.New("gencert: no PEM cert block")
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("gencert: parse cert: %w", err)
	}
	kb, _ := pem.Decode(p.KeyPEM)
	if kb == nil {
		return nil, nil, errors.New("gencert: no PEM key block")
	}
	key, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("gencert: parse key: %w", err)
	}
	return cert, key, nil
}
