// Package testcert provides certificates for in-process TLS carrier tests.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// LocalName is the DNS identity used by the in-process TLS tests.
const LocalName = "localhost"

// Local creates a self-signed server certificate and its client trust pool.
func Local(t testing.TB, ips ...net.IP) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: LocalName},
		DNSNames: []string{LocalName}, IPAddresses: ips, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	trust := x509.NewCertPool()
	trust.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, trust
}
