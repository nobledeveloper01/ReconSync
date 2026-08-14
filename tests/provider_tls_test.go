package tests

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/provider"
)

// A bank issues us a client certificate and signs its own with a private CA
// that no public trust store carries. Both halves are exercised here, because
// either one missing means the connection cannot be made at all.
type pki struct {
	caPEM      []byte
	caCert     *x509.Certificate
	caKey      *ecdsa.PrivateKey
	certFile   string
	keyFile    string
	caFile     string
	serverCert tls.Certificate
}

func newPKI(t *testing.T) *pki {
	t.Helper()
	dir := t.TempDir()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test Bank CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	p := &pki{caPEM: caPEM, caCert: caCert, caKey: caKey}
	p.caFile = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(p.caFile, caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	// The certificate the bank issued us.
	p.certFile, p.keyFile = p.issue(t, dir, "reconsync-client", nil)
	// The bank's own certificate, for the server side.
	certFile, keyFile := p.issue(t, dir, "bank.internal", []string{"127.0.0.1"})
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("server keypair: %v", err)
	}
	p.serverCert = cert
	return p
}

// issue signs a leaf certificate with the CA.
func (p *pki) issue(t *testing.T, dir, cn string, ips []string) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{cn},
	}
	for _, ip := range ips {
		tmpl.IPAddresses = append(tmpl.IPAddresses, net.ParseIP(ip))
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatalf("sign %s: %v", cn, err)
	}

	certFile = filepath.Join(dir, cn+".pem")
	keyFile = filepath.Join(dir, cn+"-key.pem")
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// bankServer demands a client certificate, exactly as a bank endpoint does.
func (p *pki) bankServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(p.caPEM)

	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{p.serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// Without this, no bank adapter can connect at all — the failure is in
// principle, not in configuration.
func TestProviderConnectsWithAClientCertificate(t *testing.T) {
	p := newPKI(t)

	var sawClientCN string
	srv := p.bankServer(t, func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) > 0 {
			sawClientCN = r.TLS.PeerCertificates[0].Subject.CommonName
		}
		_, _ = w.Write([]byte(`{"data":{"status":"success","amount":5000000}}`))
	})

	rail, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "sterling",
		URLTemplate:   srv.URL + "/tsq/{reference}",
		StatusPath:    "data.status",
		AmountPath:    "data.amount",
		SettledValues: []string{"success"},
		TLS: provider.TLSConfig{
			ClientCertFile: p.certFile,
			ClientKeyFile:  p.keyFile,
			CAFile:         p.caFile,
		},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	got, err := rail.Query(context.Background(), provider.Ref{
		TransactionID: "TXN-1", AmountMinor: 5_000_000,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Settled {
		t.Fatalf("outcome = %s, want settled: %s", got.Outcome, got.Detail)
	}
	// The bank saw our certificate, which is how they know the connection is ours.
	if sawClientCN != "reconsync-client" {
		t.Errorf("bank saw client %q, want reconsync-client", sawClientCN)
	}
}

// Without the client certificate the bank refuses us, and that must surface as
// unknown — never as a verdict about the transaction.
func TestProviderWithoutAClientCertificateIsUnknownNotAVerdict(t *testing.T) {
	p := newPKI(t)
	srv := p.bankServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"success"}}`))
	})

	rail, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "sterling",
		URLTemplate:   srv.URL + "/tsq/{reference}",
		StatusPath:    "data.status",
		SettledValues: []string{"success"},
		// Their CA, but no certificate of our own.
		TLS: provider.TLSConfig{CAFile: p.caFile},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	got, err := rail.Query(context.Background(), provider.Ref{TransactionID: "TXN-1"})
	if err != nil {
		t.Fatalf("Query returned an error rather than an outcome: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown when the bank refuses the handshake", got.Outcome)
	}
}

// A bank signs with a private CA. Without it the connection fails verification
// against the public trust store, and that too must be unknown.
func TestProviderWithoutTheBankCAIsUnknown(t *testing.T) {
	p := newPKI(t)
	srv := p.bankServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"status":"success"}}`))
	})

	rail, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "sterling",
		URLTemplate:   srv.URL + "/tsq/{reference}",
		StatusPath:    "data.status",
		SettledValues: []string{"success"},
		TLS: provider.TLSConfig{
			ClientCertFile: p.certFile,
			ClientKeyFile:  p.keyFile,
			// No CAFile: their certificate is not signed by anything public.
		},
	})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}

	got, err := rail.Query(context.Background(), provider.Ref{TransactionID: "TXN-1"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Errorf("outcome = %s, want unknown without the bank's CA", got.Outcome)
	}
}

// Half a keypair would otherwise surface as a handshake failure against the
// bank, which is far harder to debug than a startup error.
func TestProviderRejectsHalfAKeypair(t *testing.T) {
	p := newPKI(t)

	_, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "sterling",
		URLTemplate:   "https://bank.internal/tsq/{reference}",
		StatusPath:    "data.status",
		SettledValues: []string{"success"},
		TLS:           provider.TLSConfig{ClientCertFile: p.certFile},
	})
	if err == nil {
		t.Fatal("accepted a certificate with no key")
	}
	if !strings.Contains(err.Error(), "client_key_file") {
		t.Errorf("error = %v, want it to name the missing field", err)
	}

	// And a CA file that is not one is caught at startup too.
	junk := filepath.Join(t.TempDir(), "not-a-ca.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := provider.NewHTTP(provider.HTTPConfig{
		ProviderName:  "sterling",
		URLTemplate:   "https://bank.internal/tsq/{reference}",
		StatusPath:    "data.status",
		SettledValues: []string{"success"},
		TLS:           provider.TLSConfig{CAFile: junk},
	}); err == nil {
		t.Error("accepted a CA file containing no certificates")
	}
}
