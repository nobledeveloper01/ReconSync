package provider

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Mutual TLS, which is what actually stands between us and a bank.
//
// Every Nigerian bank connection and NIBSS itself require a client certificate
// the institution issues, usually against a private CA that no public trust
// store carries. A plain http.Client cannot make that connection at all, so
// until this existed the bank adapters were unreachable in principle rather
// than merely unconfigured.
//
// The NIBSS-specific request and response shapes still need their published
// spec. This is the transport underneath them, and it is the part that can be
// proven correct without an access agreement.

// TLSConfig describes the certificates for a rail.
type TLSConfig struct {
	// ClientCertFile and ClientKeyFile are the certificate the institution
	// issued us, which is how they know the connection is ours.
	ClientCertFile string
	ClientKeyFile  string

	// CAFile is the authority that signed *their* certificate. Banks sign with
	// a private CA, so without this the connection fails verification against
	// the public trust store.
	CAFile string

	// ServerName overrides the name verified in their certificate, for the case
	// where the host we dial and the name they issued differ — an IP allowlist
	// with a named certificate behind it, which is common inside bank networks.
	ServerName string
}

// Configured reports whether anything needs building.
func (c TLSConfig) Configured() bool {
	return c.ClientCertFile != "" || c.ClientKeyFile != "" || c.CAFile != "" || c.ServerName != ""
}

// NewTLSClient builds an HTTP client for a mutually authenticated connection.
//
// There is deliberately no option to skip verification. A bank connection that
// does not verify the far end is worse than no connection: it looks like
// corroboration from an authoritative source while being trivially spoofable,
// and this system turns that answer into money movement. Anyone needing a
// private authority supplies its CA.
func NewTLSClient(cfg TLSConfig, timeout time.Duration) (*http.Client, error) {
	if !cfg.Configured() {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: cfg.ServerName,
	}

	switch {
	case cfg.ClientCertFile != "" && cfg.ClientKeyFile != "":
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("provider: load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	case cfg.ClientCertFile != "" || cfg.ClientKeyFile != "":
		// Half a keypair is a configuration mistake that would otherwise
		// surface as a handshake failure against the bank, which is a much
		// harder thing to debug than a startup error.
		return nil, errors.New("provider: client_cert_file and client_key_file must be set together")
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(filepath.Clean(cfg.CAFile))
		if err != nil {
			return nil, fmt.Errorf("provider: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("provider: %s contains no usable certificates", cfg.CAFile)
		}
		tlsCfg.RootCAs = pool
	}

	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: tlsCfg,
			// Bank endpoints are few and long-lived, and a fresh handshake per
			// query would put mutual-TLS negotiation on the detection path.
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}, nil
}
