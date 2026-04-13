package tlsca

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

// ServerTLSConfig returns a *tls.Config for a SAGE REST API server in quorum mode.
// It loads the node's certificate and key, and trusts the quorum CA for client verification.
// Client auth is not required (NoClientCert) — upgrade to mTLS in v7.0.
func ServerTLSConfig(certsDir string) (*tls.Config, error) {
	certPath := filepath.Join(certsDir, NodeCertFile)
	keyPath := filepath.Join(certsDir, NodeKeyFile)
	caPath := filepath.Join(certsDir, CACertFile)

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load node certificate: %w", err)
	}

	caPool, err := loadCAPool(caPath)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caPool,
		ClientAuth:   tls.NoClientCert, // Server TLS only for v6.5; mTLS in v7.0.
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLSConfig returns a *tls.Config for HTTP clients that need to trust the quorum CA.
// Used by SDKs and internal services connecting to TLS-enabled SAGE nodes.
func ClientTLSConfig(certsDir string) (*tls.Config, error) {
	caPath := filepath.Join(certsDir, CACertFile)

	caPool, err := loadCAPool(caPath)
	if err != nil {
		return nil, err
	}

	// Optionally load client cert for future mTLS.
	certPath := filepath.Join(certsDir, NodeCertFile)
	keyPath := filepath.Join(certsDir, NodeKeyFile)
	var certs []tls.Certificate
	if cert, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil {
		certs = append(certs, cert)
	}

	return &tls.Config{
		RootCAs:      caPool,
		Certificates: certs,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLSConfigFromCA returns a *tls.Config that trusts a CA loaded from the given PEM file.
// This is used by external clients that only have the CA cert (no client cert).
func ClientTLSConfigFromCA(caPath string) (*tls.Config, error) {
	caPool, err := loadCAPool(caPath)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS13,
	}, nil
}

// ServerTLSConfigFromFiles returns a *tls.Config from individual cert, key, and CA file paths.
// Used by the amid Docker deployment where cert paths are passed as CLI flags.
func ServerTLSConfigFromFiles(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load TLS certificate: %w", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
		MinVersion:   tls.VersionTLS13,
	}

	if caFile != "" {
		caPool, err := loadCAPool(caFile)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = caPool
	}

	return cfg, nil
}

func loadCAPool(caPath string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA certificate: %w", err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("failed to parse CA certificate from %s", caPath)
	}

	return pool, nil
}
