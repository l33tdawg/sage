// Package tlsca provides certificate authority and TLS certificate management
// for SAGE quorum nodes. It uses ECDSA P-256 for TLS certificates (broad client
// compatibility) while the Ed25519 identity system remains unchanged.
package tlsca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	// CACertFile is the filename for the quorum CA certificate.
	CACertFile = "ca.crt"
	// CAKeyFile is the filename for the quorum CA private key.
	CAKeyFile = "ca.key"
	// NodeCertFile is the filename for this node's TLS certificate.
	NodeCertFile = "node.crt"
	// NodeKeyFile is the filename for this node's TLS private key.
	NodeKeyFile = "node.key"

	caValidityYears   = 10
	nodeValidityYears = 1
	// Backdate certs by 24h to tolerate LAN clock skew.
	clockSkewBuffer = 24 * time.Hour
)

// GenerateCA creates a new ECDSA P-256 certificate authority for a SAGE quorum.
// The CA is self-signed with a 10-year validity period.
func GenerateCA(chainID string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate CA key: %w", err)
	}

	serialNumber, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("sage-ca-%s", chainID),
			Organization: []string{"SAGE Quorum"},
		},
		NotBefore:             now.Add(-clockSkewBuffer),
		NotAfter:              now.AddDate(caValidityYears, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA certificate: %w", err)
	}

	return cert, key, nil
}

// GenerateNodeCert creates a TLS certificate for a SAGE node, signed by the quorum CA.
// The sans parameter accepts IP addresses and DNS names for the certificate's SAN field.
// Certificates include both ServerAuth and ClientAuth extended key usage for future mTLS.
func GenerateNodeCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, nodeID string, sans []string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate node key: %w", err)
	}

	serialNumber, err := randomSerial()
	if err != nil {
		return nil, nil, err
	}

	var ips []net.IP
	var dnsNames []string
	for _, san := range sans {
		if ip := net.ParseIP(san); ip != nil {
			ips = append(ips, ip)
		} else {
			dnsNames = append(dnsNames, san)
		}
	}
	// Always include localhost for local connections.
	ips = append(ips, net.IPv4(127, 0, 0, 1), net.IPv6loopback)

	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("sage-node-%s", nodeID),
			Organization: []string{"SAGE Quorum"},
		},
		NotBefore: now.Add(-clockSkewBuffer),
		NotAfter:  now.AddDate(nodeValidityYears, 0, 0),
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth, // Ready for mTLS in v7.0.
		},
		IPAddresses: ips,
		DNSNames:    dnsNames,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create node certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("parse node certificate: %w", err)
	}

	return cert, key, nil
}

// WriteCert writes an X.509 certificate to a PEM file with 0644 permissions.
func WriteCert(path string, cert *x509.Certificate) error {
	return writePEM(path, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}, 0644)
}

// WriteKey writes an ECDSA private key to a PEM file with 0600 permissions.
func WriteKey(path string, key *ecdsa.PrivateKey) error {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}
	return writePEM(path, &pem.Block{Type: "EC PRIVATE KEY", Bytes: der}, 0600)
}

// ReadCert reads an X.509 certificate from a PEM file.
func ReadCert(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("no CERTIFICATE PEM block found in %s", path)
	}
	return x509.ParseCertificate(block.Bytes)
}

// ReadKey reads an ECDSA private key from a PEM file.
func ReadKey(path string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data)
	if block == nil || block.Type != "EC PRIVATE KEY" {
		return nil, fmt.Errorf("no EC PRIVATE KEY PEM block found in %s", path)
	}
	return x509.ParseECPrivateKey(block.Bytes)
}

// EncodeCertPEM returns the PEM-encoded bytes of a certificate.
func EncodeCertPEM(cert *x509.Certificate) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}))
}

// EncodeKeyPEM returns the PEM-encoded bytes of an ECDSA private key.
func EncodeKeyPEM(key *ecdsa.PrivateKey) (string, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return "", fmt.Errorf("marshal private key: %w", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})), nil
}

// DecodeCertPEM parses a PEM-encoded certificate string.
func DecodeCertPEM(pemStr string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("no CERTIFICATE PEM block found")
	}
	return x509.ParseCertificate(block.Bytes)
}

// DecodeKeyPEM parses a PEM-encoded ECDSA private key string.
func DecodeKeyPEM(pemStr string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil || block.Type != "EC PRIVATE KEY" {
		return nil, fmt.Errorf("no EC PRIVATE KEY PEM block found")
	}
	return x509.ParseECPrivateKey(block.Bytes)
}

// LoadOrGenerateCA loads an existing CA from certsDir, or generates a new one if none exists.
func LoadOrGenerateCA(certsDir, chainID string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPath := filepath.Join(certsDir, CACertFile)
	keyPath := filepath.Join(certsDir, CAKeyFile)

	// Try loading existing CA.
	cert, certErr := ReadCert(certPath)
	key, keyErr := ReadKey(keyPath)
	if certErr == nil && keyErr == nil {
		return cert, key, nil
	}

	// Generate new CA.
	if err := os.MkdirAll(certsDir, 0700); err != nil {
		return nil, nil, fmt.Errorf("create certs directory: %w", err)
	}

	cert, key, err := GenerateCA(chainID)
	if err != nil {
		return nil, nil, err
	}

	if err := WriteCert(certPath, cert); err != nil {
		return nil, nil, fmt.Errorf("write CA cert: %w", err)
	}
	if err := WriteKey(keyPath, key); err != nil {
		return nil, nil, fmt.Errorf("write CA key: %w", err)
	}

	return cert, key, nil
}

// CertsExist returns true if both node cert and key files exist in certsDir.
func CertsExist(certsDir string) bool {
	_, certErr := os.Stat(filepath.Join(certsDir, NodeCertFile))
	_, keyErr := os.Stat(filepath.Join(certsDir, NodeKeyFile))
	return certErr == nil && keyErr == nil
}

// ParseHostPort extracts the host portion from a "host:port" string.
// If the input has no port, it is returned as-is.
func ParseHostPort(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address // No port, return as-is.
	}
	return host
}

func writePEM(path string, block *pem.Block, perm os.FileMode) error {
	data := pem.EncodeToMemory(block)
	if err := os.WriteFile(path, data, perm); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func randomSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}
	return serial, nil
}
