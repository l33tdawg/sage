//go:build integration

package integration

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// generateTestCA creates a self-signed CA cert and key for testing.
func generateTestCA(dir, cn string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"SAGE Test"}},
		NotBefore:             time.Now().Add(-24 * time.Hour),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            1,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}
	// Write to disk for reference.
	os.WriteFile(filepath.Join(dir, "ca.crt"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), 0644)
	return cert, key, nil
}

// generateTestNodeCert creates a node certificate signed by the test CA.
func generateTestNodeCert(dir string, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, nodeID string, sans []string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))

	var ips []net.IP
	var dnsNames []string
	for _, san := range sans {
		if ip := net.ParseIP(san); ip != nil {
			ips = append(ips, ip)
		} else {
			dnsNames = append(dnsNames, san)
		}
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "sage-node-" + nodeID, Organization: []string{"SAGE Test"}},
		NotBefore:    time.Now().Add(-24 * time.Hour),
		NotAfter:     time.Now().AddDate(1, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  ips,
		DNSNames:     dnsNames,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

// pemEncode returns PEM-encoded bytes for the given type and data.
func pemEncode(blockType string, data []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data})
}

// pemEncodeKey encodes an ECDSA private key to PEM bytes.
func pemEncodeKey(key *ecdsa.PrivateKey) []byte {
	der, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}

// certPoolFromCert creates a CertPool containing a single certificate.
func certPoolFromCert(cert *x509.Certificate) *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return pool
}
