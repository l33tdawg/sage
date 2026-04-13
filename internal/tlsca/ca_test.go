package tlsca

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGenerateCA(t *testing.T) {
	cert, key, err := GenerateCA("test-chain")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if key == nil {
		t.Fatal("CA key is nil")
	}
	if !cert.IsCA {
		t.Error("certificate is not a CA")
	}
	if cert.Subject.CommonName != "sage-ca-test-chain" {
		t.Errorf("CN = %q, want sage-ca-test-chain", cert.Subject.CommonName)
	}
	if cert.MaxPathLen != 1 {
		t.Errorf("MaxPathLen = %d, want 1", cert.MaxPathLen)
	}
	if cert.NotBefore.After(time.Now()) {
		t.Error("NotBefore is in the future (no clock skew buffer)")
	}
	if cert.NotAfter.Before(time.Now().AddDate(9, 0, 0)) {
		t.Error("NotAfter is less than 9 years from now")
	}
}

func TestGenerateNodeCert(t *testing.T) {
	caCert, caKey, err := GenerateCA("test-chain")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	sans := []string{"192.168.1.10", "node1.local"}
	cert, key, err := GenerateNodeCert(caCert, caKey, "abc123", sans)
	if err != nil {
		t.Fatalf("GenerateNodeCert: %v", err)
	}
	if key == nil {
		t.Fatal("node key is nil")
	}
	if cert.Subject.CommonName != "sage-node-abc123" {
		t.Errorf("CN = %q, want sage-node-abc123", cert.Subject.CommonName)
	}

	// Check SANs.
	foundIP := false
	for _, ip := range cert.IPAddresses {
		if ip.Equal(net.ParseIP("192.168.1.10")) {
			foundIP = true
		}
	}
	if !foundIP {
		t.Error("192.168.1.10 not in certificate IP SANs")
	}

	foundDNS := false
	for _, dns := range cert.DNSNames {
		if dns == "node1.local" {
			foundDNS = true
		}
	}
	if !foundDNS {
		t.Error("node1.local not in certificate DNS SANs")
	}

	// Localhost should always be present.
	foundLocalhost := false
	for _, ip := range cert.IPAddresses {
		if ip.Equal(net.IPv4(127, 0, 0, 1)) {
			foundLocalhost = true
		}
	}
	if !foundLocalhost {
		t.Error("127.0.0.1 not in certificate IP SANs")
	}

	// Verify chain.
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Errorf("certificate chain verification failed: %v", err)
	}

	// Check ExtKeyUsage includes both server and client auth.
	hasServer, hasClient := false, false
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			hasServer = true
		}
		if usage == x509.ExtKeyUsageClientAuth {
			hasClient = true
		}
	}
	if !hasServer {
		t.Error("missing ExtKeyUsageServerAuth")
	}
	if !hasClient {
		t.Error("missing ExtKeyUsageClientAuth")
	}
}

func TestWriteReadCert(t *testing.T) {
	dir := t.TempDir()
	cert, _, err := GenerateCA("test")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	path := filepath.Join(dir, "test.crt")
	if err := WriteCert(path, cert); err != nil {
		t.Fatalf("WriteCert: %v", err)
	}

	loaded, err := ReadCert(path)
	if err != nil {
		t.Fatalf("ReadCert: %v", err)
	}
	if loaded.Subject.CommonName != cert.Subject.CommonName {
		t.Errorf("CN = %q, want %q", loaded.Subject.CommonName, cert.Subject.CommonName)
	}
}

func TestWriteReadKey(t *testing.T) {
	dir := t.TempDir()
	_, key, err := GenerateCA("test")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	path := filepath.Join(dir, "test.key")
	if err := WriteKey(path, key); err != nil {
		t.Fatalf("WriteKey: %v", err)
	}

	// Verify file permissions are restrictive.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0077 != 0 {
		t.Errorf("key file permissions %o allow group/other access", perm)
	}

	loaded, err := ReadKey(path)
	if err != nil {
		t.Fatalf("ReadKey: %v", err)
	}
	if !loaded.Equal(key) {
		t.Error("loaded key does not match original")
	}
}

func TestEncodeDecode(t *testing.T) {
	cert, key, err := GenerateCA("test")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}

	// Round-trip cert.
	certPEM := EncodeCertPEM(cert)
	decoded, err := DecodeCertPEM(certPEM)
	if err != nil {
		t.Fatalf("DecodeCertPEM: %v", err)
	}
	if decoded.Subject.CommonName != cert.Subject.CommonName {
		t.Errorf("cert round-trip: CN = %q, want %q", decoded.Subject.CommonName, cert.Subject.CommonName)
	}

	// Round-trip key.
	keyPEM, err := EncodeKeyPEM(key)
	if err != nil {
		t.Fatalf("EncodeKeyPEM: %v", err)
	}
	decodedKey, err := DecodeKeyPEM(keyPEM)
	if err != nil {
		t.Fatalf("DecodeKeyPEM: %v", err)
	}
	if !decodedKey.Equal(key) {
		t.Error("key round-trip mismatch")
	}
}

func TestLoadOrGenerateCA(t *testing.T) {
	dir := t.TempDir()
	certsDir := filepath.Join(dir, "certs")

	// First call should generate.
	cert1, key1, err := LoadOrGenerateCA(certsDir, "test")
	if err != nil {
		t.Fatalf("first LoadOrGenerateCA: %v", err)
	}

	// Second call should load the same CA.
	cert2, key2, err := LoadOrGenerateCA(certsDir, "test")
	if err != nil {
		t.Fatalf("second LoadOrGenerateCA: %v", err)
	}

	if cert1.Subject.CommonName != cert2.Subject.CommonName {
		t.Error("CA CN changed between calls")
	}
	if !key1.Equal(key2) {
		t.Error("CA key changed between calls")
	}
}

func TestServerTLSConfig(t *testing.T) {
	dir := t.TempDir()

	caCert, caKey, err := GenerateCA("test")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if err := WriteCert(filepath.Join(dir, CACertFile), caCert); err != nil {
		t.Fatalf("write CA cert: %v", err)
	}

	nodeCert, nodeKey, err := GenerateNodeCert(caCert, caKey, "node1", []string{"192.168.1.10"})
	if err != nil {
		t.Fatalf("GenerateNodeCert: %v", err)
	}
	if err := WriteCert(filepath.Join(dir, NodeCertFile), nodeCert); err != nil {
		t.Fatalf("write node cert: %v", err)
	}
	if err := WriteKey(filepath.Join(dir, NodeKeyFile), nodeKey); err != nil {
		t.Fatalf("write node key: %v", err)
	}

	cfg, err := ServerTLSConfig(dir)
	if err != nil {
		t.Fatalf("ServerTLSConfig: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Error("MinVersion is not TLS 1.3")
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("got %d certificates, want 1", len(cfg.Certificates))
	}
	if cfg.ClientAuth != tls.NoClientCert {
		t.Error("ClientAuth should be NoClientCert for v6.5")
	}
}

func TestClientTLSConfig(t *testing.T) {
	dir := t.TempDir()

	caCert, _, err := GenerateCA("test")
	if err != nil {
		t.Fatalf("GenerateCA: %v", err)
	}
	if err := WriteCert(filepath.Join(dir, CACertFile), caCert); err != nil {
		t.Fatalf("write CA cert: %v", err)
	}

	cfg, err := ClientTLSConfig(dir)
	if err != nil {
		t.Fatalf("ClientTLSConfig: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs is nil")
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Error("MinVersion is not TLS 1.3")
	}
}

func TestCertsExist(t *testing.T) {
	dir := t.TempDir()
	if CertsExist(dir) {
		t.Error("CertsExist should return false for empty dir")
	}

	caCert, caKey, _ := GenerateCA("test")
	nodeCert, nodeKey, _ := GenerateNodeCert(caCert, caKey, "n1", nil)
	_ = WriteCert(filepath.Join(dir, NodeCertFile), nodeCert)
	_ = WriteKey(filepath.Join(dir, NodeKeyFile), nodeKey)

	if !CertsExist(dir) {
		t.Error("CertsExist should return true when cert and key exist")
	}
}

func TestParseHostPort(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"192.168.1.10:26656", "192.168.1.10"},
		{"node1.local:8080", "node1.local"},
		{"192.168.1.10", "192.168.1.10"},
		{"[::1]:26656", "::1"},
	}
	for _, tc := range tests {
		got := ParseHostPort(tc.input)
		if got != tc.want {
			t.Errorf("ParseHostPort(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
