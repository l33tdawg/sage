package tlsca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func newCAKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pem, err := EncodeKeyPEM(key)
	if err != nil {
		t.Fatalf("encode key: %v", err)
	}
	return pem
}

func TestEncryptCAKey_RoundTrip(t *testing.T) {
	caKeyPEM := newCAKeyPEM(t)
	const passphrase = "correct horse battery staple"

	envelope, err := EncryptCAKey(caKeyPEM, passphrase)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if envelope == "" {
		t.Fatal("envelope is empty")
	}
	// Envelope must not leak the plaintext PEM in any obvious form.
	if strings.Contains(envelope, "BEGIN EC PRIVATE KEY") {
		t.Fatal("envelope contains plaintext PEM marker — encryption did not happen")
	}

	decoded, err := DecryptCAKey(envelope, passphrase)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if decoded != caKeyPEM {
		t.Fatalf("decrypted key does not match original\n got: %q\nwant: %q", decoded, caKeyPEM)
	}
}

func TestDecryptCAKey_WrongPassphrase(t *testing.T) {
	envelope, err := EncryptCAKey(newCAKeyPEM(t), "right one")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	_, err = DecryptCAKey(envelope, "wrong one")
	if !errors.Is(err, ErrManifestPassphrase) {
		t.Fatalf("expected ErrManifestPassphrase, got %v", err)
	}
}

func TestDecryptCAKey_TamperedCiphertextRejected(t *testing.T) {
	const passphrase = "another long passphrase here"
	envelope, err := EncryptCAKey(newCAKeyPEM(t), passphrase)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Decode, flip a byte in the ciphertext, re-encode.
	raw, err := base64.StdEncoding.DecodeString(envelope)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	var env encryptedCAKey
	if err = json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(env.Ciphertext) == 0 {
		t.Fatal("empty ciphertext")
	}
	env.Ciphertext[0] ^= 0xFF
	tampered, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	tamperedB64 := base64.StdEncoding.EncodeToString(tampered)

	if _, err := DecryptCAKey(tamperedB64, passphrase); !errors.Is(err, ErrManifestPassphrase) {
		t.Fatalf("expected ErrManifestPassphrase for tampered ciphertext, got %v", err)
	}
}

func TestDecryptCAKey_TamperedSaltRejected(t *testing.T) {
	const passphrase = "another long passphrase here"
	envelope, err := EncryptCAKey(newCAKeyPEM(t), passphrase)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	raw, _ := base64.StdEncoding.DecodeString(envelope)
	var env encryptedCAKey
	_ = json.Unmarshal(raw, &env)
	env.Salt[0] ^= 0xFF
	tampered, _ := json.Marshal(env)

	if _, err := DecryptCAKey(base64.StdEncoding.EncodeToString(tampered), passphrase); !errors.Is(err, ErrManifestPassphrase) {
		t.Fatalf("tampered salt must surface as ErrManifestPassphrase, got %v", err)
	}
}

func TestEncryptCAKey_NondeterministicEnvelopes(t *testing.T) {
	// Two encryptions of the same plaintext with the same passphrase must
	// produce different envelopes — proves we generate a fresh salt+nonce
	// each time and aren't accidentally reusing a deterministic seed.
	caKeyPEM := newCAKeyPEM(t)
	a, err := EncryptCAKey(caKeyPEM, "same passphrase")
	if err != nil {
		t.Fatalf("encrypt a: %v", err)
	}
	b, err := EncryptCAKey(caKeyPEM, "same passphrase")
	if err != nil {
		t.Fatalf("encrypt b: %v", err)
	}
	if a == b {
		t.Fatal("identical envelopes for same input — salt/nonce reuse")
	}
}

func TestEncryptCAKey_RejectsEmptyInputs(t *testing.T) {
	if _, err := EncryptCAKey("", "passphrase"); err == nil {
		t.Fatal("expected error for empty CA key")
	}
	if _, err := EncryptCAKey("PEM", ""); err == nil {
		t.Fatal("expected error for empty passphrase")
	}
	if _, err := DecryptCAKey("anything", ""); err == nil {
		t.Fatal("expected error for empty passphrase on decrypt")
	}
}

func TestDecryptCAKey_UnsupportedVersionRejected(t *testing.T) {
	env := encryptedCAKey{
		Version:    99,
		Salt:       make([]byte, manifestSaltLen),
		Nonce:      make([]byte, manifestNonceLen),
		Ciphertext: []byte{0x01},
	}
	raw, _ := json.Marshal(env)
	_, err := DecryptCAKey(base64.StdEncoding.EncodeToString(raw), "passphrase")
	if err == nil || !strings.Contains(err.Error(), "unsupported envelope version") {
		t.Fatalf("expected version error, got %v", err)
	}
}
