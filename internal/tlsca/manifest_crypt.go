package tlsca

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters for the manifest passphrase. Same envelope as
// internal/vault uses, kept local to avoid a cross-package dependency.
const (
	manifestArgonTime    = 3
	manifestArgonMemory  = 64 * 1024 // 64 MiB
	manifestArgonThreads = 4
	manifestKeyLen       = 32
	manifestSaltLen      = 16
	manifestNonceLen     = 12
)

// ErrManifestPassphrase is returned when decryption fails — almost always
// the wrong passphrase.
var ErrManifestPassphrase = errors.New("wrong passphrase or corrupt encrypted CA key")

// encryptedCAKey is the on-wire JSON envelope for an encrypted CA private key.
// It carries the Argon2id salt + GCM nonce alongside the ciphertext so the
// receiver can derive the same wrap key.
type encryptedCAKey struct {
	Version    int    `json:"v"`     // envelope version, currently 1
	Salt       []byte `json:"salt"`  // Argon2id salt
	Nonce      []byte `json:"nonce"` // GCM nonce
	Ciphertext []byte `json:"ct"`    // AES-256-GCM(Argon2id(passphrase), CA key PEM)
}

// EncryptCAKey wraps a PEM-encoded CA private key with a passphrase-derived
// AES-256-GCM key. The passphrase must be transmitted out-of-band — never
// alongside the manifest file itself, otherwise this offers no protection.
//
// Returns a single base64 string suitable for embedding in the JSON manifest.
func EncryptCAKey(caKeyPEM, passphrase string) (string, error) {
	if passphrase == "" {
		return "", errors.New("passphrase must not be empty")
	}
	if caKeyPEM == "" {
		return "", errors.New("CA key PEM must not be empty")
	}

	salt := make([]byte, manifestSaltLen)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}

	wrapKey := argon2.IDKey([]byte(passphrase), salt,
		manifestArgonTime, manifestArgonMemory, manifestArgonThreads, manifestKeyLen)

	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, manifestNonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	ct := gcm.Seal(nil, nonce, []byte(caKeyPEM), nil)

	envelope := encryptedCAKey{
		Version:    1,
		Salt:       salt,
		Nonce:      nonce,
		Ciphertext: ct,
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("marshal envelope: %w", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// DecryptCAKey reverses EncryptCAKey. Any tampering with salt/nonce/ciphertext
// will surface as ErrManifestPassphrase from the GCM open step (authenticated
// encryption).
func DecryptCAKey(encrypted, passphrase string) (string, error) {
	if passphrase == "" {
		return "", errors.New("passphrase must not be empty")
	}
	raw, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}
	var envelope encryptedCAKey
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", fmt.Errorf("parse envelope: %w", err)
	}
	if envelope.Version != 1 {
		return "", fmt.Errorf("unsupported envelope version %d", envelope.Version)
	}
	if len(envelope.Salt) != manifestSaltLen || len(envelope.Nonce) != manifestNonceLen {
		return "", errors.New("invalid envelope: bad salt/nonce length")
	}

	wrapKey := argon2.IDKey([]byte(passphrase), envelope.Salt,
		manifestArgonTime, manifestArgonMemory, manifestArgonThreads, manifestKeyLen)

	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create GCM: %w", err)
	}

	plain, err := gcm.Open(nil, envelope.Nonce, envelope.Ciphertext, nil)
	if err != nil {
		return "", ErrManifestPassphrase
	}
	return string(plain), nil
}
