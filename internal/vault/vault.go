// Package vault provides AES-256-GCM encryption for SAGE memories.
// The encryption key is derived from a user passphrase via Argon2id,
// then stored (encrypted) in a vault key file. This keeps memory data
// encrypted at rest — if the laptop is stolen or the DB is uploaded
// to cloud storage, nobody can read the contents without the passphrase.
package vault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters (OWASP recommended minimums for interactive login).
const (
	argonTime    = 3
	argonMemory  = 64 * 1024 // 64 MB
	argonThreads = 4
	argonKeyLen  = 32 // AES-256
	saltLen      = 16
	nonceLen     = 12 // AES-GCM standard
)

// ErrLocked is returned when an operation requires the vault to be unlocked.
var ErrLocked = errors.New("vault is locked — passphrase required")

// ErrWrongPassphrase is returned when the passphrase doesn't match.
var ErrWrongPassphrase = errors.New("wrong passphrase")

// keyFile is the on-disk format for the encrypted vault key.
type keyFile struct {
	Salt          []byte `json:"salt"`           // Argon2id salt
	EncryptedKey  []byte `json:"encrypted_key"`  // AES-256-GCM encrypted data key
	Nonce         []byte `json:"nonce"`           // GCM nonce for key encryption
	VerifyHash    []byte `json:"verify_hash"`     // SHA-256 of decrypted data key (for fast passphrase verification)
}

// Vault holds the encryption state. When unlocked, it can encrypt/decrypt
// memory content. When locked, all operations return ErrLocked.
type Vault struct {
	gcm    cipher.AEAD
	dataKey []byte // raw 32-byte data key (only in memory while unlocked)
}

// Init creates a new vault key file protected by the given passphrase.
// The actual data encryption key is randomly generated — the passphrase
// just protects the key file. This means changing the passphrase doesn't
// require re-encrypting the entire database.
func Init(keyFilePath, passphrase string) error {
	// Generate random data key
	dataKey := make([]byte, argonKeyLen)
	if _, err := io.ReadFull(rand.Reader, dataKey); err != nil {
		return fmt.Errorf("generate data key: %w", err)
	}

	// Generate salt for passphrase KDF
	salt := make([]byte, saltLen)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return fmt.Errorf("generate salt: %w", err)
	}

	// Derive wrapping key from passphrase
	wrapKey := argon2.IDKey([]byte(passphrase), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	// Encrypt the data key with the wrapping key
	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, nonceLen)
	if _, randErr := io.ReadFull(rand.Reader, nonce); randErr != nil {
		return fmt.Errorf("generate nonce: %w", randErr)
	}

	encryptedKey := gcm.Seal(nil, nonce, dataKey, nil)

	// Verification hash — lets us check the passphrase without full decrypt
	verifyHash := sha256.Sum256(dataKey)

	kf := keyFile{
		Salt:         salt,
		EncryptedKey: encryptedKey,
		Nonce:        nonce,
		VerifyHash:   verifyHash[:],
	}

	data, err := json.Marshal(kf)
	if err != nil {
		return fmt.Errorf("marshal key file: %w", err)
	}

	return os.WriteFile(keyFilePath, data, 0600)
}

// Open unlocks the vault using the passphrase and key file.
// Returns a Vault that can encrypt/decrypt memory content.
func Open(keyFilePath, passphrase string) (*Vault, error) {
	data, err := os.ReadFile(keyFilePath)
	if err != nil {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	var kf keyFile
	if unmarshalErr := json.Unmarshal(data, &kf); unmarshalErr != nil {
		return nil, fmt.Errorf("parse key file: %w", unmarshalErr)
	}

	// Derive wrapping key from passphrase
	wrapKey := argon2.IDKey([]byte(passphrase), kf.Salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	// Decrypt the data key
	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return nil, fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create GCM: %w", err)
	}

	dataKey, err := gcm.Open(nil, kf.Nonce, kf.EncryptedKey, nil)
	if err != nil {
		return nil, ErrWrongPassphrase
	}

	// Verify the decrypted key
	verifyHash := sha256.Sum256(dataKey)
	if !equalBytes(verifyHash[:], kf.VerifyHash) {
		return nil, ErrWrongPassphrase
	}

	// Create the data cipher for actual memory encryption
	dataBlock, err := aes.NewCipher(dataKey)
	if err != nil {
		return nil, fmt.Errorf("create data cipher: %w", err)
	}
	dataGCM, err := cipher.NewGCM(dataBlock)
	if err != nil {
		return nil, fmt.Errorf("create data GCM: %w", err)
	}

	return &Vault{gcm: dataGCM, dataKey: dataKey}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM. Each call generates a
// fresh nonce. Output format: nonce (12 bytes) || ciphertext+tag.
func (v *Vault) Encrypt(plaintext []byte) ([]byte, error) {
	if v == nil || v.gcm == nil {
		return nil, ErrLocked
	}

	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := v.gcm.Seal(nil, nonce, plaintext, nil)

	// Prepend nonce to ciphertext
	out := make([]byte, nonceLen+len(ciphertext))
	copy(out[:nonceLen], nonce)
	copy(out[nonceLen:], ciphertext)
	return out, nil
}

// Decrypt decrypts data produced by Encrypt.
func (v *Vault) Decrypt(data []byte) ([]byte, error) {
	if v == nil || v.gcm == nil {
		return nil, ErrLocked
	}

	if len(data) < nonceLen+v.gcm.Overhead() {
		return nil, errors.New("ciphertext too short")
	}

	nonce := data[:nonceLen]
	ciphertext := data[nonceLen:]
	return v.gcm.Open(nil, nonce, ciphertext, nil)
}

// EncryptString encrypts a string and returns the ciphertext bytes.
func (v *Vault) EncryptString(s string) ([]byte, error) {
	return v.Encrypt([]byte(s))
}

// DecryptString decrypts ciphertext and returns the plaintext string.
func (v *Vault) DecryptString(data []byte) (string, error) {
	plaintext, err := v.Decrypt(data)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// ChangePassphrase re-encrypts the vault key with a new passphrase.
// The data key stays the same — no need to re-encrypt the database.
func ChangePassphrase(keyFilePath, oldPassphrase, newPassphrase string) error {
	// Open with old passphrase to get the data key
	v, err := Open(keyFilePath, oldPassphrase)
	if err != nil {
		return err
	}

	// Generate new salt
	salt := make([]byte, saltLen)
	if _, saltErr := io.ReadFull(rand.Reader, salt); saltErr != nil {
		return fmt.Errorf("generate salt: %w", saltErr)
	}

	// Derive new wrapping key
	wrapKey := argon2.IDKey([]byte(newPassphrase), salt, argonTime, argonMemory, argonThreads, argonKeyLen)

	block, err := aes.NewCipher(wrapKey)
	if err != nil {
		return fmt.Errorf("create cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("create GCM: %w", err)
	}

	nonce := make([]byte, nonceLen)
	if _, nonceErr := io.ReadFull(rand.Reader, nonce); nonceErr != nil {
		return fmt.Errorf("generate nonce: %w", nonceErr)
	}

	encryptedKey := gcm.Seal(nil, nonce, v.dataKey, nil)
	verifyHash := sha256.Sum256(v.dataKey)

	kf := keyFile{
		Salt:         salt,
		EncryptedKey: encryptedKey,
		Nonce:        nonce,
		VerifyHash:   verifyHash[:],
	}

	data, err := json.Marshal(kf)
	if err != nil {
		return fmt.Errorf("marshal key file: %w", err)
	}

	return os.WriteFile(keyFilePath, data, 0600)
}

// Exists returns true if a vault key file exists at the given path.
func Exists(keyFilePath string) bool {
	_, err := os.Stat(keyFilePath)
	return err == nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var result byte
	for i := range a {
		result |= a[i] ^ b[i]
	}
	return result == 0
}
