package vault

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitAndOpen(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	passphrase := "test-passphrase-123!"

	// Init
	err := Init(keyFile, passphrase)
	require.NoError(t, err)
	assert.FileExists(t, keyFile)

	// Open with correct passphrase
	v, err := Open(keyFile, passphrase)
	require.NoError(t, err)
	require.NotNil(t, v)

	// Open with wrong passphrase
	_, err = Open(keyFile, "wrong-passphrase")
	assert.ErrorIs(t, err, ErrWrongPassphrase)
}

func TestEncryptDecrypt(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	passphrase := "my-secret-passphrase"

	require.NoError(t, Init(keyFile, passphrase))
	v, err := Open(keyFile, passphrase)
	require.NoError(t, err)

	// Encrypt and decrypt text
	plaintext := "This is a deeply personal memory about debugging Go at 3am"
	encrypted, err := v.EncryptString(plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, []byte(plaintext), encrypted)
	assert.Greater(t, len(encrypted), len(plaintext)) // ciphertext is larger

	decrypted, err := v.DecryptString(encrypted)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestEncryptDecryptBytes(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, Init(keyFile, "pass"))
	v, err := Open(keyFile, "pass")
	require.NoError(t, err)

	// Test with embedding-like binary data
	embedding := make([]byte, 768*4) // 768-dim float32 embedding
	for i := range embedding {
		embedding[i] = byte(i % 256)
	}

	encrypted, err := v.Encrypt(embedding)
	require.NoError(t, err)

	decrypted, err := v.Decrypt(encrypted)
	require.NoError(t, err)
	assert.Equal(t, embedding, decrypted)
}

func TestUniqueNonces(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, Init(keyFile, "pass"))
	v, err := Open(keyFile, "pass")
	require.NoError(t, err)

	// Same plaintext should produce different ciphertext (different nonce)
	plaintext := []byte("same content")
	enc1, err := v.Encrypt(plaintext)
	require.NoError(t, err)
	enc2, err := v.Encrypt(plaintext)
	require.NoError(t, err)

	assert.NotEqual(t, enc1, enc2, "two encryptions of same plaintext should differ")

	// Both should decrypt to same plaintext
	dec1, _ := v.Decrypt(enc1)
	dec2, _ := v.Decrypt(enc2)
	assert.Equal(t, dec1, dec2)
}

func TestChangePassphrase(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, Init(keyFile, "old-pass"))

	// Encrypt something with old passphrase
	v1, err := Open(keyFile, "old-pass")
	require.NoError(t, err)
	encrypted, err := v1.EncryptString("important memory")
	require.NoError(t, err)

	// Change passphrase
	require.NoError(t, ChangePassphrase(keyFile, "old-pass", "new-pass"))

	// Old passphrase should fail
	_, err = Open(keyFile, "old-pass")
	assert.ErrorIs(t, err, ErrWrongPassphrase)

	// New passphrase should work
	v2, err := Open(keyFile, "new-pass")
	require.NoError(t, err)

	// Data encrypted with old vault should still decrypt (same data key)
	decrypted, err := v2.DecryptString(encrypted)
	require.NoError(t, err)
	assert.Equal(t, "important memory", decrypted)
}

func TestNilVault(t *testing.T) {
	var v *Vault
	_, err := v.Encrypt([]byte("test"))
	assert.ErrorIs(t, err, ErrLocked)

	_, err = v.Decrypt([]byte("test"))
	assert.ErrorIs(t, err, ErrLocked)
}

func TestExists(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "vault.key")

	assert.False(t, Exists(keyFile))
	require.NoError(t, Init(keyFile, "pass"))
	assert.True(t, Exists(keyFile))
}

func TestRecoveryKey(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, Init(keyFile, "test-pass"))

	v, err := Open(keyFile, "test-pass")
	require.NoError(t, err)

	// Get recovery key
	recoveryKey, err := v.RecoveryKey()
	require.NoError(t, err)
	assert.NotEmpty(t, recoveryKey)

	// Encrypt something
	encrypted, err := v.EncryptString("secret memory")
	require.NoError(t, err)

	// Simulate lost passphrase — re-init from recovery key with new passphrase
	keyFile2 := filepath.Join(t.TempDir(), "vault2.key")
	require.NoError(t, InitFromRecoveryKey(keyFile2, recoveryKey, "new-pass"))

	// Open with new passphrase
	v2, err := Open(keyFile2, "new-pass")
	require.NoError(t, err)

	// Should decrypt data encrypted with original vault (same data key)
	decrypted, err := v2.DecryptString(encrypted)
	require.NoError(t, err)
	assert.Equal(t, "secret memory", decrypted)
}

func TestRecoveryKeyNilVault(t *testing.T) {
	var v *Vault
	_, err := v.RecoveryKey()
	assert.ErrorIs(t, err, ErrLocked)
}

func TestInitFromRecoveryKeyInvalid(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")

	// Invalid base64
	err := InitFromRecoveryKey(keyFile, "not-valid-base64!!!", "pass")
	assert.Error(t, err)

	// Wrong length
	err = InitFromRecoveryKey(keyFile, "dG9vc2hvcnQ=", "pass")
	assert.Error(t, err)
}

func TestKeyFilePermissions(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, Init(keyFile, "pass"))

	info, err := os.Stat(keyFile)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm(), "key file should be owner-only")
}

func TestInitRefusesToOverwrite(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, Init(keyFile, "first-pass"))

	// Encrypt something with the first key
	v1, err := Open(keyFile, "first-pass")
	require.NoError(t, err)
	encrypted, err := v1.EncryptString("precious memory")
	require.NoError(t, err)

	// Second Init must fail — refusing to overwrite
	err = Init(keyFile, "second-pass")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")

	// Original key must still work
	v2, err := Open(keyFile, "first-pass")
	require.NoError(t, err)
	decrypted, err := v2.DecryptString(encrypted)
	require.NoError(t, err)
	assert.Equal(t, "precious memory", decrypted)
}
