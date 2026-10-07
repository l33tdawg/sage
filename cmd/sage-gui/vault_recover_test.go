package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

// recoveryKeyFor creates an independent vault and returns its data key (the
// base64 recovery key), i.e. a key that belongs to a DIFFERENT vault.
func recoveryKeyFor(t *testing.T, passphrase string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(path, passphrase))
	v, err := vault.Open(path, passphrase)
	require.NoError(t, err)
	key, err := v.RecoveryKey()
	require.NoError(t, err)
	return key
}

func TestRecoverVaultRefusesAnotherVaultsKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	path := filepath.Join(home, "vault.key")
	require.NoError(t, vault.Init(path, "current-passphrase"))
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	otherKey := recoveryKeyFor(t, "other-passphrase")
	err = recoverVault(otherKey, "new-passphrase")
	require.ErrorIs(t, err, vault.ErrWrongRecoveryKey)

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, before, after, "a key from another vault must never rewrite vault.key")
	_, err = vault.Open(path, "current-passphrase")
	assert.NoError(t, err, "the live vault must still open with its original passphrase")
}

func TestRecoverVaultRefusesMalformedKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	path := filepath.Join(home, "vault.key")
	require.NoError(t, vault.Init(path, "current-passphrase"))

	err := recoverVault("not-a-base64-recovery-key!", "new-passphrase")
	require.ErrorIs(t, err, vault.ErrWrongRecoveryKey)

	_, openErr := vault.Open(path, "current-passphrase")
	assert.NoError(t, openErr)
}

func TestRecoverVaultRewrapsMatchingKey(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	path := filepath.Join(home, "vault.key")
	require.NoError(t, vault.Init(path, "current-passphrase"))
	v, err := vault.Open(path, "current-passphrase")
	require.NoError(t, err)
	key, err := v.RecoveryKey()
	require.NoError(t, err)

	require.NoError(t, recoverVault(key, "new-passphrase-123"))

	_, err = vault.Open(path, "new-passphrase-123")
	assert.NoError(t, err, "the new passphrase must open the recovered vault")
	_, err = vault.Open(path, "current-passphrase")
	assert.ErrorIs(t, err, vault.ErrWrongPassphrase)
	_, statErr := os.Stat(path + ".bak")
	assert.NoError(t, statErr, "recover backs up the previous key file")
}

func TestRecoverVaultWithoutKeyFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	// A syntactically valid 32-byte key: the missing key file must be the
	// reason for the refusal.
	err := recoverVault("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "new-passphrase")
	require.ErrorContains(t, err, "no vault.key found")
}
