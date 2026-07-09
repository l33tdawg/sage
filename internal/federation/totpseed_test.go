package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func newTOTPSeedTestManager(t *testing.T, certsDir string, bs *store.BadgerStore) *Manager {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return NewManager(Config{
		LocalChainID: "local-chain",
		CertsDir:     certsDir,
		AgentKey:     priv,
		Badger:       bs,
		Logger:       zerolog.Nop(),
	})
}

func newTOTPSeedTestStore(t *testing.T, certsDir string) (*Manager, *store.BadgerStore) {
	t.Helper()
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	require.NoError(t, bs.SetCrossFed("peer-chain", "https://peer:8444", bytes.Repeat([]byte{7}, 32), 2, 0, []string{"*"}, nil, "active"))
	return newTOTPSeedTestManager(t, certsDir, bs), bs
}

func readTOTPSeedEnvelope(t *testing.T, path string) TOTPSeedEnvelope {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	var env TOTPSeedEnvelope
	require.NoError(t, json.Unmarshal(b, &env))
	return env
}

func TestTOTPSeedVaultPassphraseWrapsAtRestAndControlsLoad(t *testing.T) {
	certsDir := filepath.Join(t.TempDir(), "certs")
	m, bs := newTOTPSeedTestStore(t, certsDir)
	require.Equal(t, 0, m.SetVaultPassphrase("correct horse battery staple"))

	seed := []byte("0123456789abcdef0123456789abcdef")
	pinPair := bytes.Repeat([]byte{9}, 32)
	commit, rollback, err := m.stageSeed("peer-chain", seed, pinPair, 1234)
	require.NoError(t, err)
	defer rollback()
	require.NoError(t, commit())

	env := readTOTPSeedEnvelope(t, m.seedPath("peer-chain"))
	require.True(t, env.SeedEstablished)
	require.True(t, env.Encrypted)
	require.NotEqual(t, encodeB64(seed), env.Body)

	got, ok := m.currentSeed("peer-chain")
	require.True(t, ok)
	require.Equal(t, seed, got)

	locked := newTOTPSeedTestManager(t, certsDir, bs)
	require.True(t, locked.seedEstablished("peer-chain"), "plaintext header must remain readable while vault-locked")
	require.Equal(t, 0, locked.LoadSeedsIntoCache())
	_, ok = locked.currentSeed("peer-chain")
	require.False(t, ok)

	require.Equal(t, 0, m.SetVaultPassphrase("wrong passphrase"), "changing to the wrong passphrase must clear stale cached seeds")
	_, ok = m.currentSeed("peer-chain")
	require.False(t, ok)

	unlocked := newTOTPSeedTestManager(t, certsDir, bs)
	require.Equal(t, 1, unlocked.SetVaultPassphrase("correct horse battery staple"))
	got, ok = unlocked.currentSeed("peer-chain")
	require.True(t, ok)
	require.Equal(t, seed, got)
}

func TestTOTPSeedPlaintextBackwardCompatibility(t *testing.T) {
	certsDir := filepath.Join(t.TempDir(), "certs")
	m, bs := newTOTPSeedTestStore(t, certsDir)

	seed := []byte("legacy-plaintext-seed-material-32")
	pinPair := bytes.Repeat([]byte{4}, 32)
	commit, rollback, err := m.stageSeed("peer-chain", seed, pinPair, 5678)
	require.NoError(t, err)
	defer rollback()
	require.NoError(t, commit())

	env := readTOTPSeedEnvelope(t, m.seedPath("peer-chain"))
	require.True(t, env.SeedEstablished)
	require.False(t, env.Encrypted)
	require.Equal(t, encodeB64(seed), env.Body)

	plainLoader := newTOTPSeedTestManager(t, certsDir, bs)
	require.Equal(t, 1, plainLoader.LoadSeedsIntoCache())
	got, ok := plainLoader.currentSeed("peer-chain")
	require.True(t, ok)
	require.Equal(t, seed, got)

	vaultLoader := newTOTPSeedTestManager(t, certsDir, bs)
	require.Equal(t, 1, vaultLoader.SetVaultPassphrase("new vault passphrase"))
	got, ok = vaultLoader.currentSeed("peer-chain")
	require.True(t, ok)
	require.Equal(t, seed, got)
}
