package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// autoImportSignerSelectsTheEnrolledCompanion is the regression guard for the
// v11.25.1 "Imported 0/N" fresh-node import: with a first-party Companion
// bootstrap configured, the import must sign as that Companion (an active
// ordinary agent), never as the genesis Root, which post-v23 embedding and
// task submission reject.
func TestAutoImportSignerSelectsTheEnrolledCompanion(t *testing.T) {
	home := t.TempDir()
	rootPath := filepath.Join(home, "agent.key")
	companionPath := filepath.Join(home, "agents", "companion", "agent.key")
	require.NoError(t, os.MkdirAll(filepath.Dir(companionPath), 0o700))
	rootSeed := bytes.Repeat([]byte{0x01}, ed25519.SeedSize)
	companionSeed := bytes.Repeat([]byte{0x02}, ed25519.SeedSize)
	require.NoError(t, os.WriteFile(rootPath, rootSeed, 0o600))
	require.NoError(t, os.WriteFile(companionPath, companionSeed, 0o600))

	rootID := hex.EncodeToString(ed25519.NewKeyFromSeed(rootSeed).Public().(ed25519.PublicKey))
	companionID := hex.EncodeToString(ed25519.NewKeyFromSeed(companionSeed).Public().(ed25519.PublicKey))

	t.Run("companion bootstrap signs as the companion", func(t *testing.T) {
		cfg := &Config{
			AgentKey: rootPath,
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
				AgentKeyFile: companionPath,
			},
		}
		priv, agentID, keyPath, err := autoImportSigner(cfg)
		require.NoError(t, err)
		require.Equal(t, companionPath, keyPath)
		require.Equal(t, companionID, agentID)
		require.NotEqual(t, rootID, agentID, "the genesis Root is not an agent post-v23")
		require.Equal(t, companionSeed, priv.Seed())
	})

	t.Run("no companion bootstrap keeps the Root signer", func(t *testing.T) {
		cfg := &Config{AgentKey: rootPath}
		priv, agentID, keyPath, err := autoImportSigner(cfg)
		require.NoError(t, err)
		require.Equal(t, rootPath, keyPath)
		require.Equal(t, rootID, agentID)
		require.Equal(t, rootSeed, priv.Seed())
	})

	t.Run("an unqualified entry lands in the companion home domain", func(t *testing.T) {
		cfg := &Config{
			AgentKey: rootPath,
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
				AgentKeyFile: companionPath,
				HomeDomain:   "companion-home",
			},
		}
		require.Equal(t, "companion-home", autoImportDefaultDomain(cfg))
		require.Equal(t, "imported", autoImportDefaultDomain(&Config{AgentKey: rootPath}))
		require.Equal(t, "imported", autoImportDefaultDomain(&Config{
			AgentKey:               rootPath,
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{HomeDomain: "  "},
		}))
	})

	t.Run("a missing companion key is an error naming that path", func(t *testing.T) {
		missing := filepath.Join(home, "agents", "absent", "agent.key")
		cfg := &Config{
			AgentKey: rootPath,
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
				AgentKeyFile: missing,
			},
		}
		priv, agentID, keyPath, err := autoImportSigner(cfg)
		require.Error(t, err)
		require.Nil(t, priv)
		require.Empty(t, agentID)
		require.Equal(t, missing, keyPath)
	})
}
