package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/store"
)

// Direct canonical-store fixtures exercise readiness policy, not InitChain,
// a running node, transport listeners, or cross-process crash recovery.
func TestFreshOriginReadinessDoesNotRequireLocalRootCustody(t *testing.T) {
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bs.CloseBadger()) })
	_, root, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, agent, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, transport, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "companion.key")
	require.NoError(t, os.WriteFile(path, agent.Seed(), 0600))
	activation := store.FreshGenesisActivation{Version: store.FreshGenesisVersion, AppVersion: 30, Scope: strings.Repeat("12", 32), BootstrapDigest: strings.Repeat("34", 32), RootID: appV23AgentIDForKey(root), Agent: &store.FreshGenesisAgent{AgentID: appV23AgentIDForKey(agent), Profile: store.AppV23ProfileCompanion, HomeDomain: "private-home", Clearance: 1, Capabilities: 15}, Validators: []store.FreshGenesisValidator{{ValidatorID: hex.EncodeToString(transport.Public().(ed25519.PublicKey)), Power: 10}}}
	require.NoError(t, bs.BootstrapFreshGenesis(activation))
	boot := &VendoredAgentBootstrapConfig{AgentKeyFile: path, HomeDomain: "private-home", Clearance: 1}
	resolver := func(id string) (ed25519.PrivateKey, bool) { return transport, id == appV23AgentIDForKey(transport) }
	before, err := bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, verifyAppV23VendoredAgentReadiness(boot, resolver, bs))
	after, err := bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
	boot.HomeDomain = "wrong-home"
	require.ErrorContains(t, verifyAppV23VendoredAgentReadiness(boot, resolver, bs), "does not match")
	boot.HomeDomain = "private-home"
	require.NoError(t, os.WriteFile(path, transport.Seed(), 0600))
	require.ErrorContains(t, verifyAppV23VendoredAgentReadiness(boot, resolver, bs), "does not match")
}

func TestLegacyDirect23ReadinessRetainsRootCustodyRequirement(t *testing.T) {
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bs.CloseBadger()) })
	_, root, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, agent, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "companion.key")
	require.NoError(t, os.WriteFile(path, agent.Seed(), 0600))
	require.NoError(t, bs.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{RootID: appV23AgentIDForKey(root), AgentID: appV23AgentIDForKey(agent), Scope: strings.Repeat("12", 32), BootstrapDigest: strings.Repeat("34", 32), Profile: store.AppV23ProfileCompanion, HomeDomain: "private-home", Clearance: 1, Capabilities: 15, Height: 1, ValidatorID: strings.Repeat("56", 32), ValidatorPower: 10, ActivateAtGenesis: true}))
	boot := &VendoredAgentBootstrapConfig{AgentKeyFile: path, HomeDomain: "private-home", Clearance: 1}
	require.ErrorContains(t, verifyAppV23VendoredAgentReadiness(boot, func(string) (ed25519.PrivateKey, bool) { return nil, false }, bs), "Root credential is not available")
	require.NoError(t, verifyAppV23VendoredAgentReadiness(boot, func(id string) (ed25519.PrivateKey, bool) { return root, id == appV23AgentIDForKey(root) }, bs))
}

func TestFreshOriginReadinessRejectsMissingOrCorruptOrigin(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		name := "missing"
		if corrupt {
			name = "corrupt"
		}
		t.Run(name, func(t *testing.T) {
			bs, err := store.NewBadgerStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bs.CloseBadger()) })
			_, root, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			_, agent, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "companion.key")
			require.NoError(t, os.WriteFile(path, agent.Seed(), 0600))
			require.NoError(t, bs.BootstrapFreshGenesis(store.FreshGenesisActivation{Version: store.FreshGenesisVersion, AppVersion: 29, Scope: strings.Repeat("12", 32), BootstrapDigest: strings.Repeat("34", 32), RootID: appV23AgentIDForKey(root), Agent: &store.FreshGenesisAgent{AgentID: appV23AgentIDForKey(agent), Profile: store.AppV23ProfileCompanion, HomeDomain: "private-home", Clearance: 1, Capabilities: 15}, Validators: []store.FreshGenesisValidator{{ValidatorID: strings.Repeat("56", 32), Power: 10}}}))
			require.NoError(t, bs.DB().Update(func(txn *badger.Txn) error {
				if corrupt {
					return txn.Set([]byte(consensuskeys.FreshGenesisActivation), []byte("{"))
				}
				return txn.Delete([]byte(consensuskeys.FreshGenesisActivation))
			}))
			before, err := bs.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Error(t, verifyAppV23VendoredAgentReadiness(&VendoredAgentBootstrapConfig{AgentKeyFile: path, HomeDomain: "private-home", Clearance: 1}, func(id string) (ed25519.PrivateKey, bool) { return root, id == appV23AgentIDForKey(root) }, bs))
			after, err := bs.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Equal(t, before, after, "invalid provenance must not be repaired")
		})
	}
}
