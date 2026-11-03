//go:build !windows

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	sageabci "github.com/l33tdawg/sage/internal/abci"
)

type preparedFreshNodeFixture struct {
	home      string
	cfg       *Config
	genesis   *cmttypes.GenesisDoc
	digest    string
	root      ed25519.PrivateKey
	companion ed25519.PrivateKey
	validator *privval.FilePV
}

func newPreparedFreshNodeFixture(t *testing.T, target uint64, count int, companion, local bool) preparedFreshNodeFixture {
	t.Helper()
	home := t.TempDir()
	require.NoError(t, os.Chmod(home, 0700))
	t.Setenv("SAGE_HOME", home)
	for _, name := range []string{"SAGE_VENDORED_AGENT_KEY_FILE", "SAGE_VENDORED_AGENT_HOME_DOMAIN", "SAGE_VENDORED_AGENT_CLEARANCE", "SAGE_INSTANCE_LOCK_FD"} {
		t.Setenv(name, "")
	}
	cfg := DefaultConfig(home)
	cfg.ChainID = "fresh-node-fixture"
	cfg.Quorum.Enabled = count > 1
	base := filepath.Join(cfg.DataDir, "cometbft")
	require.NoError(t, os.MkdirAll(filepath.Join(base, "config"), 0700))
	require.NoError(t, os.MkdirAll(filepath.Join(base, "data"), 0700))
	validator := privval.GenFilePV(filepath.Join(base, "config", "priv_validator_key.json"), filepath.Join(base, "data", "priv_validator_state.json"))
	validator.Save()
	_, root, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, agent, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	var boot *VendoredAgentBootstrapConfig
	if companion {
		boot = &VendoredAgentBootstrapConfig{AgentKeyFile: filepath.Join(home, "companion.key"), HomeDomain: "private-home", Clearance: 1}
		if local {
			cfg.VendoredAgentBootstrap = boot
			require.NoError(t, os.WriteFile(boot.AgentKeyFile, agent.Seed(), 0600))
		}
	}
	roster := []sageabci.FreshGenesisValidator{{ValidatorID: hex.EncodeToString(validator.Key.PubKey.Bytes()), Power: 10}}
	validators := []cmttypes.GenesisValidator{{Address: validator.Key.Address, PubKey: validator.Key.PubKey, Power: 10}}
	for i := 1; i < count; i++ {
		key := cmtcryptoed.GenPrivKey()
		roster = append(roster, sageabci.FreshGenesisValidator{ValidatorID: hex.EncodeToString(key.PubKey().Bytes()), Power: 10})
		validators = append(validators, cmttypes.GenesisValidator{Address: key.PubKey().Address(), PubKey: key.PubKey(), Power: 10})
	}
	state, err := freshGenesisAppStateWithKeys(cfg.ChainID, target, root, agent, boot, roster)
	require.NoError(t, err)
	params := cmttypes.DefaultConsensusParams()
	params.Version.App = target
	genesis := &cmttypes.GenesisDoc{ChainID: cfg.ChainID, GenesisTime: time.Unix(1700000000, 0).UTC(), InitialHeight: 1, ConsensusParams: params, Validators: validators, AppState: state}
	genesisBytes, err := cmtjson.Marshal(genesis)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(base, "config", "genesis.json"), genesisBytes, 0600))
	fixture := preparedFreshNodeFixture{home: home, cfg: cfg, genesis: genesis, digest: freshNodeSHA(genesisBytes), root: root, companion: agent, validator: validator}
	fixture.saveConfig(t)
	return fixture
}

func (f preparedFreshNodeFixture) saveConfig(t *testing.T) {
	t.Helper()
	// Use the actual YAML encoder used by the configuration contract, not JSON
	// assumptions about yaml field naming.
	data, err := yamlMarshalFreshNodeConfig(f.cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(f.home, "config.yaml"), data, 0600))
}

func freshNodeFixtureFiles(t *testing.T, home string) map[string]string {
	t.Helper()
	rows := map[string]string{}
	require.NoError(t, filepath.WalkDir(home, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(home, path)
		if err != nil {
			return err
		}
		rows[rel] = freshNodeSHA(data)
		return nil
	}))
	return rows
}

func TestFreshNodeInitializationBindsSharedArtifactAndKeepsPerNodeIdentity(t *testing.T) {
	for _, tc := range []struct {
		name             string
		target           uint64
		count            int
		companion, local bool
	}{
		{"supported-latest-root-only", 29, 1, false, false},
		{"compiled-app30-root-only", 30, 1, false, false},
		{"quorum-remote-companion", 30, 4, true, false},
		{"personal-local-companion", 29, 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPreparedFreshNodeFixture(t, tc.target, tc.count, tc.companion, tc.local)
			genesisPath := filepath.Join(f.cfg.DataDir, "cometbft", "config", "genesis.json")
			genesisBefore, err := os.ReadFile(genesisPath)
			require.NoError(t, err)
			require.NoError(t, initializeFreshNode(f.home, f.digest))
			transport, err := readFederationTransportKey(f.cfg.AgentKey)
			require.NoError(t, err)
			require.NotEqual(t, appV23AgentIDForKey(f.root), appV23AgentIDForKey(transport))
			require.NotEqual(t, hex.EncodeToString(f.validator.Key.PubKey.Bytes()), appV23AgentIDForKey(transport))
			genesisAfter, err := os.ReadFile(genesisPath)
			require.NoError(t, err)
			require.Equal(t, genesisBefore, genesisAfter)
			require.NoError(t, preflightFreshNodeInitialization(f.home, f.cfg))
			handled, err := preflightExistingFreshVendoredGenesis(filepath.Join(f.cfg.DataDir, "cometbft"), f.cfg, f.genesis)
			require.True(t, handled)
			require.NoError(t, err)
			before := freshNodeFixtureFiles(t, f.home)
			require.NoError(t, initializeFreshNode(f.home, f.digest))
			require.Equal(t, before, freshNodeFixtureFiles(t, f.home), "exact pristine retry must not write")
		})
	}
	latest, err := freshGenesisVersion("latest")
	require.NoError(t, err)
	require.Equal(t, uint64(29), latest, "compiled dormant30 must not change release-supported latest")
}

func TestFreshNodeInitializationPreservesExistingTransportAndPeerKey(t *testing.T) {
	f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(f.cfg.AgentKey, key.Seed(), 0600))
	node := p2pNodeKeyForFreshNodeTest(t, f)
	require.NoError(t, initializeFreshNode(f.home, f.digest))
	got, err := os.ReadFile(f.cfg.AgentKey)
	require.NoError(t, err)
	require.Equal(t, key.Seed(), got)
	got, err = os.ReadFile(filepath.Join(f.cfg.DataDir, "cometbft", "config", "node_key.json"))
	require.NoError(t, err)
	require.Equal(t, node, got)
}

func TestFreshNodeInitializationRejectsUsedRecoveryAndMismatchedInputs(t *testing.T) {
	for _, name := range []string{"wrong-digest", "used-signing-state", "bad-roster", "quorum-disabled", "state-sync", "badger", "sqlite", "backup", "symlink", "hardlink", "root-transport", "companion-mismatch", "missing-signing-state", "partial-claim", "oversized-config"} {
		t.Run(name, func(t *testing.T) {
			count := 4
			if name == "companion-mismatch" {
				count = 1
			}
			f := newPreparedFreshNodeFixture(t, 30, count, name == "companion-mismatch", false)
			expected := f.digest
			switch name {
			case "wrong-digest":
				expected = freshNodeSHA([]byte("different artifact"))
			case "used-signing-state":
				vote := &cmtproto.Vote{Type: cmtproto.PrevoteType, Height: 1, Round: 0, Timestamp: time.Unix(1700000001, 0).UTC()}
				require.NoError(t, f.validator.SignVote(f.cfg.ChainID, vote))
			case "bad-roster":
				f.genesis.Validators[0].Power++
				data, err := cmtjson.Marshal(f.genesis)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "cometbft", "config", "genesis.json"), data, 0600))
				expected = freshNodeSHA(data)
			case "quorum-disabled":
				f.cfg.Quorum.Enabled = false
				f.saveConfig(t)
			case "state-sync":
				f.cfg.Quorum.StateSync.Received = true
				f.saveConfig(t)
			case "badger":
				require.NoError(t, os.Mkdir(filepath.Join(f.cfg.DataDir, "badger"), 0700))
			case "sqlite":
				require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "sage.db"), nil, 0600))
			case "backup":
				require.NoError(t, os.Mkdir(filepath.Join(f.home, "backups"), 0700))
			case "symlink":
				require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "unused"), f.cfg.AgentKey))
			case "hardlink":
				path := filepath.Join(t.TempDir(), "key")
				require.NoError(t, os.WriteFile(path, f.root.Seed(), 0600))
				require.NoError(t, os.Link(path, f.cfg.AgentKey))
			case "root-transport":
				require.NoError(t, os.WriteFile(f.cfg.AgentKey, f.root.Seed(), 0600))
			case "companion-mismatch":
				f.cfg.Quorum.Enabled = false
				f.cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{AgentKeyFile: filepath.Join(f.home, "wrong.key"), HomeDomain: "private-home", Clearance: 1}
				require.NoError(t, os.WriteFile(f.cfg.VendoredAgentBootstrap.AgentKeyFile, f.root.Seed(), 0600))
				f.saveConfig(t)
			case "missing-signing-state":
				require.NoError(t, os.Remove(filepath.Join(f.cfg.DataDir, "cometbft", "data", "priv_validator_state.json")))
			case "partial-claim":
				require.NoError(t, os.WriteFile(filepath.Join(f.home, freshNodeClaim), []byte("{}"), 0600))
			case "oversized-config":
				require.NoError(t, os.WriteFile(filepath.Join(f.home, "config.yaml"), make([]byte, freshNodeMaxFile+1), 0600))
			}
			require.Error(t, initializeFreshNode(f.home, expected))
			_, err := os.Lstat(filepath.Join(f.home, freshNodeReady))
			require.True(t, os.IsNotExist(err), "refusal must not publish completion")
			_, err = os.Lstat(filepath.Join(f.home, versionFile))
			require.True(t, os.IsNotExist(err), "refusal must not stamp a version")
		})
	}
}

func TestFreshNodeCompletedStartupAllowsRuntimeUseButInitializerRetryRefuses(t *testing.T) {
	f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
	require.NoError(t, initializeFreshNode(f.home, f.digest))
	vote := &cmtproto.Vote{Type: cmtproto.PrevoteType, Height: 1, Round: 0, Timestamp: time.Unix(1700000001, 0).UTC()}
	require.NoError(t, f.validator.SignVote(f.cfg.ChainID, vote))
	require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "sage.db"), []byte("runtime evidence"), 0600))
	f.cfg.RESTAddr = "127.0.0.1:18080"
	f.saveConfig(t)
	require.NoError(t, preflightFreshNodeInitialization(f.home, f.cfg), "receipt validation is not a pristine-state test on later startup")
	before := freshNodeFixtureFiles(t, f.home)
	require.Error(t, initializeFreshNode(f.home, f.digest))
	require.Equal(t, before, freshNodeFixtureFiles(t, f.home))
}

func TestFreshNodeStartupRefusesUnfinishedAndCorruptCompletion(t *testing.T) {
	for _, name := range []string{"claim-only", "marker-before-ready", "corrupt-ready", "missing-claim", "changed-genesis", "changed-transport", "missing-marker"} {
		t.Run(name, func(t *testing.T) {
			f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
			if name == "claim-only" || name == "marker-before-ready" {
				require.NoError(t, os.WriteFile(filepath.Join(f.home, freshNodeClaim), []byte("{}"), 0600))
				if name == "marker-before-ready" {
					require.NoError(t, os.WriteFile(filepath.Join(f.home, versionFile), []byte("partial\n"), 0600))
				}
			} else {
				require.NoError(t, initializeFreshNode(f.home, f.digest))
				switch name {
				case "corrupt-ready":
					require.NoError(t, os.WriteFile(filepath.Join(f.home, freshNodeReady), []byte("{"), 0600))
				case "missing-claim":
					require.NoError(t, os.Remove(filepath.Join(f.home, freshNodeClaim)))
				case "changed-genesis":
					require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "cometbft", "config", "genesis.json"), []byte("{}"), 0600))
				case "changed-transport":
					require.NoError(t, os.WriteFile(f.cfg.AgentKey, f.companion.Seed(), 0600))
				case "missing-marker":
					require.NoError(t, os.Remove(filepath.Join(f.home, forkVersionFile)))
				}
			}
			before := freshNodeFixtureFiles(t, f.home)
			require.Error(t, preflightFreshNodeInitialization(f.home, f.cfg))
			require.Equal(t, before, freshNodeFixtureFiles(t, f.home), "startup never repairs partial initialization")
		})
	}
}

func TestFreshNodeCompletedStartupRecoversChainIDCacheWithoutWritingConfig(t *testing.T) {
	f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
	f.cfg.ChainID = ""
	f.saveConfig(t)
	require.NoError(t, initializeFreshNode(f.home, f.digest))
	before, err := os.ReadFile(filepath.Join(f.home, "config.yaml"))
	require.NoError(t, err)
	require.NoError(t, preflightFreshNodeInitialization(f.home, f.cfg))
	require.Equal(t, f.genesis.ChainID, f.cfg.ChainID)
	after, err := os.ReadFile(filepath.Join(f.home, "config.yaml"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestFreshNodeInitializerSharesServeLock(t *testing.T) {
	f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
	lock, err := acquireInstanceLock(f.home)
	require.NoError(t, err)
	defer lock.Close()
	require.ErrorContains(t, initializeFreshNode(f.home, f.digest), "already locked")
	_, err = os.Lstat(filepath.Join(f.home, freshNodeClaim))
	require.True(t, os.IsNotExist(err))
}

func yamlMarshalFreshNodeConfig(cfg *Config) ([]byte, error) { return yaml.Marshal(cfg) }
func p2pNodeKeyForFreshNodeTest(t *testing.T, f preparedFreshNodeFixture) []byte {
	t.Helper()
	key := p2p.NodeKey{PrivKey: cmtcryptoed.GenPrivKey()}
	data, err := cmtjson.Marshal(key)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "cometbft", "config", "node_key.json"), data, 0600))
	return data
}

// A completed root-only node does not run the local-Companion preflight. Its
// receipt check must validate the mutable signing state itself, without repair.
func TestFreshNodeCompletedStartupValidatesSigningStateBytes(t *testing.T) {
	for _, name := range []string{"invalid-json", "missing-fields", "negative-height", "invalid-step", "missing-signbytes", "bad-signature", "wrong-chain", "mismatched-height"} {
		t.Run(name, func(t *testing.T) {
			f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
			require.NoError(t, initializeFreshNode(f.home, f.digest))
			chainID := f.cfg.ChainID
			if name == "wrong-chain" {
				chainID = "other-chain"
			}
			vote := &cmtproto.Vote{Type: cmtproto.PrevoteType, Height: 2, Round: 0, Timestamp: time.Unix(1700000002, 0).UTC()}
			require.NoError(t, f.validator.SignVote(chainID, vote))
			if name != "wrong-chain" {
				require.NoError(t, preflightFreshNodeInitialization(f.home, f.cfg), "valid advanced state must remain usable")
			}
			state := f.validator.LastSignState
			switch name {
			case "negative-height":
				state.Height = -1
			case "invalid-step":
				state.Step = 0
			case "missing-signbytes":
				state.SignBytes = nil
			case "bad-signature":
				state.Signature = append([]byte(nil), state.Signature...)
				state.Signature[0] ^= 1
			case "mismatched-height":
				state.Height++
			}
			data, err := cmtjson.Marshal(state)
			require.NoError(t, err)
			if name == "invalid-json" {
				data = []byte("{")
			}
			if name == "missing-fields" {
				data = []byte("{}")
			}
			require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "cometbft", "data", "priv_validator_state.json"), data, 0600))
			before := freshNodeFixtureFiles(t, f.home)
			require.ErrorContains(t, preflightFreshNodeInitialization(f.home, f.cfg), "verify fresh-node signing state")
			require.Equal(t, before, freshNodeFixtureFiles(t, f.home), "invalid signing state must never be repaired")
		})
	}
}

// A signed typed manifest is insufficient when its enclosing artifact cannot
// pass InitChain's existing fresh-origin decoder. Reject it before any claim.
func TestFreshNodeInitializationMatchesInitChainManifestEnvelope(t *testing.T) {
	for _, name := range []string{"unknown-manifest-field", "coexisting-old-bootstrap", "oversized-app-state"} {
		t.Run(name, func(t *testing.T) {
			f := newPreparedFreshNodeFixture(t, 29, 1, false, false)
			var state map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(f.genesis.AppState, &state))
			var sage map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(state["sage"], &sage))
			expectedError := ""
			switch name {
			case "unknown-manifest-field":
				var manifest map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(sage["fresh_bootstrap"], &manifest))
				manifest["unknown_field"] = json.RawMessage("true")
				data, err := json.Marshal(manifest)
				require.NoError(t, err)
				sage["fresh_bootstrap"] = data
				expectedError = "unknown field"
			case "coexisting-old-bootstrap":
				sage["app_v23_bootstrap"] = json.RawMessage("{}")
				expectedError = "cannot coexist"
			case "oversized-app-state":
				data, err := json.Marshal(strings.Repeat("x", 128<<10))
				require.NoError(t, err)
				state["padding"] = data
				expectedError = "128 KiB"
			}
			data, err := json.Marshal(sage)
			require.NoError(t, err)
			state["sage"] = data
			f.genesis.AppState, err = json.Marshal(state)
			require.NoError(t, err)
			artifact, err := cmtjson.Marshal(f.genesis)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(f.cfg.DataDir, "cometbft", "config", "genesis.json"), artifact, 0600))
			before := freshNodeFixtureFiles(t, f.home)
			require.ErrorContains(t, initializeFreshNode(f.home, freshNodeSHA(artifact)), expectedError)
			after := freshNodeFixtureFiles(t, f.home)
			for path, digest := range before {
				require.Equal(t, digest, after[path], "input %s changed", path)
			}
			for _, path := range []string{freshNodeClaim, freshNodeReady, versionFile, forkVersionFile} {
				_, err := os.Lstat(filepath.Join(f.home, path))
				require.True(t, os.IsNotExist(err), "refusal must not publish %s", path)
			}
		})
	}
}
