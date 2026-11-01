package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/privval"
	cryptoproto "github.com/cometbft/cometbft/proto/tendermint/crypto"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestVendoredAgentBootstrapConfigFromEnvironment(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	t.Setenv("SAGE_VENDORED_AGENT_KEY_FILE", "agents/mynah/agent.key")
	t.Setenv("SAGE_VENDORED_AGENT_HOME_DOMAIN", "voice-interface")
	t.Setenv("SAGE_VENDORED_AGENT_CLEARANCE", "2")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg.VendoredAgentBootstrap)
	require.Equal(t, filepath.Join(home, "agents", "mynah", "agent.key"), cfg.VendoredAgentBootstrap.AgentKeyFile)
	require.Equal(t, "voice-interface", cfg.VendoredAgentBootstrap.HomeDomain)
	require.Equal(t, uint8(2), cfg.VendoredAgentBootstrap.Clearance)
}

func TestVendoredAgentBootstrapAcceptsPublicOnlyClearance(t *testing.T) {
	t.Setenv("SAGE_HOME", t.TempDir())
	t.Setenv("SAGE_VENDORED_AGENT_KEY_FILE", "agents/public/agent.key")
	t.Setenv("SAGE_VENDORED_AGENT_HOME_DOMAIN", "public-agent")
	t.Setenv("SAGE_VENDORED_AGENT_CLEARANCE", "0")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.NotNil(t, cfg.VendoredAgentBootstrap)
	require.Zero(t, cfg.VendoredAgentBootstrap.Clearance)
}

func TestVendoredAgentBootstrapConfigFailsClosedWhenPartial(t *testing.T) {
	t.Setenv("SAGE_HOME", t.TempDir())
	t.Setenv("SAGE_VENDORED_AGENT_KEY_FILE", "agents/mynah/agent.key")
	_, err := LoadConfig()
	require.ErrorContains(t, err, "home_domain is required")
}

func TestVendoredAgentBootstrapConfigNormalizesAndRejectsSharedHomeDomain(t *testing.T) {
	cfg := DefaultConfig(t.TempDir())
	cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{
		AgentKeyFile: "  agents/mynah/agent.key  ",
		HomeDomain:   "  voice-interface  ",
		Clearance:    1,
	}
	require.NoError(t, cfg.validate())
	require.Equal(t, "agents/mynah/agent.key", cfg.VendoredAgentBootstrap.AgentKeyFile)
	require.Equal(t, "voice-interface", cfg.VendoredAgentBootstrap.HomeDomain)
	cfg.VendoredAgentBootstrap.HomeDomain = "general"
	require.ErrorContains(t, cfg.validate(), "must be a non-shared domain")
}

func TestVendoredAgentBootstrapRequiresPersonalModeAndVoter(t *testing.T) {
	validBootstrap := func() *VendoredAgentBootstrapConfig {
		return &VendoredAgentBootstrapConfig{
			AgentKeyFile: "agents/mynah/agent.key",
			HomeDomain:   "voice-interface",
			Clearance:    1,
		}
	}

	t.Run("quorum mode rejected", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())
		cfg.VendoredAgentBootstrap = validBootstrap()
		cfg.Quorum.Enabled = true
		require.ErrorContains(t, cfg.validate(), "requires personal mode")
	})

	t.Run("disabled voter rejected", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())
		cfg.VendoredAgentBootstrap = validBootstrap()
		cfg.Voter.Enabled = false
		require.ErrorContains(t, cfg.validate(), "requires voter.enabled=true")
	})

	t.Run("generic config remains operator controlled", func(t *testing.T) {
		cfg := DefaultConfig(t.TempDir())
		cfg.Quorum.Enabled = true
		cfg.Voter.Enabled = false
		require.NoError(t, cfg.validate())
	})
}

func vendoredGenesisFixture(
	t *testing.T,
) (*cmttypes.GenesisDoc, *VendoredAgentBootstrapConfig, string) {
	t.Helper()
	cometHome := t.TempDir()
	sageHome := t.TempDir()
	bootstrap := &VendoredAgentBootstrapConfig{
		AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
		HomeDomain:   "voice-interface",
		Clearance:    1,
	}
	rootKeyPath := filepath.Join(sageHome, "agent.key")
	require.NoError(t, initCometBFTConfigWithBootstrap(
		cometHome,
		rootKeyPath,
		bootstrap,
	))
	genesis, err := cmttypes.GenesisDocFromFile(
		filepath.Join(cometHome, "config", "genesis.json"),
	)
	require.NoError(t, err)
	require.Equal(t, sageabci.AppV23GenesisAppVersion, genesis.ConsensusParams.Version.App)
	return genesis, bootstrap, rootKeyPath
}

func exactVendoredKeyResolver(paths ...string) func(string) (ed25519.PrivateKey, bool) {
	keys := make(map[string]ed25519.PrivateKey, len(paths))
	for _, path := range paths {
		key, ok := parseKeyFile(path)
		if !ok {
			continue
		}
		keys[appV23AgentIDForKey(key)] = key
	}
	return func(agentID string) (ed25519.PrivateKey, bool) {
		key, ok := keys[agentID]
		return key, ok
	}
}

func TestVendoredGenesisPreflightBindsSignedValidatorAndPolicy(t *testing.T) {
	tests := []struct {
		name                 string
		tamperGenesis        func(*testing.T, *cmttypes.GenesisDoc, *VendoredAgentBootstrapConfig)
		tamperConfig         func(*Config)
		tamperLocalValidator func(*testing.T, string)
	}{
		{name: "exact"},
		{
			name: "validator key",
			tamperGenesis: func(t *testing.T, genesis *cmttypes.GenesisDoc, _ *VendoredAgentBootstrapConfig) {
				replacement := cmtcryptoed.GenPrivKey().PubKey()
				genesis.Validators[0].PubKey = replacement
				genesis.Validators[0].Address = replacement.Address()
			},
		},
		{
			name: "validator power",
			tamperGenesis: func(_ *testing.T, genesis *cmttypes.GenesisDoc, _ *VendoredAgentBootstrapConfig) {
				genesis.Validators[0].Power++
			},
		},
		{
			name: "configured home",
			tamperGenesis: func(_ *testing.T, _ *cmttypes.GenesisDoc, bootstrap *VendoredAgentBootstrapConfig) {
				bootstrap.HomeDomain = "different-home"
			},
		},
		{
			name: "configured chain",
			tamperConfig: func(cfg *Config) {
				cfg.ChainID += "-different"
			},
		},
		{
			name: "local validator key",
			tamperLocalValidator: func(_ *testing.T, home string) {
				replacement := privval.GenFilePV(
					filepath.Join(home, "config", "priv_validator_key.json"),
					filepath.Join(home, "data", "priv_validator_state.json"),
				)
				replacement.Save()
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			home := t.TempDir()
			sageHome := t.TempDir()
			bootstrap := &VendoredAgentBootstrapConfig{
				AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
				HomeDomain:   "voice-interface", Clearance: 1,
			}
			rootKeyPath := filepath.Join(sageHome, "agent.key")
			require.NoError(t, initCometBFTConfigWithBootstrap(home, rootKeyPath, bootstrap))
			genesisPath := filepath.Join(home, "config", "genesis.json")
			genesis, err := cmttypes.GenesisDocFromFile(genesisPath)
			require.NoError(t, err)
			if testCase.tamperGenesis != nil {
				testCase.tamperGenesis(t, genesis, bootstrap)
				require.NoError(t, genesis.ValidateAndComplete())
				require.NoError(t, genesis.SaveAs(genesisPath))
			}
			if testCase.tamperLocalValidator != nil {
				testCase.tamperLocalValidator(t, home)
			}
			before, err := os.ReadFile(genesisPath)
			require.NoError(t, err)
			validatorKeyPath := filepath.Join(home, "config", "priv_validator_key.json")
			beforeValidatorKey, err := os.ReadFile(validatorKeyPath)
			require.NoError(t, err)
			cfg := &Config{
				AgentKey:               rootKeyPath,
				ChainID:                genesis.ChainID,
				VendoredAgentBootstrap: bootstrap,
			}
			if testCase.tamperConfig != nil {
				testCase.tamperConfig(cfg)
			}
			err = preflightExistingVendoredGenesis(home, cfg)
			if testCase.name == "exact" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			after, readErr := os.ReadFile(genesisPath)
			require.NoError(t, readErr)
			require.Equal(t, before, after, "preflight must be read-only")
			afterValidatorKey, readErr := os.ReadFile(validatorKeyPath)
			require.NoError(t, readErr)
			require.Equal(t, beforeValidatorKey, afterValidatorKey, "preflight must not rewrite validator identity")
		})
	}
}

func vendoredPreflightFixture(
	t *testing.T,
) (string, string, *Config, *cmttypes.GenesisDoc) {
	t.Helper()
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)
	dataDir := t.TempDir()
	cometHome := filepath.Join(dataDir, "cometbft")
	bootstrap := &VendoredAgentBootstrapConfig{
		AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
		HomeDomain:   "voice-interface",
		Clearance:    1,
	}
	rootKeyPath := filepath.Join(sageHome, "agent.key")
	require.NoError(t, initCometBFTConfigWithBootstrap(
		cometHome,
		rootKeyPath,
		bootstrap,
	))
	genesis, err := cmttypes.GenesisDocFromFile(
		filepath.Join(cometHome, "config", "genesis.json"),
	)
	require.NoError(t, err)
	return sageHome, cometHome, &Config{
		DataDir:                dataDir,
		AgentKey:               rootKeyPath,
		ChainID:                genesis.ChainID,
		VendoredAgentBootstrap: bootstrap,
	}, genesis
}

func TestVendoredMissingGenesisRequiresGenuinelyFreshOrigin(t *testing.T) {
	tests := []struct {
		name string
		path func(sageHome, dataDir, cometHome string) string
	}{
		{"Badger state", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "badger", "MANIFEST")
		}},
		{"SQLite", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "sage.db")
		}},
		{"SQLite WAL", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "sage.db-wal")
		}},
		{"SQLite rollback journal", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "sage.db-journal")
		}},
		{"Comet block store", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "data", "blockstore.db")
		}},
		{"Comet state store", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "data", "state.db")
		}},
		{"Comet transaction index", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "data", "tx_index.db")
		}},
		{"Comet evidence store", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "data", "evidence.db")
		}},
		{"Comet consensus WAL", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "data", "cs.wal", "wal")
		}},
		{"genesis backup", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "config", "genesis.json.bak")
		}},
		{"pre-remint genesis backup", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "config", "genesis.json.pre-remint.bak")
		}},
		{"interrupted genesis rewrite", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "config", "genesis.json.tmp")
		}},
		{"validator key", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "config", "priv_validator_key.json")
		}},
		{"validator state", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "data", "priv_validator_state.json")
		}},
		{"peer key", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "config", "node_key.json")
		}},
		{"Comet config", func(_, _, cometHome string) string {
			return filepath.Join(cometHome, "config", "config.toml")
		}},
		{"activation journal", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, stateSyncActivationJournalName)
		}},
		{"state sync working state", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "state-sync", "receiving", "sentinel")
		}},
		{"canonical snapshot", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "snapshots", "42", "manifest.json")
		}},
		{"activation prepared directory", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "badger.state-sync-prepared-test", "sentinel")
		}},
		{"activation quarantine directory", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "badger.state-sync-quarantine-test", "sentinel")
		}},
		{"recovery backup", func(sageHome, _, _ string) string {
			return filepath.Join(sageHome, "backups", "pre-upgrade.db")
		}},
		{"custom data-directory recovery backup", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "..", "backups", "sage-pre-redeploy.db")
		}},
		{"post-crash HALT", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, "HALT")
		}},
		{"interrupted post-crash HALT", func(_, dataDir, _ string) string {
			return filepath.Join(dataDir, ".HALT.tmp")
		}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			sageHome := t.TempDir()
			t.Setenv("SAGE_HOME", sageHome)
			dataDir := t.TempDir()
			cometHome := filepath.Join(dataDir, "cometbft")
			sentinelPath := testCase.path(sageHome, dataDir, cometHome)
			require.NoError(t, os.MkdirAll(filepath.Dir(sentinelPath), 0o700))
			before := []byte("recovery-sentinel-" + testCase.name)
			require.NoError(t, os.WriteFile(sentinelPath, before, 0o600))
			cfg := &Config{
				DataDir:  dataDir,
				AgentKey: filepath.Join(sageHome, "agent.key"),
				VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
					AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
					HomeDomain:   "voice-interface",
					Clearance:    1,
				},
			}

			err := preflightVendoredStartup(cometHome, cfg)
			require.ErrorContains(t, err, "genesis is missing")
			after, readErr := os.ReadFile(sentinelPath)
			require.NoError(t, readErr)
			require.Equal(t, before, after, "preflight must leave recovery evidence byte-identical")
			require.NoFileExists(t, cfg.AgentKey)
			require.NoFileExists(t, cfg.VendoredAgentBootstrap.AgentKeyFile)
			require.Empty(t, cfg.ChainID)
		})
	}

	t.Run("empty origin remains read only and admissible", func(t *testing.T) {
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		dataDir := t.TempDir()
		cometHome := filepath.Join(dataDir, "cometbft")
		cfg := &Config{
			DataDir:  dataDir,
			AgentKey: filepath.Join(sageHome, "agent.key"),
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
				AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
				HomeDomain:   "voice-interface",
				Clearance:    1,
			},
		}
		require.NoError(t, preflightVendoredStartup(cometHome, cfg))
		require.NoDirExists(t, cometHome)
		require.NoFileExists(t, cfg.AgentKey)
		require.NoFileExists(t, cfg.VendoredAgentBootstrap.AgentKeyFile)
	})

	t.Run("installation and process markers alone do not invent a prior identity", func(t *testing.T) {
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		dataDir := t.TempDir()
		cometHome := filepath.Join(dataDir, "cometbft")
		for path, value := range map[string]string{
			filepath.Join(sageHome, versionFile):     "11.15.0\n",
			filepath.Join(sageHome, forkVersionFile): "1\n",
			filepath.Join(sageHome, "sage.pid"):      "999999\n",
		} {
			require.NoError(t, os.WriteFile(path, []byte(value), 0o600))
		}
		cfg := &Config{
			DataDir:  dataDir,
			AgentKey: filepath.Join(sageHome, "agent.key"),
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
				AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
				HomeDomain:   "voice-interface",
				Clearance:    1,
			},
		}

		require.NoError(t, preflightVendoredStartup(cometHome, cfg))
		require.NoDirExists(t, cometHome)
		require.NoFileExists(t, cfg.AgentKey)
		require.NoFileExists(t, cfg.VendoredAgentBootstrap.AgentKeyFile)
	})

	t.Run("even an empty pre-existing Badger path is ambiguous", func(t *testing.T) {
		sageHome := t.TempDir()
		t.Setenv("SAGE_HOME", sageHome)
		dataDir := t.TempDir()
		cometHome := filepath.Join(dataDir, "cometbft")
		require.NoError(t, os.Mkdir(filepath.Join(dataDir, "badger"), 0o700))
		cfg := &Config{
			DataDir:  dataDir,
			AgentKey: filepath.Join(sageHome, "agent.key"),
			VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
				AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
				HomeDomain:   "voice-interface",
				Clearance:    1,
			},
		}
		require.ErrorContains(
			t,
			preflightVendoredStartup(cometHome, cfg),
			"Badger chain state path survives",
		)
		require.Empty(t, cfg.ChainID)
	})

	t.Run("pre-provisioned keys are validated without generation", func(t *testing.T) {
		tests := []struct {
			name      string
			rootBytes func([]byte) []byte
			agentSeed []byte
			wantError string
		}{
			{
				name: "coherent distinct seeds",
				rootBytes: func(seed []byte) []byte {
					return append([]byte(nil), seed...)
				},
				agentSeed: bytesOf(0x52, ed25519.SeedSize),
			},
			{
				name: "incoherent expanded Root",
				rootBytes: func(seed []byte) []byte {
					full := ed25519.NewKeyFromSeed(seed)
					full[len(full)-1] ^= 0xff
					return full
				},
				agentSeed: bytesOf(0x52, ed25519.SeedSize),
				wantError: "public component does not match seed",
			},
			{
				name: "same Root and agent",
				rootBytes: func(seed []byte) []byte {
					return append([]byte(nil), seed...)
				},
				agentSeed: bytesOf(0x51, ed25519.SeedSize),
				wantError: "must be distinct",
			},
		}
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				sageHome := t.TempDir()
				t.Setenv("SAGE_HOME", sageHome)
				dataDir := t.TempDir()
				cometHome := filepath.Join(dataDir, "cometbft")
				rootPath := filepath.Join(sageHome, "agent.key")
				agentPath := filepath.Join(sageHome, "agents", "mynah", "agent.key")
				require.NoError(t, os.MkdirAll(filepath.Dir(agentPath), 0o700))
				rootBytes := testCase.rootBytes(bytesOf(0x51, ed25519.SeedSize))
				require.NoError(t, os.WriteFile(rootPath, rootBytes, 0o600))
				require.NoError(t, os.WriteFile(agentPath, testCase.agentSeed, 0o600))
				cfg := &Config{
					DataDir:  dataDir,
					AgentKey: rootPath,
					VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
						AgentKeyFile: agentPath,
						HomeDomain:   "voice-interface",
						Clearance:    1,
					},
				}
				err := preflightVendoredStartup(cometHome, cfg)
				if testCase.wantError == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, testCase.wantError)
				}
				rootAfter, readErr := os.ReadFile(rootPath)
				require.NoError(t, readErr)
				require.Equal(t, rootBytes, rootAfter)
				agentAfter, readErr := os.ReadFile(agentPath)
				require.NoError(t, readErr)
				require.Equal(t, testCase.agentSeed, agentAfter)
				require.NoDirExists(t, cometHome)
			})
		}
	})
}

func bytesOf(value byte, count int) []byte {
	out := make([]byte, count)
	for index := range out {
		out[index] = value
	}
	return out
}

func TestVendoredExistingGenesisRequiresCompleteLocalCometIdentity(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(*testing.T, string, *cmttypes.GenesisDoc)
		error  string
	}{
		{
			name: "missing validator key",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				require.NoError(t, os.Remove(filepath.Join(home, "config", "priv_validator_key.json")))
			},
			error: "validator key is missing",
		},
		{
			name: "malformed validator key",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				require.NoError(t, os.WriteFile(
					filepath.Join(home, "config", "priv_validator_key.json"),
					[]byte(`{"not":"a validator"}`),
					0o600,
				))
			},
			error: "validator key",
		},
		{
			name: "missing validator state",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				require.NoError(t, os.Remove(filepath.Join(home, "data", "priv_validator_state.json")))
			},
			error: "validator signing state",
		},
		{
			name: "malformed validator state",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				require.NoError(t, os.WriteFile(
					filepath.Join(home, "data", "priv_validator_state.json"),
					[]byte(`{"height":"0","round":0,"step":0,"unknown":true}`),
					0o600,
				))
			},
			error: "unknown field",
		},
		{
			name: "incoherent signed validator state",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				state := privval.FilePVLastSignState{
					Height: 1, Round: 0, Step: 3,
					SignBytes: []byte("signed-by-someone-else"),
					Signature: make([]byte, ed25519.SignatureSize),
				}
				encoded, err := cmtjson.Marshal(state)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(
					filepath.Join(home, "data", "priv_validator_state.json"),
					encoded,
					0o600,
				))
			},
			error: "signature does not match",
		},
		{
			name: "missing peer key",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				require.NoError(t, os.Remove(filepath.Join(home, "config", "node_key.json")))
			},
			error: "peer identity",
		},
		{
			name: "malformed peer key",
			tamper: func(t *testing.T, home string, _ *cmttypes.GenesisDoc) {
				require.NoError(t, os.WriteFile(
					filepath.Join(home, "config", "node_key.json"),
					[]byte(`{"priv_key":null}`),
					0o600,
				))
			},
			error: "peer identity",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, cometHome, cfg, genesis := vendoredPreflightFixture(t)
			testCase.tamper(t, cometHome, genesis)
			genesisPath := filepath.Join(cometHome, "config", "genesis.json")
			beforeGenesis, err := os.ReadFile(genesisPath)
			require.NoError(t, err)
			err = preflightExistingVendoredGenesis(cometHome, cfg)
			require.ErrorContains(t, err, testCase.error)
			afterGenesis, readErr := os.ReadFile(genesisPath)
			require.NoError(t, readErr)
			require.Equal(t, beforeGenesis, afterGenesis)
		})
	}
}

func TestVendoredValidatorPreflightAcceptsCoherentPositiveHeightSigningState(t *testing.T) {
	_, cometHome, cfg, genesis := vendoredPreflightFixture(t)
	keyJSON, err := os.ReadFile(filepath.Join(cometHome, "config", "priv_validator_key.json"))
	require.NoError(t, err)
	var key privval.FilePVKey
	require.NoError(t, cmtjson.Unmarshal(keyJSON, &key))
	privateKey, ok := key.PrivKey.(cmtcryptoed.PrivKey)
	require.True(t, ok)
	tests := []struct {
		name      string
		step      int8
		signBytes func() []byte
	}{
		{
			name: "proposal",
			step: 1,
			signBytes: func() []byte {
				return cmttypes.ProposalSignBytes(genesis.ChainID, &cmtproto.Proposal{
					Type: cmtproto.ProposalType, Height: 42, Round: 3,
					PolRound: -1, Timestamp: time.Unix(42, 0).UTC(),
				})
			},
		},
		{
			name: "prevote",
			step: 2,
			signBytes: func() []byte {
				return cmttypes.VoteSignBytes(genesis.ChainID, &cmtproto.Vote{
					Type: cmtproto.PrevoteType, Height: 42, Round: 3,
					Timestamp: time.Unix(42, 0).UTC(),
				})
			},
		},
		{
			name: "precommit",
			step: 3,
			signBytes: func() []byte {
				return cmttypes.VoteSignBytes(genesis.ChainID, &cmtproto.Vote{
					Type: cmtproto.PrecommitType, Height: 42, Round: 3,
					Timestamp: time.Unix(42, 0).UTC(),
				})
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			signBytes := testCase.signBytes()
			signature, signErr := privateKey.Sign(signBytes)
			require.NoError(t, signErr)
			stateJSON, marshalErr := cmtjson.Marshal(privval.FilePVLastSignState{
				Height: 42, Round: 3, Step: testCase.step,
				Signature: signature, SignBytes: signBytes,
			})
			require.NoError(t, marshalErr)
			require.NoError(t, os.WriteFile(
				filepath.Join(cometHome, "data", "priv_validator_state.json"),
				stateJSON,
				0o600,
			))
			require.NoError(t, preflightExistingVendoredGenesis(cometHome, cfg))
		})
	}
}

func TestVendoredValidatorPreflightBindsCanonicalConsensusSignBytes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*privval.FilePVLastSignState, string)
		error  string
	}{
		{
			name: "arbitrary signed bytes",
			mutate: func(state *privval.FilePVLastSignState, _ string) {
				state.SignBytes = []byte("not canonical CometBFT consensus sign bytes")
			},
			error: "decode canonical vote",
		},
		{
			name: "height differs from signed vote",
			mutate: func(state *privval.FilePVLastSignState, _ string) {
				state.Height++
			},
			error: "height/round do not match",
		},
		{
			name: "round differs from signed vote",
			mutate: func(state *privval.FilePVLastSignState, _ string) {
				state.Round++
			},
			error: "height/round do not match",
		},
		{
			name: "step differs from signed vote",
			mutate: func(state *privval.FilePVLastSignState, _ string) {
				state.Step = 2
			},
			error: "does not match signing step",
		},
		{
			name: "chain differs from genesis",
			mutate: func(state *privval.FilePVLastSignState, _ string) {
				vote := &cmtproto.Vote{
					Type:   cmtproto.PrecommitType,
					Height: state.Height, Round: state.Round,
					Timestamp: time.Unix(42, 0).UTC(),
				}
				state.SignBytes = cmttypes.VoteSignBytes("different-chain", vote)
			},
			error: "chain_id does not match genesis",
		},
		{
			name: "proposal bytes in vote step",
			mutate: func(state *privval.FilePVLastSignState, chainID string) {
				proposal := &cmtproto.Proposal{
					Type:   cmtproto.ProposalType,
					Height: state.Height, Round: state.Round,
					PolRound: -1, Timestamp: time.Unix(42, 0).UTC(),
				}
				state.SignBytes = cmttypes.ProposalSignBytes(chainID, proposal)
			},
			error: "decode canonical vote",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			_, cometHome, cfg, genesis := vendoredPreflightFixture(t)
			keyJSON, err := os.ReadFile(
				filepath.Join(cometHome, "config", "priv_validator_key.json"),
			)
			require.NoError(t, err)
			var key privval.FilePVKey
			require.NoError(t, cmtjson.Unmarshal(keyJSON, &key))
			privateKey, ok := key.PrivKey.(cmtcryptoed.PrivKey)
			require.True(t, ok)
			state := privval.FilePVLastSignState{
				Height: 42, Round: 3, Step: 3,
			}
			vote := &cmtproto.Vote{
				Type:   cmtproto.PrecommitType,
				Height: state.Height, Round: state.Round,
				Timestamp: time.Unix(42, 0).UTC(),
			}
			state.SignBytes = cmttypes.VoteSignBytes(genesis.ChainID, vote)
			testCase.mutate(&state, genesis.ChainID)
			state.Signature, err = privateKey.Sign(state.SignBytes)
			require.NoError(t, err)
			stateJSON, err := cmtjson.Marshal(state)
			require.NoError(t, err)
			statePath := filepath.Join(
				cometHome,
				"data",
				"priv_validator_state.json",
			)
			require.NoError(t, os.WriteFile(statePath, stateJSON, 0o600))
			before, err := os.ReadFile(statePath)
			require.NoError(t, err)
			err = preflightExistingVendoredGenesis(cometHome, cfg)
			require.ErrorContains(t, err, testCase.error)
			after, readErr := os.ReadFile(statePath)
			require.NoError(t, readErr)
			require.Equal(t, before, after)
		})
	}
}

func TestVendoredGenesisPreflightBindsImmutableRootAndRecoversEmptyChainID(t *testing.T) {
	t.Run("empty configured chain ID adopts verified genesis in memory", func(t *testing.T) {
		_, cometHome, cfg, genesis := vendoredPreflightFixture(t)
		cfg.ChainID = ""
		configPath := filepath.Join(SageHome(), "config.yaml")
		require.NoError(t, os.WriteFile(configPath, []byte("chain_id: ''\n"), 0o600))
		before, err := os.ReadFile(configPath)
		require.NoError(t, err)
		require.NoError(t, preflightExistingVendoredGenesis(cometHome, cfg))
		require.Equal(t, genesis.ChainID, cfg.ChainID)
		after, err := os.ReadFile(configPath)
		require.NoError(t, err)
		require.Equal(t, before, after, "preflight recovery must stay in memory")
	})

	t.Run("different coherent transport key is rejected", func(t *testing.T) {
		_, cometHome, cfg, _ := vendoredPreflightFixture(t)
		replacementSeed := sha256.Sum256([]byte("wrong-stable-transport"))
		require.NoError(t, os.WriteFile(cfg.AgentKey, replacementSeed[:], 0o600))
		require.ErrorContains(
			t,
			preflightExistingVendoredGenesis(cometHome, cfg),
			"stable transport key does not match",
		)
	})

	t.Run("incoherent 64 byte Root key is rejected", func(t *testing.T) {
		_, cometHome, cfg, _ := vendoredPreflightFixture(t)
		seed, err := os.ReadFile(cfg.AgentKey)
		require.NoError(t, err)
		full := ed25519.NewKeyFromSeed(seed)
		full[len(full)-1] ^= 0xff
		require.NoError(t, os.WriteFile(cfg.AgentKey, full, 0o600))
		require.ErrorContains(
			t,
			preflightExistingVendoredGenesis(cometHome, cfg),
			"public component does not match seed",
		)
	})

	t.Run("incoherent 64 byte vendored key is rejected", func(t *testing.T) {
		_, cometHome, cfg, _ := vendoredPreflightFixture(t)
		seed, err := os.ReadFile(cfg.VendoredAgentBootstrap.AgentKeyFile)
		require.NoError(t, err)
		full := ed25519.NewKeyFromSeed(seed)
		full[len(full)-1] ^= 0xff
		require.NoError(t, os.WriteFile(cfg.VendoredAgentBootstrap.AgentKeyFile, full, 0o600))
		require.ErrorContains(
			t,
			preflightExistingVendoredGenesis(cometHome, cfg),
			"first-party agent key does not match",
		)
	})
}

func TestVendoredLockedEmptyProjectionIsNotServingReady(t *testing.T) {
	locked := scopedProjectionReadinessStatus(0, true)
	require.True(t, locked.Required)
	require.False(t, locked.OK)
	require.Contains(t, locked.Detail, "vault locked")

	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)
	health.SetScopedProjectionStatus(locked)
	health.SetVendoredAgentEnrollmentStatus(metrics.VendoredAgentEnrollmentStatus{
		Required: true,
		OK:       true,
		State:    "ready",
	})
	response := httptest.NewRecorder()
	health.ReadinessHandler(
		response,
		httptest.NewRequest(http.MethodGet, "/ready", nil),
	)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)

	unlocked := scopedProjectionReadinessStatus(0, false)
	require.False(t, unlocked.Required)
	require.True(t, unlocked.OK)
	health.SetScopedProjectionStatus(unlocked)
	response = httptest.NewRecorder()
	health.ReadinessHandler(
		response,
		httptest.NewRequest(http.MethodGet, "/ready", nil),
	)
	require.Equal(t, http.StatusOK, response.Code)
}

func TestVendoredStartupPreflightLeavesJoinMigrationAndGenesisSentinelsUntouched(t *testing.T) {
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)
	dataDir := t.TempDir()
	cometHome := filepath.Join(dataDir, "cometbft")
	rootKeyPath := filepath.Join(sageHome, "agent.key")
	require.NoError(t, initCometBFTConfigWithBootstrap(
		cometHome,
		rootKeyPath,
		nil,
	))
	genesisPath := filepath.Join(cometHome, "config", "genesis.json")
	pendingPath := pendingJoinPath()
	migrationSentinel := filepath.Join(dataDir, "migration-remint-sentinel")
	require.NoError(t, os.WriteFile(pendingPath, []byte("pending-join-sentinel"), 0o600))
	require.NoError(t, os.WriteFile(migrationSentinel, []byte("migration-sentinel"), 0o600))
	beforeGenesis, err := os.ReadFile(genesisPath)
	require.NoError(t, err)
	beforePending, err := os.ReadFile(pendingPath)
	require.NoError(t, err)
	beforeMigration, err := os.ReadFile(migrationSentinel)
	require.NoError(t, err)

	err = preflightVendoredStartup(cometHome, &Config{
		DataDir:  dataDir,
		AgentKey: rootKeyPath,
		VendoredAgentBootstrap: &VendoredAgentBootstrapConfig{
			AgentKeyFile: filepath.Join(sageHome, "agents", "mynah", "agent.key"),
			HomeDomain:   "voice-interface", Clearance: 1,
		},
	})
	require.ErrorContains(t, err, "not a direct app-v23 origin")
	afterGenesis, readErr := os.ReadFile(genesisPath)
	require.NoError(t, readErr)
	afterPending, readErr := os.ReadFile(pendingPath)
	require.NoError(t, readErr)
	afterMigration, readErr := os.ReadFile(migrationSentinel)
	require.NoError(t, readErr)
	require.Equal(t, beforeGenesis, afterGenesis)
	require.Equal(t, beforePending, afterPending)
	require.Equal(t, beforeMigration, afterMigration)
	require.NoFileExists(t, genesisPath+".bak", "legacy healer must not run")
}

func openVendoredTestApp(
	t *testing.T,
	badgerPath, projectionPath string,
) (*sageabci.SageApp, *store.BadgerStore, *store.SQLiteStore) {
	t.Helper()
	badgerStore, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	require.NoError(t, err)
	app, err := sageabci.NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	require.NoError(t, err)
	return app, badgerStore, projection
}

func vendoredInitRequest(
	genesis *cmttypes.GenesisDoc,
	validators ...[]byte,
) *abcitypes.RequestInitChain {
	updates := make([]abcitypes.ValidatorUpdate, 0, len(validators))
	for _, public := range validators {
		updates = append(updates, abcitypes.ValidatorUpdate{
			Power: 10,
			PubKey: cryptoproto.PublicKey{Sum: &cryptoproto.PublicKey_Ed25519{
				Ed25519: public,
			}},
		})
	}
	return &abcitypes.RequestInitChain{
		ChainId:       genesis.ChainID,
		AppStateBytes: genesis.AppState,
		Validators:    updates,
	}
}

func TestVendoredAgentBootstrapWaitsForAppV24BeforeFirstMemoryWrite(t *testing.T) {
	genesis, bootstrap, rootKeyPath := vendoredGenesisFixture(t)
	var appState struct {
		Sage struct {
			InitialAdmin    string                         `json:"initial_admin"`
			AppV23Bootstrap sageabci.AppV23GenesisManifest `json:"app_v23_bootstrap"`
		} `json:"sage"`
	}
	require.NoError(t, json.Unmarshal(genesis.AppState, &appState))
	manifest := appState.Sage.AppV23Bootstrap
	require.Equal(t, manifest.RootID, appState.Sage.InitialAdmin)
	_, err := sageabci.VerifyAppV23GenesisManifest(genesis.ChainID, manifest)
	require.NoError(t, err)

	badgerPath := filepath.Join(t.TempDir(), "badger")
	projectionPath := filepath.Join(t.TempDir(), "offchain.db")
	app, badgerStore, projection := openVendoredTestApp(t, badgerPath, projectionPath)
	validatorPublic := genesis.Validators[0].PubKey.Bytes()
	initRequest := vendoredInitRequest(genesis, validatorPublic)
	initResponse, err := app.InitChain(context.Background(), initRequest)
	require.NoError(t, err)
	require.Equal(t, sageabci.AppV23GenesisAppVersion, initResponse.ConsensusParams.Version.App)
	require.Len(t, initResponse.AppHash, sha256.Size)

	enrollment, err := badgerStore.GetAppV23Enrollment(manifest.AgentID)
	require.NoError(t, err)
	require.True(t, enrollment.Active)
	require.Equal(t, store.AppV23ProfileCompanion, enrollment.Profile)
	require.Equal(t, store.AgentCapabilities(15), enrollment.Capabilities)
	owner, err := badgerStore.GetDomainOwner(bootstrap.HomeDomain)
	require.NoError(t, err)
	require.Equal(t, manifest.AgentID, owner)
	_, _, _, err = badgerStore.GetAccessGrant(bootstrap.HomeDomain, manifest.AgentID)
	require.ErrorIs(t, err, store.ErrAccessGrantNotFound)
	governanceDomain, err := governance.DelegationDomainForChainID(genesis.ChainID)
	require.NoError(t, err)
	root, err := badgerStore.GetAppV23Root()
	require.NoError(t, err)
	activation, err := badgerStore.GetAppV23GenesisActivation()
	require.NoError(t, err)
	require.Equal(t, governanceDomain, root.Scope)
	require.Equal(t, governanceDomain, activation.Scope)
	persistedDomain, err := badgerStore.GetState("governance_delegation_domain_v20")
	require.NoError(t, err)
	require.Equal(t, governanceDomain, hex.EncodeToString(persistedDomain))
	require.NoError(t, verifyAppV23VendoredAgentReadiness(
		bootstrap,
		exactVendoredKeyResolver(rootKeyPath),
		badgerStore,
	))
	persistedState, err := sageabci.LoadState(badgerStore)
	require.NoError(t, err)
	require.Zero(t, persistedState.Height)
	require.Empty(t, persistedState.AppHash)

	// Crash before block 1: marker restores v23, application bookkeeping stays
	// empty, and re-Init returns the same Comet-owned height-0 AppHash.
	require.NoError(t, projection.Close())
	require.NoError(t, badgerStore.CloseBadger())
	app, badgerStore, projection = openVendoredTestApp(t, badgerPath, projectionPath)
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
		require.NoError(t, badgerStore.CloseBadger())
	})
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.Equal(t, sageabci.AppV23GenesisAppVersion, info.AppVersion)
	require.Empty(t, info.LastBlockAppHash)
	reinitResponse, err := app.InitChain(context.Background(), initRequest)
	require.NoError(t, err)
	require.Equal(t, initResponse.AppHash, reinitResponse.AppHash)

	seed, err := os.ReadFile(bootstrap.AgentKeyFile)
	require.NoError(t, err)
	agentKey := ed25519.NewKeyFromSeed(seed)
	content := "Mynah is live on its first clean boot"
	contentHash := sha256.Sum256([]byte(content))
	proofHash := sha256.Sum256([]byte(content + bootstrap.HomeDomain))
	proofTime := time.Now().Unix()
	var proofTimeBytes [8]byte
	binary.BigEndian.PutUint64(proofTimeBytes[:], uint64(proofTime))
	proofMessage := append(append([]byte(nil), proofHash[:]...), proofTimeBytes[:]...)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			ContentHash: contentHash[:], MemoryType: tx.MemoryTypeObservation,
			DomainTag: bootstrap.HomeDomain, ConfidenceScore: 0.9,
			Content: content, Classification: tx.ClearanceInternal,
		},
		AgentPubKey: agentKey.Public().(ed25519.PublicKey),
		AgentSig:    ed25519.Sign(agentKey, proofMessage), AgentBodyHash: proofHash[:],
		AgentTimestamp: proofTime, Nonce: 1,
	}
	require.NoError(t, tx.SignTx(parsed, agentKey))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	require.NoError(t, badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: "app-v24", TargetAppVersion: 24, ActivationHeight: 1,
	}))
	finalized, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 1, Time: time.Now(), Txs: [][]byte{raw},
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 1)
	require.Equal(t, uint32(11), finalized.TxResults[0].Code)
	require.Contains(t, finalized.TxResults[0].Log, "require governed app-v24 activation")
	require.NotNil(t, finalized.ConsensusParamUpdates)
	require.Equal(t, uint64(24), finalized.ConsensusParamUpdates.Version.App)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)

	firstSafe, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Now().Add(time.Second), Txs: [][]byte{raw},
	})
	require.NoError(t, err)
	require.Len(t, firstSafe.TxResults, 1)
	require.Zero(t, firstSafe.TxResults[0].Code, firstSafe.TxResults[0].Log)
}

func TestVendoredAgentBootstrapRejectsMultiValidatorGenesisWithoutDirtyRetry(t *testing.T) {
	genesis, _, _ := vendoredGenesisFixture(t)
	app, badgerStore, projection := openVendoredTestApp(
		t,
		filepath.Join(t.TempDir(), "badger"),
		filepath.Join(t.TempDir(), "offchain.db"),
	)
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
		require.NoError(t, badgerStore.CloseBadger())
	})
	validatorOne := genesis.Validators[0].PubKey.Bytes()
	validatorTwo := cmtcryptoed.GenPrivKey().PubKey().Bytes()
	_, err := app.InitChain(
		context.Background(),
		vendoredInitRequest(genesis, validatorOne, validatorTwo),
	)
	require.ErrorContains(t, err, "requires exactly one validator")
	require.Empty(t, app.ValidatorIDs())
	persistedValidators, err := badgerStore.LoadValidators()
	require.NoError(t, err)
	require.Empty(t, persistedValidators)

	response, err := app.InitChain(
		context.Background(),
		vendoredInitRequest(genesis, validatorOne),
	)
	require.NoError(t, err)
	require.Equal(t, sageabci.AppV23GenesisAppVersion, response.ConsensusParams.Version.App)
	require.Equal(t, []string{hex.EncodeToString(validatorOne)}, app.ValidatorIDs())
}

func TestVendoredAgentBootstrapRejectsStaleValidatorAndRetriesExactly(t *testing.T) {
	genesis, _, _ := vendoredGenesisFixture(t)
	badgerPath := filepath.Join(t.TempDir(), "badger")
	projectionPath := filepath.Join(t.TempDir(), "offchain.db")
	badgerStore, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	staleID := strings.Repeat("77", 32)
	require.NoError(t, badgerStore.SaveValidators(map[string]int64{staleID: 10}))
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	require.NoError(t, err)
	app, err := sageabci.NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	require.NoError(t, err)
	request := vendoredInitRequest(genesis, genesis.Validators[0].PubKey.Bytes())
	_, err = app.InitChain(context.Background(), request)
	require.ErrorContains(t, err, "preexisting consensus key")
	root, err := badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Nil(t, root)
	activation, err := badgerStore.GetAppV23GenesisActivation()
	require.NoError(t, err)
	require.Nil(t, activation)
	require.Equal(t, []string{staleID}, app.ValidatorIDs())

	require.NoError(t, badgerStore.ReplaceValidators(map[string]int64{}))
	response, err := app.InitChain(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, sageabci.AppV23GenesisAppVersion, response.ConsensusParams.Version.App)
	expectedValidatorID := hex.EncodeToString(genesis.Validators[0].PubKey.Bytes())
	require.Equal(t, []string{expectedValidatorID}, app.ValidatorIDs())
	persisted, err := badgerStore.LoadValidators()
	require.NoError(t, err)
	require.Equal(t, map[string]int64{expectedValidatorID: 10}, persisted)

	require.NoError(t, projection.Close())
	require.NoError(t, badgerStore.CloseBadger())
	app, badgerStore, projection = openVendoredTestApp(t, badgerPath, projectionPath)
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
		require.NoError(t, badgerStore.CloseBadger())
	})
	info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.Equal(t, sageabci.AppV23GenesisAppVersion, info.AppVersion)
	require.Equal(t, []string{expectedValidatorID}, app.ValidatorIDs())
}

func TestVendoredAgentBootstrapRejectsRootKeyReuse(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "agent.key")
	_, err := genesisAppStateForVendoredAgent(
		"sage-test", keyPath,
		&VendoredAgentBootstrapConfig{
			AgentKeyFile: keyPath,
			HomeDomain:   "voice-interface",
			Clearance:    1,
		},
		strings.Repeat("11", 32),
		10,
	)
	require.ErrorContains(t, err, "must be distinct")
}

func TestVendoredAgentReadinessNeverRepairsLegacyLookalike(t *testing.T) {
	badgerStore, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	rootSeed := sha256.Sum256([]byte("legacy-root"))
	rootKey := ed25519.NewKeyFromSeed(rootSeed[:])
	targetSeed := sha256.Sum256([]byte("legacy-target"))
	targetKey := ed25519.NewKeyFromSeed(targetSeed[:])
	targetPath := filepath.Join(t.TempDir(), "mynah.key")
	require.NoError(t, os.WriteFile(targetPath, targetSeed[:], 0o600))
	rootPath := filepath.Join(t.TempDir(), "root.key")
	require.NoError(t, os.WriteFile(rootPath, rootSeed[:], 0o600))
	rootID := appV23AgentIDForKey(rootKey)
	targetID := appV23AgentIDForKey(targetKey)
	require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
		rootID, "root", store.AppV23RoleAdmin, "", "", "", 1, 0,
	))
	require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
		targetID, "lookalike", store.AppV23RoleMember, "", "", "", 2,
		store.DefaultSelfRegisteredAgentCapabilities,
	))
	require.NoError(t, badgerStore.EnsureAppV23Root("legacy-scope", 100))
	before, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	err = verifyAppV23VendoredAgentReadiness(&VendoredAgentBootstrapConfig{
		AgentKeyFile: targetPath,
		HomeDomain:   "voice-interface",
		Clearance:    1,
	}, exactVendoredKeyResolver(rootPath), badgerStore)
	require.ErrorContains(t, err, "authenticated fresh genesis")
	after, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after, "readiness verification must never mutate legacy state")
}

func TestVendoredAgentReadinessRejectsMismatchedInitialRootKey(t *testing.T) {
	genesis, bootstrap, _ := vendoredGenesisFixture(t)
	app, badgerStore, projection := openVendoredTestApp(
		t,
		filepath.Join(t.TempDir(), "badger"),
		filepath.Join(t.TempDir(), "offchain.db"),
	)
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
		require.NoError(t, badgerStore.CloseBadger())
	})
	_, err := app.InitChain(
		context.Background(),
		vendoredInitRequest(genesis, genesis.Validators[0].PubKey.Bytes()),
	)
	require.NoError(t, err)
	mismatchSeed := sha256.Sum256([]byte("mismatched-initial-root"))
	mismatchPath := filepath.Join(t.TempDir(), "wrong-root.key")
	require.NoError(t, os.WriteFile(mismatchPath, mismatchSeed[:], 0o600))
	before, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	err = verifyAppV23VendoredAgentReadiness(
		bootstrap,
		exactVendoredKeyResolver(mismatchPath),
		badgerStore,
	)
	require.ErrorContains(t, err, "current committed CEREBRUM Root credential")
	after, err := badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestVendoredAgentReadinessSurvivesLegitimateRootHandover(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	genesis, bootstrap, initialRootPath := vendoredGenesisFixture(t)
	badgerPath := filepath.Join(t.TempDir(), "badger")
	projectionPath := filepath.Join(t.TempDir(), "offchain.db")
	app, badgerStore, projection := openVendoredTestApp(
		t,
		badgerPath,
		projectionPath,
	)
	_, err := app.InitChain(
		context.Background(),
		vendoredInitRequest(genesis, genesis.Validators[0].PubKey.Bytes()),
	)
	require.NoError(t, err)
	replacementSeed := sha256.Sum256([]byte("replacement-root"))
	replacementKey := ed25519.NewKeyFromSeed(replacementSeed[:])
	require.NoError(t, badgerStore.RotateAppV23RootCredential(
		1,
		appV23AgentIDForKey(replacementKey),
		2,
	))
	require.ErrorContains(t, verifyAppV23VendoredAgentReadiness(
		bootstrap,
		exactVendoredKeyResolver(initialRootPath),
		badgerStore,
	), "current committed CEREBRUM Root credential")
	require.NoError(t, projection.Close())
	require.NoError(t, badgerStore.CloseBadger())

	// A restart must preserve the immutable original Root principal while
	// requiring the currently committed generation-2 credential. The retired
	// generation-1 key alone is never sufficient.
	app, badgerStore, projection = openVendoredTestApp(t, badgerPath, projectionPath)
	_ = app
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
		require.NoError(t, badgerStore.CloseBadger())
	})
	replacementPath := filepath.Join(home, "bundles", appV23AgentIDForKey(replacementKey), "agent.key")
	require.NoError(t, os.MkdirAll(filepath.Dir(replacementPath), 0o700))
	require.NoError(t, os.WriteFile(replacementPath, replacementSeed[:], 0o600))
	require.ErrorContains(t, verifyAppV23VendoredAgentReadiness(
		bootstrap,
		exactVendoredKeyResolver(initialRootPath),
		badgerStore,
	), "current committed CEREBRUM Root credential")
	cfg := DefaultConfig(home)
	cfg.AgentKey = initialRootPath
	cfg.VendoredAgentBootstrap = bootstrap
	require.NoError(t, verifyAppV23VendoredAgentReadiness(
		bootstrap,
		localAgentKeyResolverForConfig(cfg),
		badgerStore,
	))
	root, err := badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotEqual(t, root.PrincipalID, root.CredentialID)
}

func TestVendoredAgentReadinessUsesConfiguredExternalKeyResolver(t *testing.T) {
	// Keep SAGE_HOME disjoint from every fixture key. The companion credential
	// is available only through the exact application-owned path in
	// VendoredAgentBootstrap, not any conventional ~/.sage key directory. The
	// same resolver must also retain the current Root path for readiness.
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	genesis, bootstrap, initialRootPath := vendoredGenesisFixture(t)
	app, badgerStore, projection := openVendoredTestApp(
		t,
		filepath.Join(t.TempDir(), "badger"),
		filepath.Join(t.TempDir(), "offchain.db"),
	)
	t.Cleanup(func() {
		require.NoError(t, projection.Close())
		require.NoError(t, badgerStore.CloseBadger())
	})
	_, err := app.InitChain(
		context.Background(),
		vendoredInitRequest(genesis, genesis.Validators[0].PubKey.Bytes()),
	)
	require.NoError(t, err)

	companionKey, ok := parseKeyFile(bootstrap.AgentKeyFile)
	require.True(t, ok)
	companionID := appV23AgentIDForKey(companionKey)

	cfg := DefaultConfig(home)
	cfg.AgentKey = initialRootPath
	cfg.VendoredAgentBootstrap = bootstrap
	resolver := localAgentKeyResolverForConfig(cfg)
	resolvedCompanion, found := resolver(companionID)
	require.True(t, found)
	require.Equal(t, companionKey, resolvedCompanion)
	require.NoError(t, verifyAppV23VendoredAgentReadiness(
		bootstrap,
		resolver,
		badgerStore,
	))
}

func TestVendoredAgentReadinessAllowsWriteCapablePromotionAndRejectsReadOnly(t *testing.T) {
	tests := []struct {
		name      string
		role      string
		profile   string
		clearance uint8
		caps      store.AgentCapabilities
		wantError bool
	}{
		{
			name: "manager promotion remains ready", role: store.AppV23RoleManager,
			profile: store.AppV23ProfileStandard, clearance: 1,
		},
		{
			name: "read-only policy blocks readiness", role: store.AppV23RoleMember,
			profile: store.AppV23ProfileReadOnly, clearance: 1,
			caps: store.AgentCapabilityReadAllDomains, wantError: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			genesis, bootstrap, initialRootPath := vendoredGenesisFixture(t)
			app, badgerStore, projection := openVendoredTestApp(
				t,
				filepath.Join(t.TempDir(), "badger"),
				filepath.Join(t.TempDir(), "offchain.db"),
			)
			t.Cleanup(func() {
				require.NoError(t, projection.Close())
				require.NoError(t, badgerStore.CloseBadger())
			})
			_, err := app.InitChain(
				context.Background(),
				vendoredInitRequest(genesis, genesis.Validators[0].PubKey.Bytes()),
			)
			require.NoError(t, err)
			var appState struct {
				Sage struct {
					AppV23Bootstrap sageabci.AppV23GenesisManifest `json:"app_v23_bootstrap"`
				} `json:"sage"`
			}
			require.NoError(t, json.Unmarshal(genesis.AppState, &appState))
			manifest := appState.Sage.AppV23Bootstrap
			require.NoError(t, badgerStore.SetAppV23Policy(
				manifest.RootID,
				manifest.AgentID,
				testCase.role,
				store.AppV23ProfileCompanion,
				testCase.profile,
				testCase.clearance,
				testCase.caps,
				1,
				1,
				2,
			))
			err = verifyAppV23VendoredAgentReadiness(
				bootstrap,
				exactVendoredKeyResolver(initialRootPath),
				badgerStore,
			)
			if testCase.wantError {
				require.ErrorContains(t, err, "cannot write")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
