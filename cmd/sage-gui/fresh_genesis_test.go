package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtconfig "github.com/cometbft/cometbft/config"
	rpctypes "github.com/cometbft/cometbft/rpc/jsonrpc/types"
	cmttypes "github.com/cometbft/cometbft/types"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/pairing"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestFreshPersonalProductionGenesisStartsAtLatest(t *testing.T) {
	for _, companion := range []bool{false, true} {
		t.Run(strconv.FormatBool(companion), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			cfg := DefaultConfig(home)
			if companion {
				cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{
					AgentKeyFile: filepath.Join(home, "companion.key"), HomeDomain: "private-home", Clearance: 1,
				}
			}
			cometHome := filepath.Join(cfg.DataDir, "cometbft")
			require.NoError(t, ensureGenesisSeedWithConfig(cometHome, cfg, zerolog.Nop()))
			genesisPath := filepath.Join(cometHome, "config", "genesis.json")
			genesis, err := cmttypes.GenesisDocFromFile(genesisPath)
			require.NoError(t, err)
			require.Equal(t, sageabci.MaxSupportedAppVersion(), genesis.ConsensusParams.Version.App)
			require.LessOrEqual(t, genesis.ConsensusParams.Version.App, sageabci.MaxSupportedAppVersion(), "default install must not activate a compiled dormant fork")
			before := snapshotTestTree(t, home)
			require.NoError(t, ensureGenesisSeedWithConfig(cometHome, cfg, zerolog.Nop()))
			if companion {
				require.NoError(t, preflightExistingVendoredGenesis(cometHome, cfg))
			}
			require.Equal(t, before, snapshotTestTree(t, home), "reopening must not mint or rewrite genesis/keys")
			app, bs, sql := openVendoredTestApp(t, filepath.Join(t.TempDir(), "badger"), filepath.Join(t.TempDir(), "projection.db"))
			defer bs.CloseBadger()
			defer sql.Close()
			request := vendoredInitRequest(genesis, genesis.Validators[0].PubKey.Bytes())
			params := genesis.ConsensusParams.ToProto()
			request.ConsensusParams = &params
			response, err := app.InitChain(context.Background(), request)
			require.NoError(t, err)
			require.Equal(t, sageabci.MaxSupportedAppVersion(), response.ConsensusParams.Version.App)
			info, err := app.Info(context.Background(), &abcitypes.RequestInfo{})
			require.NoError(t, err)
			require.Equal(t, sageabci.MaxSupportedAppVersion(), info.AppVersion)
			require.True(t, app.IsAppV24ActiveForNextTx(), "first memory need not wait for an upgrade")
			if companion {
				require.NoError(t, verifyAppV23VendoredAgentReadiness(cfg.VendoredAgentBootstrap,
					exactVendoredKeyResolver(cfg.AgentKey, cfg.VendoredAgentBootstrap.AgentKeyFile), bs))
			}
		})
	}
}

func TestInitGenesisProducesSignedMultiValidatorArtifact(t *testing.T) {
	for _, target := range []string{"latest", "28", "29", "30"} {
		t.Run(target, func(t *testing.T) {
			dir := t.TempDir()
			rootPath := filepath.Join(dir, "root.key")
			agentPath := filepath.Join(dir, "companion.key")
			root, err := loadOrGenerateKey(rootPath)
			require.NoError(t, err)
			agent, err := loadOrGenerateKey(agentPath)
			require.NoError(t, err)
			validators := make([]sageabci.FreshGenesisValidator, 0, 4)
			for i := 0; i < 4; i++ {
				public, _, err := ed25519.GenerateKey(rand.Reader)
				require.NoError(t, err)
				validators = append(validators, sageabci.FreshGenesisValidator{ValidatorID: hex.EncodeToString(public), Power: 10})
			}
			sort.Slice(validators, func(i, j int) bool { return validators[i].ValidatorID > validators[j].ValidatorID })
			roster, err := json.Marshal(validators)
			require.NoError(t, err)
			rosterPath := filepath.Join(dir, "validators.json")
			require.NoError(t, os.WriteFile(rosterPath, roster, 0600))
			output := filepath.Join(dir, "genesis.json")
			args := []string{"--chain-id", "fresh-multi-" + target, "--root-key", rootPath, "--validators", rosterPath,
				"--output", output, "--app-version", target, "--agent-key", agentPath, "--home-domain", "private-home",
				"--genesis-time", "2026-11-01T00:00:00Z"}
			require.NoError(t, runInitGenesis(args))
			genesis, err := cmttypes.GenesisDocFromFile(output)
			require.NoError(t, err)
			want, err := freshGenesisVersion(target)
			require.NoError(t, err)
			require.Equal(t, want, genesis.ConsensusParams.Version.App)
			require.Len(t, genesis.Validators, 4)
			var state struct {
				Sage struct {
					Bootstrap sageabci.FreshGenesisManifest `json:"fresh_bootstrap"`
				} `json:"sage"`
			}
			require.NoError(t, json.Unmarshal(genesis.AppState, &state))
			manifest := state.Sage.Bootstrap
			_, err = sageabci.VerifyFreshGenesisManifest(genesis.ChainID, manifest)
			require.NoError(t, err)
			require.Equal(t, appV23AgentIDForKey(root), manifest.RootID)
			require.Equal(t, appV23AgentIDForKey(agent), manifest.Agent.AgentID)
			require.True(t, sort.SliceIsSorted(manifest.Validators, func(i, j int) bool { return manifest.Validators[i].ValidatorID < manifest.Validators[j].ValidatorID }))
			before, err := os.ReadFile(output)
			require.NoError(t, err)
			require.ErrorContains(t, runInitGenesis(args), "refusing to replace")
			after, err := os.ReadFile(output)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestFreshGenesisVersionAndInputsFailClosed(t *testing.T) {
	for _, invalid := range []string{"", "023", "23", "031", "31", "latest ", "-1"} {
		_, err := freshGenesisVersion(invalid)
		require.Error(t, err, invalid)
	}
	dir := t.TempDir()
	missingRoot := filepath.Join(dir, "root.key")
	err := runInitGenesis([]string{"--chain-id", "fresh-no-key", "--root-key", missingRoot,
		"--validators", filepath.Join(dir, "validators.json"), "--output", filepath.Join(dir, "genesis.json")})
	require.ErrorContains(t, err, "root key")
	require.NoFileExists(t, missingRoot, "artifact generation must not mint an unrequested Root")
	require.NoFileExists(t, filepath.Join(dir, "genesis.json"))
}

func TestFreshQuorumLegacyCommandsRefuseBeforeMutation(t *testing.T) {
	for _, command := range []string{"quorum-init", "quorum-join"} {
		for _, begun := range []bool{false, true} {
			t.Run(command+"/begun="+strconv.FormatBool(begun), func(t *testing.T) {
				home := t.TempDir()
				t.Setenv("SAGE_HOME", home)
				if begun {
					cometHome := filepath.Join(home, "data", "cometbft")
					require.NoError(t, initCometBFTConfig(cometHome))
					require.NoError(t, os.WriteFile(filepath.Join(cometHome, "data", "priv_validator_state.json"), []byte(`{"height":"8","round":0,"step":3}`), 0600))
				}
				before := snapshotTestTree(t, home)
				original := os.Args
				os.Args = []string{"sage-gui", command}
				t.Cleanup(func() { os.Args = original })
				var err error
				if command == "quorum-init" {
					err = runQuorumInit()
				} else {
					err = runQuorumJoin()
				}
				require.ErrorContains(t, err, "finalized signed genesis")
				require.Equal(t, before, snapshotTestTree(t, home))
			})
		}
	}
	home := t.TempDir()
	cfg := DefaultConfig(home)
	cfg.Quorum.Enabled = true
	err := ensureGenesisSeedWithConfig(filepath.Join(cfg.DataDir, "cometbft"), cfg, zerolog.Nop())
	require.ErrorContains(t, err, "finalized signed genesis")
	require.NoDirExists(t, cfg.DataDir)
}

func TestFreshGenesisPreflightRejectsTamperingWithoutMutation(t *testing.T) {
	cases := map[string]func(*cmttypes.GenesisDoc, *Config){
		"unchanged":         func(_ *cmttypes.GenesisDoc, _ *Config) {},
		"validator power":   func(g *cmttypes.GenesisDoc, _ *Config) { g.Validators[0].Power++ },
		"consensus version": func(g *cmttypes.GenesisDoc, _ *Config) { g.ConsensusParams.Version.App-- },
		"Companion policy":  func(_ *cmttypes.GenesisDoc, c *Config) { c.VendoredAgentBootstrap.Clearance++ },
		"chain ID":          func(g *cmttypes.GenesisDoc, _ *Config) { g.ChainID += "-other" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			cfg := DefaultConfig(home)
			cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{AgentKeyFile: filepath.Join(home, "companion.key"), HomeDomain: "private-home", Clearance: 1}
			cometHome := filepath.Join(cfg.DataDir, "cometbft")
			require.NoError(t, ensureGenesisSeedWithConfig(cometHome, cfg, zerolog.Nop()))
			genesisPath := filepath.Join(cometHome, "config", "genesis.json")
			genesis, err := cmttypes.GenesisDocFromFile(genesisPath)
			require.NoError(t, err)
			mutate(genesis, cfg)
			require.NoError(t, genesis.SaveAs(genesisPath))
			before := snapshotTestTree(t, home)
			require.Empty(t, cfg.ChainID)
			err = preflightExistingVendoredGenesis(cometHome, cfg)
			if name == "unchanged" {
				require.NoError(t, err)
				require.Equal(t, genesis.ChainID, cfg.ChainID)
			} else {
				require.Error(t, err)
				require.Empty(t, cfg.ChainID, "failed inspection must not adopt an unverified chain ID")
			}
			require.Equal(t, before, snapshotTestTree(t, home))
		})
	}
}

func TestFreshGenesisRedeployCannotRewriteSignedOrigin(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	cfg := DefaultConfig(home)
	cometHome := filepath.Join(cfg.DataDir, "cometbft")
	require.NoError(t, ensureGenesisSeedWithConfig(cometHome, cfg, zerolog.Nop()))
	cometCfg := cmtconfig.DefaultConfig().SetRoot(cometHome)
	controller := &SageNodeController{cometCfg: cometCfg}
	before := snapshotTestTree(t, home)
	require.ErrorContains(t, controller.RegenerateGenesis(nil), "cannot regenerate a signed fresh genesis")
	require.Equal(t, before, snapshotTestTree(t, home))
}

func TestFreshGenesisRealCometFirstWriteAndRestart(t *testing.T) {
	for _, target := range []uint64{sageabci.MaxSupportedAppVersion(), 30} {
		t.Run(strconv.FormatUint(target, 10), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			cfg := DefaultConfig(home)
			cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{AgentKeyFile: filepath.Join(home, "companion.key"), HomeDomain: "private-home", Clearance: 1}
			cometHome := filepath.Join(cfg.DataDir, "cometbft")
			require.NoError(t, initCometBFTConfigWithGenesisVersion(cometHome, cfg.AgentKey, cfg.VendoredAgentBootstrap, target))
			genesis, err := cmttypes.GenesisDocFromFile(filepath.Join(cometHome, "config", "genesis.json"))
			require.NoError(t, err)
			badgerPath := filepath.Join(t.TempDir(), "badger")
			projectionPath := filepath.Join(t.TempDir(), "projection.db")
			app, bs, projection := openVendoredTestApp(t, badgerPath, projectionPath)
			require.NoError(t, app.SetExpectedGovernanceDelegationDomain(genesis.ChainID))
			recorder := &recordingGenesisApplication{Application: app}
			controller := startVendoredCometTestNode(t, cometHome, filepath.Dir(badgerPath), recorder)
			t.Cleanup(func() { _ = controller.StopChain(); _ = projection.Close(); _ = bs.CloseBadger() })
			_, inits := recorder.snapshot()
			require.Len(t, inits, 1)
			require.Equal(t, target, inits[0].ConsensusParams.Version.App)
			rpc, err := controller.GetCometNode().ConfigureRPC()
			require.NoError(t, err)
			broadcast, err := rpc.BroadcastTxCommit(&rpctypes.Context{}, cmttypes.Tx(vendoredFirstMemoryRaw(t, cfg.VendoredAgentBootstrap)))
			require.NoError(t, err)
			require.Zero(t, broadcast.CheckTx.Code, broadcast.CheckTx.Log)
			require.Zero(t, broadcast.TxResult.Code, broadcast.TxResult.Log)
			require.NotEmpty(t, broadcast.TxResult.Data)
			plan, err := bs.GetUpgradePlan()
			require.ErrorIs(t, err, store.ErrNoUpgradePlan)
			require.Nil(t, plan, "first write must not schedule an upgrade")
			require.NoError(t, controller.StopChain())
			require.NoError(t, projection.Close())
			require.NoError(t, bs.CloseBadger())
			app, bs, projection = openVendoredTestApp(t, badgerPath, projectionPath)
			require.NoError(t, app.SetExpectedGovernanceDelegationDomain(genesis.ChainID))
			recorder = &recordingGenesisApplication{Application: app}
			controller = startVendoredCometTestNode(t, cometHome, filepath.Dir(badgerPath), recorder)
			infos, inits := recorder.snapshot()
			require.NotEmpty(t, infos)
			require.Equal(t, target, infos[0])
			require.Empty(t, inits, "restart must reopen committed origin without replay")
			require.NoError(t, verifyAppV23VendoredAgentReadiness(cfg.VendoredAgentBootstrap, exactVendoredKeyResolver(cfg.AgentKey), bs))
		})
	}
}

func TestFailedPairLeavesCurrentFreshOrigin(t *testing.T) {
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	token, err := pairing.EncodeToken("127.0.0.1:1", "unavailable-test-host", make([]byte, 32))
	require.NoError(t, err)
	require.ErrorContains(t, runPair([]string{token}), "contact host")
	cfg := DefaultConfig(home)
	cometHome := filepath.Join(cfg.DataDir, "cometbft")
	genesis, err := cmttypes.GenesisDocFromFile(filepath.Join(cometHome, "config", "genesis.json"))
	require.NoError(t, err)
	require.Equal(t, sageabci.MaxSupportedAppVersion(), genesis.ConsensusParams.Version.App)
	require.True(t, hasSignedFreshGenesis(genesis.AppState))
	before := snapshotTestTree(t, home)
	require.NoError(t, ensureGenesisSeedWithConfig(cometHome, cfg, zerolog.Nop()))
	require.Equal(t, before, snapshotTestTree(t, home))
}
