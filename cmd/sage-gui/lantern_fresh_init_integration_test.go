package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cmttypes "github.com/cometbft/cometbft/types"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"gopkg.in/yaml.v3"
)

func TestLanternFreshInitRealGenesisIntegration(t *testing.T) {
	identities, chains := map[string]bool{}, map[string]bool{}
	for _, unit := range []string{"fixture-a", "fixture-b"} {
		t.Run(unit, func(t *testing.T) {
			home, cfg := lanternInitFixture(t)
			t.Setenv("SAGE_HOME", home)
			t.Setenv("SAGE_LANTERN_PRIVATE_LISTENERS", "1")
			t.Setenv("REST_ADDR", "127.0.0.1:8080")
			t.Setenv("SAGE_TLS_ADDR", "127.0.0.1:8443")
			t.Setenv("SAGE_CMT_RPC_ADDR", "tcp://127.0.0.1:26657")
			t.Setenv("SAGE_CMT_P2P_ADDR", "tcp://127.0.0.1:26656")
			for _, name := range []string{"SAGE_VENDORED_AGENT_KEY_FILE", "SAGE_VENDORED_AGENT_HOME_DOMAIN", "SAGE_VENDORED_AGENT_CLEARANCE", "SAGE_VOTER_ENABLED", "SAGE_VOTER_REQUIRED"} {
				t.Setenv(name, "")
			}
			cfg.Federation.Enabled = false
			cfg.Quorum.Enabled = false
			cfg.Voter.Enabled = true
			raw, err := yaml.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(home, "config.yaml"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err = LoadConfig()
			if err != nil {
				t.Fatal(err)
			}
			publicPath := filepath.Join(filepath.Dir(home), "identity", "lantern-public.key")
			result, err := initializeFreshLantern(home, cfg, initCometBFTConfigWithBootstrap, func(path string) (string, error) {
				key, err := loadOrGenerateKey(path)
				if err != nil {
					return "", err
				}
				return hex.EncodeToString(key.Public().(ed25519.PublicKey)), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			publicIDs := map[string]string{}
			preserved := map[string][]byte{}
			for role, path := range map[string]string{"root": cfg.AgentKey, "private": cfg.VendoredAgentBootstrap.AgentKeyFile, "public": publicPath} {
				seed, err := os.ReadFile(path)
				if err != nil || len(seed) != ed25519.SeedSize {
					t.Fatal("invalid generated seed", role, err)
				}
				info, err := os.Lstat(path)
				if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
					t.Fatal("unsafe generated key permissions", role)
				}
				identity := hex.EncodeToString(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey))
				if identities[identity] {
					t.Fatal("duplicate generated public identity")
				}
				identities[identity], publicIDs[role], preserved[path] = true, identity, seed
			}
			if result["public_agent_id"] != publicIDs["public"] || result["ready"] != false || result["public_enrolled"] != false || result["vault_initialized"] != false {
				t.Fatal("incorrect initialization result")
			}
			comet := filepath.Join(cfg.DataDir, "cometbft")
			genesisPath := filepath.Join(comet, "config", "genesis.json")
			genesis, err := cmttypes.GenesisDocFromFile(genesisPath)
			if err != nil {
				t.Fatal(err)
			}
			chain, err := readChainIDFromGenesis(comet)
			if err != nil || chain == "" || chain != genesis.ChainID || chains[chain] {
				t.Fatal("invalid or duplicate chain ID", err)
			}
			chains[chain] = true
			var app struct {
				Sage struct {
					InitialAdmin string                         `json:"initial_admin"`
					Manifest     sageabci.AppV23GenesisManifest `json:"app_v23_bootstrap"`
				} `json:"sage"`
			}
			if err := json.Unmarshal(genesis.AppState, &app); err != nil {
				t.Fatal(err)
			}
			manifest := app.Sage.Manifest
			if _, err := sageabci.VerifyAppV23GenesisManifest(chain, manifest); err != nil {
				t.Fatal("real signatures rejected", err)
			}
			if app.Sage.InitialAdmin != publicIDs["root"] || manifest.RootID != publicIDs["root"] || manifest.AgentID != publicIDs["private"] || manifest.Capabilities != 15 || manifest.HomeDomain != cfg.VendoredAgentBootstrap.HomeDomain || manifest.Clearance != cfg.VendoredAgentBootstrap.Clearance {
				t.Fatal("bootstrap identity/policy mismatch")
			}
			if _, err := sageabci.VerifyAppV23GenesisManifest(chain+"-wrong", manifest); err == nil {
				t.Fatal("signature accepted wrong chain")
			}
			for _, role := range []string{"root", "private"} {
				altered := manifest
				if role == "root" {
					altered.RootSignature = strings.Repeat("0", 128)
				} else {
					altered.AgentSignature = strings.Repeat("0", 128)
				}
				if _, err := sageabci.VerifyAppV23GenesisManifest(chain, altered); err == nil {
					t.Fatal("invalid signature accepted", role)
				}
			}
			for _, path := range []string{genesisPath, filepath.Join(comet, "config", "priv_validator_key.json"), filepath.Join(comet, "config", "node_key.json"), filepath.Join(comet, "data", "priv_validator_state.json"), filepath.Join(home, "config.yaml")} {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				preserved[path] = data
			}
			for attempt := 0; attempt < 2; attempt++ {
				reopened, err := LoadConfig()
				if err != nil {
					t.Fatal(err)
				}
				if err := preflightExistingVendoredGenesis(comet, reopened); err != nil {
					t.Fatal("startup genesis preflight failed", err)
				}
				if reopened.ChainID != chain || reopened.Federation.Enabled || reopened.Quorum.Enabled || reopened.RESTAddr != "127.0.0.1:8080" || reopened.Quorum.TLSAddr != "127.0.0.1:8443" {
					t.Fatal("reopened config inconsistent")
				}
			}
			if _, err := initializeFreshLantern(home, cfg, initCometBFTConfigWithBootstrap, func(string) (string, error) { t.Fatal("retry reached generator"); return "", nil }); err == nil {
				t.Fatal("repeat init accepted")
			}
			for path, prior := range preserved {
				after, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(prior, after) {
					t.Fatal("reread/retry changed generated state")
				}
			}
			for _, path := range []string{filepath.Join(cfg.DataDir, "sage.db"), filepath.Join(cfg.DataDir, "badger"), filepath.Join(home, "certs")} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatal("initialization created runtime state", err)
				}
			}
		})
	}
}
