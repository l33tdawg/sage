package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func markerInitFixture(t *testing.T) (string, *Config) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	home := filepath.Join(base, "sage-personal")
	for _, path := range []string{home, filepath.Join(base, "identity")} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("SAGE_HOME", home)
	cfg := DefaultConfig(home)
	cfg.Quorum.TLSAddr = "127.0.0.1:8443"
	cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{
		AgentKeyFile: filepath.Join(base, "identity", "lantern-agent.key"),
		HomeDomain:   "private-fixture", Clearance: 1,
	}
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "config.yaml"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	prior := version
	version = "lantern-local.marker-regression-v27"
	t.Cleanup(func() { version = prior })
	return home, cfg
}

func realMarkerInitialization(t *testing.T, home string, cfg *Config) map[string]any {
	t.Helper()
	result, err := initializeFreshLantern(home, cfg, initCometBFTConfigWithBootstrap, func(path string) (string, error) {
		key, err := loadOrGenerateKey(path)
		if err != nil {
			return "", err
		}
		return hex.EncodeToString(key[32:]), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertNoInitMarkers(t *testing.T, home string) {
	t.Helper()
	for _, name := range []string{versionFile, forkVersionFile} {
		if _, err := os.Lstat(filepath.Join(home, name)); !os.IsNotExist(err) {
			t.Fatalf("unexpected marker %s: %v", name, err)
		}
	}
}

func TestFreshLanternReleaseInitializationMigration(t *testing.T) {
	home, cfg := markerInitFixture(t)
	result := realMarkerInitialization(t, home, cfg)
	assertNoInitMarkers(t, home)
	comet := filepath.Join(cfg.DataDir, "cometbft")
	preserved := map[string][]byte{}
	for _, path := range []string{cfg.AgentKey, cfg.VendoredAgentBootstrap.AgentKeyFile,
		filepath.Join(filepath.Dir(home), "identity", "lantern-public.key"),
		filepath.Join(comet, "config", "genesis.json"),
		filepath.Join(comet, "config", "priv_validator_key.json"),
		filepath.Join(comet, "config", "node_key.json"),
		filepath.Join(comet, "data", "priv_validator_state.json"), filepath.Join(home, "config.yaml")} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		preserved[path] = raw
	}
	if _, err := migrateOnUpgrade(cfg.DataDir); !errors.Is(err, errAutomaticChainResetRefused) {
		t.Fatalf("markerless existing genesis must still refuse: %v", err)
	}
	assertNoInitMarkers(t, home)
	if err := completeFreshLanternInitialization(home, cfg, result); err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string]string{versionFile: version + "\n", forkVersionFile: "1\n"} {
		path := filepath.Join(home, name)
		raw, err := os.ReadFile(path)
		if err != nil || string(raw) != expected {
			t.Fatalf("incorrect lineage marker %s", name)
		}
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			t.Fatalf("unsafe lineage marker %s", name)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := preflightVendoredStartup(comet, cfg); err != nil {
			t.Fatal(err)
		}
		fresh, err := genuinelyFreshNodeOrigin(cfg.DataDir)
		if err != nil || fresh {
			t.Fatalf("initialized origin misclassified: %v", err)
		}
		if err := preflightGenesisOrigin(comet, fresh); err != nil {
			t.Fatal(err)
		}
		if migrated, err := migrateOnUpgrade(cfg.DataDir); err != nil || migrated {
			t.Fatalf("release migration failed or reset state: %v", err)
		}
	}
	for path, before := range preserved {
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("initialization completion changed identity/genesis/config")
		}
	}
	for _, path := range []string{filepath.Join(cfg.DataDir, "badger"), filepath.Join(cfg.DataDir, "sage.db")} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatal("completion created runtime store")
		}
	}
	if err := completeFreshLanternInitialization(home, cfg, result); err == nil {
		t.Fatal("repeated completion accepted")
	}
}

func TestFreshLanternMarkersNeverOverwrite(t *testing.T) {
	for _, name := range []string{versionFile, forkVersionFile} {
		for _, kind := range []string{"valid", "malformed", "symlink", "directory"} {
			t.Run(name+"/"+kind, func(t *testing.T) {
				home, cfg := markerInitFixture(t)
				result := realMarkerInitialization(t, home, cfg)
				path := filepath.Join(home, name)
				raw := []byte("1\n")
				var err error
				switch kind {
				case "directory":
					err = os.Mkdir(path, 0700)
				case "symlink":
					err = os.Symlink("absent-target", path)
				default:
					if kind == "malformed" {
						raw = []byte("not-lineage\n")
					}
					err = os.WriteFile(path, raw, 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
				before, _ := os.Lstat(path)
				if err := completeFreshLanternInitialization(home, cfg, result); err == nil {
					t.Fatal("existing marker accepted")
				}
				after, err := os.Lstat(path)
				if err != nil || !os.SameFile(before, after) || before.Mode() != after.Mode() {
					t.Fatal("existing marker replaced")
				}
				if kind == "valid" || kind == "malformed" {
					actual, err := os.ReadFile(path)
					if err != nil || !bytes.Equal(actual, raw) {
						t.Fatal("existing marker overwritten")
					}
				}
				other := versionFile
				if name == versionFile {
					other = forkVersionFile
				}
				if _, err := os.Lstat(filepath.Join(home, other)); !os.IsNotExist(err) {
					t.Fatal("created another marker despite conflict")
				}
				if _, err := os.Stat(filepath.Join(home, "lantern-initialization-started")); err != nil {
					t.Fatal("partial claim lost")
				}
			})
		}
	}
}

func TestFreshLanternFailedInitializationHasNoMarkers(t *testing.T) {
	for _, failure := range []string{"initializer", "public-identity", "genesis", "root-key", "chain-id", "duplicate-identity"} {
		t.Run(failure, func(t *testing.T) {
			home, cfg := markerInitFixture(t)
			if failure == "initializer" || failure == "public-identity" {
				initialize := initCometBFTConfigWithBootstrap
				if failure == "initializer" {
					initialize = func(string, string, *VendoredAgentBootstrapConfig) error { return errors.New("injected") }
				}
				if _, err := initializeFreshLantern(home, cfg, initialize, func(string) (string, error) { return "", errors.New("injected") }); err == nil {
					t.Fatal("failed initializer accepted")
				}
			} else {
				result := realMarkerInitialization(t, home, cfg)
				switch failure {
				case "genesis":
					if err := os.WriteFile(filepath.Join(cfg.DataDir, "cometbft", "config", "genesis.json"), []byte("invalid"), 0600); err != nil {
						t.Fatal(err)
					}
				case "root-key":
					if err := os.WriteFile(cfg.AgentKey, []byte("invalid"), 0600); err != nil {
						t.Fatal(err)
					}
				case "chain-id":
					cfg.ChainID = "sage-personal-wrong"
				case "duplicate-identity":
					seed, err := os.ReadFile(cfg.AgentKey)
					if err != nil {
						t.Fatal(err)
					}
					result["public_agent_id"] = hex.EncodeToString(ed25519.NewKeyFromSeed(seed)[32:])
				}
				if err := completeFreshLanternInitialization(home, cfg, result); err == nil {
					t.Fatal("failed verification accepted")
				}
			}
			assertNoInitMarkers(t, home)
			if _, err := os.Stat(filepath.Join(home, "lantern-initialization-started")); err != nil {
				t.Fatal("partial claim lost")
			}
		})
	}
}

func TestFreshLanternPreexistingMarkersRefuseBeforeGenerating(t *testing.T) {
	for _, name := range []string{versionFile, forkVersionFile} {
		t.Run(name, func(t *testing.T) {
			home, cfg := markerInitFixture(t)
			path := filepath.Join(home, name)
			if err := os.WriteFile(path, []byte("unreviewed\n"), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := initializeFreshLantern(home, cfg,
				func(string, string, *VendoredAgentBootstrapConfig) error {
					t.Fatal("preexisting lineage reached key/genesis generation")
					return nil
				}, func(string) (string, error) {
					t.Fatal("preexisting lineage reached public-key generation")
					return "", nil
				})
			if err == nil {
				t.Fatal("preexisting marker accepted")
			}
			if _, err := os.Lstat(filepath.Join(home, "lantern-initialization-started")); !os.IsNotExist(err) {
				t.Fatal("claimed an existing origin")
			}
			actual, err := os.ReadFile(path)
			if err != nil || string(actual) != "unreviewed\n" {
				t.Fatal("preexisting marker changed")
			}
		})
	}
}
