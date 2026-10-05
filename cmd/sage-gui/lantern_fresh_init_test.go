package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func lanternInitFixture(t *testing.T) (string, *Config) {
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
	if err := os.WriteFile(filepath.Join(home, "config.yaml"), []byte("synthetic test input"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig(home)
	cfg.VendoredAgentBootstrap = &VendoredAgentBootstrapConfig{AgentKeyFile: filepath.Join(base, "identity", "lantern-agent.key"), HomeDomain: "private-fixture", Clearance: 1}
	return home, cfg
}

func TestLanternFreshInitDelegatesWithoutRealKeys(t *testing.T) {
	home, cfg := lanternInitFixture(t)
	calls := 0
	initialize := func(comet, root string, bootstrap *VendoredAgentBootstrapConfig) error {
		calls++
		if comet != filepath.Join(home, "data", "cometbft") || root != cfg.AgentKey || bootstrap != cfg.VendoredAgentBootstrap {
			t.Fatal("wrong initializer binding")
		}
		return nil
	}
	public := func(path string) (string, error) {
		calls++
		if path != filepath.Join(filepath.Dir(home), "identity", "lantern-public.key") {
			t.Fatal("wrong public key destination")
		}
		return "synthetic-public-id", nil
	}
	result, err := initializeFreshLantern(home, cfg, initialize, public)
	if err != nil || calls != 2 || result["ready"] != false || result["public_enrolled"] != false {
		t.Fatal(result, err, calls)
	}
	if _, err := initializeFreshLantern(home, cfg, initialize, public); err == nil || calls != 2 {
		t.Fatal("repeated initialization admitted")
	}
	if _, err := os.Stat(cfg.AgentKey); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("test created a real key")
	}
}

func TestLanternFreshInitPartialFailureContained(t *testing.T) {
	home, cfg := lanternInitFixture(t)
	_, err := initializeFreshLantern(home, cfg, func(string, string, *VendoredAgentBootstrapConfig) error { return errors.New("injected failure") },
		func(string) (string, error) { t.Fatal("public generation after failure"); return "", nil })
	if err == nil {
		t.Fatal("failure hidden")
	}
	if err := freshLanternPaths(home, cfg); err == nil {
		t.Fatal("partial state reset permitted")
	}
}

func TestLanternFreshInitConcurrentClaim(t *testing.T) {
	home, cfg := lanternInitFixture(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := initializeFreshLantern(home, cfg, func(string, string, *VendoredAgentBootstrapConfig) error {
			close(entered)
			<-release
			return nil
		}, func(string) (string, error) { return "synthetic", nil })
		done <- err
	}()
	<-entered
	_, err := initializeFreshLantern(home, cfg, func(string, string, *VendoredAgentBootstrapConfig) error {
		t.Fatal("second initializer admitted")
		return nil
	}, func(string) (string, error) { return "synthetic", nil })
	close(release)
	if firstErr := <-done; firstErr != nil || err == nil {
		t.Fatal("exclusive claim failed", firstErr, err)
	}
}

func TestLanternFreshInitPublicFailureContained(t *testing.T) {
	home, cfg := lanternInitFixture(t)
	_, err := initializeFreshLantern(home, cfg, func(string, string, *VendoredAgentBootstrapConfig) error { return nil },
		func(string) (string, error) { return "", errors.New("injected public key failure") })
	if err == nil || freshLanternPaths(home, cfg) == nil {
		t.Fatal("public failure permitted retry")
	}
}

func TestLanternFreshInitRejectsExistingOrMismatched(t *testing.T) {
	for _, name := range []string{"root", "data", "public", "symlink", "permissions", "chain", "companion"} {
		t.Run(name, func(t *testing.T) {
			home, cfg := lanternInitFixture(t)
			switch name {
			case "root":
				os.WriteFile(cfg.AgentKey, []byte("not a key"), 0600)
			case "data":
				os.Mkdir(cfg.DataDir, 0700)
			case "public":
				os.WriteFile(filepath.Join(filepath.Dir(home), "identity", "lantern-public.key"), []byte("not a key"), 0600)
			case "symlink":
				os.Remove(filepath.Join(home, "config.yaml"))
				os.Symlink("/nonexistent", filepath.Join(home, "config.yaml"))
			case "permissions":
				os.Chmod(home, 0755)
			case "chain":
				cfg.ChainID = "copied-chain"
			case "companion":
				cfg.VendoredAgentBootstrap = nil
			}
			if err := freshLanternPaths(home, cfg); err == nil {
				t.Fatal("unsafe initialization admitted")
			}
		})
	}
}
