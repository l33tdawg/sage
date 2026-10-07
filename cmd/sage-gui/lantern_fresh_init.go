package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const lanternHome = "/var/lib/sage-lantern/sage-personal"

func freshLanternPaths(home string, cfg *Config) error {
	if cfg.VendoredAgentBootstrap == nil || cfg.DataDir != filepath.Join(home, "data") ||
		cfg.AgentKey != filepath.Join(home, "agent.key") ||
		cfg.VendoredAgentBootstrap.AgentKeyFile != filepath.Join(filepath.Dir(home), "identity", "lantern-agent.key") || cfg.ChainID != "" {
		return errors.New("explicit fixed-path Lantern Companion bootstrap required")
	}
	for path := home; ; path = filepath.Dir(path) {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || (info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0) {
			return errors.New("unsafe Lantern initialization directory")
		}
		if path == filepath.Dir(path) {
			break
		}
	}
	info, err := os.Lstat(home)
	if err != nil || info.Mode().Perm() != 0700 {
		return errors.New("private home must be 0700")
	}
	identityDir := filepath.Join(filepath.Dir(home), "identity")
	info, err = os.Lstat(identityDir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("prepared private identity directory required")
	}
	identityEntries, err := os.ReadDir(identityDir)
	if err != nil {
		return err
	}
	for _, entry := range identityEntries {
		if entry.Name() != "EXTERNAL_IDENTITY_REQUIREMENTS.txt" {
			return errors.New("existing identity material; reconciliation required")
		}
	}
	entries, err := os.ReadDir(home)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() != "config.yaml" && entry.Name() != "sage.instance.lock" {
			return errors.New("existing identity or partial initialization; reconciliation required")
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return errors.New("unsafe initialization input")
		}
	}
	return nil
}

func initializeFreshLantern(home string, cfg *Config, initialize func(string, string, *VendoredAgentBootstrapConfig) error,
	publicIdentity func(string) (string, error)) (map[string]any, error) {
	if err := freshLanternPaths(home, cfg); err != nil {
		return nil, err
	}
	claim, err := os.OpenFile(filepath.Join(home, "lantern-initialization-started"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, errors.New("initialization already claimed")
	}
	if syncErr := claim.Sync(); syncErr != nil {
		claim.Close()
		return nil, syncErr
	}
	if closeErr := claim.Close(); closeErr != nil {
		return nil, closeErr
	}
	directory, err := os.Open(home)
	if err != nil {
		return nil, err
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		return nil, err
	}
	if initErr := initialize(filepath.Join(cfg.DataDir, "cometbft"), cfg.AgentKey, cfg.VendoredAgentBootstrap); initErr != nil {
		return nil, errors.New("initialization incomplete; preserve state for reconciliation")
	}
	publicID, err := publicIdentity(filepath.Join(filepath.Dir(home), "identity", "lantern-public.key"))
	if err != nil {
		return nil, errors.New("public identity incomplete; preserve state for reconciliation")
	}
	return map[string]any{"schema": "sage.lantern.initialized.v1", "public_agent_id": publicID,
		"public_enrolled": false, "vault_initialized": false, "ready": false, "provisioned": false}, nil
}

func runLanternFreshInit(args []string) error {
	if len(args) != 2 || args[0] != "--owner-approved-companion-bootstrap" || (args[1] != "unit-a" && args[1] != "unit-b") ||
		SageHome() != lanternHome || os.Getenv("SAGE_LANTERN_PRIVATE_LISTENERS") != "1" {
		return errors.New("explicit owner Companion bootstrap consent and fixed private home required")
	}
	account, err := user.Current()
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" || err != nil || account.Username != "sage-lantern" || os.Geteuid() == 0 {
		return errors.New("initialization requires the native ARM64 sage-lantern account")
	}
	metadata, err := os.ReadFile("/etc/sage-lantern/unit.env")
	if err != nil || len(metadata) > 16384 || strings.Count(string(metadata), "UNIT_ID=") != 1 ||
		!strings.Contains("\n"+string(metadata), "\nUNIT_ID="+args[1]+"\n") {
		return errors.New("prepared unit binding mismatch")
	}
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if pathErr := freshLanternPaths(lanternHome, cfg); pathErr != nil {
		return pathErr
	}
	lock, err := acquireInstanceLock(lanternHome)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	result, err := initializeFreshLantern(lanternHome, cfg, initCometBFTConfigWithBootstrap, func(path string) (string, error) {
		key, keyErr := loadOrGenerateKey(path)
		if keyErr != nil {
			return "", keyErr
		}
		return hex.EncodeToString(key[32:]), nil
	})
	if err != nil {
		return err
	}
	if err := completeFreshLanternInitialization(lanternHome, cfg, result); err != nil {
		return err
	}
	result["unit_id"] = args[1]
	return json.NewEncoder(os.Stdout).Encode(result)
}

func completeFreshLanternInitialization(home string, cfg *Config, result map[string]any) error {
	if err := preflightExistingVendoredGenesis(filepath.Join(cfg.DataDir, "cometbft"), cfg); err != nil {
		return errors.New("generated genesis verification failed; preserve partial state")
	}
	chainID, err := readChainIDFromGenesis(filepath.Join(cfg.DataDir, "cometbft"))
	if err != nil {
		return err
	}
	seen := map[string]bool{result["public_agent_id"].(string): true}
	for name, path := range map[string]string{"root_id": cfg.AgentKey, "private_agent_id": cfg.VendoredAgentBootstrap.AgentKeyFile} {
		seed, err := os.ReadFile(path)
		if err != nil || len(seed) != ed25519.SeedSize {
			return errors.New("generated key verification failed")
		}
		identity := hex.EncodeToString(ed25519.NewKeyFromSeed(seed)[32:])
		if seen[identity] {
			return errors.New("generated identities are not distinct")
		}
		seen[identity] = true
		result[name] = identity
	}
	result["chain_id"] = chainID
	result["local_enrollment_committed"] = false
	return writeFreshLanternMarkers(home)
}

func writeFreshLanternMarkers(home string) error {
	if strings.TrimSpace(version) != version || version == "" || strings.ContainsAny(version, "\r\n") || ConsensusForkVersion <= 0 {
		return errors.New("invalid initialization lineage metadata")
	}
	markers := []struct{ name, value string }{
		{versionFile, version + "\n"},
		{forkVersionFile, strconv.Itoa(ConsensusForkVersion) + "\n"},
	}
	for _, marker := range markers {
		if _, err := os.Lstat(filepath.Join(home, marker.name)); !os.IsNotExist(err) {
			return errors.New("initialization lineage marker exists or is inaccessible; preserve partial state")
		}
	}
	directory, err := os.Open(home)
	if err != nil {
		return err
	}
	defer directory.Close()
	for _, marker := range markers {
		file, err := os.OpenFile(filepath.Join(home, marker.name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return err
		}
		if _, err := file.WriteString(marker.value); err != nil {
			file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		if err := directory.Sync(); err != nil {
			return err
		}
	}
	return nil
}
