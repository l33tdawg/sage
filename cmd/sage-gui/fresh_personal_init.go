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

// freshPersonalPaths verifies that the prepared on-disk layout for a fresh
// personal-SAGE initialization is exactly what the profile promised. Every
// check here is a refusal, not a repair: partially-prepared state is preserved
// for reconciliation rather than reset.
func freshPersonalPaths(profile *provisioningProfile, cfg *Config) error {
	home, identityDir := profile.Home, profile.IdentityDir
	if cfg.VendoredAgentBootstrap == nil || cfg.DataDir != filepath.Join(home, "data") ||
		cfg.AgentKey != filepath.Join(home, "agent.key") ||
		filepath.Dir(cfg.VendoredAgentBootstrap.AgentKeyFile) != identityDir || cfg.ChainID != "" {
		return errors.New("explicit fixed-path companion bootstrap required")
	}
	for path := home; ; path = filepath.Dir(path) {
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || (info.Mode().Perm()&0022 != 0 && info.Mode()&os.ModeSticky == 0) {
			return errors.New("unsafe initialization directory")
		}
		if path == filepath.Dir(path) {
			break
		}
	}
	info, err := os.Lstat(home)
	if err != nil || info.Mode().Perm() != 0700 {
		return errors.New("private home must be 0700")
	}
	info, err = os.Lstat(identityDir)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return errors.New("prepared private identity directory required")
	}
	identityEntries, err := os.ReadDir(identityDir)
	if err != nil {
		return err
	}
	for _, entry := range identityEntries {
		if entry.Name() != profile.IdentityRequirementsFile {
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

// initializeFreshPersonalSage performs one claimed, all-or-nothing
// initialization. A failed attempt deliberately leaves its claim marker in
// place so a retry cannot silently continue from partial state.
func initializeFreshPersonalSage(profile *provisioningProfile, cfg *Config, initialize func(string, string, *VendoredAgentBootstrapConfig) error,
	publicIdentity func(string) (string, error)) (map[string]any, error) {
	home := profile.Home
	if err := freshPersonalPaths(profile, cfg); err != nil {
		return nil, err
	}
	claim, err := os.OpenFile(filepath.Join(home, profile.ClaimFile), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
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
	publicID, err := publicIdentity(filepath.Join(profile.IdentityDir, profile.PublicKeyFile))
	if err != nil {
		return nil, errors.New("public identity incomplete; preserve state for reconciliation")
	}
	return map[string]any{"schema": profile.ResultSchema, "public_agent_id": publicID,
		"public_enrolled": false, "vault_initialized": false, "ready": false, "provisioned": false}, nil
}

// parseFreshPersonalInitArgs accepts exactly one consent flag, one profile and
// one unit. Anything extra — a repeated flag, a positional argument, an empty
// value — is refused, so consent can never be inferred from a near miss.
func parseFreshPersonalInitArgs(args []string) (profilePath, unit string, ok bool) {
	const consentFlag = "--owner-approved-companion-bootstrap"
	if len(args) != 5 {
		return "", "", false
	}
	consent := false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case consentFlag:
			if consent {
				return "", "", false
			}
			consent = true
		case "--profile":
			if profilePath != "" || i+1 >= len(args) {
				return "", "", false
			}
			// Bounds are proven by the i+1 >= len(args) guard above; gosec cannot
			// follow that guard through the loop counter.
			profilePath = args[i+1] //nolint:gosec // G602: guarded immediately above
			i++
		case "--unit":
			if unit != "" || i+1 >= len(args) {
				return "", "", false
			}
			unit = args[i+1] //nolint:gosec // G602: guarded immediately above
			i++
		default:
			return "", "", false
		}
	}
	if !consent || profilePath == "" || unit == "" {
		return "", "", false
	}
	return profilePath, unit, true
}

func runFreshPersonalInit(args []string) error {
	profilePath, unit, ok := parseFreshPersonalInitArgs(args)
	if !ok {
		return errors.New("explicit owner Companion bootstrap consent and fixed private home required")
	}
	profile, err := loadProvisioningProfile(profilePath)
	if err != nil {
		return err
	}
	if !profile.PrivateListeners || SageHome() != profile.Home {
		return errors.New("explicit owner Companion bootstrap consent and fixed private home required")
	}
	account, err := user.Current()
	if runtime.GOOS != "linux" || runtime.GOARCH != "arm64" || err != nil ||
		account.Username != profile.Account || os.Geteuid() == 0 {
		return errors.New("initialization requires the prepared native ARM64 account")
	}
	if bindErr := profile.verifyUnitBinding(unit); bindErr != nil {
		return bindErr
	}
	// LoadConfig re-reads the active profile to enforce the private-listener
	// policy. Pin it to the profile this invocation was authorized against so
	// the two can never disagree. The command exits after initializing, so this
	// process-wide binding cannot outlive the check it protects.
	provisioningProfilePath = profilePath
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	if pathErr := freshPersonalPaths(profile, cfg); pathErr != nil {
		return pathErr
	}
	lock, err := acquireInstanceLock(profile.Home)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	result, err := initializeFreshPersonalSage(profile, cfg, initCometBFTConfigForFreshChain, func(path string) (string, error) {
		key, keyErr := loadOrGenerateKey(path)
		if keyErr != nil {
			return "", keyErr
		}
		return hex.EncodeToString(key[32:]), nil
	})
	if err != nil {
		return err
	}
	if err := completeFreshPersonalInitialization(profile, cfg, result); err != nil {
		return err
	}
	result["unit_id"] = unit
	return json.NewEncoder(os.Stdout).Encode(result)
}

func completeFreshPersonalInitialization(profile *provisioningProfile, cfg *Config, result map[string]any) error {
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
	return writeFreshPersonalMarkers(profile.Home)
}

func writeFreshPersonalMarkers(home string) error {
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
