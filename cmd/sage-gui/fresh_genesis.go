package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmttypes "github.com/cometbft/cometbft/types"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
)

// latest means the release's supported version, never a compiled dormant fork.
func freshGenesisVersion(value string) (uint64, error) {
	if value == "latest" {
		return sageabci.MaxSupportedAppVersion(), nil
	}
	v, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(v, 10) != value || v < 24 || v > sageabci.MaxCompiledAppVersion() {
		return 0, fmt.Errorf("app version must be latest or a compiled fresh-genesis version from 24 through %d", sageabci.MaxCompiledAppVersion())
	}
	return v, nil
}

func freshGenesisAppStateWithKeys(
	chainID string,
	appVersion uint64,
	rootKey, agentKey ed25519.PrivateKey,
	bootstrap *VendoredAgentBootstrapConfig,
	validators []sageabci.FreshGenesisValidator,
) (json.RawMessage, error) {
	if len(rootKey) != ed25519.PrivateKeySize {
		return nil, errors.New("fresh genesis requires an Ed25519 Root key")
	}
	manifest := sageabci.FreshGenesisManifest{
		Version:    sageabci.FreshGenesisManifestVersion,
		AppVersion: appVersion,
		RootID:     appV23AgentIDForKey(rootKey),
		Validators: append([]sageabci.FreshGenesisValidator(nil), validators...),
	}
	sort.Slice(manifest.Validators, func(i, j int) bool {
		return manifest.Validators[i].ValidatorID < manifest.Validators[j].ValidatorID
	})
	if bootstrap != nil {
		if len(agentKey) != ed25519.PrivateKeySize {
			return nil, errors.New("fresh genesis requires the configured agent key")
		}
		manifest.Agent = &sageabci.FreshGenesisAgent{
			AgentID: appV23AgentIDForKey(agentKey), Profile: store.AppV23ProfileCompanion,
			Clearance: bootstrap.Clearance, Capabilities: uint32(store.AgentCapabilityReadAllDomains |
				store.AgentCapabilityDenySharedDomainWrite | store.AgentCapabilityDenyDomainClaim |
				store.AgentCapabilityDenyForeignDomainWrite),
			HomeDomain: strings.TrimSpace(bootstrap.HomeDomain),
		}
	}
	signBytes := sageabci.FreshGenesisManifestSignBytes(chainID, manifest)
	manifest.RootSignature = hex.EncodeToString(ed25519.Sign(rootKey, signBytes))
	if manifest.Agent != nil {
		manifest.Agent.Signature = hex.EncodeToString(ed25519.Sign(agentKey, signBytes))
	}
	if _, err := sageabci.VerifyFreshGenesisManifest(chainID, manifest); err != nil {
		return nil, fmt.Errorf("verify fresh genesis manifest: %w", err)
	}
	return json.Marshal(struct {
		Sage struct {
			InitialAdmin string                        `json:"initial_admin"`
			Bootstrap    sageabci.FreshGenesisManifest `json:"fresh_bootstrap"`
		} `json:"sage"`
	}{Sage: struct {
		InitialAdmin string                        `json:"initial_admin"`
		Bootstrap    sageabci.FreshGenesisManifest `json:"fresh_bootstrap"`
	}{manifest.RootID, manifest}})
}

func genesisAppStateForFreshChain(
	chainID, rootKeyPath string,
	bootstrap *VendoredAgentBootstrapConfig,
	appVersion uint64,
	validators []sageabci.FreshGenesisValidator,
) (json.RawMessage, error) {
	rootPath := filepath.Clean(expandTilde(rootKeyPath))
	if bootstrap != nil && filepath.Clean(expandTilde(bootstrap.AgentKeyFile)) == rootPath {
		return nil, errors.New("fresh genesis agent key must be distinct from the Root key")
	}
	if err := os.MkdirAll(filepath.Dir(rootPath), 0700); err != nil {
		return nil, err
	}
	rootKey, err := loadOrGenerateKey(rootPath)
	if err != nil {
		return nil, fmt.Errorf("load fresh genesis Root key: %w", err)
	}
	var agentKey ed25519.PrivateKey
	if bootstrap != nil {
		agentPath := filepath.Clean(expandTilde(bootstrap.AgentKeyFile))
		if mkdirErr := os.MkdirAll(filepath.Dir(agentPath), 0700); mkdirErr != nil {
			return nil, mkdirErr
		}
		agentKey, err = loadOrGenerateKey(agentPath)
		if err != nil {
			return nil, fmt.Errorf("load fresh genesis agent key: %w", err)
		}
	}
	return freshGenesisAppStateWithKeys(chainID, appVersion, rootKey, agentKey, bootstrap, validators)
}

// init-genesis creates an immutable artifact, not a node or an upgrade. One
// operator signs the complete validator roster and distributes this exact file
// to every new node. Private validator keys are neither accepted nor emitted.
func runInitGenesis(args []string) error {
	fs := flag.NewFlagSet("init-genesis", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	chainID := fs.String("chain-id", "", "globally unique new chain ID")
	rootPath := fs.String("root-key", "", "existing Ed25519 Root key file")
	validatorsPath := fs.String("validators", "", "JSON array of validator_id and power")
	outputPath := fs.String("output", "", "new genesis JSON output file")
	appVersion := fs.String("app-version", "latest", "latest or explicit compiled version")
	agentPath := fs.String("agent-key", "", "optional existing Companion key file")
	homeDomain := fs.String("home-domain", "", "Companion-owned non-shared domain")
	clearance := fs.Uint("clearance", 1, "Companion classification clearance (0..4)")
	genesisTime := fs.String("genesis-time", "", "optional RFC3339Nano UTC genesis time")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || *chainID == "" || strings.TrimSpace(*chainID) != *chainID || *rootPath == "" || *validatorsPath == "" || *outputPath == "" {
		return errors.New("init-genesis requires --chain-id, --root-key, --validators and --output; see init-genesis --help")
	}
	target, err := freshGenesisVersion(*appVersion)
	if err != nil {
		return err
	}
	if (*agentPath == "") != (*homeDomain == "") || *clearance > 4 {
		return errors.New("companion bootstrap requires both --agent-key and --home-domain, with --clearance from 0 through 4")
	}
	when := time.Now().UTC()
	if *genesisTime != "" {
		when, err = time.Parse(time.RFC3339Nano, *genesisTime)
		if err != nil {
			return fmt.Errorf("parse genesis time: %w", err)
		}
	}
	if _, statErr := os.Lstat(*outputPath); !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("genesis output already exists or is inaccessible; refusing to replace it")
	}
	rootKey, ok := parseKeyFile(expandTilde(*rootPath))
	if !ok {
		return errors.New("root key is unavailable or invalid")
	}
	var agentKey ed25519.PrivateKey
	var bootstrap *VendoredAgentBootstrapConfig
	if *agentPath != "" {
		agentKey, ok = parseKeyFile(expandTilde(*agentPath))
		if !ok {
			return errors.New("companion key is unavailable or invalid")
		}
		bootstrap = &VendoredAgentBootstrapConfig{AgentKeyFile: *agentPath, HomeDomain: *homeDomain, Clearance: uint8(*clearance)} // #nosec G115 -- bounded to 0..4 above
	}
	file, err := os.Open(expandTilde(*validatorsPath))
	if err != nil {
		return fmt.Errorf("open public validator roster: %w", err)
	}
	defer file.Close()
	rosterBytes, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return fmt.Errorf("read public validator roster: %w", err)
	}
	if len(rosterBytes) > 1<<20 {
		return errors.New("public validator roster exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(rosterBytes))
	decoder.DisallowUnknownFields()
	var validators []sageabci.FreshGenesisValidator
	if decodeErr := decoder.Decode(&validators); decodeErr != nil {
		return fmt.Errorf("decode public validator roster: %w", decodeErr)
	}
	var trailing any
	if decodeErr := decoder.Decode(&trailing); decodeErr != io.EOF {
		return errors.New("public validator roster has trailing data")
	}
	appState, err := freshGenesisAppStateWithKeys(*chainID, target, rootKey, agentKey, bootstrap, validators)
	if err != nil {
		return err
	}
	doc := cmttypes.GenesisDoc{ChainID: *chainID, GenesisTime: when, ConsensusParams: cmttypes.DefaultConsensusParams(), AppState: appState}
	doc.ConsensusParams.Version.App = target
	sort.Slice(validators, func(i, j int) bool { return validators[i].ValidatorID < validators[j].ValidatorID })
	for _, validator := range validators {
		public, decodeErr := hex.DecodeString(validator.ValidatorID)
		if decodeErr != nil {
			return decodeErr // The manifest verifier above rejects this first.
		}
		key := cmtcryptoed.PubKey(public)
		doc.Validators = append(doc.Validators, cmttypes.GenesisValidator{Address: key.Address(), PubKey: key, Power: validator.Power})
	}
	if validationErr := doc.ValidateAndComplete(); validationErr != nil {
		return fmt.Errorf("validate fresh genesis: %w", validationErr)
	}
	encoded, err := cmtjson.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	output, err := os.OpenFile(*outputPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("create fresh genesis output: %w", err)
	}
	_, writeErr := io.Copy(output, bytes.NewReader(encoded))
	if writeErr == nil {
		writeErr = output.Sync()
	}
	closeErr := output.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"genesis_file": *outputPath, "chain_id": *chainID, "app_version": target,
		"root_id": appV23AgentIDForKey(rootKey), "validators": len(validators),
	})
}

// A signed origin cannot be turned into a mutable historical genesis by a
// redeployment. Presence is sufficient here; malformed manifests fail closed.
func hasSignedFreshGenesis(appState json.RawMessage) bool {
	var state struct {
		Sage map[string]json.RawMessage `json:"sage"`
	}
	if json.Unmarshal(appState, &state) != nil {
		return false
	}
	_, present := state.Sage["fresh_bootstrap"]
	return present
}
