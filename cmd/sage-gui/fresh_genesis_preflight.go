package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cmttypes "github.com/cometbft/cometbft/types"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/store"
)

// The latest-version origin is immutable, just like historical direct-v23.
// Inspecting it may recover the config's chain-ID cache, never mint identities.
func preflightExistingFreshVendoredGenesis(cometHome string, cfg *Config, genesis *cmttypes.GenesisDoc) (bool, error) {
	var appState struct {
		Sage struct {
			InitialAdmin string                         `json:"initial_admin"`
			Bootstrap    *sageabci.FreshGenesisManifest `json:"fresh_bootstrap"`
		} `json:"sage"`
	}
	if err := json.Unmarshal(genesis.AppState, &appState); err != nil || appState.Sage.Bootstrap == nil {
		return false, nil
	}
	manifest := appState.Sage.Bootstrap
	fail := func(err error) (bool, error) { return true, err }
	if _, err := sageabci.VerifyFreshGenesisManifest(genesis.ChainID, *manifest); err != nil {
		return fail(fmt.Errorf("verify existing fresh genesis: %w", err))
	}
	if genesis.ConsensusParams == nil || genesis.ConsensusParams.Version.App != manifest.AppVersion || appState.Sage.InitialAdmin != manifest.RootID {
		return fail(errors.New("fresh genesis consensus version or initial Root differs from its signed manifest"))
	}
	if cfg.ChainID != "" && cfg.ChainID != genesis.ChainID {
		return fail(errors.New("fresh genesis chain ID differs from configured chain ID"))
	}
	if len(genesis.Validators) != len(manifest.Validators) {
		return fail(errors.New("fresh genesis validator roster differs from its signed manifest"))
	}
	powers := make(map[string]int64, len(manifest.Validators))
	for _, validator := range manifest.Validators {
		powers[validator.ValidatorID] = validator.Power
	}
	for _, validator := range genesis.Validators {
		if _, ok := validator.PubKey.(cmtcryptoed.PubKey); !ok || len(validator.PubKey.Bytes()) != ed25519.PublicKeySize {
			return fail(errors.New("fresh genesis validator must use Ed25519"))
		}
		id := hex.EncodeToString(validator.PubKey.Bytes())
		if powers[id] != validator.Power {
			return fail(errors.New("fresh genesis validator power differs from its signed manifest"))
		}
		delete(powers, id)
	}
	if len(powers) != 0 {
		return fail(errors.New("fresh genesis is missing a signed validator"))
	}
	localID, available, err := localVendoredValidatorID(cometHome)
	if err != nil || !available {
		return fail(errors.New("local validator key is unavailable for the fresh genesis"))
	}
	var localPublic cmtcryptoed.PubKey
	for _, validator := range genesis.Validators {
		if hex.EncodeToString(validator.PubKey.Bytes()) == localID {
			localPublic = validator.PubKey.(cmtcryptoed.PubKey)
		}
	}
	if localPublic == nil {
		return fail(errors.New("local validator is absent from signed fresh genesis"))
	}
	if err := validateVendoredValidatorState(filepath.Join(cometHome, "data", "priv_validator_state.json"), localPublic, genesis.ChainID); err != nil {
		return fail(fmt.Errorf("verify fresh genesis validator state: %w", err))
	}
	if err := validateVendoredNodeKey(filepath.Join(cometHome, "config", "node_key.json")); err != nil {
		return fail(err)
	}
	rootKey, err := readFederationTransportKey(cfg.AgentKey)
	if err != nil || appV23AgentIDForKey(rootKey) != manifest.RootID {
		return fail(errors.New("stable transport key differs from signed fresh genesis Root"))
	}
	bootstrap := cfg.VendoredAgentBootstrap
	if bootstrap == nil || manifest.Agent == nil {
		return fail(errors.New("configured Companion is missing from signed fresh genesis"))
	}
	agentKey, available := parseKeyFile(bootstrap.AgentKeyFile)
	if !available || appV23AgentIDForKey(agentKey) != manifest.Agent.AgentID ||
		manifest.Agent.Profile != store.AppV23ProfileCompanion || manifest.Agent.Capabilities != 15 ||
		manifest.Agent.HomeDomain != bootstrap.HomeDomain || manifest.Agent.Clearance != bootstrap.Clearance {
		return fail(errors.New("configured Companion identity or policy differs from signed fresh genesis"))
	}
	if cfg.ChainID == "" {
		cfg.ChainID = genesis.ChainID
	}
	return true, nil
}
