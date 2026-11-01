package abci

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/validator"
)

const FreshGenesisManifestVersion uint8 = store.FreshGenesisVersion

type FreshGenesisValidator = store.FreshGenesisValidator

type FreshGenesisAgent struct {
	AgentID      string `json:"agent_id"`
	Profile      string `json:"profile"`
	Clearance    uint8  `json:"clearance"`
	Capabilities uint32 `json:"capabilities"`
	HomeDomain   string `json:"home_domain"`
	Signature    string `json:"signature"`
}

// FreshGenesisManifest binds a new chain's complete initial authority and
// rules. It is accepted only at InitChain on an empty consensus store.
type FreshGenesisManifest struct {
	Version       uint8                   `json:"version"`
	AppVersion    uint64                  `json:"app_version"`
	RootID        string                  `json:"root_id"`
	Agent         *FreshGenesisAgent      `json:"agent,omitempty"`
	Validators    []FreshGenesisValidator `json:"validators"`
	RootSignature string                  `json:"root_signature"`
}

func FreshGenesisManifestSignBytes(chainID string, manifest FreshGenesisManifest) []byte {
	out := appendAppV23ManifestPart(nil, "sage/fresh-genesis/v1")
	out = appendAppV23ManifestPart(out, chainID)
	out = append(out, manifest.Version)
	out = binary.BigEndian.AppendUint64(out, manifest.AppVersion)
	out = appendAppV23ManifestPart(out, manifest.RootID)
	if agent := manifest.Agent; agent != nil {
		out = append(out, 1)
		out = appendAppV23ManifestPart(out, agent.AgentID)
		out = appendAppV23ManifestPart(out, agent.Profile)
		out = append(out, agent.Clearance)
		out = binary.BigEndian.AppendUint32(out, agent.Capabilities)
		out = appendAppV23ManifestPart(out, agent.HomeDomain)
	} else {
		out = append(out, 0)
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(manifest.Validators))) // #nosec G115 -- verified roster max256
	for _, validator := range manifest.Validators {
		out = appendAppV23ManifestPart(out, validator.ValidatorID)
		out = binary.BigEndian.AppendUint64(out, uint64(validator.Power)) // #nosec G115 -- verified positive
	}
	return out
}

func (manifest FreshGenesisManifest) activation(scope, digest string) store.FreshGenesisActivation {
	activation := store.FreshGenesisActivation{Version: manifest.Version, AppVersion: manifest.AppVersion, RootID: manifest.RootID, Scope: scope, BootstrapDigest: digest, Validators: append([]store.FreshGenesisValidator(nil), manifest.Validators...)}
	if agent := manifest.Agent; agent != nil {
		activation.Agent = &store.FreshGenesisAgent{AgentID: agent.AgentID, Profile: agent.Profile, Clearance: agent.Clearance, Capabilities: agent.Capabilities, HomeDomain: agent.HomeDomain}
	}
	return activation
}

func VerifyFreshGenesisManifest(chainID string, manifest FreshGenesisManifest) (string, error) {
	if chainID == "" || strings.TrimSpace(chainID) != chainID || len(chainID) > 64 {
		return "", errors.New("fresh genesis requires a canonical chain_id of at most 64 bytes")
	}
	scope, err := governance.DelegationDomainForChainID(chainID)
	if err != nil {
		return "", err
	}
	if err := store.ValidateFreshGenesisActivation(manifest.activation(scope, strings.Repeat("0", 64))); err != nil {
		return "", err
	}
	if manifest.AppVersion > MaxCompiledAppVersion() {
		return "", errors.New("fresh genesis app version is not compiled")
	}
	_, rootKey, err := decodeManifestIdentity("root_id", manifest.RootID)
	if err != nil {
		return "", err
	}
	decodeSignature := func(name, value string) ([]byte, error) {
		sig, err := hex.DecodeString(value)
		if err != nil || len(sig) != ed25519.SignatureSize || hex.EncodeToString(sig) != value {
			return nil, fmt.Errorf("fresh genesis %s must be canonical Ed25519 hex", name)
		}
		return sig, nil
	}
	rootSig, err := decodeSignature("root_signature", manifest.RootSignature)
	if err != nil {
		return "", err
	}
	signBytes := FreshGenesisManifestSignBytes(chainID, manifest)
	if !ed25519.Verify(rootKey, signBytes, rootSig) {
		return "", errors.New("fresh genesis Root signature is invalid")
	}
	digest := sha256.New()
	digest.Write(signBytes)
	digest.Write(rootSig)
	if agent := manifest.Agent; agent != nil {
		_, key, err := decodeManifestIdentity("agent_id", agent.AgentID)
		if err != nil {
			return "", err
		}
		sig, err := decodeSignature("agent.signature", agent.Signature)
		if err != nil {
			return "", err
		}
		if !ed25519.Verify(key, signBytes, sig) {
			return "", errors.New("fresh genesis agent signature is invalid")
		}
		digest.Write(sig)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func prevalidateFreshGenesisRequest(req *abcitypes.RequestInitChain) (*FreshGenesisManifest, error) {
	if req == nil || len(req.AppStateBytes) == 0 {
		return nil, nil
	}
	var state struct {
		Sage struct {
			InitialAdmin string          `json:"initial_admin"`
			Fresh        json.RawMessage `json:"fresh_bootstrap"`
			Old          json.RawMessage `json:"app_v23_bootstrap"`
		} `json:"sage"`
	}
	if json.Unmarshal(req.AppStateBytes, &state) != nil {
		if bytes.Contains(req.AppStateBytes, []byte(`"fresh_bootstrap"`)) {
			return nil, errors.New("invalid fresh genesis app_state JSON")
		}
		return nil, nil
	}
	if len(state.Sage.Fresh) == 0 {
		return nil, nil
	}
	if len(req.AppStateBytes) > 128<<10 {
		return nil, errors.New("fresh genesis app_state exceeds 128 KiB")
	}
	if len(state.Sage.Old) > 0 && string(state.Sage.Old) != "null" {
		return nil, errors.New("fresh and app-v23 genesis manifests cannot coexist")
	}
	var manifest FreshGenesisManifest
	decoder := json.NewDecoder(bytes.NewReader(state.Sage.Fresh))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode fresh genesis manifest: %w", err)
	}
	if _, err := VerifyFreshGenesisManifest(req.ChainId, manifest); err != nil {
		return nil, err
	}
	if state.Sage.InitialAdmin != manifest.RootID {
		return nil, errors.New("fresh genesis root_id must equal sage.initial_admin")
	}
	if req.ConsensusParams == nil || req.ConsensusParams.Version == nil || req.ConsensusParams.Version.App != manifest.AppVersion {
		return nil, errors.New("fresh genesis consensus app version differs from signed manifest")
	}
	if req.InitialHeight != 0 && req.InitialHeight != 1 {
		return nil, errors.New("fresh genesis initial height must be 1")
	}
	if len(req.Validators) != len(manifest.Validators) {
		return nil, errors.New("fresh genesis validator roster differs from signed manifest")
	}
	want := make(map[string]int64, len(manifest.Validators))
	for _, validator := range manifest.Validators {
		want[validator.ValidatorID] = validator.Power
	}
	for _, update := range req.Validators {
		if len(update.PubKey.GetEd25519()) != ed25519.PublicKeySize {
			return nil, errors.New("fresh genesis validators must use Ed25519")
		}
		id := hex.EncodeToString(update.PubKey.GetEd25519())
		if power, found := want[id]; !found || power != update.Power {
			return nil, errors.New("fresh genesis validator roster differs from signed manifest")
		}
		delete(want, id)
	}
	if len(want) != 0 {
		return nil, errors.New("fresh genesis validator roster is incomplete")
	}
	return &manifest, nil
}

func (app *SageApp) bootstrapFreshGenesis(req *abcitypes.RequestInitChain, manifest FreshGenesisManifest) error {
	digest, err := VerifyFreshGenesisManifest(req.ChainId, manifest)
	if err != nil {
		return err
	}
	scope, err := governance.DelegationDomainForChainID(req.ChainId)
	if err != nil {
		return err
	}
	if expected := app.expectedConsensusChainIDValue(); expected != "" && expected != req.ChainId {
		return errors.New("fresh genesis chain_id differs from runtime binding")
	}
	if err := app.badgerStore.BootstrapFreshGenesis(manifest.activation(scope, digest)); err != nil {
		return err
	}
	if err := app.refreshFreshGenesis(); err != nil {
		return err
	}
	for _, existing := range app.validators.GetAll() {
		if err := app.validators.RemoveValidator(existing.ID); err != nil {
			return err
		}
	}
	for _, update := range req.Validators {
		key := append(ed25519.PublicKey(nil), update.PubKey.GetEd25519()...)
		if err := app.validators.AddValidator(&validator.ValidatorInfo{ID: hex.EncodeToString(key), PublicKey: key, Power: update.Power}); err != nil {
			return err
		}
	}
	return app.SetExpectedGovernanceDelegationDomain(req.ChainId)
}

func (app *SageApp) postFreshGenesisRules(version uint64, height int64) bool {
	return height > 0 && app.freshGenesisAppVersion >= version
}

func (app *SageApp) refreshFreshGenesis() error {
	origin, err := app.badgerStore.GetFreshGenesisActivation()
	if err != nil {
		return err
	}
	if origin == nil {
		app.freshGenesisAppVersion = 0
		return nil
	}
	if err := app.badgerStore.ValidateFreshGenesisLineage(); err != nil {
		return err
	}
	root, err := app.badgerStore.GetAppV23Root()
	if err != nil || root == nil || root.PrincipalID != origin.RootID || root.Scope != origin.Scope || root.BootstrapDigest != origin.BootstrapDigest {
		return errors.New("fresh genesis origin does not match Root state")
	}
	domain, err := app.badgerStore.GetState(governanceDelegationDomainStateKey)
	want, _ := hex.DecodeString(origin.Scope)
	if err != nil || !bytes.Equal(domain, want) {
		return errors.New("fresh genesis governance domain mismatch")
	}
	if app.state != nil && app.state.Height == 0 {
		if err := app.badgerStore.ValidateFreshGenesisInitialState(); err != nil {
			return err
		}
	}
	if err := app.validateAppV23StateForCurrentUpgrade(); err != nil {
		return fmt.Errorf("fresh genesis local RBAC: %w", err)
	}
	if origin.AppVersion >= 26 {
		if err := app.badgerStore.ValidateAppV26AccessGroupAuthorities(); err != nil {
			return err
		}
	}
	if origin.AppVersion >= 28 {
		if err := app.badgerStore.ValidatePublicMemoryStage(); err != nil {
			return err
		}
		if err := app.badgerStore.ValidateCoCommitTombstoneStage(); err != nil {
			return err
		}
	}
	app.freshGenesisAppVersion = origin.AppVersion
	if app.govEngine != nil {
		app.govEngine.SetMemoryTaintGenesisActive(origin.AppVersion >= 30)
	}
	return nil
}
