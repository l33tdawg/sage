package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/consensuskeys"
)

const (
	FreshGenesisVersion       uint8  = 1
	FreshGenesisMinAppVersion uint64 = 24
	FreshGenesisMaxAppVersion uint64 = 30
	FreshGenesisMaxValidators        = 256
)

type FreshGenesisValidator struct {
	ValidatorID string `json:"validator_id"`
	Power       int64  `json:"power"`
}

type FreshGenesisAgent struct {
	AgentID      string `json:"agent_id"`
	Profile      string `json:"profile"`
	Clearance    uint8  `json:"clearance"`
	Capabilities uint32 `json:"capabilities"`
	HomeDomain   string `json:"home_domain"`
}

// FreshGenesisActivation records an actual chain origin, not a synthetic
// history of upgrades. Its signed digest binds the target rules and roster.
type FreshGenesisActivation struct {
	Version         uint8                   `json:"version"`
	AppVersion      uint64                  `json:"app_version"`
	Scope           string                  `json:"scope"`
	BootstrapDigest string                  `json:"bootstrap_digest"`
	RootID          string                  `json:"root_id"`
	Agent           *FreshGenesisAgent      `json:"agent,omitempty"`
	Validators      []FreshGenesisValidator `json:"validators"`
}

func canonicalFreshDigest(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size && hex.EncodeToString(raw) == value
}

func ValidateFreshGenesisActivation(input FreshGenesisActivation) error {
	if input.Version != FreshGenesisVersion || input.AppVersion < FreshGenesisMinAppVersion || input.AppVersion > FreshGenesisMaxAppVersion {
		return errors.New("unsupported fresh genesis schema or app version")
	}
	if !canonicalFreshDigest(input.Scope) || !canonicalFreshDigest(input.BootstrapDigest) {
		return errors.New("fresh genesis requires canonical scope and bootstrap digest")
	}
	if err := validateCanonicalAgentID("fresh genesis root", input.RootID); err != nil {
		return err
	}
	if len(input.Validators) == 0 || len(input.Validators) > FreshGenesisMaxValidators {
		return errors.New("fresh genesis requires 1..256 validators")
	}
	var power int64
	for index, validator := range input.Validators {
		if err := validateCanonicalAgentID("fresh genesis validator", validator.ValidatorID); err != nil {
			return err
		}
		if index > 0 && input.Validators[index-1].ValidatorID >= validator.ValidatorID {
			return errors.New("fresh genesis validators must be unique and sorted")
		}
		// CometBFT reserves headroom for voting-power arithmetic (MaxInt64/8).
		if validator.Power <= 0 || validator.Power > ((int64(1)<<60)-1)-power {
			return errors.New("fresh genesis validator power exceeds consensus bound")
		}
		power += validator.Power
	}
	if agent := input.Agent; agent != nil {
		if err := validateCanonicalAgentID("fresh genesis agent", agent.AgentID); err != nil {
			return err
		}
		if agent.AgentID == input.RootID || !ValidAppV23Profile(agent.Profile) || agent.Profile == AppV23ProfileRoot || agent.Profile == AppV23ProfileLegacyRestricted {
			return errors.New("fresh genesis agent requires a distinct ordinary profile")
		}
		if agent.Clearance > 4 || !AgentCapabilities(agent.Capabilities).Valid() {
			return errors.New("invalid fresh genesis agent clearance or capabilities")
		}
		if agent.Profile == AppV23ProfileCompanion && agent.Capabilities != 15 {
			return errors.New("fresh genesis Companion must use capability mask 15")
		}
		if err := ValidateAppV23EnrollmentPolicy(AppV23RoleMember, agent.Profile, AgentCapabilities(agent.Capabilities), agent.Clearance, true); err != nil {
			return err
		}
		if agent.Profile == AppV23ProfileReadOnly {
			if agent.HomeDomain != "" {
				return errors.New("fresh genesis read-only agent must be domainless")
			}
			return nil
		}
		if err := ValidateAppV23DomainName(agent.HomeDomain); err != nil {
			return err
		}
		if IsSharedDomainName(agent.HomeDomain) {
			return errors.New("fresh genesis agent home must be non-shared")
		}
	}
	return nil
}

func freshGenesisActivationKey() []byte { return []byte(consensuskeys.FreshGenesisActivation) }

func (s *BadgerStore) GetFreshGenesisActivation() (*FreshGenesisActivation, error) {
	var activation FreshGenesisActivation
	err := s.view(func(txn *badger.Txn) error { return appV23ReadJSON(txn, freshGenesisActivationKey(), &activation) })
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := ValidateFreshGenesisActivation(activation); err != nil {
		return nil, fmt.Errorf("invalid fresh genesis activation: %w", err)
	}
	return &activation, nil
}

// BootstrapFreshGenesis atomically creates every origin-bound policy and index
// prerequisite. A nonempty store can only replay the exact uncommitted origin.
func (s *BadgerStore) BootstrapFreshGenesis(input FreshGenesisActivation) error {
	if err := ValidateFreshGenesisActivation(input); err != nil {
		return err
	}
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			var existing FreshGenesisActivation
			if err := appV23ReadJSON(txn, freshGenesisActivationKey(), &existing); err == nil {
				if !reflect.DeepEqual(existing, input) {
					return errors.New("fresh genesis already exists with a different manifest")
				}
				if item, heightErr := txn.Get(stateKey("height")); heightErr == nil {
					value, valueErr := item.ValueCopy(nil)
					if valueErr != nil || len(value) != 8 || binary.BigEndian.Uint64(value) != 0 {
						return errors.New("fresh genesis cannot reinitialize committed state")
					}
				} else if !errors.Is(heightErr, badger.ErrKeyNotFound) {
					return heightErr
				}
				return s.validateFreshGenesisInitialTxn(txn, input)
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			options := badger.DefaultIteratorOptions
			options.PrefetchValues = false
			iterator := txn.NewIterator(options)
			iterator.Rewind()
			if iterator.Valid() {
				key := iterator.Item().KeyCopy(nil)
				iterator.Close()
				return fmt.Errorf("fresh genesis found preexisting consensus key %q", key)
			}
			iterator.Close()
			entries, err := freshGenesisEntries(input)
			if err != nil {
				return err
			}
			for key, value := range entries {
				if err := s.txnSet(txn, []byte(key), value); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// freshGenesisEntries defines the entire pristine keyspace. Replays compare
// every key and value, so a partially initialized or contaminated image cannot
// become a valid new chain merely by presenting the same manifest again.
func freshGenesisEntries(input FreshGenesisActivation) (map[string][]byte, error) {
	history, err := appV23NextRootHistoryDigest("", 1, input.RootID)
	if err != nil {
		return nil, err
	}
	root := AppV23RootState{PrincipalID: input.RootID, CredentialID: input.RootID, Scope: input.Scope, Generation: 1, HistoryDigest: history, EstablishedAt: 1, BootstrapDigest: input.BootstrapDigest}
	entries := map[string][]byte{}
	addJSON := func(key []byte, value any) error {
		data, err := json.Marshal(value)
		if err == nil {
			entries[string(key)] = data
		}
		return err
	}
	values := []struct {
		key   []byte
		value any
	}{
		{freshGenesisActivationKey(), input},
		{agentOnChainKey(input.RootID), OnChainAgent{AgentID: input.RootID, Role: AppV23RoleAdmin, Clearance: 4, RegisteredAt: 1}},
		{appV23RootKey(), root},
		{appV23EnrollmentKey(input.RootID), AppV23LocalEnrollment{AgentID: input.RootID, ApprovedBy: input.RootID, RootGeneration: 1, Profile: AppV23ProfileRoot, Clearance: 4, Active: true, Revision: 1, UpdatedHeight: 1}},
		{appV23RoleKey(input.RootID), AppV23RoleState{AgentID: input.RootID, Role: AppV23RoleAdmin, Revision: 1, UpdatedBy: input.RootID, UpdatedHeight: 1}},
	}
	if agent := input.Agent; agent != nil {
		values = append(values,
			struct {
				key   []byte
				value any
			}{agentOnChainKey(agent.AgentID), OnChainAgent{AgentID: agent.AgentID, Role: AppV23RoleMember, Clearance: agent.Clearance, Capabilities: AgentCapabilities(agent.Capabilities), RegisteredAt: 1}},
			struct {
				key   []byte
				value any
			}{appV23EnrollmentKey(agent.AgentID), AppV23LocalEnrollment{AgentID: agent.AgentID, ApprovedBy: input.RootID, RootGeneration: 1, Profile: agent.Profile, HomeDomain: agent.HomeDomain, Clearance: agent.Clearance, Capabilities: AgentCapabilities(agent.Capabilities), Active: true, Revision: 1, UpdatedHeight: 1}},
			struct {
				key   []byte
				value any
			}{appV23RoleKey(agent.AgentID), AppV23RoleState{AgentID: agent.AgentID, Role: AppV23RoleMember, Revision: 1, UpdatedBy: input.RootID, UpdatedHeight: 1}})
		if agent.HomeDomain != "" {
			entries[string(domainKey(agent.HomeDomain))] = appV23EncodeDomain(agent.AgentID, 1)
		}
	}
	for _, value := range values {
		if err := addJSON(value.key, value.value); err != nil {
			return nil, err
		}
	}
	entries[string(appV23RootCredentialKey(input.RootID))] = appV23RootCredentialGenerationValue(1)
	entries[string(appV23AdminKey(input.RootID))] = []byte{1}
	entries[string(stateKey(consensuskeys.GovernanceDelegationDomainV20))], _ = hex.DecodeString(input.Scope)
	for _, validator := range input.Validators {
		entries["validator:"+validator.ValidatorID] = binary.BigEndian.AppendUint64(nil, uint64(validator.Power)) // #nosec G115 -- positive bound validated before bootstrap
	}
	if input.AppVersion >= 28 {
		// Source hash binds the canonical empty graph to the signed origin, rather
		// than fabricating an H-1 upgrade block.
		base, _ := hex.DecodeString(input.BootstrapDigest)
		rootKey := publicNodeKey([32]byte{}, 0)
		hash := sha256.New()
		hash.Write(rootKey)
		hash.Write(publicEmpty[0][:])
		manifest := binary.BigEndian.AppendUint64(nil, 1)
		manifest = append(manifest, base...)
		manifest = append(manifest, publicEmpty[0][:]...)
		manifest = append(manifest, hash.Sum(nil)...)
		entries[string(publicStageKey(rootKey))] = publicEmpty[0][:]
		entries[string(rootKey)] = publicEmpty[0][:]
		entries[string(publicStagePromoted)] = manifest
		co := binary.BigEndian.AppendUint64(nil, 1)
		co = append(co, base...)
		co = binary.BigEndian.AppendUint64(co, 0)
		empty := sha256.Sum256(nil)
		co = append(co, empty[:]...)
		entries[string(coCommitTombstonePromoted)] = co
	}
	return entries, nil
}

func (s *BadgerStore) validateFreshGenesisInitialTxn(txn *badger.Txn, input FreshGenesisActivation) error {
	expected, err := freshGenesisEntries(input)
	if err != nil {
		return err
	}
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		key := string(it.Item().Key())
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			return err
		}
		want, ok := expected[key]
		if !ok || !bytes.Equal(want, value) {
			return fmt.Errorf("fresh genesis pristine state differs at %q", key)
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		return errors.New("fresh genesis pristine state is incomplete")
	}
	return nil
}

func (s *BadgerStore) ValidateFreshGenesisLineage() error {
	activation, err := s.GetFreshGenesisActivation()
	if err != nil || activation == nil {
		return err
	}
	if old, err := s.GetAppV23GenesisActivation(); err != nil {
		return err
	} else if old != nil {
		return errors.New("fresh genesis cannot coexist with app-v23 genesis")
	}
	if migration, err := s.HasAppV23MigrationArtifacts(); err != nil {
		return err
	} else if migration {
		return errors.New("fresh genesis cannot coexist with migration artifacts")
	}
	for version := uint64(2); version <= activation.AppVersion; version++ {
		record, err := s.GetAppliedUpgrade(fmt.Sprintf("app-v%d", version))
		if err != nil {
			return err
		}
		if record != nil {
			return fmt.Errorf("fresh genesis cannot coexist with applied app-v%d", version)
		}
	}
	return nil
}

func (s *BadgerStore) ValidateFreshGenesisInitialState() error {
	activation, err := s.GetFreshGenesisActivation()
	if err != nil {
		return err
	}
	if activation == nil {
		return errors.New("fresh genesis activation is missing")
	}
	return s.view(func(txn *badger.Txn) error { return s.validateFreshGenesisInitialTxn(txn, *activation) })
}
