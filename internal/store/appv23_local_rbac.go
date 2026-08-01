package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/consensuskeys"
)

const (
	AppV23RoleMember  = "member"
	AppV23RoleManager = "manager"
	AppV23RoleAdmin   = "admin"

	AppV23ProfileRoot      = "root"
	AppV23ProfileStandard  = "standard"
	AppV23ProfileCompanion = "companion"
	AppV23ProfileReadOnly  = "read_only"
	// AppV23ProfileLegacyRestricted is a migration-only compatibility profile.
	// It preserves an exact operator-set app-v22 capability mask until the
	// operator explicitly replaces it with one of app-v23's named presets.
	// Fresh enrollment and ordinary policy mutation must never select it.
	AppV23ProfileLegacyRestricted = "legacy_restricted"

	AppV23MaxGroupMembers          = 64
	AppV23MaxGroupsPerAgent        = 16
	AppV23MaxAdmins                = 32
	AppV23MaxGroups                = 256
	AppV23MaxMembershipLinks       = 4096
	AppV23MaxElevationWindow       = int64(20)
	AppV23MaxLegacyMembershipLinks = 65536
	AppV23MaxLegacyFederations     = 4096

	// Small rosters retain the compact one-transaction representation used by
	// early app-v23 development. Above this protocol-fixed boundary activation
	// uses the crash-recoverable promoted stage. The final marker transaction
	// then contains only Root, at most AppV23MaxAdmins indices, and migration
	// headers -- independent of roster size and comfortably below Badger's
	// default and supported transaction limits.
	appV23MaxInlineMigrationAgents = 512
)

var (
	ErrAppV23RevisionConflict = errors.New("app-v23 revision conflict")
	ErrAppV23NeedsApproval    = errors.New("app-v23 local agent needs approval")
)

type AppV23RootState struct {
	PrincipalID     string `json:"principal_id"`
	CredentialID    string `json:"credential_id"`
	Scope           string `json:"scope"`
	Generation      uint64 `json:"generation"`
	HistoryDigest   string `json:"history_digest"`
	EstablishedAt   int64  `json:"established_at"`
	BootstrapDigest string `json:"bootstrap_digest,omitempty"`
}

type AppV23LocalEnrollment struct {
	AgentID                 string            `json:"agent_id"`
	ApprovedBy              string            `json:"approved_by"`
	RootGeneration          uint64            `json:"root_generation"`
	Profile                 string            `json:"profile"`
	HomeDomain              string            `json:"home_domain,omitempty"`
	ExpectedHomeDomainOwner string            `json:"-"`
	TransferHomeDomain      bool              `json:"-"`
	Clearance               uint8             `json:"clearance"`
	Capabilities            AgentCapabilities `json:"capabilities"`
	Active                  bool              `json:"active"`
	Revision                uint64            `json:"revision"`
	UpdatedHeight           int64             `json:"updated_height"`
}

type AppV23RoleState struct {
	AgentID       string `json:"agent_id"`
	Role          string `json:"role"`
	Revision      uint64 `json:"revision"`
	UpdatedBy     string `json:"updated_by"`
	UpdatedHeight int64  `json:"updated_height"`
}

type AppV23AccessGroup struct {
	GroupID       string   `json:"group_id"`
	Name          string   `json:"name"`
	Members       []string `json:"members"`
	Revision      uint64   `json:"revision"`
	UpdatedBy     string   `json:"updated_by"`
	UpdatedHeight int64    `json:"updated_height"`
}

type AppV23GenesisBootstrap struct {
	RootID          string
	Scope           string
	AgentID         string
	Profile         string
	HomeDomain      string
	Clearance       uint8
	Capabilities    AgentCapabilities
	Height          int64
	BootstrapDigest string
	ValidatorID     string
	ValidatorPower  int64
	// ActivateAtGenesis is reserved for the dual-signed first-party genesis
	// manifest. Ordinary tests and fixtures may bootstrap policy state without
	// changing the chain's consensus-version origin.
	ActivateAtGenesis bool
}

type AppV23GenesisActivation struct {
	Version         uint64 `json:"version"`
	Scope           string `json:"scope"`
	BootstrapDigest string `json:"bootstrap_digest"`
	RootID          string `json:"root_id"`
	AgentID         string `json:"agent_id"`
	Profile         string `json:"profile"`
	HomeDomain      string `json:"home_domain"`
	Clearance       uint8  `json:"clearance"`
	Capabilities    uint32 `json:"capabilities"`
	ValidatorID     string `json:"validator_id"`
	ValidatorPower  int64  `json:"validator_power"`
}

type AppV23MigrationDisposition struct {
	AgentID            string `json:"agent_id"`
	LegacyPolicyDigest string `json:"legacy_policy_digest"`
	Disposition        string `json:"disposition"`
	Profile            string `json:"profile"`
	HomeDomain         string `json:"home_domain,omitempty"`
	Active             bool   `json:"active"`
}

type AppV23LegacyOrgMembership struct {
	OrgID     string `json:"org_id"`
	Clearance uint8  `json:"clearance"`
	Role      string `json:"role"`
}

type AppV23LegacyDeptMembership struct {
	OrgID     string `json:"org_id"`
	DeptID    string `json:"dept_id"`
	Clearance uint8  `json:"clearance"`
	Role      string `json:"role"`
}

// AppV23LegacyReadBaseline is an immutable, migration-only H-1 snapshot. It
// retains explicit configured local read policy and membership edges without
// reviving the historical empty-DomainAccess allow-all default.
type AppV23LegacyReadBaseline struct {
	AgentID            string                       `json:"agent_id"`
	LegacyPolicyDigest string                       `json:"legacy_policy_digest"`
	DomainAccess       string                       `json:"domain_access,omitempty"`
	VisibleAgents      string                       `json:"visible_agents,omitempty"`
	OrgMemberships     []AppV23LegacyOrgMembership  `json:"org_memberships,omitempty"`
	DeptMemberships    []AppV23LegacyDeptMembership `json:"dept_memberships,omitempty"`
}

type AppV23LegacyFederationBaseline struct {
	FederationID   string   `json:"federation_id"`
	ProposerOrgID  string   `json:"proposer_org_id"`
	TargetOrgID    string   `json:"target_org_id"`
	AllowedDomains []string `json:"allowed_domains"`
	AllowedDepts   []string `json:"allowed_depts,omitempty"`
	MaxClearance   uint8    `json:"max_clearance"`
	ExpiresAt      int64    `json:"expires_at"`
	Status         string   `json:"status"`
}

type AppV23LegacyReadDecision struct {
	Eligible                  bool
	Allowed                   bool
	ExplicitDomainRestriction bool
}

type AppV23MigrationState struct {
	SchemaDigest          string   `json:"schema_digest"`
	ManifestDigest        string   `json:"manifest_digest"`
	StageDigest           string   `json:"stage_digest"`
	StageCount            int      `json:"stage_count"`
	RootBootstrapDigest   string   `json:"root_bootstrap_digest,omitempty"`
	AgentCount            int      `json:"agent_count"`
	LegacyAdmins          []string `json:"legacy_admins,omitempty"`
	LegacyAdminCount      int      `json:"legacy_admin_count"`
	LegacyAdminDigest     string   `json:"legacy_admin_digest"`
	LegacyReadCount       int      `json:"legacy_read_count,omitempty"`
	LegacyFederationCount int      `json:"legacy_federation_count,omitempty"`
	Height                int64    `json:"height"`
}

func appV23MigrationSchemaDigest() string {
	sum := sha256.Sum256([]byte("sage/app-v23-migration-schema/6:single-root,legacy-admin-review,observer-read-only-pipe-preserved,owned-home,pending-disposition-complete-fingerprint,deterministic-shared-safe-home,exact-legacy-restrictions,domainless-deny-claim-compatibility,immutable-local-read-baseline,promoted-large-roster-stage,bounded-admin-audit,ownership-only-home"))
	return hex.EncodeToString(sum[:])
}

const appV23InlineLegacyAdminAuditLimit = 1024

func appV23LegacyAdminDigest(admins []string) string {
	digest := sha256.New()
	var length [8]byte
	for _, admin := range admins {
		binary.BigEndian.PutUint64(length[:], uint64(len(admin)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(admin))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func appV23SetLegacyAdminSummary(migration *AppV23MigrationState, admins []string) {
	sort.Strings(admins)
	migration.LegacyAdminCount = len(admins)
	migration.LegacyAdminDigest = appV23LegacyAdminDigest(admins)
	if len(admins) <= appV23InlineLegacyAdminAuditLimit {
		migration.LegacyAdmins = append([]string(nil), admins...)
	} else {
		migration.LegacyAdmins = nil
	}
}

type AppV23DomainVerb uint8

const (
	AppV23VerbRead AppV23DomainVerb = iota + 1
	AppV23VerbWrite
	AppV23VerbModify
)

type AppV23Authorization struct {
	Allowed      bool
	ExplicitDeny bool
	Reason       string
}

// AppV23ElevationUse is the consensus replay record for one root-countersigned
// delegated Admin action. Cryptographic verification happens in ABCI; stores
// re-check generation/freshness and consume the nonce in the same Badger
// transaction as the authorized mutation.
type AppV23ElevationUse struct {
	AdminID          string
	RootGeneration   uint64
	ValidFromHeight  int64
	ValidUntilHeight int64
	Nonce            string
}

func appV23RootKey() []byte { return []byte("appv23:root") }
func appV23RootCredentialKey(id string) []byte {
	return []byte("appv23:root_credential:" + id)
}
func appV23RootCredentialGenerationValue(generation uint64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, generation)
	return value
}
func appV23NextRootHistoryDigest(previous string, generation uint64, credentialID string) (string, error) {
	if generation == 0 {
		return "", errors.New("root credential history generation must be positive")
	}
	var previousBytes []byte
	if generation == 1 {
		if previous != "" {
			return "", errors.New("first root credential history digest must not have a predecessor")
		}
	} else {
		decoded, err := hex.DecodeString(previous)
		if err != nil || len(decoded) != sha256.Size {
			return "", errors.New("previous root credential history digest is invalid")
		}
		previousBytes = decoded
	}
	credentialBytes, err := hex.DecodeString(credentialID)
	if err != nil || len(credentialBytes) != 32 {
		return "", errors.New("root credential history identity is invalid")
	}
	generationBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(generationBytes, generation)
	digest := sha256.New()
	_, _ = digest.Write(previousBytes)
	_, _ = digest.Write(generationBytes)
	_, _ = digest.Write(credentialBytes)
	return hex.EncodeToString(digest.Sum(nil)), nil
}
func appV23EnrollmentKey(id string) []byte { return []byte("appv23:enroll:" + id) }
func appV23RoleKey(id string) []byte       { return []byte("appv23:role:" + id) }
func appV23AdminKey(id string) []byte      { return []byte("appv23:admin:" + id) }
func appV23GroupKey(id string) []byte      { return []byte("appv23:group:" + id) }
func appV23MigrationKey(id string) []byte  { return []byte("appv23:migration:" + id) }
func appV23LegacyReadKey(id string) []byte { return []byte("appv23:legacy_read:" + id) }
func appV23LegacyFederationKey(id string) []byte {
	return []byte("appv23:legacy_federation:" + id)
}
func appV23MigrationStateKey() []byte { return []byte("appv23:migration_state") }
func appV23GenesisActivationKey() []byte {
	return []byte(consensuskeys.AppV23GenesisActivation)
}
func appV23ProjectedAgentKey(id string) []byte {
	return []byte("appv23:agent:" + id)
}
func appV23LegacyAdminAuditKey(id string) []byte {
	return []byte("appv23:legacy_admin:" + id)
}
func appV23ElevationNonceKey(generation uint64, adminID, nonce string) []byte {
	return []byte(fmt.Sprintf("appv23:elevation_nonce:%020d:%s:%s", generation, adminID, nonce))
}
func appV23MemberGroupPrefix(id string) []byte {
	return []byte("appv23:member_group:" + id + ":")
}
func appV23MemberGroupKey(id, groupID string) []byte {
	return []byte("appv23:member_group:" + id + ":" + groupID)
}

// validateAppV23GrantKeyComponentsTxn activates only once the app-v23 Root
// record is consensus-visible. It gives direct store callers the same
// delimiter-safety boundary as the app-v23 ABCI handlers without changing
// historical replay before activation.
func validateAppV23GrantKeyComponentsTxn(
	txn *badger.Txn,
	domain, agentID string,
) error {
	if _, err := txn.Get(appV23RootKey()); errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	if err := ValidateAppV23DomainName(domain); err != nil {
		return fmt.Errorf("invalid app-v23 grant domain: %w", err)
	}
	return validateCanonicalAgentID("app-v23 grant agent", agentID)
}

func appV23Marshal(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal app-v23 state: %w", err)
	}
	return data, nil
}

func appV23ReadJSON(txn *badger.Txn, key []byte, target any) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	return item.Value(func(value []byte) error {
		if err := json.Unmarshal(value, target); err != nil {
			return fmt.Errorf("decode %s: %w", key, err)
		}
		return nil
	})
}

type appV23MigrationPreparation struct {
	Scope          string `json:"scope"`
	Height         int64  `json:"height"`
	SchemaDigest   string `json:"schema_digest"`
	PlanDigest     string `json:"plan_digest"`
	ManifestDigest string `json:"manifest_digest"`
	StageDigest    string `json:"stage_digest"`
	StageCount     int    `json:"stage_count"`
}

type appV23MigrationPlan struct {
	root            AppV23RootState
	rootPreexisting bool
	migration       AppV23MigrationState
	stage           map[string][]byte
	activation      map[string][]byte
	preparation     appV23MigrationPreparation
}

func appV23StageDigest(stage map[string][]byte) (string, error) {
	keys := make([]string, 0, len(stage))
	for target := range stage {
		if target == "" {
			return "", errors.New("app-v23 migration stage contains an empty target")
		}
		keys = append(keys, target)
	}
	sort.Strings(keys)
	digest := sha256.New()
	var length [8]byte
	for _, target := range keys {
		value := stage[target]
		binary.BigEndian.PutUint64(length[:], uint64(len(target)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write([]byte(target))
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = digest.Write(length[:])
		_, _ = digest.Write(value)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func appV23PlanDigest(root AppV23RootState, migration AppV23MigrationState) (string, error) {
	value, err := json.Marshal(struct {
		Root      AppV23RootState      `json:"root"`
		Migration AppV23MigrationState `json:"migration"`
	}{Root: root, Migration: migration})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:]), nil
}

func appV23MigrationActiveTxn(txn *badger.Txn) (bool, error) {
	_, err := txn.Get(appV23MigrationStateKey())
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	default:
		return false, err
	}
}

// appV23ReadStageValue reads the promoted migration stage. A scoped
// FinalizeBlock transaction was opened before EnsureAppV23Root durably built
// the stage, so its Badger snapshot cannot see those keys; in that one case
// read the immutable external stage snapshot. The activation marker remains in
// the scoped transaction, so an uncommitted/partial stage is never visible.
func (s *BadgerStore) appV23ReadStageValue(txn *badger.Txn, target []byte) ([]byte, error) {
	stageKey := consensuskeys.AppV23MigrationStageKey(target)
	if s.txn == nil {
		item, err := txn.Get(stageKey)
		if err != nil {
			return nil, err
		}
		return item.ValueCopy(nil)
	}
	var value []byte
	err := s.db.View(func(external *badger.Txn) error {
		item, err := external.Get(stageKey)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

func (s *BadgerStore) appV23ReadEffectiveValueTxn(txn *badger.Txn, target []byte) ([]byte, error) {
	if item, err := txn.Get(target); err == nil {
		return item.ValueCopy(nil)
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, err
	}
	active, err := appV23MigrationActiveTxn(txn)
	if err != nil || !active {
		return nil, errOrKeyNotFound(err)
	}
	return s.appV23ReadStageValue(txn, target)
}

func errOrKeyNotFound(err error) error {
	if err != nil {
		return err
	}
	return badger.ErrKeyNotFound
}

func (s *BadgerStore) appV23ReadEffectiveJSONTxn(txn *badger.Txn, target []byte, value any) error {
	data, err := s.appV23ReadEffectiveValueTxn(txn, target)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode effective %s: %w", target, err)
	}
	return nil
}

// appV23MigrationAgentsTxn is the fail-closed roster enumerator for migration,
// replay validation, and state sync. The historical ListRegisteredAgents API
// deliberately remains best-effort for pre-v23 callers, but an authority
// migration may never silently omit malformed rows or detach an agent value
// from the canonical identity in its key.
func (s *BadgerStore) appV23MigrationAgentsTxn(txn *badger.Txn) ([]OnChainAgent, error) {
	prefix := []byte("agent:")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	agents := make([]OnChainAgent, 0)
	seen := make(map[string]struct{})
	rawCount := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		rawCount++
		key := it.Item().KeyCopy(nil)
		agentID, ok := canonicalAgentIDFromPrefixedKey(key, prefix)
		if !ok {
			return nil, fmt.Errorf("app-v23 migration agent key %q is not canonical", key)
		}
		rawValue, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		var raw OnChainAgent
		if err := json.Unmarshal(rawValue, &raw); err != nil {
			return nil, fmt.Errorf("decode app-v23 migration agent %s: %w", agentID, err)
		}
		if raw.AgentID != agentID {
			return nil, fmt.Errorf(
				"app-v23 migration agent key/value identity mismatch: key %s value %s",
				agentID, raw.AgentID,
			)
		}

		effective := raw
		projected, projectedErr := s.appV23ReadEffectiveValueTxn(
			txn, appV23ProjectedAgentKey(agentID),
		)
		if projectedErr == nil {
			if err := json.Unmarshal(projected, &effective); err != nil {
				if _, directErr := txn.Get(appV23ProjectedAgentKey(agentID)); errors.Is(
					directErr, badger.ErrKeyNotFound,
				) {
					return nil, fmt.Errorf(
						"app-v23 promoted migration stage digest mismatch: decode projected agent %s: %w",
						agentID, err,
					)
				}
				return nil, fmt.Errorf(
					"decode effective app-v23 migration agent %s: %w", agentID, err,
				)
			}
			if effective.AgentID != agentID {
				return nil, fmt.Errorf(
					"effective app-v23 migration agent key/value identity mismatch: key %s value %s",
					agentID, effective.AgentID,
				)
			}
		} else if !errors.Is(projectedErr, badger.ErrKeyNotFound) {
			return nil, projectedErr
		}
		if _, duplicate := seen[effective.AgentID]; duplicate {
			return nil, fmt.Errorf(
				"app-v23 migration roster contains duplicate effective agent %s",
				effective.AgentID,
			)
		}
		seen[effective.AgentID] = struct{}{}
		agents = append(agents, effective)
	}
	if len(agents) != rawCount || len(seen) != rawCount {
		return nil, errors.New("app-v23 migration roster cardinality mismatch")
	}
	sort.Slice(agents, func(i, j int) bool {
		return agents[i].AgentID < agents[j].AgentID
	})
	return agents, nil
}

func (s *BadgerStore) listAppV23MigrationAgents() ([]OnChainAgent, error) {
	var agents []OnChainAgent
	err := s.view(func(txn *badger.Txn) error {
		var err error
		agents, err = s.appV23MigrationAgentsTxn(txn)
		return err
	})
	return agents, err
}

// appV23EffectivePrefixTxn merges promoted staged logical records with later
// ordinary-key overrides. Stage rows are immutable; a post-activation mutation
// wins by writing the logical target key.
func (s *BadgerStore) appV23EffectivePrefixTxn(
	txn *badger.Txn,
	prefix []byte,
	fn func(key, value []byte) error,
) error {
	values := make(map[string][]byte)
	active, err := appV23MigrationActiveTxn(txn)
	if err != nil {
		return err
	}
	if active {
		readStage := func(stageTxn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = consensuskeys.AppV23MigrationStagePrefix
			opts.PrefetchValues = false
			it := stageTxn.NewIterator(opts)
			defer it.Close()
			for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
				target, ok := consensuskeys.AppV23MigrationStageTarget(it.Item().Key())
				if !ok || !strings.HasPrefix(string(target), string(prefix)) {
					continue
				}
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}
				values[string(target)] = value
			}
			return nil
		}
		if s.txn == nil {
			if err := readStage(txn); err != nil {
				return err
			}
		} else if err := s.db.View(readStage); err != nil {
			return err
		}
	}
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			it.Close()
			return err
		}
		values[string(it.Item().KeyCopy(nil))] = value
	}
	it.Close()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if err := fn([]byte(key), values[key]); err != nil {
			return err
		}
	}
	return nil
}

func (s *BadgerStore) appV23PromotedStageMapTxn(txn *badger.Txn) (map[string][]byte, error) {
	active, err := appV23MigrationActiveTxn(txn)
	if err != nil {
		return nil, err
	}
	if !active {
		return nil, badger.ErrKeyNotFound
	}
	stage := make(map[string][]byte)
	read := func(stageTxn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = consensuskeys.AppV23MigrationStagePrefix
		opts.PrefetchValues = false
		it := stageTxn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			target, ok := consensuskeys.AppV23MigrationStageTarget(it.Item().Key())
			if !ok {
				return errors.New("malformed app-v23 migration stage key")
			}
			value, copyErr := it.Item().ValueCopy(nil)
			if copyErr != nil {
				return copyErr
			}
			stage[string(target)] = value
		}
		return nil
	}
	if s.txn == nil {
		err = read(txn)
	} else {
		err = s.db.View(read)
	}
	return stage, err
}

func appV23DomainIsSharedTxn(txn *badger.Txn, name string) (bool, error) {
	if IsSharedDomainName(name) {
		return true, nil
	}
	_, err := txn.Get(stateKey("shared_domain:" + name))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	default:
		return false, err
	}
}

// IsAppV23SharedDomain returns the effective static-or-consensus shared-domain
// classification used by app-v23 advisory REST/dashboard policy checks.
func (s *BadgerStore) IsAppV23SharedDomain(name string) (bool, error) {
	var shared bool
	err := s.view(func(txn *badger.Txn) error {
		var err error
		shared, err = appV23DomainIsSharedTxn(txn, name)
		return err
	})
	return shared, err
}

func (s *BadgerStore) appV23ValidateHomeDomainReleaseTxn(
	txn *badger.Txn,
	domain, currentOwner, newOwner string,
	makeShared bool,
) error {
	if currentOwner == "" || (currentOwner == newOwner && !makeShared) {
		return nil
	}
	var root AppV23RootState
	if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
		return err
	}
	// Root is the recovery identity and does not depend on a home domain.
	// This is the intentional Root -> first-party Companion repair path.
	if currentOwner == root.PrincipalID {
		return nil
	}
	var enrollment AppV23LocalEnrollment
	err := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(currentOwner), &enrollment)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if enrollment.Active && enrollment.Profile != AppV23ProfileReadOnly &&
		enrollment.HomeDomain == domain {
		return errors.New("cannot transfer or share an active local agent's required home domain")
	}
	return nil
}

// TransferDomainAppV23 preserves the active-home invariant while retaining
// DomainReassign for historical replay. Dynamic shared-domain promotion is
// included in the same check and write transaction, so a valid app-v23 tx can
// never commit a state that restart/state-sync validation rejects.
func (s *BadgerStore) TransferDomainAppV23(
	name, newOwnerID, parentDomain string,
	height int64,
	makeShared bool,
) error {
	if err := ValidateAppV23DomainName(name); err != nil {
		return fmt.Errorf("invalid app-v23 transfer domain: %w", err)
	}
	if parentDomain != "" {
		if err := ValidateAppV23DomainName(parentDomain); err != nil {
			return fmt.Errorf("invalid app-v23 transfer parent domain: %w", err)
		}
	}
	if err := validateCanonicalAgentID("new_owner_id", newOwnerID); err != nil {
		return err
	}
	return s.withDomainOwnershipMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(name))
			if err != nil {
				return err
			}
			var currentOwner string
			var decodeErr error
			currentOwner, _, decodeErr = decodeString(value, 0)
			if decodeErr != nil {
				return decodeErr
			}
			if err := s.appV23ValidateHomeDomainReleaseTxn(
				txn, name, currentOwner, newOwnerID, makeShared,
			); err != nil {
				return err
			}
			if err := s.txnSet(
				txn, domainKey(name),
				appV23EncodeDomainWithParent(newOwnerID, parentDomain, height),
			); err != nil {
				return err
			}
			if makeShared {
				return s.txnSet(txn, stateKey("shared_domain:"+name), []byte{1})
			}
			return nil
		})
	})
}

func validateAppV23ElevationNonce(nonce string) error {
	if len(nonce) < 16 || len(nonce) > 128 {
		return errors.New("app-v23 elevation nonce length must be 16..128")
	}
	for i := range nonce {
		c := nonce[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') &&
			(c < '0' || c > '9') && c != '-' && c != '_' {
			return errors.New("app-v23 elevation nonce is not canonical")
		}
	}
	return nil
}

func (s *BadgerStore) consumeAppV23ElevationTxn(txn *badger.Txn, use *AppV23ElevationUse, height int64) error {
	if use == nil {
		return nil
	}
	if err := validateCanonicalAgentID("elevation admin_id", use.AdminID); err != nil {
		return err
	}
	if err := validateAppV23ElevationNonce(use.Nonce); err != nil {
		return err
	}
	if use.ValidFromHeight > height || use.ValidUntilHeight < height ||
		use.ValidUntilHeight < use.ValidFromHeight ||
		use.ValidUntilHeight-use.ValidFromHeight > AppV23MaxElevationWindow {
		return errors.New("app-v23 elevation proof is outside its consensus height window")
	}
	var root AppV23RootState
	if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
		return err
	}
	if root.Generation != use.RootGeneration {
		return errors.New("app-v23 elevation root generation mismatch")
	}
	key := appV23ElevationNonceKey(use.RootGeneration, use.AdminID, use.Nonce)
	if _, err := txn.Get(key); err == nil {
		return errors.New("app-v23 elevation proof replay")
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	return s.txnSet(txn, key, []byte{1})
}

// ConsumeAppV23Elevation commits a replay marker for legacy control-plane
// handlers whose historical mutation primitives cannot share a Badger
// transaction with app-v23 state. New app-v23 mutations consume inside their
// own atomic write transaction instead.
func (s *BadgerStore) ConsumeAppV23Elevation(use *AppV23ElevationUse, height int64) error {
	if use == nil {
		return errors.New("missing app-v23 elevation proof")
	}
	return s.update(func(txn *badger.Txn) error {
		return s.consumeAppV23ElevationTxn(txn, use, height)
	})
}

func ValidAppV23Role(role string) bool {
	return role == AppV23RoleMember || role == AppV23RoleManager || role == AppV23RoleAdmin
}

func ValidAppV23Profile(profile string) bool {
	return profile == AppV23ProfileRoot || profile == AppV23ProfileStandard ||
		profile == AppV23ProfileCompanion || profile == AppV23ProfileReadOnly ||
		profile == AppV23ProfileLegacyRestricted
}

func AppV23ProfileAllowsRole(profile, role string) bool {
	switch profile {
	case AppV23ProfileRoot:
		return role == AppV23RoleAdmin
	case AppV23ProfileStandard:
		return ValidAppV23Role(role)
	case AppV23ProfileCompanion:
		return role == AppV23RoleMember
	case AppV23ProfileReadOnly:
		return role == AppV23RoleMember
	case AppV23ProfileLegacyRestricted:
		return role == AppV23RoleMember
	default:
		return false
	}
}

// AppV23AllowsMigratedDomainless reports the narrow fail-closed compatibility
// case where migration may keep an active principal without inventing a home
// domain it was forbidden to claim under app-v22. Fresh enrollment still
// requires a home; this only lets restore and authorization preserve an exact
// deny-claim mask.
func AppV23AllowsMigratedDomainless(
	profile string,
	capabilities AgentCapabilities,
) bool {
	return (profile == AppV23ProfileLegacyRestricted ||
		profile == AppV23ProfileCompanion) &&
		capabilities.Has(AgentCapabilityDenyDomainClaim)
}

// ValidateAppV23Policy rejects internally contradictory role/profile/capability
// combinations. It is used by every mutation, authorization and restore path;
// no caller may infer compatibility from the profile name alone.
func ValidateAppV23Policy(role, profile string, capabilities AgentCapabilities, clearance uint8) error {
	if !ValidAppV23Role(role) || !ValidAppV23Profile(profile) ||
		!capabilities.Valid() || clearance > 4 {
		return errors.New("invalid app-v23 policy fields")
	}
	switch profile {
	case AppV23ProfileRoot:
		if role != AppV23RoleAdmin || clearance != 4 || capabilities != 0 {
			return errors.New("root profile requires admin role, top-secret clearance, and capability mask 0")
		}
	case AppV23ProfileCompanion:
		if role != AppV23RoleMember ||
			capabilities&^AgentCapabilityDenyFederatedPipe != 15 {
			return errors.New("companion profile requires member role and capability mask 15, optionally with federated messaging disabled")
		}
	case AppV23ProfileReadOnly:
		if role != AppV23RoleMember ||
			capabilities&^AgentCapabilityDenyFederatedPipe != AgentCapabilityReadAllDomains {
			return errors.New("read-only profile requires member role and exactly the read-all capability, optionally with federated messaging disabled")
		}
	case AppV23ProfileStandard:
		baseCapabilities := capabilities &^ AgentCapabilityDenyFederatedPipe
		switch role {
		case AppV23RoleMember:
			if baseCapabilities != 0 {
				return errors.New("standard member policy allows only the optional federated messaging disable bit")
			}
		case AppV23RoleManager:
			if baseCapabilities != 0 {
				return errors.New("standard manager policy allows only the optional federated messaging disable bit")
			}
		case AppV23RoleAdmin:
			if baseCapabilities != AgentCapabilityReadAllDomains || clearance != 4 {
				return errors.New("standard admin policy requires clearance 4 and read-all, optionally with federated messaging disabled")
			}
		}
	case AppV23ProfileLegacyRestricted:
		if role != AppV23RoleMember {
			return errors.New("legacy-restricted profile requires member role")
		}
	}
	return nil
}

// ValidateAppV23EnrollmentPolicy keeps enrollment state on the same canonical
// role/profile/capability matrix whether it is active or quarantined. The
// immutable migration disposition, not a live capability mask, records why a
// legacy principal entered pending review.
func ValidateAppV23EnrollmentPolicy(
	role, profile string,
	capabilities AgentCapabilities,
	clearance uint8,
	_ bool,
) error {
	return ValidateAppV23Policy(role, profile, capabilities, clearance)
}

func (s *BadgerStore) GetAppV23Root() (*AppV23RootState, error) {
	var root AppV23RootState
	err := s.view(func(txn *badger.Txn) error {
		return appV23ReadJSON(txn, appV23RootKey(), &root)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &root, nil
}

// IsAppV23RootCredential reports whether id has ever represented CEREBRUM
// Root. Markers are permanent: a credential retired by tx39 must never later
// reappear as an ordinary agent or be selected by another tx39 handover.
func (s *BadgerStore) IsAppV23RootCredential(id string) (bool, error) {
	if !isCanonicalAgentID(id) {
		return false, nil
	}
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(appV23RootCredentialKey(id))
		return err
	})
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	default:
		return false, err
	}
}

// GetAppV23RootCredentialGeneration returns the immutable generation assigned
// to one exact Root credential. It lets off-consensus signed capability rows
// preserve historical Root provenance without treating that retired key as a
// current request credential.
func (s *BadgerStore) GetAppV23RootCredentialGeneration(
	id string,
) (uint64, bool, error) {
	if !isCanonicalAgentID(id) {
		return 0, false, nil
	}
	var generation uint64
	err := s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(appV23RootCredentialKey(id))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			if len(value) != 8 {
				return errors.New("invalid app-v23 Root credential generation marker")
			}
			generation = binary.BigEndian.Uint64(value)
			if generation == 0 {
				return errors.New("invalid zero app-v23 Root credential generation")
			}
			return nil
		})
	})
	switch {
	case err == nil:
		return generation, true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return 0, false, nil
	default:
		return 0, false, err
	}
}

func (s *BadgerStore) GetAppV23MigrationState() (*AppV23MigrationState, error) {
	var state AppV23MigrationState
	err := s.view(func(txn *badger.Txn) error {
		return appV23ReadJSON(txn, appV23MigrationStateKey(), &state)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &state, nil
}

// GetAppV23MigrationDisposition returns the immutable, manifest-covered
// activation disposition for one local principal. It is the authoritative
// provenance for narrowly-scoped migration repair; current policy fields alone
// must never be treated as proof that an operator did not already review an
// agent.
func (s *BadgerStore) GetAppV23MigrationDisposition(agentID string) (*AppV23MigrationDisposition, error) {
	if err := validateCanonicalAgentID("migration agent", agentID); err != nil {
		return nil, err
	}
	var disposition AppV23MigrationDisposition
	err := s.view(func(txn *badger.Txn) error {
		return s.appV23ReadEffectiveJSONTxn(txn, appV23MigrationKey(agentID), &disposition)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if disposition.AgentID != agentID {
		return nil, errors.New("app-v23 migration disposition key mismatch")
	}
	return &disposition, nil
}

// AppV23AllowsGrandfatheredSharedWrite preserves the exact app-v22 shared
// write authority of an unchanged migrated Member. Before app-v23, every
// active non-observer without DenySharedDomainWrite could write a shared
// domain without an explicit grant. Fresh app-v23 principals use the stricter
// named-policy/grant model; this compatibility path is therefore bound to the
// immutable migration record and the initial policy revisions. Any explicit
// review or policy mutation increments a revision and permanently leaves this
// path.
func (s *BadgerStore) AppV23AllowsGrandfatheredSharedWrite(agentID string) (bool, error) {
	if err := validateCanonicalAgentID("shared-write agent", agentID); err != nil {
		return false, err
	}
	allowed := false
	err := s.view(func(txn *badger.Txn) error {
		var enrollment AppV23LocalEnrollment
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23EnrollmentKey(agentID), &enrollment,
		); errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		var role AppV23RoleState
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23RoleKey(agentID), &role,
		); errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		var disposition AppV23MigrationDisposition
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23MigrationKey(agentID), &disposition,
		); errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		if disposition.AgentID != agentID ||
			len(disposition.LegacyPolicyDigest) != sha256.Size*2 {
			return nil
		}
		decodedDigest, err := hex.DecodeString(disposition.LegacyPolicyDigest)
		if err != nil ||
			hex.EncodeToString(decodedDigest) != disposition.LegacyPolicyDigest {
			return nil
		}
		if !enrollment.Active || !disposition.Active ||
			enrollment.Revision != 1 || role.Revision != 1 ||
			enrollment.AgentID != agentID || role.AgentID != agentID ||
			role.Role != AppV23RoleMember ||
			enrollment.Capabilities.Has(AgentCapabilityDenySharedDomainWrite) ||
			disposition.Profile != enrollment.Profile ||
			disposition.HomeDomain != enrollment.HomeDomain {
			return nil
		}
		switch disposition.Disposition {
		case "member":
			allowed = enrollment.Profile == AppV23ProfileStandard
		case "legacy_restricted", "legacy_admin_review":
			allowed = enrollment.Profile == AppV23ProfileLegacyRestricted
		}
		return nil
	})
	return allowed, err
}

// AppV23AllowsGrandfatheredSharedDomainWrite applies the legacy shared-write
// compatibility rule to one exact domain. A non-static domain promoted to
// shared solely because app-v25 found multiple historical writers is scoped by
// its recovered continuity group instead: treating that promotion like an
// ordinary H-1 shared domain would grant every unchanged mask-0 Member write
// authority even when it never wrote the domain and is not in the group.
//
// Compile-time shared names retain their historical semantics. A dynamically
// shared domain with only one continuity writer was necessarily already shared
// before recovery, so it also retains the legacy rule. Exact continuity
// writers are admitted earlier by AppV25AllowsHistoricalDomainWrite; current
// Admin and explicit grants remain separate additive paths.
func (s *BadgerStore) AppV23AllowsGrandfatheredSharedDomainWrite(
	agentID, domain string,
) (bool, error) {
	allowed, err := s.AppV23AllowsGrandfatheredSharedWrite(agentID)
	if err != nil || !allowed {
		return allowed, err
	}
	record, err := s.GetAppV25DomainContinuity(domain)
	if err != nil || record == nil {
		return allowed, err
	}
	if IsSharedDomainName(domain) || len(record.Writers) <= 1 {
		return true, nil
	}
	return false, nil
}

// AppV23AllowsLegacyForeignModify preserves the narrower app-v22 meaning of
// DenyForeignDomainWrite for an unchanged migration-only policy. Bit 8 blocked
// memory creation despite a level-2 grant; it did not revoke an independently
// granted level-3 lifecycle verb. Fresh app-v23 policies cannot select
// Legacy-restricted, and the first explicit policy review increments a revision
// and permanently leaves this compatibility path.
func (s *BadgerStore) AppV23AllowsLegacyForeignModify(agentID string) (bool, error) {
	if err := validateCanonicalAgentID("legacy foreign-modify agent", agentID); err != nil {
		return false, err
	}
	allowed := false
	err := s.view(func(txn *badger.Txn) error {
		var enrollment AppV23LocalEnrollment
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23EnrollmentKey(agentID), &enrollment,
		); errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		var role AppV23RoleState
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23RoleKey(agentID), &role,
		); errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		var disposition AppV23MigrationDisposition
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23MigrationKey(agentID), &disposition,
		); errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		if enrollment.Active && disposition.Active &&
			enrollment.Revision == 1 && role.Revision == 1 &&
			enrollment.AgentID == agentID && role.AgentID == agentID &&
			disposition.AgentID == agentID &&
			role.Role == AppV23RoleMember &&
			enrollment.Profile == AppV23ProfileLegacyRestricted &&
			disposition.Profile == enrollment.Profile &&
			disposition.HomeDomain == enrollment.HomeDomain &&
			len(disposition.LegacyPolicyDigest) == sha256.Size*2 &&
			enrollment.Capabilities.Has(AgentCapabilityDenyForeignDomainWrite) {
			if decoded, err := hex.DecodeString(disposition.LegacyPolicyDigest); err != nil ||
				hex.EncodeToString(decoded) != disposition.LegacyPolicyDigest {
				return nil
			}
			switch disposition.Disposition {
			case "legacy_restricted", "legacy_admin_review":
				allowed = true
			}
		}
		return nil
	})
	return allowed, err
}

type appV23LegacyReadContext struct {
	enrollment  AppV23LocalEnrollment
	role        AppV23RoleState
	disposition AppV23MigrationDisposition
	baseline    AppV23LegacyReadBaseline
	agent       OnChainAgent
}

func (s *BadgerStore) appV23LegacyReadContextTxn(
	txn *badger.Txn, agentID string,
) (appV23LegacyReadContext, bool, error) {
	var context appV23LegacyReadContext
	read := func(key []byte, target any) error {
		return s.appV23ReadEffectiveJSONTxn(txn, key, target)
	}
	if err := read(appV23EnrollmentKey(agentID), &context.enrollment); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return context, false, nil
		}
		return context, false, err
	}
	if err := read(appV23RoleKey(agentID), &context.role); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return context, false, nil
		}
		return context, false, err
	}
	if err := read(appV23MigrationKey(agentID), &context.disposition); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return context, false, nil
		}
		return context, false, err
	}
	if err := read(appV23LegacyReadKey(agentID), &context.baseline); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return context, false, nil
		}
		return context, false, err
	}
	if err := read(appV23ProjectedAgentKey(agentID), &context.agent); err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return context, false, err
		}
		if err := appV23ReadJSON(txn, agentOnChainKey(agentID), &context.agent); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return context, false, nil
			}
			return context, false, err
		}
	}

	if !context.enrollment.Active || !context.disposition.Active ||
		context.enrollment.Revision != 1 || context.role.Revision != 1 ||
		context.enrollment.AgentID != agentID ||
		context.role.AgentID != agentID ||
		context.disposition.AgentID != agentID ||
		context.baseline.AgentID != agentID ||
		context.agent.AgentID != agentID ||
		context.baseline.LegacyPolicyDigest == "" ||
		context.baseline.LegacyPolicyDigest != context.disposition.LegacyPolicyDigest ||
		context.disposition.Profile != context.enrollment.Profile ||
		context.disposition.HomeDomain != context.enrollment.HomeDomain {
		return context, false, nil
	}
	switch context.disposition.Disposition {
	case "member", "legacy_restricted", "legacy_admin_review", "observer_read_only":
		return context, true, nil
	default:
		return context, false, nil
	}
}

func appV23LegacyDomainAccessAllows(policy, domain string) bool {
	var access []struct {
		Domain string `json:"domain"`
		Read   bool   `json:"read"`
		Modify bool   `json:"modify"`
	}
	if json.Unmarshal([]byte(policy), &access) != nil || len(access) == 0 {
		return false
	}
	for _, entry := range access {
		if organizationFederationDomainAllowed([]string{entry.Domain}, domain) {
			return entry.Read || entry.Modify
		}
	}
	return false
}

func appV23LegacyDepartmentAllowed(
	memberships map[string]struct{},
	allowed []string,
) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if candidate == "*" {
			return true
		}
		if _, exists := memberships[candidate]; exists {
			return true
		}
	}
	return false
}

func (s *BadgerStore) appV23LegacyOwnerBaselineTxn(
	txn *badger.Txn, ownerID string,
) (AppV23LegacyReadBaseline, bool, error) {
	var baseline AppV23LegacyReadBaseline
	if err := s.appV23ReadEffectiveJSONTxn(
		txn, appV23LegacyReadKey(ownerID), &baseline,
	); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return baseline, false, nil
		}
		return baseline, false, err
	}
	var disposition AppV23MigrationDisposition
	if err := s.appV23ReadEffectiveJSONTxn(
		txn, appV23MigrationKey(ownerID), &disposition,
	); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return baseline, false, nil
		}
		return baseline, false, err
	}
	if baseline.AgentID != ownerID || disposition.AgentID != ownerID ||
		baseline.LegacyPolicyDigest == "" ||
		baseline.LegacyPolicyDigest != disposition.LegacyPolicyDigest {
		return baseline, false, nil
	}
	return baseline, true, nil
}

func (s *BadgerStore) appV23LegacyOrgReadAllowed(
	caller appV23LegacyReadContext,
	domain string,
	classification uint8,
	at time.Time,
) (bool, error) {
	ownerID, _, err := s.ResolveAppV23OwningAncestor(domain)
	if err != nil || ownerID == "" {
		return false, err
	}
	var ownerBaseline AppV23LegacyReadBaseline
	var ownerBaselineFound bool
	federations := make([]AppV23LegacyFederationBaseline, 0)
	federationScopeEntries := 0
	err = s.view(func(txn *badger.Txn) error {
		var readErr error
		ownerBaseline, ownerBaselineFound, readErr =
			s.appV23LegacyOwnerBaselineTxn(txn, ownerID)
		if readErr != nil || !ownerBaselineFound {
			return readErr
		}
		prefix := []byte("appv23:legacy_federation:")
		return s.appV23EffectivePrefixTxn(txn, prefix, func(_, value []byte) error {
			if len(federations) == AppV23MaxLegacyFederations {
				return errors.New("legacy federation baseline exceeds bound")
			}
			var federation AppV23LegacyFederationBaseline
			if unmarshalErr := json.Unmarshal(value, &federation); unmarshalErr != nil {
				return unmarshalErr
			}
			federationScopeEntries +=
				len(federation.AllowedDomains) + len(federation.AllowedDepts)
			if federationScopeEntries > AppV23MaxLegacyMembershipLinks {
				return errors.New("legacy federation baseline scope exceeds bound")
			}
			federations = append(federations, federation)
			return nil
		})
	})
	if err != nil || !ownerBaselineFound {
		return false, err
	}

	baselineAllowed := false
	ownerOrgs := make(map[string]struct{}, len(ownerBaseline.OrgMemberships))
	for _, membership := range ownerBaseline.OrgMemberships {
		ownerOrgs[membership.OrgID] = struct{}{}
	}
	callerDepts := make(map[string]map[string]struct{})
	for _, membership := range caller.baseline.DeptMemberships {
		depts := callerDepts[membership.OrgID]
		if depts == nil {
			depts = make(map[string]struct{})
			callerDepts[membership.OrgID] = depts
		}
		depts[membership.DeptID] = struct{}{}
	}
	federationsByOrg := make(
		map[string][]AppV23LegacyFederationBaseline,
		len(federations)*2,
	)
	for _, federation := range federations {
		federationsByOrg[federation.ProposerOrgID] = append(
			federationsByOrg[federation.ProposerOrgID], federation,
		)
		federationsByOrg[federation.TargetOrgID] = append(
			federationsByOrg[federation.TargetOrgID], federation,
		)
	}
	for _, callerOrg := range caller.baseline.OrgMemberships {
		if callerOrg.Clearance < classification {
			continue
		}
		if _, sameOrg := ownerOrgs[callerOrg.OrgID]; sameOrg {
			baselineAllowed = true
			break
		}
		for _, federation := range federationsByOrg[callerOrg.OrgID] {
			otherOrg := ""
			switch callerOrg.OrgID {
			case federation.ProposerOrgID:
				otherOrg = federation.TargetOrgID
			case federation.TargetOrgID:
				otherOrg = federation.ProposerOrgID
			default:
				continue
			}
			if _, ownsViaOrg := ownerOrgs[otherOrg]; !ownsViaOrg ||
				federation.Status != "active" ||
				(federation.ExpiresAt > 0 && at.Unix() >= federation.ExpiresAt) ||
				classification > federation.MaxClearance ||
				!organizationFederationDomainAllowed(federation.AllowedDomains, domain) ||
				!appV23LegacyDepartmentAllowed(
					callerDepts[callerOrg.OrgID],
					federation.AllowedDepts,
				) {
				continue
			}
			baselineAllowed = true
			break
		}
		if baselineAllowed {
			break
		}
	}
	if !baselineAllowed {
		return false, nil
	}
	// The immutable H-1 graph is only a ceiling. Removing a current membership
	// or federation immediately removes compatibility authority.
	return s.HasAccessMultiOrgWithFederationPolicy(
		domain, caller.baseline.AgentID, classification, at, true, true,
	)
}

// AppV23LegacyReadCompatibility evaluates the immutable local-only H-1 read
// envelope. It can preserve authority for unchanged migrated principals, but
// is never used for federated callers and never revives app-v22's empty-policy
// allow-all default.
func (s *BadgerStore) AppV23LegacyReadCompatibility(
	agentID, domain string,
	classification uint8,
	at time.Time,
) (AppV23LegacyReadDecision, error) {
	var decision AppV23LegacyReadDecision
	if err := validateCanonicalAgentID("legacy read agent", agentID); err != nil {
		return decision, err
	}
	if domain == "" || classification > uint8(ClearanceTopSecret) {
		return decision, nil
	}
	// Current direct authority over an exact app-v25-repaired domain is
	// independent of the obsolete H-1 compatibility projection. Evaluate it
	// before decoding that projection so a malformed legacy row cannot make a
	// governed recovered domain write-only. The helper deliberately excludes
	// ordinary grants and ordinary Access Groups, which remain bounded by a
	// valid frozen explicit-domain ceiling.
	credentialID := agentID
	root, rootErr := s.GetAppV23Root()
	if rootErr != nil {
		return decision, rootErr
	}
	if root != nil && agentID == root.PrincipalID {
		credentialID = root.CredentialID
	}
	recovered, recoveredErr := s.AuthorizeAppV25RecoveredDirectRead(credentialID, domain)
	if recoveredErr != nil {
		return decision, recoveredErr
	}
	if recovered {
		enrollment, enrollmentErr := s.GetAppV23Enrollment(agentID)
		if enrollmentErr != nil {
			return decision, enrollmentErr
		}
		if enrollment != nil && enrollment.Active {
			decision.Eligible = true
			decision.Allowed = classification <= enrollment.Clearance
			return decision, nil
		}
	}
	var context appV23LegacyReadContext
	var eligible bool
	err := s.view(func(txn *badger.Txn) error {
		var readErr error
		context, eligible, readErr = s.appV23LegacyReadContextTxn(txn, agentID)
		return readErr
	})
	if err != nil || !eligible {
		return decision, err
	}
	decision.Eligible = true
	// Migration may allocate an owned home domain that did not exist in the
	// agent's immutable app-v22 DomainAccess allowlist. That allowlist remains
	// a ceiling for every other legacy resource, but it must never make the
	// required app-v23 home domain write-only: an active agent that owns its
	// home domain can always read records there up to its current clearance.
	//
	// Keep this exception in the central compatibility decision so explicit
	// domain reads, list/tag reads, semantic recall, tasks, and MCP inception
	// all observe the same write-implies-read invariant.
	if context.enrollment.HomeDomain != "" {
		owner, ownedDomain, ownerErr := s.ResolveAppV23OwningAncestor(domain)
		if ownerErr != nil {
			return decision, ownerErr
		}
		shared, sharedErr := s.IsAppV23SharedDomain(domain)
		if sharedErr != nil {
			return decision, sharedErr
		}
		if !shared && owner == agentID &&
			ownedDomain == context.enrollment.HomeDomain &&
			classification <= context.enrollment.Clearance {
			decision.Allowed = true
			return decision, nil
		}
	}
	if context.enrollment.Capabilities.Has(AgentCapabilityReadAllDomains) {
		decision.Allowed = classification <= context.enrollment.Clearance
		return decision, nil
	}
	if context.baseline.DomainAccess != "" {
		decision.ExplicitDomainRestriction = true
		if context.agent.DomainAccess != context.baseline.DomainAccess {
			return decision, nil
		}
		domainAllowed := appV23LegacyDomainAccessAllows(
			context.baseline.DomainAccess, domain,
		)
		if !domainAllowed {
			return decision, nil
		}
		if classification <= context.enrollment.Clearance {
			decision.Allowed = true
			return decision, nil
		}
		// app-v22's explicit DomainAccess was the domain-scope gate, not the
		// classification ceiling. A caller could pair that exact allowlist with
		// a higher live organization/federation membership clearance. Preserve
		// only the immutable H-1 ceiling intersected with the still-live graph.
		decision.Allowed, err = s.appV23LegacyOrgReadAllowed(
			context, domain, classification, at,
		)
		return decision, err
	}
	decision.Allowed, err = s.appV23LegacyOrgReadAllowed(
		context, domain, classification, at,
	)
	return decision, err
}

// AppV23LegacyVisibleAgents returns the unchanged explicit app-v22 submitter
// filter for an eligible migration-only compatibility principal.
func (s *BadgerStore) AppV23LegacyVisibleAgents(
	agentID string,
) (string, bool, error) {
	if err := validateCanonicalAgentID("legacy visibility agent", agentID); err != nil {
		return "", false, err
	}
	var context appV23LegacyReadContext
	var eligible bool
	err := s.view(func(txn *badger.Txn) error {
		var readErr error
		context, eligible, readErr = s.appV23LegacyReadContextTxn(txn, agentID)
		return readErr
	})
	if err != nil || !eligible ||
		context.enrollment.Capabilities.Has(AgentCapabilityReadAllDomains) ||
		context.baseline.VisibleAgents == "" {
		return "", false, err
	}
	if context.agent.VisibleAgents != context.baseline.VisibleAgents {
		return "", true, nil
	}
	return context.baseline.VisibleAgents, true, nil
}

// RotateAppV23RootCredential atomically advances the credential generation
// while preserving the immutable root principal. The old credential ceases to
// identify root as soon as this transaction commits.
func (s *BadgerStore) RotateAppV23RootCredential(expectedGeneration uint64, newCredentialID string, height int64) error {
	if err := validateCanonicalAgentID("new root credential", newCredentialID); err != nil {
		return err
	}
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			var root AppV23RootState
			if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
				return err
			}
			if root.Generation != expectedGeneration || newCredentialID == root.CredentialID {
				return ErrAppV23RevisionConflict
			}
			// Root handover is forward-only. Every credential generation remains
			// permanently reserved as a non-agent identity, so a retired key can
			// never be reactivated by rotating back to it.
			if _, err := txn.Get(appV23RootCredentialKey(newCredentialID)); err == nil {
				return errors.New("new root credential was previously used by CEREBRUM root")
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			// A Root credential authenticates the immutable Root principal; it must
			// never also name an ordinary agent principal. Otherwise every action
			// signed by that agent is silently re-attributed to Root while the
			// agent's enrollment and home-domain identity become unreachable.
			//
			// A persisted agent/enrollment is also a hard collision. Perform
			// these reads in the same transaction as the rotation so a concurrent
			// registration cannot race this invariant. The principal exception is
			// unreachable here because its permanent history marker rejects it
			// above; retaining the guard keeps old-state error handling explicit.
			if newCredentialID != root.PrincipalID {
				if _, err := txn.Get(agentOnChainKey(newCredentialID)); err == nil {
					return errors.New("new root credential collides with a registered agent")
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				if _, err := txn.Get(appV23EnrollmentKey(newCredentialID)); err == nil {
					return errors.New("new root credential collides with an enrolled agent")
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			nextGeneration := root.Generation + 1
			historyDigest, err := appV23NextRootHistoryDigest(
				root.HistoryDigest, nextGeneration, newCredentialID,
			)
			if err != nil {
				return err
			}
			root.CredentialID = newCredentialID
			root.Generation = nextGeneration
			root.HistoryDigest = historyDigest
			rootData, err := appV23Marshal(root)
			if err != nil {
				return err
			}
			if setErr := s.txnSet(txn, appV23RootKey(), rootData); setErr != nil {
				return setErr
			}
			if markerErr := s.txnSet(
				txn,
				appV23RootCredentialKey(newCredentialID),
				appV23RootCredentialGenerationValue(root.Generation),
			); markerErr != nil {
				return markerErr
			}
			var enrollment AppV23LocalEnrollment
			if readErr := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(root.PrincipalID), &enrollment); readErr != nil {
				return readErr
			}
			enrollment.ApprovedBy = newCredentialID
			enrollment.RootGeneration = root.Generation
			enrollment.Revision++
			enrollment.UpdatedHeight = height
			enrollmentData, err := appV23Marshal(enrollment)
			if err != nil {
				return err
			}
			return s.txnSet(txn, appV23EnrollmentKey(root.PrincipalID), enrollmentData)
		})
	})
}

func (s *BadgerStore) GetAppV23Enrollment(agentID string) (*AppV23LocalEnrollment, error) {
	var enrollment AppV23LocalEnrollment
	err := s.view(func(txn *badger.Txn) error {
		return s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(agentID), &enrollment)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &enrollment, nil
}

func (s *BadgerStore) GetAppV23Role(agentID string) (*AppV23RoleState, error) {
	var role AppV23RoleState
	err := s.view(func(txn *badger.Txn) error {
		return s.appV23ReadEffectiveJSONTxn(txn, appV23RoleKey(agentID), &role)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &role, nil
}

func (s *BadgerStore) GetAppV23AccessGroup(groupID string) (*AppV23AccessGroup, error) {
	var group AppV23AccessGroup
	err := s.view(func(txn *badger.Txn) error {
		return appV23ReadJSON(txn, appV23GroupKey(groupID), &group)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &group, nil
}

func (s *BadgerStore) ListAppV23AgentGroups(agentID string) ([]AppV23AccessGroup, error) {
	groups := make([]AppV23AccessGroup, 0, 2)
	err := s.view(func(txn *badger.Txn) error {
		prefix := appV23MemberGroupPrefix(agentID)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if len(groups) == AppV23MaxGroupsPerAgent {
				return errors.New("app-v23 per-agent group index exceeds deterministic bound")
			}
			groupID := string(it.Item().Key()[len(prefix):])
			var group AppV23AccessGroup
			if err := appV23ReadJSON(txn, appV23GroupKey(groupID), &group); err != nil {
				return err
			}
			groups = append(groups, group)
		}
		return nil
	})
	return groups, err
}

func appV23EncodeDomain(ownerID string, height int64) []byte {
	return appV23EncodeDomainWithParent(ownerID, "", height)
}

func appV23EncodeDomainWithParent(ownerID, parent string, height int64) []byte {
	value := make([]byte, 4+len(ownerID)+4+len(parent)+8)
	offset := encodeString(value, 0, ownerID)
	offset = encodeString(value, offset, parent)
	binary.BigEndian.PutUint64(value[offset:offset+8], uint64(height)) // #nosec G115 -- consensus height is non-negative
	return value
}

func (s *BadgerStore) BootstrapAppV23Genesis(input AppV23GenesisBootstrap) error {
	if err := validateCanonicalAgentID("root_id", input.RootID); err != nil {
		return err
	}
	if err := validateCanonicalAgentID("agent_id", input.AgentID); err != nil {
		return err
	}
	if input.RootID == input.AgentID {
		return errors.New("app-v23 bootstrap root and companion agent must be distinct")
	}
	if input.Scope == "" || input.Height <= 0 {
		return errors.New("app-v23 bootstrap requires scope and positive height")
	}
	if !ValidAppV23Profile(input.Profile) ||
		input.Profile == AppV23ProfileRoot ||
		input.Profile == AppV23ProfileLegacyRestricted {
		return fmt.Errorf("invalid app-v23 bootstrap profile %q", input.Profile)
	}
	if err := ValidateAppV23DomainName(input.HomeDomain); err != nil {
		return fmt.Errorf("invalid app-v23 bootstrap home domain: %w", err)
	}
	if IsSharedDomainName(input.HomeDomain) {
		return fmt.Errorf("app-v23 bootstrap home domain %q must be non-shared", input.HomeDomain)
	}
	if input.Clearance > 4 || !input.Capabilities.Valid() {
		return errors.New("invalid app-v23 bootstrap clearance or capabilities")
	}
	var genesisActivation *AppV23GenesisActivation
	var governanceDomain []byte
	if input.ActivateAtGenesis {
		decoded, err := hex.DecodeString(input.Scope)
		if err != nil || len(decoded) != sha256.Size ||
			hex.EncodeToString(decoded) != input.Scope {
			return errors.New("app-v23 genesis activation scope must be a canonical governance delegation domain")
		}
		if len(input.BootstrapDigest) != sha256.Size*2 {
			return errors.New("app-v23 genesis activation requires a canonical bootstrap digest")
		}
		if _, err := hex.DecodeString(input.BootstrapDigest); err != nil {
			return errors.New("app-v23 genesis activation requires a canonical bootstrap digest")
		}
		if err := validateCanonicalAgentID("app-v23 genesis validator", input.ValidatorID); err != nil {
			return err
		}
		if input.ValidatorPower <= 0 {
			return errors.New("app-v23 genesis activation requires positive validator power")
		}
		governanceDomain = decoded
		genesisActivation = &AppV23GenesisActivation{
			Version: consensuskeys.AppV23GenesisVersion, Scope: input.Scope,
			BootstrapDigest: input.BootstrapDigest,
			RootID:          input.RootID, AgentID: input.AgentID, Profile: input.Profile,
			HomeDomain: input.HomeDomain, Clearance: input.Clearance,
			Capabilities: uint32(input.Capabilities),
			ValidatorID:  input.ValidatorID, ValidatorPower: input.ValidatorPower,
		}
	}
	if existing, err := s.GetAppV23Root(); err != nil {
		return err
	} else if existing != nil {
		if existing.PrincipalID == input.RootID &&
			existing.Scope == input.Scope &&
			existing.BootstrapDigest == input.BootstrapDigest {
			enrollment, enrollmentErr := s.GetAppV23Enrollment(input.AgentID)
			if enrollmentErr != nil || enrollment == nil || !enrollment.Active ||
				enrollment.Profile != input.Profile ||
				enrollment.HomeDomain != input.HomeDomain ||
				enrollment.Clearance != input.Clearance ||
				enrollment.Capabilities != input.Capabilities {
				return errors.New("app-v23 bootstrap companion state does not match manifest")
			}
			owner, ownerErr := s.GetDomainOwner(input.HomeDomain)
			if ownerErr != nil || owner != input.AgentID {
				return errors.New("app-v23 bootstrap home domain ownership invariant failed")
			}
			persistedActivation, activationErr := s.GetAppV23GenesisActivation()
			if activationErr != nil {
				return activationErr
			}
			if genesisActivation == nil {
				if persistedActivation != nil {
					return errors.New("app-v23 bootstrap genesis activation invariant failed")
				}
			} else if persistedActivation == nil ||
				*persistedActivation != *genesisActivation {
				return errors.New("app-v23 bootstrap genesis activation invariant failed")
			} else {
				if err := s.ValidateAppV23GenesisLineage(); err != nil {
					return err
				}
				persistedValidators, validatorErr := s.LoadValidators()
				if validatorErr != nil {
					return fmt.Errorf("read app-v23 genesis validator: %w", validatorErr)
				}
				if len(persistedValidators) != 1 ||
					persistedValidators[input.ValidatorID] != input.ValidatorPower {
					return errors.New("app-v23 bootstrap validator set does not match manifest")
				}
				persistedDomain, domainErr := s.GetState(
					consensuskeys.GovernanceDelegationDomainV20,
				)
				if domainErr != nil || !bytes.Equal(persistedDomain, governanceDomain) {
					return errors.New("app-v23 bootstrap governance domain invariant failed")
				}
			}
			return s.ValidateAppV23State()
		}
		return errors.New("app-v23 root already exists with a different bootstrap manifest")
	}

	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			if _, err := txn.Get(appV23RootKey()); err == nil {
				return errors.New("app-v23 root already exists")
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if genesisActivation != nil {
				// The exact singleton validator is written below in this same
				// transaction. Any pre-existing consensus key, including a
				// validator-prefixed row, proves this is not a fresh lineage.
				all := badger.DefaultIteratorOptions
				all.PrefetchValues = false
				allIterator := txn.NewIterator(all)
				for allIterator.Rewind(); allIterator.Valid(); allIterator.Next() {
					dirty := string(allIterator.Item().KeyCopy(nil))
					allIterator.Close()
					return fmt.Errorf(
						"app-v23 genesis activation found preexisting consensus key %q",
						dirty,
					)
				}
				allIterator.Close()
				if _, err := txn.Get(appV23MigrationStateKey()); err == nil {
					return errors.New("app-v23 genesis activation cannot coexist with migration state")
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				opts := badger.DefaultIteratorOptions
				opts.Prefix = []byte("upgrade:applied:")
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				hasAppliedUpgrade := false
				for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
					hasAppliedUpgrade = true
					break
				}
				it.Close()
				if hasAppliedUpgrade {
					return errors.New("app-v23 genesis activation cannot coexist with applied upgrades")
				}
			}
			for _, id := range []string{input.RootID, input.AgentID} {
				if _, err := txn.Get(agentOnChainKey(id)); err == nil {
					return fmt.Errorf("bootstrap agent %s already exists", id)
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			if _, err := txn.Get(domainKey(input.HomeDomain)); err == nil {
				return fmt.Errorf("bootstrap home domain %s already exists", input.HomeDomain)
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			rootAgent := OnChainAgent{
				AgentID: input.RootID, Role: AppV23RoleAdmin, Clearance: 4,
				RegisteredAt: input.Height,
			}
			agent := OnChainAgent{
				AgentID: input.AgentID, Role: AppV23RoleMember,
				Clearance: input.Clearance, Capabilities: input.Capabilities,
				RegisteredAt: input.Height,
			}
			historyDigest, err := appV23NextRootHistoryDigest("", 1, input.RootID)
			if err != nil {
				return err
			}
			root := AppV23RootState{
				PrincipalID: input.RootID, CredentialID: input.RootID,
				Scope: input.Scope, Generation: 1, HistoryDigest: historyDigest,
				EstablishedAt:   input.Height,
				BootstrapDigest: input.BootstrapDigest,
			}
			rootEnrollment := AppV23LocalEnrollment{
				AgentID: input.RootID, ApprovedBy: input.RootID, RootGeneration: 1,
				Profile: AppV23ProfileRoot, Clearance: 4, Active: true,
				Revision: 1, UpdatedHeight: input.Height,
			}
			agentEnrollment := AppV23LocalEnrollment{
				AgentID: input.AgentID, ApprovedBy: input.RootID, RootGeneration: 1,
				Profile: input.Profile, HomeDomain: input.HomeDomain,
				Clearance: input.Clearance, Capabilities: input.Capabilities,
				Active: true, Revision: 1, UpdatedHeight: input.Height,
			}
			rootRole := AppV23RoleState{
				AgentID: input.RootID, Role: AppV23RoleAdmin, Revision: 1,
				UpdatedBy: input.RootID, UpdatedHeight: input.Height,
			}
			agentRole := AppV23RoleState{
				AgentID: input.AgentID, Role: AppV23RoleMember, Revision: 1,
				UpdatedBy: input.RootID, UpdatedHeight: input.Height,
			}
			values := []struct {
				key   []byte
				value any
			}{
				{agentOnChainKey(input.RootID), rootAgent},
				{agentOnChainKey(input.AgentID), agent},
				{appV23RootKey(), root},
				{appV23EnrollmentKey(input.RootID), rootEnrollment},
				{appV23EnrollmentKey(input.AgentID), agentEnrollment},
				{appV23RoleKey(input.RootID), rootRole},
				{appV23RoleKey(input.AgentID), agentRole},
			}
			for _, entry := range values {
				data, err := appV23Marshal(entry.value)
				if err != nil {
					return err
				}
				if err := s.txnSet(txn, entry.key, data); err != nil {
					return err
				}
			}
			if err := s.txnSet(
				txn,
				appV23RootCredentialKey(input.RootID),
				appV23RootCredentialGenerationValue(1),
			); err != nil {
				return err
			}
			if err := s.txnSet(txn, appV23AdminKey(input.RootID), []byte{1}); err != nil {
				return err
			}
			if err := s.txnSet(txn, domainKey(input.HomeDomain), appV23EncodeDomain(input.AgentID, input.Height)); err != nil {
				return err
			}
			if genesisActivation != nil {
				activationData, err := appV23Marshal(genesisActivation)
				if err != nil {
					return err
				}
				if err := s.txnSet(txn, appV23GenesisActivationKey(), activationData); err != nil {
					return err
				}
				if err := s.txnSet(
					txn,
					stateKey(consensuskeys.GovernanceDelegationDomainV20),
					governanceDomain,
				); err != nil {
					return err
				}
				validatorPower := make([]byte, 8)
				binary.BigEndian.PutUint64(validatorPower, uint64(input.ValidatorPower)) // #nosec G115 -- positive above
				if err := s.txnSet(
					txn,
					[]byte("validator:"+input.ValidatorID),
					validatorPower,
				); err != nil {
					return err
				}
			}
			// The first-party Companion owns its home domain. Do not also mint
			// a redundant grant: ownership is the canonical authority source.
			return nil
		})
	})
}

func (s *BadgerStore) GetAppV23GenesisActivation() (*AppV23GenesisActivation, error) {
	var activation AppV23GenesisActivation
	err := s.view(func(txn *badger.Txn) error {
		return appV23ReadJSON(txn, appV23GenesisActivationKey(), &activation)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	decodedScope, scopeErr := hex.DecodeString(activation.Scope)
	decodedDigest, digestErr := hex.DecodeString(activation.BootstrapDigest)
	if activation.Version != consensuskeys.AppV23GenesisVersion ||
		scopeErr != nil || len(decodedScope) != sha256.Size ||
		hex.EncodeToString(decodedScope) != activation.Scope ||
		digestErr != nil || len(decodedDigest) != sha256.Size ||
		hex.EncodeToString(decodedDigest) != activation.BootstrapDigest ||
		validateCanonicalAgentID("app-v23 genesis root", activation.RootID) != nil ||
		validateCanonicalAgentID("app-v23 genesis agent", activation.AgentID) != nil ||
		validateCanonicalAgentID("app-v23 genesis validator", activation.ValidatorID) != nil ||
		activation.RootID == activation.AgentID ||
		activation.Profile != AppV23ProfileCompanion ||
		ValidateAppV23DomainName(activation.HomeDomain) != nil ||
		IsSharedDomainName(activation.HomeDomain) ||
		activation.Clearance > 4 ||
		activation.Capabilities != 15 ||
		activation.ValidatorPower <= 0 {
		return nil, errors.New("invalid app-v23 genesis activation marker")
	}
	return &activation, nil
}

// HasAppV23MigrationArtifacts reports whether any governed app-v23 migration
// lineage is present. A chain born directly at app-v23 must never coexist with
// migration state, per-agent dispositions, or the crash-recoverable prepare/
// stage sidecar; those bytes prove a legacy upgrade path instead.
func (s *BadgerStore) HasAppV23MigrationArtifacts() (bool, error) {
	found := false
	err := s.view(func(txn *badger.Txn) error {
		for _, key := range [][]byte{
			appV23MigrationStateKey(),
			consensuskeys.AppV23MigrationPrepareKey,
		} {
			if _, err := txn.Get(key); err == nil {
				found = true
				return nil
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		for _, prefix := range [][]byte{
			[]byte("appv23:migration:"),
			consensuskeys.AppV23MigrationStagePrefix,
		} {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			if it.Seek(prefix); it.ValidForPrefix(prefix) {
				found = true
			}
			it.Close()
			if found {
				return nil
			}
		}
		return nil
	})
	return found, err
}

// ValidateAppV23GenesisLineage rejects every artifact that belongs to the
// canonical v2..v23 governed-upgrade path. Future post-v23 upgrades are not
// rejected: a chain born at v23 must be able to advance normally without
// erasing its immutable genesis marker.
func (s *BadgerStore) ValidateAppV23GenesisLineage() error {
	hasMigration, err := s.HasAppV23MigrationArtifacts()
	if err != nil {
		return err
	}
	if hasMigration {
		return errors.New("app-v23 genesis activation cannot coexist with migration artifacts")
	}
	for version := uint64(2); version <= consensuskeys.AppV23GenesisVersion; version++ {
		applied, err := s.GetAppliedUpgrade(fmt.Sprintf("app-v%d", version))
		if err != nil {
			return err
		}
		if applied != nil {
			return fmt.Errorf(
				"app-v23 genesis activation cannot coexist with applied app-v%d",
				version,
			)
		}
	}
	return nil
}

// EnsureAppV23Root deterministically migrates every legacy agent from exactly
// one existing global admin. Only the ambiguous self-registration fingerprint
// (mask30 with no owned non-shared domain) is quarantined for review.
func (s *BadgerStore) EnsureAppV23Root(scope string, height int64) error {
	if scope == "" || height <= 0 {
		return errors.New("app-v23 root bootstrap requires scope and positive height")
	}
	if root, err := s.GetAppV23Root(); err != nil {
		return err
	} else if root != nil {
		if root.PrincipalID == "" || root.CredentialID == "" || root.Generation == 0 || root.Scope == "" {
			return errors.New("invalid persisted app-v23 root")
		}
		if root.Scope != scope {
			return errors.New("persisted app-v23 root scope mismatch")
		}
		migration, err := s.GetAppV23MigrationState()
		if err != nil {
			return err
		}
		if migration != nil {
			return s.ValidateAppV23State()
		}
		agents, listErr := s.listAppV23MigrationAgents()
		if listErr != nil {
			return listErr
		}
		if len(agents) > appV23MaxInlineMigrationAgents {
			return s.ensureAppV23BootstrapRosterStaged(root, agents, height, true)
		}
		return s.reconcileAppV23BootstrapRoster(root, height)
	}
	agents, agentsErr := s.listAppV23MigrationAgents()
	if agentsErr != nil {
		return agentsErr
	}
	if len(agents) > appV23MaxInlineMigrationAgents {
		return s.ensureAppV23LegacyRootStaged(scope, agents, height, true)
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].AgentID < agents[j].AgentID })
	admins := make([]OnChainAgent, 0, 2)
	for _, agent := range agents {
		if agent.Role == AppV23RoleAdmin {
			admins = append(admins, agent)
		}
	}
	if len(admins) == 0 {
		return errors.New("app-v23 activation requires at least one legacy admin")
	}
	sort.Slice(admins, func(i, j int) bool {
		if admins[i].RegisteredAt != admins[j].RegisteredAt {
			return admins[i].RegisteredAt < admins[j].RegisteredAt
		}
		return admins[i].AgentID < admins[j].AgentID
	})
	admin := admins[0]
	owned := make(map[string][]string)
	domainOwners := make(map[string]string)
	if viewErr := s.view(func(txn *badger.Txn) error {
		prefix := []byte("domain:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			name := string(it.Item().Key()[len(prefix):])
			shared, sharedErr := appV23DomainIsSharedTxn(txn, name)
			if sharedErr != nil {
				return sharedErr
			}
			if shared {
				continue
			}
			var owner string
			if valueErr := it.Item().Value(func(value []byte) error {
				var decodeErr error
				owner, _, decodeErr = decodeString(value, 0)
				return decodeErr
			}); valueErr != nil {
				return valueErr
			}
			domainOwners[name] = owner
			if owner != "" {
				owned[owner] = append(owned[owner], name)
			}
		}
		return nil
	}); viewErr != nil {
		return viewErr
	}
	for id := range owned {
		sort.Strings(owned[id])
	}
	grantHolders, err := s.appV23MigrationGrantHolders()
	if err != nil {
		return err
	}
	sharedNames, err := s.appV23MigrationSharedDomainNames()
	if err != nil {
		return err
	}
	legacyRead, legacyFederations, err := s.appV23MigrationReadBaselines(agents)
	if err != nil {
		return err
	}

	schemaDigest := appV23MigrationSchemaDigest()
	dispositions := make([]AppV23MigrationDisposition, 0, len(agents))
	enrollments := make(map[string]AppV23LocalEnrollment, len(agents))
	roles := make(map[string]AppV23RoleState, len(agents))
	projected := make(map[string]OnChainAgent, len(agents))
	newDomains := make(map[string]string)
	for _, legacy := range agents {
		legacyJSON, marshalErr := json.Marshal(legacy)
		if marshalErr != nil {
			return marshalErr
		}
		legacySum := sha256.Sum256(legacyJSON)
		disposition := AppV23MigrationDisposition{
			AgentID: legacy.AgentID, LegacyPolicyDigest: hex.EncodeToString(legacySum[:]),
		}
		next := legacy
		next.Role = AppV23RoleMember
		var profile string
		active := true
		home := ""
		switch {
		case legacy.AgentID == admin.AgentID:
			next.Role = AppV23RoleAdmin
			next.Clearance = 4
			next.Capabilities = 0
			profile = AppV23ProfileRoot
			disposition.Disposition = "root"
		case legacy.Role == "observer":
			profile = AppV23ProfileReadOnly
			next.Capabilities = AgentCapabilityReadAllDomains |
				(legacy.Capabilities & AgentCapabilityDenyFederatedPipe)
			disposition.Disposition = "observer_read_only"
		default:
			legacyAdminReview := legacy.Role == AppV23RoleAdmin
			_, hasExplicitGrant := grantHolders[legacy.AgentID]
			hasReviewEvidence := appV23LegacyHasReviewEvidence(
				legacy, legacyRead[legacy.AgentID], hasExplicitGrant,
			)
			migrated, migrateErr := appV23MigrateLegacyMember(
				legacy, legacyAdminReview, hasReviewEvidence,
				owned, domainOwners, newDomains, sharedNames,
			)
			if migrateErr != nil {
				return fmt.Errorf("migrate legacy agent %s: %w", legacy.AgentID, migrateErr)
			}
			next.Role = migrated.Role
			next.Clearance = migrated.Clearance
			next.Capabilities = migrated.Capabilities
			profile = migrated.Profile
			active = migrated.Active
			home = migrated.HomeDomain
			disposition.Disposition = migrated.Disposition
		}
		enrollment := AppV23LocalEnrollment{
			AgentID: legacy.AgentID, ApprovedBy: admin.AgentID, RootGeneration: 1,
			Profile: profile, HomeDomain: home, Clearance: next.Clearance,
			Capabilities: next.Capabilities, Active: active, Revision: 1,
			UpdatedHeight: height,
		}
		if policyErr := ValidateAppV23EnrollmentPolicy(
			next.Role, profile, next.Capabilities, next.Clearance, active,
		); policyErr != nil {
			return fmt.Errorf("legacy agent %s has incompatible migration policy: %w", legacy.AgentID, policyErr)
		}
		disposition.Profile, disposition.HomeDomain, disposition.Active = profile, home, active
		dispositions = append(dispositions, disposition)
		enrollments[legacy.AgentID] = enrollment
		roles[legacy.AgentID] = AppV23RoleState{
			AgentID: legacy.AgentID, Role: next.Role, Revision: 1,
			UpdatedBy: admin.AgentID, UpdatedHeight: height,
		}
		projected[legacy.AgentID] = next
	}
	manifestJSON, err := json.Marshal(dispositions)
	if err != nil {
		return err
	}
	manifestSum := sha256.Sum256(manifestJSON)
	manifestDigest := hex.EncodeToString(manifestSum[:])
	migrationState := AppV23MigrationState{
		SchemaDigest: schemaDigest, ManifestDigest: manifestDigest,
		AgentCount: len(dispositions), LegacyReadCount: len(legacyRead),
		LegacyFederationCount: len(legacyFederations), Height: height,
	}
	legacyAdminIDs := make([]string, 0, len(admins))
	for _, legacyAdmin := range admins {
		legacyAdminIDs = append(legacyAdminIDs, legacyAdmin.AgentID)
	}
	appV23SetLegacyAdminSummary(&migrationState, legacyAdminIDs)
	historyDigest, err := appV23NextRootHistoryDigest("", 1, admin.AgentID)
	if err != nil {
		return err
	}
	root := AppV23RootState{
		PrincipalID: admin.AgentID, CredentialID: admin.AgentID,
		Scope: scope, Generation: 1, HistoryDigest: historyDigest,
		EstablishedAt:   height,
		BootstrapDigest: manifestDigest,
	}
	return s.update(func(txn *badger.Txn) error {
		if _, err := txn.Get(appV23RootKey()); err == nil {
			return ErrAppV23RevisionConflict
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		writeJSON := func(key []byte, value any) error {
			data, err := appV23Marshal(value)
			if err != nil {
				return err
			}
			return s.txnSet(txn, key, data)
		}
		if err := writeJSON(appV23RootKey(), root); err != nil {
			return err
		}
		if err := s.txnSet(
			txn,
			appV23RootCredentialKey(root.CredentialID),
			appV23RootCredentialGenerationValue(1),
		); err != nil {
			return err
		}
		if err := writeJSON(appV23MigrationStateKey(), migrationState); err != nil {
			return err
		}
		for id, baseline := range legacyRead {
			if err := writeJSON(appV23LegacyReadKey(id), baseline); err != nil {
				return err
			}
		}
		for id, baseline := range legacyFederations {
			if err := writeJSON(appV23LegacyFederationKey(id), baseline); err != nil {
				return err
			}
		}
		for _, disposition := range dispositions {
			id := disposition.AgentID
			if err := writeJSON(agentOnChainKey(id), projected[id]); err != nil {
				return err
			}
			if err := writeJSON(appV23EnrollmentKey(id), enrollments[id]); err != nil {
				return err
			}
			if err := writeJSON(appV23RoleKey(id), roles[id]); err != nil {
				return err
			}
			if err := writeJSON(appV23MigrationKey(id), disposition); err != nil {
				return err
			}
			if roles[id].Role == AppV23RoleAdmin {
				if err := s.txnSet(txn, appV23AdminKey(id), []byte{1}); err != nil {
					return err
				}
			}
			// Home authority is ownership, not a duplicate explicit grant.
			// Keeping the migration projection grant-free avoids two mutable
			// sources of truth and is byte-for-byte consistent with the staged
			// representation used for large legacy rosters.
		}
		for domain, owner := range newDomains {
			if _, exists := domainOwners[domain]; !exists {
				if err := s.txnSet(txn, domainKey(domain), appV23EncodeDomain(owner, height)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *BadgerStore) appV23MigrationDomainOwners() (map[string][]string, map[string]string, error) {
	owned := make(map[string][]string)
	domainOwners := make(map[string]string)
	err := s.view(func(txn *badger.Txn) error {
		prefix := []byte("domain:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			name := string(it.Item().Key()[len(prefix):])
			shared, err := appV23DomainIsSharedTxn(txn, name)
			if err != nil {
				return err
			}
			if shared {
				continue
			}
			var owner string
			if err := it.Item().Value(func(value []byte) error {
				var decodeErr error
				owner, _, decodeErr = decodeString(value, 0)
				return decodeErr
			}); err != nil {
				return err
			}
			domainOwners[name] = owner
			if owner != "" {
				owned[owner] = append(owned[owner], name)
			}
		}
		return nil
	})
	for id := range owned {
		sort.Strings(owned[id])
	}
	return owned, domainOwners, err
}

func (s *BadgerStore) appV23MigrationSharedDomainNames() (map[string]struct{}, error) {
	shared := make(map[string]struct{})
	err := s.view(func(txn *badger.Txn) error {
		prefix := stateKey("shared_domain:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			name := string(it.Item().Key()[len(prefix):])
			if name == "" {
				return errors.New("invalid empty dynamic shared-domain marker")
			}
			shared[name] = struct{}{}
		}
		return nil
	})
	return shared, err
}

func appV23AddStagedJSON(stage map[string][]byte, key []byte, value any) error {
	data, err := appV23Marshal(value)
	if err != nil {
		return err
	}
	stage[string(key)] = data
	return nil
}

func appV23AllocateExistingMigrationHome(
	agentID string,
	owned map[string][]string,
	sharedNames map[string]struct{},
) (string, error) {
	for _, home := range owned[agentID] {
		if err := ValidateAppV23DomainName(home); err != nil {
			// Preserve the historical domain row for reads/audit, but never
			// bind new app-v23 authority to an ambiguous legacy key component.
			continue
		}
		if _, dynamicallyShared := sharedNames[home]; dynamicallyShared {
			continue
		}
		if !IsSharedDomainName(home) {
			return home, nil
		}
	}
	return "", nil
}

func appV23AllocateMigrationHome(
	agentID string,
	owned map[string][]string,
	domainOwners map[string]string,
	newDomains map[string]string,
	sharedNames map[string]struct{},
) (string, error) {
	for _, home := range owned[agentID] {
		_, dynamicallyShared := sharedNames[home]
		if ValidateAppV23DomainName(home) == nil &&
			!IsSharedDomainName(home) && !dynamicallyShared {
			return home, nil
		}
	}
	base := "local-" + agentID
	home := base
	for suffix := 1; ; suffix++ {
		_, dynamicallyShared := sharedNames[home]
		_, newlyAllocated := newDomains[home]
		owner, exists := domainOwners[home]
		if !IsSharedDomainName(home) && !dynamicallyShared && !newlyAllocated &&
			(!exists || owner == agentID) {
			break
		}
		home = fmt.Sprintf("%s-%d", base, suffix)
		if suffix == 1024 {
			return "", errors.New("unable to allocate deterministic app-v23 home domain")
		}
	}
	newDomains[home] = agentID
	return home, nil
}

// appV23MigrationGrantHolders returns principals with at least one explicit
// legacy read-or-higher grant. The presence of such a row is deterministic
// evidence that a mask-30 principal was reviewed or used beyond bare
// self-registration, so migration must preserve its read authority instead of
// quarantining it solely because it owns no domain.
func (s *BadgerStore) appV23MigrationGrantHolders() (map[string]struct{}, error) {
	holders := make(map[string]struct{})
	err := s.view(func(txn *badger.Txn) error {
		prefix := []byte("grant:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			split := bytes.LastIndexByte(key, ':')
			if split <= len(prefix) || split+1 >= len(key) {
				return errors.New("invalid legacy access-grant key")
			}
			agentID := string(key[split+1:])
			if !isCanonicalAgentID(agentID) {
				return errors.New("invalid legacy access-grant principal")
			}
			var level uint8
			if err := it.Item().Value(func(value []byte) error {
				if len(value) < 13 {
					return errors.New("invalid legacy access-grant value")
				}
				level = value[0]
				if level > 3 {
					return errors.New("invalid legacy access-grant level")
				}
				granterID, offset, err := decodeString(value, 9)
				if err != nil || offset != len(value) ||
					!isCanonicalAgentID(granterID) {
					return errors.New("invalid legacy access-grant granter")
				}
				return nil
			}); err != nil {
				return err
			}
			if level >= 1 {
				holders[agentID] = struct{}{}
			}
		}
		return nil
	})
	return holders, err
}

func appV23DecodeLegacyMembership(value []byte) (uint8, string, error) {
	if len(value) < 1+4+8 {
		return 0, "", errors.New("invalid legacy membership value")
	}
	clearance := value[0]
	if clearance > uint8(ClearanceTopSecret) {
		return 0, "", errors.New("invalid legacy membership clearance")
	}
	role, offset, err := decodeString(value, 1)
	if err != nil || role == "" || offset+8 != len(value) {
		return 0, "", errors.New("invalid legacy membership role or height")
	}
	return clearance, role, nil
}

func appV23DecodeLegacyFederation(
	id string, value []byte,
) (AppV23LegacyFederationBaseline, error) {
	result := AppV23LegacyFederationBaseline{FederationID: id}
	var offset int
	var err error
	result.ProposerOrgID, offset, err = decodeString(value, 0)
	if err != nil {
		return result, err
	}
	result.TargetOrgID, offset, err = decodeString(value, offset)
	if err != nil || result.ProposerOrgID == "" || result.TargetOrgID == "" ||
		offset+10 > len(value) {
		return result, errors.New("invalid legacy federation identity")
	}
	result.MaxClearance = value[offset]
	if result.MaxClearance > uint8(ClearanceTopSecret) {
		return result, errors.New("invalid legacy federation clearance")
	}
	offset++
	result.ExpiresAt = int64(binary.BigEndian.Uint64(value[offset : offset+8])) // #nosec G115 -- stored consensus timestamp
	offset += 8
	if value[offset] > 1 {
		return result, errors.New("invalid legacy federation approval flag")
	}
	offset++ // requiresApproval is audit data, not a read-authority predicate.
	result.Status, offset, err = decodeString(value, offset)
	if err != nil {
		return result, err
	}
	if result.Status == "" {
		return result, errors.New("invalid empty legacy federation status")
	}
	decodeList := func(optional bool) ([]string, error) {
		if offset+4 > len(value) {
			if optional && offset == len(value) {
				return nil, nil
			}
			return nil, errors.New("invalid legacy federation list")
		}
		count := int(binary.BigEndian.Uint32(value[offset : offset+4]))
		offset += 4
		if count > AppV23MaxLegacyMembershipLinks {
			return nil, errors.New("legacy federation list exceeds bound")
		}
		out := make([]string, 0, count)
		for i := 0; i < count; i++ {
			entry, next, decodeErr := decodeString(value, offset)
			if decodeErr != nil {
				return nil, decodeErr
			}
			offset = next
			out = append(out, entry)
		}
		return out, nil
	}
	result.AllowedDomains, err = decodeList(true)
	if err != nil {
		return result, err
	}
	result.AllowedDepts, err = decodeList(true)
	if err != nil {
		return result, err
	}
	if offset != len(value) {
		return result, errors.New("legacy federation has trailing bytes")
	}
	return result, nil
}

func (s *BadgerStore) appV23MigrationReadBaselines(
	agents []OnChainAgent,
) (
	map[string]AppV23LegacyReadBaseline,
	map[string]AppV23LegacyFederationBaseline,
	error,
) {
	baselines := make(map[string]AppV23LegacyReadBaseline, len(agents))
	for _, legacy := range agents {
		legacyJSON, err := json.Marshal(legacy)
		if err != nil {
			return nil, nil, err
		}
		sum := sha256.Sum256(legacyJSON)
		baselines[legacy.AgentID] = AppV23LegacyReadBaseline{
			AgentID: legacy.AgentID, LegacyPolicyDigest: hex.EncodeToString(sum[:]),
			DomainAccess: legacy.DomainAccess, VisibleAgents: legacy.VisibleAgents,
		}
	}
	federations := make(map[string]AppV23LegacyFederationBaseline)
	membershipCount := 0
	federationScopeEntries := 0
	err := s.view(func(txn *badger.Txn) error {
		scanMemberships := func(prefix []byte, department bool) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				membershipCount++
				if membershipCount > AppV23MaxLegacyMembershipLinks {
					return errors.New("legacy membership snapshot exceeds bound")
				}
				suffix := string(it.Item().Key()[len(prefix):])
				separator := strings.LastIndexByte(suffix, ':')
				if separator <= 0 || separator == len(suffix)-1 {
					return errors.New("malformed legacy membership key")
				}
				scope, agentID := suffix[:separator], suffix[separator+1:]
				if !isCanonicalAgentID(agentID) {
					return errors.New("malformed legacy membership principal")
				}
				var clearance uint8
				var role string
				if err := it.Item().Value(func(value []byte) error {
					var decodeErr error
					clearance, role, decodeErr = appV23DecodeLegacyMembership(value)
					return decodeErr
				}); err != nil {
					return err
				}
				baseline, tracked := baselines[agentID]
				if !tracked {
					continue
				}
				if department {
					parts := strings.SplitN(scope, ":", 2)
					if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
						return errors.New("malformed legacy department membership scope")
					}
					baseline.DeptMemberships = append(
						baseline.DeptMemberships,
						AppV23LegacyDeptMembership{
							OrgID: parts[0], DeptID: parts[1],
							Clearance: clearance, Role: role,
						},
					)
				} else {
					if scope == "" {
						return errors.New("malformed legacy organization membership scope")
					}
					baseline.OrgMemberships = append(
						baseline.OrgMemberships,
						AppV23LegacyOrgMembership{
							OrgID: scope, Clearance: clearance, Role: role,
						},
					)
				}
				baselines[agentID] = baseline
			}
			return nil
		}
		if err := scanMemberships([]byte("org_member:"), false); err != nil {
			return err
		}
		if err := scanMemberships([]byte("dept_member:"), true); err != nil {
			return err
		}
		prefix := []byte("federation:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if len(federations) == AppV23MaxLegacyFederations {
				return errors.New("legacy federation snapshot exceeds bound")
			}
			id := string(it.Item().Key()[len(prefix):])
			if id == "" {
				return errors.New("malformed legacy federation key")
			}
			var baseline AppV23LegacyFederationBaseline
			if err := it.Item().Value(func(value []byte) error {
				var decodeErr error
				baseline, decodeErr = appV23DecodeLegacyFederation(id, value)
				return decodeErr
			}); err != nil {
				return err
			}
			federationScopeEntries +=
				len(baseline.AllowedDomains) + len(baseline.AllowedDepts)
			if federationScopeEntries > AppV23MaxLegacyMembershipLinks {
				return errors.New("legacy federation scope snapshot exceeds bound")
			}
			federations[id] = baseline
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	for id, baseline := range baselines {
		sort.Slice(baseline.OrgMemberships, func(i, j int) bool {
			return baseline.OrgMemberships[i].OrgID < baseline.OrgMemberships[j].OrgID
		})
		sort.Slice(baseline.DeptMemberships, func(i, j int) bool {
			left, right := baseline.DeptMemberships[i], baseline.DeptMemberships[j]
			if left.OrgID != right.OrgID {
				return left.OrgID < right.OrgID
			}
			return left.DeptID < right.DeptID
		})
		baselines[id] = baseline
	}
	return baselines, federations, nil
}

func appV23LegacyHasReviewEvidence(
	legacy OnChainAgent,
	baseline AppV23LegacyReadBaseline,
	hasExplicitGrant bool,
) bool {
	return hasExplicitGrant ||
		legacy.Clearance != uint8(ClearanceInternal) ||
		legacy.DomainAccess != "" ||
		legacy.VisibleAgents != "" ||
		legacy.OrgID != "" ||
		legacy.DeptID != "" ||
		len(baseline.OrgMemberships) > 0 ||
		len(baseline.DeptMemberships) > 0
}

type appV23LegacyMemberMigration struct {
	Role         string
	Profile      string
	HomeDomain   string
	Clearance    uint8
	Capabilities AgentCapabilities
	Active       bool
	Disposition  string
}

// appV23MigrateLegacyMember projects one non-Root, non-observer legacy agent.
// Capability bits are never silently cleared. Canonical masks use the simple
// named app-v23 presets; every other exact mask remains under the hidden
// migration-only Legacy-restricted profile until an explicit policy change.
func appV23MigrateLegacyMember(
	legacy OnChainAgent,
	legacyAdminReview, hasReviewEvidence bool,
	owned map[string][]string,
	domainOwners, newDomains map[string]string,
	sharedNames map[string]struct{},
) (appV23LegacyMemberMigration, error) {
	result := appV23LegacyMemberMigration{
		Role:         AppV23RoleMember,
		Profile:      AppV23ProfileLegacyRestricted,
		Clearance:    legacy.Clearance,
		Capabilities: legacy.Capabilities,
		Active:       true,
		Disposition:  "legacy_restricted",
	}
	if !legacy.Capabilities.Valid() {
		return result, errors.New("legacy agent has unknown capability bits")
	}

	if home, err := appV23AllocateExistingMigrationHome(
		legacy.AgentID, owned, sharedNames,
	); err != nil {
		return result, err
	} else if home != "" {
		result.HomeDomain = home
	} else if legacy.Capabilities == DefaultSelfRegisteredAgentCapabilities &&
		!legacyAdminReview && !hasReviewEvidence {
		// Exact app-v22 self-registration fingerprint: no owned domain and no
		// explicit grant means there is no operator-approved authority to
		// preserve. Keep the exact mask for audit, but make it inactive.
		result.Active = false
		result.Disposition = "pending_review"
		return result, nil
	} else if !legacy.Capabilities.Has(AgentCapabilityDenyDomainClaim) {
		// The principal could already claim an unowned non-shared domain under
		// app-v22, so assigning its required app-v23 home does not create a new
		// class of authority.
		home, err := appV23AllocateMigrationHome(
			legacy.AgentID, owned, domainOwners, newDomains, sharedNames,
		)
		if err != nil {
			return result, err
		}
		result.HomeDomain = home
	}

	switch legacy.Capabilities {
	case 0, AgentCapabilityDenyFederatedPipe:
		result.Profile = AppV23ProfileStandard
		result.Disposition = "member"
	case 15, 15 | AgentCapabilityDenyFederatedPipe:
		result.Profile = AppV23ProfileCompanion
		result.Disposition = "companion"
	}
	if legacyAdminReview {
		result.Profile = AppV23ProfileLegacyRestricted
		result.Disposition = "legacy_admin_review"
	}
	return result, nil
}

func (s *BadgerStore) ensureAppV23LegacyRootStaged(
	scope string,
	agents []OnChainAgent,
	height int64,
	promote bool,
) error {
	sort.Slice(agents, func(i, j int) bool { return agents[i].AgentID < agents[j].AgentID })
	admins := make([]OnChainAgent, 0, 2)
	for _, agent := range agents {
		if agent.Role == AppV23RoleAdmin {
			admins = append(admins, agent)
		}
	}
	if len(admins) == 0 {
		return errors.New("app-v23 activation requires at least one legacy admin")
	}
	sort.Slice(admins, func(i, j int) bool {
		if admins[i].RegisteredAt != admins[j].RegisteredAt {
			return admins[i].RegisteredAt < admins[j].RegisteredAt
		}
		return admins[i].AgentID < admins[j].AgentID
	})
	rootAgent := admins[0]
	owned, domainOwners, ownersErr := s.appV23MigrationDomainOwners()
	if ownersErr != nil {
		return ownersErr
	}
	grantHolders, grantsErr := s.appV23MigrationGrantHolders()
	if grantsErr != nil {
		return grantsErr
	}
	sharedNames, sharedErr := s.appV23MigrationSharedDomainNames()
	if sharedErr != nil {
		return sharedErr
	}
	legacyRead, legacyFederations, baselineErr := s.appV23MigrationReadBaselines(agents)
	if baselineErr != nil {
		return baselineErr
	}
	newDomains := make(map[string]string)
	stage := make(map[string][]byte, len(agents)*6)
	dispositions := make([]AppV23MigrationDisposition, 0, len(agents))
	activation := make(map[string][]byte, AppV23MaxAdmins+4)

	for _, legacy := range agents {
		legacyJSON, marshalErr := json.Marshal(legacy)
		if marshalErr != nil {
			return marshalErr
		}
		legacySum := sha256.Sum256(legacyJSON)
		disposition := AppV23MigrationDisposition{
			AgentID: legacy.AgentID, LegacyPolicyDigest: hex.EncodeToString(legacySum[:]),
		}
		next := legacy
		next.Role = AppV23RoleMember
		var profile string
		active := true
		home := ""
		switch {
		case legacy.AgentID == rootAgent.AgentID:
			next.Role = AppV23RoleAdmin
			next.Clearance = 4
			next.Capabilities = 0
			profile = AppV23ProfileRoot
			disposition.Disposition = "root"
		case legacy.Role == "observer":
			profile = AppV23ProfileReadOnly
			next.Capabilities = AgentCapabilityReadAllDomains |
				(legacy.Capabilities & AgentCapabilityDenyFederatedPipe)
			disposition.Disposition = "observer_read_only"
		default:
			legacyAdminReview := legacy.Role == AppV23RoleAdmin
			_, hasExplicitGrant := grantHolders[legacy.AgentID]
			hasReviewEvidence := appV23LegacyHasReviewEvidence(
				legacy, legacyRead[legacy.AgentID], hasExplicitGrant,
			)
			migrated, migrateErr := appV23MigrateLegacyMember(
				legacy, legacyAdminReview, hasReviewEvidence,
				owned, domainOwners, newDomains, sharedNames,
			)
			if migrateErr != nil {
				return fmt.Errorf("migrate legacy agent %s: %w", legacy.AgentID, migrateErr)
			}
			next.Role = migrated.Role
			next.Clearance = migrated.Clearance
			next.Capabilities = migrated.Capabilities
			profile = migrated.Profile
			active = migrated.Active
			home = migrated.HomeDomain
			disposition.Disposition = migrated.Disposition
		}
		enrollment := AppV23LocalEnrollment{
			AgentID: legacy.AgentID, ApprovedBy: rootAgent.AgentID, RootGeneration: 1,
			Profile: profile, HomeDomain: home, Clearance: next.Clearance,
			Capabilities: next.Capabilities, Active: active, Revision: 1,
			UpdatedHeight: height,
		}
		if err := ValidateAppV23EnrollmentPolicy(
			next.Role, profile, next.Capabilities, next.Clearance, active,
		); err != nil {
			return fmt.Errorf("legacy agent %s has incompatible migration policy: %w", legacy.AgentID, err)
		}
		role := AppV23RoleState{
			AgentID: legacy.AgentID, Role: next.Role, Revision: 1,
			UpdatedBy: rootAgent.AgentID, UpdatedHeight: height,
		}
		disposition.Profile, disposition.HomeDomain, disposition.Active = profile, home, active
		dispositions = append(dispositions, disposition)
		if err := appV23AddStagedJSON(stage, appV23ProjectedAgentKey(legacy.AgentID), next); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(stage, appV23EnrollmentKey(legacy.AgentID), enrollment); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(stage, appV23RoleKey(legacy.AgentID), role); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(stage, appV23MigrationKey(legacy.AgentID), disposition); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(
			stage, appV23LegacyReadKey(legacy.AgentID), legacyRead[legacy.AgentID],
		); err != nil {
			return err
		}
		if role.Role == AppV23RoleAdmin {
			activation[string(appV23AdminKey(legacy.AgentID))] = []byte{1}
		}
	}
	for domain, owner := range newDomains {
		stage[string(domainKey(domain))] = appV23EncodeDomain(owner, height)
	}
	for _, legacyAdmin := range admins {
		stage[string(appV23LegacyAdminAuditKey(legacyAdmin.AgentID))] = []byte{1}
	}
	for id, baseline := range legacyFederations {
		if err := appV23AddStagedJSON(
			stage, appV23LegacyFederationKey(id), baseline,
		); err != nil {
			return err
		}
	}
	manifestJSON, err := json.Marshal(dispositions)
	if err != nil {
		return err
	}
	manifestSum := sha256.Sum256(manifestJSON)
	manifestDigest := hex.EncodeToString(manifestSum[:])
	historyDigest, err := appV23NextRootHistoryDigest("", 1, rootAgent.AgentID)
	if err != nil {
		return err
	}
	root := AppV23RootState{
		PrincipalID: rootAgent.AgentID, CredentialID: rootAgent.AgentID,
		Scope: scope, Generation: 1, HistoryDigest: historyDigest,
		EstablishedAt: height, BootstrapDigest: manifestDigest,
	}
	legacyAdmins := make([]string, 0, len(admins))
	for _, admin := range admins {
		legacyAdmins = append(legacyAdmins, admin.AgentID)
	}
	sort.Strings(legacyAdmins)
	stageDigest, err := appV23StageDigest(stage)
	if err != nil {
		return err
	}
	migration := AppV23MigrationState{
		SchemaDigest: appV23MigrationSchemaDigest(), ManifestDigest: manifestDigest,
		StageDigest: stageDigest, StageCount: len(stage),
		AgentCount: len(dispositions), LegacyReadCount: len(legacyRead),
		LegacyFederationCount: len(legacyFederations), Height: height,
	}
	appV23SetLegacyAdminSummary(&migration, legacyAdmins)
	planDigest, err := appV23PlanDigest(root, migration)
	if err != nil {
		return err
	}
	plan := &appV23MigrationPlan{
		root: root, migration: migration, stage: stage, activation: activation,
		preparation: appV23MigrationPreparation{
			Scope: scope, Height: height, SchemaDigest: migration.SchemaDigest,
			PlanDigest: planDigest, ManifestDigest: manifestDigest,
			StageDigest: stageDigest, StageCount: len(stage),
		},
	}
	if !promote {
		return s.prepareAppV23MigrationPlan(plan)
	}
	return s.promoteAppV23MigrationPlan(plan)
}

func (s *BadgerStore) ensureAppV23BootstrapRosterStaged(
	root *AppV23RootState,
	agents []OnChainAgent,
	height int64,
	promote bool,
) error {
	sort.Slice(agents, func(i, j int) bool { return agents[i].AgentID < agents[j].AgentID })
	owned, domainOwners, ownersErr := s.appV23MigrationDomainOwners()
	if ownersErr != nil {
		return ownersErr
	}
	grantHolders, grantsErr := s.appV23MigrationGrantHolders()
	if grantsErr != nil {
		return grantsErr
	}
	sharedNames, sharedErr := s.appV23MigrationSharedDomainNames()
	if sharedErr != nil {
		return sharedErr
	}
	legacyRead, legacyFederations, baselineErr := s.appV23MigrationReadBaselines(agents)
	if baselineErr != nil {
		return baselineErr
	}
	legacyAdmins := []string{root.PrincipalID}
	for _, agent := range agents {
		if agent.AgentID != root.PrincipalID && agent.Role == AppV23RoleAdmin {
			legacyAdmins = append(legacyAdmins, agent.AgentID)
		}
	}
	stage := make(map[string][]byte, len(agents)*6)
	activation := make(map[string][]byte, AppV23MaxAdmins)
	newDomains := make(map[string]string)
	dispositions := make([]AppV23MigrationDisposition, 0, len(agents))
	for _, legacy := range agents {
		legacyJSON, marshalErr := json.Marshal(legacy)
		if marshalErr != nil {
			return marshalErr
		}
		sum := sha256.Sum256(legacyJSON)
		disposition := AppV23MigrationDisposition{
			AgentID: legacy.AgentID, LegacyPolicyDigest: hex.EncodeToString(sum[:]),
		}
		existingEnrollment, err := s.GetAppV23Enrollment(legacy.AgentID)
		if err != nil {
			return err
		}
		existingRole, err := s.GetAppV23Role(legacy.AgentID)
		if err != nil {
			return err
		}
		if existingEnrollment != nil || existingRole != nil {
			if existingEnrollment == nil || existingRole == nil ||
				ValidateAppV23EnrollmentPolicy(
					existingRole.Role, existingEnrollment.Profile,
					existingEnrollment.Capabilities, existingEnrollment.Clearance,
					existingEnrollment.Active,
				) != nil {
				return fmt.Errorf("bootstrap principal %s has incomplete app-v23 policy", legacy.AgentID)
			}
			disposition.Disposition = "bootstrap_preserved"
			disposition.Profile = existingEnrollment.Profile
			disposition.HomeDomain = existingEnrollment.HomeDomain
			disposition.Active = existingEnrollment.Active
			dispositions = append(dispositions, disposition)
			if err := appV23AddStagedJSON(stage, appV23MigrationKey(legacy.AgentID), disposition); err != nil {
				return err
			}
			if err := appV23AddStagedJSON(
				stage, appV23LegacyReadKey(legacy.AgentID), legacyRead[legacy.AgentID],
			); err != nil {
				return err
			}
			continue
		}

		next := legacy
		next.Role = AppV23RoleMember
		var profile string
		active := true
		home := ""
		legacyAdminReview := legacy.Role == AppV23RoleAdmin
		switch legacy.Role {
		case "observer":
			profile = AppV23ProfileReadOnly
			next.Capabilities = AgentCapabilityReadAllDomains |
				(legacy.Capabilities & AgentCapabilityDenyFederatedPipe)
			disposition.Disposition = "observer_read_only"
		default:
			_, hasExplicitGrant := grantHolders[legacy.AgentID]
			hasReviewEvidence := appV23LegacyHasReviewEvidence(
				legacy, legacyRead[legacy.AgentID], hasExplicitGrant,
			)
			migrated, migrateErr := appV23MigrateLegacyMember(
				legacy, legacyAdminReview, hasReviewEvidence,
				owned, domainOwners, newDomains, sharedNames,
			)
			if migrateErr != nil {
				return fmt.Errorf("migrate bootstrap roster agent %s: %w", legacy.AgentID, migrateErr)
			}
			next.Role = migrated.Role
			next.Clearance = migrated.Clearance
			next.Capabilities = migrated.Capabilities
			profile = migrated.Profile
			active = migrated.Active
			home = migrated.HomeDomain
			disposition.Disposition = migrated.Disposition
		}
		enrollment := AppV23LocalEnrollment{
			AgentID: legacy.AgentID, ApprovedBy: root.CredentialID,
			RootGeneration: root.Generation, Profile: profile, HomeDomain: home,
			Clearance: next.Clearance, Capabilities: next.Capabilities,
			Active: active, Revision: 1, UpdatedHeight: height,
		}
		if err := ValidateAppV23EnrollmentPolicy(
			next.Role, profile, next.Capabilities, next.Clearance, active,
		); err != nil {
			return fmt.Errorf("bootstrap roster agent %s has incompatible migration policy: %w", legacy.AgentID, err)
		}
		role := AppV23RoleState{
			AgentID: legacy.AgentID, Role: next.Role, Revision: 1,
			UpdatedBy: root.CredentialID, UpdatedHeight: height,
		}
		disposition.Profile, disposition.HomeDomain, disposition.Active = profile, home, active
		dispositions = append(dispositions, disposition)
		if err := appV23AddStagedJSON(stage, appV23ProjectedAgentKey(legacy.AgentID), next); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(stage, appV23EnrollmentKey(legacy.AgentID), enrollment); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(stage, appV23RoleKey(legacy.AgentID), role); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(stage, appV23MigrationKey(legacy.AgentID), disposition); err != nil {
			return err
		}
		if err := appV23AddStagedJSON(
			stage, appV23LegacyReadKey(legacy.AgentID), legacyRead[legacy.AgentID],
		); err != nil {
			return err
		}
		if role.Role == AppV23RoleAdmin {
			activation[string(appV23AdminKey(legacy.AgentID))] = []byte{1}
		}
	}
	for domain, owner := range newDomains {
		stage[string(domainKey(domain))] = appV23EncodeDomain(owner, height)
	}
	sort.Strings(legacyAdmins)
	for _, legacyAdmin := range legacyAdmins {
		stage[string(appV23LegacyAdminAuditKey(legacyAdmin))] = []byte{1}
	}
	for id, baseline := range legacyFederations {
		if stageErr := appV23AddStagedJSON(
			stage, appV23LegacyFederationKey(id), baseline,
		); stageErr != nil {
			return stageErr
		}
	}
	manifestJSON, err := json.Marshal(dispositions)
	if err != nil {
		return err
	}
	manifestSum := sha256.Sum256(manifestJSON)
	manifestDigest := hex.EncodeToString(manifestSum[:])
	stageDigest, err := appV23StageDigest(stage)
	if err != nil {
		return err
	}
	migration := AppV23MigrationState{
		SchemaDigest: appV23MigrationSchemaDigest(), ManifestDigest: manifestDigest,
		StageDigest: stageDigest, StageCount: len(stage),
		RootBootstrapDigest: root.BootstrapDigest,
		AgentCount:          len(dispositions), LegacyReadCount: len(legacyRead),
		LegacyFederationCount: len(legacyFederations), Height: height,
	}
	appV23SetLegacyAdminSummary(&migration, legacyAdmins)
	planDigest, err := appV23PlanDigest(*root, migration)
	if err != nil {
		return err
	}
	plan := &appV23MigrationPlan{
		root: *root, rootPreexisting: true, migration: migration,
		stage: stage, activation: activation,
		preparation: appV23MigrationPreparation{
			Scope: root.Scope, Height: height, SchemaDigest: migration.SchemaDigest,
			PlanDigest: planDigest, ManifestDigest: manifestDigest,
			StageDigest: stageDigest, StageCount: len(stage),
		},
	}
	if !promote {
		return s.prepareAppV23MigrationPlan(plan)
	}
	return s.promoteAppV23MigrationPlan(plan)
}

const appV23MigrationStageBatchSize = 256

// PrepareAppV23Migration durably builds the AppHash-excluded large-roster
// projection before FinalizeBlock opens its speculative Badger transaction.
// The activation block is a deterministic quiescence barrier, so this snapshot
// is exactly H-1 state. EnsureAppV23Root later verifies the bytes from inside H
// and atomically promotes them with appv23:migration_state.
func (s *BadgerStore) PrepareAppV23Migration(scope string, height int64) error {
	if s.txn != nil {
		return errors.New("app-v23 migration preparation must precede the consensus transaction")
	}
	if scope == "" || height <= 0 {
		return errors.New("app-v23 migration preparation requires scope and positive height")
	}
	root, rootErr := s.GetAppV23Root()
	if rootErr != nil {
		return rootErr
	}
	if root != nil {
		if root.Scope != scope {
			return errors.New("persisted app-v23 root scope mismatch")
		}
		migration, migrationErr := s.GetAppV23MigrationState()
		if migrationErr != nil {
			return migrationErr
		}
		if migration != nil {
			return s.ValidateAppV23State()
		}
	}
	agents, agentsErr := s.listAppV23MigrationAgents()
	if agentsErr != nil {
		return agentsErr
	}
	if len(agents) <= appV23MaxInlineMigrationAgents {
		return nil
	}
	if root != nil {
		return s.ensureAppV23BootstrapRosterStaged(root, agents, height, false)
	}
	return s.ensureAppV23LegacyRootStaged(scope, agents, height, false)
}

func (s *BadgerStore) prepareAppV23MigrationPlan(plan *appV23MigrationPlan) error {
	if s.appV23MigrationMu == nil {
		return errors.New("app-v23 migration stage lock is unavailable")
	}
	s.appV23MigrationMu.Lock()
	defer s.appV23MigrationMu.Unlock()
	if err := s.verifyAppV23MigrationStage(plan); err == nil {
		return nil
	}
	if err := s.rebuildAppV23MigrationStage(plan); err != nil {
		return fmt.Errorf("prepare app-v23 migration stage: %w", err)
	}
	if err := s.verifyAppV23MigrationStage(plan); err != nil {
		return fmt.Errorf("verify app-v23 migration stage: %w", err)
	}
	return nil
}

func (s *BadgerStore) rebuildAppV23MigrationStage(plan *appV23MigrationPlan) error {
	// The readiness record is deleted and synced first. A crash anywhere after
	// this point leaves only AppHash-excluded rows and forces an exact rebuild.
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(consensuskeys.AppV23MigrationPrepareKey)
	}); err != nil {
		return err
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("sync app-v23 stage readiness deletion: %w", err)
	}

	var stale [][]byte
	if err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = consensuskeys.AppV23MigrationStagePrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			stale = append(stale, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		return err
	}
	batchNumber := 0
	for start := 0; start < len(stale); start += appV23MigrationStageBatchSize {
		end := start + appV23MigrationStageBatchSize
		if end > len(stale) {
			end = len(stale)
		}
		if err := s.db.Update(func(txn *badger.Txn) error {
			for _, key := range stale[start:end] {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		batchNumber++
		if s.appV23StageFaultHook != nil {
			if err := s.appV23StageFaultHook(batchNumber); err != nil {
				return err
			}
		}
	}
	targets := make([]string, 0, len(plan.stage))
	for target := range plan.stage {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for start := 0; start < len(targets); start += appV23MigrationStageBatchSize {
		end := start + appV23MigrationStageBatchSize
		if end > len(targets) {
			end = len(targets)
		}
		if err := s.db.Update(func(txn *badger.Txn) error {
			for _, target := range targets[start:end] {
				if err := txn.Set(
					consensuskeys.AppV23MigrationStageKey([]byte(target)),
					plan.stage[target],
				); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		batchNumber++
		if s.appV23StageFaultHook != nil {
			if err := s.appV23StageFaultHook(batchNumber); err != nil {
				return err
			}
		}
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("sync app-v23 migration stage: %w", err)
	}
	ready, err := appV23Marshal(plan.preparation)
	if err != nil {
		return err
	}
	if err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(consensuskeys.AppV23MigrationPrepareKey, ready)
	}); err != nil {
		return err
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("sync app-v23 migration readiness: %w", err)
	}
	return nil
}

func (s *BadgerStore) verifyAppV23MigrationStage(plan *appV23MigrationPlan) error {
	var preparation appV23MigrationPreparation
	var stage map[string][]byte
	read := func(txn *badger.Txn) error {
		if err := appV23ReadJSON(txn, consensuskeys.AppV23MigrationPrepareKey, &preparation); err != nil {
			return err
		}
		stage = make(map[string][]byte, preparation.StageCount)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = consensuskeys.AppV23MigrationStagePrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			target, ok := consensuskeys.AppV23MigrationStageTarget(it.Item().Key())
			if !ok {
				return errors.New("malformed app-v23 migration stage key")
			}
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			stage[string(target)] = value
		}
		return nil
	}
	if err := s.db.View(read); err != nil {
		return err
	}
	if preparation != plan.preparation {
		return errors.New("app-v23 migration preparation identity mismatch")
	}
	if len(stage) != plan.preparation.StageCount {
		return errors.New("app-v23 migration stage count mismatch")
	}
	digest, err := appV23StageDigest(stage)
	if err != nil {
		return err
	}
	if digest != plan.preparation.StageDigest {
		return errors.New("app-v23 migration stage digest mismatch")
	}
	return nil
}

func (s *BadgerStore) promoteAppV23MigrationPlan(plan *appV23MigrationPlan) error {
	if s.txn == nil {
		if s.appV23MigrationMu == nil {
			return errors.New("app-v23 migration stage lock is unavailable")
		}
		s.appV23MigrationMu.Lock()
		defer s.appV23MigrationMu.Unlock()
		if migration, err := s.GetAppV23MigrationState(); err != nil {
			return err
		} else if migration != nil {
			return s.ValidateAppV23State()
		}
		if err := s.verifyAppV23MigrationStage(plan); err != nil {
			if err := s.rebuildAppV23MigrationStage(plan); err != nil {
				return fmt.Errorf("prepare app-v23 migration stage: %w", err)
			}
			if err := s.verifyAppV23MigrationStage(plan); err != nil {
				return fmt.Errorf("verify app-v23 migration stage: %w", err)
			}
		}
	} else if err := s.verifyAppV23MigrationStage(plan); err != nil {
		return fmt.Errorf("verify pre-transaction app-v23 migration stage: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if _, err := txn.Get(appV23MigrationStateKey()); err == nil {
			return ErrAppV23RevisionConflict
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		writeJSON := func(key []byte, value any) error {
			data, err := appV23Marshal(value)
			if err != nil {
				return err
			}
			return s.txnSet(txn, key, data)
		}
		if !plan.rootPreexisting {
			if _, err := txn.Get(appV23RootKey()); err == nil {
				return ErrAppV23RevisionConflict
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if err := writeJSON(appV23RootKey(), plan.root); err != nil {
				return err
			}
			if err := s.txnSet(
				txn, appV23RootCredentialKey(plan.root.CredentialID),
				appV23RootCredentialGenerationValue(1),
			); err != nil {
				return err
			}
		}
		activationKeys := make([]string, 0, len(plan.activation))
		for key := range plan.activation {
			activationKeys = append(activationKeys, key)
		}
		sort.Strings(activationKeys)
		for _, key := range activationKeys {
			if err := s.txnSet(txn, []byte(key), plan.activation[key]); err != nil {
				return err
			}
		}
		// The preparation record is only a crash-recovery sidecar. Prune it in
		// the same transaction as the activation marker so a pre-commit crash
		// retains readiness for exact replay, while a committed activation
		// cannot leave obsolete local lifecycle state behind.
		if err := s.txnDelete(txn, consensuskeys.AppV23MigrationPrepareKey); err != nil {
			return err
		}
		// This is the sole visibility edge: before this write every stage row
		// is local/AppHash-excluded; after commit the exact StageDigest set is
		// consensus state. Never repair stage bytes once this marker exists.
		return writeJSON(appV23MigrationStateKey(), plan.migration)
	})
}

// reconcileAppV23BootstrapRoster handles fresh vendored chains whose root and
// Companion were atomically seeded at genesis but which registered additional
// agents before app-v23 activation. Existing bootstrap principals are preserved
// byte-for-byte; every other registered principal receives a deterministic
// migration disposition and all records enter one digest-validated ledger.
func (s *BadgerStore) reconcileAppV23BootstrapRoster(root *AppV23RootState, height int64) error {
	agents, agentsErr := s.listAppV23MigrationAgents()
	if agentsErr != nil {
		return agentsErr
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].AgentID < agents[j].AgentID })
	owned := make(map[string][]string)
	domainOwners := make(map[string]string)
	if viewErr := s.view(func(txn *badger.Txn) error {
		prefix := []byte("domain:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			name := string(it.Item().Key()[len(prefix):])
			shared, sharedErr := appV23DomainIsSharedTxn(txn, name)
			if sharedErr != nil {
				return sharedErr
			}
			if shared {
				continue
			}
			var owner string
			if valueErr := it.Item().Value(func(value []byte) error {
				var decodeErr error
				owner, _, decodeErr = decodeString(value, 0)
				return decodeErr
			}); valueErr != nil {
				return valueErr
			}
			domainOwners[name] = owner
			if owner != "" {
				owned[owner] = append(owned[owner], name)
			}
		}
		return nil
	}); viewErr != nil {
		return viewErr
	}
	for id := range owned {
		sort.Strings(owned[id])
	}
	grantHolders, grantsErr := s.appV23MigrationGrantHolders()
	if grantsErr != nil {
		return grantsErr
	}
	sharedNames, sharedErr := s.appV23MigrationSharedDomainNames()
	if sharedErr != nil {
		return sharedErr
	}
	legacyRead, legacyFederations, baselineErr := s.appV23MigrationReadBaselines(agents)
	if baselineErr != nil {
		return baselineErr
	}

	dispositions := make([]AppV23MigrationDisposition, 0, len(agents))
	enrollments := make(map[string]AppV23LocalEnrollment)
	roles := make(map[string]AppV23RoleState)
	projected := make(map[string]OnChainAgent)
	newDomains := make(map[string]string)
	legacyAdmins := []string{root.PrincipalID}
	for _, agent := range agents {
		if agent.AgentID != root.PrincipalID && agent.Role == AppV23RoleAdmin {
			// Preserve the complete pre-v23 Admin roster for immutable audit,
			// while requiring every non-Root historical Admin to pass the
			// app-v23 same-machine promotion ceremony.
			legacyAdmins = append(legacyAdmins, agent.AgentID)
		}
	}
	for _, legacy := range agents {
		legacyJSON, marshalErr := json.Marshal(legacy)
		if marshalErr != nil {
			return marshalErr
		}
		sum := sha256.Sum256(legacyJSON)
		disposition := AppV23MigrationDisposition{
			AgentID: legacy.AgentID, LegacyPolicyDigest: hex.EncodeToString(sum[:]),
		}
		existingEnrollment, err := s.GetAppV23Enrollment(legacy.AgentID)
		if err != nil {
			return err
		}
		existingRole, err := s.GetAppV23Role(legacy.AgentID)
		if err != nil {
			return err
		}
		if existingEnrollment != nil || existingRole != nil {
			if existingEnrollment == nil || existingRole == nil ||
				ValidateAppV23EnrollmentPolicy(
					existingRole.Role, existingEnrollment.Profile,
					existingEnrollment.Capabilities, existingEnrollment.Clearance,
					existingEnrollment.Active,
				) != nil {
				return fmt.Errorf("bootstrap principal %s has incomplete app-v23 policy", legacy.AgentID)
			}
			disposition.Disposition = "bootstrap_preserved"
			disposition.Profile = existingEnrollment.Profile
			disposition.HomeDomain = existingEnrollment.HomeDomain
			disposition.Active = existingEnrollment.Active
			dispositions = append(dispositions, disposition)
			continue
		}

		next := legacy
		next.Role = AppV23RoleMember
		var profile string
		active := true
		home := ""
		legacyAdminReview := legacy.Role == AppV23RoleAdmin
		switch legacy.Role {
		case "observer":
			profile = AppV23ProfileReadOnly
			next.Capabilities = AgentCapabilityReadAllDomains |
				(legacy.Capabilities & AgentCapabilityDenyFederatedPipe)
			disposition.Disposition = "observer_read_only"
		default:
			_, hasExplicitGrant := grantHolders[legacy.AgentID]
			hasReviewEvidence := appV23LegacyHasReviewEvidence(
				legacy, legacyRead[legacy.AgentID], hasExplicitGrant,
			)
			migrated, migrateErr := appV23MigrateLegacyMember(
				legacy, legacyAdminReview, hasReviewEvidence,
				owned, domainOwners, newDomains, sharedNames,
			)
			if migrateErr != nil {
				return fmt.Errorf("migrate bootstrap roster agent %s: %w", legacy.AgentID, migrateErr)
			}
			next.Role = migrated.Role
			next.Clearance = migrated.Clearance
			next.Capabilities = migrated.Capabilities
			profile = migrated.Profile
			active = migrated.Active
			home = migrated.HomeDomain
			disposition.Disposition = migrated.Disposition
		}
		enrollment := AppV23LocalEnrollment{
			AgentID: legacy.AgentID, ApprovedBy: root.CredentialID,
			RootGeneration: root.Generation, Profile: profile, HomeDomain: home,
			Clearance: next.Clearance, Capabilities: next.Capabilities,
			Active: active, Revision: 1, UpdatedHeight: height,
		}
		if err := ValidateAppV23EnrollmentPolicy(
			next.Role, profile, next.Capabilities, next.Clearance, active,
		); err != nil {
			return fmt.Errorf("bootstrap roster agent %s has incompatible migration policy: %w", legacy.AgentID, err)
		}
		disposition.Profile, disposition.HomeDomain, disposition.Active = profile, home, active
		dispositions = append(dispositions, disposition)
		enrollments[legacy.AgentID] = enrollment
		roles[legacy.AgentID] = AppV23RoleState{
			AgentID: legacy.AgentID, Role: next.Role, Revision: 1,
			UpdatedBy: root.CredentialID, UpdatedHeight: height,
		}
		projected[legacy.AgentID] = next
	}
	sort.Strings(legacyAdmins)
	manifestJSON, err := json.Marshal(dispositions)
	if err != nil {
		return err
	}
	manifestSum := sha256.Sum256(manifestJSON)
	migration := AppV23MigrationState{
		SchemaDigest:          appV23MigrationSchemaDigest(),
		ManifestDigest:        hex.EncodeToString(manifestSum[:]),
		RootBootstrapDigest:   root.BootstrapDigest,
		AgentCount:            len(dispositions),
		LegacyReadCount:       len(legacyRead),
		LegacyFederationCount: len(legacyFederations),
		Height:                height,
	}
	appV23SetLegacyAdminSummary(&migration, legacyAdmins)
	return s.update(func(txn *badger.Txn) error {
		if _, err := txn.Get(appV23MigrationStateKey()); err == nil {
			return ErrAppV23RevisionConflict
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		writeJSON := func(key []byte, value any) error {
			data, err := appV23Marshal(value)
			if err != nil {
				return err
			}
			return s.txnSet(txn, key, data)
		}
		if err := writeJSON(appV23MigrationStateKey(), migration); err != nil {
			return err
		}
		for id, baseline := range legacyRead {
			if err := writeJSON(appV23LegacyReadKey(id), baseline); err != nil {
				return err
			}
		}
		for id, baseline := range legacyFederations {
			if err := writeJSON(appV23LegacyFederationKey(id), baseline); err != nil {
				return err
			}
		}
		for _, disposition := range dispositions {
			id := disposition.AgentID
			if _, exists := enrollments[id]; exists {
				if err := writeJSON(agentOnChainKey(id), projected[id]); err != nil {
					return err
				}
				if err := writeJSON(appV23EnrollmentKey(id), enrollments[id]); err != nil {
					return err
				}
				if err := writeJSON(appV23RoleKey(id), roles[id]); err != nil {
					return err
				}
				if roles[id].Role == AppV23RoleAdmin {
					if err := s.txnSet(txn, appV23AdminKey(id), []byte{1}); err != nil {
						return err
					}
				}
				// The owned home domain itself confers authority. Migration does
				// not synthesize a redundant explicit grant.
			}
			if err := writeJSON(appV23MigrationKey(id), disposition); err != nil {
				return err
			}
		}
		for domain, owner := range newDomains {
			if _, exists := domainOwners[domain]; !exists {
				if err := s.txnSet(txn, domainKey(domain), appV23EncodeDomain(owner, height)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *BadgerStore) ApproveAppV23LocalAgent(
	input AppV23LocalEnrollment,
	role string,
	expectedRevision, expectedRoleRevision uint64,
	elevation ...*AppV23ElevationUse,
) error {
	if err := validateCanonicalAgentID("agent_id", input.AgentID); err != nil {
		return err
	}
	if input.Profile == AppV23ProfileLegacyRestricted {
		return errors.New("legacy-restricted profile is migration-only; select a named app-v23 profile")
	}
	if !ValidAppV23Profile(input.Profile) || input.Profile == AppV23ProfileRoot {
		return fmt.Errorf("invalid local enrollment profile %q", input.Profile)
	}
	if !input.Active {
		role = AppV23RoleMember
	}
	if err := ValidateAppV23EnrollmentPolicy(
		role, input.Profile, input.Capabilities, input.Clearance, input.Active,
	); err != nil {
		return err
	}
	if input.Active && input.Profile == AppV23ProfileReadOnly && input.HomeDomain != "" {
		return errors.New("active Read-only enrollment must not retain a home domain")
	}
	if input.HomeDomain != "" {
		if err := ValidateAppV23DomainName(input.HomeDomain); err != nil {
			return fmt.Errorf("invalid app-v23 home domain: %w", err)
		}
	}
	if input.Active && input.Profile != AppV23ProfileReadOnly &&
		(input.HomeDomain == "" || IsSharedDomainName(input.HomeDomain)) {
		return errors.New("active local enrollment requires a non-shared home domain")
	}
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			if len(elevation) > 1 {
				return errors.New("multiple app-v23 elevation proofs")
			}
			if len(elevation) == 1 {
				if err := s.consumeAppV23ElevationTxn(txn, elevation[0], input.UpdatedHeight); err != nil {
					return err
				}
			}
			var root AppV23RootState
			if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
				return err
			}
			if input.AgentID == root.PrincipalID {
				return errors.New("CEREBRUM root enrollment is immutable")
			}
			if input.Active && input.Profile != AppV23ProfileReadOnly {
				shared, err := appV23DomainIsSharedTxn(txn, input.HomeDomain)
				if err != nil {
					return err
				}
				if shared {
					return errors.New("active local enrollment requires a non-shared home domain")
				}
			}
			var agent OnChainAgent
			if projectionErr := s.appV23ReadEffectiveJSONTxn(txn, appV23ProjectedAgentKey(input.AgentID), &agent); errors.Is(projectionErr, badger.ErrKeyNotFound) {
				if readErr := appV23ReadJSON(txn, agentOnChainKey(input.AgentID), &agent); readErr != nil {
					return fmt.Errorf("approved agent is not registered: %w", readErr)
				}
			} else if projectionErr != nil {
				return fmt.Errorf("approved agent is not registered: %w", projectionErr)
			}
			var current AppV23LocalEnrollment
			currentErr := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(input.AgentID), &current)
			switch {
			case errors.Is(currentErr, badger.ErrKeyNotFound):
				if expectedRevision != 0 {
					return ErrAppV23RevisionConflict
				}
			case currentErr != nil:
				return currentErr
			case current.Revision != expectedRevision:
				return ErrAppV23RevisionConflict
			}
			var currentRole AppV23RoleState
			roleErr := s.appV23ReadEffectiveJSONTxn(txn, appV23RoleKey(input.AgentID), &currentRole)
			switch {
			case errors.Is(roleErr, badger.ErrKeyNotFound):
				if expectedRoleRevision != 0 {
					return ErrAppV23RevisionConflict
				}
			case roleErr != nil:
				return roleErr
			case currentRole.Revision != expectedRoleRevision:
				return ErrAppV23RevisionConflict
			}
			if role == AppV23RoleAdmin {
				count, countErr := s.countAppV23AdminsTxn(txn, AppV23MaxAdmins)
				if countErr != nil {
					return countErr
				}
				if currentRole.Role != AppV23RoleAdmin && count >= AppV23MaxAdmins {
					return fmt.Errorf("app-v23 admin limit %d reached", AppV23MaxAdmins)
				}
			}
			homeExists := false
			homeOwner := ""
			homeParent := ""
			if input.Active && input.Profile != AppV23ProfileReadOnly {
				if value, getErr := s.appV23ReadEffectiveValueTxn(txn, domainKey(input.HomeDomain)); getErr == nil {
					homeExists = true
					var decodeErr error
					var off int
					homeOwner, off, decodeErr = decodeString(value, 0)
					if decodeErr != nil {
						return decodeErr
					}
					homeParent, _, decodeErr = decodeString(value, off)
					if decodeErr != nil {
						return decodeErr
					}
					if homeOwner != input.AgentID {
						if !input.TransferHomeDomain || input.ExpectedHomeDomainOwner != homeOwner {
							return fmt.Errorf("home domain ownership compare-and-swap failed")
						}
						if err := s.appV23ValidateHomeDomainReleaseTxn(
							txn, input.HomeDomain, homeOwner, input.AgentID, false,
						); err != nil {
							return err
						}
					} else if input.TransferHomeDomain {
						return errors.New("home domain transfer target already owns domain")
					}
				} else if errors.Is(getErr, badger.ErrKeyNotFound) {
					if input.TransferHomeDomain || input.ExpectedHomeDomainOwner != "" {
						return fmt.Errorf("home domain ownership compare-and-swap failed")
					}
				} else {
					return getErr
				}
			}

			input.Revision = expectedRevision + 1
			data, marshalErr := appV23Marshal(input)
			if marshalErr != nil {
				return marshalErr
			}
			if err := s.txnSet(txn, appV23EnrollmentKey(input.AgentID), data); err != nil {
				return err
			}
			agent.Clearance = input.Clearance
			agent.Capabilities = input.Capabilities
			agent.Role = role
			nextRole := AppV23RoleState{
				AgentID: input.AgentID, Role: role,
				Revision: expectedRoleRevision + 1, UpdatedBy: input.ApprovedBy,
				UpdatedHeight: input.UpdatedHeight,
			}
			roleData, roleMarshalErr := appV23Marshal(nextRole)
			if roleMarshalErr != nil {
				return roleMarshalErr
			}
			if err := s.txnSet(txn, appV23RoleKey(input.AgentID), roleData); err != nil {
				return err
			}
			if role == AppV23RoleAdmin {
				if err := s.txnSet(txn, appV23AdminKey(input.AgentID), []byte{1}); err != nil {
					return err
				}
			} else if err := s.txnDelete(txn, appV23AdminKey(input.AgentID)); err != nil {
				return err
			}
			if !input.Active {
				if err := s.removeAppV23MemberFromGroupTxn(txn, input.AgentID, input.ApprovedBy, input.UpdatedHeight); err != nil {
					return err
				}
			}
			agentData, agentMarshalErr := appV23Marshal(agent)
			if agentMarshalErr != nil {
				return agentMarshalErr
			}
			if err := s.txnSet(txn, appV23ProjectedAgentKey(input.AgentID), agentData); err != nil {
				return err
			}
			if input.Active && input.Profile != AppV23ProfileReadOnly {
				if !homeExists || input.TransferHomeDomain {
					if input.TransferHomeDomain {
						prefix := []byte("grant:" + input.HomeDomain + ":")
						opts := badger.DefaultIteratorOptions
						opts.Prefix = prefix
						opts.PrefetchValues = false
						it := txn.NewIterator(opts)
						var grantKeys [][]byte
						for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
							key := it.Item().Key()
							keyDomain, _, canonical := parseCanonicalGrantKey(key)
							if !canonical || keyDomain != input.HomeDomain {
								continue
							}
							grantKeys = append(grantKeys, append([]byte(nil), key...))
						}
						it.Close()
						for _, key := range grantKeys {
							if err := s.txnDelete(txn, key); err != nil {
								return err
							}
						}
					}
					if err := s.txnSet(
						txn, domainKey(input.HomeDomain),
						appV23EncodeDomainWithParent(input.AgentID, homeParent, input.UpdatedHeight),
					); err != nil {
						return err
					}
				}
				// Home ownership is sufficient. Explicit grants remain a
				// separate access-control operation.
			}
			return nil
		})
	})
}

func (s *BadgerStore) SetAppV23Policy(
	actorID, targetID, role, expectedProfile, profile string,
	clearance uint8,
	capabilities AgentCapabilities,
	expectedRoleRevision, enrollmentRevision uint64,
	height int64,
	elevation ...*AppV23ElevationUse,
) error {
	if profile == AppV23ProfileLegacyRestricted {
		return errors.New("legacy-restricted profile is migration-only; select a named app-v23 profile")
	}
	if err := ValidateAppV23Policy(role, profile, capabilities, clearance); err != nil {
		return err
	}
	if profile == AppV23ProfileRoot {
		// ValidateAppV23Policy must accept the singleton Root record during
		// bootstrap/restore validation. Ordinary policy mutation is a different
		// authority boundary: Root is non-delegable and cannot be minted by
		// promoting an agent into an otherwise structurally valid root profile.
		return errors.New("root profile is reserved for the CEREBRUM singleton")
	}
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			if len(elevation) > 1 {
				return errors.New("multiple app-v23 elevation proofs")
			}
			if len(elevation) == 1 {
				if err := s.consumeAppV23ElevationTxn(txn, elevation[0], height); err != nil {
					return err
				}
			}
			var root AppV23RootState
			if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
				return err
			}
			if targetID == root.PrincipalID {
				return errors.New("CEREBRUM root role is immutable")
			}
			var enrollment AppV23LocalEnrollment
			if err := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(targetID), &enrollment); err != nil {
				return ErrAppV23NeedsApproval
			}
			if !enrollment.Active || enrollment.Revision != enrollmentRevision {
				return ErrAppV23NeedsApproval
			}
			if enrollment.Profile != expectedProfile {
				return ErrAppV23RevisionConflict
			}
			if profile != AppV23ProfileReadOnly {
				shared, err := appV23DomainIsSharedTxn(txn, enrollment.HomeDomain)
				if err != nil {
					return err
				}
				if enrollment.HomeDomain == "" || shared {
					return errors.New("non-read-only policy requires an owned non-shared home domain")
				}
				value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(enrollment.HomeDomain))
				if err != nil {
					return err
				}
				var homeOwner string
				var decodeErr error
				homeOwner, _, decodeErr = decodeString(value, 0)
				if decodeErr != nil {
					return decodeErr
				}
				if homeOwner != targetID {
					return errors.New("non-read-only policy home domain is not owned by target agent")
				}
			}
			var current AppV23RoleState
			currentErr := s.appV23ReadEffectiveJSONTxn(txn, appV23RoleKey(targetID), &current)
			switch {
			case errors.Is(currentErr, badger.ErrKeyNotFound):
				if expectedRoleRevision != 0 {
					return ErrAppV23RevisionConflict
				}
			case currentErr != nil:
				return currentErr
			case current.Revision != expectedRoleRevision:
				return ErrAppV23RevisionConflict
			}
			if role == AppV23RoleAdmin {
				count, countErr := s.countAppV23AdminsTxn(txn, AppV23MaxAdmins)
				if countErr != nil {
					return countErr
				}
				if current.Role != AppV23RoleAdmin && count >= AppV23MaxAdmins {
					return fmt.Errorf("app-v23 admin limit %d reached", AppV23MaxAdmins)
				}
			}
			var agent OnChainAgent
			if projectionErr := s.appV23ReadEffectiveJSONTxn(txn, appV23ProjectedAgentKey(targetID), &agent); errors.Is(projectionErr, badger.ErrKeyNotFound) {
				if readErr := appV23ReadJSON(txn, agentOnChainKey(targetID), &agent); readErr != nil {
					return readErr
				}
			} else if projectionErr != nil {
				return projectionErr
			}
			agent.Role = role
			agent.Clearance = clearance
			agent.Capabilities = capabilities
			enrollment.Profile = profile
			enrollment.Clearance = clearance
			enrollment.Capabilities = capabilities
			if profile == AppV23ProfileReadOnly {
				// Read-only is a consent boundary, not a reversible role toggle.
				// Keep any existing domain ownership intact, but drop its special
				// home binding so leaving Read-only must return through tx36 with
				// the target key's fresh signature.
				enrollment.HomeDomain = ""
			}
			enrollment.Revision++
			enrollment.ApprovedBy = actorID
			if actorID == root.CredentialID {
				enrollment.RootGeneration = root.Generation
			}
			enrollment.UpdatedHeight = height
			enrollmentData, enrollmentMarshalErr := appV23Marshal(enrollment)
			if enrollmentMarshalErr != nil {
				return enrollmentMarshalErr
			}
			if err := s.txnSet(txn, appV23EnrollmentKey(targetID), enrollmentData); err != nil {
				return err
			}
			agentData, agentMarshalErr := appV23Marshal(agent)
			if agentMarshalErr != nil {
				return agentMarshalErr
			}
			if err := s.txnSet(txn, appV23ProjectedAgentKey(targetID), agentData); err != nil {
				return err
			}
			if role == AppV23RoleAdmin {
				if err := s.txnSet(txn, appV23AdminKey(targetID), []byte{1}); err != nil {
					return err
				}
			} else if err := s.txnDelete(txn, appV23AdminKey(targetID)); err != nil {
				return err
			}
			next := AppV23RoleState{
				AgentID: targetID, Role: role, Revision: expectedRoleRevision + 1,
				UpdatedBy: actorID, UpdatedHeight: height,
			}
			roleData, roleMarshalErr := appV23Marshal(next)
			if roleMarshalErr != nil {
				return roleMarshalErr
			}
			return s.txnSet(txn, appV23RoleKey(targetID), roleData)
		})
	})
}

func (s *BadgerStore) countAppV23AdminsTxn(txn *badger.Txn, limit int) (int, error) {
	prefix := []byte("appv23:admin:")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
		if count > limit {
			return count, nil
		}
	}
	return count, nil
}

func validateAppV23GroupID(groupID string) error {
	if len(groupID) == 0 || len(groupID) > 64 {
		return errors.New("group_id length must be 1..64")
	}
	for i := range groupID {
		c := groupID[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' {
			return errors.New("group_id must contain only lowercase letters, digits, '-', '_' or '.'")
		}
	}
	return nil
}

func (s *BadgerStore) MutateAppV23AccessGroup(actorID, groupID, name string, members []string, expectedRevision uint64, deleteGroup bool, height int64, elevation ...*AppV23ElevationUse) error {
	if err := validateAppV23GroupID(groupID); err != nil {
		return err
	}
	if len(name) > 128 || len(members) > AppV23MaxGroupMembers {
		return errors.New("app-v23 access group exceeds deterministic bounds")
	}
	if !sort.StringsAreSorted(members) {
		return errors.New("app-v23 access group members must be sorted")
	}
	for i, member := range members {
		if err := validateCanonicalAgentID("group member", member); err != nil {
			return err
		}
		if i > 0 && members[i-1] == member {
			return errors.New("app-v23 access group members must be unique")
		}
	}
	if deleteGroup && (name != "" || len(members) != 0) {
		return errors.New("deleted access group must have empty name and members")
	}

	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			if len(elevation) > 1 {
				return errors.New("multiple app-v23 elevation proofs")
			}
			if len(elevation) == 1 {
				if err := s.consumeAppV23ElevationTxn(txn, elevation[0], height); err != nil {
					return err
				}
			}
			var current AppV23AccessGroup
			currentErr := appV23ReadJSON(txn, appV23GroupKey(groupID), &current)
			creating := false
			switch {
			case errors.Is(currentErr, badger.ErrKeyNotFound):
				if expectedRevision != 0 {
					return ErrAppV23RevisionConflict
				}
				if deleteGroup {
					return ErrAppV23RevisionConflict
				}
				creating = true
			case currentErr != nil:
				return currentErr
			case current.Revision != expectedRevision:
				return ErrAppV23RevisionConflict
			}
			if creating {
				count, countErr := countAppV23PrefixTxn(
					txn, []byte("appv23:group:"), AppV23MaxGroups,
				)
				if countErr != nil {
					return countErr
				}
				if count >= AppV23MaxGroups {
					return errors.New("app-v23 global access group limit reached")
				}
			}
			linkCount, countErr := countAppV23PrefixTxn(
				txn, []byte("appv23:member_group:"), AppV23MaxMembershipLinks,
			)
			if countErr != nil {
				return countErr
			}
			if linkCount > AppV23MaxMembershipLinks || len(current.Members) > linkCount {
				return errors.New("app-v23 membership link index is inconsistent")
			}
			nextMemberCount := len(members)
			if deleteGroup {
				nextMemberCount = 0
			}
			if linkCount-len(current.Members)+nextMemberCount > AppV23MaxMembershipLinks {
				return errors.New("app-v23 global membership link limit reached")
			}
			var root AppV23RootState
			if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
				return err
			}
			for _, member := range members {
				if member == root.PrincipalID {
					return errors.New("CEREBRUM root cannot be an Access Group member")
				}
				var enrollment AppV23LocalEnrollment
				if err := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(member), &enrollment); err != nil || !enrollment.Active {
					return fmt.Errorf("group member %s needs active local approval", member)
				}
				if sort.SearchStrings(current.Members, member) >= len(current.Members) ||
					current.Members[sort.SearchStrings(current.Members, member)] != member {
					count, countErr := s.countAppV23AgentGroupsTxn(txn, member, AppV23MaxGroupsPerAgent)
					if countErr != nil {
						return countErr
					}
					if count >= AppV23MaxGroupsPerAgent {
						return fmt.Errorf("group member %s reached the per-agent group limit", member)
					}
				}
			}
			for _, member := range current.Members {
				if err := s.txnDelete(txn, appV23MemberGroupKey(member, groupID)); err != nil {
					return err
				}
			}
			if deleteGroup {
				return s.txnDelete(txn, appV23GroupKey(groupID))
			}
			next := AppV23AccessGroup{
				GroupID: groupID, Name: name, Members: append([]string(nil), members...),
				Revision: expectedRevision + 1, UpdatedBy: actorID, UpdatedHeight: height,
			}
			data, marshalErr := appV23Marshal(next)
			if marshalErr != nil {
				return marshalErr
			}
			if err := s.txnSet(txn, appV23GroupKey(groupID), data); err != nil {
				return err
			}
			for _, member := range members {
				if err := s.txnSet(txn, appV23MemberGroupKey(member, groupID), []byte{1}); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func countAppV23PrefixTxn(txn *badger.Txn, prefix []byte, limit int) (int, error) {
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
		if count > limit {
			return count, nil
		}
	}
	return count, nil
}

func (s *BadgerStore) countAppV23AgentGroupsTxn(txn *badger.Txn, agentID string, limit int) (int, error) {
	prefix := appV23MemberGroupPrefix(agentID)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
		if count > limit {
			return count, nil
		}
	}
	return count, nil
}

func (s *BadgerStore) removeAppV23MemberFromGroupTxn(txn *badger.Txn, agentID, actorID string, height int64) error {
	prefix := appV23MemberGroupPrefix(agentID)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	groupIDs := make([]string, 0, 2)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if len(groupIDs) == AppV23MaxGroupsPerAgent {
			it.Close()
			return errors.New("app-v23 per-agent group index exceeds deterministic bound")
		}
		groupIDs = append(groupIDs, string(it.Item().Key()[len(prefix):]))
	}
	it.Close()
	for _, groupID := range groupIDs {
		var group AppV23AccessGroup
		if err := appV23ReadJSON(txn, appV23GroupKey(groupID), &group); err != nil {
			return err
		}
		index := sort.SearchStrings(group.Members, agentID)
		if index < len(group.Members) && group.Members[index] == agentID {
			group.Members = append(group.Members[:index], group.Members[index+1:]...)
			group.Revision++
			group.UpdatedBy = actorID
			group.UpdatedHeight = height
			data, err := appV23Marshal(group)
			if err != nil {
				return err
			}
			if err := s.txnSet(txn, appV23GroupKey(groupID), data); err != nil {
				return err
			}
		}
		if err := s.txnDelete(txn, appV23MemberGroupKey(agentID, groupID)); err != nil {
			return err
		}
	}
	return nil
}

func (s *BadgerStore) AuthorizeAppV23LocalDomain(agentID, domain string, verb AppV23DomainVerb, shared bool) (AppV23Authorization, error) {
	policyID := agentID
	if root, err := s.GetAppV23Root(); err != nil {
		return AppV23Authorization{}, err
	} else if root != nil {
		if agentID == root.CredentialID {
			policyID = root.PrincipalID
		} else {
			wasRoot, markerErr := s.IsAppV23RootCredential(agentID)
			if markerErr != nil {
				return AppV23Authorization{}, markerErr
			}
			if wasRoot {
				return AppV23Authorization{Reason: ErrAppV23NeedsApproval.Error()}, nil
			}
		}
	}
	return s.authorizeAppV23LocalDomainPolicy(policyID, domain, verb, shared)
}

// AuthorizeAppV23PolicyPrincipalDomain evaluates an immutable, consensus-held
// policy principal. It is for deterministic state enumeration only (for
// example, freezing a challenge electorate). Request/credential authorization
// must use AuthorizeAppV23LocalDomain so retired Root credentials stay denied.
func (s *BadgerStore) AuthorizeAppV23PolicyPrincipalDomain(policyID, domain string, verb AppV23DomainVerb, shared bool) (AppV23Authorization, error) {
	return s.authorizeAppV23LocalDomainPolicy(policyID, domain, verb, shared)
}

// authorizeAppV23LocalDomainPolicy evaluates an immutable policy principal.
// It is deliberately unexported: credential-bearing callers must use
// AuthorizeAppV23LocalDomain so an obsolete Root credential cannot be
// confused with Root's stable policy identity.
func (s *BadgerStore) authorizeAppV23LocalDomainPolicy(policyID, domain string, verb AppV23DomainVerb, shared bool) (AppV23Authorization, error) {
	agent, err := s.GetRegisteredAgent(policyID)
	if err != nil {
		return AppV23Authorization{}, err
	}
	enrollment, err := s.GetAppV23Enrollment(policyID)
	if err != nil {
		return AppV23Authorization{}, err
	}
	if enrollment == nil || !enrollment.Active {
		return AppV23Authorization{Reason: ErrAppV23NeedsApproval.Error()}, nil
	}
	role, err := s.GetAppV23Role(policyID)
	if err != nil {
		return AppV23Authorization{}, err
	}
	if root, rootErr := s.GetAppV23Root(); rootErr != nil {
		return AppV23Authorization{}, rootErr
	} else if root != nil &&
		policyID != root.PrincipalID &&
		role != nil && role.Role == AppV23RoleAdmin &&
		enrollment.RootGeneration != root.Generation {
		return AppV23Authorization{
			ExplicitDeny: true,
			Reason:       "delegated Admin approval belongs to an obsolete Root generation",
		}, nil
	}
	if role == nil || role.Role != agent.Role ||
		agent.Clearance != enrollment.Clearance ||
		agent.Capabilities != enrollment.Capabilities ||
		ValidateAppV23Policy(role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance) != nil {
		return AppV23Authorization{ExplicitDeny: true, Reason: "incompatible app-v23 role/profile state"}, nil
	}
	if verb >= AppV23VerbWrite && enrollment.Profile == AppV23ProfileReadOnly {
		return AppV23Authorization{ExplicitDeny: true, Reason: "read-only profile denies mutation"}, nil
	}
	// app-v25 restores only authority proven by an exact, validator-attested
	// historical (agent, domain) pair. The entitlement itself is revision-bound,
	// so a later explicit policy, owner, or recovered-group change revokes it.
	// An unchanged historical mask-2/mask-8 still applies everywhere else.
	if verb <= AppV23VerbModify {
		var restored bool
		var restoreErr error
		if verb == AppV23VerbModify {
			restored, restoreErr = s.AppV25AllowsHistoricalDomainModify(policyID, domain)
		} else {
			restored, restoreErr = s.AppV25AllowsHistoricalDomainWrite(policyID, domain)
		}
		if restoreErr != nil {
			return AppV23Authorization{}, restoreErr
		}
		if restored {
			return AppV23Authorization{Allowed: true}, nil
		}
	}
	if verb >= AppV23VerbWrite && shared && enrollment.Capabilities.Has(AgentCapabilityDenySharedDomainWrite) {
		return AppV23Authorization{ExplicitDeny: true, Reason: "profile denies shared-domain writes"}, nil
	}

	owner, _, err := s.ResolveAppV23OwningAncestor(domain)
	if err != nil {
		return AppV23Authorization{}, err
	}
	if verb == AppV23VerbRead && enrollment.Capabilities.Has(AgentCapabilityReadAllDomains) {
		return AppV23Authorization{Allowed: true}, nil
	}
	if role.Role == AppV23RoleAdmin {
		return AppV23Authorization{Allowed: true}, nil
	}
	if shared {
		recoveredGroup, recoveredGroupErr :=
			s.AuthorizeAppV25RecoveredGroupDomain(policyID, domain, verb)
		if recoveredGroupErr != nil {
			return AppV23Authorization{}, recoveredGroupErr
		}
		if recoveredGroup {
			return AppV23Authorization{Allowed: true}, nil
		}
		if verb == AppV23VerbWrite {
			grandfathered, grandfatherErr :=
				s.AppV23AllowsGrandfatheredSharedDomainWrite(policyID, domain)
			if grandfatherErr != nil {
				return AppV23Authorization{}, grandfatherErr
			}
			if grandfathered {
				return AppV23Authorization{Allowed: true}, nil
			}
		}
		// Shared resources have no effective owner or group-derived authority,
		// including dynamically promoted names that retain a historical domain
		// row. Explicit exact grants are evaluated by the consensus caller.
		return AppV23Authorization{}, nil
	}
	if owner == "" {
		return AppV23Authorization{}, nil
	}
	if verb >= AppV23VerbWrite && owner != policyID &&
		enrollment.Capabilities.Has(AgentCapabilityDenyForeignDomainWrite) {
		legacyModify := false
		if verb == AppV23VerbModify {
			legacyModify, err = s.AppV23AllowsLegacyForeignModify(policyID)
			if err != nil {
				return AppV23Authorization{}, err
			}
		}
		if !legacyModify {
			return AppV23Authorization{ExplicitDeny: true, Reason: "profile denies foreign-domain mutation"}, nil
		}
	}
	if owner == policyID {
		return AppV23Authorization{Allowed: true}, nil
	}
	groups, err := s.ListAppV23AgentGroups(policyID)
	if err != nil {
		return AppV23Authorization{}, err
	}
	if len(groups) == 0 {
		return AppV23Authorization{}, nil
	}
	sharesGroup := false
	for _, group := range groups {
		i := sort.SearchStrings(group.Members, owner)
		if i < len(group.Members) && group.Members[i] == owner {
			sharesGroup = true
			break
		}
	}
	if !sharesGroup {
		return AppV23Authorization{}, nil
	}
	switch role.Role {
	case AppV23RoleMember:
		return AppV23Authorization{Allowed: verb == AppV23VerbRead}, nil
	case AppV23RoleManager:
		return AppV23Authorization{Allowed: verb >= AppV23VerbRead && verb <= AppV23VerbModify}, nil
	default:
		return AppV23Authorization{}, nil
	}
}

// HasAppV23AccessOrAncestor is the fork-scoped grant lookup for app-v23 data
// policy. Legacy HasAccessOrAncestor intentionally treats every shared-domain
// name as a barrier, including the exact requested name; changing that helper
// would rewrite pre-v23 authorization and challenge electorates. App-v23 keeps
// the ancestor barrier but permits an exact grant on the shared resource.
func (s *BadgerStore) HasAppV23AccessOrAncestor(
	domain, principalID string,
	requiredLevel uint8,
	blockTime time.Time,
	shared bool,
) (bool, error) {
	if shared {
		return s.HasAccess(domain, principalID, requiredLevel, blockTime)
	}
	segments := splitDomainSegments(domain)
	if len(segments) == 0 || len(segments) > 16 {
		return false, nil
	}
	now := blockTime.Unix()
	allowed := false
	err := s.view(func(txn *badger.Txn) error {
		for i := len(segments); i >= 1; i-- {
			candidate := strings.Join(segments[:i], ".")
			candidateShared, sharedErr := appV23DomainIsSharedTxn(txn, candidate)
			if sharedErr != nil {
				return sharedErr
			}
			if candidateShared {
				// Exact shared grants are handled by the shared branch above.
				// A shared ancestor stops inheritance from every broader name.
				return nil
			}
			item, getErr := txn.Get(grantKey(candidate, principalID))
			switch {
			case errors.Is(getErr, badger.ErrKeyNotFound):
				continue
			case getErr != nil:
				return getErr
			}
			if valueErr := item.Value(func(value []byte) error {
				if len(value) < 9 {
					return errors.New("invalid grant entry")
				}
				level := value[0]
				expiresAt := int64(binary.BigEndian.Uint64(value[1:9])) // #nosec G115 -- expiry timestamp fits in int64
				allowed = level >= requiredLevel && (expiresAt == 0 || now < expiresAt)
				return nil
			}); valueErr != nil {
				return valueErr
			}
			if allowed {
				return nil
			}
		}
		return nil
	})
	return allowed, err
}

// AppV23ModifyVerbHoldersUpTo is the fork-scoped bounded modify electorate.
// It mirrors HasAppV23AccessOrAncestor's consensus-state-aware shared-domain
// barrier. A shared resource has no owner or inheritable ancestors, but an
// exact level-3 grant on that resource remains effective and is frozen into
// the electorate.
func (s *BadgerStore) AppV23ModifyVerbHoldersUpTo(
	domain string,
	shared bool,
	blockTime time.Time,
	limit int,
) (holders []string, overLimit bool, err error) {
	if domain == "" {
		return nil, false, nil
	}
	if limit <= 0 {
		return nil, false, errors.New("modify-holder limit must be positive")
	}
	segments := splitDomainSegments(domain)
	if len(segments) == 0 || len(segments) > 16 {
		return nil, false, nil
	}
	now := blockTime.Unix()
	set := make(map[string]struct{}, limit+1)
	rawGrantSteps := 0
	err = s.view(func(txn *badger.Txn) error {
		add := func(principalID string) bool {
			if principalID == "" {
				return false
			}
			set[principalID] = struct{}{}
			return len(set) > limit
		}
		for i := len(segments); i >= 1; i-- {
			candidate := strings.Join(segments[:i], ".")
			if shared {
				if i != len(segments) {
					break
				}
			} else {
				candidateShared, sharedErr := appV23DomainIsSharedTxn(txn, candidate)
				if sharedErr != nil {
					return sharedErr
				}
				if candidateShared {
					break
				}
			}

			// Shared resources have no effective owner. Non-shared candidates
			// retain owner authority until a shared boundary stops the walk.
			if !shared {
				value, ownerErr := s.appV23ReadEffectiveValueTxn(txn, domainKey(candidate))
				if ownerErr == nil {
					owner, _, decodeErr := decodeString(value, 0)
					if decodeErr != nil {
						return decodeErr
					}
					if add(owner) {
						overLimit = true
						return nil
					}
				} else if !errors.Is(ownerErr, badger.ErrKeyNotFound) {
					return ownerErr
				}
			}

			prefix := []byte("grant:" + candidate + ":")
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				rawGrantSteps++
				if rawGrantSteps > maxChallengeV21PrefixScanSteps {
					it.Close()
					return ErrChallengeV21PrefixScanLimit
				}
				item := it.Item()
				principalID, canonical := canonicalAgentIDFromPrefixedKey(item.Key(), prefix)
				if !canonical {
					continue
				}
				activeModify := false
				if valueErr := item.Value(func(value []byte) error {
					if len(value) < 9 {
						return nil
					}
					level := value[0]
					expiresAt := int64(binary.BigEndian.Uint64(value[1:9])) // #nosec G115 -- expiry timestamp fits in int64
					activeModify = level >= 3 && (expiresAt == 0 || now < expiresAt)
					return nil
				}); valueErr != nil {
					it.Close()
					return valueErr
				}
				if activeModify && add(principalID) {
					it.Close()
					overLimit = true
					return nil
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil || overLimit {
		return nil, overLimit, err
	}
	holders = make([]string, 0, len(set))
	for principalID := range set {
		holders = append(holders, principalID)
	}
	sort.Strings(holders)
	return holders, false, nil
}

// AppV23AdditionalModifyHolders returns root-approved managers in the owning
// member's group plus approved administrators. It is bounded by the protocol
// limits and intended to be merged with the legacy grant/owner electorate.
func (s *BadgerStore) AppV23AdditionalModifyHolders(domain string, shared bool, limit int) ([]string, bool, error) {
	if limit <= 0 {
		return nil, false, errors.New("modify-holder limit must be positive")
	}
	owner, _, err := s.ResolveAppV23OwningAncestor(domain)
	if err != nil {
		return nil, false, err
	}
	set := make(map[string]struct{})
	if owner != "" && !shared {
		if groups, groupsErr := s.ListAppV23AgentGroups(owner); groupsErr != nil {
			return nil, false, groupsErr
		} else {
			for _, group := range groups {
				for _, member := range group.Members {
					authorization, authErr := s.authorizeAppV23LocalDomainPolicy(member, domain, AppV23VerbModify, shared)
					if authErr != nil {
						return nil, false, authErr
					}
					if authorization.Allowed && !authorization.ExplicitDeny {
						set[member] = struct{}{}
					}
				}
			}
		}
	}
	err = s.view(func(txn *badger.Txn) error {
		prefix := []byte("appv23:admin:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		steps := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			steps++
			if steps > AppV23MaxAdmins+1 {
				return errors.New("app-v23 admin index exceeds deterministic bound")
			}
			key := it.Item().Key()
			agentID := string(key[len(prefix):])
			if !isCanonicalAgentID(agentID) {
				continue
			}
			authorization, authErr := s.authorizeAppV23LocalDomainPolicy(agentID, domain, AppV23VerbModify, shared)
			if authErr != nil {
				return authErr
			}
			if authorization.Allowed && !authorization.ExplicitDeny {
				set[agentID] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if len(set) > limit {
		return nil, true, nil
	}
	out := make([]string, 0, len(set))
	for agentID := range set {
		out = append(out, agentID)
	}
	sort.Strings(out)
	return out, false, nil
}

func (s *BadgerStore) ValidateAppV23State() error {
	root, err := s.GetAppV23Root()
	if err != nil || root == nil {
		return errors.New("app-v23 state has no root")
	}
	if err := validateCanonicalAgentID("root principal", root.PrincipalID); err != nil {
		return err
	}
	if err := validateCanonicalAgentID("root credential", root.CredentialID); err != nil {
		return err
	}
	if root.Generation == 0 || strings.TrimSpace(root.Scope) == "" ||
		len(root.HistoryDigest) != sha256.Size*2 {
		return errors.New("app-v23 root credential state is invalid")
	}
	return s.view(func(txn *badger.Txn) error {
		roster, rosterErr := s.appV23MigrationAgentsTxn(txn)
		if rosterErr != nil {
			return rosterErr
		}
		rosterIDs := make(map[string]struct{}, len(roster))
		rosterByID := make(map[string]OnChainAgent, len(roster))
		for _, agent := range roster {
			if _, duplicate := rosterIDs[agent.AgentID]; duplicate {
				return fmt.Errorf(
					"app-v23 migration roster contains duplicate agent %s",
					agent.AgentID,
				)
			}
			rosterIDs[agent.AgentID] = struct{}{}
			rosterByID[agent.AgentID] = agent
		}
		if root.CredentialID != root.PrincipalID {
			if _, err := txn.Get(agentOnChainKey(root.CredentialID)); err == nil {
				return errors.New("app-v23 root credential collides with a registered agent")
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if _, err := txn.Get(appV23EnrollmentKey(root.CredentialID)); err == nil {
				return errors.New("app-v23 root credential collides with an enrolled agent")
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		historyPrefix := []byte("appv23:root_credential:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = historyPrefix
		history := txn.NewIterator(opts)
		var historyCount uint64
		principalMarked := false
		currentMarked := false
		historyGenerations := make(map[uint64]string)
		for history.Seek(historyPrefix); history.ValidForPrefix(historyPrefix); history.Next() {
			historyCount++
			id := string(history.Item().Key()[len(historyPrefix):])
			if err := validateCanonicalAgentID("root credential history", id); err != nil {
				history.Close()
				return err
			}
			value, valueErr := history.Item().ValueCopy(nil)
			if valueErr != nil {
				history.Close()
				return valueErr
			}
			if len(value) != 8 {
				history.Close()
				return errors.New("app-v23 root credential history generation is malformed")
			}
			generation := binary.BigEndian.Uint64(value)
			if generation == 0 || generation > root.Generation {
				history.Close()
				return fmt.Errorf(
					"app-v23 root credential history generation %d is outside current generation %d",
					generation, root.Generation,
				)
			}
			if _, exists := historyGenerations[generation]; exists {
				history.Close()
				return fmt.Errorf("app-v23 root credential history generation %d is duplicated", generation)
			}
			historyGenerations[generation] = id
			if id == root.PrincipalID {
				if generation != 1 {
					history.Close()
					return errors.New("app-v23 root principal history generation is not one")
				}
				principalMarked = true
			}
			if id == root.CredentialID {
				if generation != root.Generation {
					history.Close()
					return errors.New("app-v23 current root credential history generation is stale")
				}
				currentMarked = true
			}
			if id == root.PrincipalID {
				continue
			}
			label := "root credential"
			if id != root.CredentialID {
				label = "retired root credential"
			}
			if _, err := txn.Get(agentOnChainKey(id)); err == nil {
				history.Close()
				return fmt.Errorf("app-v23 %s collides with a registered agent", label)
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				history.Close()
				return err
			}
			if _, err := txn.Get(appV23EnrollmentKey(id)); err == nil {
				history.Close()
				return fmt.Errorf("app-v23 %s collides with an enrolled agent", label)
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				history.Close()
				return err
			}
		}
		history.Close()
		if historyCount != root.Generation {
			return fmt.Errorf(
				"app-v23 root credential history count %d does not match generation %d",
				historyCount, root.Generation,
			)
		}
		if !principalMarked || !currentMarked {
			return errors.New("app-v23 root credential history is missing principal or current credential")
		}
		historyDigest := ""
		for generation := uint64(1); generation <= root.Generation; generation++ {
			credentialID, ok := historyGenerations[generation]
			if !ok {
				return fmt.Errorf("app-v23 root credential history generation %d is missing", generation)
			}
			nextDigest, digestErr := appV23NextRootHistoryDigest(
				historyDigest, generation, credentialID,
			)
			if digestErr != nil {
				return digestErr
			}
			historyDigest = nextDigest
		}
		if historyDigest != root.HistoryDigest {
			return errors.New("app-v23 root credential history digest mismatch")
		}
		migrationDispositions := make(map[string]AppV23MigrationDisposition)
		var migration AppV23MigrationState
		migrationErr := appV23ReadJSON(txn, appV23MigrationStateKey(), &migration)
		if migrationErr == nil {
			if migration.SchemaDigest != appV23MigrationSchemaDigest() ||
				migration.AgentCount <= 0 ||
				migration.LegacyReadCount != migration.AgentCount ||
				migration.LegacyFederationCount < 0 ||
				migration.LegacyFederationCount > AppV23MaxLegacyFederations ||
				migration.ManifestDigest == "" ||
				migration.LegacyAdminCount <= 0 ||
				len(migration.LegacyAdminDigest) != sha256.Size*2 ||
				!sort.StringsAreSorted(migration.LegacyAdmins) {
				return errors.New("app-v23 migration state invariant failed")
			}
			legacyAdmins := append([]string(nil), migration.LegacyAdmins...)
			if len(legacyAdmins) == 0 {
				auditPrefix := []byte("appv23:legacy_admin:")
				if err := s.appV23EffectivePrefixTxn(txn, auditPrefix, func(key, _ []byte) error {
					agentID := string(key[len(auditPrefix):])
					if err := validateCanonicalAgentID("legacy admin audit", agentID); err != nil {
						return err
					}
					legacyAdmins = append(legacyAdmins, agentID)
					return nil
				}); err != nil {
					return err
				}
			}
			if len(legacyAdmins) != migration.LegacyAdminCount ||
				appV23LegacyAdminDigest(legacyAdmins) != migration.LegacyAdminDigest {
				return errors.New("app-v23 legacy Admin audit digest mismatch")
			}
			if (migration.StageCount == 0) != (migration.StageDigest == "") ||
				migration.StageCount < 0 {
				return errors.New("app-v23 migration stage header invariant failed")
			}
			if migration.StageCount > 0 {
				stage, stageErr := s.appV23PromotedStageMapTxn(txn)
				if stageErr != nil {
					return fmt.Errorf("read promoted app-v23 migration stage: %w", stageErr)
				}
				if len(stage) != migration.StageCount {
					return errors.New("app-v23 promoted migration stage count mismatch")
				}
				digest, digestErr := appV23StageDigest(stage)
				if digestErr != nil {
					return digestErr
				}
				if digest != migration.StageDigest {
					return errors.New("app-v23 promoted migration stage digest mismatch")
				}
			}
			if migration.RootBootstrapDigest != "" {
				if migration.RootBootstrapDigest != root.BootstrapDigest {
					return errors.New("app-v23 bootstrap-root digest mismatch")
				}
			} else if root.BootstrapDigest != migration.ManifestDigest {
				return errors.New("app-v23 legacy migration root digest mismatch")
			}
			dispositions := make([]AppV23MigrationDisposition, 0, migration.AgentCount)
			prefix := []byte("appv23:migration:")
			if err := s.appV23EffectivePrefixTxn(txn, prefix, func(key, value []byte) error {
				var disposition AppV23MigrationDisposition
				if err := json.Unmarshal(value, &disposition); err != nil {
					return err
				}
				if disposition.AgentID != string(key[len(prefix):]) {
					return errors.New("app-v23 migration disposition key mismatch")
				}
				migrationDispositions[disposition.AgentID] = disposition
				dispositions = append(dispositions, disposition)
				return nil
			}); err != nil {
				return err
			}
			if len(dispositions) != migration.AgentCount {
				return errors.New("app-v23 migration disposition count mismatch")
			}
			manifest, manifestErr := json.Marshal(dispositions)
			if manifestErr != nil {
				return manifestErr
			}
			sum := sha256.Sum256(manifest)
			if hex.EncodeToString(sum[:]) != migration.ManifestDigest {
				return errors.New("app-v23 migration manifest digest mismatch")
			}

			legacyMembershipLinks := 0
			legacyReadCount := 0
			legacyReadPrefix := []byte("appv23:legacy_read:")
			if err := s.appV23EffectivePrefixTxn(
				txn, legacyReadPrefix, func(key, value []byte) error {
					legacyReadCount++
					var baseline AppV23LegacyReadBaseline
					if err := json.Unmarshal(value, &baseline); err != nil {
						return err
					}
					agentID := string(key[len(legacyReadPrefix):])
					disposition, ok := migrationDispositions[agentID]
					if !ok || baseline.AgentID != agentID ||
						baseline.LegacyPolicyDigest != disposition.LegacyPolicyDigest {
						return errors.New("app-v23 legacy read baseline identity mismatch")
					}
					if err := validateCanonicalAgentID(
						"legacy read baseline agent", agentID,
					); err != nil {
						return err
					}
					decoded, err := hex.DecodeString(baseline.LegacyPolicyDigest)
					if err != nil || len(decoded) != sha256.Size ||
						hex.EncodeToString(decoded) != baseline.LegacyPolicyDigest {
						return errors.New("app-v23 legacy read baseline digest is invalid")
					}
					for i, membership := range baseline.OrgMemberships {
						legacyMembershipLinks++
						if legacyMembershipLinks > AppV23MaxLegacyMembershipLinks ||
							membership.OrgID == "" || membership.Role == "" ||
							membership.Clearance > uint8(ClearanceTopSecret) ||
							(i > 0 &&
								baseline.OrgMemberships[i-1].OrgID >= membership.OrgID) {
							return errors.New("app-v23 legacy organization baseline is invalid")
						}
					}
					for i, membership := range baseline.DeptMemberships {
						legacyMembershipLinks++
						if legacyMembershipLinks > AppV23MaxLegacyMembershipLinks ||
							membership.OrgID == "" || membership.DeptID == "" ||
							membership.Role == "" ||
							membership.Clearance > uint8(ClearanceTopSecret) {
							return errors.New("app-v23 legacy department baseline is invalid")
						}
						if i > 0 {
							previous := baseline.DeptMemberships[i-1]
							if previous.OrgID > membership.OrgID ||
								(previous.OrgID == membership.OrgID &&
									previous.DeptID >= membership.DeptID) {
								return errors.New("app-v23 legacy department baseline is not canonical")
							}
						}
					}
					return nil
				},
			); err != nil {
				return err
			}
			if legacyReadCount != migration.LegacyReadCount {
				return errors.New("app-v23 legacy read baseline count mismatch")
			}

			legacyFederationCount := 0
			legacyFederationScopeEntries := 0
			legacyFederationPrefix := []byte("appv23:legacy_federation:")
			if err := s.appV23EffectivePrefixTxn(
				txn, legacyFederationPrefix, func(key, value []byte) error {
					legacyFederationCount++
					if legacyFederationCount > AppV23MaxLegacyFederations {
						return errors.New("app-v23 legacy federation baseline exceeds bound")
					}
					var federation AppV23LegacyFederationBaseline
					if err := json.Unmarshal(value, &federation); err != nil {
						return err
					}
					if federation.FederationID !=
						string(key[len(legacyFederationPrefix):]) ||
						federation.FederationID == "" ||
						federation.ProposerOrgID == "" ||
						federation.TargetOrgID == "" ||
						federation.Status == "" ||
						federation.MaxClearance > uint8(ClearanceTopSecret) ||
						len(federation.AllowedDomains) >
							AppV23MaxLegacyMembershipLinks ||
						len(federation.AllowedDepts) >
							AppV23MaxLegacyMembershipLinks {
						return errors.New("app-v23 legacy federation baseline is invalid")
					}
					legacyFederationScopeEntries +=
						len(federation.AllowedDomains) + len(federation.AllowedDepts)
					if legacyFederationScopeEntries > AppV23MaxLegacyMembershipLinks {
						return errors.New("app-v23 legacy federation scope exceeds bound")
					}
					return nil
				},
			); err != nil {
				return err
			}
			if legacyFederationCount != migration.LegacyFederationCount {
				return errors.New("app-v23 legacy federation baseline count mismatch")
			}
		} else if !errors.Is(migrationErr, badger.ErrKeyNotFound) {
			return migrationErr
		}
		pendingRegistrationFloor := int64(0)
		switch {
		case migrationErr == nil:
			pendingRegistrationFloor = migration.Height
		case errors.Is(migrationErr, badger.ErrKeyNotFound):
			var activation AppV23GenesisActivation
			if err := appV23ReadJSON(
				txn, appV23GenesisActivationKey(), &activation,
			); err != nil {
				return errors.New(
					"app-v23 state without migration has no genesis activation",
				)
			}
			if activation.RootID != root.PrincipalID ||
				activation.Scope != root.Scope ||
				activation.BootstrapDigest != root.BootstrapDigest ||
				root.EstablishedAt <= 0 {
				return errors.New(
					"app-v23 genesis activation does not match Root state",
				)
			}
			pendingRegistrationFloor = root.EstablishedAt
		}

		enrollments := make(map[string]AppV23LocalEnrollment)
		active := make(map[string]AppV23LocalEnrollment)
		roles := make(map[string]AppV23RoleState)
		enrollmentPrefix := []byte("appv23:enroll:")
		if err := s.appV23EffectivePrefixTxn(txn, enrollmentPrefix, func(key, value []byte) error {
			var enrollment AppV23LocalEnrollment
			if err := json.Unmarshal(value, &enrollment); err != nil {
				return err
			}
			if enrollment.AgentID != string(key[len(enrollmentPrefix):]) || enrollment.Revision == 0 {
				return errors.New("app-v23 enrollment key/value invariant failed")
			}
			if err := validateCanonicalAgentID("enrollment agent", enrollment.AgentID); err != nil {
				return err
			}
			enrollments[enrollment.AgentID] = enrollment
			if enrollment.Active {
				active[enrollment.AgentID] = enrollment
			}
			return nil
		}); err != nil {
			return err
		}

		rolePrefix := []byte("appv23:role:")
		if err := s.appV23EffectivePrefixTxn(txn, rolePrefix, func(key, value []byte) error {
			var role AppV23RoleState
			if err := json.Unmarshal(value, &role); err != nil {
				return err
			}
			if role.AgentID != string(key[len(rolePrefix):]) || role.Revision == 0 {
				return errors.New("app-v23 role key/value invariant failed")
			}
			if err := validateCanonicalAgentID("role agent", role.AgentID); err != nil {
				return err
			}
			roles[role.AgentID] = role
			return nil
		}); err != nil {
			return err
		}

		if len(enrollments) != len(roles) {
			return errors.New("app-v23 enrollment/role cardinality mismatch")
		}
		for agentID := range rosterIDs {
			if _, enrolled := enrollments[agentID]; enrolled {
				if _, hasRole := roles[agentID]; !hasRole {
					return fmt.Errorf(
						"app-v23 enrolled agent %s has no role", agentID,
					)
				}
				continue
			}
			if _, hasRole := roles[agentID]; hasRole {
				return fmt.Errorf(
					"app-v23 pending agent %s has an orphan role", agentID,
				)
			}
			agent := rosterByID[agentID]
			_, migrated := migrationDispositions[agentID]
			if migrated ||
				agent.RegisteredAt <= pendingRegistrationFloor ||
				agent.Role != AppV23RoleMember ||
				agent.Clearance != uint8(ClearanceInternal) ||
				agent.Capabilities != DefaultSelfRegisteredAgentCapabilities ||
				agent.OrgID != "" ||
				agent.DeptID != "" ||
				agent.DomainAccess != "" ||
				agent.VisibleAgents != "" {
				return fmt.Errorf(
					"app-v23 registered agent %s has no valid pending enrollment",
					agentID,
				)
			}
		}
		for agentID, enrollment := range enrollments {
			if _, ok := rosterIDs[agentID]; !ok {
				return fmt.Errorf(
					"app-v23 enrollment %s has no registered agent", agentID,
				)
			}
			if enrollment.Profile == AppV23ProfileRoot && agentID != root.PrincipalID {
				return fmt.Errorf(
					"app-v23 non-root agent %s has reserved root profile",
					agentID,
				)
			}
			role, ok := roles[agentID]
			if !ok || ValidateAppV23EnrollmentPolicy(
				role.Role, enrollment.Profile, enrollment.Capabilities,
				enrollment.Clearance, enrollment.Active,
			) != nil {
				return fmt.Errorf("app-v23 policy invariant failed for agent %s", agentID)
			}
			if enrollment.Profile == AppV23ProfileLegacyRestricted ||
				(enrollment.HomeDomain == "" &&
					enrollment.Profile == AppV23ProfileCompanion) {
				disposition, migrated := migrationDispositions[agentID]
				matchesMigration := migrated &&
					disposition.Profile == enrollment.Profile &&
					disposition.HomeDomain == enrollment.HomeDomain &&
					disposition.Active == enrollment.Active
				matchesContinuity := false
				if migrated && !matchesMigration {
					var continuityErr error
					matchesContinuity, continuityErr =
						s.appV25ContinuityActivationMatchesTxn(txn, enrollment, disposition)
					if continuityErr != nil {
						return continuityErr
					}
				}
				if !matchesMigration && !matchesContinuity {
					return fmt.Errorf(
						"app-v23 migration-only policy has no matching disposition for agent %s",
						agentID,
					)
				}
			}
			var agent OnChainAgent
			if projectionErr := s.appV23ReadEffectiveJSONTxn(
				txn, appV23ProjectedAgentKey(agentID), &agent,
			); errors.Is(projectionErr, badger.ErrKeyNotFound) {
				if readErr := appV23ReadJSON(txn, agentOnChainKey(agentID), &agent); readErr != nil {
					return readErr
				}
			} else if projectionErr != nil {
				return projectionErr
			}
			// The duplicate agent-shaped record is a historical serving
			// projection. Role, clearance, and capability authority lives in the
			// separately versioned role and enrollment records validated above.
			// Older upgrade paths can leave those derived fields stale; startup
			// rebuilds its SQLite projection from the canonical policy instead of
			// bricking the whole node over that recoverable drift.
			if enrollment.Active && agentID != root.PrincipalID && enrollment.Profile != AppV23ProfileReadOnly {
				if enrollment.HomeDomain == "" &&
					AppV23AllowsMigratedDomainless(
						enrollment.Profile, enrollment.Capabilities,
					) {
					continue
				}
				if err := ValidateAppV23DomainName(enrollment.HomeDomain); err != nil {
					return fmt.Errorf(
						"app-v23 agent %s has invalid home domain: %w",
						agentID, err,
					)
				}
				shared, err := appV23DomainIsSharedTxn(txn, enrollment.HomeDomain)
				if err != nil {
					return err
				}
				if enrollment.HomeDomain == "" || shared {
					return fmt.Errorf("app-v23 agent %s has no non-shared home domain", agentID)
				}
				var owner string
				value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(enrollment.HomeDomain))
				if err == nil {
					var decodeErr error
					owner, _, decodeErr = decodeString(value, 0)
					err = decodeErr
				}
				if err != nil || owner != agentID {
					return fmt.Errorf("app-v23 home domain owner mismatch for %s", agentID)
				}
			}
			if enrollment.Active && enrollment.Profile == AppV23ProfileReadOnly &&
				enrollment.HomeDomain != "" {
				return fmt.Errorf("app-v23 Read-only agent %s retains a home domain", agentID)
			}
		}
		rootEnrollment, ok := active[root.PrincipalID]
		if !ok || rootEnrollment.Profile != AppV23ProfileRoot ||
			rootEnrollment.RootGeneration != root.Generation {
			return errors.New("app-v23 root enrollment invariant failed")
		}
		if rootRole, ok := roles[root.PrincipalID]; !ok || rootRole.Role != AppV23RoleAdmin {
			return errors.New("app-v23 root role invariant failed")
		}

		adminSet := make(map[string]struct{})
		adminPrefix := []byte("appv23:admin:")
		opts = badger.DefaultIteratorOptions
		opts.Prefix = adminPrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		for it.Seek(adminPrefix); it.ValidForPrefix(adminPrefix); it.Next() {
			if len(adminSet) == AppV23MaxAdmins {
				it.Close()
				return errors.New("app-v23 admin index exceeds bound")
			}
			adminSet[string(it.Item().Key()[len(adminPrefix):])] = struct{}{}
		}
		it.Close()
		for agentID, role := range roles {
			_, indexed := adminSet[agentID]
			if (role.Role == AppV23RoleAdmin) != indexed {
				return fmt.Errorf("app-v23 admin index mismatch for %s", agentID)
			}
		}
		for agentID := range adminSet {
			role, ok := roles[agentID]
			if !ok || role.Role != AppV23RoleAdmin {
				return fmt.Errorf("app-v23 orphan admin index for %s", agentID)
			}
		}

		groupPrefix := []byte("appv23:group:")
		opts = badger.DefaultIteratorOptions
		opts.Prefix = groupPrefix
		it = txn.NewIterator(opts)
		groupCount := 0
		linkCount := 0
		for it.Seek(groupPrefix); it.ValidForPrefix(groupPrefix); it.Next() {
			groupCount++
			if groupCount > AppV23MaxGroups {
				it.Close()
				return errors.New("app-v23 group count exceeds bound")
			}
			var group AppV23AccessGroup
			if err := it.Item().Value(func(value []byte) error { return json.Unmarshal(value, &group) }); err != nil {
				it.Close()
				return err
			}
			if group.GroupID != string(it.Item().Key()[len(groupPrefix):]) ||
				group.Revision == 0 || len(group.Members) > AppV23MaxGroupMembers ||
				!sort.StringsAreSorted(group.Members) {
				it.Close()
				return errors.New("app-v23 group record invariant failed")
			}
			if err := validateAppV23GroupID(group.GroupID); err != nil {
				it.Close()
				return err
			}
			if len(group.Name) > 128 {
				it.Close()
				return errors.New("app-v23 group name exceeds deterministic bound")
			}
			for index, member := range group.Members {
				if index > 0 && group.Members[index-1] == member {
					it.Close()
					return errors.New("app-v23 group contains duplicate member")
				}
				if member == root.PrincipalID {
					it.Close()
					return errors.New("app-v23 group contains CEREBRUM root")
				}
				if _, ok := active[member]; !ok {
					it.Close()
					return errors.New("app-v23 group contains inactive member")
				}
				if _, err := txn.Get(appV23MemberGroupKey(member, group.GroupID)); err != nil {
					it.Close()
					return errors.New("app-v23 group reverse index is missing")
				}
				linkCount++
				if linkCount > AppV23MaxMembershipLinks {
					it.Close()
					return errors.New("app-v23 membership links exceed bound")
				}
			}
		}
		it.Close()

		reversePrefix := []byte("appv23:member_group:")
		opts = badger.DefaultIteratorOptions
		opts.Prefix = reversePrefix
		opts.PrefetchValues = false
		it = txn.NewIterator(opts)
		reverseCount := 0
		reversePerAgent := make(map[string]int)
		for it.Seek(reversePrefix); it.ValidForPrefix(reversePrefix); it.Next() {
			reverseCount++
			if reverseCount > AppV23MaxMembershipLinks {
				it.Close()
				return errors.New("app-v23 reverse membership links exceed bound")
			}
			suffix := string(it.Item().Key()[len(reversePrefix):])
			separator := strings.LastIndexByte(suffix, ':')
			if separator <= 0 || separator == len(suffix)-1 {
				it.Close()
				return errors.New("app-v23 malformed reverse membership key")
			}
			agentID, groupID := suffix[:separator], suffix[separator+1:]
			reversePerAgent[agentID]++
			if reversePerAgent[agentID] > AppV23MaxGroupsPerAgent {
				it.Close()
				return fmt.Errorf("app-v23 agent %s exceeds group membership bound", agentID)
			}
			var group AppV23AccessGroup
			if err := appV23ReadJSON(txn, appV23GroupKey(groupID), &group); err != nil {
				it.Close()
				return err
			}
			index := sort.SearchStrings(group.Members, agentID)
			if index == len(group.Members) || group.Members[index] != agentID {
				it.Close()
				return errors.New("app-v23 stale reverse membership index")
			}
		}
		it.Close()
		if reverseCount != linkCount {
			return errors.New("app-v23 forward/reverse membership count mismatch")
		}
		if err := validateAppV23TaskIdempotencyStateTxn(txn); err != nil {
			return err
		}
		return nil
	})
}
