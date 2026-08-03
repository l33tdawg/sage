package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	AppV25MaxDomainContinuityWriters = AppV23MaxGroupMembers
	appV25DomainContinuityVersion    = uint8(1)
)

// ErrAppV25DomainContinuityStateConflict marks deterministic current-policy
// drift that makes a validator-attested continuity payload impossible to
// apply. Consensus may reject such a stale proposal before it is marked
// executed; malformed state and storage errors remain fatal.
var ErrAppV25DomainContinuityStateConflict = errors.New("app-v25 domain continuity state conflict")

// AppV25DomainContinuity is the consensus-held compatibility entitlement for
// one exact historical domain. It does not rewrite memory authorship or clear
// any capability bit: authorization bypasses bits 2/8 only for a listed local
// pre-upgrade writer and this exact domain.
type AppV25DomainContinuity struct {
	Version        uint8    `json:"version"`
	Domain         string   `json:"domain"`
	Writers        []string `json:"writers"`
	Owner          string   `json:"owner,omitempty"`
	GroupID        string   `json:"group_id,omitempty"`
	Shared         bool     `json:"shared"`
	PlanDigest     string   `json:"plan_digest"`
	RootGeneration uint64   `json:"root_generation"`
	AppliedHeight  int64    `json:"applied_height"`
}

type AppV25DomainContinuityActivation struct {
	AgentID            string `json:"agent_id"`
	HomeDomain         string `json:"home_domain"`
	LegacyPolicyDigest string `json:"legacy_policy_digest"`
	EnrollmentRevision uint64 `json:"enrollment_revision,omitempty"`
	AppliedHeight      int64  `json:"applied_height"`
}

type AppV25DomainContinuityBatchEntry struct {
	Domain  string
	Owner   string
	Writers []string
}

type appV25PreparedContinuityWriter struct {
	original     AppV23LocalEnrollment
	enrollment   AppV23LocalEnrollment
	disposition  AppV23MigrationDisposition
	changed      bool
	allocateHome bool
}

type appV25PreparedContinuityDomain struct {
	entry       AppV25DomainContinuityBatchEntry
	existing    *AppV25DomainContinuity
	owner       string
	groupID     string
	shared      bool
	repairOwner bool
}

type appV25PreparedContinuityBatch struct {
	root    AppV23RootState
	domains []appV25PreparedContinuityDomain
	writers map[string]*appV25PreparedContinuityWriter
	groups  map[string][]string
}

func appV25DomainContinuityDigest(domain string) string {
	sum := sha256.Sum256([]byte(domain))
	return hex.EncodeToString(sum[:])
}

func appV25DomainContinuityKey(domain string) []byte {
	return []byte("appv25:domain_continuity:" + appV25DomainContinuityDigest(domain))
}

func appV25DomainContinuityGrantKey(agentID, domain string) []byte {
	return []byte("appv25:domain_continuity_grant:" + agentID + ":" + appV25DomainContinuityDigest(domain))
}

func appV25DomainContinuityGrantValue(enrollmentRevision uint64) []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, enrollmentRevision)
	return value
}

func appV25DomainContinuityActivationKey(agentID string) []byte {
	return []byte("appv25:domain_continuity_activation:" + agentID)
}

func AppV25DomainContinuityGroupID(writers []string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("sage/app-v25/local-domain-group/v1\x00"))
	for _, writer := range writers {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(writer)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write([]byte(writer))
	}
	sum := digest.Sum(nil)
	return "legacy-" + hex.EncodeToString(sum[:20])
}

func validateAppV25DomainContinuityInput(
	domain string,
	writers []string,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	if err := ValidateAppV23DomainName(domain); err != nil {
		return fmt.Errorf("invalid continuity domain: %w", err)
	}
	if len(writers) == 0 || len(writers) > AppV25MaxDomainContinuityWriters {
		return fmt.Errorf("domain continuity writers must contain 1..%d agents", AppV25MaxDomainContinuityWriters)
	}
	if !sort.StringsAreSorted(writers) {
		return errors.New("domain continuity writers must be sorted")
	}
	for i, writer := range writers {
		if err := validateCanonicalAgentID("domain continuity writer", writer); err != nil {
			return err
		}
		if i > 0 && writers[i-1] == writer {
			return errors.New("domain continuity writers must be unique")
		}
	}
	if len(planDigest) != sha256.Size {
		return errors.New("domain continuity plan digest must be SHA-256")
	}
	if rootGeneration == 0 || height <= 0 {
		return errors.New("domain continuity requires positive Root generation and height")
	}
	return nil
}

func validateAppV25DomainContinuityBatchOwner(owner string, writers []string, rootID string) error {
	if err := validateCanonicalAgentID("domain continuity owner", owner); err != nil {
		return err
	}
	if owner == rootID {
		return nil
	}
	index := sort.SearchStrings(writers, owner)
	if index >= len(writers) || writers[index] != owner {
		return errors.New("domain continuity owner must be one of the historical writers")
	}
	return nil
}

func appV25AllocateContinuityHomeTxn(
	s *BadgerStore,
	txn *badger.Txn,
	agentID string,
	height int64,
	apply bool,
) (string, error) {
	base := "local-" + agentID
	for suffix := 0; suffix <= 1024; suffix++ {
		candidate := base
		if suffix > 0 {
			candidate = fmt.Sprintf("%s-%d", base, suffix)
		}
		if err := ValidateAppV23DomainName(candidate); err != nil {
			return "", err
		}
		shared, err := appV23DomainIsSharedTxn(txn, candidate)
		if err != nil {
			return "", err
		}
		if shared {
			continue
		}
		value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(candidate))
		switch {
		case errors.Is(err, badger.ErrKeyNotFound):
			if apply {
				if setErr := s.txnSet(txn, domainKey(candidate), appV23EncodeDomain(agentID, height)); setErr != nil {
					return "", setErr
				}
			}
			return candidate, nil
		case err != nil:
			return "", err
		default:
			owner, _, decodeErr := decodeString(value, 0)
			if decodeErr != nil {
				return "", decodeErr
			}
			if owner == agentID {
				return candidate, nil
			}
		}
	}
	return "", fmt.Errorf(
		"%w: unable to allocate deterministic app-v25 continuity home",
		ErrAppV25DomainContinuityStateConflict,
	)
}

// ValidateAppV25DomainContinuity runs the complete deterministic preparation
// path against one read-only snapshot. ApplyAppV25DomainContinuity calls the
// same helper again inside its atomic write transaction, so governance can
// reject current-policy drift before it records StatusExecuted.
func (s *BadgerStore) ValidateAppV25DomainContinuity(
	domain string,
	writers []string,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	if err := validateAppV25DomainContinuityInput(
		domain, writers, planDigest, rootGeneration, height,
	); err != nil {
		return err
	}
	writers = append([]string(nil), writers...)
	return s.view(func(txn *badger.Txn) error {
		return s.prepareAndMaybeApplyAppV25DomainContinuityTxn(
			txn, domain, writers, planDigest, rootGeneration, height, false,
		)
	})
}

// ApplyAppV25DomainContinuity installs one validator-attested exact-domain
// compatibility record. All writes are one AppHash-covered Badger transaction;
// a conflicting/replayed payload fails without partially changing authority.
func (s *BadgerStore) ApplyAppV25DomainContinuity(
	domain string,
	writers []string,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	if err := validateAppV25DomainContinuityInput(
		domain, writers, planDigest, rootGeneration, height,
	); err != nil {
		return err
	}
	writers = append([]string(nil), writers...)
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			return s.prepareAndMaybeApplyAppV25DomainContinuityTxn(
				txn, domain, writers, planDigest, rootGeneration, height, true,
			)
		})
	})
}

// ApplyAppV26DomainContinuity is the post-v26 form of the historical repair.
// It differs only when this exact operation creates/replays a recovered local
// Access Group: a previously unversioned group is stamped with the safe read
// tier in the same atomic transaction. It never scans or rewrites unrelated
// groups and never overwrites a later explicit operator tier.
func (s *BadgerStore) ApplyAppV26DomainContinuity(
	domain string,
	writers []string,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	if err := validateAppV25DomainContinuityInput(
		domain, writers, planDigest, rootGeneration, height,
	); err != nil {
		return err
	}
	writers = append([]string(nil), writers...)
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			if err := s.prepareAndMaybeApplyAppV25DomainContinuityTxn(
				txn, domain, writers, planDigest, rootGeneration, height, true,
			); err != nil {
				return err
			}
			var record AppV25DomainContinuity
			if err := appV23ReadJSON(txn, appV25DomainContinuityKey(domain), &record); err != nil {
				return err
			}
			return s.setAppV26RecoveredGroupAuthorityTxn(txn, record.GroupID)
		})
	})
}

// ValidateAppV25DomainContinuityBatch prepares a complete v2 batch without
// mutating state. Apply repeats this exact preparation inside one Badger
// transaction, so one stale final entry cannot partially publish earlier ones.
func (s *BadgerStore) ValidateAppV25DomainContinuityBatch(
	entries []AppV25DomainContinuityBatchEntry,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	return s.view(func(txn *badger.Txn) error {
		_, err := s.prepareAppV25DomainContinuityBatchTxn(
			txn, entries, planDigest, rootGeneration, height,
		)
		return err
	})
}

// ApplyAppV25DomainContinuityBatch atomically installs or repairs every exact
// entry in one v2 manifest. Each unique writer is activated/revised at most
// once and every grant is stamped with that final revision.
func (s *BadgerStore) ApplyAppV25DomainContinuityBatch(
	entries []AppV25DomainContinuityBatchEntry,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			plan, err := s.prepareAppV25DomainContinuityBatchTxn(
				txn, entries, planDigest, rootGeneration, height,
			)
			if err != nil {
				return err
			}
			return s.applyAppV25DomainContinuityBatchTxn(
				txn, plan, planDigest, rootGeneration, height,
			)
		})
	})
}

// ApplyAppV26DomainContinuityBatch is the atomic post-v26 batch form. Only
// recovered groups named by this exact prepared manifest are normalized.
func (s *BadgerStore) ApplyAppV26DomainContinuityBatch(
	entries []AppV25DomainContinuityBatchEntry,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	return s.withFederationAuthorizationMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			plan, err := s.prepareAppV25DomainContinuityBatchTxn(
				txn, entries, planDigest, rootGeneration, height,
			)
			if err != nil {
				return err
			}
			if err := s.applyAppV25DomainContinuityBatchTxn(
				txn, plan, planDigest, rootGeneration, height,
			); err != nil {
				return err
			}
			for groupID := range plan.groups {
				if err := s.setAppV26RecoveredGroupAuthorityTxn(txn, groupID); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func (s *BadgerStore) setAppV26RecoveredGroupAuthorityTxn(txn *badger.Txn, groupID string) error {
	if groupID == "" {
		return nil
	}
	var group AppV23AccessGroup
	if err := appV23ReadJSON(txn, appV23GroupKey(groupID), &group); err != nil {
		return err
	}
	if group.MemberAuthority != "" {
		return ValidateAppV26GroupAuthority(group.MemberAuthority)
	}
	group.MemberAuthority = AppV26GroupAuthorityRead
	data, err := appV23Marshal(group)
	if err != nil {
		return err
	}
	return s.txnSet(txn, appV23GroupKey(groupID), data)
}

func (s *BadgerStore) prepareAppV25DomainContinuityBatchTxn(
	txn *badger.Txn,
	entries []AppV25DomainContinuityBatchEntry,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) (*appV25PreparedContinuityBatch, error) {
	if len(entries) == 0 || len(entries) > 128 {
		return nil, errors.New("domain continuity batch must contain 1..128 entries")
	}
	if len(planDigest) != sha256.Size || rootGeneration == 0 || height <= 0 {
		return nil, errors.New("domain continuity batch requires a SHA-256 plan, positive Root generation and height")
	}
	for i := range entries {
		if err := validateAppV25DomainContinuityInput(
			entries[i].Domain, entries[i].Writers, planDigest, rootGeneration, height,
		); err != nil {
			return nil, fmt.Errorf("domain continuity entry %d: %w", i, err)
		}
		if i > 0 && entries[i-1].Domain >= entries[i].Domain {
			return nil, errors.New("domain continuity batch domains must be strictly sorted")
		}
	}
	var root AppV23RootState
	if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
		return nil, err
	}
	if root.Generation != rootGeneration {
		return nil, fmt.Errorf("%w: Root generation changed", ErrAppV25DomainContinuityStateConflict)
	}
	for i := range entries {
		if err := validateAppV25DomainContinuityBatchOwner(
			entries[i].Owner, entries[i].Writers, root.PrincipalID,
		); err != nil {
			return nil, fmt.Errorf("domain continuity entry %d: %w", i, err)
		}
	}
	plan := &appV25PreparedContinuityBatch{
		root: root, writers: make(map[string]*appV25PreparedContinuityWriter),
		groups:  make(map[string][]string),
		domains: make([]appV25PreparedContinuityDomain, 0, len(entries)),
	}
	for _, input := range entries {
		entry := AppV25DomainContinuityBatchEntry{
			Domain: input.Domain, Owner: input.Owner,
			Writers: append([]string(nil), input.Writers...),
		}
		for _, writer := range entry.Writers {
			if writer == root.PrincipalID {
				return nil, fmt.Errorf("%w: CEREBRUM Root is not a local continuity group member", ErrAppV25DomainContinuityStateConflict)
			}
			if plan.writers[writer] != nil {
				continue
			}
			var disposition AppV23MigrationDisposition
			if err := s.appV23ReadEffectiveJSONTxn(txn, appV23MigrationKey(writer), &disposition); err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return nil, fmt.Errorf("%w: writer %s has no pre-upgrade local provenance", ErrAppV25DomainContinuityStateConflict, writer)
				}
				return nil, err
			}
			if disposition.AgentID != writer || len(disposition.LegacyPolicyDigest) != sha256.Size*2 {
				return nil, fmt.Errorf("%w: writer %s has invalid pre-upgrade provenance", ErrAppV25DomainContinuityStateConflict, writer)
			}
			var enrollment AppV23LocalEnrollment
			if err := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(writer), &enrollment); err != nil {
				if errors.Is(err, badger.ErrKeyNotFound) {
					return nil, fmt.Errorf("%w: writer %s has no local enrollment", ErrAppV25DomainContinuityStateConflict, writer)
				}
				return nil, err
			}
			if enrollment.AgentID != writer || enrollment.Profile == AppV23ProfileRoot || enrollment.Profile == AppV23ProfileReadOnly {
				return nil, fmt.Errorf("%w: writer %s is not an ordinary local writer", ErrAppV25DomainContinuityStateConflict, writer)
			}
			if !enrollment.Active && (disposition.Disposition != "pending_review" || enrollment.Capabilities != DefaultSelfRegisteredAgentCapabilities) {
				return nil, fmt.Errorf("%w: writer %s is not an eligible historical pending agent", ErrAppV25DomainContinuityStateConflict, writer)
			}
			plan.writers[writer] = &appV25PreparedContinuityWriter{
				original: enrollment, enrollment: enrollment, disposition: disposition,
			}
		}

		prepared := appV25PreparedContinuityDomain{entry: entry}
		if item, err := txn.Get(appV25DomainContinuityKey(entry.Domain)); err == nil {
			if valueErr := item.Value(func(value []byte) error {
				var existing AppV25DomainContinuity
				if unmarshalErr := json.Unmarshal(value, &existing); unmarshalErr != nil {
					return unmarshalErr
				}
				if existing.Version != appV25DomainContinuityVersion || existing.Domain != entry.Domain ||
					existing.RootGeneration != rootGeneration || !reflectStringSlicesEqual(existing.Writers, entry.Writers) {
					return ErrAppV25DomainContinuityStateConflict
				}
				if existing.Owner != "" && existing.Owner != entry.Owner {
					return ErrAppV25DomainContinuityStateConflict
				}
				prepared.existing = &existing
				prepared.owner, prepared.groupID, prepared.shared = existing.Owner, existing.GroupID, existing.Shared
				if prepared.owner == "" {
					// Version-1 shared records predate the domain-scoped owner.
					// A governed v2 repair may fill it, but can never replace one.
					prepared.owner = entry.Owner
					prepared.repairOwner = true
				}
				return nil
			}); valueErr != nil {
				return nil, valueErr
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return nil, err
		}
		if prepared.existing == nil {
			shared, err := appV23DomainIsSharedTxn(txn, entry.Domain)
			if err != nil {
				return nil, err
			}
			prepared.shared = len(entry.Writers) > 1 || shared
			prepared.owner = entry.Owner
			if prepared.shared {
				prepared.groupID = AppV25DomainContinuityGroupID(entry.Writers)
				if err := validateAppV23GroupID(prepared.groupID); err != nil {
					return nil, err
				}
			}
		}
		if err := s.validateAppV25PreparedDomainStateTxn(txn, &prepared, root.PrincipalID); err != nil {
			return nil, err
		}
		if prepared.shared {
			if _, exists := plan.groups[prepared.groupID]; !exists {
				plan.groups[prepared.groupID] = append([]string(nil), entry.Writers...)
			}
		}
		plan.domains = append(plan.domains, prepared)
	}

	if err := s.validateAppV25BatchGroupCapacityTxn(txn, plan.groups); err != nil {
		return nil, err
	}
	sharedDomains := make(map[string]struct{}, len(plan.domains))
	for _, domain := range plan.domains {
		if domain.shared {
			sharedDomains[domain.entry.Domain] = struct{}{}
		}
	}
	for writer, prepared := range plan.writers {
		if !prepared.enrollment.Active {
			prepared.enrollment.Active = true
			prepared.changed = true
		}
		_, homeBecomesShared := sharedDomains[prepared.enrollment.HomeDomain]
		if prepared.enrollment.HomeDomain == "" || homeBecomesShared {
			home, err := appV25AllocateContinuityHomeTxn(s, txn, writer, height, false)
			if err != nil {
				return nil, err
			}
			prepared.enrollment.HomeDomain = home
			prepared.changed = true
			prepared.allocateHome = true
		}
		if prepared.changed {
			prepared.enrollment.Revision++
			prepared.enrollment.UpdatedHeight = height
		}
	}
	for _, domain := range plan.domains {
		if domain.existing == nil {
			continue
		}
		for _, writer := range domain.entry.Writers {
			current := plan.writers[writer].enrollment
			grantRevision, found, err := appV25ContinuityGrantRevisionTxn(txn, writer, domain.entry.Domain)
			if err != nil {
				return nil, err
			}
			if found && grantRevision == current.Revision {
				continue
			}
			owned, proofErr := s.appV25ContinuityOwnedEnrollmentTxn(txn, writer, plan.writers[writer])
			if proofErr != nil {
				return nil, proofErr
			}
			if !owned {
				return nil, fmt.Errorf("%w: stale continuity grant follows an explicit policy mutation", ErrAppV25DomainContinuityStateConflict)
			}
		}
	}
	return plan, nil
}

func (s *BadgerStore) validateAppV25PreparedDomainStateTxn(
	txn *badger.Txn,
	prepared *appV25PreparedContinuityDomain,
	rootID string,
) error {
	if prepared == nil {
		return errors.New("domain continuity prepared domain is unavailable")
	}
	shared, err := appV23DomainIsSharedTxn(txn, prepared.entry.Domain)
	if err != nil {
		return err
	}
	if prepared.existing != nil && shared != prepared.shared {
		return fmt.Errorf("%w: shared-domain state changed", ErrAppV25DomainContinuityStateConflict)
	}
	if value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(prepared.entry.Domain)); err == nil {
		currentOwner, offset, decodeErr := decodeString(value, 0)
		if decodeErr != nil {
			return decodeErr
		}
		_, offset, decodeErr = decodeString(value, offset)
		if decodeErr != nil {
			return decodeErr
		}
		if len(value)-offset < 8 {
			return errors.New("invalid recovered domain height")
		}
		ownerUpdatedHeight := int64(binary.BigEndian.Uint64(value[offset : offset+8])) // #nosec G115 -- consensus height
		if prepared.repairOwner && ownerUpdatedHeight > prepared.existing.AppliedHeight {
			return fmt.Errorf("%w: recovered domain owner changed after legacy continuity", ErrAppV25DomainContinuityStateConflict)
		}
		index := sort.SearchStrings(prepared.entry.Writers, currentOwner)
		writerOwns := index < len(prepared.entry.Writers) && prepared.entry.Writers[index] == currentOwner
		if (prepared.existing == nil || prepared.repairOwner) && currentOwner != "" && currentOwner != rootID && !writerOwns {
			return fmt.Errorf("%w: domain continuity would displace unrelated owner %s", ErrAppV25DomainContinuityStateConflict, currentOwner)
		}
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	} else if prepared.existing != nil && prepared.owner != "" &&
		!prepared.repairOwner {
		return fmt.Errorf("%w: recovered domain owner is missing", ErrAppV25DomainContinuityStateConflict)
	}
	if !prepared.shared {
		return nil
	}
	var existing AppV23AccessGroup
	if err := appV23ReadJSON(txn, appV23GroupKey(prepared.groupID), &existing); err == nil {
		if existing.GroupID != prepared.groupID || !reflectStringSlicesEqual(existing.Members, prepared.entry.Writers) {
			return fmt.Errorf("%w: recovered access group input conflicts with current state", ErrAppV25DomainContinuityStateConflict)
		}
		for _, writer := range prepared.entry.Writers {
			if _, indexErr := txn.Get(appV23MemberGroupKey(writer, prepared.groupID)); errors.Is(indexErr, badger.ErrKeyNotFound) {
				return fmt.Errorf("%w: recovered access group membership index is incomplete", ErrAppV25DomainContinuityStateConflict)
			} else if indexErr != nil {
				return indexErr
			}
		}
		return nil
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	if prepared.existing != nil {
		return fmt.Errorf("%w: recovered access group is missing", ErrAppV25DomainContinuityStateConflict)
	}
	return nil
}

func (s *BadgerStore) validateAppV25BatchGroupCapacityTxn(
	txn *badger.Txn,
	groups map[string][]string,
) error {
	newGroups := make(map[string][]string)
	for groupID, writers := range groups {
		if _, err := txn.Get(appV23GroupKey(groupID)); errors.Is(err, badger.ErrKeyNotFound) {
			newGroups[groupID] = writers
		} else if err != nil {
			return err
		}
	}
	if len(newGroups) == 0 {
		return nil
	}
	groupCount, err := countAppV23PrefixTxn(txn, []byte("appv23:group:"), AppV23MaxGroups)
	if err != nil {
		return err
	}
	if groupCount+len(newGroups) > AppV23MaxGroups {
		return fmt.Errorf("%w: app-v23 global access group limit reached", ErrAppV25DomainContinuityStateConflict)
	}
	linkCount, err := countAppV23PrefixTxn(txn, []byte("appv23:member_group:"), AppV23MaxMembershipLinks)
	if err != nil {
		return err
	}
	newLinks := 0
	perWriter := make(map[string]int)
	for _, writers := range newGroups {
		newLinks += len(writers)
		for _, writer := range writers {
			perWriter[writer]++
		}
	}
	if linkCount+newLinks > AppV23MaxMembershipLinks {
		return fmt.Errorf("%w: app-v23 global membership link limit reached", ErrAppV25DomainContinuityStateConflict)
	}
	for writer, added := range perWriter {
		current, countErr := s.countAppV23AgentGroupsTxn(txn, writer, AppV23MaxGroupsPerAgent)
		if countErr != nil {
			return countErr
		}
		if current+added > AppV23MaxGroupsPerAgent {
			return fmt.Errorf("%w: group member %s reached the per-agent group limit", ErrAppV25DomainContinuityStateConflict, writer)
		}
	}
	return nil
}

func appV25ContinuityGrantRevisionTxn(
	txn *badger.Txn,
	agentID string,
	domain string,
) (uint64, bool, error) {
	item, err := txn.Get(appV25DomainContinuityGrantKey(agentID, domain))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	var revision uint64
	if err := item.Value(func(value []byte) error {
		if len(value) != 8 {
			return errors.New("invalid app-v25 domain continuity grant")
		}
		revision = binary.BigEndian.Uint64(value)
		return nil
	}); err != nil {
		return 0, false, err
	}
	return revision, true, nil
}

func (s *BadgerStore) appV25ContinuityOwnedEnrollmentTxn(
	txn *badger.Txn,
	agentID string,
	prepared *appV25PreparedContinuityWriter,
) (bool, error) {
	if prepared == nil {
		return false, nil
	}
	var activation AppV25DomainContinuityActivation
	if err := appV23ReadJSON(txn, appV25DomainContinuityActivationKey(agentID), &activation); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	current := prepared.original
	if activation.AgentID != agentID || activation.HomeDomain != current.HomeDomain ||
		activation.LegacyPolicyDigest != prepared.disposition.LegacyPolicyDigest ||
		activation.AppliedHeight != current.UpdatedHeight {
		return false, nil
	}
	if activation.EnrollmentRevision != 0 {
		return activation.EnrollmentRevision == current.Revision, nil
	}
	prefix := []byte("appv25:domain_continuity_grant:" + agentID + ":")
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = true
	iterator := txn.NewIterator(options)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		var revision uint64
		if err := iterator.Item().Value(func(value []byte) error {
			if len(value) != 8 {
				return errors.New("invalid app-v25 domain continuity grant")
			}
			revision = binary.BigEndian.Uint64(value)
			return nil
		}); err != nil {
			return false, err
		}
		if revision == current.Revision {
			return true, nil
		}
	}
	return false, nil
}

func (s *BadgerStore) applyAppV25DomainContinuityBatchTxn(
	txn *badger.Txn,
	plan *appV25PreparedContinuityBatch,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
) error {
	for writer, prepared := range plan.writers {
		if !prepared.changed {
			continue
		}
		if prepared.allocateHome {
			allocated, err := appV25AllocateContinuityHomeTxn(s, txn, writer, height, true)
			if err != nil {
				return err
			}
			if allocated != prepared.enrollment.HomeDomain {
				return fmt.Errorf("%w: deterministic continuity home changed", ErrAppV25DomainContinuityStateConflict)
			}
		}
		data, err := appV23Marshal(prepared.enrollment)
		if err != nil {
			return err
		}
		if setErr := s.txnSet(txn, appV23EnrollmentKey(writer), data); setErr != nil {
			return setErr
		}
		activation, err := appV23Marshal(AppV25DomainContinuityActivation{
			AgentID: writer, HomeDomain: prepared.enrollment.HomeDomain,
			LegacyPolicyDigest: prepared.disposition.LegacyPolicyDigest,
			EnrollmentRevision: prepared.enrollment.Revision,
			AppliedHeight:      height,
		})
		if err != nil {
			return err
		}
		if err := s.txnSet(txn, appV25DomainContinuityActivationKey(writer), activation); err != nil {
			return err
		}
	}
	for groupID, writers := range plan.groups {
		if _, err := txn.Get(appV23GroupKey(groupID)); err == nil {
			continue
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		groupData, err := appV23Marshal(AppV23AccessGroup{
			GroupID: groupID, Name: "Recovered historical domain",
			Members: append([]string(nil), writers...), Revision: 1,
			UpdatedBy: plan.root.PrincipalID, UpdatedHeight: height,
		})
		if err != nil {
			return err
		}
		if err := s.txnSet(txn, appV23GroupKey(groupID), groupData); err != nil {
			return err
		}
		for _, writer := range writers {
			if err := s.txnSet(txn, appV23MemberGroupKey(writer, groupID), []byte{1}); err != nil {
				return err
			}
		}
	}
	for _, prepared := range plan.domains {
		if prepared.shared {
			if err := s.txnSet(txn, stateKey("shared_domain:"+prepared.entry.Domain), []byte{1}); err != nil {
				return err
			}
		}
		if prepared.existing == nil || prepared.repairOwner {
			if err := s.txnSet(txn, domainKey(prepared.entry.Domain), appV23EncodeDomain(prepared.owner, height)); err != nil {
				return err
			}
		}
		if prepared.existing == nil || prepared.repairOwner {
			recordData, err := appV23Marshal(AppV25DomainContinuity{
				Version: appV25DomainContinuityVersion, Domain: prepared.entry.Domain,
				Writers: append([]string(nil), prepared.entry.Writers...),
				Owner:   prepared.owner, GroupID: prepared.groupID, Shared: prepared.shared,
				PlanDigest: hex.EncodeToString(planDigest), RootGeneration: rootGeneration,
				AppliedHeight: height,
			})
			if err != nil {
				return err
			}
			if err := s.txnSet(txn, appV25DomainContinuityKey(prepared.entry.Domain), recordData); err != nil {
				return err
			}
		}
		for _, writer := range prepared.entry.Writers {
			if err := s.txnSet(txn, appV25DomainContinuityGrantKey(writer, prepared.entry.Domain), appV25DomainContinuityGrantValue(plan.writers[writer].enrollment.Revision)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *BadgerStore) prepareAndMaybeApplyAppV25DomainContinuityTxn(
	txn *badger.Txn,
	domain string,
	writers []string,
	planDigest []byte,
	rootGeneration uint64,
	height int64,
	apply bool,
) error {
	var root AppV23RootState
	if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
		return err
	}
	if root.Generation != rootGeneration {
		return fmt.Errorf(
			"%w: Root generation changed",
			ErrAppV25DomainContinuityStateConflict,
		)
	}
	if existingItem, err := txn.Get(appV25DomainContinuityKey(domain)); err == nil {
		return existingItem.Value(func(value []byte) error {
			var existing AppV25DomainContinuity
			if unmarshalErr := json.Unmarshal(value, &existing); unmarshalErr != nil {
				return unmarshalErr
			}
			if existing.Domain == domain &&
				existing.RootGeneration == rootGeneration &&
				existing.PlanDigest == hex.EncodeToString(planDigest) &&
				reflectStringSlicesEqual(existing.Writers, writers) {
				return nil
			}
			return ErrAppV25DomainContinuityStateConflict
		})
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	enrollments := make(map[string]AppV23LocalEnrollment, len(writers))
	dispositions := make(map[string]AppV23MigrationDisposition, len(writers))
	for _, writer := range writers {
		if writer == root.PrincipalID {
			return fmt.Errorf(
				"%w: CEREBRUM Root is not a local continuity group member",
				ErrAppV25DomainContinuityStateConflict,
			)
		}
		var disposition AppV23MigrationDisposition
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23MigrationKey(writer), &disposition,
		); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf(
					"%w: writer %s has no pre-upgrade local provenance",
					ErrAppV25DomainContinuityStateConflict, writer,
				)
			}
			return fmt.Errorf("read writer %s pre-upgrade local provenance: %w", writer, err)
		}
		if disposition.AgentID != writer || len(disposition.LegacyPolicyDigest) != sha256.Size*2 {
			return fmt.Errorf(
				"%w: writer %s has invalid pre-upgrade provenance",
				ErrAppV25DomainContinuityStateConflict, writer,
			)
		}
		var enrollment AppV23LocalEnrollment
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23EnrollmentKey(writer), &enrollment,
		); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf(
					"%w: writer %s has no local enrollment",
					ErrAppV25DomainContinuityStateConflict, writer,
				)
			}
			return fmt.Errorf("read writer %s local enrollment: %w", writer, err)
		}
		if enrollment.AgentID != writer {
			return fmt.Errorf(
				"%w: writer %s enrollment mismatch",
				ErrAppV25DomainContinuityStateConflict, writer,
			)
		}
		if !enrollment.Active &&
			(disposition.Disposition != "pending_review" ||
				enrollment.Capabilities != DefaultSelfRegisteredAgentCapabilities) {
			return fmt.Errorf(
				"%w: writer %s is not an eligible historical pending agent",
				ErrAppV25DomainContinuityStateConflict, writer,
			)
		}
		if enrollment.Profile == AppV23ProfileRoot ||
			enrollment.Profile == AppV23ProfileReadOnly {
			return fmt.Errorf(
				"%w: writer %s is not an ordinary local writer",
				ErrAppV25DomainContinuityStateConflict, writer,
			)
		}
		enrollments[writer] = enrollment
		dispositions[writer] = disposition
	}

	effectiveShared, sharedErr := appV23DomainIsSharedTxn(txn, domain)
	if sharedErr != nil {
		return sharedErr
	}
	shared := len(writers) > 1 || effectiveShared
	groupID := ""
	owner := ""
	if shared {
		groupID = AppV25DomainContinuityGroupID(writers)
		if validationErr := validateAppV23GroupID(groupID); validationErr != nil {
			return validationErr
		}
	} else {
		owner = writers[0]
	}

	if value, readErr := s.appV23ReadEffectiveValueTxn(txn, domainKey(domain)); readErr == nil {
		currentOwner, _, decodeErr := decodeString(value, 0)
		if decodeErr != nil {
			return decodeErr
		}
		index := sort.SearchStrings(writers, currentOwner)
		writerOwns := index < len(writers) && writers[index] == currentOwner
		if currentOwner != "" && currentOwner != root.PrincipalID && !writerOwns {
			return fmt.Errorf(
				"%w: domain continuity would displace unrelated owner %s",
				ErrAppV25DomainContinuityStateConflict, currentOwner,
			)
		}
	} else if !errors.Is(readErr, badger.ErrKeyNotFound) {
		return readErr
	}

	for _, writer := range writers {
		enrollment := enrollments[writer]
		changed := false
		if !enrollment.Active {
			enrollment.Active = true
			changed = true
		}
		if !shared && enrollment.HomeDomain != domain {
			enrollment.HomeDomain = domain
			changed = true
		} else if shared &&
			(enrollment.HomeDomain == "" || enrollment.HomeDomain == domain) {
			home, allocationErr := appV25AllocateContinuityHomeTxn(s, txn, writer, height, apply)
			if allocationErr != nil {
				return allocationErr
			}
			enrollment.HomeDomain = home
			changed = true
		}
		if changed {
			enrollment.Revision++
			enrollment.UpdatedHeight = height
			data, marshalErr := appV23Marshal(enrollment)
			if marshalErr != nil {
				return marshalErr
			}
			if apply {
				if setErr := s.txnSet(txn, appV23EnrollmentKey(writer), data); setErr != nil {
					return setErr
				}
			}
			activation := AppV25DomainContinuityActivation{
				AgentID: writer, HomeDomain: enrollment.HomeDomain,
				LegacyPolicyDigest: dispositions[writer].LegacyPolicyDigest,
				EnrollmentRevision: enrollment.Revision,
				AppliedHeight:      height,
			}
			activationData, activationMarshalErr := appV23Marshal(activation)
			if activationMarshalErr != nil {
				return activationMarshalErr
			}
			if apply {
				if setErr := s.txnSet(
					txn, appV25DomainContinuityActivationKey(writer), activationData,
				); setErr != nil {
					return setErr
				}
			}
		}
		enrollments[writer] = enrollment
	}

	if shared {
		if apply {
			if setErr := s.txnSet(txn, stateKey("shared_domain:"+domain), []byte{1}); setErr != nil {
				return setErr
			}
		}
		var existing AppV23AccessGroup
		if err := appV23ReadJSON(txn, appV23GroupKey(groupID), &existing); err == nil {
			if existing.GroupID != groupID ||
				!reflectStringSlicesEqual(existing.Members, writers) {
				return fmt.Errorf(
					"%w: recovered access group input conflicts with current state",
					ErrAppV25DomainContinuityStateConflict,
				)
			}
			for _, writer := range writers {
				if _, indexErr := txn.Get(appV23MemberGroupKey(writer, groupID)); errors.Is(
					indexErr, badger.ErrKeyNotFound,
				) {
					return fmt.Errorf(
						"%w: recovered access group membership index is incomplete",
						ErrAppV25DomainContinuityStateConflict,
					)
				} else if indexErr != nil {
					return indexErr
				}
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		} else {
			groupCount, countErr := countAppV23PrefixTxn(
				txn, []byte("appv23:group:"), AppV23MaxGroups,
			)
			if countErr != nil {
				return countErr
			}
			if groupCount >= AppV23MaxGroups {
				return fmt.Errorf(
					"%w: app-v23 global access group limit reached",
					ErrAppV25DomainContinuityStateConflict,
				)
			}
			linkCount, countErr := countAppV23PrefixTxn(
				txn, []byte("appv23:member_group:"), AppV23MaxMembershipLinks,
			)
			if countErr != nil {
				return countErr
			}
			if linkCount+len(writers) > AppV23MaxMembershipLinks {
				return fmt.Errorf(
					"%w: app-v23 global membership link limit reached",
					ErrAppV25DomainContinuityStateConflict,
				)
			}
			for _, writer := range writers {
				memberGroups, countErr := s.countAppV23AgentGroupsTxn(
					txn, writer, AppV23MaxGroupsPerAgent,
				)
				if countErr != nil {
					return countErr
				}
				if memberGroups >= AppV23MaxGroupsPerAgent {
					return fmt.Errorf(
						"%w: group member %s reached the per-agent group limit",
						ErrAppV25DomainContinuityStateConflict, writer,
					)
				}
			}
			group := AppV23AccessGroup{
				GroupID: groupID, Name: "Recovered historical domain",
				Members: append([]string(nil), writers...), Revision: 1,
				UpdatedBy: root.PrincipalID, UpdatedHeight: height,
			}
			data, marshalErr := appV23Marshal(group)
			if marshalErr != nil {
				return marshalErr
			}
			if apply {
				if err := s.txnSet(txn, appV23GroupKey(groupID), data); err != nil {
					return err
				}
				for _, writer := range writers {
					if err := s.txnSet(
						txn, appV23MemberGroupKey(writer, groupID), []byte{1},
					); err != nil {
						return err
					}
				}
			}
		}
	} else {
		if apply {
			if err := s.txnSet(
				txn, domainKey(domain), appV23EncodeDomain(owner, height),
			); err != nil {
				return err
			}
		}
	}

	record := AppV25DomainContinuity{
		Version: appV25DomainContinuityVersion, Domain: domain,
		Writers: writers, Owner: owner, GroupID: groupID, Shared: shared,
		PlanDigest:     hex.EncodeToString(planDigest),
		RootGeneration: rootGeneration, AppliedHeight: height,
	}
	recordData, err := appV23Marshal(record)
	if err != nil {
		return err
	}
	if apply {
		if err := s.txnSet(txn, appV25DomainContinuityKey(domain), recordData); err != nil {
			return err
		}
	}
	for _, writer := range writers {
		enrollment := enrollments[writer]
		if apply {
			if err := s.txnSet(
				txn,
				appV25DomainContinuityGrantKey(writer, domain),
				appV25DomainContinuityGrantValue(enrollment.Revision),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func reflectStringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// AppV25AllowsHistoricalDomainWrite is intentionally exact-domain only.
func (s *BadgerStore) AppV25AllowsHistoricalDomainWrite(agentID, domain string) (bool, error) {
	if err := validateCanonicalAgentID("historical writer", agentID); err != nil {
		return false, err
	}
	if err := ValidateAppV23DomainName(domain); err != nil {
		return false, err
	}
	allowed := false
	viewErr := s.view(func(txn *badger.Txn) error {
		grant, getErr := txn.Get(appV25DomainContinuityGrantKey(agentID, domain))
		if errors.Is(getErr, badger.ErrKeyNotFound) {
			return nil
		} else if getErr != nil {
			return getErr
		}
		var grantRevision uint64
		if valueErr := grant.Value(func(value []byte) error {
			if len(value) != 8 {
				return errors.New("invalid app-v25 domain continuity grant")
			}
			grantRevision = binary.BigEndian.Uint64(value)
			return nil
		}); valueErr != nil {
			return valueErr
		}
		var record AppV25DomainContinuity
		if readRecordErr := appV23ReadJSON(txn, appV25DomainContinuityKey(domain), &record); readRecordErr != nil {
			return readRecordErr
		}
		if record.Version != appV25DomainContinuityVersion ||
			record.Domain != domain ||
			!sort.StringsAreSorted(record.Writers) {
			return errors.New("invalid app-v25 domain continuity record")
		}
		index := sort.SearchStrings(record.Writers, agentID)
		if index >= len(record.Writers) || record.Writers[index] != agentID {
			return nil
		}
		var enrollment AppV23LocalEnrollment
		if enrollmentErr := s.appV23ReadEffectiveJSONTxn(
			txn, appV23EnrollmentKey(agentID), &enrollment,
		); enrollmentErr != nil {
			return enrollmentErr
		}
		// The grant is a migration compatibility entitlement, not an immortal
		// override. Any later explicit policy mutation increments the enrollment
		// revision and therefore revokes this historical exception.
		if !enrollment.Active || enrollment.Profile == AppV23ProfileReadOnly ||
			enrollment.Revision != grantRevision {
			return nil
		}
		shared, sharedErr := appV23DomainIsSharedTxn(txn, domain)
		if sharedErr != nil {
			return sharedErr
		}
		if shared != record.Shared {
			return nil
		}
		if record.Shared {
			if record.GroupID == "" {
				return errors.New("invalid app-v25 shared domain continuity record")
			}
			var group AppV23AccessGroup
			if err := s.appV23ReadEffectiveJSONTxn(
				txn, appV23GroupKey(record.GroupID), &group,
			); errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			} else if err != nil {
				return err
			}
			member := sort.SearchStrings(group.Members, agentID)
			allowed = group.GroupID == record.GroupID &&
				member < len(group.Members) && group.Members[member] == agentID
			return nil
		}
		value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(domain))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		owner, _, err := decodeString(value, 0)
		if err != nil {
			return err
		}
		allowed = record.Owner == agentID && owner == agentID
		return nil
	})
	return allowed, viewErr
}

// AppV25AllowsHistoricalDomainModify extends the revision-bound exact
// continuity entitlement only to the domain-scoped owner selected by the
// governed recovery plan. It exists separately from the durable recovered
// group path so the initial legacy capability mask cannot make the recovered
// owner unable to manage its own domain, while a later explicit policy
// revision still restores the current capability mask as a hard deny.
func (s *BadgerStore) AppV25AllowsHistoricalDomainModify(
	agentID, domain string,
) (bool, error) {
	allowed, err := s.AppV25AllowsHistoricalDomainWrite(agentID, domain)
	if err != nil || !allowed {
		return allowed, err
	}
	record, err := s.GetAppV25DomainContinuity(domain)
	if err != nil || record == nil {
		return false, err
	}
	currentOwner, err := s.GetDomainOwner(domain)
	if err != nil {
		return false, err
	}
	return record.Owner == agentID && currentOwner == agentID, nil
}

// AuthorizeAppV25RecoveredGroupDomain applies current local role semantics to
// the exact Access Group created for a recovered multiwriter/shared domain.
// Ordinary shared domains remain ownerless and never derive group authority;
// this exception requires an immutable app-v25 continuity record naming the
// exact group plus the caller's current membership in that group.
//
// The surrounding app-v23 policy entry points evaluate active enrollment,
// named-profile hard restrictions, capability denies, and Admin authority
// before calling this helper. Every current recovered-group member retains
// Read and Write. The current canonical domain-row owner receives
// domain-scoped Modify authority; recovery initially sets that row from the
// immutable provenance Owner, while a later governed Root transfer can change
// current ownership without rewriting history. A global Manager role does not
// broaden that authority. Removing a non-owner member from the recovered group
// revokes this path immediately.
func (s *BadgerStore) AuthorizeAppV25RecoveredGroupDomain(
	agentID, domain string,
	verb AppV23DomainVerb,
) (bool, error) {
	if err := validateCanonicalAgentID("recovered group agent", agentID); err != nil {
		return false, err
	}
	if err := ValidateAppV23DomainName(domain); err != nil {
		return false, err
	}
	record, err := s.GetAppV25DomainContinuity(domain)
	if err != nil || record == nil || !record.Shared || record.GroupID == "" {
		return false, err
	}
	role, err := s.GetAppV23Role(agentID)
	if err != nil || role == nil {
		return false, err
	}
	if role.Role != AppV23RoleMember && role.Role != AppV23RoleManager {
		return false, nil
	}
	currentOwner := ""
	ownerFound := false
	err = s.view(func(txn *badger.Txn) error {
		value, readErr := s.appV23ReadEffectiveValueTxn(txn, domainKey(domain))
		if errors.Is(readErr, badger.ErrKeyNotFound) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		var decodeErr error
		currentOwner, _, decodeErr = decodeString(value, 0)
		ownerFound = decodeErr == nil
		return decodeErr
	})
	if err != nil {
		return false, err
	}
	if !ownerFound && record.Owner != "" {
		// A v2 record always commits its current owner row atomically. Missing
		// it is malformed state, not an authorization denial. Version-1 shared
		// records deliberately had Owner="" and no row; their recovered-group
		// members must retain Read/Write while the governed v2 repair fills it.
		return false, errors.New("app-v25 recovered domain owner is missing")
	}
	if currentOwner == agentID {
		return verb >= AppV23VerbRead && verb <= AppV23VerbModify, nil
	}
	group, err := s.GetAppV23AccessGroup(record.GroupID)
	if err != nil || group == nil {
		return false, err
	}
	member := sort.SearchStrings(group.Members, agentID)
	if member >= len(group.Members) || group.Members[member] != agentID {
		return false, nil
	}
	if verb == AppV23VerbRead || verb == AppV23VerbWrite {
		return true, nil
	}
	return false, nil
}

// AuthorizeAppV25RecoveredDirectRead reports whether a credential has direct
// current authority over an exact app-v25-repaired domain. It deliberately
// excludes ordinary grants and ordinary Access Groups: a frozen H-1 explicit
// domain ceiling still governs those additive paths. This narrow decision is
// safe to use when the obsolete H-1 compatibility projection itself cannot be
// decoded, so that legacy corruption cannot make a governed recovered domain
// write-only for its owner, continuity writers/group members, Admin, or Root.
func (s *BadgerStore) AuthorizeAppV25RecoveredDirectRead(
	credentialID, domain string,
) (bool, error) {
	record, err := s.GetAppV25DomainContinuity(domain)
	if err != nil || record == nil {
		return false, err
	}
	root, err := s.GetAppV23Root()
	if err != nil || root == nil {
		return false, err
	}
	policyID := credentialID
	if credentialID == root.CredentialID {
		policyID = root.PrincipalID
	} else {
		wasRoot, markerErr := s.IsAppV23RootCredential(credentialID)
		if markerErr != nil {
			return false, markerErr
		}
		if wasRoot {
			return false, nil
		}
	}
	shared, err := s.IsAppV23SharedDomain(domain)
	if err != nil {
		return false, err
	}
	decision, err := s.AuthorizeAppV23LocalDomain(
		credentialID, domain, AppV23VerbRead, shared,
	)
	if err != nil || decision.ExplicitDeny || !decision.Allowed {
		return false, err
	}
	if policyID == root.PrincipalID {
		return true, nil
	}
	role, err := s.GetAppV23Role(policyID)
	if err != nil {
		return false, err
	}
	if role != nil && role.Role == AppV23RoleAdmin {
		return true, nil
	}
	recoveredGroup, err := s.AuthorizeAppV25RecoveredGroupDomain(
		policyID, domain, AppV23VerbRead,
	)
	if err != nil || recoveredGroup {
		return recoveredGroup, err
	}
	owner, err := s.GetDomainOwner(domain)
	if err != nil {
		return false, err
	}
	if owner == policyID {
		return true, nil
	}
	return s.AppV25AllowsHistoricalDomainWrite(policyID, domain)
}

// GetAppV25DomainContinuity returns the immutable first governed restoration
// for an exact domain. Later memories never silently expand that authority;
// an operator can still make an explicit current-policy change.
func (s *BadgerStore) GetAppV25DomainContinuity(
	domain string,
) (*AppV25DomainContinuity, error) {
	if err := ValidateAppV23DomainName(domain); err != nil {
		return nil, err
	}
	var record AppV25DomainContinuity
	err := s.view(func(txn *badger.Txn) error {
		return appV23ReadJSON(txn, appV25DomainContinuityKey(domain), &record)
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if record.Version != appV25DomainContinuityVersion ||
		record.Domain != domain ||
		!sort.StringsAreSorted(record.Writers) {
		return nil, errors.New("invalid app-v25 domain continuity record")
	}
	return &record, nil
}

func (s *BadgerStore) appV25ContinuityActivationMatchesTxn(
	txn *badger.Txn,
	enrollment AppV23LocalEnrollment,
	disposition AppV23MigrationDisposition,
) (bool, error) {
	var activation AppV25DomainContinuityActivation
	err := appV23ReadJSON(txn, appV25DomainContinuityActivationKey(enrollment.AgentID), &activation)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	baseMatches := activation.AgentID == enrollment.AgentID &&
		activation.LegacyPolicyDigest == disposition.LegacyPolicyDigest &&
		enrollment.Active && enrollment.Profile == disposition.Profile
	if !baseMatches {
		return false, nil
	}
	if activation.HomeDomain == enrollment.HomeDomain {
		return true, nil
	}
	// App-v26 may have replaced only the invalid home binding committed by the
	// historical app-v25 batch path. Preserve the original activation as audit
	// history and accept the new enrollment only when the append-only repair
	// record bridges both exact revisions and domains.
	var repair AppV26HomeRepair
	if err := appV23ReadJSON(txn, appV26HomeRepairKey(enrollment.AgentID), &repair); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return repair.AgentID == enrollment.AgentID &&
		repair.PreviousHome == activation.HomeDomain &&
		repair.ReplacementHome == enrollment.HomeDomain &&
		repair.PreviousRevision == activation.EnrollmentRevision &&
		repair.NewRevision == enrollment.Revision &&
		repair.NewRevision == repair.PreviousRevision+1 &&
		repair.AppliedHeight > activation.AppliedHeight, nil
}
