package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

const appV26DomainOwnershipHistoryPrefix = "appv26:domain-owner-history:"
const appV26DomainOwnershipHistorySequencePrefix = "appv26:domain-owner-history-sequence:"

// AppV26DomainOwnershipHistory preserves the authority transition separately
// from immutable memory authorship. Domain registry rows expose only the
// current owner; this record keeps the prior owner inspectable after an agent
// is removed and Root assumes operational control.
type AppV26DomainOwnershipHistory struct {
	Sequence        uint64 `json:"sequence"`
	Domain          string `json:"domain"`
	PreviousOwner   string `json:"previous_owner"`
	NewOwner        string `json:"new_owner"`
	DomainCreatedAt int64  `json:"domain_created_at"`
	TransferredAt   int64  `json:"transferred_at"`
	Reason          string `json:"reason"`
}

type appV26DomainOwnershipTransfer struct {
	name, parent, previousOwner, reason string
	domainCreatedAt                     int64
}

func appV26DomainOwnershipHistoryKey(domain string, height int64, sequence uint64) []byte {
	domainDigest := sha256.Sum256([]byte(domain))
	return []byte(fmt.Sprintf("%s%s:%020d:%020d",
		appV26DomainOwnershipHistoryPrefix,
		hex.EncodeToString(domainDigest[:]), height,
		sequence,
	))
}

func appV26DomainOwnershipHistorySequenceKey(domain string) []byte {
	domainDigest := sha256.Sum256([]byte(domain))
	return []byte(appV26DomainOwnershipHistorySequencePrefix + hex.EncodeToString(domainDigest[:]))
}

func (s *BadgerStore) nextAppV26DomainOwnershipHistorySequenceTxn(
	txn *badger.Txn, domain string,
) (uint64, error) {
	key := appV26DomainOwnershipHistorySequenceKey(domain)
	sequence := uint64(0)
	item, err := txn.Get(key)
	switch {
	case err == nil:
		value, valueErr := item.ValueCopy(nil)
		if valueErr != nil {
			return 0, valueErr
		}
		if len(value) != 8 {
			return 0, errors.New("invalid app-v26 domain ownership history sequence")
		}
		sequence = binary.BigEndian.Uint64(value)
	case errors.Is(err, badger.ErrKeyNotFound):
	default:
		return 0, err
	}
	if sequence == ^uint64(0) {
		return 0, errors.New("app-v26 domain ownership history sequence exhausted")
	}
	sequence++
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, sequence)
	if err := s.txnSet(txn, key, encoded); err != nil {
		return 0, err
	}
	return sequence, nil
}

var (
	ErrAppV26DomainOwnerChanged   = errors.New("app-v26 domain owner changed")
	ErrAppV26DomainOwnerUnchanged = errors.New("app-v26 domain owner is already the requested target")
	ErrAppV26GrantLimitExceeded   = errors.New("app-v26 domain grant purge limit exceeded")
	ErrAppV26InvalidOwnerTarget   = errors.New("app-v26 domain owner target is not an active mutable local principal")
)

// validateAppV26DomainOwnerTargetTxn repeats the transfer-target policy inside
// the same transaction as the owner CAS. CEREBRUM filters these targets for a
// useful operator experience, but consensus/store callers must not be able to
// bypass that filter with a raw DomainReassign transaction. Root identities,
// pending/inactive principals, Read-only principals, stale-generation Admins,
// and inconsistent role/profile projections can never become the current
// mutable owner of an ordinary domain.
func (s *BadgerStore) validateAppV26DomainOwnerTargetTxn(
	txn *badger.Txn, targetID string,
) error {
	var root AppV23RootState
	if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
		return err
	}
	if targetID == root.PrincipalID || targetID == root.CredentialID {
		return ErrAppV26InvalidOwnerTarget
	}
	if _, err := txn.Get(appV23RootCredentialKey(targetID)); err == nil {
		return ErrAppV26InvalidOwnerTarget
	} else if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}

	var enrollment AppV23LocalEnrollment
	if err := s.appV23ReadEffectiveJSONTxn(
		txn, appV23EnrollmentKey(targetID), &enrollment,
	); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrAppV26InvalidOwnerTarget
		}
		return err
	}
	if enrollment.AgentID != targetID || !enrollment.Active ||
		(enrollment.Profile != AppV23ProfileStandard &&
			enrollment.Profile != AppV23ProfileCompanion) {
		return ErrAppV26InvalidOwnerTarget
	}

	var role AppV23RoleState
	if err := s.appV23ReadEffectiveJSONTxn(txn, appV23RoleKey(targetID), &role); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrAppV26InvalidOwnerTarget
		}
		return err
	}
	if role.AgentID != targetID ||
		ValidateAppV23Policy(
			role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
		) != nil ||
		(role.Role == AppV23RoleAdmin && enrollment.RootGeneration != root.Generation) {
		return ErrAppV26InvalidOwnerTarget
	}

	var agent OnChainAgent
	if err := s.appV23ReadEffectiveJSONTxn(
		txn, appV23ProjectedAgentKey(targetID), &agent,
	); errors.Is(err, badger.ErrKeyNotFound) {
		if err := appV23ReadJSON(txn, agentOnChainKey(targetID), &agent); err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrAppV26InvalidOwnerTarget
			}
			return err
		}
	} else if err != nil {
		return err
	}
	if agent.AgentID != targetID || agent.Role != role.Role ||
		agent.Clearance != enrollment.Clearance ||
		agent.Capabilities != enrollment.Capabilities {
		return ErrAppV26InvalidOwnerTarget
	}
	return nil
}

// TransferDomainAppV26CAS applies one governed whole-domain authority change
// as one Badger transaction. The owner CAS, required-home invariant, preserved
// creation height, append-only ownership history, exact-domain grant purge,
// optional shared marker, and proposal consumption marker either all commit or
// none do. transitionID is the single-use governance proposal ID.
func (s *BadgerStore) TransferDomainAppV26CAS(
	name, newOwnerID, parentDomain, expectedOwnerID, transitionID string,
	height int64,
	makeShared bool,
	maxGrants int,
) (int, error) {
	if err := ValidateAppV23DomainName(name); err != nil {
		return 0, fmt.Errorf("invalid app-v26 transfer domain: %w", err)
	}
	if parentDomain != "" {
		if err := ValidateAppV23DomainName(parentDomain); err != nil {
			return 0, fmt.Errorf("invalid app-v26 transfer parent domain: %w", err)
		}
	}
	if err := validateCanonicalAgentID("new_owner_id", newOwnerID); err != nil {
		return 0, err
	}
	if err := validateCanonicalAgentID("expected_owner_id", expectedOwnerID); err != nil {
		return 0, err
	}
	if strings.TrimSpace(transitionID) == "" || height <= 0 || maxGrants < 0 {
		return 0, errors.New("invalid app-v26 domain transfer binding")
	}

	purged := 0
	err := s.withDomainOwnershipMutation(func() error {
		return s.update(func(txn *badger.Txn) error {
			if err := s.validateAppV26DomainOwnerTargetTxn(txn, newOwnerID); err != nil {
				return err
			}
			value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(name))
			if err != nil {
				return err
			}
			currentOwner, off, err := decodeString(value, 0)
			if err != nil {
				return err
			}
			currentParent, off, err := decodeString(value, off)
			if err != nil {
				return err
			}
			if len(value) < off+8 {
				return errors.New("invalid domain entry: short height")
			}
			createdHeight := int64(binary.BigEndian.Uint64(value[off : off+8])) // #nosec G115 -- stored consensus height
			if currentOwner != expectedOwnerID {
				return fmt.Errorf("%w: expected=%s current=%s",
					ErrAppV26DomainOwnerChanged, expectedOwnerID, currentOwner)
			}
			if currentOwner == newOwnerID {
				// A raw governed transaction must not bypass CEREBRUM's same-owner
				// guard. Treating this as a transfer would append false custody
				// history, consume the proposal, and destructively purge otherwise
				// valid grants despite changing no authority.
				return ErrAppV26DomainOwnerUnchanged
			}
			if parentDomain != "" && parentDomain != currentParent {
				return fmt.Errorf("app-v26 domain parent changed: expected=%q current=%q", parentDomain, currentParent)
			}
			parent := parentDomain
			if parent == "" {
				parent = currentParent
			}
			if err := s.appV23ValidateHomeDomainReleaseTxn(
				txn, name, currentOwner, newOwnerID, makeShared,
			); err != nil {
				return err
			}

			prefix := []byte("grant:" + name + ":")
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			grantKeys := make([][]byte, 0)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				keyDomain, _, canonical := parseCanonicalGrantKey(key)
				if !canonical || keyDomain != name {
					continue
				}
				if len(grantKeys) >= maxGrants {
					it.Close()
					return fmt.Errorf("%w: limit=%d", ErrAppV26GrantLimitExceeded, maxGrants)
				}
				grantKeys = append(grantKeys, it.Item().KeyCopy(nil))
			}
			it.Close()

			sequence, err := s.nextAppV26DomainOwnershipHistorySequenceTxn(txn, name)
			if err != nil {
				return err
			}
			history := AppV26DomainOwnershipHistory{
				Sequence: sequence,
				Domain:   name, PreviousOwner: currentOwner, NewOwner: newOwnerID,
				DomainCreatedAt: createdHeight, TransferredAt: height,
				Reason: "governed_domain_reassign",
			}
			encodedHistory, err := json.Marshal(history)
			if err != nil {
				return err
			}
			if err := s.txnSet(txn,
				appV26DomainOwnershipHistoryKey(name, height, sequence),
				encodedHistory,
			); err != nil {
				return err
			}
			if err := s.txnSet(txn, domainKey(name),
				appV23EncodeDomainWithParent(newOwnerID, parent, createdHeight)); err != nil {
				return err
			}
			for _, key := range grantKeys {
				if err := s.txnDelete(txn, key); err != nil {
					return err
				}
			}
			if makeShared {
				if err := s.txnSet(txn, stateKey("shared_domain:"+name), []byte{1}); err != nil {
					return err
				}
			}
			if err := s.txnSet(txn,
				stateKey("gov:proposal:"+transitionID+":consumed"), []byte{1},
			); err != nil {
				return err
			}
			purged = len(grantKeys)
			return nil
		})
	})
	return purged, err
}

// transferAppV26OwnedDomainsToRootTxn is part of the same consensus mutation
// as local-agent deactivation. It changes only domains currently owned by the
// retiring principal. Grants, shared-domain markers, memory author rows, and
// every unrelated domain are deliberately untouched.
func (s *BadgerStore) transferAppV26OwnedDomainsToRootTxn(
	txn *badger.Txn, retiringOwner, rootPrincipal string, height int64,
) (int, error) {
	if err := validateCanonicalAgentID("retiring domain owner", retiringOwner); err != nil {
		return 0, err
	}
	if err := validateCanonicalAgentID("root domain owner", rootPrincipal); err != nil {
		return 0, err
	}
	if retiringOwner == rootPrincipal || height <= 0 {
		return 0, errors.New("invalid app-v26 domain ownership retirement")
	}
	prefix := []byte("domain:")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	owned := make([]appV26DomainOwnershipTransfer, 0)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.KeyCopy(nil)
		value, err := item.ValueCopy(nil)
		if err != nil {
			it.Close()
			return 0, err
		}
		owner, off, err := decodeString(value, 0)
		if err != nil {
			it.Close()
			return 0, err
		}
		parent, off, err := decodeString(value, off)
		if err != nil {
			it.Close()
			return 0, err
		}
		if len(value) < off+8 {
			it.Close()
			return 0, errors.New("invalid domain entry: short height")
		}
		domainCreatedAt := int64(binary.BigEndian.Uint64(value[off : off+8])) // #nosec G115 -- stored consensus height
		if owner == retiringOwner {
			owned = append(owned, appV26DomainOwnershipTransfer{
				name: strings.TrimPrefix(string(key), string(prefix)), parent: parent,
				previousOwner: retiringOwner, reason: "agent_deactivated",
				domainCreatedAt: domainCreatedAt,
			})
		}
	}
	it.Close()
	sort.Slice(owned, func(i, j int) bool { return owned[i].name < owned[j].name })
	if err := s.applyAppV26DomainOwnershipTransfersTxn(txn, owned, rootPrincipal, height); err != nil {
		return 0, err
	}
	return len(owned), nil
}

func (s *BadgerStore) applyAppV26DomainOwnershipTransfersTxn(
	txn *badger.Txn,
	transfers []appV26DomainOwnershipTransfer,
	rootPrincipal string,
	height int64,
) error {
	if err := validateCanonicalAgentID("root domain owner", rootPrincipal); err != nil {
		return err
	}
	if height <= 0 {
		return errors.New("invalid app-v26 domain ownership transfer height")
	}
	for _, domain := range transfers {
		sequence, err := s.nextAppV26DomainOwnershipHistorySequenceTxn(txn, domain.name)
		if err != nil {
			return err
		}
		history := AppV26DomainOwnershipHistory{
			Sequence: sequence,
			Domain:   domain.name, PreviousOwner: domain.previousOwner,
			NewOwner: rootPrincipal, DomainCreatedAt: domain.domainCreatedAt,
			TransferredAt: height,
			Reason:        domain.reason,
		}
		encoded, err := json.Marshal(history)
		if err != nil {
			return err
		}
		if err := s.txnSet(txn,
			appV26DomainOwnershipHistoryKey(domain.name, height, sequence), encoded,
		); err != nil {
			return err
		}
		if err := s.txnSet(txn, domainKey(domain.name),
			appV23EncodeDomainWithParent(rootPrincipal, domain.parent, domain.domainCreatedAt)); err != nil {
			return err
		}
	}
	return nil
}

// reconcileAppV26InactiveDomainOwnersToRootTxn is the activation migration
// for current domain authority. Canonical local identities that are inactive,
// absent from the app-v23 enrollment directory, or historical Root
// credentials are normalized to the stable Root principal. Non-canonical
// legacy owner labels cannot identify an active local directory principal, so
// Root becomes the auditable current owner while the exact prior label remains
// preserved in immutable transition history for later governed reassignment.
func (s *BadgerStore) reconcileAppV26InactiveDomainOwnersToRootTxn(
	txn *badger.Txn,
	root AppV23RootState,
	height int64,
) (int, error) {
	if err := validateCanonicalAgentID("root domain owner", root.PrincipalID); err != nil {
		return 0, err
	}
	prefix := []byte("domain:")
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	transfers := make([]appV26DomainOwnershipTransfer, 0)
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().KeyCopy(nil)
		value, err := it.Item().ValueCopy(nil)
		if err != nil {
			it.Close()
			return 0, err
		}
		owner, off, err := decodeString(value, 0)
		if err != nil {
			it.Close()
			return 0, err
		}
		parent, off, err := decodeString(value, off)
		if err != nil {
			it.Close()
			return 0, err
		}
		if len(value) < off+8 {
			it.Close()
			return 0, errors.New("invalid domain entry: short height")
		}
		domainCreatedAt := int64(binary.BigEndian.Uint64(value[off : off+8])) // #nosec G115 -- stored consensus height
		if owner == root.PrincipalID {
			continue
		}
		reason := ""
		if !isCanonicalAgentID(owner) {
			reason = "noncanonical_owner_reconciled"
		} else if _, rootErr := txn.Get(appV23RootCredentialKey(owner)); rootErr == nil {
			reason = "root_credential_normalized"
		} else if !errors.Is(rootErr, badger.ErrKeyNotFound) {
			it.Close()
			return 0, rootErr
		} else {
			var enrollment AppV23LocalEnrollment
			enrollmentErr := s.appV23ReadEffectiveJSONTxn(
				txn, appV23EnrollmentKey(owner), &enrollment,
			)
			switch {
			case enrollmentErr == nil:
				if enrollment.AgentID != owner {
					it.Close()
					return 0, errors.New("app-v26 domain owner enrollment identity mismatch")
				}
				if enrollment.Active {
					continue
				}
				reason = "inactive_agent_reconciled"
			case errors.Is(enrollmentErr, badger.ErrKeyNotFound):
				reason = "missing_agent_reconciled"
			default:
				it.Close()
				return 0, enrollmentErr
			}
		}
		transfers = append(transfers, appV26DomainOwnershipTransfer{
			name: strings.TrimPrefix(string(key), string(prefix)), parent: parent,
			previousOwner: owner, reason: reason,
			domainCreatedAt: domainCreatedAt,
		})
	}
	it.Close()
	sort.Slice(transfers, func(i, j int) bool { return transfers[i].name < transfers[j].name })
	if err := s.applyAppV26DomainOwnershipTransfersTxn(
		txn, transfers, root.PrincipalID, height,
	); err != nil {
		return 0, err
	}
	return len(transfers), nil
}

// ListAppV26DomainOwnershipHistory returns the auditable owner transitions for
// one domain in consensus order. It never rewrites or infers memory authorship.
func (s *BadgerStore) ListAppV26DomainOwnershipHistory(
	domain string,
) ([]AppV26DomainOwnershipHistory, error) {
	if err := ValidateAppV23DomainName(domain); err != nil {
		return nil, err
	}
	digest := sha256.Sum256([]byte(domain))
	prefix := []byte(appV26DomainOwnershipHistoryPrefix + hex.EncodeToString(digest[:]) + ":")
	result := make([]AppV26DomainOwnershipHistory, 0)
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var record AppV26DomainOwnershipHistory
			if err := it.Item().Value(func(value []byte) error {
				return json.Unmarshal(value, &record)
			}); err != nil {
				return err
			}
			if record.Domain != domain {
				return errors.New("app-v26 domain ownership history digest mismatch")
			}
			result = append(result, record)
		}
		return nil
	})
	return result, err
}
