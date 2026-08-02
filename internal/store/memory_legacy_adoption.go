package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const memoryLegacyAdoptionReceiptPrefix = "appv25:legacy-adoption:"

// MemoryLegacyAdoptionEntry is consensus-ready evidence for one recoverable
// historical projection row. It intentionally excludes plaintext.
type MemoryLegacyAdoptionEntry struct {
	MemoryID        string
	Status          string
	ContentHash     []byte
	Domain          string
	Author          string
	AuthorPrincipal string
	Classification  uint8
}

// MemoryLegacyAdoptionResult reports deterministic whole-batch dispositions.
// A conflicting entry rejects the complete consensus batch before mutation;
// the off-consensus planner isolates it into recovery and proposes later
// eligible entries in a new atomic batch.
type MemoryLegacyAdoptionResult struct {
	Adopted  int
	Existing int
}

// MemoryLegacyAdoptionCandidateState is one off-consensus planner inspection
// from a shared Badger read snapshot.
type MemoryLegacyAdoptionCandidateState struct {
	MemoryID string
	Receipt  bool
	State    *MemoryDisclosureState
	Err      error
}

// ErrMemoryLegacyAdoptionConflict means the local plan no longer matches
// canonical business state. The proposal must be rejected atomically and the
// off-consensus planner may isolate/replan the affected row.
var ErrMemoryLegacyAdoptionConflict = errors.New("memory legacy adoption conflict")

func memoryLegacyAdoptionReceiptKey(memoryID string) []byte {
	return []byte(memoryLegacyAdoptionReceiptPrefix + memoryID)
}

func memoryLegacyAdoptionEntryDigest(planDigest []byte, entry MemoryLegacyAdoptionEntry) []byte {
	h := sha256.New()
	_, _ = h.Write([]byte("sage/memory-legacy-adoption/receipt/v1\x00"))
	_, _ = h.Write(planDigest)
	write := func(value []byte) {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value))) // #nosec G115 -- protocol bounds
		_, _ = h.Write(size[:])
		_, _ = h.Write(value)
	}
	write([]byte(entry.MemoryID))
	write([]byte(entry.Status))
	write(entry.ContentHash)
	write([]byte(entry.Domain))
	write([]byte(entry.Author))
	write([]byte(entry.AuthorPrincipal))
	_, _ = h.Write([]byte{entry.Classification})
	return h.Sum(nil)
}

// AdoptLegacyMemories atomically installs every entry in the proposed batch.
// A conflict rejects the complete batch before any write. The local planner
// isolates/replans that row and continues with later bounded batches, keeping
// on-chain receipts unambiguous while ensuring one bad row cannot stall the
// whole migration. Existing exact receipts are idempotent no-ops.
func (s *BadgerStore) AdoptLegacyMemories(
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
) (MemoryLegacyAdoptionResult, error) {
	return s.adoptLegacyMemories(planDigest, entries, false)
}

// AdoptLegacyMemoriesAppV26 permits a Root-governed historical author to be
// mapped onto an active ordinary local principal. It preserves memauthor and
// changes only the separate policy-principal projection.
func (s *BadgerStore) AdoptLegacyMemoriesAppV26(
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
) (MemoryLegacyAdoptionResult, error) {
	return s.adoptLegacyMemories(planDigest, entries, true)
}

func (s *BadgerStore) adoptLegacyMemories(
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
	allowAssignedPrincipal bool,
) (MemoryLegacyAdoptionResult, error) {
	var result MemoryLegacyAdoptionResult
	err := s.update(func(txn *badger.Txn) error {
		var (
			prepared []preparedMemoryLegacyAdoption
			err      error
		)
		result, prepared, err = validateMemoryLegacyAdoptionTxn(
			s, txn, planDigest, entries, allowAssignedPrincipal,
		)
		if err != nil {
			return err
		}

		// Pass 2 publishes the complete validated batch atomically.
		for _, candidate := range prepared {
			entry := candidate.entry
			for _, marker := range candidate.creditedVoteMarkers {
				if err := s.txnSet(txn, marker, []byte{1}); err != nil {
					return fmt.Errorf("record legacy vote credit for %s: %w", entry.MemoryID, err)
				}
			}
			for _, key := range candidate.staleVoteKeys {
				if err := s.txnDelete(txn, key); err != nil {
					return fmt.Errorf("clear stale legacy vote for %s: %w", entry.MemoryID, err)
				}
			}
			writes := []struct {
				key   []byte
				value []byte
			}{
				{memoryKey(entry.MemoryID), encodeMemoryHashEntry(entry.ContentHash, entry.Status)},
				{memoryDomainKey(entry.MemoryID), []byte(entry.Domain)},
				{memoryAuthorKey(entry.MemoryID), []byte(entry.Author)},
				{memoryAuthorPrincipalKey(entry.MemoryID), []byte(entry.AuthorPrincipal)},
				{memClassKey(entry.MemoryID), []byte{entry.Classification}},
				{memoryLegacyAdoptionReceiptKey(entry.MemoryID), candidate.digest},
			}
			for _, write := range writes {
				if err := s.txnSet(txn, write.key, write.value); err != nil {
					return fmt.Errorf("adopt legacy memory %s: %w", entry.MemoryID, err)
				}
			}
			result.Adopted++
		}
		return nil
	})
	return result, err
}

type preparedMemoryLegacyAdoption struct {
	entry               MemoryLegacyAdoptionEntry
	digest              []byte
	staleVoteKeys       [][]byte
	creditedVoteMarkers [][]byte
}

const memoryLegacyRevoteStatePrefix = "appv25:legacy_revote_credit:"

func memoryLegacyRevoteCreditStateKey(memoryID, validatorID string) string {
	return memoryLegacyRevoteStatePrefix + memoryID + ":" + validatorID
}

// ConsumeMemoryLegacyRevoteCredit suppresses duplicate PoE accounting for the
// first validator ballot recast after app-v25 repairs a historical proposed
// row. The replacement ballot still participates in fresh quorum calculation.
func (s *BadgerStore) ConsumeMemoryLegacyRevoteCredit(
	memoryID, validatorID string,
) (bool, error) {
	key := stateKey(memoryLegacyRevoteCreditStateKey(memoryID, validatorID))
	found := false
	err := s.update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		switch {
		case err == nil:
			found = true
			return s.txnDelete(txn, key)
		case errors.Is(err, badger.ErrKeyNotFound):
			return nil
		default:
			return err
		}
	})
	return found, err
}

// ValidateMemoryLegacyAdoptions rechecks a proposed batch in one read-only
// canonical snapshot. It is used immediately before governance execution.
func (s *BadgerStore) ValidateMemoryLegacyAdoptions(
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
) error {
	return s.validateMemoryLegacyAdoptions(planDigest, entries, false)
}

func (s *BadgerStore) ValidateMemoryLegacyAdoptionsAppV26(
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
) error {
	return s.validateMemoryLegacyAdoptions(planDigest, entries, true)
}

func (s *BadgerStore) validateMemoryLegacyAdoptions(
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
	allowAssignedPrincipal bool,
) error {
	return s.view(func(txn *badger.Txn) error {
		_, _, err := validateMemoryLegacyAdoptionTxn(
			s, txn, planDigest, entries, allowAssignedPrincipal,
		)
		return err
	})
}

func validateMemoryLegacyAdoptionTxn(
	s *BadgerStore,
	txn *badger.Txn,
	planDigest []byte,
	entries []MemoryLegacyAdoptionEntry,
	allowAssignedPrincipal bool,
) (MemoryLegacyAdoptionResult, []preparedMemoryLegacyAdoption, error) {
	var result MemoryLegacyAdoptionResult
	if len(planDigest) != sha256.Size {
		return result, nil, fmt.Errorf("legacy adoption plan digest length %d, want %d", len(planDigest), sha256.Size)
	}
	if len(entries) == 0 || len(entries) > 256 {
		return result, nil, fmt.Errorf("legacy adoption entry count %d is outside 1..256", len(entries))
	}
	prepared := make([]preparedMemoryLegacyAdoption, 0, len(entries))
	for _, entry := range entries {
		entryDigest := memoryLegacyAdoptionEntryDigest(planDigest, entry)
		receipt, receiptErr := txn.Get(memoryLegacyAdoptionReceiptKey(entry.MemoryID))
		switch {
		case receiptErr == nil:
			var existingDigest []byte
			if err := receipt.Value(func(value []byte) error {
				existingDigest = append([]byte(nil), value...)
				return nil
			}); err != nil {
				return result, nil, fmt.Errorf("read legacy adoption receipt %s: %w", entry.MemoryID, err)
			}
			if !bytes.Equal(existingDigest, entryDigest) {
				return result, nil, fmt.Errorf("%w: receipt mismatch for %s", ErrMemoryLegacyAdoptionConflict, entry.MemoryID)
			}
			if err := validateAdoptedLegacyMemoryTxn(txn, entry); err != nil {
				return result, nil, fmt.Errorf("%w: receipt state for %s: %v", ErrMemoryLegacyAdoptionConflict, entry.MemoryID, err)
			}
			result.Existing++
			continue
		case !errors.Is(receiptErr, badger.ErrKeyNotFound):
			return result, nil, fmt.Errorf("lookup legacy adoption receipt %s: %w", entry.MemoryID, receiptErr)
		}
		if _, memoryErr := txn.Get(memoryKey(entry.MemoryID)); memoryErr == nil {
			repairable, repairErr := legacyAdoptionHashlessEnvelopeMatchesTxn(
				txn, entry,
			)
			if repairErr != nil {
				return result, nil, fmt.Errorf(
					"inspect existing canonical memory %s: %w",
					entry.MemoryID, repairErr,
				)
			}
			if !repairable {
				return result, nil, fmt.Errorf(
					"%w: canonical memory already exists for %s",
					ErrMemoryLegacyAdoptionConflict, entry.MemoryID,
				)
			}
		} else if !errors.Is(memoryErr, badger.ErrKeyNotFound) {
			return result, nil, fmt.Errorf("lookup canonical memory %s: %w", entry.MemoryID, memoryErr)
		}
		if entry.Status != "proposed" &&
			entry.Status != "committed" &&
			entry.Status != "deprecated" ||
			len(entry.ContentHash) != sha256.Size ||
			entry.MemoryID == "" || entry.Domain == "" || entry.Author == "" ||
			entry.AuthorPrincipal == "" ||
			entry.Classification > uint8(ClearanceTopSecret) {
			return result, nil, fmt.Errorf("%w: malformed entry for %s", ErrMemoryLegacyAdoptionConflict, entry.MemoryID)
		}
		if principalErr := validateLegacyAdoptionPrincipalTxn(
			s, txn, entry.Author, entry.AuthorPrincipal, allowAssignedPrincipal,
		); principalErr != nil {
			return result, nil, fmt.Errorf(
				"%w: principal mapping for %s: %v",
				ErrMemoryLegacyAdoptionConflict, entry.MemoryID, principalErr,
			)
		}
		candidate := preparedMemoryLegacyAdoption{entry: entry, digest: entryDigest}
		if entry.Status == "proposed" {
			var err error
			candidate.staleVoteKeys, candidate.creditedVoteMarkers, err =
				legacyAdoptionStaleVoteKeysTxn(txn, entry.MemoryID)
			if err != nil {
				return result, nil, err
			}
		}
		prepared = append(prepared, candidate)
	}
	return result, prepared, nil
}

// legacyAdoptionStaleVoteKeysTxn forgets only pre-adoption ballot receipts.
// A SQL-only or hashless proposed row could have accumulated votes that never
// had a valid canonical target. Reusing those receipts would strand the newly
// adopted envelope because the ordinary voter would believe it had already
// voted. Exact adoption-receipt replays never enter this path, so valid votes
// cast after adoption are preserved.
func legacyAdoptionStaleVoteKeysTxn(
	txn *badger.Txn,
	memoryID string,
) (keys, creditedVoteMarkers [][]byte, err error) {
	const maxStaleVoteKeysPerMemory = 1024
	votePrefix := []byte("state:vote:" + memoryID + ":")
	prefixes := [][]byte{
		votePrefix,
		[]byte("state:scope-vote-height:" + memoryID + ":"),
	}
	keys = make([][]byte, 0)
	for _, prefix := range prefixes {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if len(keys) >= maxStaleVoteKeysPerMemory {
				it.Close()
				return nil, nil, fmt.Errorf(
					"%w: stale vote set for %s exceeds %d keys",
					ErrMemoryLegacyAdoptionConflict,
					memoryID,
					maxStaleVoteKeysPerMemory,
				)
			}
			key := it.Item().KeyCopy(nil)
			keys = append(keys, key)
			if bytes.HasPrefix(key, votePrefix) {
				validatorID := string(key[len(votePrefix):])
				if validatorID != "" {
					creditedVoteMarkers = append(
						creditedVoteMarkers,
						stateKey(memoryLegacyRevoteCreditStateKey(memoryID, validatorID)),
					)
				}
			}
		}
		it.Close()
	}
	return keys, creditedVoteMarkers, nil
}

// legacyAdoptionHashlessEnvelopeMatchesTxn recognizes the narrow historical
// transition defect repaired by app-v25: a canonical envelope whose
// status and every non-hash disclosure field already match the attested SQL
// row, but whose content hash was replaced with a zero-length value. No other
// existing canonical state is eligible for adoption or overwrite.
func legacyAdoptionHashlessEnvelopeMatchesTxn(
	txn *badger.Txn,
	entry MemoryLegacyAdoptionEntry,
) (bool, error) {
	item, err := txn.Get(memoryKey(entry.MemoryID))
	if err != nil {
		return false, err
	}
	var contentHash []byte
	var status string
	if err := item.Value(func(value []byte) error {
		var decodeErr error
		contentHash, status, decodeErr = decodeMemoryHashEntry(value)
		return decodeErr
	}); err != nil {
		return false, err
	}
	if len(contentHash) != 0 || status != entry.Status {
		return false, nil
	}
	expect := map[string][]byte{
		string(memoryDomainKey(entry.MemoryID)): []byte(entry.Domain),
		string(memoryAuthorKey(entry.MemoryID)): []byte(entry.Author),
		string(memClassKey(entry.MemoryID)):     []byte{entry.Classification},
	}
	for rawKey, expected := range expect {
		field, fieldErr := txn.Get([]byte(rawKey))
		if errors.Is(fieldErr, badger.ErrKeyNotFound) {
			return false, nil
		}
		if fieldErr != nil {
			return false, fieldErr
		}
		matches := false
		if valueErr := field.Value(func(value []byte) error {
			matches = bytes.Equal(value, expected)
			return nil
		}); valueErr != nil {
			return false, valueErr
		}
		if !matches {
			return false, nil
		}
	}
	if principal, principalErr := txn.Get(
		memoryAuthorPrincipalKey(entry.MemoryID),
	); principalErr == nil {
		matches := false
		if valueErr := principal.Value(func(value []byte) error {
			matches = bytes.Equal(value, []byte(entry.AuthorPrincipal))
			return nil
		}); valueErr != nil {
			return false, valueErr
		}
		if !matches {
			return false, nil
		}
	} else if !errors.Is(principalErr, badger.ErrKeyNotFound) {
		return false, principalErr
	}
	return true, nil
}

func validateLegacyAdoptionPrincipalTxn(
	s *BadgerStore,
	txn *badger.Txn,
	author, principal string,
	allowAssignedPrincipal bool,
) error {
	var root AppV23RootState
	if err := appV23ReadJSON(txn, appV23RootKey(), &root); err != nil {
		return fmt.Errorf("read Root state: %w", err)
	}
	if principal == root.PrincipalID {
		if _, err := txn.Get(appV23RootCredentialKey(author)); err != nil {
			return fmt.Errorf("author is not a durable Root credential: %w", err)
		}
		return nil
	}
	if principal != author {
		if !allowAssignedPrincipal {
			return errors.New("ordinary agent principal must equal its immutable credential ID")
		}
		var enrollment AppV23LocalEnrollment
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23EnrollmentKey(principal), &enrollment,
		); err != nil {
			return fmt.Errorf("read assigned local enrollment: %w", err)
		}
		var role AppV23RoleState
		if err := s.appV23ReadEffectiveJSONTxn(
			txn, appV23RoleKey(principal), &role,
		); err != nil {
			return fmt.Errorf("read assigned local role: %w", err)
		}
		if enrollment.AgentID != principal || !enrollment.Active ||
			(enrollment.Profile != AppV23ProfileStandard &&
				enrollment.Profile != AppV23ProfileCompanion) ||
			role.AgentID != principal || !ValidAppV23Role(role.Role) ||
			!AppV23ProfileAllowsRole(enrollment.Profile, role.Role) {
			return errors.New("assigned principal must be an active ordinary local agent")
		}
		return nil
	}
	var enrollment AppV23LocalEnrollment
	if err := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(principal), &enrollment); err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("read local enrollment: %w", err)
		}
		// Historical authorship is immutable provenance, not current authority.
		// Older SAGE nodes legitimately contain memories signed by per-project or
		// retired Ed25519 identities that were never enrolled in app-v23. Preserve
		// that exact identity without creating an enrollment, role, grant, or
		// capability. Non-canonical legacy labels remain recovery-only.
		if !isCanonicalAgentID(author) {
			return errors.New("unenrolled historical author is not a canonical agent identity")
		}
		if _, rootErr := txn.Get(appV23RootCredentialKey(author)); rootErr == nil {
			return errors.New("historical Root credential must resolve to the permanent Root principal")
		} else if !errors.Is(rootErr, badger.ErrKeyNotFound) {
			return fmt.Errorf("read historical Root credential: %w", rootErr)
		}
		return nil
	}
	if enrollment.AgentID != principal {
		return errors.New("local enrollment identity mismatch")
	}
	return nil
}

func validateAdoptedLegacyMemoryTxn(txn *badger.Txn, entry MemoryLegacyAdoptionEntry) error {
	expect := map[string][]byte{
		string(memoryKey(entry.MemoryID)):                encodeMemoryHashEntry(entry.ContentHash, entry.Status),
		string(memoryDomainKey(entry.MemoryID)):          []byte(entry.Domain),
		string(memoryAuthorKey(entry.MemoryID)):          []byte(entry.Author),
		string(memoryAuthorPrincipalKey(entry.MemoryID)): []byte(entry.AuthorPrincipal),
		string(memClassKey(entry.MemoryID)):              []byte{entry.Classification},
	}
	for rawKey, expected := range expect {
		item, err := txn.Get([]byte(rawKey))
		if err != nil {
			return err
		}
		if err := item.Value(func(value []byte) error {
			if !bytes.Equal(value, expected) {
				return errors.New("canonical field mismatch")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// HasMemoryLegacyAdoptionReceipt reports whether consensus has already adopted
// the memory. It is intended for local progress reporting only.
func (s *BadgerStore) HasMemoryLegacyAdoptionReceipt(memoryID string) (bool, error) {
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(memoryLegacyAdoptionReceiptKey(memoryID))
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// InspectMemoryLegacyAdoptionCandidates batches receipt + canonical-envelope
// reads in one snapshot. A large legacy inventory therefore needs one Badger
// transaction per SQL page rather than two transactions per row.
func (s *BadgerStore) InspectMemoryLegacyAdoptionCandidates(
	memoryIDs []string,
) ([]MemoryLegacyAdoptionCandidateState, error) {
	if len(memoryIDs) == 0 {
		return nil, nil
	}
	if len(memoryIDs) > 1024 {
		return nil, fmt.Errorf("legacy adoption candidate inspection exceeds 1024 rows")
	}
	results := make([]MemoryLegacyAdoptionCandidateState, len(memoryIDs))
	err := s.view(func(txn *badger.Txn) error {
		reader := &BadgerStore{db: s.db, txn: txn}
		for i, memoryID := range memoryIDs {
			results[i].MemoryID = memoryID
			if memoryID == "" {
				results[i].Err = errors.New("memory disclosure state requires a memory id")
				continue
			}
			if _, err := txn.Get(memoryLegacyAdoptionReceiptKey(memoryID)); err == nil {
				results[i].Receipt = true
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("inspect legacy adoption receipt %s: %w", memoryID, err)
			}
			state, err := reader.GetMemoryDisclosureState(memoryID)
			switch {
			case err == nil:
				results[i].State = state
			case errors.Is(err, ErrMemoryDisclosureNotFound),
				errors.Is(err, ErrMemoryDisclosureCorrupt):
				results[i].Err = err
			default:
				return fmt.Errorf("inspect canonical memory %s: %w", memoryID, err)
			}
		}
		return nil
	})
	return results, err
}
