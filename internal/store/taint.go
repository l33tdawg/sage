package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

var (
	ErrTaintStateMalformed = errors.New("taint consensus state is malformed")
	ErrTaintCapacity       = errors.New("active taint decision capacity is exhausted")
	ErrTaintConflict       = errors.New("taint decision conflicts with existing state")
	ErrTaintNotActive      = errors.New("taint decision is not active")
	ErrTaintBudget         = errors.New("taint admission budget exceeded")
)

type InheritedTaintResult struct {
	Decisions int
	Units     int
}

// ApplyTaintDecision installs the target membership and first frontier entry
// for an accepted apply proposal. Callers validate the target against canonical
// memory metadata before entering this atomic mutation.
func (s *BadgerStore) ApplyTaintDecision(
	localChainID, decisionID string,
	target tx.ParentRef,
	payloadHash [sha256.Size]byte,
) (bool, error) {
	return s.ApplyTaintDecisionWithScope(
		localChainID,
		decisionID,
		target,
		payloadHash,
		TaintDecisionScope{Kind: tx.TaintScopeDirect},
	)
}

// ApplyTaintDecisionWithScope installs the target membership, immutable scope
// state and first frontier entry for an accepted apply proposal.
func (s *BadgerStore) ApplyTaintDecisionWithScope(
	localChainID, decisionID string,
	target tx.ParentRef,
	payloadHash [sha256.Size]byte,
	decisionScope TaintDecisionScope,
) (bool, error) {
	if err := taint.ValidateDecisionID(decisionID); err != nil {
		return false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	if len(localChainID) == 0 || len(localChainID) > tx.MaxDerivedFromChainIDBytes ||
		target.SourceChainID != localChainID {
		return false, fmt.Errorf("%w: invalid local taint target chain", ErrTaintStateMalformed)
	}
	targetID, err := tx.ParentRefIDHex(target)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	membership, err := taint.EncodeMembership(taint.Membership{
		Generation: 1,
		Depth:      0,
		Origin:     taint.OriginTarget,
	})
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	queueEntry, err := taint.EncodeQueueEntry(taint.QueueEntry{
		ParentRefID: targetID,
		MemoryID:    target.MemoryID,
		Depth:       0,
	})
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	headerBytes, err := taint.EncodeHeader(taint.Header{
		Action:       taint.ActionApply,
		Status:       taint.StatusExpanding,
		Generation:   1,
		FrontierTail: 1,
		Materialized: 1,
		PayloadHash:  payloadHash,
	})
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}

	scopeBytes, err := encodeTaintDecisionScope(payloadHash, decisionScope)
	if err != nil {
		return false, err
	}

	anchorBytes, err := encodeTaintTargetAnchor(taintTargetAnchor{Target: target, PayloadHash: payloadHash})
	if err != nil {
		return false, err
	}

	created := false
	updateErr := s.update(func(txn *badger.Txn) error {
		existing, found, err := readTaintHeaderTxn(txn, decisionID)
		if err != nil {
			return err
		}
		anchor, anchorFound, err := readTaintTargetTxn(txn, decisionID)
		if err != nil {
			return err
		}
		if found {
			if !anchorFound || anchor.PayloadHash != existing.PayloadHash {
				return fmt.Errorf("%w: missing or inconsistent target anchor", ErrTaintStateMalformed)
			}
			if existing.Action != taint.ActionApply ||
				existing.PayloadHash != payloadHash || anchor.Target != target {
				return ErrTaintConflict
			}
			storedScope, found, scopeErr := readTaintDecisionScopeTxn(txn, decisionID, payloadHash, nil)
			if scopeErr != nil {
				return scopeErr
			}
			if !found {
				return fmt.Errorf("%w: existing decision has no scope", ErrTaintStateMalformed)
			}
			if !equalTaintDecisionScope(storedScope, decisionScope) {
				return ErrTaintConflict
			}
			_, indexed, indexErr := readBoundedMeteredTaintValueTxn(txn, taint.ActiveDecisionKey(decisionID), 0, nil)
			if indexErr != nil {
				return indexErr
			}
			if indexed != (existing.Status.Active() || existing.Status == taint.StatusCleaning) {
				return fmt.Errorf("%w: decision active index is inconsistent", ErrTaintStateMalformed)
			}
			return nil
		}
		if anchorFound {
			return fmt.Errorf("%w: target anchor has no decision header", ErrTaintStateMalformed)
		}
		_, scopeFound, scopeErr := readBoundedMeteredTaintValueTxn(txn, TaintDecisionScopeKey(decisionID), taintScopeValueMaxBytes, nil)
		if scopeErr != nil {
			return scopeErr
		}
		_, indexed, indexErr := readBoundedMeteredTaintValueTxn(txn, taint.ActiveDecisionKey(decisionID), 0, nil)
		if indexErr != nil {
			return indexErr
		}
		if scopeFound || indexed {
			return fmt.Errorf("%w: scope/index has no decision header", ErrTaintStateMalformed)
		}
		active, err := readTaintUint64Txn(txn, taint.ActiveCountKey())
		if err != nil {
			return err
		}
		if active >= taint.MaxActiveDecisions {
			return ErrTaintCapacity
		}
		writes := []struct {
			key   []byte
			value []byte
		}{
			{taint.DecisionKey(decisionID), headerBytes},
			{taint.TargetKey(decisionID), anchorBytes},
			{TaintDecisionScopeKey(decisionID), scopeBytes},
			{taint.ActiveDecisionKey(decisionID), []byte{}},
			{taint.MembershipKey(target.MemoryID, decisionID), membership},
			{taint.DecisionIndexKey(decisionID, target.MemoryID), []byte{}},
			{taint.FrontierKey(decisionID, 1, 0), queueEntry},
		}
		for _, write := range writes {
			if err := s.txnSet(txn, write.key, write.value); err != nil {
				return err
			}
		}
		if err := writeTaintUint64Txn(s, txn, taint.ActiveCountKey(), active+1); err != nil {
			return err
		}
		if err := bumpTaintRevisionTxn(s, txn); err != nil {
			return err
		}
		created = true
		return nil
	})
	return created, updateErr
}

func (s *BadgerStore) GetTaintDecision(decisionID string) (taint.Header, bool, error) {
	if err := taint.ValidateDecisionID(decisionID); err != nil {
		return taint.Header{}, false, err
	}
	var header taint.Header
	found := false
	err := s.view(func(txn *badger.Txn) error {
		var err error
		header, found, err = readTaintHeaderTxn(txn, decisionID)
		return err
	})
	return header, found, err
}

// GetEffectiveTaintDecisions returns sorted decision IDs whose active header
// generation matches a materialized membership on the memory.
func (s *BadgerStore) GetEffectiveTaintDecisions(memoryID string) ([]string, error) {
	var decisions []string
	err := s.view(func(txn *badger.Txn) error {
		var err error
		decisions, _, err = getEffectiveTaintDecisionsTxn(txn, memoryID, nil)
		return err
	})
	return decisions, err
}

func getEffectiveTaintDecisionsTxn(txn *badger.Txn, memoryID string, meter *taintUnitMeter) ([]string, map[string]taint.Header, error) {
	if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
		return nil, nil, fmt.Errorf("%w: invalid memory ID", ErrTaintStateMalformed)
	}
	prefix := taint.MembershipPrefix(memoryID)
	if err := chargeTaintUnits(meter, 1); err != nil {
		return nil, nil, err
	}
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := txn.NewIterator(options)
	defer it.Close()
	decisions := make([]string, 0, taint.MaxActiveDecisions)
	headers := make(map[string]taint.Header)
	entries := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if entries == taint.MaxActiveDecisions {
			return nil, nil, fmt.Errorf("%w: too many memberships", ErrTaintStateMalformed)
		}
		entries++
		if chargeErr := chargeTaintUnits(meter, 1); chargeErr != nil {
			return nil, nil, chargeErr
		}
		key := it.Item().Key()
		if len(key) != len(prefix)+taint.DecisionIDBytes || it.Item().ValueSize() != 11 {
			return nil, nil, fmt.Errorf("%w: malformed membership key/value", ErrTaintStateMalformed)
		}
		id := string(key[len(prefix):])
		if err := taint.ValidateDecisionID(id); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
		raw, err := it.Item().ValueCopy(nil)
		if err != nil {
			return nil, nil, err
		}
		membership, err := taint.DecodeMembership(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
		if chargeErr := chargeTaintUnits(meter, 1); chargeErr != nil {
			return nil, nil, chargeErr
		}
		header, found, err := readTaintHeaderTxn(txn, id)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			return nil, nil, fmt.Errorf("%w: missing membership header", ErrTaintStateMalformed)
		}
		headers[id] = header
		if header.MembershipEffective(membership) {
			decisions = append(decisions, id)
		}
	}
	sort.Strings(decisions)
	return decisions, headers, nil
}

// ClearTaintDecision withdraws one apply decision before cleanup. Exact replay
// on a cleaning/clean decision is idempotent.
func (s *BadgerStore) ClearTaintDecision(decisionID string) (bool, error) {
	if err := taint.ValidateDecisionID(decisionID); err != nil {
		return false, err
	}
	changed := false
	err := s.update(func(txn *badger.Txn) error {
		header, found, err := readTaintHeaderTxn(txn, decisionID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: unknown decision %s", ErrTaintNotActive, decisionID)
		}
		if header.Action != taint.ActionApply {
			return ErrTaintStateMalformed
		}
		_, scoped, scopeErr := readTaintDecisionScopeTxn(txn, decisionID, header.PayloadHash, nil)
		if scopeErr != nil {
			return scopeErr
		}
		_, indexed, indexErr := readBoundedMeteredTaintValueTxn(txn, taint.ActiveDecisionKey(decisionID), 0, nil)
		if indexErr != nil {
			return indexErr
		}
		if !scoped || indexed != (header.Status.Active() || header.Status == taint.StatusCleaning) {
			return fmt.Errorf("%w: decision scope/active index is inconsistent", ErrTaintStateMalformed)
		}
		if header.Status == taint.StatusCleaning || header.Status == taint.StatusClean {
			return nil
		}
		if !header.Status.Active() {
			return ErrTaintNotActive
		}
		if header.Generation == ^uint64(0) {
			return fmt.Errorf("%w: generation overflow", ErrTaintStateMalformed)
		}
		header.Status = taint.StatusCleaning
		header.Generation++
		header.CleanupPhase = taint.CleanupMembers
		header.ParentRefID = ""
		header.ParentMemoryID = ""
		header.ParentDepth = 0
		header.LastEdge = ""
		encoded, err := taint.EncodeHeader(header)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
		if err := s.txnSet(txn, taint.DecisionKey(decisionID), encoded); err != nil {
			return err
		}
		if err := bumpTaintRevisionTxn(s, txn); err != nil {
			return err
		}
		changed = true
		return nil
	})
	return changed, err
}

// InheritTaintMemberships copies every effective membership from the exact
// resolved parent memory IDs to a new child. The caller writes the matching
// lineage edges and provenance marker in the same consensus transaction.
func (s *BadgerStore) InheritTaintMemberships(
	localChainID, childID string,
	parents []tx.ParentRef,
) (InheritedTaintResult, error) {
	var result InheritedTaintResult
	if len(parents) == 0 {
		return result, nil
	}
	if len(childID) == 0 || len(childID) > taint.MaxMemoryIDBytes {
		return result, fmt.Errorf("%w: invalid child memory ID", ErrTaintStateMalformed)
	}
	if _, err := tx.EncodeDerivedFromExtension(parents); err != nil {
		return result, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	err := s.update(func(txn *badger.Txn) error {
		plan, err := planTaintInheritanceTxn(
			txn,
			localChainID,
			childID,
			parents,
		)
		if err != nil {
			return err
		}
		for _, item := range plan.decisions {
			value, err := taint.EncodeMembership(taint.Membership{
				Generation: item.header.Generation,
				Depth:      0,
				Origin:     taint.OriginAdmit,
			})
			if err != nil {
				return err
			}
			if err := s.txnSet(txn, taint.MembershipKey(childID, item.decisionID), value); err != nil {
				return err
			}
			if err := s.txnSet(txn, taint.DecisionIndexKey(item.decisionID, childID), []byte{}); err != nil {
				return err
			}
		}
		if len(plan.decisions) > 0 {
			if err := bumpTaintRevisionTxn(s, txn); err != nil {
				return err
			}
		}
		result.Decisions, result.Units = len(plan.decisions), plan.units
		return nil
	})
	return result, err
}

// ValidateTaintInheritance performs the read-only admission preflight without
// writing memberships or advancing the taint revision.
func (s *BadgerStore) ValidateTaintInheritance(
	localChainID, childID string,
	parents []tx.ParentRef,
) (InheritedTaintResult, error) {
	var result InheritedTaintResult
	if len(parents) == 0 {
		return result, nil
	}
	if len(childID) == 0 || len(childID) > taint.MaxMemoryIDBytes {
		return result, fmt.Errorf("%w: invalid child memory ID", ErrTaintStateMalformed)
	}
	if _, err := tx.EncodeDerivedFromExtension(parents); err != nil {
		return result, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	err := s.view(func(txn *badger.Txn) error {
		plan, err := planTaintInheritanceTxn(txn, localChainID, childID, parents)
		if err != nil {
			return err
		}
		result.Decisions, result.Units = len(plan.decisions), plan.units
		return nil
	})
	return result, err
}

// ApplyTaintAdmission copies effective parent memberships and adds every active
// decision whose credential or Root-generation scope matches the child's exact
// creation provenance.
func (s *BadgerStore) ApplyTaintAdmission(
	localChainID, childID string,
	parents []tx.ParentRef,
	signers []AppV30ProvenanceSigner,
) (InheritedTaintResult, error) {
	var result InheritedTaintResult
	if len(childID) == 0 || len(childID) > taint.MaxMemoryIDBytes {
		return result, fmt.Errorf("%w: invalid child memory ID", ErrTaintStateMalformed)
	}
	if len(parents) == 0 && len(signers) == 0 {
		return result, nil
	}
	if len(parents) > 0 {
		if _, err := tx.EncodeDerivedFromExtension(parents); err != nil {
			return result, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
	}
	err := s.update(func(txn *badger.Txn) error {
		plan, err := planTaintAdmissionTxn(
			txn,
			localChainID,
			childID,
			parents,
			signers,
		)
		if err != nil {
			return err
		}
		for _, item := range plan.decisions {
			value, err := taint.EncodeMembership(taint.Membership{
				Generation: item.header.Generation,
				Depth:      0,
				Origin:     taint.OriginAdmit,
			})
			if err != nil {
				return err
			}
			if err := s.txnSet(txn, taint.MembershipKey(childID, item.decisionID), value); err != nil {
				return err
			}
			if err := s.txnSet(txn, taint.DecisionIndexKey(item.decisionID, childID), []byte{}); err != nil {
				return err
			}
		}
		if len(plan.decisions) > 0 {
			if err := bumpTaintRevisionTxn(s, txn); err != nil {
				return err
			}
		}
		result.Decisions, result.Units = len(plan.decisions), plan.units
		return nil
	})
	return result, err
}

// ValidateTaintAdmission performs the read-only scope and parent inheritance
// preflight without writing memberships.
func (s *BadgerStore) ValidateTaintAdmission(
	localChainID, childID string,
	parents []tx.ParentRef,
	signers []AppV30ProvenanceSigner,
) (InheritedTaintResult, error) {
	var result InheritedTaintResult
	if len(childID) == 0 || len(childID) > taint.MaxMemoryIDBytes {
		return result, fmt.Errorf("%w: invalid child memory ID", ErrTaintStateMalformed)
	}
	if len(parents) == 0 && len(signers) == 0 {
		return result, nil
	}
	if len(parents) > 0 {
		if _, err := tx.EncodeDerivedFromExtension(parents); err != nil {
			return result, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
	}
	err := s.view(func(txn *badger.Txn) error {
		plan, err := planTaintAdmissionTxn(
			txn,
			localChainID,
			childID,
			parents,
			signers,
		)
		if err != nil {
			return err
		}
		result.Decisions, result.Units = len(plan.decisions), plan.units
		return nil
	})
	return result, err
}

type plannedTaintMembership struct {
	decisionID string
	header     taint.Header
}

type taintInheritancePlan struct {
	decisions []plannedTaintMembership
	units     int
}

func planTaintInheritanceTxn(
	txn *badger.Txn,
	localChainID, childID string,
	parents []tx.ParentRef,
) (taintInheritancePlan, error) {
	var plan taintInheritancePlan
	units := 0
	charge := func(n int) error {
		if units+n > taint.MaxAdmissionTaintUnits {
			return ErrTaintBudget
		}
		units += n
		return nil
	}
	headers := make(map[string]taint.Header)
	effective := make(map[string]taint.Header)
	for _, parent := range parents {
		if parent.SourceChainID != localChainID {
			return plan, fmt.Errorf("%w: cross-chain admission is not active", ErrTaintStateMalformed)
		}
		if parent.MemoryID == childID {
			return plan, fmt.Errorf("%w: self-reference is not allowed", ErrTaintStateMalformed)
		}
		if err := charge(1); err != nil {
			return plan, err
		}
		prefix := taint.MembershipPrefix(parent.MemoryID)
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		options.PrefetchValues = false
		it := txn.NewIterator(options)
		entries := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			entries++
			if entries > taint.MaxActiveDecisions {
				it.Close()
				return plan, fmt.Errorf("%w: parent membership cap exceeded", ErrTaintStateMalformed)
			}
			if err := charge(1); err != nil {
				it.Close()
				return plan, err
			}
			key := it.Item().Key()
			if len(key) != len(prefix)+taint.DecisionIDBytes || it.Item().ValueSize() != 11 {
				it.Close()
				return plan, fmt.Errorf("%w: malformed parent membership key/value", ErrTaintStateMalformed)
			}
			decisionID := string(bytes.TrimPrefix(key, prefix))
			if err := taint.ValidateDecisionID(decisionID); err != nil {
				it.Close()
				return plan, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				it.Close()
				return plan, err
			}
			membership, err := taint.DecodeMembership(value)
			if err != nil {
				it.Close()
				return plan, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			header, cached := headers[decisionID]
			if !cached {
				if chargeErr := charge(1); chargeErr != nil {
					it.Close()
					return plan, chargeErr
				}
				var found bool
				header, found, err = readTaintHeaderTxn(txn, decisionID)
				if err != nil {
					it.Close()
					return plan, err
				}
				if !found {
					it.Close()
					return plan, fmt.Errorf("%w: missing decision header", ErrTaintStateMalformed)
				}
				headers[decisionID] = header
			}
			if header.MembershipEffective(membership) {
				effective[decisionID] = header
			}
		}
		it.Close()
	}

	decisionIDs := make([]string, 0, len(effective))
	for decisionID := range effective {
		decisionIDs = append(decisionIDs, decisionID)
	}
	sort.Strings(decisionIDs)
	plan.decisions = make([]plannedTaintMembership, 0, len(decisionIDs))
	for _, decisionID := range decisionIDs {
		if err := charge(2); err != nil {
			return plan, err
		}
		plan.decisions = append(plan.decisions, plannedTaintMembership{
			decisionID: decisionID,
			header:     effective[decisionID],
		})
	}
	if len(plan.decisions) > 0 {
		if err := charge(1); err != nil {
			return plan, err
		}
	}
	plan.units = units
	return plan, nil
}

func planTaintAdmissionTxn(
	txn *badger.Txn,
	localChainID, childID string,
	parents []tx.ParentRef,
	signers []AppV30ProvenanceSigner,
) (taintInheritancePlan, error) {
	if len(signers) > 0 {
		if _, err := encodeAppV30Provenance(signers); err != nil {
			return taintInheritancePlan{}, err
		}
	}
	plan, err := planTaintInheritanceTxn(txn, localChainID, childID, parents)
	if err != nil || len(signers) == 0 {
		return plan, err
	}
	meter := newTaintUnitMeter(
		taint.MaxProvenanceAdmissionTaintUnits,
		ErrTaintBudget,
	)
	meter.units = plan.units
	activeScopes, err := readActiveTaintDecisionScopesTxn(txn, meter)
	if err != nil {
		return plan, err
	}
	provenance := AuthorProvenance{
		Signers:       append([]AppV30ProvenanceSigner(nil), signers...),
		Exact:         true,
		CoverageKnown: true,
	}
	effective := make(map[string]taint.Header, len(plan.decisions)+len(activeScopes))
	for _, item := range plan.decisions {
		effective[item.decisionID] = item.header
	}
	for _, active := range activeScopes {
		if !active.Scope.Matches(provenance) {
			continue
		}
		if _, exists := effective[active.DecisionID]; exists {
			continue
		}
		if err := meter.charge(2); err != nil {
			return plan, err
		}
		effective[active.DecisionID] = active.Header
	}
	if len(plan.decisions) == 0 && len(effective) > 0 {
		if err := meter.charge(1); err != nil {
			return plan, err
		}
	}
	if len(effective) > taint.MaxActiveDecisions {
		return plan, fmt.Errorf("%w: admission decision union exceeds cap", ErrTaintStateMalformed)
	}
	decisionIDs := make([]string, 0, len(effective))
	for decisionID := range effective {
		decisionIDs = append(decisionIDs, decisionID)
	}
	sort.Strings(decisionIDs)
	plan.decisions = make([]plannedTaintMembership, 0, len(decisionIDs))
	for _, decisionID := range decisionIDs {
		plan.decisions = append(plan.decisions, plannedTaintMembership{
			decisionID: decisionID,
			header:     effective[decisionID],
		})
	}
	plan.units = meter.units
	return plan, nil
}

func (s *BadgerStore) TaintRevision() (uint64, error) {
	var revision uint64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		revision, err = readTaintUint64Txn(txn, taint.RevisionKey())
		return err
	})
	return revision, err
}

func (s *BadgerStore) ActiveTaintDecisionCount() (uint64, error) {
	var count uint64
	err := s.view(func(txn *badger.Txn) error {
		var err error
		count, err = readTaintUint64Txn(txn, taint.ActiveCountKey())
		return err
	})
	return count, err
}

func readTaintHeaderTxn(txn *badger.Txn, decisionID string) (taint.Header, bool, error) {
	item, err := txn.Get(taint.DecisionKey(decisionID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return taint.Header{}, false, nil
	}
	if err != nil {
		return taint.Header{}, false, err
	}
	if item.ValueSize() > int64(79+taint.DecisionIDBytes+taint.MaxMemoryIDBytes+taint.MaxCursorBytes) {
		return taint.Header{}, false, fmt.Errorf("%w: oversized decision header", ErrTaintStateMalformed)
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return taint.Header{}, false, err
	}
	header, err := taint.DecodeHeader(raw)
	if err != nil {
		return taint.Header{}, false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	return header, true, nil
}

func readTaintUint64Txn(txn *badger.Txn, key []byte) (uint64, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("%w: invalid uint64 state %q", ErrTaintStateMalformed, key)
	}
	return binary.BigEndian.Uint64(raw), nil
}

func writeTaintUint64Txn(s *BadgerStore, txn *badger.Txn, key []byte, value uint64) error {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, value)
	return s.txnSet(txn, key, raw)
}

func bumpTaintRevisionTxn(s *BadgerStore, txn *badger.Txn) error {
	revision, err := readTaintUint64Txn(txn, taint.RevisionKey())
	if err != nil {
		return err
	}
	if revision == ^uint64(0) {
		return fmt.Errorf("%w: taint revision overflow", ErrTaintStateMalformed)
	}
	return writeTaintUint64Txn(s, txn, taint.RevisionKey(), revision+1)
}
