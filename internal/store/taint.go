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

	created := false
	err = s.update(func(txn *badger.Txn) error {
		existing, found, err := readTaintHeaderTxn(txn, decisionID)
		if err != nil {
			return err
		}
		if found {
			if existing.Action != taint.ActionApply ||
				existing.PayloadHash != payloadHash {
				return ErrTaintConflict
			}
			return nil
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
	return created, err
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
	if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
		return nil, fmt.Errorf("%w: invalid memory ID", ErrTaintStateMalformed)
	}
	prefix := taint.MembershipPrefix(memoryID)
	decisions := make([]string, 0, taint.MaxActiveDecisions)
	err := s.view(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		it := txn.NewIterator(options)
		defer it.Close()
		seen := make(map[string]struct{})
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if len(seen) >= taint.MaxActiveDecisions {
				return fmt.Errorf("%w: too many memberships for %s", ErrTaintStateMalformed, memoryID)
			}
			decisionID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
			if _, duplicate := seen[decisionID]; duplicate {
				return fmt.Errorf("%w: duplicate decision membership", ErrTaintStateMalformed)
			}
			seen[decisionID] = struct{}{}
			if err := taint.ValidateDecisionID(decisionID); err != nil {
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			membership, err := taint.DecodeMembership(value)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			header, found, err := readTaintHeaderTxn(txn, decisionID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("%w: missing decision header %s", ErrTaintStateMalformed, decisionID)
			}
			if header.MembershipEffective(membership) {
				decisions = append(decisions, decisionID)
			}
		}
		return nil
	})
	sort.Strings(decisions)
	return decisions, err
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
	err := s.update(func(txn *badger.Txn) error {
		for _, parent := range parents {
			if parent.SourceChainID != localChainID {
				return fmt.Errorf("%w: cross-chain admission is not active", ErrTaintStateMalformed)
			}
			if parent.MemoryID == childID {
				return fmt.Errorf("%w: self-reference is not allowed", ErrTaintStateMalformed)
			}
			if err := charge(1); err != nil {
				return err
			}
			prefix := taint.MembershipPrefix(parent.MemoryID)
			options := badger.DefaultIteratorOptions
			options.Prefix = prefix
			it := txn.NewIterator(options)
			entries := 0
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				entries++
				if entries > taint.MaxActiveDecisions {
					it.Close()
					return fmt.Errorf("%w: parent membership cap exceeded", ErrTaintStateMalformed)
				}
				if err := charge(1); err != nil {
					it.Close()
					return err
				}
				decisionID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
				if err := taint.ValidateDecisionID(decisionID); err != nil {
					it.Close()
					return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
				}
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					it.Close()
					return err
				}
				membership, err := taint.DecodeMembership(value)
				if err != nil {
					it.Close()
					return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
				}
				header, cached := headers[decisionID]
				if !cached {
					if err := charge(1); err != nil {
						it.Close()
						return err
					}
					var found bool
					header, found, err = readTaintHeaderTxn(txn, decisionID)
					if err != nil {
						it.Close()
						return err
					}
					if !found {
						it.Close()
						return fmt.Errorf("%w: missing decision header", ErrTaintStateMalformed)
					}
					headers[decisionID] = header
				}
				if header.MembershipEffective(membership) {
					effective[decisionID] = header
				}
			}
			it.Close()
		}

		decisions := make([]string, 0, len(effective))
		for decisionID := range effective {
			decisions = append(decisions, decisionID)
		}
		sort.Strings(decisions)
		for _, decisionID := range decisions {
			header := effective[decisionID]
			value, err := taint.EncodeMembership(taint.Membership{
				Generation: header.Generation,
				Depth:      0,
				Origin:     taint.OriginAdmit,
			})
			if err != nil {
				return err
			}
			if err := charge(2); err != nil {
				return err
			}
			if err := s.txnSet(txn, taint.MembershipKey(childID, decisionID), value); err != nil {
				return err
			}
			if err := s.txnSet(txn, taint.DecisionIndexKey(decisionID, childID), []byte{}); err != nil {
				return err
			}
		}
		if len(decisions) > 0 {
			if err := charge(1); err != nil {
				return err
			}
			if err := bumpTaintRevisionTxn(s, txn); err != nil {
				return err
			}
		}
		result.Decisions, result.Units = len(decisions), units
		return nil
	})
	return result, err
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
