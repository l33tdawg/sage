package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	taintFixedMutationUnits = 3
	taintQueuePopUnits      = 2
	// The accepted estimator charges two units to read a child edge and its
	// membership, then three writes for a new child. Resolving the child's
	// fixed parent_ref_id needs one additional canonical memory read.
	taintNewChildUnits      = 6
	taintEmptyIteratorUnits = 1
	taintMemberCleanupUnits = 3
	taintQueueCleanupUnits  = 2
	taintActiveReleaseUnits = 2
)

type TaintExpandResult struct {
	Units        int
	Scanned      int
	Materialized int
	Enqueued     int
	Status       taint.Status
	Changed      bool
}

type TaintCleanupResult struct {
	Units          int
	MembersDeleted int
	QueueDeleted   int
	Phase          taint.CleanupPhase
	Status         taint.Status
	ActiveReleased bool
	Changed        bool
}

// ExpandTaintDecision advances one deterministic BFS batch. A child transition
// is touched only when its complete membership, index and queue write can fit.
func (s *BadgerStore) ExpandTaintDecision(
	localChainID, decisionID string,
	budget int,
) (TaintExpandResult, error) {
	var result TaintExpandResult
	if err := taint.ValidateDecisionID(decisionID); err != nil {
		return result, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	if len(localChainID) == 0 || len(localChainID) > tx.MaxDerivedFromChainIDBytes {
		return result, fmt.Errorf("%w: invalid local chain ID", ErrTaintStateMalformed)
	}
	if budget < taintFixedMutationUnits+taintQueuePopUnits ||
		budget > taint.PerExecutionUnitBudget {
		return result, ErrTaintBudget
	}

	err := s.update(func(txn *badger.Txn) error {
		header, found, err := readTaintHeaderTxn(txn, decisionID)
		if err != nil {
			return err
		}
		if !found {
			return ErrTaintNotActive
		}
		result.Units = 1
		result.Status = header.Status
		if header.Action != taint.ActionApply {
			return ErrTaintStateMalformed
		}
		switch header.Status {
		case taint.StatusComplete, taint.StatusWorkExhausted:
			return nil
		case taint.StatusExpanding, taint.StatusDepthExhausted:
		default:
			return ErrTaintNotActive
		}
		if header.FrontierHead > header.FrontierTail {
			return fmt.Errorf("%w: frontier head exceeds tail", ErrTaintStateMalformed)
		}

		remaining := budget - taintFixedMutationUnits
		operationUnits := 0
		changed := false
		fresh := 0

		for {
			if header.ParentRefID == "" {
				if header.FrontierHead == header.FrontierTail {
					if header.Status != taint.StatusDepthExhausted {
						header.Status = taint.StatusComplete
						changed = true
					}
					break
				}
				if remaining < taintQueuePopUnits {
					break
				}
				queueKey := taint.FrontierKey(
					decisionID,
					header.Generation,
					header.FrontierHead,
				)
				entry, found, err := readTaintQueueEntryTxn(txn, queueKey)
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf(
						"%w: missing frontier entry %d",
						ErrTaintStateMalformed,
						header.FrontierHead,
					)
				}
				if err := s.txnDelete(txn, queueKey); err != nil {
					return err
				}
				remaining -= taintQueuePopUnits
				operationUnits += taintQueuePopUnits
				header.FrontierHead++
				header.ParentRefID = entry.ParentRefID
				header.ParentMemoryID = entry.MemoryID
				header.ParentDepth = entry.Depth
				header.LastEdge = ""
				changed = true
			}

			if fresh >= taint.MaxNewMembershipsPerExec ||
				remaining < taintNewChildUnits {
				break
			}

			prefix := lineage.ReversePrefix(header.ParentRefID)
			options := badger.DefaultIteratorOptions
			options.Prefix = prefix
			options.PrefetchValues = false
			it := txn.NewIterator(options)
			start := prefix
			if header.LastEdge != "" {
				start = append(
					append([]byte(nil), lineage.ReverseKey(header.ParentRefID, header.LastEdge)...),
					0,
				)
			}
			it.Seek(start)
			processedEdges := 0
			stopped := false
			for it.ValidForPrefix(prefix) {
				if fresh >= taint.MaxNewMembershipsPerExec ||
					remaining < taintNewChildUnits {
					stopped = true
					break
				}

				childID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
				if err := lineage.ValidateChildID(childID); err != nil {
					it.Close()
					return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
				}
				remaining--
				operationUnits++

				membershipRaw, membershipFound, err := readTaintValueTxn(
					txn,
					taint.MembershipKey(childID, decisionID),
				)
				if err != nil {
					it.Close()
					return err
				}
				remaining--
				operationUnits++

				if membershipFound {
					membership, err := taint.DecodeMembership(membershipRaw)
					if err != nil {
						it.Close()
						return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
					}
					if membership.Generation != header.Generation {
						it.Close()
						return fmt.Errorf(
							"%w: stale membership generation for %s",
							ErrTaintStateMalformed,
							childID,
						)
					}
				} else {
					childDepth := header.ParentDepth + 1
					if childDepth > taint.MaxDepth {
						header.Status = taint.StatusDepthExhausted
					} else if header.Materialized >= taint.MaxExpansionMemberships {
						header.Status = taint.StatusWorkExhausted
						stopped = true
						break
					} else {
						childRefID, childErr := readChildParentRefIDTxn(
							txn,
							localChainID,
							childID,
						)
						if childErr != nil {
							it.Close()
							return childErr
						}
						remaining--
						operationUnits++

						membershipValue, membershipErr := taint.EncodeMembership(taint.Membership{
							Generation: header.Generation,
							Depth:      childDepth,
							Origin:     taint.OriginExpand,
						})
						if membershipErr != nil {
							it.Close()
							return membershipErr
						}
						queueValue, queueErr := taint.EncodeQueueEntry(taint.QueueEntry{
							ParentRefID: childRefID,
							MemoryID:    childID,
							Depth:       childDepth,
						})
						if queueErr != nil {
							it.Close()
							return queueErr
						}
						if header.FrontierTail == ^uint64(0) {
							it.Close()
							return fmt.Errorf("%w: frontier tail overflow", ErrTaintStateMalformed)
						}
						queueKey := taint.FrontierKey(
							decisionID,
							header.Generation,
							header.FrontierTail,
						)
						remaining -= 3
						operationUnits += 3
						if err := s.txnSet(
							txn,
							taint.MembershipKey(childID, decisionID),
							membershipValue,
						); err != nil {
							it.Close()
							return err
						}
						if err := s.txnSet(
							txn,
							taint.DecisionIndexKey(decisionID, childID),
							[]byte{},
						); err != nil {
							it.Close()
							return err
						}
						if err := s.txnSet(txn, queueKey, queueValue); err != nil {
							it.Close()
							return err
						}
						header.FrontierTail++
						header.Materialized++
						if uint32(childDepth) > header.MaxDepth {
							header.MaxDepth = uint32(childDepth)
						}
						fresh++
						result.Materialized++
						result.Enqueued++
					}
				}

				if header.Scanned == ^uint32(0) {
					it.Close()
					return fmt.Errorf("%w: scanned counter overflow", ErrTaintStateMalformed)
				}
				header.Scanned++
				result.Scanned++
				header.LastEdge = childID
				changed = true
				processedEdges++
				it.Next()
			}
			it.Close()

			if stopped {
				break
			}
			if processedEdges == 0 {
				if remaining < taintEmptyIteratorUnits {
					break
				}
				remaining -= taintEmptyIteratorUnits
				operationUnits += taintEmptyIteratorUnits
			}
			header.ParentRefID = ""
			header.ParentMemoryID = ""
			header.ParentDepth = 0
			header.LastEdge = ""
			changed = true
		}

		result.Status = header.Status
		if !changed {
			result.Units = 1 + operationUnits
			return nil
		}
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
		result.Units = taintFixedMutationUnits + operationUnits
		result.Changed = true
		return nil
	})
	return result, err
}

// CleanupTaintDecision advances one cleanup phase. Membership cleanup always
// completes before queue cleanup starts.
func (s *BadgerStore) CleanupTaintDecision(
	decisionID string,
	budget int,
) (TaintCleanupResult, error) {
	var result TaintCleanupResult
	if err := taint.ValidateDecisionID(decisionID); err != nil {
		return result, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	if budget < taintFixedMutationUnits || budget > taint.PerExecutionUnitBudget {
		return result, ErrTaintBudget
	}

	err := s.update(func(txn *badger.Txn) error {
		header, found, err := readTaintHeaderTxn(txn, decisionID)
		if err != nil {
			return err
		}
		if !found {
			return ErrTaintNotActive
		}
		result.Units = 1
		result.Status = header.Status
		result.Phase = header.CleanupPhase
		if header.Status == taint.StatusClean {
			return nil
		}
		if header.Status != taint.StatusCleaning {
			return ErrTaintNotActive
		}

		remaining := budget - taintFixedMutationUnits
		operationUnits := 0
		changed := false

		switch header.CleanupPhase {
		case taint.CleanupMembers:
			prefix := taint.DecisionIndexPrefix(decisionID)
			options := badger.DefaultIteratorOptions
			options.Prefix = prefix
			options.PrefetchValues = false
			it := txn.NewIterator(options)
			start := prefix
			if header.LastEdge != "" {
				start = taint.DecisionIndexKey(decisionID, header.LastEdge)
			}
			it.Seek(start)
			if header.LastEdge != "" && it.ValidForPrefix(prefix) &&
				bytes.Equal(it.Item().Key(), start) {
				it.Next()
			}
			for it.ValidForPrefix(prefix) && remaining >= taintMemberCleanupUnits {
				memoryID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
				if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
					it.Close()
					return fmt.Errorf("%w: invalid cleanup memory ID", ErrTaintStateMalformed)
				}
				key := append([]byte(nil), it.Item().Key()...)
				if err := s.txnDelete(txn, key); err != nil {
					it.Close()
					return err
				}
				if err := s.txnDelete(
					txn,
					taint.MembershipKey(memoryID, decisionID),
				); err != nil {
					it.Close()
					return err
				}
				remaining -= taintMemberCleanupUnits
				operationUnits += taintMemberCleanupUnits
				header.LastEdge = memoryID
				result.MembersDeleted++
				changed = true
				it.Next()
			}
			if !it.ValidForPrefix(prefix) {
				header.CleanupPhase = taint.CleanupQueue
				header.LastEdge = ""
				changed = true
			}
			it.Close()

		case taint.CleanupQueue:
			prefix := taint.FrontierPrefix(decisionID)
			options := badger.DefaultIteratorOptions
			options.Prefix = prefix
			options.PrefetchValues = false
			it := txn.NewIterator(options)
			start := prefix
			if header.LastEdge != "" {
				start = append(append([]byte(nil), prefix...), []byte(header.LastEdge)...)
			}
			it.Seek(start)
			if header.LastEdge != "" && it.ValidForPrefix(prefix) &&
				bytes.Equal(it.Item().Key(), start) {
				it.Next()
			}
			for it.ValidForPrefix(prefix) && remaining >= taintQueueCleanupUnits {
				key := append([]byte(nil), it.Item().Key()...)
				if err := s.txnDelete(txn, key); err != nil {
					it.Close()
					return err
				}
				remaining -= taintQueueCleanupUnits
				operationUnits += taintQueueCleanupUnits
				header.LastEdge = string(bytes.TrimPrefix(key, prefix))
				result.QueueDeleted++
				changed = true
				it.Next()
			}
			queueExhausted := !it.ValidForPrefix(prefix)
			it.Close()
			if queueExhausted {
				header.LastEdge = ""
				changed = true
				if remaining < taintActiveReleaseUnits {
					header.CleanupPhase = taint.CleanupQueue
				} else {
					active, activeErr := readTaintUint64Txn(txn, taint.ActiveCountKey())
					if activeErr != nil {
						return activeErr
					}
					if active == 0 {
						return fmt.Errorf("%w: active decision count underflow", ErrTaintStateMalformed)
					}
					if writeErr := writeTaintUint64Txn(
						s,
						txn,
						taint.ActiveCountKey(),
						active-1,
					); writeErr != nil {
						return writeErr
					}
					remaining -= taintActiveReleaseUnits
					operationUnits += taintActiveReleaseUnits
					header.Status = taint.StatusClean
					header.CleanupPhase = taint.CleanupNone
					result.ActiveReleased = true
				}
			}

		default:
			return fmt.Errorf("%w: invalid cleanup phase %d", ErrTaintStateMalformed, header.CleanupPhase)
		}

		result.Status = header.Status
		result.Phase = header.CleanupPhase
		if !changed {
			result.Units = 1 + operationUnits
			return nil
		}
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
		result.Units = taintFixedMutationUnits + operationUnits
		result.Changed = true
		return nil
	})
	return result, err
}

func readTaintQueueEntryTxn(
	txn *badger.Txn,
	key []byte,
) (taint.QueueEntry, bool, error) {
	raw, found, err := readTaintValueTxn(txn, key)
	if err != nil || !found {
		return taint.QueueEntry{}, found, err
	}
	entry, err := taint.DecodeQueueEntry(raw)
	if err != nil {
		return taint.QueueEntry{}, false, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	return entry, true, nil
}

func readTaintValueTxn(
	txn *badger.Txn,
	key []byte,
) ([]byte, bool, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	raw, err := item.ValueCopy(nil)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func readChildParentRefIDTxn(
	txn *badger.Txn,
	localChainID, childID string,
) (string, error) {
	item, err := txn.Get(memoryKey(childID))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", fmt.Errorf("%w: lineage child %s has no canonical memory", ErrTaintStateMalformed, childID)
	}
	if err != nil {
		return "", err
	}
	contentHash, _, err := decodeMemoryHashEntryFromItem(item)
	if err != nil || len(contentHash) != sha256.Size {
		return "", fmt.Errorf("%w: lineage child %s has an invalid content hash", ErrTaintStateMalformed, childID)
	}
	ref := tx.ParentRef{SourceChainID: localChainID, MemoryID: childID}
	copy(ref.ContentHash[:], contentHash)
	refID, err := tx.ParentRefIDHex(ref)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	return refID, nil
}
