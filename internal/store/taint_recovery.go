package store

import (
	"encoding/binary"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
)

// ValidateTaintState proves decision, membership, index and frontier
// directions agree and that the active-count singleton matches the headers.
func (s *BadgerStore) ValidateTaintState() error {
	return s.view(func(txn *badger.Txn) error {
		headers := make(map[string]taint.Header)
		scopes := make(map[string]TaintDecisionScope)
		decisionPrefix := []byte("taintdec:")
		options := badger.DefaultIteratorOptions
		options.Prefix = decisionPrefix
		decisions := txn.NewIterator(options)
		for decisions.Seek(decisionPrefix); decisions.ValidForPrefix(decisionPrefix); decisions.Next() {
			decisionID := string(decisions.Item().Key()[len(decisionPrefix):])
			if err := taint.ValidateDecisionID(decisionID); err != nil {
				decisions.Close()
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			header, found, err := readTaintHeaderTxn(txn, decisionID)
			if err != nil {
				decisions.Close()
				return err
			}
			if !found {
				decisions.Close()
				return fmt.Errorf("%w: missing decision header", ErrTaintStateMalformed)
			}
			if header.Materialized > taint.MaxExpansionMemberships {
				decisions.Close()
				return fmt.Errorf("%w: expansion cap exceeded", ErrTaintStateMalformed)
			}
			decisionScope, scopeFound, err := readTaintDecisionScopeTxn(
				txn,
				decisionID,
				header.PayloadHash,
				nil,
			)
			if err != nil {
				decisions.Close()
				return err
			}
			if !scopeFound {
				decisions.Close()
				return fmt.Errorf("%w: decision has no canonical scope", ErrTaintStateMalformed)
			}
			headers[decisionID] = header
			scopes[decisionID] = decisionScope
		}
		decisions.Close()

		expectedActive := uint64(0)
		for _, header := range headers {
			if header.Status.Active() || header.Status == taint.StatusCleaning {
				expectedActive++
			}
		}
		active, err := readTaintUint64Txn(txn, taint.ActiveCountKey())
		if err != nil {
			return err
		}
		if active != expectedActive {
			return fmt.Errorf(
				"%w: active count %d does not match %d retained decisions",
				ErrTaintStateMalformed,
				active,
				expectedActive,
			)
		}

		scopePrefix := []byte("taintscope:")
		scopeOptions := badger.DefaultIteratorOptions
		scopeOptions.Prefix = scopePrefix
		scopeItems := txn.NewIterator(scopeOptions)
		seenScopes := 0
		for scopeItems.Seek(scopePrefix); scopeItems.ValidForPrefix(scopePrefix); scopeItems.Next() {
			decisionID := string(scopeItems.Item().Key()[len(scopePrefix):])
			if err := taint.ValidateDecisionID(decisionID); err != nil {
				scopeItems.Close()
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			header, found := headers[decisionID]
			if !found {
				scopeItems.Close()
				return fmt.Errorf("%w: decision scope has no header", ErrTaintStateMalformed)
			}
			raw, err := scopeItems.Item().ValueCopy(nil)
			if err != nil {
				scopeItems.Close()
				return err
			}
			payloadHash, decisionScope, err := decodeTaintDecisionScope(raw)
			if err != nil {
				scopeItems.Close()
				return err
			}
			if payloadHash != header.PayloadHash ||
				!equalTaintDecisionScope(decisionScope, scopes[decisionID]) {
				scopeItems.Close()
				return fmt.Errorf("%w: decision scope changed during recovery validation", ErrTaintStateMalformed)
			}
			seenScopes++
		}
		scopeItems.Close()
		if seenScopes != len(headers) {
			return fmt.Errorf(
				"%w: %d decision headers have %d scopes",
				ErrTaintStateMalformed,
				len(headers),
				seenScopes,
			)
		}

		activeDecisionPrefix := taint.ActiveDecisionPrefix()
		activeDecisionOptions := badger.DefaultIteratorOptions
		activeDecisionOptions.Prefix = activeDecisionPrefix
		activeDecisionItems := txn.NewIterator(activeDecisionOptions)
		indexedActive := make(map[string]struct{}, taint.MaxActiveDecisions)
		for activeDecisionItems.Seek(activeDecisionPrefix); activeDecisionItems.ValidForPrefix(activeDecisionPrefix); activeDecisionItems.Next() {
			decisionID := string(activeDecisionItems.Item().Key()[len(activeDecisionPrefix):])
			if err := taint.ValidateDecisionID(decisionID); err != nil {
				activeDecisionItems.Close()
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			header, found := headers[decisionID]
			if !found || !header.Status.Active() {
				activeDecisionItems.Close()
				return fmt.Errorf("%w: active index references a non-active decision", ErrTaintStateMalformed)
			}
			if _, duplicate := indexedActive[decisionID]; duplicate {
				activeDecisionItems.Close()
				return fmt.Errorf("%w: duplicate active decision index", ErrTaintStateMalformed)
			}
			indexedActive[decisionID] = struct{}{}
			if len(indexedActive) > taint.MaxActiveDecisions {
				activeDecisionItems.Close()
				return fmt.Errorf("%w: active decision index exceeds cap", ErrTaintStateMalformed)
			}
		}
		activeDecisionItems.Close()
		expectedIndexedActive := 0
		for decisionID, header := range headers {
			if !header.Status.Active() {
				continue
			}
			expectedIndexedActive++
			if _, found := indexedActive[decisionID]; !found {
				return fmt.Errorf("%w: active decision has no active index", ErrTaintStateMalformed)
			}
		}
		if len(indexedActive) != expectedIndexedActive {
			return fmt.Errorf(
				"%w: %d active decisions have %d active indexes",
				ErrTaintStateMalformed,
				expectedIndexedActive,
				len(indexedActive),
			)
		}

		membershipPrefix := []byte("taintm:")
		membershipOptions := badger.DefaultIteratorOptions
		membershipOptions.Prefix = membershipPrefix
		memberships := txn.NewIterator(membershipOptions)
		for memberships.Seek(membershipPrefix); memberships.ValidForPrefix(membershipPrefix); memberships.Next() {
			memoryID, decisionID, err := parseTaintMembershipKey(string(memberships.Item().Key()))
			if err != nil {
				memberships.Close()
				return err
			}
			header, found := headers[decisionID]
			if !found {
				memberships.Close()
				return fmt.Errorf("%w: membership has no decision header", ErrTaintStateMalformed)
			}
			if header.Status == taint.StatusClean {
				memberships.Close()
				return fmt.Errorf("%w: clean decision retains membership", ErrTaintStateMalformed)
			}
			raw, err := memberships.Item().ValueCopy(nil)
			if err != nil {
				memberships.Close()
				return err
			}
			membership, err := taint.DecodeMembership(raw)
			if err != nil {
				memberships.Close()
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			if header.Status.Active() && membership.Generation != header.Generation {
				memberships.Close()
				return fmt.Errorf("%w: active decision has stale membership generation", ErrTaintStateMalformed)
			}
			indexFound, err := keyExistsTxn(
				txn,
				taint.DecisionIndexKey(decisionID, memoryID),
			)
			if err != nil {
				memberships.Close()
				return err
			}
			if !indexFound {
				memberships.Close()
				return fmt.Errorf("%w: membership has no decision index", ErrTaintStateMalformed)
			}
		}
		memberships.Close()

		indexPrefix := []byte("taintd:")
		indexOptions := badger.DefaultIteratorOptions
		indexOptions.Prefix = indexPrefix
		indexes := txn.NewIterator(indexOptions)
		for indexes.Seek(indexPrefix); indexes.ValidForPrefix(indexPrefix); indexes.Next() {
			key := string(indexes.Item().Key())
			if len(key) <= len(indexPrefix)+taint.DecisionIDBytes ||
				key[len(indexPrefix)+taint.DecisionIDBytes] != ':' {
				indexes.Close()
				return fmt.Errorf("%w: malformed decision index key", ErrTaintStateMalformed)
			}
			decisionID := key[len(indexPrefix) : len(indexPrefix)+taint.DecisionIDBytes]
			memoryID := key[len(indexPrefix)+taint.DecisionIDBytes+1:]
			if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
				indexes.Close()
				return fmt.Errorf("%w: invalid decision index memory ID", ErrTaintStateMalformed)
			}
			if _, found := headers[decisionID]; !found {
				indexes.Close()
				return fmt.Errorf("%w: decision index has no header", ErrTaintStateMalformed)
			}
			membershipRaw, found, err := readTaintValueTxn(
				txn,
				taint.MembershipKey(memoryID, decisionID),
			)
			if err != nil {
				indexes.Close()
				return err
			}
			if !found {
				indexes.Close()
				return fmt.Errorf("%w: decision index has no membership", ErrTaintStateMalformed)
			}
			if _, err := taint.DecodeMembership(membershipRaw); err != nil {
				indexes.Close()
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
		}
		indexes.Close()

		queuePrefix := []byte("taintq:")
		queueOptions := badger.DefaultIteratorOptions
		queueOptions.Prefix = queuePrefix
		queues := txn.NewIterator(queueOptions)
		queueCounts := make(map[string]uint64)
		for queues.Seek(queuePrefix); queues.ValidForPrefix(queuePrefix); queues.Next() {
			decisionID, generation, sequence, err := parseTaintQueueKey(string(queues.Item().Key()))
			if err != nil {
				queues.Close()
				return err
			}
			header, found := headers[decisionID]
			if !found {
				queues.Close()
				return fmt.Errorf("%w: frontier has no decision header", ErrTaintStateMalformed)
			}
			if header.Status == taint.StatusClean {
				queues.Close()
				return fmt.Errorf("%w: clean decision retains frontier", ErrTaintStateMalformed)
			}
			if header.Status.Active() &&
				(generation != header.Generation || sequence < header.FrontierHead || sequence >= header.FrontierTail) {
				queues.Close()
				return fmt.Errorf("%w: frontier key does not match active decision cursor", ErrTaintStateMalformed)
			}
			raw, err := queues.Item().ValueCopy(nil)
			if err != nil {
				queues.Close()
				return err
			}
			if _, err := taint.DecodeQueueEntry(raw); err != nil {
				queues.Close()
				return fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
			}
			queueCounts[decisionID]++
		}
		queues.Close()

		for decisionID, header := range headers {
			switch header.Status {
			case taint.StatusExpanding, taint.StatusDepthExhausted:
				if header.FrontierTail-header.FrontierHead != queueCounts[decisionID] {
					return fmt.Errorf(
						"%w: frontier cardinality mismatch for %s",
						ErrTaintStateMalformed,
						decisionID,
					)
				}
			case taint.StatusComplete:
				if header.FrontierHead != header.FrontierTail || queueCounts[decisionID] != 0 {
					return fmt.Errorf(
						"%w: terminal active decision retains frontier for %s",
						ErrTaintStateMalformed,
						decisionID,
					)
				}
			}
		}
		return nil
	})
}

func parseTaintMembershipKey(key string) (memoryID, decisionID string, err error) {
	key = strings.TrimPrefix(key, "taintm:")
	separator := strings.LastIndexByte(key, ':')
	if separator <= 0 || len(key)-separator-1 != taint.DecisionIDBytes {
		return "", "", fmt.Errorf("%w: malformed membership key", ErrTaintStateMalformed)
	}
	memoryID, decisionID = key[:separator], key[separator+1:]
	if len(memoryID) > taint.MaxMemoryIDBytes {
		return "", "", fmt.Errorf("%w: invalid membership memory ID", ErrTaintStateMalformed)
	}
	if err := taint.ValidateDecisionID(decisionID); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
	}
	return memoryID, decisionID, nil
}

func parseTaintQueueKey(key string) (decisionID string, generation, sequence uint64, err error) {
	key = strings.TrimPrefix(key, "taintq:")
	if len(key) != taint.DecisionIDBytes+18 || key[taint.DecisionIDBytes] != ':' ||
		key[taint.DecisionIDBytes+9] != ':' {
		return "", 0, 0, fmt.Errorf("%w: malformed frontier key", ErrTaintStateMalformed)
	}
	decisionID = key[:taint.DecisionIDBytes]
	if decisionErr := taint.ValidateDecisionID(decisionID); decisionErr != nil {
		return "", 0, 0, fmt.Errorf("%w: %v", ErrTaintStateMalformed, decisionErr)
	}
	generation = binary.BigEndian.Uint64([]byte(key[taint.DecisionIDBytes+1 : taint.DecisionIDBytes+9]))
	sequence = binary.BigEndian.Uint64([]byte(key[taint.DecisionIDBytes+10:]))
	return decisionID, generation, sequence, nil
}
