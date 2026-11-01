package store

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/tx"
)

var ErrAppV30LineageIndex = errors.New("app-v30 lineage index is inconsistent")

// ValidateLineageIndexes proves both lineage directions agree. Forward edges
// are authoritative; reverse-only, missing and malformed entries fail closed.
func (s *BadgerStore) ValidateLineageIndexes() error {
	return s.view(func(txn *badger.Txn) error {
		return validateLineageIndexesTxn(txn)
	})
}

// RebuildLineageReverseIndex materializes every missing reverse edge from the
// authoritative forward edges and returns how many keys it inserted.
func (s *BadgerStore) RebuildLineageReverseIndex() (int, error) {
	inserted := 0
	err := s.update(func(txn *badger.Txn) error {
		prefix := []byte("lineage:")
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			childID, parentID, err := parseLineageForwardKey(string(it.Item().Key()))
			if err != nil {
				return err
			}
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if valueErr := validateForwardLineageValue(childID, parentID, value); valueErr != nil {
				return valueErr
			}
			reverseKey := lineage.ReverseKey(parentID, childID)
			existing, found, err := readTaintValueTxn(txn, reverseKey)
			if err != nil {
				return err
			}
			if found {
				if len(existing) != 0 {
					return fmt.Errorf("%w: reverse edge value is not empty", ErrAppV30LineageIndex)
				}
				continue
			}
			if err := s.txnSet(txn, reverseKey, []byte{}); err != nil {
				return err
			}
			inserted++
		}
		return nil
	})
	return inserted, err
}

func validateLineageIndexesTxn(txn *badger.Txn) error {
	forwardPrefix := []byte("lineage:")
	forwardOptions := badger.DefaultIteratorOptions
	forwardOptions.Prefix = forwardPrefix
	forward := txn.NewIterator(forwardOptions)
	for forward.Seek(forwardPrefix); forward.ValidForPrefix(forwardPrefix); forward.Next() {
		childID, parentID, err := parseLineageForwardKey(string(forward.Item().Key()))
		if err != nil {
			forward.Close()
			return err
		}
		value, err := forward.Item().ValueCopy(nil)
		if err != nil {
			forward.Close()
			return err
		}
		if valueErr := validateForwardLineageValue(childID, parentID, value); valueErr != nil {
			forward.Close()
			return valueErr
		}
		reverseValue, found, err := readTaintValueTxn(
			txn,
			lineage.ReverseKey(parentID, childID),
		)
		if err != nil {
			forward.Close()
			return err
		}
		if !found {
			forward.Close()
			return fmt.Errorf("%w: missing reverse edge for %s/%s", ErrAppV30LineageIndex, childID, parentID)
		}
		if len(reverseValue) != 0 {
			forward.Close()
			return fmt.Errorf("%w: reverse edge value is not empty", ErrAppV30LineageIndex)
		}
	}
	forward.Close()

	reversePrefix := []byte("lineage-ref:")
	reverseOptions := badger.DefaultIteratorOptions
	reverseOptions.Prefix = reversePrefix
	reverse := txn.NewIterator(reverseOptions)
	for reverse.Seek(reversePrefix); reverse.ValidForPrefix(reversePrefix); reverse.Next() {
		parentID, childID, err := parseLineageReverseKey(string(reverse.Item().Key()))
		if err != nil {
			reverse.Close()
			return err
		}
		value, err := reverse.Item().ValueCopy(nil)
		if err != nil {
			reverse.Close()
			return err
		}
		if len(value) != 0 {
			reverse.Close()
			return fmt.Errorf("%w: reverse edge value is not empty", ErrAppV30LineageIndex)
		}
		forwardValue, found, err := readTaintValueTxn(
			txn,
			lineage.ForwardKey(childID, parentID),
		)
		if err != nil {
			reverse.Close()
			return err
		}
		if !found {
			reverse.Close()
			return fmt.Errorf("%w: reverse edge has no forward edge for %s/%s", ErrAppV30LineageIndex, childID, parentID)
		}
		if err := validateForwardLineageValue(childID, parentID, forwardValue); err != nil {
			reverse.Close()
			return err
		}
	}
	reverse.Close()
	return nil
}

func parseLineageForwardKey(key string) (childID, parentID string, err error) {
	key = strings.TrimPrefix(key, "lineage:")
	separator := strings.LastIndexByte(key, ':')
	if separator <= 0 || separator == len(key)-1 {
		return "", "", fmt.Errorf("%w: malformed forward key", ErrAppV30LineageIndex)
	}
	childID, parentID = key[:separator], key[separator+1:]
	if err := lineage.ValidateIDs(childID, parentID); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrAppV30LineageIndex, err)
	}
	return childID, parentID, nil
}

func parseLineageReverseKey(key string) (parentID, childID string, err error) {
	key = strings.TrimPrefix(key, "lineage-ref:")
	if len(key) <= lineage.ParentIDBytes*2+1 || key[lineage.ParentIDBytes*2] != ':' {
		return "", "", fmt.Errorf("%w: malformed reverse key", ErrAppV30LineageIndex)
	}
	parentID = key[:lineage.ParentIDBytes*2]
	childID = key[lineage.ParentIDBytes*2+1:]
	if err := lineage.ValidateIDs(childID, parentID); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrAppV30LineageIndex, err)
	}
	return parentID, childID, nil
}

func validateForwardLineageValue(childID, parentID string, value []byte) error {
	ref, err := tx.DecodeParentRefValue(value)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrAppV30LineageIndex, err)
	}
	derivedID, err := tx.ParentRefIDHex(ref)
	if err != nil || derivedID != parentID {
		return fmt.Errorf("%w: forward value does not match parent identity", ErrAppV30LineageIndex)
	}
	encoded, err := tx.EncodeParentRefValue(ref)
	if err != nil || !bytes.Equal(encoded, value) {
		return fmt.Errorf("%w: forward value is not canonical", ErrAppV30LineageIndex)
	}
	if ref.MemoryID == childID {
		return fmt.Errorf("%w: self-reference", ErrAppV30LineageIndex)
	}
	return nil
}
