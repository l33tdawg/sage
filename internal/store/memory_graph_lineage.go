package store

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/tx"
)

var (
	ErrMemoryLineageConflict    = errors.New("memory lineage conflicts with existing state")
	ErrMemoryLineageMalformed   = errors.New("memory lineage is malformed")
	ErrMemoryLineageUnavailable = errors.New("memory lineage parent is unavailable")
)

type lineageEdge struct {
	parentID string
	value    []byte
	ref      tx.ParentRef
}

// SetMemoryLineage atomically installs the immutable forward and reverse edges
// for one child. Exact replay is idempotent; any different existing edge set is
// rejected. The caller gates this on app-v30 and resolves each parent against
// committed local state before calling it.
func (s *BadgerStore) SetMemoryLineage(childID string, refs []tx.ParentRef) error {
	if len(refs) == 0 {
		return fmt.Errorf("%w: at least one parent is required", ErrMemoryLineageMalformed)
	}
	if _, err := tx.EncodeDerivedFromExtension(refs); err != nil {
		return fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
	}
	expected := make([]lineageEdge, 0, len(refs))
	for _, ref := range refs {
		parentID, err := tx.ParentRefIDHex(ref)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
		}
		value, err := tx.EncodeParentRefValue(ref)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
		}
		if err := lineage.ValidateIDs(childID, parentID); err != nil {
			return fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
		}
		expected = append(expected, lineageEdge{parentID: parentID, value: value, ref: ref})
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].parentID < expected[j].parentID })

	return s.update(func(txn *badger.Txn) error {
		existing, err := s.memoryLineageTxn(txn, childID)
		if err != nil {
			return err
		}
		if len(existing) > 0 {
			if len(existing) != len(expected) {
				return ErrMemoryLineageConflict
			}
			for i := range expected {
				if existing[i].parentID != expected[i].parentID ||
					!bytes.Equal(existing[i].value, expected[i].value) {
					return ErrMemoryLineageConflict
				}
			}
			return nil
		}

		for _, edge := range expected {
			found, err := keyExistsTxn(txn, lineage.ReverseKey(edge.parentID, childID))
			if err != nil {
				return err
			}
			if found {
				return fmt.Errorf("%w: reverse edge exists without forward edge for child %q and parent %q",
					ErrMemoryLineageMalformed, childID, edge.parentID)
			}
		}
		for _, edge := range expected {
			if err := s.txnSet(txn, lineage.ForwardKey(childID, edge.parentID), edge.value); err != nil {
				return err
			}
			if err := s.txnSet(txn, lineage.ReverseKey(edge.parentID, childID), []byte{}); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetMemoryLineage reads one child's immutable parent refs in canonical tuple
// order. No edges means the record predates app-v30 or has no lineage.
func (s *BadgerStore) GetMemoryLineage(childID string) ([]tx.ParentRef, error) {
	if err := lineage.ValidateChildID(childID); err != nil {
		return nil, err
	}
	var refs []tx.ParentRef
	err := s.view(func(txn *badger.Txn) error {
		edges, err := s.memoryLineageTxn(txn, childID)
		if err != nil {
			return err
		}
		refs = make([]tx.ParentRef, len(edges))
		for i := range edges {
			refs[i] = edges[i].ref
		}
		tx.SortParentRefs(refs)
		return nil
	})
	return refs, err
}

// ValidateMemoryLineageParents resolves every ref against one canonical
// snapshot. App-v30 accepts local-chain committed or challenged parents with
// the exact asserted content hash. Missing, hidden, deprecated and wrong-hash
// targets deliberately share one error to avoid existence probing.
func (s *BadgerStore) ValidateMemoryLineageParents(
	childID, localChainID string,
	refs []tx.ParentRef,
) error {
	if err := lineage.ValidateChildID(childID); err != nil {
		return fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
	}
	if localChainID == "" || len(localChainID) > tx.MaxDerivedFromChainIDBytes {
		return fmt.Errorf("%w: invalid local chain ID", ErrMemoryLineageMalformed)
	}
	if _, err := tx.EncodeDerivedFromExtension(refs); err != nil {
		return fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
	}
	return s.view(func(txn *badger.Txn) error {
		for _, ref := range refs {
			if ref.SourceChainID != localChainID {
				return fmt.Errorf("%w: cross-chain parent refs are not active", ErrMemoryLineageMalformed)
			}
			if ref.MemoryID == childID {
				return fmt.Errorf("%w: self-reference is not allowed", ErrMemoryLineageMalformed)
			}
			item, err := txn.Get(memoryKey(ref.MemoryID))
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrMemoryLineageUnavailable
			}
			if err != nil {
				return err
			}
			contentHash, status, err := decodeMemoryHashEntryFromItem(item)
			if err != nil ||
				(status != "committed" && status != "challenged") ||
				!bytes.Equal(contentHash, ref.ContentHash[:]) {
				return ErrMemoryLineageUnavailable
			}
		}
		return nil
	})
}

// ListLineageChildren returns up to limit children in raw child-ID order after
// the optional exclusive afterChild cursor.
func (s *BadgerStore) ListLineageChildren(
	parentID, afterChild string,
	limit int,
) ([]string, error) {
	if limit <= 0 || limit > 1024 {
		return nil, errors.New("lineage child page limit must be between 1 and 1024")
	}
	if err := lineage.ValidateParentID(parentID); err != nil {
		return nil, err
	}
	if len(afterChild) > tx.MaxDerivedFromMemoryIDBytes {
		return nil, errors.New("lineage child cursor is too long")
	}
	prefix := lineage.ReversePrefix(parentID)
	children := make([]string, 0, limit)
	err := s.view(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		options.PrefetchValues = false
		it := txn.NewIterator(options)
		defer it.Close()
		start := prefix
		if afterChild != "" {
			start = append(append([]byte(nil), lineage.ReverseKey(parentID, afterChild)...), 0)
		}
		for it.Seek(start); it.ValidForPrefix(prefix) && len(children) < limit; it.Next() {
			childID := string(bytes.TrimPrefix(it.Item().Key(), prefix))
			if childID == "" || len(childID) > tx.MaxDerivedFromMemoryIDBytes {
				return fmt.Errorf("%w: invalid child ID in reverse edge", ErrMemoryLineageMalformed)
			}
			children = append(children, childID)
		}
		return nil
	})
	return children, err
}

func (s *BadgerStore) memoryLineageTxn(txn *badger.Txn, childID string) ([]lineageEdge, error) {
	prefix := lineage.ForwardPrefix(childID)
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	it := txn.NewIterator(options)
	defer it.Close()
	var edges []lineageEdge
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		parentID := string(bytes.TrimPrefix(item.Key(), prefix))
		value, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		if validateErr := lineage.ValidateIDs(childID, parentID); validateErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, validateErr)
		}
		ref, err := tx.DecodeParentRefValue(value)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrMemoryLineageMalformed, err)
		}
		derivedID, err := tx.ParentRefIDHex(ref)
		if err != nil || derivedID != parentID {
			return nil, fmt.Errorf("%w: parent value does not match key identity", ErrMemoryLineageMalformed)
		}
		reverseFound, err := keyExistsTxn(txn, lineage.ReverseKey(parentID, childID))
		if err != nil {
			return nil, err
		}
		if !reverseFound {
			return nil, fmt.Errorf("%w: missing reverse edge for child %q and parent %q",
				ErrMemoryLineageMalformed, childID, parentID)
		}
		edges = append(edges, lineageEdge{
			parentID: parentID, value: append([]byte(nil), value...), ref: ref,
		})
	}
	return edges, nil
}

func keyExistsTxn(txn *badger.Txn, key []byte) (bool, error) {
	_, err := txn.Get(key)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	default:
		return false, err
	}
}

func decodeMemoryHashEntryFromItem(item *badger.Item) ([]byte, string, error) {
	var contentHash []byte
	var status string
	err := item.Value(func(value []byte) error {
		var decodeErr error
		contentHash, status, decodeErr = decodeMemoryHashEntry(value)
		return decodeErr
	})
	return contentHash, status, err
}
