package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/scope"
)

// MemoryProjectionInventory is process-local recovery data, never a public
// health response or write-admission authority. All fields come from one
// committed Badger view; arbitrary-byte IDs remain exact Go-string bytes.
type MemoryProjectionInventory struct {
	MemoryIDs              []string
	ScopedContentIDs       []string
	ReadTs                 uint64
	FinalizedHeight        int64
	HeightPresent          bool
	AppHash                []byte
	AppHashPresent         bool
	RawIdentityRules       bool
	AppV30AppliedHeight    int64
	FreshGenesisAppVersion uint64
}

// RawIdentityRulesAt qualifies the signed snapshot point, not today's app
// version. An older V1 authorization is never silently reinterpreted as V2.
func (inventory *MemoryProjectionInventory) RawIdentityRulesAt(height int64) bool {
	if inventory == nil || height <= 0 || !inventory.HeightPresent || height > inventory.FinalizedHeight {
		return false
	}
	if inventory.FreshGenesisAppVersion >= 30 {
		return true
	}
	return inventory.AppV30AppliedHeight > 0 && height > inventory.AppV30AppliedHeight
}

// MemoryProjectionInventory reads no SQL and refuses borrowed transaction
// handles: omission authorizations may only capture committed canonical data.
// maxIDs/maxBytes bound each complete ID inventory; a cap/error returns no data.
func (s *BadgerStore) MemoryProjectionInventory(maxIDs, maxBytes int) (*MemoryProjectionInventory, error) {
	if s == nil || s.db == nil || s.txn != nil {
		return nil, errors.New("projection inventory requires an owning canonical store")
	}
	if maxIDs <= 0 || maxBytes <= 0 {
		return nil, errors.New("projection inventory requires positive resource bounds")
	}
	var result *MemoryProjectionInventory
	err := s.db.View(func(txn *badger.Txn) error {
		reader := &BadgerStore{db: s.db, txn: txn, taintServingReadOnly: true}
		mode, err := reader.memoryIdentityModeInSnapshot()
		if err != nil {
			return err
		}
		inventory := &MemoryProjectionInventory{ReadTs: txn.ReadTs(), FinalizedHeight: mode.height, HeightPresent: mode.heightPresent, RawIdentityRules: mode.raw, AppV30AppliedHeight: mode.appliedHeight, FreshGenesisAppVersion: mode.freshAppVersion}
		inventory.AppHash, inventory.AppHashPresent, err = exactMemorySnapshotState(txn, "app_hash", sha256.Size)
		if err != nil {
			return err
		}
		qualifiedEmptyH0 := mode.freshAppVersion > 0 && mode.height == 0 && len(inventory.AppHash) == 0
		if inventory.AppHashPresent && len(inventory.AppHash) != sha256.Size && !qualifiedEmptyH0 {
			return errors.New("canonical projection AppHash must contain exactly32 bytes outside qualified pristine H0")
		}
		validID := func(id string) bool { return len(id) > 0 && (mode.useRaw(id) || strings.TrimSpace(id) != "") }
		appendID := func(ids *[]string, total *int, id string) error {
			if !validID(id) {
				return errors.New("canonical projection inventory contains an invalid ID")
			}
			if len(*ids) >= maxIDs || len(id) > maxBytes || *total > maxBytes-len(id) {
				return errors.New("canonical projection inventory exceeds its resource bound")
			}
			*total += len(id)
			*ids = append(*ids, id)
			return nil
		}
		memoryPrefix := []byte("memory:")
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = memoryPrefix
		iterator := txn.NewIterator(opts)
		total := 0
		for iterator.Seek(memoryPrefix); iterator.ValidForPrefix(memoryPrefix); iterator.Next() {
			id := string(iterator.Item().KeyCopy(nil)[len(memoryPrefix):])
			if err := appendID(&inventory.MemoryIDs, &total, id); err != nil {
				iterator.Close()
				return err
			}
		}
		iterator.Close()
		contentPrefix := []byte("state:scope-content:")
		opts = badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = contentPrefix
		iterator = txn.NewIterator(opts)
		defer iterator.Close()
		total = 0
		for iterator.Seek(contentPrefix); iterator.ValidForPrefix(contentPrefix); iterator.Next() {
			item := iterator.Item()
			id := string(item.KeyCopy(nil)[len(contentPrefix):])
			if err := appendID(&inventory.ScopedContentIDs, &total, id); err != nil {
				return err
			}
			if item.ValueSize() > int64(maxBytes) {
				return errors.New("scoped content exceeds the recovery byte bound")
			}
			if err := item.Value(func(value []byte) error {
				content, err := scope.DecodeContent(value)
				if err != nil {
					return err
				}
				if content.MemoryID != id {
					return errors.New("scoped content key disagrees with its canonical envelope")
				}
				return nil
			}); err != nil {
				return err
			}
		}
		result = inventory
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read canonical projection identity inventory: %w", err)
	}
	return result, nil
}
