package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/memory"
)

// MemoryHashReanchorClassificationReader is the local SQL evidence required by
// the app-v24 reanchor planner and validator vote attestation. It remains a
// narrow optional interface so unrelated MemoryStore test doubles and remote
// backends do not silently invent a default classification.
type MemoryHashReanchorClassificationReader interface {
	GetMemoryClassificationLocal(context.Context, string) (int, error)
}

// PlanMemoryHashReanchorEntries scans canonical Badger memory IDs in raw-byte
// order, not the SQL mirror. This makes a missing SQL row a hard planning
// failure and makes every chunk a deterministic consecutive slice of the
// globally sorted eligible population.
//
// remaining is true only when at least one additional eligible candidate was
// observed after the returned bounded chunk.
func PlanMemoryHashReanchorEntries(
	ctx context.Context,
	memories MemoryStore,
	canonical *BadgerStore,
	limit int,
) (entries []MemoryHashReanchorEntry, remaining bool, err error) {
	if memories == nil || canonical == nil {
		return nil, false, errors.New("memory hash reanchor planning requires SQL and canonical stores")
	}
	if limit <= 0 || limit > maxMemoryHashReanchorEntries {
		return nil, false, fmt.Errorf(
			"memory hash reanchor planning limit %d is outside 1..%d",
			limit, maxMemoryHashReanchorEntries,
		)
	}
	classifications, ok := memories.(MemoryHashReanchorClassificationReader)
	if !ok {
		return nil, false, errors.New("memory hash reanchor planning requires exact SQL classification reads")
	}
	err = canonical.withMemoryIdentitySnapshot(func(reader *BadgerStore, mode memoryIdentityMode) error {
		var inspectErr error
		entries, remaining, inspectErr = func() ([]MemoryHashReanchorEntry, bool, error) {
			memoryIDs, inventoryErr := reader.canonicalMemoryIDsInSnapshot(mode)
			if inventoryErr != nil {
				return nil, false, inventoryErr
			}
			for _, memoryID := range memoryIDs {
				state, stateErr := reader.memoryDisclosureForIdentityMode(memoryID, mode)
				if stateErr != nil {
					return nil, false, fmt.Errorf("inspect canonical memory %s for reanchor: %w", memoryID, stateErr)
				}
				if len(state.ContentHash) != 0 || !state.AuthorPrincipalRecorded {
					continue
				}
				record, recordErr := memories.GetMemory(ctx, memoryID)
				if recordErr != nil {
					return nil, false, fmt.Errorf("load SQL evidence for reanchor memory %s: %w", memoryID, recordErr)
				}
				contentHash, validateErr := validateMemoryHashReanchorProjection(
					ctx, classifications, record, state,
				)
				if validateErr != nil {
					return nil, false, validateErr
				}
				if len(entries) == limit {
					return entries, true, nil
				}
				entries = append(entries, MemoryHashReanchorEntry{
					MemoryID:       memoryID,
					ExpectedStatus: state.Status,
					ContentHash:    contentHash,
				})
			}
			return entries, false, nil
		}()
		return inspectErr
	})
	if err != nil {
		return nil, false, err
	}
	return entries, remaining, nil
}

// AttestMemoryHashReanchorEntries independently recomputes every local SQL
// content hash and matches the complete SQL/Badger envelope before a validator
// UI may emit an accept vote. Consensus never calls this function.
func AttestMemoryHashReanchorEntries(
	ctx context.Context,
	memories MemoryStore,
	canonical *BadgerStore,
	entries []MemoryHashReanchorEntry,
) error {
	if memories == nil || canonical == nil {
		return errors.New("memory hash reanchor attestation requires SQL and canonical stores")
	}
	if err := validateMemoryHashReanchorEntries(entries); err != nil {
		return err
	}
	classifications, ok := memories.(MemoryHashReanchorClassificationReader)
	if !ok {
		return errors.New("memory hash reanchor attestation requires exact SQL classification reads")
	}
	return canonical.withMemoryIdentitySnapshot(func(reader *BadgerStore, mode memoryIdentityMode) error {
		for _, entry := range entries {
			state, err := reader.memoryDisclosureForIdentityMode(entry.MemoryID, mode)
			if err != nil {
				return fmt.Errorf("inspect canonical reanchor memory %s: %w", entry.MemoryID, err)
			}
			record, err := memories.GetMemory(ctx, entry.MemoryID)
			if err != nil {
				return fmt.Errorf("load SQL reanchor evidence for %s: %w", entry.MemoryID, err)
			}
			contentHash, err := validateMemoryHashReanchorProjection(
				ctx, classifications, record, state,
			)
			if err != nil {
				return err
			}
			if state.Status != entry.ExpectedStatus {
				return fmt.Errorf("memory hash reanchor status changed for %s", entry.MemoryID)
			}
			if !bytes.Equal(contentHash, entry.ContentHash) {
				return fmt.Errorf("memory hash reanchor content evidence changed for %s", entry.MemoryID)
			}
		}
		return nil
	})
}

// CanonicalMemoryIDs returns the complete sorted memory:<id> inventory from
// Badger. It is used only by process-local projection verification and repair
// planning; callers must not expose the identifiers through public health
// responses.
func CanonicalMemoryIDs(canonical *BadgerStore) ([]string, error) {
	if canonical == nil {
		return nil, errors.New("canonical memory inventory requires a store")
	}
	var ids []string
	err := canonical.withMemoryIdentitySnapshot(func(reader *BadgerStore, mode memoryIdentityMode) error {
		var err error
		ids, err = reader.canonicalMemoryIDsInSnapshot(mode)
		return err
	})
	return ids, err
}

func (canonical *BadgerStore) canonicalMemoryIDsInSnapshot(mode memoryIdentityMode) ([]string, error) {
	var ids []string
	err := canonical.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte("memory:")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			key := it.Item().Key()
			memoryID := string(key[len(opts.Prefix):])
			if len(memoryID) == 0 || (!mode.useRaw(memoryID) && strings.TrimSpace(memoryID) == "") {
				return errors.New("canonical memory inventory contains an empty ID")
			}
			ids = append(ids, memoryID)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan canonical memory inventory for reanchor: %w", err)
	}
	// Badger already iterates lexicographically. Keep the explicit sort as a
	// defense against future inventory-source changes.
	sort.Strings(ids)
	return ids, nil
}

func validateMemoryHashReanchorProjection(
	ctx context.Context,
	classifications MemoryHashReanchorClassificationReader,
	record *memory.MemoryRecord,
	state *MemoryDisclosureState,
) ([]byte, error) {
	if record == nil || state == nil || record.MemoryID == "" {
		return nil, errors.New("memory hash reanchor evidence is incomplete")
	}
	if len(state.ContentHash) != 0 {
		return nil, fmt.Errorf("memory hash reanchor target %s is not hashless", record.MemoryID)
	}
	if !state.AuthorPrincipalRecorded ||
		!state.DomainRecorded ||
		!state.AuthorRecorded ||
		!state.ClassificationRecorded ||
		state.CoCommitRecorded {
		return nil, fmt.Errorf("memory hash reanchor canonical envelope is ineligible for %s", record.MemoryID)
	}
	switch state.Status {
	case string(memory.StatusCommitted), string(memory.StatusDeprecated):
	default:
		return nil, fmt.Errorf("memory hash reanchor status %q is not terminal for %s", state.Status, record.MemoryID)
	}
	if string(record.Status) != state.Status ||
		record.DomainTag != state.Domain ||
		record.SubmittingAgent != state.Author {
		return nil, fmt.Errorf("memory hash reanchor SQL envelope mismatches canonical state for %s", record.MemoryID)
	}
	classification, err := classifications.GetMemoryClassificationLocal(ctx, record.MemoryID)
	if err != nil {
		return nil, fmt.Errorf("load SQL classification for reanchor memory %s: %w", record.MemoryID, err)
	}
	if classification != int(state.Classification) {
		return nil, fmt.Errorf("memory hash reanchor SQL classification mismatches canonical state for %s", record.MemoryID)
	}
	if record.Content == "" {
		return nil, fmt.Errorf("memory hash reanchor SQL content is empty for %s", record.MemoryID)
	}
	contentHash := sha256.Sum256([]byte(record.Content))
	if len(record.ContentHash) != sha256.Size ||
		!bytes.Equal(record.ContentHash, contentHash[:]) {
		return nil, fmt.Errorf("memory hash reanchor SQL content/hash evidence mismatches for %s", record.MemoryID)
	}
	return contentHash[:], nil
}
