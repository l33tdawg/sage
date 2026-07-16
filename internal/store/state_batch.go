package store

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// StateWrite is one key/value update in BadgerStore's state namespace.
// SetStatesAtomic applies a slice of these writes as one Badger transaction.
type StateWrite struct {
	Key   string
	Value []byte
}

// AtomicStateWriteStage identifies the durability boundaries exercised by the
// v11.9 crash fixture. Production builds compile a no-op hook at each point.
type AtomicStateWriteStage string

const (
	AtomicStateWriteBeforeTransaction AtomicStateWriteStage = "before_transaction"
	AtomicStateWriteBeforeCommit      AtomicStateWriteStage = "before_atomic_commit"
	AtomicStateWriteAfterCommit       AtomicStateWriteStage = "after_atomic_commit"
	AtomicStateWriteAfterSync         AtomicStateWriteStage = "after_sync"
)

// SetStatesAtomic persists all writes in one Badger transaction and syncs the
// value log before returning success. A transaction error leaves every prior
// value intact; a Sync error is returned after the all-or-nothing transaction
// and callers must fail-stop because durability can no longer be asserted.
func (s *BadgerStore) SetStatesAtomic(writes []StateWrite) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("atomic state write: store is nil")
	}
	if len(writes) == 0 {
		return nil
	}
	// An app-v20 FinalizeBlock keeps one consensus transaction open through
	// Commit. In that path the handshake tuple must join the existing
	// transaction: independently committing or syncing here would recreate the
	// exact partial-durability window the outer transaction closes.
	if s.txn != nil {
		return s.update(func(txn *badger.Txn) error {
			for i := range writes {
				if err := s.txnSet(txn, stateKey(writes[i].Key), writes[i].Value); err != nil {
					return fmt.Errorf("set staged state entry %d (%q): %w", i, writes[i].Key, err)
				}
			}
			return nil
		})
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteBeforeTransaction); err != nil {
		return fmt.Errorf("atomic state write before transaction: %w", err)
	}

	if err := s.db.Update(func(txn *badger.Txn) error {
		for i := range writes {
			if err := s.txnSet(txn, stateKey(writes[i].Key), writes[i].Value); err != nil {
				return fmt.Errorf("set state entry %d (%q): %w", i, writes[i].Key, err)
			}
		}
		if err := runAtomicStateWriteFaultHook(AtomicStateWriteBeforeCommit); err != nil {
			return fmt.Errorf("before atomic commit: %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("commit atomic state transaction: %w", err)
	}

	if err := runAtomicStateWriteFaultHook(AtomicStateWriteAfterCommit); err != nil {
		return fmt.Errorf("after atomic state commit: %w", err)
	}
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("sync atomic state transaction: %w", err)
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteAfterSync); err != nil {
		return fmt.Errorf("after atomic state sync: %w", err)
	}
	return nil
}
