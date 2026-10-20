package store

// Node-local org policy state (RFC #443): the enrollment outcome records, the
// accepted policy high-water mark per org, and the latest equivocation notice.
//
// This is deliberately NOT stored in BadgerDB. Every Badger key outside a short
// exclusion list feeds the consensus AppHash, and this state is written by one
// node's REST handlers, outside consensus. Storing it there would make the
// enrolling node's AppHash diverge from its peers at the next block. It lives
// here instead, next to the org enrollment tokens, in the node's own SQLite
// database (WAL, synchronous=FULL), which no AppHash rule covers.
//
// The table is a plain key/value map owned by internal/orgpolicy, which keeps
// its key layout and encoding; the REST layer adapts these two methods to
// orgpolicy.KV. SetOrgPolicyStatesAtomic applies all writes in ONE SQLite
// transaction, which is what makes token consumption and the replayable
// enrollment result a single durable write. It fires the same crash-fixture
// hook stages as BadgerStore.SetStatesAtomic so the SIGKILL matrix exercises
// this path (no-ops outside the v119testfixture build).

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// migrateOrgPolicyState creates the org_policy_state table. Idempotent.
func (s *SQLiteStore) migrateOrgPolicyState(ctx context.Context) error {
	if _, err := s.writeExecContext(ctx, `
	CREATE TABLE IF NOT EXISTS org_policy_state (
		key        TEXT PRIMARY KEY,
		value      BLOB NOT NULL,
		updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
	)`); err != nil {
		return fmt.Errorf("create org policy state table: %w", err)
	}
	return nil
}

// GetOrgPolicyState returns the value stored under key, or (nil, nil) when the
// key is absent.
func (s *SQLiteStore) GetOrgPolicyState(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	err := s.conn.QueryRowContext(ctx, `SELECT value FROM org_policy_state WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get org policy state: %w", err)
	}
	return value, nil
}

// SetOrgPolicyStatesAtomic upserts every write in one SQLite transaction: all
// of them become durable together, or none do.
func (s *SQLiteStore) SetOrgPolicyStatesAtomic(ctx context.Context, writes []StateWrite) error {
	if len(writes) == 0 {
		return nil
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteBeforeTransaction); err != nil {
		return fmt.Errorf("org policy state write before transaction: %w", err)
	}
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return fmt.Errorf("begin org policy state transaction: %w", err)
	}
	defer unlock()
	for i := range writes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO org_policy_state (key, value) VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value,
				updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
			writes[i].Key, writes[i].Value); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("set org policy state entry %d (%q): %w", i, writes[i].Key, err)
		}
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteBeforeCommit); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("org policy state write before commit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit org policy state transaction: %w", err)
	}
	// With synchronous=FULL the commit above is already fsynced, so the
	// after-commit and after-sync boundaries coincide; both are fired so the
	// crash matrix covers the same four stages as the Badger path.
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteAfterCommit); err != nil {
		return fmt.Errorf("org policy state write after commit: %w", err)
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteAfterSync); err != nil {
		return fmt.Errorf("org policy state write after sync: %w", err)
	}
	return nil
}

// CompareAndSetOrgPolicyStatesAtomic checks every condition and applies all
// writes in one SQLite transaction. It is the concurrency fence for single-use
// enrollment and policy high-water-mark updates.
func (s *SQLiteStore) CompareAndSetOrgPolicyStatesAtomic(ctx context.Context, conditions []StateCondition, writes []StateWrite) (bool, error) {
	if len(writes) == 0 {
		return true, nil
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteBeforeTransaction); err != nil {
		return false, fmt.Errorf("org policy compare-and-set before transaction: %w", err)
	}
	tx, unlock, err := s.beginTxLocked(ctx)
	if err != nil {
		return false, fmt.Errorf("begin org policy compare-and-set transaction: %w", err)
	}
	defer unlock()
	for i := range conditions {
		var current []byte
		err := tx.QueryRowContext(ctx, `SELECT value FROM org_policy_state WHERE key = ?`, conditions[i].Key).Scan(&current)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			if conditions[i].Value != nil {
				_ = tx.Rollback()
				return false, nil
			}
		case err != nil:
			_ = tx.Rollback()
			return false, fmt.Errorf("read org policy condition %d (%q): %w", i, conditions[i].Key, err)
		case conditions[i].Value == nil || !bytes.Equal(current, conditions[i].Value):
			_ = tx.Rollback()
			return false, nil
		}
	}
	for i := range writes {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO org_policy_state (key, value) VALUES (?, ?)
			ON CONFLICT(key) DO UPDATE SET value = excluded.value,
				updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')`,
			writes[i].Key, writes[i].Value); err != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("set org policy state entry %d (%q): %w", i, writes[i].Key, err)
		}
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteBeforeCommit); err != nil {
		_ = tx.Rollback()
		return false, fmt.Errorf("org policy compare-and-set before commit: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit org policy compare-and-set transaction: %w", err)
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteAfterCommit); err != nil {
		return false, fmt.Errorf("org policy compare-and-set after commit: %w", err)
	}
	if err := runAtomicStateWriteFaultHook(AtomicStateWriteAfterSync); err != nil {
		return false, fmt.Errorf("org policy compare-and-set after sync: %w", err)
	}
	return true, nil
}
