package store

import (
	"bytes"
	"fmt"
	"sync"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
)

const taintServingHeaderReadLimit = 256
const taintServingCacheEntries = 4

// TaintServingReadScope bounds added cold header reads across one request.
// It contains no caller, memory or decision identity. A zero value has no cold
// allowance; callers should use NewTaintServingReadScope.
type TaintServingReadScope struct {
	mu        sync.Mutex
	remaining int
}

func NewTaintServingReadScope() *TaintServingReadScope {
	return &TaintServingReadScope{remaining: taintServingHeaderReadLimit}
}

func (s *TaintServingReadScope) takeHeaderRead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.remaining == 0 {
		return false
	}
	s.remaining--
	return true
}

type taintIncompleteStatus uint8

const (
	taintIncompleteUnknown taintIncompleteStatus = iota
	taintIncompletePresent
	taintIncompleteNone
)

type taintStatusEntry struct {
	readTs uint64
	status taintIncompleteStatus
}

// Entries are owned by this exact DB instance and contain only snapshot/status.
type taintServingStatusCache struct {
	mu      sync.Mutex
	db      *badger.DB
	entries []taintStatusEntry // oldest first, at most four entries
}

func newTaintServingStatusCache(db *badger.DB) *taintServingStatusCache {
	return &taintServingStatusCache{db: db}
}

func (c *taintServingStatusCache) lookup(db *badger.DB, ts uint64) (taintIncompleteStatus, bool) {
	if c == nil || c.db != db {
		return taintIncompleteUnknown, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, entry := range c.entries {
		if entry.readTs == ts {
			copy(c.entries[i:], c.entries[i+1:])
			c.entries[len(c.entries)-1] = entry
			return entry.status, true
		}
	}
	return taintIncompleteUnknown, false
}

func (c *taintServingStatusCache) publish(db *badger.DB, ts uint64, status taintIncompleteStatus) {
	if c == nil || c.db != db {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, entry := range c.entries {
		if entry.readTs == ts {
			// A concurrent full proof is stronger than a capped scan. Partial
			// request scans are never published by the caller.
			if entry.status != taintIncompleteUnknown {
				status = entry.status
			}
			copy(c.entries[i:], c.entries[i+1:])
			c.entries = c.entries[:len(c.entries)-1]
			break
		}
	}
	if len(c.entries) == taintServingCacheEntries {
		copy(c.entries, c.entries[1:])
		c.entries = c.entries[:len(c.entries)-1]
	}
	c.entries = append(c.entries, taintStatusEntry{readTs: ts, status: status})
}

// Only this constructor marks a committed DB.View reader cache-eligible.
// An existing write/speculative reader preserves its snapshot but never gains
// eligibility, and its callback handle rejects typed mutations.
func (s *BadgerStore) withTaintServingReadSnapshot(fn func(*BadgerStore) error) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("canonical store is unavailable")
	}
	read := func(txn *badger.Txn, committed bool) error {
		return fn(&BadgerStore{
			db:                              s.db,
			txn:                             txn,
			canonicalMemoryProjectionHealth: s.canonicalMemoryProjectionHealth,
			taintServingCache:               s.taintServingCache,
			taintServingCommittedRead:       committed,
			taintServingReadOnly:            true,
		})
	}
	if s.txn != nil {
		return read(s.txn, s.taintServingCommittedRead)
	}
	return s.db.View(func(txn *badger.Txn) error { return read(txn, true) })
}

func (s *BadgerStore) incompleteTaintStatus(scope *TaintServingReadScope) (taintIncompleteStatus, error) {
	if s.taintServingCommittedRead {
		if status, ok := s.taintServingCache.lookup(s.db, s.txn.ReadTs()); ok {
			return status, nil
		}
	}
	prefix := taint.DecisionPrefix()
	options := badger.DefaultIteratorOptions
	options.Prefix = prefix
	options.PrefetchValues = false
	it := s.txn.NewIterator(options)
	defer it.Close()
	status := taintIncompleteNone
	read := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		if read == taintServingHeaderReadLimit {
			// The next key is present, but its value is not read.
			status = taintIncompleteUnknown
			break
		}
		if !scope.takeHeaderRead() {
			// This request's partial scan is not a property of the snapshot.
			return taintIncompleteUnknown, nil
		}
		read++
		key := it.Item().Key()
		id := string(bytes.TrimPrefix(key, prefix))
		if err := taint.ValidateDecisionID(id); err != nil {
			return taintIncompleteUnknown, fmt.Errorf("%w: invalid decision header key: %v", ErrTaintStateMalformed, err)
		}
		raw, err := it.Item().ValueCopy(nil)
		if err != nil {
			return taintIncompleteUnknown, err
		}
		header, err := taint.DecodeHeader(raw)
		if err != nil {
			return taintIncompleteUnknown, fmt.Errorf("%w: %v", ErrTaintStateMalformed, err)
		}
		if header.Status.Incomplete() {
			status = taintIncompletePresent
			break
		}
	}
	if s.taintServingCommittedRead {
		s.taintServingCache.publish(s.db, s.txn.ReadTs(), status)
	}
	return status, nil
}
