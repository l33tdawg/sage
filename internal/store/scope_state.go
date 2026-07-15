package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/scope"
)

var (
	ErrScopeRevision = errors.New("scope revision conflict")
	ErrScopeRetired  = errors.New("scope is permanently retired")
)

// scopeRecordKey is AppHash-covered canonical v11.9 scope state. Scope records
// are written only from deterministic ABCI execution once app-v20 is active.
func scopeRecordKey(scopeID string) []byte { return []byte("state:scope:" + scopeID) }

// scopeDomainKey maps one exact selected domain to its canonical scope. The
// mapping makes scope lookup an O(1) on-chain read and allows the writer to
// reject two active scopes claiming the same domain atomically.
func scopeDomainKey(domain string) []byte { return []byte("state:scope-domain:" + domain) }

// scopeRevisionKey is the immutable audit anchor for one accepted roster
// revision. The fixed-width decimal suffix preserves revision order in prefix
// scans while the complete scope ID remains part of the key.
func scopeRevisionKey(scopeID string, revision uint64) []byte {
	return []byte("state:scope-revision:" + scopeID + ":" + fmt.Sprintf("%020d", revision))
}

func scopeRevisionDigest(encoded []byte) []byte {
	preimage := append([]byte("sage:scope-revision:v1\x00"), encoded...)
	digest := sha256.Sum256(preimage)
	return digest[:]
}

// SetScopeRecord creates or advances one canonical scope roster atomically.
// Reapplying the exact same record is allowed for crash replay; a different
// record at the same revision and every revision regression fail closed. Scope
// domain mappings are replaced in the same Badger transaction, so no committed
// state can point at a partially updated roster.
func (s *BadgerStore) SetScopeRecord(record scope.Record) error {
	encoded, err := scope.Encode(record)
	if err != nil {
		return fmt.Errorf("encode scope record: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		key := scopeRecordKey(record.ScopeID)
		var previous *scope.Record
		exactReplay := false
		item, getErr := txn.Get(key)
		if getErr == nil {
			decodeErr := item.Value(func(value []byte) error {
				decoded, err := scope.Decode(value)
				if err != nil {
					return err
				}
				previous = &decoded
				return nil
			})
			if decodeErr != nil {
				return fmt.Errorf("decode existing scope %q: %w", record.ScopeID, decodeErr)
			}
		} else if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}

		if previous != nil {
			switch {
			case previous.State == scope.StateRetired && record.State != scope.StateRetired:
				return ErrScopeRetired
			case record.Revision < previous.Revision:
				return fmt.Errorf("%w: %d < %d", ErrScopeRevision, record.Revision, previous.Revision)
			case record.Revision == previous.Revision:
				if err := item.Value(func(value []byte) error {
					if bytes.Equal(value, encoded) {
						exactReplay = true
						return nil
					}
					return fmt.Errorf("%w: revision %d has different content", ErrScopeRevision, record.Revision)
				}); err != nil {
					return err
				}
			}
		}
		if exactReplay {
			return setScopeRevisionAnchor(txn, record.ScopeID, record.Revision, encoded)
		}

		// Check all new domain claims before deleting any old mapping. A collision
		// therefore leaves the previous complete record and mapping set intact.
		for _, domain := range record.Domains {
			mapped, err := getScopeDomainMapping(txn, domain.Name)
			if err != nil {
				return err
			}
			if mapped != "" && mapped != record.ScopeID {
				return fmt.Errorf("domain %q is already bound to scope %q", domain.Name, mapped)
			}
		}

		if previous != nil {
			for _, oldDomain := range previous.Domains {
				if !containsScopeDomain(record.Domains, oldDomain.Name) {
					if err := txn.Delete(scopeDomainKey(oldDomain.Name)); err != nil {
						return err
					}
				}
			}
		}
		if record.State != scope.StateRetired {
			for _, domain := range record.Domains {
				if err := txn.Set(scopeDomainKey(domain.Name), []byte(record.ScopeID)); err != nil {
					return err
				}
			}
		} else if previous != nil {
			for _, oldDomain := range previous.Domains {
				if err := txn.Delete(scopeDomainKey(oldDomain.Name)); err != nil {
					return err
				}
			}
		}
		if err := setScopeRevisionAnchor(txn, record.ScopeID, record.Revision, encoded); err != nil {
			return err
		}
		return txn.Set(key, encoded)
	})
}

func setScopeRevisionAnchor(txn *badger.Txn, scopeID string, revision uint64, encoded []byte) error {
	key := scopeRevisionKey(scopeID, revision)
	want := scopeRevisionDigest(encoded)
	item, err := txn.Get(key)
	if err == nil {
		return item.Value(func(value []byte) error {
			if bytes.Equal(value, want) {
				return nil
			}
			return fmt.Errorf("%w: revision %d audit anchor differs", ErrScopeRevision, revision)
		})
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	return txn.Set(key, want)
}

// GetScopeRecord returns one decoded canonical record, or (nil, nil) if it has
// not been created. A malformed persisted value is an error; callers must not
// treat corrupted consensus state as an absent scope.
func (s *BadgerStore) GetScopeRecord(scopeID string) (*scope.Record, error) {
	var record *scope.Record
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(scopeRecordKey(scopeID))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			decoded, err := scope.Decode(value)
			if err != nil {
				return err
			}
			record = &decoded
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scope record %q: %w", scopeID, err)
	}
	return record, nil
}

// ListScopeRecords returns every canonical scope head in bytewise scope-ID
// order. It validates that each key agrees with the encoded record so operator
// visibility never papers over corrupted consensus state.
func (s *BadgerStore) ListScopeRecords() ([]scope.Record, error) {
	prefix := []byte("state:scope:")
	records := make([]scope.Record, 0)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			scopeID := string(item.Key()[len(prefix):])
			if err := item.Value(func(value []byte) error {
				record, err := scope.Decode(value)
				if err != nil {
					return err
				}
				if record.ScopeID != scopeID {
					return fmt.Errorf("scope key %q disagrees with record %q", scopeID, record.ScopeID)
				}
				records = append(records, record)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list scope records: %w", err)
	}
	return records, nil
}

// GetScopeRevisionHash returns the immutable domain-separated SHA-256 anchor
// for one accepted revision, or nil when that revision has never existed.
func (s *BadgerStore) GetScopeRevisionHash(scopeID string, revision uint64) ([]byte, error) {
	var digest []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(scopeRevisionKey(scopeID, revision))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			digest = append([]byte(nil), value...)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get scope revision %q/%d: %w", scopeID, revision, err)
	}
	if len(digest) != sha256.Size {
		return nil, fmt.Errorf("scope revision %q/%d has invalid digest length %d", scopeID, revision, len(digest))
	}
	return digest, nil
}

// GetScopeForDomain returns the exact-domain scope mapping and record, or
// (nil, nil) when no active/paused scope claims domain. Retired scopes remove
// their mapping and therefore never accidentally turn an old domain back on.
func (s *BadgerStore) GetScopeForDomain(domain string) (*scope.Record, error) {
	var scopeID string
	err := s.db.View(func(txn *badger.Txn) error {
		mapped, err := getScopeDomainMapping(txn, domain)
		if err != nil {
			return err
		}
		scopeID = mapped
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("get scope mapping for domain %q: %w", domain, err)
	}
	if scopeID == "" {
		return nil, nil
	}
	record, err := s.GetScopeRecord(scopeID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("scope mapping for domain %q points to missing scope %q", domain, scopeID)
	}
	return record, nil
}

func getScopeDomainMapping(txn *badger.Txn, domain string) (string, error) {
	item, err := txn.Get(scopeDomainKey(domain))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	var value string
	if err := item.Value(func(raw []byte) error {
		if len(raw) == 0 {
			return errors.New("empty scope domain mapping")
		}
		value = string(raw)
		return nil
	}); err != nil {
		return "", err
	}
	return value, nil
}

func containsScopeDomain(domains []scope.Domain, target string) bool {
	for _, domain := range domains {
		if domain.Name == target {
			return true
		}
	}
	return false
}
