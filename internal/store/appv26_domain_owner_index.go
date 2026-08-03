package store

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

const (
	appV26DomainOwnerIndexPrefix = "appv26:domain-owner-index:"
	appV26DomainOwnerIndexReady  = "appv26:domain-owner-index-ready"
)

func appV26DomainOwnerIndexKey(ownerID, domain string) []byte {
	return []byte(appV26DomainOwnerIndexPrefix + ownerID + ":" + domain)
}

func appV26DomainOwnerIndexOwnerPrefix(ownerID string) []byte {
	return []byte(appV26DomainOwnerIndexPrefix + ownerID + ":")
}

func appV26DomainOwnerIndexActiveTxn(txn *badger.Txn) (bool, error) {
	_, err := txn.Get([]byte(appV26DomainOwnerIndexReady))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

func decodeDomainOwner(value []byte) (string, error) {
	owner, _, err := decodeString(value, 0)
	return owner, err
}

// rebuildAppV26DomainOwnerIndexTxn creates the authoritative reverse index at
// the app-v26 activation boundary. The ordinary domain registry remains the
// source of truth; this index exists solely to make caller-owned pagination
// complete and bounded after transfers.
func (s *BadgerStore) rebuildAppV26DomainOwnerIndexTxn(txn *badger.Txn) error {
	err := s.appV23EffectivePrefixTxn(txn, []byte("domain:"), func(key, value []byte) error {
		owner, err := decodeDomainOwner(value)
		if err != nil {
			return fmt.Errorf("decode app-v26 domain owner index row: %w", err)
		}
		domain := strings.TrimPrefix(string(key), "domain:")
		if domain == "" {
			return errors.New("empty domain in app-v26 owner index")
		}
		return s.txnSet(txn, appV26DomainOwnerIndexKey(owner, domain), []byte{1})
	})
	if err != nil {
		return err
	}
	return s.txnSet(txn, []byte(appV26DomainOwnerIndexReady), []byte{1})
}

// ListOwnedDomainsPage returns one stable lexicographic page from the
// consensus-maintained owner index. after is the last domain from the previous
// page (exclusive), not an opaque global roster cursor.
func (s *BadgerStore) ListOwnedDomainsPage(ownerID, after string, limit int) ([]string, bool, error) {
	if err := validateCanonicalAgentID("domain owner", ownerID); err != nil {
		return nil, false, err
	}
	if limit < 1 || limit > 100 {
		return nil, false, errors.New("owned domain page limit must be between 1 and 100")
	}
	prefix := appV26DomainOwnerIndexOwnerPrefix(ownerID)
	result := make([]string, 0, limit+1)
	err := s.view(func(txn *badger.Txn) error {
		active, err := appV26DomainOwnerIndexActiveTxn(txn)
		if err != nil {
			return err
		}
		if !active {
			return errors.New("app-v26 domain owner index is unavailable")
		}
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		seek := prefix
		if after != "" {
			seek = appV26DomainOwnerIndexKey(ownerID, after)
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			domain := string(bytes.TrimPrefix(key, prefix))
			if domain == "" || (after != "" && domain <= after) {
				continue
			}
			result = append(result, domain)
			if len(result) == limit+1 {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	more := len(result) > limit
	if more {
		result = result[:limit]
	}
	return result, more, nil
}
