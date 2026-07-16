package store

// Read-only Badger views used by the federation dashboard's peer-RBAC
// projection. The grant/domain records themselves remain the ordinary
// consensus-owned keys; this file only enumerates them for an operator UI.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
)

// RegisteredDomain is the decoded, AppHash-visible domain registry record.
type RegisteredDomain struct {
	DomainName    string
	OwnerAgentID  string
	ParentDomain  string
	CreatedHeight int64
}

// AgentAccessGrant is one AppHash-visible direct grant issued to an agent.
// Ancestor grants retain their normal meaning at the authorization layer; the
// dashboard lists the exact grant row so an operator can revoke it precisely.
type AgentAccessGrant struct {
	Domain    string
	AgentID   string
	Level     uint8
	ExpiresAt int64
	GranterID string
}

// ListRegisteredDomains returns every consensus domain registry row ordered by
// domain name. A corrupt row fails the whole view instead of silently hiding an
// authorization object from the operator.
func (s *BadgerStore) ListRegisteredDomains() ([]RegisteredDomain, error) {
	prefix := []byte("domain:")
	var out []RegisteredDomain
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			name := string(item.KeyCopy(nil)[len(prefix):])
			var rec RegisteredDomain
			rec.DomainName = name
			if err := item.Value(func(value []byte) error {
				owner, off, err := decodeString(value, 0)
				if err != nil {
					return err
				}
				parent, off, err := decodeString(value, off)
				if err != nil {
					return err
				}
				if len(value) != off+8 {
					return fmt.Errorf("invalid domain entry %q", name)
				}
				rec.OwnerAgentID = owner
				rec.ParentDomain = parent
				rec.CreatedHeight = int64(binary.BigEndian.Uint64(value[off : off+8])) // #nosec G115 -- stored block height
				return nil
			}); err != nil {
				return fmt.Errorf("decode domain %q: %w", name, err)
			}
			out = append(out, rec)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].DomainName < out[j].DomainName })
	return out, err
}

// ListAccessGrantsForAgent returns every exact direct grant for agentID,
// ordered by domain. The fixed agent-id suffix makes this unambiguous even
// though grant keys are indexed primarily by domain.
func (s *BadgerStore) ListAccessGrantsForAgent(agentID string) ([]AgentAccessGrant, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("agent id is required")
	}
	prefix := []byte("grant:")
	suffix := []byte(":" + agentID)
	var out []AgentAccessGrant
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if !bytes.HasSuffix(key, suffix) {
				continue
			}
			domainEnd := len(key) - len(suffix)
			if domainEnd <= len(prefix) {
				return fmt.Errorf("invalid access-grant key")
			}
			rec := AgentAccessGrant{
				Domain:  string(key[len(prefix):domainEnd]),
				AgentID: agentID,
			}
			if err := item.Value(func(value []byte) error {
				if len(value) < 13 {
					return fmt.Errorf("invalid grant entry")
				}
				rec.Level = value[0]
				rec.ExpiresAt = int64(binary.BigEndian.Uint64(value[1:9])) // #nosec G115 -- stored unix timestamp
				granter, off, err := decodeString(value, 9)
				if err != nil {
					return err
				}
				if off != len(value) {
					return fmt.Errorf("invalid trailing grant data")
				}
				rec.GranterID = granter
				return nil
			}); err != nil {
				return fmt.Errorf("decode grant %q/%s: %w", rec.Domain, agentID, err)
			}
			out = append(out, rec)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	return out, err
}
