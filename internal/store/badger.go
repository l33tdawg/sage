package store

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// BadgerStore manages on-chain state in BadgerDB.
type BadgerStore struct {
	db *badger.DB
}

// NewBadgerStore opens or creates a BadgerDB at the given path.
func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Suppress BadgerDB logs

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	return &BadgerStore{db: db}, nil
}

// memoryKey returns the BadgerDB key for a memory's on-chain state.
func memoryKey(memoryID string) []byte {
	return []byte("memory:" + memoryID)
}

// nonceKey returns the BadgerDB key for an agent's nonce.
func nonceKey(agentID string) []byte {
	return []byte("nonce:" + agentID)
}

// stateKey returns the BadgerDB key for app state.
func stateKey(key string) []byte {
	return []byte("state:" + key)
}

// agentOnChainKey returns the BadgerDB key for an agent's on-chain state.
func agentOnChainKey(agentID string) []byte {
	return []byte("agent:" + agentID)
}

// OnChainAgent represents an agent's on-chain state in BadgerDB.
type OnChainAgent struct {
	AgentID        string `json:"agent_id"`
	Name           string `json:"name"`                      // Mutable display name (GUI-editable)
	RegisteredName string `json:"registered_name,omitempty"` // Immutable name assigned at registration
	Role           string `json:"role"`
	BootBio        string `json:"boot_bio,omitempty"`
	Provider       string `json:"provider,omitempty"`
	P2PAddress     string `json:"p2p_address,omitempty"`
	Clearance      uint8  `json:"clearance"`
	DomainAccess   string `json:"domain_access,omitempty"`
	VisibleAgents  string `json:"visible_agents,omitempty"`
	OrgID          string `json:"org_id,omitempty"`
	DeptID         string `json:"dept_id,omitempty"`
	RegisteredAt   int64  `json:"registered_at"` // Block height
}

// MemoryHashEntry represents the on-chain state for a memory.
type MemoryHashEntry struct {
	ContentHash []byte
	Status      string
}

// SetMemoryHash stores or updates a memory's on-chain hash and status.
func (s *BadgerStore) SetMemoryHash(memoryID string, contentHash []byte, status string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Encode: contentHash length (4 bytes) + contentHash + status bytes
		val := make([]byte, 4+len(contentHash)+len(status))
		binary.BigEndian.PutUint32(val[:4], uint32(len(contentHash))) // #nosec G115 -- content hash length fits in uint32
		copy(val[4:4+len(contentHash)], contentHash)
		copy(val[4+len(contentHash):], status)
		return txn.Set(memoryKey(memoryID), val)
	})
}

// GetMemoryHash retrieves a memory's on-chain hash and status.
func (s *BadgerStore) GetMemoryHash(memoryID string) (contentHash []byte, status string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(memoryKey(memoryID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) < 4 {
				return fmt.Errorf("invalid memory hash entry")
			}
			hashLen := binary.BigEndian.Uint32(val[:4])
			if int(4+hashLen) > len(val) { // #nosec G115 -- hashLen from 4-byte prefix, always fits in int
				return fmt.Errorf("invalid memory hash entry")
			}
			contentHash = make([]byte, hashLen)
			copy(contentHash, val[4:4+hashLen])
			status = string(val[4+hashLen:])
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, "", fmt.Errorf("memory not found: %s", memoryID)
	}
	return
}

// SetNonce stores or updates an agent's nonce.
func (s *BadgerStore) SetNonce(agentID string, nonce uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, nonce)
		return txn.Set(nonceKey(agentID), val)
	})
}

// GetNonce retrieves an agent's current nonce.
func (s *BadgerStore) GetNonce(agentID string) (uint64, error) {
	var nonce uint64
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(nonceKey(agentID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid nonce entry")
			}
			nonce = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return 0, nil // New agent, nonce starts at 0
	}
	return nonce, err
}

// SetState stores a key-value pair in the state namespace.
func (s *BadgerStore) SetState(key string, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(stateKey(key), value)
	})
}

// GetState retrieves a value from the state namespace.
func (s *BadgerStore) GetState(key string) ([]byte, error) {
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(stateKey(key))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(v []byte) error {
			val = make([]byte, len(v))
			copy(val, v)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

// ComputeAppHash computes a deterministic SHA-256 hash over all state.
// CRITICAL: This must be deterministic — sorted key iteration.
func (s *BadgerStore) ComputeAppHash() ([]byte, error) {
	h := sha256.New()

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		// Collect all keys for sorting (BadgerDB iterates in sorted order by default,
		// but we make it explicit for safety)
		type kv struct {
			key, val []byte
		}
		var entries []kv

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := make([]byte, len(item.Key()))
			copy(k, item.Key())
			valErr := item.Value(func(v []byte) error {
				val := make([]byte, len(v))
				copy(val, v)
				entries = append(entries, kv{key: k, val: val})
				return nil
			})
			if valErr != nil {
				return valErr
			}
		}

		// Sort by key for determinism
		sort.Slice(entries, func(i, j int) bool {
			return string(entries[i].key) < string(entries[j].key)
		})

		for _, e := range entries {
			h.Write(e.key)
			h.Write(e.val)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("compute app hash: %w", err)
	}

	hash := h.Sum(nil)
	return hash, nil
}

// validatorStatsKey returns the BadgerDB key for a validator's vote stats.
func validatorStatsKey(validatorID string) []byte {
	return []byte("vstats:" + validatorID)
}

// ValidatorStats holds per-validator vote counters stored on-chain.
type ValidatorStats struct {
	TotalVotes  uint64
	AcceptVotes uint64
	LastBlockHeight uint64
}

// encodeValidatorStats encodes stats to bytes (24 bytes: 3 x uint64).
func encodeValidatorStats(s *ValidatorStats) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], s.TotalVotes)
	binary.BigEndian.PutUint64(buf[8:16], s.AcceptVotes)
	binary.BigEndian.PutUint64(buf[16:24], s.LastBlockHeight)
	return buf
}

// decodeValidatorStats decodes stats from bytes.
func decodeValidatorStats(data []byte) (*ValidatorStats, error) {
	if len(data) != 24 {
		return nil, fmt.Errorf("invalid validator stats: expected 24 bytes, got %d", len(data))
	}
	return &ValidatorStats{
		TotalVotes:      binary.BigEndian.Uint64(data[0:8]),
		AcceptVotes:     binary.BigEndian.Uint64(data[8:16]),
		LastBlockHeight: binary.BigEndian.Uint64(data[16:24]),
	}, nil
}

// IncrementVoteStats increments a validator's vote counters on-chain.
func (s *BadgerStore) IncrementVoteStats(validatorID string, accepted bool, blockHeight uint64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		stats := &ValidatorStats{}

		// Try to read existing stats
		item, getErr := txn.Get(validatorStatsKey(validatorID))
		if getErr == nil {
			valErr := item.Value(func(val []byte) error {
				existing, decErr := decodeValidatorStats(val)
				if decErr != nil {
					return decErr
				}
				stats = existing
				return nil
			})
			if valErr != nil {
				return valErr
			}
		} else if getErr != badger.ErrKeyNotFound {
			return getErr
		}

		stats.TotalVotes++
		if accepted {
			stats.AcceptVotes++
		}
		stats.LastBlockHeight = blockHeight

		return txn.Set(validatorStatsKey(validatorID), encodeValidatorStats(stats))
	})
}

// GetValidatorStats retrieves a validator's on-chain vote stats.
func (s *BadgerStore) GetValidatorStats(validatorID string) (*ValidatorStats, error) {
	var stats *ValidatorStats
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(validatorStatsKey(validatorID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			decoded, decErr := decodeValidatorStats(val)
			if decErr != nil {
				return decErr
			}
			stats = decoded
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return &ValidatorStats{}, nil
	}
	if err != nil {
		return nil, err
	}
	return stats, nil
}

// GetAllValidatorStats scans all validator stats from BadgerDB (sorted by ID).
func (s *BadgerStore) GetAllValidatorStats() (map[string]*ValidatorStats, error) {
	result := make(map[string]*ValidatorStats)
	prefix := []byte("vstats:")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			validatorID := key[len("vstats:"):]

			valErr := item.Value(func(val []byte) error {
				decoded, decErr := decodeValidatorStats(val)
				if decErr != nil {
					return decErr
				}
				result[validatorID] = decoded
				return nil
			})
			if valErr != nil {
				return valErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan validator stats: %w", err)
	}
	return result, nil
}

// SaveValidators persists the validator set to BadgerDB.
func (s *BadgerStore) SaveValidators(validators map[string]int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for id, power := range validators {
			key := []byte("validator:" + id)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(power)) // #nosec G115 -- validator power is always non-negative
			if err := txn.Set(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// LoadValidators loads the persisted validator set from BadgerDB.
func (s *BadgerStore) LoadValidators() (map[string]int64, error) {
	result := make(map[string]int64)
	prefix := []byte("validator:")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			validatorID := key[len("validator:"):]

			valErr := item.Value(func(val []byte) error {
				if len(val) != 8 {
					return fmt.Errorf("invalid validator power: expected 8 bytes")
				}
				power := int64(binary.BigEndian.Uint64(val)) // #nosec G115 -- validator power fits in int64
				result[validatorID] = power
				return nil
			})
			if valErr != nil {
				return valErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("load validators: %w", err)
	}
	return result, nil
}

// CloseBadger closes the BadgerDB.
func (s *BadgerStore) CloseBadger() error {
	return s.db.Close()
}

// --- Federation Access Control ---

// grantKey returns the BadgerDB key for an access grant.
func grantKey(domain, agentID string) []byte {
	return []byte("grant:" + domain + ":" + agentID)
}

// domainKey returns the BadgerDB key for a domain registry entry.
func domainKey(name string) []byte {
	return []byte("domain:" + name)
}

// accessReqKey returns the BadgerDB key for an access request.
func accessReqKey(requestID string) []byte {
	return []byte("access_req:" + requestID)
}

// accessLogKey returns the BadgerDB key for an access log entry.
func accessLogKey(height int64, seq int) []byte {
	return []byte(fmt.Sprintf("access_log:%016d:%08d", height, seq))
}

// encodeString writes a length-prefixed string to buf at offset, returns new offset.
func encodeString(buf []byte, offset int, s string) int {
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(s))) // #nosec G115 -- string length fits in uint32
	copy(buf[offset+4:offset+4+len(s)], s)
	return offset + 4 + len(s)
}

// decodeString reads a length-prefixed string from buf at offset, returns string and new offset.
func decodeString(buf []byte, offset int) (string, int, error) {
	if offset+4 > len(buf) {
		return "", 0, fmt.Errorf("buffer too short for string length at offset %d", offset)
	}
	sLen := int(binary.BigEndian.Uint32(buf[offset : offset+4])) // #nosec G115 -- string length from 4-byte prefix, always fits in int
	if offset+4+sLen > len(buf) {
		return "", 0, fmt.Errorf("buffer too short for string data at offset %d", offset)
	}
	s := string(buf[offset+4 : offset+4+sLen])
	return s, offset + 4 + sLen, nil
}

// SetAccessGrant stores an access grant in BadgerDB.
// Encoding: level (1 byte) + expiresAt (8 bytes) + granterID (length-prefixed).
func (s *BadgerStore) SetAccessGrant(domain, agentID string, level uint8, expiresAt int64, granterID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 1+8+4+len(granterID))
		val[0] = level
		binary.BigEndian.PutUint64(val[1:9], uint64(expiresAt)) // #nosec G115 -- expiry timestamp is always non-negative
		encodeString(val, 9, granterID)
		return txn.Set(grantKey(domain, agentID), val)
	})
}

// GetAccessGrant retrieves an access grant from BadgerDB.
func (s *BadgerStore) GetAccessGrant(domain, agentID string) (level uint8, expiresAt int64, granterID string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(grantKey(domain, agentID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) < 9 {
				return fmt.Errorf("invalid grant entry")
			}
			level = val[0]
			expiresAt = int64(binary.BigEndian.Uint64(val[1:9])) // #nosec G115 -- expiry timestamp fits in int64
			var decErr error
			granterID, _, decErr = decodeString(val, 9)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return 0, 0, "", fmt.Errorf("grant not found: %s/%s", domain, agentID)
	}
	return
}

// DeleteAccessGrant removes an access grant from BadgerDB.
func (s *BadgerStore) DeleteAccessGrant(domain, agentID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(grantKey(domain, agentID))
	})
}

// HasAccess checks if an agent has the required access level on a domain.
// Uses blockTime for deterministic expiry checks (not time.Now()).
func (s *BadgerStore) HasAccess(domain, agentID string, requiredLevel uint8, blockTime time.Time) (bool, error) {
	var has bool
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(grantKey(domain, agentID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			if len(val) < 9 {
				return fmt.Errorf("invalid grant entry")
			}
			level := val[0]
			expiresAt := int64(binary.BigEndian.Uint64(val[1:9])) // #nosec G115 -- expiry timestamp fits in int64

			if level < requiredLevel {
				has = false
				return nil
			}
			if expiresAt > 0 && blockTime.Unix() >= expiresAt {
				has = false
				return nil
			}
			has = true
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return has, nil
}

// RegisterDomain registers a domain in BadgerDB.
// Encoding: ownerID (length-prefixed) + parentDomain (length-prefixed) + height (8 bytes).
func (s *BadgerStore) RegisterDomain(name, ownerID, parentDomain string, height int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(ownerID)+4+len(parentDomain)+8)
		offset := encodeString(val, 0, ownerID)
		offset = encodeString(val, offset, parentDomain)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		return txn.Set(domainKey(name), val)
	})
}

// GetDomainOwner retrieves the owner of a domain.
func (s *BadgerStore) GetDomainOwner(name string) (string, error) {
	var ownerID string
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(domainKey(name))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			var decErr error
			ownerID, _, decErr = decodeString(val, 0)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", fmt.Errorf("domain not found: %s", name)
	}
	return ownerID, err
}

// IsDomainOwnerOrAncestor checks if agentID owns the given domain or any ancestor.
// Walks up the hierarchy by splitting on ".".
func (s *BadgerStore) IsDomainOwnerOrAncestor(domain, agentID string) (bool, error) {
	parts := strings.Split(domain, ".")
	for i := len(parts); i > 0; i-- {
		ancestor := strings.Join(parts[:i], ".")
		owner, err := s.GetDomainOwner(ancestor)
		if err != nil {
			// Domain not registered at this level, continue up
			continue
		}
		if owner == agentID {
			return true, nil
		}
	}
	return false, nil
}

// SetAccessRequest stores an access request in BadgerDB.
// Encoding: requesterID (length-prefixed) + targetDomain (length-prefixed) + status (length-prefixed) + createdHeight (8 bytes).
func (s *BadgerStore) SetAccessRequest(requestID string, requesterID, targetDomain, status string, createdHeight int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(requesterID)+4+len(targetDomain)+4+len(status)+8)
		offset := encodeString(val, 0, requesterID)
		offset = encodeString(val, offset, targetDomain)
		offset = encodeString(val, offset, status)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(createdHeight)) // #nosec G115 -- block height is always non-negative
		return txn.Set(accessReqKey(requestID), val)
	})
}

// GetAccessRequest retrieves an access request from BadgerDB.
func (s *BadgerStore) GetAccessRequest(requestID string) (requesterID, targetDomain, status string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(accessReqKey(requestID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var offset int
			var decErr error
			requesterID, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			targetDomain, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			status, _, decErr = decodeString(val, offset)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", "", fmt.Errorf("access request not found: %s", requestID)
	}
	return
}

// UpdateAccessRequestStatus updates the status of an access request in BadgerDB.
func (s *BadgerStore) UpdateAccessRequestStatus(requestID, status string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(accessReqKey(requestID))
		if err != nil {
			return err
		}
		var requesterID, targetDomain string
		var createdHeight int64
		err = item.Value(func(val []byte) error {
			var offset int
			var decErr error
			requesterID, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			targetDomain, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			// Skip old status
			_, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			if offset+8 > len(val) {
				return fmt.Errorf("invalid access request entry")
			}
			createdHeight = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115 -- block height fits in int64
			return nil
		})
		if err != nil {
			return err
		}

		// Re-encode with new status
		newVal := make([]byte, 4+len(requesterID)+4+len(targetDomain)+4+len(status)+8)
		offset := encodeString(newVal, 0, requesterID)
		offset = encodeString(newVal, offset, targetDomain)
		offset = encodeString(newVal, offset, status)
		binary.BigEndian.PutUint64(newVal[offset:offset+8], uint64(createdHeight)) // #nosec G115 -- block height is always non-negative
		return txn.Set(accessReqKey(requestID), newVal)
	})
}

// --- Organization / Federation / Classification ---

// orgKey returns the BadgerDB key for an organization.
func orgKey(orgID string) []byte {
	return []byte("org:" + orgID)
}

// orgMemberKey returns the BadgerDB key for an org membership.
func orgMemberKey(orgID, agentID string) []byte {
	return []byte("org_member:" + orgID + ":" + agentID)
}

// agentOrgKey returns the BadgerDB key for the agent→org reverse lookup.
func agentOrgKey(agentID string) []byte {
	return []byte("agent_org:" + agentID)
}

// federationKey returns the BadgerDB key for a federation entry.
func federationKey(fedID string) []byte {
	return []byte("federation:" + fedID)
}

// memClassKey returns the BadgerDB key for a memory classification.
func memClassKey(memoryID string) []byte {
	return []byte("mem_class:" + memoryID)
}

// RegisterOrg registers an organization in BadgerDB.
// Encoding: name (length-prefixed) + description (length-prefixed) + adminAgent (length-prefixed) + height (8 bytes).
func (s *BadgerStore) RegisterOrg(orgID, name, description, adminAgent string, height int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(name)+4+len(description)+4+len(adminAgent)+8)
		offset := encodeString(val, 0, name)
		offset = encodeString(val, offset, description)
		offset = encodeString(val, offset, adminAgent)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		return txn.Set(orgKey(orgID), val)
	})
}

// GetOrg retrieves an organization's name and admin agent from BadgerDB.
func (s *BadgerStore) GetOrg(orgID string) (name, adminAgent string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(orgKey(orgID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var offset int
			var decErr error
			name, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			// skip description
			_, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			adminAgent, _, decErr = decodeString(val, offset)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", fmt.Errorf("org not found: %s", orgID)
	}
	return
}

// AddOrgMember adds a member to an organization in BadgerDB.
// Encoding: clearance (1 byte) + role (length-prefixed) + height (8 bytes).
// Also sets the agent→org reverse lookup.
func (s *BadgerStore) AddOrgMember(orgID, agentID string, clearance uint8, role string, height int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 1+4+len(role)+8)
		val[0] = clearance
		encodeString(val, 1, role)
		binary.BigEndian.PutUint64(val[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is always non-negative
		if err := txn.Set(orgMemberKey(orgID, agentID), val); err != nil {
			return err
		}
		// Reverse lookup: agent→org
		return txn.Set(agentOrgKey(agentID), []byte(orgID))
	})
}

// RemoveOrgMember removes a member from an organization in BadgerDB.
func (s *BadgerStore) RemoveOrgMember(orgID, agentID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(orgMemberKey(orgID, agentID)); err != nil {
			return err
		}
		return txn.Delete(agentOrgKey(agentID))
	})
}

// GetMemberClearance retrieves a member's clearance level and role.
func (s *BadgerStore) GetMemberClearance(orgID, agentID string) (clearance uint8, role string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(orgMemberKey(orgID, agentID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) < 1 {
				return fmt.Errorf("invalid member entry")
			}
			clearance = val[0]
			var decErr error
			role, _, decErr = decodeString(val, 1)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return 0, "", fmt.Errorf("member not found: %s/%s", orgID, agentID)
	}
	return
}

// SetMemberClearance updates a member's clearance level in BadgerDB.
func (s *BadgerStore) SetMemberClearance(orgID, agentID string, clearance uint8) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(orgMemberKey(orgID, agentID))
		if err != nil {
			return err
		}
		var role string
		var height int64
		err = item.Value(func(val []byte) error {
			if len(val) < 1 {
				return fmt.Errorf("invalid member entry")
			}
			var offset int
			var decErr error
			role, offset, decErr = decodeString(val, 1)
			if decErr != nil {
				return decErr
			}
			if offset+8 > len(val) {
				return fmt.Errorf("invalid member entry: missing height")
			}
			height = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115 -- block height fits in int64
			return nil
		})
		if err != nil {
			return err
		}

		newVal := make([]byte, 1+4+len(role)+8)
		newVal[0] = clearance
		encodeString(newVal, 1, role)
		binary.BigEndian.PutUint64(newVal[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is always non-negative
		return txn.Set(orgMemberKey(orgID, agentID), newVal)
	})
}

// GetAgentOrg retrieves the organization an agent belongs to (reverse lookup).
func (s *BadgerStore) GetAgentOrg(agentID string) (orgID string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(agentOrgKey(agentID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			orgID = string(val)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", fmt.Errorf("agent org not found: %s", agentID)
	}
	return
}

// SetFederation stores a federation entry in BadgerDB.
// Encoding: proposerOrg (length-prefixed) + targetOrg (length-prefixed) + maxClearance (1 byte)
//   + expiresAt (8 bytes) + requiresApproval (1 byte) + status (length-prefixed)
//   + allowedDomains count (4 bytes) + each domain (length-prefixed)
//   + allowedDepts count (4 bytes) + each dept (length-prefixed).
func (s *BadgerStore) SetFederation(fedID string, proposerOrg, targetOrg string, allowedDomains []string, maxClearance uint8, expiresAt int64, requiresApproval bool, status string, allowedDepts ...[]string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		var depts []string
		if len(allowedDepts) > 0 {
			depts = allowedDepts[0]
		}
		// Calculate total size
		size := 4 + len(proposerOrg) + 4 + len(targetOrg) + 1 + 8 + 1 + 4 + len(status) + 4
		for _, d := range allowedDomains {
			size += 4 + len(d)
		}
		size += 4 // allowedDepts count
		for _, d := range depts {
			size += 4 + len(d)
		}
		val := make([]byte, size)
		offset := encodeString(val, 0, proposerOrg)
		offset = encodeString(val, offset, targetOrg)
		val[offset] = maxClearance
		offset++
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(expiresAt)) // #nosec G115 -- expiry timestamp is always non-negative
		offset += 8
		if requiresApproval {
			val[offset] = 1
		} else {
			val[offset] = 0
		}
		offset++
		offset = encodeString(val, offset, status)
		binary.BigEndian.PutUint32(val[offset:offset+4], uint32(len(allowedDomains))) // #nosec G115 -- slice length fits in uint32
		offset += 4
		for _, d := range allowedDomains {
			offset = encodeString(val, offset, d)
		}
		binary.BigEndian.PutUint32(val[offset:offset+4], uint32(len(depts))) // #nosec G115 -- slice length fits in uint32
		offset += 4
		for _, d := range depts {
			offset = encodeString(val, offset, d)
		}
		return txn.Set(federationKey(fedID), val)
	})
}

// GetFederation retrieves a federation entry from BadgerDB.
func (s *BadgerStore) GetFederation(fedID string) (proposerOrg, targetOrg string, maxClearance uint8, expiresAt int64, status string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(federationKey(fedID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var offset int
			var decErr error
			proposerOrg, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			targetOrg, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			if offset >= len(val) {
				return fmt.Errorf("invalid federation entry")
			}
			maxClearance = val[offset]
			offset++
			if offset+8 > len(val) {
				return fmt.Errorf("invalid federation entry: missing expiresAt")
			}
			expiresAt = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115 -- expiry timestamp fits in int64
			offset += 8
			// skip requiresApproval (1 byte)
			offset++
			status, _, decErr = decodeString(val, offset)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", 0, 0, "", fmt.Errorf("federation not found: %s", fedID)
	}
	return
}

// UpdateFederationStatus updates the status field of a federation entry.
func (s *BadgerStore) UpdateFederationStatus(fedID, status string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(federationKey(fedID))
		if err != nil {
			return err
		}
		// Read existing values
		var proposerOrg, targetOrg string
		var maxClearance uint8
		var expiresAt int64
		var requiresApproval bool
		var allowedDomains []string
		var allowedDepts []string

		err = item.Value(func(val []byte) error {
			var offset int
			var decErr error
			proposerOrg, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			targetOrg, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			maxClearance = val[offset]
			offset++
			expiresAt = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115 -- expiry timestamp fits in int64
			offset += 8
			requiresApproval = val[offset] == 1
			offset++
			// skip old status
			_, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			// Read allowed domains
			if offset+4 <= len(val) {
				count := int(binary.BigEndian.Uint32(val[offset : offset+4])) // #nosec G115 -- array count fits in int
				offset += 4
				for i := 0; i < count; i++ {
					var d string
					d, offset, decErr = decodeString(val, offset)
					if decErr != nil {
						return decErr
					}
					allowedDomains = append(allowedDomains, d)
				}
			}
			// Read allowed depts (backward compat)
			if offset+4 <= len(val) {
				count := int(binary.BigEndian.Uint32(val[offset : offset+4])) // #nosec G115 -- array count fits in int
				offset += 4
				for i := 0; i < count; i++ {
					var d string
					d, offset, decErr = decodeString(val, offset)
					if decErr != nil {
						return decErr
					}
					allowedDepts = append(allowedDepts, d)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}

		// Re-encode with new status
		size := 4 + len(proposerOrg) + 4 + len(targetOrg) + 1 + 8 + 1 + 4 + len(status) + 4
		for _, d := range allowedDomains {
			size += 4 + len(d)
		}
		size += 4
		for _, d := range allowedDepts {
			size += 4 + len(d)
		}
		newVal := make([]byte, size)
		offset := encodeString(newVal, 0, proposerOrg)
		offset = encodeString(newVal, offset, targetOrg)
		newVal[offset] = maxClearance
		offset++
		binary.BigEndian.PutUint64(newVal[offset:offset+8], uint64(expiresAt)) // #nosec G115 -- expiry timestamp is always non-negative
		offset += 8
		if requiresApproval {
			newVal[offset] = 1
		} else {
			newVal[offset] = 0
		}
		offset++
		offset = encodeString(newVal, offset, status)
		binary.BigEndian.PutUint32(newVal[offset:offset+4], uint32(len(allowedDomains))) // #nosec G115 -- slice length fits in uint32
		offset += 4
		for _, d := range allowedDomains {
			offset = encodeString(newVal, offset, d)
		}
		binary.BigEndian.PutUint32(newVal[offset:offset+4], uint32(len(allowedDepts))) // #nosec G115 -- slice length fits in uint32
		offset += 4
		for _, d := range allowedDepts {
			offset = encodeString(newVal, offset, d)
		}
		return txn.Set(federationKey(fedID), newVal)
	})
}

// FindFederation scans for an active federation between two orgs (either direction).
func (s *BadgerStore) FindFederation(orgA, orgB string) (fedID string, err error) {
	prefix := []byte("federation:")
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			id := key[len("federation:"):]

			foundErr := item.Value(func(val []byte) error {
				var offset int
				var decErr error
				var proposer, target string
				proposer, offset, decErr = decodeString(val, 0)
				if decErr != nil {
					return decErr
				}
				target, offset, decErr = decodeString(val, offset)
				if decErr != nil {
					return decErr
				}
				// Check both directions
				if (proposer != orgA || target != orgB) && (proposer != orgB || target != orgA) {
					return nil
				}
				// Skip maxClearance(1) + expiresAt(8) + requiresApproval(1)
				offset += 10
				var status string
				status, _, decErr = decodeString(val, offset)
				if decErr != nil {
					return decErr
				}
				if status == "active" {
					fedID = id
				}
				return nil
			})
			if foundErr != nil {
				return foundErr
			}
			if fedID != "" {
				return nil // Found it
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if fedID == "" {
		return "", fmt.Errorf("no active federation between %s and %s", orgA, orgB)
	}
	return fedID, nil
}

// SetMemoryClassification stores a memory's classification level in BadgerDB.
func (s *BadgerStore) SetMemoryClassification(memoryID string, classification uint8) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(memClassKey(memoryID), []byte{classification})
	})
}

// GetMemoryClassification retrieves a memory's classification level.
func (s *BadgerStore) GetMemoryClassification(memoryID string) (uint8, error) {
	var classification uint8
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(memClassKey(memoryID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			if len(val) != 1 {
				return fmt.Errorf("invalid classification entry")
			}
			classification = val[0]
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		// Default to INTERNAL (1) for backward compat
		return 1, nil
	}
	return classification, err
}

// --- Department Management ---

// deptKey returns the BadgerDB key for a department.
func deptKey(orgID, deptID string) []byte {
	return []byte("dept:" + orgID + ":" + deptID)
}

// deptMemberKey returns the BadgerDB key for a department membership.
func deptMemberKey(orgID, deptID, agentID string) []byte {
	return []byte("dept_member:" + orgID + ":" + deptID + ":" + agentID)
}

// agentDeptKey returns the BadgerDB key for the agent→dept reverse lookup.
func agentDeptKey(agentID string) []byte {
	return []byte("agent_dept:" + agentID)
}

// RegisterDept registers a department within an organization in BadgerDB.
// Encoding: name (length-prefixed) + description (length-prefixed) + parentDept (length-prefixed) + height (8 bytes).
func (s *BadgerStore) RegisterDept(orgID, deptID, name, description, parentDept string, height int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(name)+4+len(description)+4+len(parentDept)+8)
		offset := encodeString(val, 0, name)
		offset = encodeString(val, offset, description)
		offset = encodeString(val, offset, parentDept)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		return txn.Set(deptKey(orgID, deptID), val)
	})
}

// GetDept retrieves a department's name and description from BadgerDB.
func (s *BadgerStore) GetDept(orgID, deptID string) (name, description string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(deptKey(orgID, deptID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var offset int
			var decErr error
			name, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			description, _, decErr = decodeString(val, offset)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", fmt.Errorf("dept not found: %s/%s", orgID, deptID)
	}
	return
}

// GetOrgDepts returns all department IDs for an organization by scanning the dept prefix.
func (s *BadgerStore) GetOrgDepts(orgID string) ([]string, error) {
	var deptIDs []string
	prefix := []byte("dept:" + orgID + ":")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := string(it.Item().Key())
			// key = "dept:{orgID}:{deptID}"
			deptID := key[len("dept:"+orgID+":"):]
			deptIDs = append(deptIDs, deptID)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan org depts: %w", err)
	}
	return deptIDs, nil
}

// AddDeptMember adds a member to a department in BadgerDB.
// Encoding: clearance (1 byte) + role (length-prefixed) + height (8 bytes).
// Also sets the agent→dept reverse lookup.
func (s *BadgerStore) AddDeptMember(orgID, deptID, agentID string, clearance uint8, role string, height int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 1+4+len(role)+8)
		val[0] = clearance
		encodeString(val, 1, role)
		binary.BigEndian.PutUint64(val[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is always non-negative
		if err := txn.Set(deptMemberKey(orgID, deptID, agentID), val); err != nil {
			return err
		}
		// Reverse lookup: agent→dept (value = "orgID:deptID")
		return txn.Set(agentDeptKey(agentID), []byte(orgID+":"+deptID))
	})
}

// RemoveDeptMember removes a member from a department in BadgerDB.
func (s *BadgerStore) RemoveDeptMember(orgID, deptID, agentID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(deptMemberKey(orgID, deptID, agentID)); err != nil {
			return err
		}
		return txn.Delete(agentDeptKey(agentID))
	})
}

// GetDeptMemberClearance retrieves a department member's clearance level and role.
func (s *BadgerStore) GetDeptMemberClearance(orgID, deptID, agentID string) (clearance uint8, role string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(deptMemberKey(orgID, deptID, agentID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) < 1 {
				return fmt.Errorf("invalid dept member entry")
			}
			clearance = val[0]
			var decErr error
			role, _, decErr = decodeString(val, 1)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return 0, "", fmt.Errorf("dept member not found: %s/%s/%s", orgID, deptID, agentID)
	}
	return
}

// SetDeptMemberClearance updates a department member's clearance level in BadgerDB.
func (s *BadgerStore) SetDeptMemberClearance(orgID, deptID, agentID string, clearance uint8) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(deptMemberKey(orgID, deptID, agentID))
		if err != nil {
			return err
		}
		var role string
		var height int64
		err = item.Value(func(val []byte) error {
			if len(val) < 1 {
				return fmt.Errorf("invalid dept member entry")
			}
			var offset int
			var decErr error
			role, offset, decErr = decodeString(val, 1)
			if decErr != nil {
				return decErr
			}
			if offset+8 > len(val) {
				return fmt.Errorf("invalid dept member entry: missing height")
			}
			height = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115 -- block height fits in int64
			return nil
		})
		if err != nil {
			return err
		}

		newVal := make([]byte, 1+4+len(role)+8)
		newVal[0] = clearance
		encodeString(newVal, 1, role)
		binary.BigEndian.PutUint64(newVal[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is always non-negative
		return txn.Set(deptMemberKey(orgID, deptID, agentID), newVal)
	})
}

// GetAgentDept retrieves the department an agent belongs to (reverse lookup).
// Returns orgID and deptID by splitting the stored value on ":".
func (s *BadgerStore) GetAgentDept(agentID string) (orgID, deptID string, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		var item *badger.Item
		item, err = txn.Get(agentDeptKey(agentID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			parts := strings.SplitN(string(val), ":", 2)
			if len(parts) != 2 {
				return fmt.Errorf("invalid agent dept value: %s", string(val))
			}
			orgID = parts[0]
			deptID = parts[1]
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", fmt.Errorf("agent dept not found: %s", agentID)
	}
	return
}

// GetFederationAllowedDepts retrieves the allowed departments for a federation.
func (s *BadgerStore) GetFederationAllowedDepts(fedID string) ([]string, error) {
	var allowedDepts []string
	err := s.db.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get(federationKey(fedID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			var offset int
			var decErr error
			// Skip proposerOrg
			_, offset, decErr = decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			// Skip targetOrg
			_, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			// Skip maxClearance(1) + expiresAt(8) + requiresApproval(1)
			offset += 10
			// Skip status
			_, offset, decErr = decodeString(val, offset)
			if decErr != nil {
				return decErr
			}
			// Read allowedDomains
			if offset+4 > len(val) {
				return nil // No domains or depts
			}
			domainCount := int(binary.BigEndian.Uint32(val[offset : offset+4])) // #nosec G115 -- array count fits in int
			offset += 4
			for i := 0; i < domainCount; i++ {
				_, offset, decErr = decodeString(val, offset)
				if decErr != nil {
					return decErr
				}
			}
			// Read allowedDepts (if present — backward compat)
			if offset+4 > len(val) {
				return nil // No dept data
			}
			deptCount := int(binary.BigEndian.Uint32(val[offset : offset+4])) // #nosec G115 -- array count fits in int
			offset += 4
			for i := 0; i < deptCount; i++ {
				var d string
				d, offset, decErr = decodeString(val, offset)
				if decErr != nil {
					return decErr
				}
				allowedDepts = append(allowedDepts, d)
			}
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, fmt.Errorf("federation not found: %s", fedID)
	}
	if err != nil {
		return nil, err
	}
	return allowedDepts, nil
}

// HasAccessMultiOrg checks multi-org access: direct grants, same-org clearance, or federation agreements.
// Uses blockTime for deterministic expiry checks (not time.Now()).
func (s *BadgerStore) HasAccessMultiOrg(domain, agentID string, memoryClassification uint8, blockTime time.Time) (bool, error) {
	// Step 1: Check direct grant (existing HasAccess — covers same-org grants)
	directAccess, err := s.HasAccess(domain, agentID, 1, blockTime)
	if err == nil && directAccess {
		return true, nil
	}

	// Step 2: Check org membership
	agentOrg, err := s.GetAgentOrg(agentID)
	if err != nil {
		// Agent not in any org — only direct grants work
		return false, nil
	}

	// Step 3: Get agent's clearance within their org
	agentClearance, _, err := s.GetMemberClearance(agentOrg, agentID)
	if err != nil {
		return false, nil
	}

	// Step 4: Check if the domain belongs to the same org
	domainOwner, err := s.GetDomainOwner(domain)
	if err != nil {
		return false, nil // Domain doesn't exist
	}
	domainOrg, err := s.GetAgentOrg(domainOwner)
	if err != nil {
		// Domain owner not in an org — fall back to direct grants only
		return false, nil
	}

	if agentOrg == domainOrg {
		// Same org — just check clearance level
		if agentClearance >= memoryClassification {
			return true, nil
		}
		return false, nil
	}

	// Step 5: Different org — need federation agreement
	fedID, err := s.FindFederation(agentOrg, domainOrg)
	if err != nil {
		return false, nil // No federation
	}

	// Step 6: Get federation details and check constraints
	_, _, maxClearance, expiresAtUnix, status, err := s.GetFederation(fedID)
	if err != nil || status != "active" {
		return false, nil
	}

	// Check expiry
	if expiresAtUnix > 0 && blockTime.Unix() >= expiresAtUnix {
		return false, nil
	}

	// Check clearance ceiling
	if memoryClassification > maxClearance {
		return false, nil // Memory classification exceeds federation ceiling
	}
	if agentClearance < memoryClassification {
		return false, nil // Agent's clearance insufficient for this memory
	}

	// Step 7: Department-aware filtering for cross-org federation
	_, agentDept, deptErr := s.GetAgentDept(agentID)
	if deptErr == nil && agentDept != "" {
		// Agent is in a department — check if federation restricts by dept
		allowedDepts, fedDeptErr := s.GetFederationAllowedDepts(fedID)
		if fedDeptErr == nil && len(allowedDepts) > 0 {
			// Check for wildcard
			hasWildcard := false
			for _, d := range allowedDepts {
				if d == "*" {
					hasWildcard = true
					break
				}
			}
			if !hasWildcard {
				// Verify agent's dept is in the allowed list
				deptAllowed := false
				for _, d := range allowedDepts {
					if d == agentDept {
						deptAllowed = true
						break
					}
				}
				if !deptAllowed {
					return false, nil
				}
			}
		}
	}

	return true, nil
}

// AppendAccessLog appends an audit log entry to BadgerDB.
// Encoding: agentID (length-prefixed) + domain (length-prefixed) + action (length-prefixed).
func (s *BadgerStore) AppendAccessLog(height int64, agentID, domain, action string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		// Find next sequence number for this height by scanning prefix
		prefix := []byte(fmt.Sprintf("access_log:%016d:", height))
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		seq := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			seq++
		}

		val := make([]byte, 4+len(agentID)+4+len(domain)+4+len(action))
		offset := encodeString(val, 0, agentID)
		offset = encodeString(val, offset, domain)
		encodeString(val, offset, action)

		return txn.Set(accessLogKey(height, seq), val)
	})
}

// RegisterAgent stores a new agent's on-chain identity.
func (s *BadgerStore) RegisterAgent(agentID, name, role, bio, provider, p2pAddress string, height int64) error {
	agent := &OnChainAgent{
		AgentID:        agentID,
		Name:           name,
		RegisteredName: name, // Immutable — preserved forever as the original identity
		Role:           role,
		BootBio:        bio,
		Provider:       provider,
		P2PAddress:     p2pAddress,
		Clearance:      1, // Default: INTERNAL
		RegisteredAt:   height,
	}
	data, err := json.Marshal(agent)
	if err != nil {
		return fmt.Errorf("marshal agent: %w", err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(agentOnChainKey(agentID), data)
	})
}

// GetRegisteredAgent retrieves an agent's on-chain state.
func (s *BadgerStore) GetRegisteredAgent(agentID string) (*OnChainAgent, error) {
	var agent OnChainAgent
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(agentOnChainKey(agentID))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &agent)
		})
	})
	if err != nil {
		return nil, err
	}
	// Backfill for agents registered before RegisteredName was introduced
	if agent.RegisteredName == "" {
		agent.RegisteredName = agent.Name
	}
	return &agent, nil
}

// IsAgentRegistered checks if an agent exists on-chain.
func (s *BadgerStore) IsAgentRegistered(agentID string) bool {
	err := s.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(agentOnChainKey(agentID))
		return err
	})
	return err == nil
}

// UpdateAgentMeta updates an agent's mutable display name and bio on-chain.
// RegisteredName is the permanent on-chain identity and is NEVER modified here.
func (s *BadgerStore) UpdateAgentMeta(agentID, name, bio string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(agentOnChainKey(agentID))
		if err != nil {
			return fmt.Errorf("agent not found: %w", err)
		}
		var agent OnChainAgent
		if valErr := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &agent)
		}); valErr != nil {
			return valErr
		}
		// Backfill RegisteredName for agents created before v5.2.0
		if agent.RegisteredName == "" {
			agent.RegisteredName = agent.Name
		}
		// Only update mutable fields — RegisteredName is immutable and must not be touched.
		agent.Name = name
		agent.BootBio = bio
		data, err := json.Marshal(&agent)
		if err != nil {
			return err
		}
		return txn.Set(agentOnChainKey(agentID), data)
	})
}

// --- Dynamic Validator Governance ---

// ValidatorPersist holds validator power and public key for enhanced persistence.
type ValidatorPersist struct {
	Power  int64  `json:"power"`
	PubKey string `json:"pubkey"` // hex-encoded Ed25519 public key
}

// DeleteState removes a key from the state namespace.
func (s *BadgerStore) DeleteState(key string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(stateKey(key))
	})
}

// PrefixKeys returns all keys in the state namespace matching the given prefix,
// sorted lexicographically. Keys are returned WITHOUT the "state:" prefix.
func (s *BadgerStore) PrefixKeys(prefix string) ([]string, error) {
	fullPrefix := stateKey(prefix)
	var keys []string

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // keys only
		opts.Prefix = fullPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(fullPrefix); it.ValidForPrefix(fullPrefix); it.Next() {
			key := string(it.Item().Key())
			// Strip the "state:" prefix to return the logical key
			if len(key) > 6 {
				keys = append(keys, key[6:])
			}
		}
		return nil
	})

	return keys, err
}

// SetGovProposal stores a governance proposal in BadgerDB.
func (s *BadgerStore) SetGovProposal(proposalID string, data []byte) error {
	return s.SetState("gov:proposal:"+proposalID, data)
}

// GetGovProposal retrieves a governance proposal from BadgerDB.
func (s *BadgerStore) GetGovProposal(proposalID string) ([]byte, error) {
	return s.GetState("gov:proposal:" + proposalID)
}

// SetGovVote stores a governance vote.
func (s *BadgerStore) SetGovVote(proposalID, validatorID, decision string) error {
	key := "gov:vote:" + proposalID + ":" + validatorID
	return s.SetState(key, []byte(decision))
}

// GetGovVote retrieves a single governance vote.
func (s *BadgerStore) GetGovVote(proposalID, validatorID string) (string, error) {
	key := "gov:vote:" + proposalID + ":" + validatorID
	val, err := s.GetState(key)
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return string(val), nil
}

// GetGovVotes retrieves all votes for a governance proposal.
// Returns map[validatorID]decision. Uses sorted prefix scan for determinism.
func (s *BadgerStore) GetGovVotes(proposalID string) (map[string]string, error) {
	result := make(map[string]string)
	prefix := []byte("state:gov:vote:" + proposalID + ":")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			// key = "state:gov:vote:{proposalID}:{validatorID}"
			validatorID := key[len("state:gov:vote:"+proposalID+":"):]

			valErr := item.Value(func(val []byte) error {
				result[validatorID] = string(val)
				return nil
			})
			if valErr != nil {
				return valErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("scan gov votes: %w", err)
	}
	return result, nil
}

// SetActiveProposal sets the currently active governance proposal ID.
func (s *BadgerStore) SetActiveProposal(proposalID string) error {
	return s.SetState("gov:active", []byte(proposalID))
}

// GetActiveProposal returns the currently active governance proposal ID, or "" if none.
func (s *BadgerStore) GetActiveProposal() (string, error) {
	val, err := s.GetState("gov:active")
	if err != nil {
		return "", nil // No active proposal is not an error
	}
	if val == nil {
		return "", nil
	}
	return string(val), nil
}

// ClearActiveProposal removes the active proposal pointer.
func (s *BadgerStore) ClearActiveProposal() error {
	return s.DeleteState("gov:active")
}

// SetGovCooldown records the last proposal height for a proposer.
func (s *BadgerStore) SetGovCooldown(proposerID string, height int64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(height)) // #nosec G115 -- block height is always non-negative
	return s.SetState("gov:cooldown:"+proposerID, val)
}

// GetGovCooldown returns the last proposal height for a proposer, or 0 if none.
func (s *BadgerStore) GetGovCooldown(proposerID string) (int64, error) {
	val, err := s.GetState("gov:cooldown:" + proposerID)
	if err != nil {
		return 0, nil // No cooldown is not an error
	}
	if val == nil || len(val) != 8 {
		return 0, nil
	}
	return int64(binary.BigEndian.Uint64(val)), nil // #nosec G115 -- block height fits in int64
}

// SaveValidatorsV2 persists validators with both power and public key.
func (s *BadgerStore) SaveValidatorsV2(validators map[string]ValidatorPersist) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for id, vp := range validators {
			key := []byte("validator:" + id)
			data, err := json.Marshal(vp)
			if err != nil {
				return fmt.Errorf("marshal validator %s: %w", id, err)
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}
		}
		return nil
	})
}

// LoadValidatorsV2 loads validators with power and public key.
// Backward compatible: if value is exactly 8 bytes, treats as legacy power-only
// and derives pubkey from validator ID (hex-encoded pubkey).
func (s *BadgerStore) LoadValidatorsV2() (map[string]ValidatorPersist, error) {
	result := make(map[string]ValidatorPersist)
	prefix := []byte("validator:")

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := string(item.Key())
			validatorID := key[len("validator:"):]

			valErr := item.Value(func(val []byte) error {
				if len(val) == 8 {
					// Legacy format: 8-byte big-endian power
					power := int64(binary.BigEndian.Uint64(val)) // #nosec G115 -- validator power fits in int64
					result[validatorID] = ValidatorPersist{
						Power:  power,
						PubKey: validatorID, // ID IS the hex pubkey for non-app validators
					}
				} else {
					// New format: JSON
					var vp ValidatorPersist
					if err := json.Unmarshal(val, &vp); err != nil {
						return fmt.Errorf("unmarshal validator %s: %w", validatorID, err)
					}
					result[validatorID] = vp
				}
				return nil
			})
			if valErr != nil {
				return valErr
			}
		}
		return nil
	})

	return result, err
}

// SetRawForTest writes a raw key-value pair to BadgerDB. Test-only.
func (s *BadgerStore) SetRawForTest(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// SetAgentPermission updates an agent's permissions on-chain.
func (s *BadgerStore) SetAgentPermission(agentID string, clearance uint8, domainAccess, visibleAgents, orgID, deptID string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(agentOnChainKey(agentID))
		if err != nil {
			return fmt.Errorf("agent not found: %w", err)
		}
		var agent OnChainAgent
		if valErr := item.Value(func(val []byte) error {
			return json.Unmarshal(val, &agent)
		}); valErr != nil {
			return valErr
		}
		agent.Clearance = clearance
		agent.DomainAccess = domainAccess
		agent.VisibleAgents = visibleAgents
		if orgID != "" {
			agent.OrgID = orgID
		}
		if deptID != "" {
			agent.DeptID = deptID
		}
		data, err := json.Marshal(&agent)
		if err != nil {
			return err
		}
		return txn.Set(agentOnChainKey(agentID), data)
	})
}

// ListRegisteredAgents returns all on-chain registered agents.
func (s *BadgerStore) ListRegisteredAgents() ([]OnChainAgent, error) {
	var agents []OnChainAgent
	prefix := []byte("agent:")
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var agent OnChainAgent
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &agent)
			}); err != nil {
				continue
			}
			agents = append(agents, agent)
		}
		return nil
	})
	return agents, err
}
