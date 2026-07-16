package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/poe"
)

// ErrDomainAlreadyRegistered is returned by RegisterDomain when the domain
// already has a non-empty owner. Use TransferDomain for authorized ownership changes.
var ErrDomainAlreadyRegistered = errors.New("domain already registered")

// ErrDomainPathTooDeep prevents callers from confusing a deliberately refused
// ancestor walk with a genuinely unowned domain and auto-claiming beneath it.
var ErrDomainPathTooDeep = errors.New("domain path exceeds 16 segments")

// ErrAgentProofReplayed is returned when a delegated app-v17 agent proof has
// already been consumed. The marker is consensus state, not the REST process's
// in-memory replay cache, so a Byzantine proposer cannot bypass it.
var ErrAgentProofReplayed = errors.New("agent proof already consumed")

// ErrAccessGrantNotFound distinguishes a genuinely absent exact grant from a
// storage/corruption failure. Cleanup code must never treat an arbitrary read
// error as proof that a security-sensitive grant was revoked.
var ErrAccessGrantNotFound = errors.New("access grant not found")

var agentProofPrefix = []byte("agentproof:")

// A strict post-app-v20 block is one bounded Badger transaction. Limit
// opportunistic proof-marker GC on a transaction-scoped store so an old marker
// backlog cannot exhaust that transaction independently of the block byte/count
// budget. Later claims and blocks continue deterministic expiry-order pruning.
const maxScopedAgentProofPrunePerClaim = 8

// BadgerStore manages on-chain state in BadgerDB.
type BadgerStore struct {
	db *badger.DB

	// Startup index repair is constructor-local, but the exported Ensure
	// methods remain safe if an operator tool calls them concurrently. The
	// first migration under this lock scrubs the short-lived dirty-tree Badger
	// progress keys before either sidecar is trusted.
	indexBackfillMu                    sync.Mutex
	legacyIndexBackfillMarkersScrubbed bool

	// txn is non-nil only on a transaction-scoped clone returned by
	// BeginConsensusTransaction. Keeping the transaction on a clone (rather
	// than the process-wide store) prevents concurrent CheckTx/query readers
	// from observing or joining an uncommitted FinalizeBlock.
	txn *badger.Txn

	// mutationHook is an unexported deterministic fault-injection seam owned by
	// the ABCI tests. Production callers always pass nil. It runs after each
	// successful typed Update boundary while the write is still uncommitted.
	mutationHook  func(int)
	mutationCount int

	// updateMutated/writeFailed describe the currently executing typed update
	// boundary. All Set/Delete calls go through txnSet/txnDelete, allowing an
	// error after an earlier staged write (or a failed Badger write such as
	// ErrTxnTooBig) to poison the outer transaction. Validation/read errors that
	// happen before any mutation remain ordinary transaction rejections.
	updateMutated  bool
	writeFailed    bool
	poisoned       error
	writeFaultHook func(int) error
	writeAttempts  int
}

// view executes fn against the transaction snapshot when this is a scoped
// consensus store, otherwise it retains the ordinary one-shot Badger view.
func (s *BadgerStore) view(fn func(*badger.Txn) error) error {
	if s.txn != nil {
		return fn(s.txn)
	}
	return s.db.View(fn)
}

// update stages fn in the long-lived consensus transaction when present. Each
// typed store method remains its own deterministic mutation boundary, but none
// of those boundaries becomes durable until CommitConsensusTransaction.
func (s *BadgerStore) update(fn func(*badger.Txn) error) error {
	if s.txn == nil {
		return s.db.Update(fn)
	}
	if s.poisoned != nil {
		return s.poisoned
	}
	s.updateMutated = false
	s.writeFailed = false
	err := fn(s.txn)
	mutated := s.updateMutated
	writeFailed := s.writeFailed
	s.updateMutated = false
	s.writeFailed = false
	if err != nil {
		if mutated || writeFailed {
			s.poisoned = fmt.Errorf("store: consensus transaction poisoned after partial mutation: %w", err)
		}
		return err
	}
	s.mutationCount++
	if s.mutationHook != nil {
		s.mutationHook(s.mutationCount)
	}
	return nil
}

// txnSet and txnDelete are the only mutation primitives used inside typed
// update closures. Tracking successful and failed attempts gives update a
// savepoint-like safety property: Badger has no rollback-to-savepoint, so a
// boundary that errors after touching the transaction poisons the whole outer
// transaction and Commit is refused.
func (s *BadgerStore) txnSet(txn *badger.Txn, key, value []byte) error {
	if s.txn != nil {
		s.writeAttempts++
		if s.writeFaultHook != nil {
			if err := s.writeFaultHook(s.writeAttempts); err != nil {
				s.writeFailed = true
				return err
			}
		}
	}
	err := txn.Set(key, value)
	if s.txn != nil {
		if err != nil {
			s.writeFailed = true
		} else {
			s.updateMutated = true
		}
	}
	return err
}

func (s *BadgerStore) txnDelete(txn *badger.Txn, key []byte) error {
	if s.txn != nil {
		s.writeAttempts++
		if s.writeFaultHook != nil {
			if err := s.writeFaultHook(s.writeAttempts); err != nil {
				s.writeFailed = true
				return err
			}
		}
	}
	err := txn.Delete(key)
	if s.txn != nil {
		if err != nil {
			s.writeFailed = true
		} else {
			s.updateMutated = true
		}
	}
	return err
}

// BeginConsensusTransaction returns a clone whose complete typed read/write
// surface shares one Badger write transaction. The caller must eventually call
// CommitConsensusTransaction or DiscardConsensusTransaction on the clone.
func (s *BadgerStore) BeginConsensusTransaction(mutationHook func(int)) *BadgerStore {
	if s.txn != nil {
		panic("store: nested consensus transaction")
	}
	return &BadgerStore{
		db:           s.db,
		txn:          s.db.NewTransaction(true),
		mutationHook: mutationHook,
	}
}

// InConsensusTransaction reports whether this handle is the transaction-scoped
// clone used by the app-v20 FinalizeBlock/Commit boundary. It lets the ABCI layer
// apply post-fork full-record limits without changing historical store behavior.
func (s *BadgerStore) InConsensusTransaction() bool {
	return s != nil && s.txn != nil
}

// CommitConsensusTransaction atomically publishes every staged mutation.
func (s *BadgerStore) CommitConsensusTransaction() error {
	if s.txn == nil {
		return errors.New("store: no consensus transaction to commit")
	}
	if s.poisoned != nil {
		err := s.poisoned
		s.DiscardConsensusTransaction()
		return err
	}
	if err := s.txn.Commit(); err != nil {
		return err
	}
	s.txn = nil
	// The normal SaveState path has the same durability contract. A successful
	// app-v20 Commit must not return until the transaction containing every
	// FinalizeBlock mutation and the handshake tuple has crossed Badger's Sync
	// boundary.
	if err := s.db.Sync(); err != nil {
		return fmt.Errorf("sync consensus transaction: %w", err)
	}
	return nil
}

// ConsensusTransactionError reports whether a typed update partially mutated
// the transaction before failing. Callers must discard rather than publish a
// FinalizeBlock response in this state.
func (s *BadgerStore) ConsensusTransactionError() error {
	if s == nil {
		return nil
	}
	return s.poisoned
}

// DiscardConsensusTransaction drops every staged mutation. It is idempotent so
// panic/error cleanup may call it unconditionally.
func (s *BadgerStore) DiscardConsensusTransaction() {
	if s == nil || s.txn == nil {
		return
	}
	s.txn.Discard()
	s.txn = nil
}

// DB returns the underlying *badger.DB handle. Intended for the v7.5
// snapshot integration (internal/snapshot.Options.LiveBadger) — passing
// the live handle to snapshot.Take lets the snapshotter call
// (*badger.DB).Backup directly without reopening the directory, which
// would conflict with the lockfile held by the running node.
//
// Do NOT use this accessor for general read/write operations — those
// should go through the typed methods on BadgerStore so call sites are
// audited against the on-chain key schema. Only the snapshot path has
// a legitimate need for the raw handle (the Backup primitive is part
// of badger's public API and the snapshot package documents the
// constraint).
func (s *BadgerStore) DB() *badger.DB {
	return s.db
}

// NewBadgerStore opens or creates a BadgerDB at the given path.
func NewBadgerStore(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Suppress BadgerDB logs

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	store := &BadgerStore{db: db}

	// Backfill the multi-org agent→orgs reverse index from the authoritative
	// org_member forward index. Cheap, idempotent — required for in-place
	// upgrades from versions that only maintained the single-slot agent_org
	// reverse lookup (which silently dropped non-primary memberships from
	// access checks).
	if backfillErr := store.EnsureAgentOrgsIndex(); backfillErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("backfill agent_orgs index: %w", backfillErr)
	}

	// Backfill the name→orgIDs reverse index from the authoritative org:*
	// forward entries. Idempotent — required for in-place upgrades from
	// pre-v6.6.9 binaries that didn't maintain it, so GET /v1/org/by-name
	// works against existing chain state without a reset.
	if backfillErr := store.EnsureOrgNameIndex(); backfillErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("backfill org_name index: %w", backfillErr)
	}

	return store, nil
}

// OpenBadgerStoreReadOnly opens an existing Badger database without running
// constructor backfills or permitting writes. State-sync and snapshot
// verification use this to recompute AppHash against the received bytes without
// validation itself changing consensus state.
func OpenBadgerStoreReadOnly(path string) (*BadgerStore, error) {
	opts := badger.DefaultOptions(path).WithReadOnly(true)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger read-only: %w", err)
	}
	return &BadgerStore{db: db}, nil
}

// OpenBadgerStoreWithoutMigrations opens an existing Badger database writable
// without running constructor backfills. It is reserved for a state-sync
// database that has already been restored and verified read-only against a
// trusted AppHash. Running NewBadgerStore on those bytes would be unsafe during
// activation because a historical index backfill can change consensus state
// after verification but before CometBFT performs its Info check.
//
// Ordinary boot and upgrade paths must continue to use NewBadgerStore.
func OpenBadgerStoreWithoutMigrations(path string) (*BadgerStore, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect migration-free badger path: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("migration-free badger path must be a real directory")
	}
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger without migrations: %w", err)
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

func agentProofKey(fingerprint []byte, expiresAt int64) []byte {
	key := make([]byte, 0, len(agentProofPrefix)+8+len(fingerprint))
	key = append(key, agentProofPrefix...)
	var expiry [8]byte
	// Flip the sign bit so signed unix timestamps retain numeric order under
	// Badger's unsigned lexicographic key ordering.
	binary.BigEndian.PutUint64(expiry[:], uint64(expiresAt)^(uint64(1)<<63)) // #nosec G115 -- preserve signed bits
	key = append(key, expiry[:]...)
	return append(key, fingerprint...)
}

func agentProofKeyExpiry(key []byte) (int64, error) {
	if len(key) != len(agentProofPrefix)+8+sha256.Size || !bytes.HasPrefix(key, agentProofPrefix) {
		return 0, fmt.Errorf("invalid agent proof marker key")
	}
	sortable := binary.BigEndian.Uint64(key[len(agentProofPrefix) : len(agentProofPrefix)+8])
	return int64(sortable ^ (uint64(1) << 63)), nil // #nosec G115 -- reverse the sign-bit transform
}

// HasAgentProof reports whether a delegated proof fingerprint is still live at
// consensusTime. It is read-only and intended for advisory CheckTx rejection;
// ClaimAgentProof is the authoritative atomic consensus operation.
func (s *BadgerStore) HasAgentProof(fingerprint []byte, consensusTime time.Time, expiresAt int64) (bool, error) {
	if len(fingerprint) != sha256.Size {
		return false, fmt.Errorf("invalid agent proof fingerprint length: %d", len(fingerprint))
	}
	if expiresAt < consensusTime.Unix() {
		return false, nil
	}

	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(agentProofKey(fingerprint, expiresAt))
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// ClaimAgentProof atomically prunes expired delegated-proof markers, rejects a
// live duplicate, and stores this proof through expiresAt. Both times must come
// from the deterministic block/proof data supplied by consensus callers.
func (s *BadgerStore) ClaimAgentProof(fingerprint []byte, consensusTime time.Time, expiresAt int64) error {
	if len(fingerprint) != sha256.Size {
		return fmt.Errorf("invalid agent proof fingerprint length: %d", len(fingerprint))
	}
	if expiresAt < consensusTime.Unix() {
		return fmt.Errorf("agent proof already expired")
	}

	key := agentProofKey(fingerprint, expiresAt)
	return s.update(func(txn *badger.Txn) error {
		// Validate the replay outcome before opportunistic pruning. A duplicate
		// proof is an ordinary invalid transaction and must not mutate (or poison)
		// the surrounding app-v20 block transaction.
		if _, err := txn.Get(key); err == nil {
			return ErrAgentProofReplayed
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		opts := badger.DefaultIteratorOptions
		opts.Prefix = agentProofPrefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		pruned := 0
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			expiry, err := agentProofKeyExpiry(item.Key())
			if err != nil {
				return err
			}
			if expiry >= consensusTime.Unix() {
				break // keys are expiry-sorted; every remaining marker is live
			}
			if s.txn != nil && pruned >= maxScopedAgentProofPrunePerClaim {
				break
			}
			if err := s.txnDelete(txn, item.KeyCopy(nil)); err != nil {
				return err
			}
			pruned++
		}

		return s.txnSet(txn, key, []byte{1})
	})
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
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, memoryKey(memoryID), encodeMemoryHashEntry(contentHash, status))
	})
}

// encodeMemoryHashEntry encodes the value stored under memory:<id>.
func encodeMemoryHashEntry(contentHash []byte, status string) []byte {
	// Encode: contentHash length (4 bytes) + contentHash + status bytes.
	val := make([]byte, 4+len(contentHash)+len(status))
	binary.BigEndian.PutUint32(val[:4], uint32(len(contentHash))) // #nosec G115 -- content hash length fits in uint32
	copy(val[4:4+len(contentHash)], contentHash)
	copy(val[4+len(contentHash):], status)
	return val
}

// GetMemoryHash retrieves a memory's on-chain hash and status.
func (s *BadgerStore) GetMemoryHash(memoryID string) (contentHash []byte, status string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, nonce)
		return s.txnSet(txn, nonceKey(agentID), val)
	})
}

// GetNonce retrieves an agent's current nonce.
func (s *BadgerStore) GetNonce(agentID string) (uint64, error) {
	var nonce uint64
	err := s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, stateKey(key), value)
	})
}

// GetState retrieves a value from the state namespace.
func (s *BadgerStore) GetState(key string) ([]byte, error) {
	var val []byte
	err := s.view(func(txn *badger.Txn) error {
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

// isIndexBackfillProgressKey identifies the two legacy dirty-tree migration
// markers. Current progress lives in local sidecars and startup scrubs these
// keys, but exact exclusion remains defence in depth for verification/recovery
// paths that inspect bytes before an ordinary writable constructor runs.
func isIndexBackfillProgressKey(key []byte) bool {
	return consensuskeys.IsAppHashExcludedLocalKey(key)
}

// ComputeAppHash computes a deterministic SHA-256 hash over all consensus
// state. Exact legacy startup-migration marker keys are skipped defensively.
// CRITICAL: This must be deterministic — sorted key iteration.
func (s *BadgerStore) ComputeAppHash() ([]byte, error) {
	h := sha256.New()

	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// PrefetchValues=false: the loop consumes each value immediately via
		// h.Write inside the .Value() callback, so Badger's prefetch buffer
		// would only hold values we discard at once. Lazy per-value reads cut
		// per-block allocation by ~another order of magnitude (issue #26
		// follow-up from @ihubanov). The hash is identical either way — same
		// bytes, just read lazily (pinned by TestComputeAppHash_ByteIdentical*).
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		// BadgerDB's default forward iterator yields keys in lexicographic
		// byte order — exactly the order the app hash requires (it matches a
		// `string(a) < string(b)` sort). So we stream each key+value straight
		// into the digest in iteration order. This is byte-identical to the
		// previous collect-into-slice-then-sort approach (same input → same
		// hash, so no consensus change), but avoids allocating the entire DB
		// into a Go slice on every FinalizeBlock — that per-block allocation
		// made GC pressure (and thus CPU) grow linearly with chain height.
		// See issue #26. h.Write consumes the key/value synchronously, so the
		// iterator's borrowed slices stay valid for the duration of the call.
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if isIndexBackfillProgressKey(item.Key()) {
				continue
			}
			h.Write(item.Key())
			if valErr := item.Value(func(v []byte) error {
				h.Write(v)
				return nil
			}); valErr != nil {
				return valErr
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("compute app hash: %w", err)
	}

	hash := h.Sum(nil)
	return hash, nil
}

// ComputeAppHashExcludingState is the app-v12 AppHash rule: identical to
// ComputeAppHash except EVERY key in the "state:" namespace is skipped.
//
// KNOWN-FLAWED — SUPERSEDED BY app-v13 (ComputeAppHashExcludingBookkeeping).
// The v12 rule was aimed at the three LAST-block bookkeeping keys SaveState
// rewrites every block (state:height / state:app_hash / state:epoch), whose
// inclusion made the hash unable to reach a fixed point and kept CometBFT's
// needProofBlock minting empty blocks forever on an idle chain (issue #40).
// But the whole-prefix skip ALSO drops real consensus state that lives under
// SetState's "state:" namespace — the governance engine's proposals/votes
// (state:gov:*), memory quorum votes (state:vote:*), consumed-proposal
// markers, and shared-domain sentinels — gutting the AppHash's integrity
// cover for exactly the subsystem that activates forks. app-v13 narrows the
// exclusion to the three bookkeeping keys by exact match. This function is
// retained ONLY so blocks executed on a chain while it was at app-v12 replay
// byte-identically; nothing else may use it.
//
// CRITICAL: deterministic, identical across nodes, CONSENSUS-BREAKING
// relative to ComputeAppHash — callers must fork-gate the choice
// (internal/abci postAppV12Rules/postAppV13Rules).
func (s *BadgerStore) ComputeAppHashExcludingState() ([]byte, error) {
	statePrefix := []byte("state:")
	h := sha256.New()

	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// Same lazy-read iteration as ComputeAppHash (see the rationale there);
		// the only difference is the state:-prefix skip.
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if bytes.HasPrefix(item.Key(), statePrefix) || isIndexBackfillProgressKey(item.Key()) {
				continue
			}
			h.Write(item.Key())
			if valErr := item.Value(func(v []byte) error {
				h.Write(v)
				return nil
			}); valErr != nil {
				return valErr
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("compute app hash (excluding state): %w", err)
	}

	return h.Sum(nil), nil
}

// appHashBookkeepingKeys are the exact keys the app-v13 AppHash rule excludes:
// the LAST-block metadata Commit's SaveState rewrites after every block
// (internal/abci/state.go). Hashing them prevented the hash from ever reaching
// a fixed point (the hash included the previous hash plus the height), which
// kept CometBFT's needProofBlock minting empty blocks forever on an idle chain
// (issue #40). They are bookkeeping for boot-time LoadState, not consensus
// state in their own right: everything they describe is already determined by
// the block being executed. Exact-match exclusion — every OTHER state: key
// (gov:*, vote:*, shared_domain:*, …) stays in the hash, fixing app-v12's
// whole-prefix flaw.
var appHashBookkeepingKeys = [][]byte{
	[]byte("state:height"),
	[]byte("state:app_hash"),
	[]byte("state:epoch"),
}

// ComputeAppHashExcludingBookkeeping is the post-app-v13 AppHash rule:
// identical to ComputeAppHash except the three exact SaveState bookkeeping
// keys (appHashBookkeepingKeys) are skipped. Unlike the superseded app-v12
// rule it keeps the rest of the state: namespace — governance proposals and
// votes, memory quorum votes, consumed-proposal markers, shared-domain
// sentinels — committed to by the hash, while still letting an idle chain's
// hash reach a fixed point (SaveState's three keys are the only state:
// writes on an empty block).
//
// CRITICAL: deterministic, identical across nodes, CONSENSUS-BREAKING
// relative to both ComputeAppHash and ComputeAppHashExcludingState — callers
// must fork-gate the choice (internal/abci postAppV13Rules).
func (s *BadgerStore) ComputeAppHashExcludingBookkeeping() ([]byte, error) {
	h := sha256.New()

	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// Same lazy-read iteration as ComputeAppHash (see the rationale there);
		// the only difference is the exact-key bookkeeping skip.
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

	scan:
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if isIndexBackfillProgressKey(item.Key()) {
				continue
			}
			for _, bk := range appHashBookkeepingKeys {
				if bytes.Equal(item.Key(), bk) {
					continue scan
				}
			}
			h.Write(item.Key())
			if valErr := item.Value(func(v []byte) error {
				h.Write(v)
				return nil
			}); valErr != nil {
				return valErr
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("compute app hash (excluding bookkeeping): %w", err)
	}

	return h.Sum(nil), nil
}

// validatorStatsKey returns the BadgerDB key for a validator's vote stats.
func validatorStatsKey(validatorID string) []byte {
	return []byte("vstats:" + validatorID)
}

// ValidatorStats holds per-validator vote counters stored on-chain.
//
// The first three fields are the v8.2-and-earlier 24-byte record. v8.3
// (app-v4) appends the four PoE-signal fields, growing the record to 56
// bytes the first time a validator's stats are written post-fork:
//   - EWMAWeightedSum / EWMAWeightDenom / EWMACount are the three fields of
//     poe.EWMATracker, accumulating the validator's verdict-correctness
//     (did its vote match the final committed verdict) at quorum-resolution
//     time. EWMACount counts terminal-verdict participations, which is NOT
//     TotalVotes (votes cast, incl. on never-resolved memories).
//   - CorrCount is the lifetime count of verdict matches (corroboration).
type ValidatorStats struct {
	TotalVotes      uint64
	AcceptVotes     uint64
	LastBlockHeight uint64

	// v8.3 fields — zero on legacy 24-byte records (which read as Phase-1
	// values: EWMA cold-start 0.5, corroboration 0).
	EWMAWeightedSum float64
	EWMAWeightDenom float64
	EWMACount       uint64
	CorrCount       uint64
}

const (
	// validatorStatsLenLegacy is the v8.2-and-earlier record: 3 x uint64.
	validatorStatsLenLegacy = 24
	// validatorStatsLenV83 is the v8.3 record: legacy + EWMAWeightedSum,
	// EWMAWeightDenom (IEEE-754 float64), EWMACount, CorrCount (uint64).
	validatorStatsLenV83 = 56
)

// encodeValidatorStats encodes stats. v83=false writes the legacy 24-byte
// layout byte-identical to v8.2.x; v83=true appends the four PoE-signal
// fields for a 56-byte record. The flag is threaded from the abci layer
// (postV8_3Fork) so pre-fork blocks replay byte-identical.
func encodeValidatorStats(s *ValidatorStats, v83 bool) []byte {
	n := validatorStatsLenLegacy
	if v83 {
		n = validatorStatsLenV83
	}
	buf := make([]byte, n)
	binary.BigEndian.PutUint64(buf[0:8], s.TotalVotes)
	binary.BigEndian.PutUint64(buf[8:16], s.AcceptVotes)
	binary.BigEndian.PutUint64(buf[16:24], s.LastBlockHeight)
	if v83 {
		binary.BigEndian.PutUint64(buf[24:32], math.Float64bits(s.EWMAWeightedSum))
		binary.BigEndian.PutUint64(buf[32:40], math.Float64bits(s.EWMAWeightDenom))
		binary.BigEndian.PutUint64(buf[40:48], s.EWMACount)
		binary.BigEndian.PutUint64(buf[48:56], s.CorrCount)
	}
	return buf
}

// decodeValidatorStats decodes either a 24-byte legacy record (the four v8.3
// fields default to zero) or a 56-byte v8.3 record. Length-dispatch keeps old
// chains' records readable post-upgrade and lets mixed-length records coexist
// during the transition epoch.
func decodeValidatorStats(data []byte) (*ValidatorStats, error) {
	if len(data) != validatorStatsLenLegacy && len(data) != validatorStatsLenV83 {
		return nil, fmt.Errorf("invalid validator stats: expected %d or %d bytes, got %d",
			validatorStatsLenLegacy, validatorStatsLenV83, len(data))
	}
	s := &ValidatorStats{
		TotalVotes:      binary.BigEndian.Uint64(data[0:8]),
		AcceptVotes:     binary.BigEndian.Uint64(data[8:16]),
		LastBlockHeight: binary.BigEndian.Uint64(data[16:24]),
	}
	if len(data) == validatorStatsLenV83 {
		s.EWMAWeightedSum = math.Float64frombits(binary.BigEndian.Uint64(data[24:32]))
		s.EWMAWeightDenom = math.Float64frombits(binary.BigEndian.Uint64(data[32:40]))
		s.EWMACount = binary.BigEndian.Uint64(data[40:48])
		s.CorrCount = binary.BigEndian.Uint64(data[48:56])
	}
	return s, nil
}

// IncrementVoteStats increments a validator's vote counters on-chain. v83
// selects the record encoding: pre-fork (false) writes 24 bytes byte-identical
// to v8.2.x; post-fork (true) writes the 56-byte v8.3 record, preserving any
// EWMA/corroboration fields already set by UpdateVerdictStats (read-modify-write
// decodes whatever length is present and re-encodes at the requested length —
// a lazy per-validator migration from 24 → 56 bytes on the first post-fork vote).
func (s *BadgerStore) IncrementVoteStats(validatorID string, accepted bool, blockHeight uint64, v83 bool) error {
	return s.update(func(txn *badger.Txn) error {
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

		return s.txnSet(txn, validatorStatsKey(validatorID), encodeValidatorStats(stats, v83))
	})
}

// UpdateVerdictStats credits per-validator PoE signals when a memory reaches a
// terminal verdict. For each validator in matches, match=true means its vote
// agreed with the final committed verdict. Both signals derive from this one
// event:
//   - Accuracy: feed match (1.0/0.0) into the verdict-correctness EWMA via
//     poe.EWMATracker.Update — the single source of truth for the η-decay
//     recurrence (inlining the constant would risk a silent consensus split).
//   - Corroboration: increment CorrCount on a match.
//
// Always writes the 56-byte v8.3 record — this is only ever called post-fork
// (the abci caller gates on postV8_3Fork). Validator IDs are sorted before
// iterating (belt-and-braces; BadgerDB's commit log is order-independent at the
// key-set level, but sorting keeps the write sequence deterministic regardless).
// The whole batch runs in one db.Update so a mid-batch error leaves no record
// changed (atomicity). LastBlockHeight is intentionally NOT touched here — it
// records vote time (set by IncrementVoteStats), not verdict-resolution time.
func (s *BadgerStore) UpdateVerdictStats(matches map[string]bool) error {
	ids := make([]string, 0, len(matches))
	for id := range matches {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return s.update(func(txn *badger.Txn) error {
		for _, id := range ids {
			stats := &ValidatorStats{}
			item, getErr := txn.Get(validatorStatsKey(id))
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

			tracker := &poe.EWMATracker{
				WeightedSum: stats.EWMAWeightedSum,
				WeightDenom: stats.EWMAWeightDenom,
				Count:       int64(stats.EWMACount), // #nosec G115 -- non-negative count
			}
			outcome := 0.0
			if matches[id] {
				outcome = 1.0
				stats.CorrCount++
			}
			tracker.Update(outcome)
			stats.EWMAWeightedSum = tracker.WeightedSum
			stats.EWMAWeightDenom = tracker.WeightDenom
			stats.EWMACount = uint64(tracker.Count) // #nosec G115 -- Count is monotonic non-negative

			if err := s.txnSet(txn, validatorStatsKey(id), encodeValidatorStats(stats, true)); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetValidatorStats retrieves a validator's on-chain vote stats.
func (s *BadgerStore) GetValidatorStats(validatorID string) (*ValidatorStats, error) {
	var stats *ValidatorStats
	err := s.view(func(txn *badger.Txn) error {
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

	err := s.view(func(txn *badger.Txn) error {
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

// --- Per-domain validator stats (v8.4) ---

// validatorDomainStatsKey returns the BadgerDB key for a validator's
// verdict-correctness stats scoped to a single domain. The validator ID is a
// fixed-width 64-char hex string (Ed25519 pubkey) so the trailing domain — which
// may itself contain ':' — is unambiguous for the point lookups this key serves;
// there is no prefix-scan that needs to split it back apart (the quorum path only
// ever does direct gets).
func validatorDomainStatsKey(validatorID, domain string) []byte {
	return []byte("vstats_domain:" + validatorID + ":" + domain)
}

// memoryDomainKey returns the BadgerDB key recording a memory's domain tag.
// Written at submit time post-v8.4 so checkAndApplyQuorum can resolve the
// memory's domain deterministically — the memory:<id> record stores only
// contentHash+status, not the domain.
func memoryDomainKey(memoryID string) []byte {
	return []byte("memdomain:" + memoryID)
}

// SetMemoryDomain records a memory's domain tag on-chain. The caller gates the
// write on post-v8.4 or post-app-v16 rules so older blocks never gain this key
// during replay.
func (s *BadgerStore) SetMemoryDomain(memoryID, domain string) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, memoryDomainKey(memoryID), []byte(domain))
	})
}

// GetMemoryDomain returns a memory's recorded domain tag, or "" if no
// memdomain: key exists (legacy/pre-fork memory, or a memory submitted with an
// empty domain). A missing key is not an error — the quorum treats "" as
// "unknown domain" and falls back to the v8.2 scalar weight.
func (s *BadgerStore) GetMemoryDomain(memoryID string) (string, error) {
	var domain string
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(memoryDomainKey(memoryID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			domain = string(val)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return domain, nil
}

func memoryAuthorKey(memoryID string) []byte {
	return []byte("memauthor:" + memoryID)
}

// --- CoCommit (v11 / app-v15) key helpers + setters/getters ---
//
// All cocommit:* keys are keyed by the content-derived, height-free SharedID.
// None collide with the existing prefixes (corrob: differs at char 2), and
// because ComputeAppHash* hash every non-bookkeeping key, these values enter the
// AppHash automatically with NO exclusion-rule change — provided every value is a
// PURE function of tx bytes (which they are: sorted coauthors, verbatim hashes,
// BE-fixed-width ints — never time.Now). Callers gate every write on
// postAppV15Fork so pre-fork blocks never write them (byte-identical replay).

func cocommitCoreKey(sharedID string) []byte      { return []byte("cocommit:core:" + sharedID) }
func cocommitCoauthorsKey(sharedID string) []byte { return []byte("cocommit:coauthors:" + sharedID) }
func cocommitSharedKey(sharedID string) []byte    { return []byte("cocommit:shared:" + sharedID) }

func cocommitAnchorKey(sharedID, peerChainID string) []byte {
	// sharedID is hex and peerChainID is a chain_id (charset [a-z2-7-]); neither
	// contains ':', so the composite key is unambiguous.
	return []byte("cocommit:anchor:" + sharedID + ":" + peerChainID)
}

// SetCoCommitShared marks a SharedID as a co-committed memory and records its
// schema version (4 BE bytes).
func (s *BadgerStore) SetCoCommitShared(sharedID string, schemaVersion uint32) error {
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v, schemaVersion)
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, cocommitSharedKey(sharedID), v)
	})
}

// SetCoCommitCore records the shared CoreHash for a co-committed memory. The
// attest path binds a peer receipt's CoreHash against this value (fail-closed).
func (s *BadgerStore) SetCoCommitCore(sharedID string, coreHash []byte) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, cocommitCoreKey(sharedID), coreHash)
	})
}

// GetCoCommitCore returns the recorded CoreHash for a SharedID, or nil if no
// cocommit:core: key exists (a missing key is not an error — an attest for an
// unknown SharedID fails the fail-closed bind).
func (s *BadgerStore) GetCoCommitCore(sharedID string) ([]byte, error) {
	var core []byte
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(cocommitCoreKey(sharedID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			core = append([]byte(nil), val...)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return core, nil
}

// SetCoCommitCoauthors stores the deterministic (sorted) coauthor blob for a
// SharedID, produced by tx.EncodeCoauthorsCanonical (a pure function of tx bytes).
func (s *BadgerStore) SetCoCommitCoauthors(sharedID string, blob []byte) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, cocommitCoauthorsKey(sharedID), blob)
	})
}

// GetCoCommitCoauthors returns the raw coauthor blob for a SharedID, or nil if
// none (a missing key is not an error). Decode with tx.DecodeCoauthorsCanonical.
func (s *BadgerStore) GetCoCommitCoauthors(sharedID string) ([]byte, error) {
	var blob []byte
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(cocommitCoauthorsKey(sharedID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			blob = append([]byte(nil), val...)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return blob, nil
}

// SetCoCommitAnchor records a peer's cross-attestation anchor
// (sha256(canonical(receipt))) for a SharedID. Idempotent (re-attest overwrites
// identical bytes) and late-bindable (a missing anchor = "unconfirmed").
func (s *BadgerStore) SetCoCommitAnchor(sharedID, peerChainID string, anchorHash []byte) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, cocommitAnchorKey(sharedID, peerChainID), anchorHash)
	})
}

// GetCoCommitAnchor returns the recorded cross-anchor for (sharedID, peer), or
// nil if the peer has not been anchored yet (missing = "unconfirmed", not an
// error). Read-only — used by the transport layer for idempotent receipt
// delivery and confirmation status.
func (s *BadgerStore) GetCoCommitAnchor(sharedID, peerChainID string) ([]byte, error) {
	var anchor []byte
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(cocommitAnchorKey(sharedID, peerChainID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			anchor = append([]byte(nil), val...)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return anchor, nil
}

// ListCoCommitAnchors returns every recorded peer anchor for a SharedID, keyed
// by peer chain id. Read-only prefix scan (cocommit:anchor:<sharedID>:) — the
// key charset guarantees ':' appears only as the separator.
func (s *BadgerStore) ListCoCommitAnchors(sharedID string) (map[string][]byte, error) {
	anchors := make(map[string][]byte)
	prefix := []byte("cocommit:anchor:" + sharedID + ":")
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			peerChainID := string(item.Key()[len(prefix):])
			if err := item.Value(func(val []byte) error {
				anchors[peerChainID] = append([]byte(nil), val...)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return anchors, err
}

// --- CrossFed (v11 / app-v15) Mode-1 exchange TERMS ---
//
// Consensus-authoritative terms record keyed by remote_chain_id, one per remote
// chain (upsert). Hand-rolled length-prefixed binary local to the store (no
// internal/tx import — same cycle-avoidance reason the federation: blob is
// hand-rolled). A dedicated top-level "cross_fed:" prefix (not state:-prefixed)
// so it is hashed under every AppHash rule. PURE function of the inputs
// (BE-fixed-width ints, verbatim PeerPubKey, ExpiresAt as DATA — no time.Now) so
// it hashes deterministically into the AppHash, exactly like cocommit:*.

func crossFedKey(remoteChainID string) []byte { return []byte("cross_fed:" + remoteChainID) }

func encodeCrossFedBlob(endpoint string, peerPubKey []byte, maxClearance uint8, expiresAt int64, status string, allowedDomains, allowedDepts []string) []byte {
	size := 1 // version
	size += 4 + len(endpoint)
	size += 4 + len(peerPubKey)
	size += 1 // maxClearance
	size += 8 // expiresAt
	size += 4 + len(status)
	size += 4
	for _, d := range allowedDomains {
		size += 4 + len(d)
	}
	size += 4
	for _, d := range allowedDepts {
		size += 4 + len(d)
	}
	val := make([]byte, size)
	val[0] = 1 // version
	offset := 1
	offset = encodeString(val, offset, endpoint)
	offset = encodeString(val, offset, string(peerPubKey))
	val[offset] = maxClearance
	offset++
	binary.BigEndian.PutUint64(val[offset:offset+8], uint64(expiresAt)) // #nosec G115 -- expiry non-negative
	offset += 8
	offset = encodeString(val, offset, status)
	binary.BigEndian.PutUint32(val[offset:offset+4], uint32(len(allowedDomains))) // #nosec G115
	offset += 4
	for _, d := range allowedDomains {
		offset = encodeString(val, offset, d)
	}
	binary.BigEndian.PutUint32(val[offset:offset+4], uint32(len(allowedDepts))) // #nosec G115
	offset += 4
	for _, d := range allowedDepts {
		offset = encodeString(val, offset, d)
	}
	return val
}

func decodeCrossFedBlob(val []byte) (endpoint string, peerPubKey []byte, maxClearance uint8, expiresAt int64, status string, allowedDomains, allowedDepts []string, err error) {
	if len(val) < 1 {
		return "", nil, 0, 0, "", nil, nil, fmt.Errorf("invalid cross_fed entry: empty")
	}
	offset := 1 // skip version byte
	endpoint, offset, err = decodeString(val, offset)
	if err != nil {
		return
	}
	var pk string
	pk, offset, err = decodeString(val, offset)
	if err != nil {
		return
	}
	peerPubKey = []byte(pk)
	if offset >= len(val) {
		return "", nil, 0, 0, "", nil, nil, fmt.Errorf("invalid cross_fed entry: missing clearance")
	}
	maxClearance = val[offset]
	offset++
	if offset+8 > len(val) {
		return "", nil, 0, 0, "", nil, nil, fmt.Errorf("invalid cross_fed entry: missing expiresAt")
	}
	expiresAt = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115
	offset += 8
	status, offset, err = decodeString(val, offset)
	if err != nil {
		return
	}
	// allowedDomains, allowedDepts — grow via append (no pre-size); decodeString
	// fails fast if the count outruns the buffer, so no unbounded allocation.
	allowedDomains, offset, err = decodeStringSliceBlob(val, offset)
	if err != nil {
		return
	}
	allowedDepts, _, err = decodeStringSliceBlob(val, offset)
	return
}

// decodeStringSliceBlob reads a uint32 count then that many length-prefixed
// strings, growing via append (never pre-sizing from the count) so a corrupt
// count cannot pre-allocate.
func decodeStringSliceBlob(val []byte, offset int) ([]string, int, error) {
	if offset+4 > len(val) {
		return nil, 0, fmt.Errorf("invalid cross_fed entry: missing slice count")
	}
	count := binary.BigEndian.Uint32(val[offset : offset+4])
	offset += 4
	out := make([]string, 0)
	for i := uint32(0); i < count; i++ {
		var s string
		var err error
		s, offset, err = decodeString(val, offset)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, s)
	}
	return out, offset, nil
}

// SetCrossFed upserts a cross-network exchange terms record for remoteChainID.
// No height/clock input — the value is a pure function of the arguments.
func (s *BadgerStore) SetCrossFed(remoteChainID, endpoint string, peerPubKey []byte, maxClearance uint8, expiresAt int64, allowedDomains, allowedDepts []string, status string) error {
	blob := encodeCrossFedBlob(endpoint, peerPubKey, maxClearance, expiresAt, status, allowedDomains, allowedDepts)
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, crossFedKey(remoteChainID), blob)
	})
}

// GetCrossFed returns the terms record for remoteChainID (surfacing the transport
// coordinates endpoint/peerPubKey, unlike GetFederation). Returns an error if the
// agreement does not exist.
func (s *BadgerStore) GetCrossFed(remoteChainID string) (endpoint string, peerPubKey []byte, maxClearance uint8, expiresAt int64, allowedDomains, allowedDepts []string, status string, err error) {
	err = s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(crossFedKey(remoteChainID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			var decErr error
			endpoint, peerPubKey, maxClearance, expiresAt, status, allowedDomains, allowedDepts, decErr = decodeCrossFedBlob(val)
			return decErr
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", nil, 0, 0, nil, nil, "", fmt.Errorf("cross_fed not found: %s", remoteChainID)
	}
	return
}

// UpdateCrossFedStatus round-trips ALL fields and rewrites only the status
// (mirrors UpdateFederationStatus; truncating the extended fields would drop the
// transport coordinates).
func (s *BadgerStore) UpdateCrossFedStatus(remoteChainID, newStatus string) error {
	return s.update(func(txn *badger.Txn) error {
		item, err := txn.Get(crossFedKey(remoteChainID))
		if err != nil {
			return err
		}
		var endpoint, status string
		var peerPubKey []byte
		var maxClearance uint8
		var expiresAt int64
		var allowedDomains, allowedDepts []string
		if err := item.Value(func(val []byte) error {
			var decErr error
			endpoint, peerPubKey, maxClearance, expiresAt, status, allowedDomains, allowedDepts, decErr = decodeCrossFedBlob(val)
			return decErr
		}); err != nil {
			return err
		}
		_ = status // replaced below
		blob := encodeCrossFedBlob(endpoint, peerPubKey, maxClearance, expiresAt, newStatus, allowedDomains, allowedDepts)
		return s.txnSet(txn, crossFedKey(remoteChainID), blob)
	})
}

// CrossFedRecord is the decoded form of one cross_fed:<remote_chain_id> terms
// record, used by the OFF-consensus transport layer (federation listener, query
// proxy, receipt exchange) which needs to enumerate agreements — the consensus
// path only ever does point lookups via GetCrossFed.
type CrossFedRecord struct {
	RemoteChainID  string
	Endpoint       string
	PeerPubKey     []byte
	MaxClearance   uint8
	ExpiresAt      int64
	AllowedDomains []string
	AllowedDepts   []string
	Status         string
}

// ListCrossFed returns every cross_fed terms record. Read-only prefix scan —
// never called on the consensus path.
func (s *BadgerStore) ListCrossFed() ([]CrossFedRecord, error) {
	var records []CrossFedRecord
	prefix := []byte("cross_fed:")
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			rec := CrossFedRecord{RemoteChainID: string(item.Key()[len(prefix):])}
			if err := item.Value(func(val []byte) error {
				var decErr error
				rec.Endpoint, rec.PeerPubKey, rec.MaxClearance, rec.ExpiresAt, rec.Status,
					rec.AllowedDomains, rec.AllowedDepts, decErr = decodeCrossFedBlob(val)
				return decErr
			}); err != nil {
				continue // skip a corrupt record rather than failing the scan
			}
			records = append(records, rec)
		}
		return nil
	})
	return records, err
}

// SetMemoryAuthor records a memory's submitting agent (its author) on-chain.
// Caller gates on postAppV10Fork so pre-fork blocks never write this key
// (byte-identical replay). This is the consensus-authoritative author field, and
// the source the app-v10 corroboration guard checks to reject self-corroboration.
func (s *BadgerStore) SetMemoryAuthor(memoryID, agentID string) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, memoryAuthorKey(memoryID), []byte(agentID))
	})
}

// GetMemoryAuthor returns a memory's recorded author (submitting agent), or ""
// if no memauthor: key exists (a memory submitted before app-v10 activated). A
// missing key is not an error — the corroboration guard treats "" as "author
// unknown on-chain" and does NOT reject (the forward-looking boundary).
func (s *BadgerStore) GetMemoryAuthor(memoryID string) (string, error) {
	var author string
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(memoryAuthorKey(memoryID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			author = string(val)
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return author, nil
}

// corroborationKey is "corrob:<memoryID>:<agentID>". agentID is fixed-length hex
// (auth.PublicKeyToAgentID), so the trailing segment is unambiguous even if a
// memoryID contains ':' — distinct (memoryID, agentID) pairs never collide.
func corroborationKey(memoryID, agentID string) []byte {
	return []byte("corrob:" + memoryID + ":" + agentID)
}

// SetCorroborated marks on-chain that agentID has corroborated memoryID. Caller
// gates on postAppV10Fork so pre-fork blocks never write this key.
func (s *BadgerStore) SetCorroborated(memoryID, agentID string) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, corroborationKey(memoryID, agentID), []byte{1})
	})
}

// HasCorroborated reports whether agentID has already corroborated memoryID (an
// on-chain corrob: marker exists). The app-v10 dedup guard uses it so one agent
// cannot inflate a memory's corroboration count by corroborating it twice.
func (s *BadgerStore) HasCorroborated(memoryID, agentID string) (bool, error) {
	found := false
	err := s.view(func(txn *badger.Txn) error {
		_, getErr := txn.Get(corroborationKey(memoryID, agentID))
		if getErr == badger.ErrKeyNotFound {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		found = true
		return nil
	})
	return found, err
}

// GetValidatorDomainStats retrieves a validator's verdict-correctness stats for
// one domain. Returns a zero-valued record (which reads as the EWMA cold-start
// prior 0.5) when the validator has no history in that domain — so a generalist
// who never voted on the domain starts neutral and re-accrues. Reuses the
// v8.3 24/56-byte codec; the key prefix is the only difference from vstats:.
func (s *BadgerStore) GetValidatorDomainStats(validatorID, domain string) (*ValidatorStats, error) {
	var stats *ValidatorStats
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(validatorDomainStatsKey(validatorID, domain))
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

// UpdateDomainVerdictStats is the per-domain sibling of UpdateVerdictStats. It
// credits the same verdict-correctness EWMA + corroboration signals, but scoped
// to the memory's domain D, into vstats_domain:<v>:<D>. Same atomicity (one
// db.Update, sorted iteration) and same "always write the 56-byte record"
// discipline — this is only ever called post-v8.4-fork for a non-shared domain.
// The global vstats: record is credited separately by UpdateVerdictStats; the
// two are independent accumulators fed from the one terminal-verdict event.
func (s *BadgerStore) UpdateDomainVerdictStats(domain string, matches map[string]bool) error {
	ids := make([]string, 0, len(matches))
	for id := range matches {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return s.update(func(txn *badger.Txn) error {
		for _, id := range ids {
			key := validatorDomainStatsKey(id, domain)
			stats := &ValidatorStats{}
			item, getErr := txn.Get(key)
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

			tracker := &poe.EWMATracker{
				WeightedSum: stats.EWMAWeightedSum,
				WeightDenom: stats.EWMAWeightDenom,
				Count:       int64(stats.EWMACount), // #nosec G115 -- non-negative count
			}
			outcome := 0.0
			if matches[id] {
				outcome = 1.0
				stats.CorrCount++
			}
			tracker.Update(outcome)
			stats.EWMAWeightedSum = tracker.WeightedSum
			stats.EWMAWeightDenom = tracker.WeightDenom
			stats.EWMACount = uint64(tracker.Count) // #nosec G115 -- Count is monotonic non-negative

			if err := s.txnSet(txn, key, encodeValidatorStats(stats, true)); err != nil {
				return err
			}
		}
		return nil
	})
}

// --- PoE epoch weights (v8.2) ---

// poeWeightsPrefix is the BadgerDB key prefix for per-validator PoE epoch
// weights (`poew:<validatorID>`). The literal "poew:current" key under the
// same prefix is reserved as the epoch-number marker — readers MUST skip it
// when iterating the prefix as if it were a validator entry.
const poeWeightsPrefix = "poew:"

// poeWeightsCurrentKey holds the uvarint-encoded epoch number that
// SetEpochWeights last persisted. Its presence is the "an epoch has run"
// sentinel; cold boot uses it to distinguish "no epoch has run yet" from
// "epoch has run, validator absent" — see docs/v8.2-PLAN.md.
var poeWeightsCurrentKey = []byte("poew:current")

// poeWeightKey returns the BadgerDB key for a validator's PoE epoch weight.
func poeWeightKey(validatorID string) []byte {
	return []byte(poeWeightsPrefix + validatorID)
}

// SetEpochWeights atomically persists the normalized PoE weight set for an
// epoch. Writes `poew:current` (uvarint epoch number) and one
// `poew:<validatorID>` (IEEE-754 float64, big-endian, 8 bytes) per entry in
// `weights`. Pre-existing `poew:<id>` keys whose validator ID is not in
// `weights` are deleted in the same transaction so a validator removed via
// governance leaves no stale weight behind for the boot loader to apply
// (test W3).
//
// Validator IDs are written in sorted order — belt-and-braces even though
// BadgerDB's commit log doesn't depend on per-key write order. The empty
// string is rejected as a validator ID before opening the txn so the failure
// mode is "either all keys land, or none" (test W5 — atomicity).
//
// The on-chain encoding (uvarint epoch, big-endian float64 weight) is
// consensus-critical and pinned by test W4. Do NOT change it without a fork
// gate.
func (s *BadgerStore) SetEpochWeights(epoch uint64, weights map[string]float64) error {
	// Validate up front so a malformed call cannot leave a half-written
	// epoch. We open the txn only after every input has been checked.
	ids := make([]string, 0, len(weights))
	for id := range weights {
		if id == "" {
			return fmt.Errorf("set epoch weights: empty validator id")
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	return s.update(func(txn *badger.Txn) error {
		// 1. Collect existing poew:<id> keys so we can drop any that aren't
		//    in the new weight set (stale-validator pruning, test W3).
		stale := make(map[string]struct{})
		prefix := []byte(poeWeightsPrefix)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			// Skip the epoch marker — it is not a validator entry.
			if string(key) == string(poeWeightsCurrentKey) {
				continue
			}
			id := string(key[len(poeWeightsPrefix):])
			stale[id] = struct{}{}
		}
		it.Close()

		// 2. Write poew:current — uvarint epoch number.
		epochBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(epochBuf, epoch)
		if err := s.txnSet(txn, append([]byte(nil), poeWeightsCurrentKey...), epochBuf[:n]); err != nil {
			return err
		}

		// 3. Write poew:<id> for every validator in sorted order. Removing
		//    each written id from `stale` so what remains is exactly the
		//    pruning set.
		for _, id := range ids {
			buf := make([]byte, 8)
			binary.BigEndian.PutUint64(buf, math.Float64bits(weights[id]))
			if err := s.txnSet(txn, poeWeightKey(id), buf); err != nil {
				return err
			}
			delete(stale, id)
		}

		// 4. Delete any validator entries that survived from a prior epoch
		//    but are absent from the new set.
		for id := range stale {
			if err := s.txnDelete(txn, poeWeightKey(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetEpochWeights loads the most recently persisted PoE weight set.
// Returns (nil, false, nil) on a fresh store where SetEpochWeights has never
// been called (no `poew:current` marker exists). When the marker is present,
// iterates every `poew:<id>` key (skipping the marker itself) and decodes the
// 8-byte big-endian IEEE-754 float64 into the returned map.
//
// The epoch number is intentionally NOT returned here — boot-time hydration
// only needs the weight map. Tests and operators that want the epoch number
// use GetEpochNumber.
func (s *BadgerStore) GetEpochWeights() (weights map[string]float64, ok bool, err error) {
	err = s.view(func(txn *badger.Txn) error {
		// Marker check — if poew:current is absent, no epoch has run.
		if _, getErr := txn.Get(poeWeightsCurrentKey); getErr != nil {
			if errors.Is(getErr, badger.ErrKeyNotFound) {
				return nil
			}
			return getErr
		}

		weights = make(map[string]float64)
		prefix := []byte(poeWeightsPrefix)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			// Skip the epoch marker — it is not a validator entry.
			if string(key) == string(poeWeightsCurrentKey) {
				continue
			}
			id := string(key[len(poeWeightsPrefix):])
			valErr := item.Value(func(val []byte) error {
				if len(val) != 8 {
					return fmt.Errorf("invalid poe weight for %s: expected 8 bytes, got %d", id, len(val))
				}
				weights[id] = math.Float64frombits(binary.BigEndian.Uint64(val))
				return nil
			})
			if valErr != nil {
				return valErr
			}
		}
		ok = true
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("get epoch weights: %w", err)
	}
	return weights, ok, nil
}

// GetEpochNumber returns the epoch number that the most recent
// SetEpochWeights call persisted. Returns (0, false, nil) on a fresh store
// where the `poew:current` marker has never been written. Exposed primarily
// for tests and operator tooling (`badger get poew:current` equivalent);
// boot-time hydration only needs the weight map and uses GetEpochWeights.
func (s *BadgerStore) GetEpochNumber() (epoch uint64, ok bool, err error) {
	err = s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(poeWeightsCurrentKey)
		if getErr != nil {
			if errors.Is(getErr, badger.ErrKeyNotFound) {
				return nil
			}
			return getErr
		}
		return item.Value(func(val []byte) error {
			decoded, n := binary.Uvarint(val)
			if n <= 0 {
				return fmt.Errorf("invalid poew:current uvarint payload (%d bytes)", len(val))
			}
			epoch = decoded
			ok = true
			return nil
		})
	})
	if err != nil {
		return 0, false, fmt.Errorf("get epoch number: %w", err)
	}
	return epoch, ok, nil
}

// SaveValidators persists the validator set to BadgerDB.
func (s *BadgerStore) SaveValidators(validators map[string]int64) error {
	return s.update(func(txn *badger.Txn) error {
		for id, power := range validators {
			key := []byte("validator:" + id)
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(power)) // #nosec G115 -- validator power is always non-negative
			if err := s.txnSet(txn, key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

// ReplaceValidators atomically replaces the ENTIRE validator:* keyspace with the
// given set: it deletes every existing validator:<id> key, then writes the supplied
// map. Unlike SaveValidators (which only upserts), this removes validators that are
// no longer in the set, so stale entries cannot resurrect as phantom non-voting
// validators on restart (which would otherwise inflate totalPower and block the 2/3
// quorum). Used by the single-node legacy-reconcile path; never call it on a
// multi-validator chain (a local validator-set write forks the AppHash off-consensus).
func (s *BadgerStore) ReplaceValidators(validators map[string]int64) error {
	prefix := []byte("validator:")
	// Single atomic Update txn: collect existing validator:* keys, delete them, then
	// write the new set — so the delete-old + write-new is all-or-nothing and cannot
	// interleave with another writer.
	return s.update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		var existing [][]byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			existing = append(existing, append([]byte{}, it.Item().Key()...))
		}
		it.Close() // must close before mutating within the same txn

		for _, k := range existing {
			if err := s.txnDelete(txn, k); err != nil {
				return err
			}
		}
		for id, power := range validators {
			val := make([]byte, 8)
			binary.BigEndian.PutUint64(val, uint64(power)) // #nosec G115 -- validator power is always non-negative
			if err := s.txnSet(txn, []byte("validator:"+id), val); err != nil {
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

	err := s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 1+8+4+len(granterID))
		val[0] = level
		binary.BigEndian.PutUint64(val[1:9], uint64(expiresAt)) // #nosec G115 -- expiry timestamp is always non-negative
		encodeString(val, 9, granterID)
		return s.txnSet(txn, grantKey(domain, agentID), val)
	})
}

// GetAccessGrant retrieves an access grant from BadgerDB.
func (s *BadgerStore) GetAccessGrant(domain, agentID string) (level uint8, expiresAt int64, granterID string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, 0, "", fmt.Errorf("%w: %s/%s", ErrAccessGrantNotFound, domain, agentID)
	}
	return
}

// DeleteAccessGrant removes an access grant from BadgerDB.
func (s *BadgerStore) DeleteAccessGrant(domain, agentID string) error {
	return s.update(func(txn *badger.Txn) error {
		return s.txnDelete(txn, grantKey(domain, agentID))
	})
}

// DeleteGrantsByDomain removes every access grant on a single domain and
// returns the number of grants deleted. Used by the v8.0
// TxTypeDomainReassign handler to invalidate any inherited access whenever
// ownership is transferred — the previous owner's chain-of-trust should not
// survive the reassignment.
//
// Two-pass within a single read-then-write transaction pair: the first pass
// collects keys under a View() iterator (PrefetchValues=false, prefix-bound),
// the second deletes them in a single Update(). Iteration uses BadgerDB's
// lexicographic ordering on the "grant:<domain>:" prefix, which is the same
// layout grantKey writes — see grantKey at the top of the federation
// access-control block.
func (s *BadgerStore) DeleteGrantsByDomain(domain string) (int, error) {
	var keys [][]byte
	prefix := []byte("grant:" + domain + ":")
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			keys = append(keys, append([]byte{}, it.Item().Key()...))
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	err = s.update(func(txn *badger.Txn) error {
		for _, k := range keys {
			if dErr := s.txnDelete(txn, k); dErr != nil {
				return dErr
			}
		}
		return nil
	})
	return len(keys), err
}

// CountGrantsByDomainUpTo counts a deterministic prefix up to limit+1. The
// boolean reports that the real count exceeded the supplied safety limit
// without allocating or scanning the rest of an adversarial grant set.
func (s *BadgerStore) CountGrantsByDomainUpTo(domain string, limit int) (count int, exceeded bool, err error) {
	if limit < 0 {
		return 0, false, errors.New("grant count limit must be non-negative")
	}
	prefix := []byte("grant:" + domain + ":")
	err = s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
			if count > limit {
				exceeded = true
				break
			}
		}
		return nil
	})
	return count, exceeded, err
}

// SetSharedDomain marks a domain as shared by writing the on-chain
// shared_domain:<name> sentinel key. Used by the v8.0 TxTypeDomainReassign
// handler when OpenToShared=true, so subsequent grant/submit code paths
// (via SageApp.isSharedDomain) see the domain as shared post-fork.
//
// Thin wrapper around SetState — kept as a named method to keep the call
// site at the ABCI layer readable and to centralize the key naming.
func (s *BadgerStore) SetSharedDomain(name string) error {
	return s.SetState("shared_domain:"+name, []byte{1})
}

// HasAccess checks if an agent has the required access level on a domain.
// Uses blockTime for deterministic expiry checks (not time.Now()).
func (s *BadgerStore) HasAccess(domain, agentID string, requiredLevel uint8, blockTime time.Time) (bool, error) {
	var has bool
	err := s.view(func(txn *badger.Txn) error {
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

// HasAccessOrAncestor walks the dotted domain path from the most specific
// segment toward the root, returning true when the first valid grant on the
// agent at the required level is found. Mirrors HasAccess semantics on each
// candidate (deterministic blockTime-based expiry, level satisfies the bar)
// and short-circuits on the first hit. Caps the walk at 16 segments so a
// pathological deep path cannot turn into an unbounded read amplifier.
//
// Shared-domain barrier: candidates whose name is reserved as a shared domain
// (see IsSharedDomainName) are skipped — a grant on "general" must never
// silently cascade to "pipeline.general". Shared domains are catch-alls, not
// inheritable ancestors.
//
// Empty domain or agentID returns (false, nil). Per-segment lookup failures
// other than "key not found" are surfaced via the error return.
func (s *BadgerStore) HasAccessOrAncestor(domain, agentID string, requiredLevel uint8, blockTime time.Time) (bool, error) {
	if domain == "" || agentID == "" {
		return false, nil
	}
	// Filter empty segments so leading/trailing/double-dots ("..a.b", "a..b",
	// "a.b.") don't bury the walk in candidates like "..a" or ".a" that can
	// never match a grant. The cap is applied to the FILTERED count so the
	// pathological-path guard is also robust to dot padding.
	segments := splitDomainSegments(domain)
	if len(segments) == 0 {
		return false, nil
	}
	if len(segments) > 16 {
		// Walk-depth cap: refuse to follow pathological paths. Returning
		// false (rather than an error) keeps the caller's error semantics
		// indistinguishable from "no grant found", which is the safe
		// outcome for an access check.
		return false, nil
	}

	now := blockTime.Unix()
	var walkErr error
	err := s.view(func(txn *badger.Txn) error {
		for i := len(segments); i >= 1; i-- {
			candidate := strings.Join(segments[:i], ".")
			if candidate == "" {
				// Defensive: should not happen post-filter, but guards
				// against future regressions in splitDomainSegments.
				continue
			}
			if IsSharedDomainName(candidate) {
				// Cascade barrier — shared domains do not act as ancestors.
				continue
			}
			item, getErr := txn.Get(grantKey(candidate, agentID))
			if errors.Is(getErr, badger.ErrKeyNotFound) {
				continue
			}
			if getErr != nil {
				walkErr = getErr
				return getErr
			}
			var matched bool
			vErr := item.Value(func(val []byte) error {
				if len(val) < 9 {
					return fmt.Errorf("invalid grant entry")
				}
				level := val[0]
				expiresAt := int64(binary.BigEndian.Uint64(val[1:9])) // #nosec G115 -- expiry timestamp fits in int64
				if level < requiredLevel {
					return nil
				}
				if expiresAt > 0 && now >= expiresAt {
					return nil
				}
				matched = true
				return nil
			})
			if vErr != nil {
				walkErr = vErr
				return vErr
			}
			if matched {
				// First valid match wins — most specific grant takes effect.
				walkErr = errStopWalk
				return errStopWalk
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopWalk) {
		return false, err
	}
	if errors.Is(walkErr, errStopWalk) {
		return true, nil
	}
	return false, nil
}

// errStopWalk is a sentinel used by HasAccessOrAncestor to short-circuit the
// Badger txn body once a valid grant is found. Not exported.
var errStopWalk = errors.New("stop walk")

// splitDomainSegments splits a dotted-domain path and drops empty segments
// produced by leading, trailing, or doubled dots. Centralised so
// HasAccessOrAncestor and ResolveOwningAncestor stay in lock-step on how
// they normalise input.
func splitDomainSegments(domain string) []string {
	raw := strings.Split(domain, ".")
	out := raw[:0]
	for _, s := range raw {
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

// ResolveOwningAncestor walks the dotted domain path from the most specific
// segment toward the root and returns the nearest ancestor (or the leaf
// itself) that has a registered owner. Used by multi-org access checks to
// find the "effective owner" of a domain that may not be directly registered
// but inherits ownership from a registered parent.
//
// Returns ("", "", nil) when no ancestor is owned — this is the "domain
// doesn't exist" case and callers should treat it the same as a missing
// direct registration.
//
// Shared-domain barrier mirrors HasAccessOrAncestor: candidates that are
// reserved shared domains are skipped during the walk. Walk depth is capped
// at 16 segments to defang pathological paths.
func (s *BadgerStore) ResolveOwningAncestor(domain string) (owner, ownedDomain string, err error) {
	if domain == "" {
		return "", "", nil
	}
	segments := splitDomainSegments(domain)
	if len(segments) == 0 {
		return "", "", nil
	}
	if len(segments) > 16 {
		return "", "", ErrDomainPathTooDeep
	}
	for i := len(segments); i >= 1; i-- {
		candidate := strings.Join(segments[:i], ".")
		if candidate == "" {
			continue
		}
		if IsSharedDomainName(candidate) {
			continue
		}
		o, gerr := s.GetDomainOwner(candidate)
		if gerr != nil {
			// GetDomainOwner returns a wrapped "domain not found" — treat
			// any read failure as "no record at this level" and keep walking.
			continue
		}
		if o == "" {
			continue
		}
		return o, candidate, nil
	}
	return "", "", nil
}

// RegisterDomain registers a domain in BadgerDB.
// Encoding: ownerID (length-prefixed) + parentDomain (length-prefixed) + height (8 bytes).
// RegisterDomain atomically registers a domain with the given owner.
// Returns ErrDomainAlreadyRegistered if the domain is already registered with a non-empty owner.
// This is intentionally check-and-set to prevent ownership "capture" when a prior registration
// record is present but unexpectedly read as empty during the submit path.
func (s *BadgerStore) RegisterDomain(name, ownerID, parentDomain string, height int64) error {
	return s.update(func(txn *badger.Txn) error {
		if item, getErr := txn.Get(domainKey(name)); getErr == nil {
			var existingOwner string
			if err := item.Value(func(val []byte) error {
				owner, _, decErr := decodeString(val, 0)
				existingOwner = owner
				return decErr
			}); err == nil && existingOwner != "" {
				return ErrDomainAlreadyRegistered
			}
		} else if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}
		val := make([]byte, 4+len(ownerID)+4+len(parentDomain)+8)
		offset := encodeString(val, 0, ownerID)
		offset = encodeString(val, offset, parentDomain)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		return s.txnSet(txn, domainKey(name), val)
	})
}

// TransferDomain forcibly reassigns domain ownership. Callers are responsible for
// authorization (e.g. current owner consent or admin role). Do NOT call from
// transaction processing paths that should use RegisterDomain's check-and-set semantics.
func (s *BadgerStore) TransferDomain(name, newOwnerID, parentDomain string, height int64) error {
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(newOwnerID)+4+len(parentDomain)+8)
		offset := encodeString(val, 0, newOwnerID)
		offset = encodeString(val, offset, parentDomain)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		return s.txnSet(txn, domainKey(name), val)
	})
}

// GetDomainOwner retrieves the owner of a domain.
func (s *BadgerStore) GetDomainOwner(name string) (string, error) {
	var ownerID string
	err := s.view(func(txn *badger.Txn) error {
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

// GetDomainOwnerAndMeta returns owner, parent, and registered block height
// for a domain in a single read. Use this in any read path that informs a
// later grant/revoke/register tx — those handlers validate against Badger,
// so off-chain mirrors that disagree (e.g. chain reset without dropping
// the accessStore tables) will mislead callers into Code-34 rejections.
func (s *BadgerStore) GetDomainOwnerAndMeta(name string) (ownerID, parent string, height int64, err error) {
	err = s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(domainKey(name))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			owner, off, decErr := decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			p, off, decErr := decodeString(val, off)
			if decErr != nil {
				return decErr
			}
			if len(val) < off+8 {
				return fmt.Errorf("invalid domain entry: short height")
			}
			ownerID = owner
			parent = p
			height = int64(binary.BigEndian.Uint64(val[off : off+8])) // #nosec G115 -- height non-negative
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", 0, fmt.Errorf("domain not found: %s", name)
	}
	return
}

// IsDomainOwnerOrAncestor checks if agentID owns the given domain or any ancestor.
// Walks up the hierarchy by splitting on ".".
//
// Shared-domain barrier: candidates that are reserved shared domains (see
// IsSharedDomainName) are skipped — a hypothetical "general" ownership entry
// must never cascade into "x.general" inferring the same owner. Mirrors the
// barrier already in HasAccessOrAncestor and ResolveOwningAncestor; without
// it the three walks would disagree on what counts as an inheritable ancestor.
// Today this is defence in depth (auto-register paths refuse shared domains
// so the row should never exist), but the asymmetry would bite the first time
// a future code path writes such a row.
func (s *BadgerStore) IsDomainOwnerOrAncestor(domain, agentID string) (bool, error) {
	parts := strings.Split(domain, ".")
	for i := len(parts); i > 0; i-- {
		ancestor := strings.Join(parts[:i], ".")
		if IsSharedDomainName(ancestor) {
			// Cascade barrier — shared domains do not act as ancestors.
			continue
		}
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

// ModifyVerbHolders returns the sorted, deduplicated set of agent IDs that hold
// the "modify" verb on domain at blockTime — the deterministic quorum the
// app-v17 two-phase challenge is scaled against (C3). Membership mirrors the
// authz gate in processMemoryChallenge (IsDomainOwnerOrAncestor +
// HasAccessOrAncestor at level 3), so whoever could pass that gate is counted:
//   - the domain owner and every non-shared ancestor-domain owner (C-D2), plus
//   - every holder of an UNEXPIRED level-3+ grant on the domain or a non-shared
//     ancestor.
//
// Determinism: all inputs are committed Badger state; expiry uses the SAME
// predicate as HasAccessOrAncestor (expiresAt==0 permanent, else now<expiresAt),
// where now=blockTime.Unix(); the walk uses the filtered segments + shared-domain
// barrier + 16-segment cap of HasAccessOrAncestor; the result is deduped and
// sorted so the COUNT never depends on map-iteration order. Integer count only —
// no floats enter consensus state.
func (s *BadgerStore) ModifyVerbHolders(domain string, blockTime time.Time) ([]string, error) {
	if domain == "" {
		return nil, nil
	}
	segments := splitDomainSegments(domain)
	if len(segments) == 0 || len(segments) > 16 {
		return nil, nil
	}
	now := blockTime.Unix()
	set := make(map[string]struct{})
	err := s.view(func(txn *badger.Txn) error {
		for i := len(segments); i >= 1; i-- {
			candidate := strings.Join(segments[:i], ".")
			if candidate == "" || IsSharedDomainName(candidate) {
				// Cascade barrier — shared domains do not act as ancestors.
				continue
			}

			// Domain owner (mirrors IsDomainOwnerOrAncestor).
			if item, getErr := txn.Get(domainKey(candidate)); getErr == nil {
				if vErr := item.Value(func(val []byte) error {
					owner, _, decErr := decodeString(val, 0)
					if decErr == nil && owner != "" {
						set[owner] = struct{}{}
					}
					return nil
				}); vErr != nil {
					return vErr
				}
			} else if !errors.Is(getErr, badger.ErrKeyNotFound) {
				return getErr
			}

			// Level-3+ grant holders on this candidate (mirrors HasAccessOrAncestor).
			prefix := []byte("grant:" + candidate + ":")
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				gi := it.Item()
				agentID := string(gi.Key()[len(prefix):])
				if vErr := gi.Value(func(val []byte) error {
					if len(val) < 9 {
						return nil
					}
					level := val[0]
					expiresAt := int64(binary.BigEndian.Uint64(val[1:9])) // #nosec G115 -- expiry timestamp fits in int64
					if level >= 3 && (expiresAt == 0 || now < expiresAt) {
						set[agentID] = struct{}{}
					}
					return nil
				}); vErr != nil {
					it.Close()
					return vErr
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(set))
	for a := range set {
		out = append(out, a)
	}
	sort.Strings(out)
	return out, nil
}

// --- app-v17 two-phase challenge record ---

// ChallengeRecord is the on-chain record of an OPEN two-phase challenge
// (app-v17). It is persisted under state:challenge:<memoryID> and therefore
// folded into the AppHash (it is real consensus state, not bookkeeping — the
// app-v13 narrow exclusion list is exact-key and never matches this prefix).
//
// C-D3: QuorumCount is the modify-verb-holder count measured deterministically
// at the challenge-EXECUTION height and is NEVER re-measured at confirm/reinstate
// height. PriorHash/PriorStatus carry what a reinstate needs to restore the
// memory's real content hash rather than leave a hash-less husk.
type ChallengeRecord struct {
	ChallengerID    string
	Domain          string
	ExecutionHeight int64
	QuorumCount     uint32
	PriorHash       []byte
	PriorStatus     string
}

func challengeStateKey(memoryID string) string { return "challenge:" + memoryID }

// SetChallengeRecord persists an open challenge record (AppHash-folded).
// Encoding: challengerID(len) + domain(len) + executionHeight(8) +
// quorumCount(4) + priorHash(len) + priorStatus(len), all big-endian.
func (s *BadgerStore) SetChallengeRecord(memoryID string, rec *ChallengeRecord) error {
	val := encodeChallengeRecord(rec)
	return s.SetState(challengeStateKey(memoryID), val)
}

func encodeChallengeRecord(rec *ChallengeRecord) []byte {
	size := 4 + len(rec.ChallengerID) + 4 + len(rec.Domain) + 8 + 4 + 4 + len(rec.PriorHash) + 4 + len(rec.PriorStatus)
	val := make([]byte, size)
	off := encodeString(val, 0, rec.ChallengerID)
	off = encodeString(val, off, rec.Domain)
	binary.BigEndian.PutUint64(val[off:off+8], uint64(rec.ExecutionHeight)) // #nosec G115 -- block height is non-negative
	off += 8
	binary.BigEndian.PutUint32(val[off:off+4], rec.QuorumCount)
	off += 4
	binary.BigEndian.PutUint32(val[off:off+4], uint32(len(rec.PriorHash))) // #nosec G115 -- hash length fits in uint32
	off += 4
	copy(val[off:off+len(rec.PriorHash)], rec.PriorHash)
	off += len(rec.PriorHash)
	encodeString(val, off, rec.PriorStatus)
	return val
}

// OpenChallenge atomically changes memory:<id> to the challenged state and
// writes its AppHash-folded open-challenge record. Keeping both keys in one
// Badger transaction prevents a write failure from producing a challenged
// memory with no restoration record.
func (s *BadgerStore) OpenChallenge(memoryID string, contentHash []byte, status string, rec *ChallengeRecord) error {
	if rec == nil {
		return errors.New("challenge record is required")
	}
	memVal := encodeMemoryHashEntry(contentHash, status)
	recVal := encodeChallengeRecord(rec)
	return s.update(func(txn *badger.Txn) error {
		if err := s.txnSet(txn, memoryKey(memoryID), memVal); err != nil {
			return err
		}
		return s.txnSet(txn, stateKey(challengeStateKey(memoryID)), recVal)
	})
}

// ResolveChallenge atomically updates memory:<id> to its resolved state and
// removes the AppHash-folded open-challenge record. It is used for both confirm
// (deprecated) and reinstate (committed), so neither can leave a stale record if
// the write fails midway.
func (s *BadgerStore) ResolveChallenge(memoryID string, contentHash []byte, status string) error {
	memVal := encodeMemoryHashEntry(contentHash, status)
	return s.update(func(txn *badger.Txn) error {
		if err := s.txnSet(txn, memoryKey(memoryID), memVal); err != nil {
			return err
		}
		return s.txnDelete(txn, stateKey(challengeStateKey(memoryID)))
	})
}

// GetChallengeRecord returns the open challenge record for memoryID, or
// (nil, nil) if none is recorded.
func (s *BadgerStore) GetChallengeRecord(memoryID string) (*ChallengeRecord, error) {
	val, err := s.GetState(challengeStateKey(memoryID))
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	rec := &ChallengeRecord{}
	var off int
	rec.ChallengerID, off, err = decodeString(val, 0)
	if err != nil {
		return nil, err
	}
	rec.Domain, off, err = decodeString(val, off)
	if err != nil {
		return nil, err
	}
	if off+12 > len(val) {
		return nil, fmt.Errorf("invalid challenge record")
	}
	rec.ExecutionHeight = int64(binary.BigEndian.Uint64(val[off : off+8])) // #nosec G115 -- height fits in int64
	off += 8
	rec.QuorumCount = binary.BigEndian.Uint32(val[off : off+4])
	off += 4
	if off+4 > len(val) {
		return nil, fmt.Errorf("invalid challenge record")
	}
	hashLen := int(binary.BigEndian.Uint32(val[off : off+4])) // #nosec G115 -- length from 4-byte prefix
	off += 4
	if off+hashLen > len(val) {
		return nil, fmt.Errorf("invalid challenge record")
	}
	rec.PriorHash = make([]byte, hashLen)
	copy(rec.PriorHash, val[off:off+hashLen])
	off += hashLen
	rec.PriorStatus, _, err = decodeString(val, off)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// DeleteChallengeRecord removes an open challenge record (on resolution).
func (s *BadgerStore) DeleteChallengeRecord(memoryID string) error {
	return s.DeleteState(challengeStateKey(memoryID))
}

// SetAccessRequest stores an access request in BadgerDB.
// Encoding: requesterID (length-prefixed) + targetDomain (length-prefixed) + status (length-prefixed) + createdHeight (8 bytes).
func (s *BadgerStore) SetAccessRequest(requestID string, requesterID, targetDomain, status string, createdHeight int64) error {
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(requesterID)+4+len(targetDomain)+4+len(status)+8)
		offset := encodeString(val, 0, requesterID)
		offset = encodeString(val, offset, targetDomain)
		offset = encodeString(val, offset, status)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(createdHeight)) // #nosec G115 -- block height is always non-negative
		return s.txnSet(txn, accessReqKey(requestID), val)
	})
}

// GetAccessRequest retrieves an access request from BadgerDB.
func (s *BadgerStore) GetAccessRequest(requestID string) (requesterID, targetDomain, status string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, accessReqKey(requestID), newVal)
	})
}

// --- Organization / Federation / Classification ---

// orgKey returns the BadgerDB key for an organization.
func orgKey(orgID string) []byte {
	return []byte("org:" + orgID)
}

// orgNameKey returns the BadgerDB key for the one-to-many name→orgIDs reverse
// index. An entry exists for every (human-readable name, orgID) pair. Org
// names are NOT unique on-chain — `processOrgRegister` derives orgID from
// sha256(adminID + ":" + name + ":" + height), so two admins (or the same
// admin at different heights) can both register an org named "levelup" and
// each lands in a distinct orgID slot. Iterate by prefix
// "org_name:<name>:" to enumerate every orgID with that name. Value is
// empty — the key suffix is the membership marker.
func orgNameKey(name, orgID string) []byte {
	return []byte("org_name:" + name + ":" + orgID)
}

// orgNamePrefix returns the BadgerDB scan prefix for orgs registered under
// the given human-readable name.
func orgNamePrefix(name string) []byte {
	return []byte("org_name:" + name + ":")
}

// orgMemberKey returns the BadgerDB key for an org membership.
func orgMemberKey(orgID, agentID string) []byte {
	return []byte("org_member:" + orgID + ":" + agentID)
}

// agentOrgKey returns the BadgerDB key for the legacy single-slot agent→org
// reverse lookup. Retained for backward compatibility with existing callers
// (e.g. governance handlers that auto-pick a "primary" org). New multi-org
// access checks must iterate via ListAgentOrgs / IsAgentInOrg instead — this
// slot only ever holds one of the agent's orgs.
func agentOrgKey(agentID string) []byte {
	return []byte("agent_org:" + agentID)
}

// agentOrgsMemberKey returns the BadgerDB key for the one-to-many agent→orgs
// reverse index. An entry exists for every (agent, org) the agent is a member
// of. Iterate by prefix "agent_orgs:<agentID>:" to enumerate. Value is empty —
// the key itself is the membership marker.
func agentOrgsMemberKey(agentID, orgID string) []byte {
	return []byte("agent_orgs:" + agentID + ":" + orgID)
}

// agentOrgsPrefix returns the BadgerDB scan prefix for an agent's org memberships.
func agentOrgsPrefix(agentID string) []byte {
	return []byte("agent_orgs:" + agentID + ":")
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
// Maintains the name→orgIDs reverse index (org_name:<name>:<orgID>) so the
// SDK and operators can look up an org by its human-readable name without
// scanning every org entry on-chain. Names are not unique — see
// orgNameKey for why the index is one-to-many.
func (s *BadgerStore) RegisterOrg(orgID, name, description, adminAgent string, height int64) error {
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(name)+4+len(description)+4+len(adminAgent)+8)
		offset := encodeString(val, 0, name)
		offset = encodeString(val, offset, description)
		offset = encodeString(val, offset, adminAgent)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		if err := s.txnSet(txn, orgKey(orgID), val); err != nil {
			return err
		}
		// Reverse index — empty value, suffix is the orgID marker.
		return s.txnSet(txn, orgNameKey(name, orgID), nil)
	})
}

// GetOrg retrieves an organization's name and admin agent from BadgerDB.
func (s *BadgerStore) GetOrg(orgID string) (name, adminAgent string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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

// GetOrgWithMeta returns name, description, adminAgent, and registered
// block height for an org in a single read. Use this in any read path
// that informs a later admin op — those handlers validate against Badger,
// so off-chain mirrors that disagree (mirror has org row, chain doesn't)
// produce false "I'm org admin" answers and Code-54 rejections.
func (s *BadgerStore) GetOrgWithMeta(orgID string) (name, description, adminAgent string, height int64, err error) {
	err = s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(orgKey(orgID))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			n, off, decErr := decodeString(val, 0)
			if decErr != nil {
				return decErr
			}
			d, off, decErr := decodeString(val, off)
			if decErr != nil {
				return decErr
			}
			a, off, decErr := decodeString(val, off)
			if decErr != nil {
				return decErr
			}
			if len(val) < off+8 {
				return fmt.Errorf("invalid org entry: short height")
			}
			name = n
			description = d
			adminAgent = a
			height = int64(binary.BigEndian.Uint64(val[off : off+8])) // #nosec G115 -- height non-negative
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		return "", "", "", 0, fmt.Errorf("org not found: %s", orgID)
	}
	return
}

// ListOrgMembers returns every member of an org from Badger by scanning
// the org_member:<orgID>: prefix. Each entry decodes to (agentID,
// clearance, role, registeredHeight). Chain-authoritative — operators
// downstream of off-chain accessStore reads should use this when the
// answer needs to match what the ABCI handlers will see.
func (s *BadgerStore) ListOrgMembers(orgID string) ([]OrgMemberEntry, error) {
	prefix := []byte("org_member:" + orgID + ":")
	var out []OrgMemberEntry
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			agentID := string(item.Key()[len(prefix):])
			var entry OrgMemberEntry
			entry.OrgID = orgID
			entry.AgentID = agentID
			if valErr := item.Value(func(val []byte) error {
				if len(val) < 1+4 {
					return fmt.Errorf("invalid org_member entry for %s/%s", orgID, agentID)
				}
				entry.Clearance = ClearanceLevel(val[0])
				role, off, decErr := decodeString(val, 1)
				if decErr != nil {
					return decErr
				}
				entry.Role = role
				if len(val) >= off+8 {
					entry.CreatedHeight = int64(binary.BigEndian.Uint64(val[off : off+8])) // #nosec G115 -- height non-negative
				}
				return nil
			}); valErr != nil {
				return valErr
			}
			out = append(out, entry)
		}
		return nil
	})
	return out, err
}

// AddOrgMember adds a member to an organization in BadgerDB.
// Encoding: clearance (1 byte) + role (length-prefixed) + height (8 bytes).
// Maintains both the legacy single-slot agent_org reverse lookup (last add
// wins, for backward compat) and the one-to-many agent_orgs reverse index
// that supports multi-org membership.
func (s *BadgerStore) AddOrgMember(orgID, agentID string, clearance uint8, role string, height int64) error {
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 1+4+len(role)+8)
		val[0] = clearance
		encodeString(val, 1, role)
		binary.BigEndian.PutUint64(val[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is always non-negative
		if err := s.txnSet(txn, orgMemberKey(orgID, agentID), val); err != nil {
			return err
		}
		// Multi-org reverse index — additive, supports membership in N orgs.
		if err := s.txnSet(txn, agentOrgsMemberKey(agentID, orgID), nil); err != nil {
			return err
		}
		// Legacy single-slot reverse lookup — last add wins.
		return s.txnSet(txn, agentOrgKey(agentID), []byte(orgID))
	})
}

// RemoveOrgMember removes a member from an organization in BadgerDB.
// Removes the forward membership entry, the multi-org reverse index entry,
// and updates the legacy single-slot reverse lookup deterministically (points
// at any remaining membership in lexical order, or is deleted if none remain).
func (s *BadgerStore) RemoveOrgMember(orgID, agentID string) error {
	return s.update(func(txn *badger.Txn) error {
		if err := s.txnDelete(txn, orgMemberKey(orgID, agentID)); err != nil {
			return err
		}
		if err := s.txnDelete(txn, agentOrgsMemberKey(agentID, orgID)); err != nil {
			return err
		}
		// Recompute the legacy single-slot from remaining memberships so
		// callers that still use GetAgentOrg keep observing a valid org.
		remaining, err := scanAgentOrgs(txn, agentID)
		if err != nil {
			return err
		}
		if len(remaining) == 0 {
			return s.txnDelete(txn, agentOrgKey(agentID))
		}
		// Deterministic: lexically smallest orgID wins.
		sort.Strings(remaining)
		return s.txnSet(txn, agentOrgKey(agentID), []byte(remaining[0]))
	})
}

// scanAgentOrgs lists an agent's org memberships from the multi-org reverse
// index using the given txn. Internal helper — callers outside this file
// should use ListAgentOrgs.
func scanAgentOrgs(txn *badger.Txn, agentID string) ([]string, error) {
	prefix := agentOrgsPrefix(agentID)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()
	var orgs []string
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		orgs = append(orgs, string(key[len(prefix):]))
	}
	return orgs, nil
}

// ListAgentOrgs returns every org the agent is a member of. Empty slice if none.
// Used by multi-org access checks (HasAccessMultiOrg, agentHasTopSecretClearance).
func (s *BadgerStore) ListAgentOrgs(agentID string) ([]string, error) {
	var orgs []string
	err := s.view(func(txn *badger.Txn) error {
		var scanErr error
		orgs, scanErr = scanAgentOrgs(txn, agentID)
		return scanErr
	})
	return orgs, err
}

// IsAgentInOrg reports whether the agent is a member of the given org.
// Cheaper than ListAgentOrgs when only one org needs to be verified.
func (s *BadgerStore) IsAgentInOrg(agentID, orgID string) (bool, error) {
	var found bool
	err := s.view(func(txn *badger.Txn) error {
		_, getErr := txn.Get(agentOrgsMemberKey(agentID, orgID))
		if getErr == nil {
			found = true
			return nil
		}
		if getErr == badger.ErrKeyNotFound {
			return nil
		}
		return getErr
	})
	return found, err
}

const (
	// Startup index backfills must stay far below Badger's transaction ceiling.
	// The byte budget includes a conservative per-derived-entry allowance. The
	// count budget is a second, independent bound for databases containing many
	// tiny keys; the bounded cursor is persisted separately in the sidecar.
	defaultIndexBackfillMaxEntries = 512
	defaultIndexBackfillMaxBytes   = 1 << 20
	indexBackfillEntryOverhead     = 64

	indexBackfillInProgress byte = 0
	indexBackfillComplete   byte = 1
)

var (
	agentOrgsIndexBackfillProgressKey = []byte(consensuskeys.AgentOrgsIndexBackfillProgress)
	orgNameIndexBackfillProgressKey   = []byte(consensuskeys.OrgNameIndexBackfillProgress)
)

// indexBackfillOptions is deliberately private: production always uses the
// conservative defaults above, while store tests use small limits and an
// afterBatch hook to prove crash/restart behavior without a huge fixture.
type indexBackfillOptions struct {
	maxEntries  int
	maxBytes    int
	afterDBSync func(batch int, complete bool) error
	afterBatch  func(batch int, complete bool) error
}

type indexBackfillSpec struct {
	name         string
	sourcePrefix []byte
	progressFile string
	derive       func(item *badger.Item) (key, value []byte, skip bool, err error)
}

// ensureIndexBackfill rebuilds a derived index in bounded, durable batches.
// Every batch commits its derived rows, explicitly Syncs Badger, and only then
// atomically replaces a fsynced local sidecar cursor. A crash can therefore
// leave an old cursor with newer idempotent index rows (which are replayed), but
// can never expose a cursor that skipped an unsynced index row. The sidecar
// lives inside the Badger directory but outside its logical key/value space, so
// it cannot alter historical AppHash, snapshots, or Internet state-sync images.
func (s *BadgerStore) ensureIndexBackfill(spec indexBackfillSpec, options indexBackfillOptions) error {
	if s.txn != nil {
		// Joining the app-v20 block transaction would turn the loop below back
		// into one unbounded transaction and make a startup migration part of
		// FinalizeBlock. These migrations are constructor-only maintenance.
		return fmt.Errorf("%s index backfill cannot run in a consensus transaction", spec.name)
	}
	s.indexBackfillMu.Lock()
	defer s.indexBackfillMu.Unlock()
	if !s.legacyIndexBackfillMarkersScrubbed {
		// Two short-lived v11.9 development builds wrote progress into Badger.
		// Delete both exact keys in one transaction and cross the durability
		// boundary before trusting a sidecar or scanning a source namespace. This
		// restores byte-for-byte compatibility with v11.8's AppHash while the
		// exact hash/export exclusions remain as defence in depth.
		legacyMarkerFound := false
		if err := s.db.View(func(txn *badger.Txn) error {
			for _, key := range [][]byte{agentOrgsIndexBackfillProgressKey, orgNameIndexBackfillProgressKey} {
				if _, err := txn.Get(key); err == nil {
					legacyMarkerFound = true
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("inspect legacy index backfill markers: %w", err)
		}
		if legacyMarkerFound {
			if err := s.db.Update(func(txn *badger.Txn) error {
				if err := txn.Delete(agentOrgsIndexBackfillProgressKey); err != nil {
					return err
				}
				return txn.Delete(orgNameIndexBackfillProgressKey)
			}); err != nil {
				return fmt.Errorf("scrub legacy index backfill markers: %w", err)
			}
			if err := s.db.Sync(); err != nil {
				return fmt.Errorf("sync legacy index backfill marker scrub: %w", err)
			}
		}
		s.legacyIndexBackfillMarkersScrubbed = true
	}
	if options.maxEntries <= 0 {
		options.maxEntries = defaultIndexBackfillMaxEntries
	}
	if options.maxBytes <= 0 {
		options.maxBytes = defaultIndexBackfillMaxBytes
	}
	badgerDir := filepath.Clean(s.db.Opts().Dir)
	progress, err := readIndexBackfillProgress(badgerDir, spec.progressFile, spec.sourcePrefix)
	if err != nil {
		return fmt.Errorf("read %s index backfill progress: %w", spec.name, err)
	}
	if progress.complete {
		// If an earlier rename became visible but its parent-directory fsync
		// reported failure, a retry must re-establish the full durability barrier
		// before treating completion as successful. Replacing one bounded file is
		// still O(1) and works on both POSIX and Windows.
		if err := writeIndexBackfillProgress(badgerDir, spec.progressFile, progress); err != nil {
			return fmt.Errorf("reconfirm completed %s index backfill progress: %w", spec.name, err)
		}
		return nil
	}
	cursor := progress.cursor

	for batch := 1; ; batch++ {
		var (
			batchComplete bool
			lastSourceKey []byte
		)

		err := s.db.Update(func(txn *badger.Txn) error {
			if len(cursor) > 0 {
				// A checksummed but nonsensical cursor must never skip earlier rows.
				// Requiring its authoritative source entry to remain present makes
				// corruption/deletion fail closed instead of seeking past it.
				if _, getErr := txn.Get(cursor); getErr != nil {
					return fmt.Errorf("%s index backfill cursor source is unavailable: %w", spec.name, getErr)
				}
			}

			iteratorOptions := badger.DefaultIteratorOptions
			iteratorOptions.Prefix = spec.sourcePrefix
			iteratorOptions.PrefetchValues = false
			it := txn.NewIterator(iteratorOptions)
			defer it.Close()

			seek := spec.sourcePrefix
			if len(cursor) > 0 {
				seek = cursor
			}
			it.Seek(seek)
			// The cursor is the last fully indexed source key. Seek is inclusive,
			// so advance once; the existence check above rejects deletion or a
			// forged within-prefix cursor instead of silently skipping past it.
			if len(cursor) > 0 && it.ValidForPrefix(spec.sourcePrefix) && bytes.Equal(it.Item().Key(), cursor) {
				it.Next()
			}

			processed := 0
			derivedBytes := 0
			for it.ValidForPrefix(spec.sourcePrefix) {
				if processed >= options.maxEntries {
					break
				}
				item := it.Item()
				sourceKey := item.KeyCopy(nil)
				indexKey, indexValue, skip, deriveErr := spec.derive(item)
				if deriveErr != nil {
					return fmt.Errorf("%s index backfill source %q: %w", spec.name, sourceKey, deriveErr)
				}

				entryBytes := 0
				if !skip {
					entryBytes = len(indexKey) + len(indexValue) + indexBackfillEntryOverhead
				}
				if derivedBytes+entryBytes > options.maxBytes {
					if processed == 0 {
						return fmt.Errorf("%s index backfill source %q exceeds %d-byte batch limit", spec.name, sourceKey, options.maxBytes)
					}
					break
				}

				if !skip {
					if setErr := txn.Set(indexKey, indexValue); setErr != nil {
						return setErr
					}
				}
				processed++
				derivedBytes += entryBytes
				lastSourceKey = sourceKey
				it.Next()
			}

			if !it.ValidForPrefix(spec.sourcePrefix) {
				batchComplete = true
				return nil
			}
			if len(lastSourceKey) == 0 {
				return fmt.Errorf("%s index backfill made no progress", spec.name)
			}
			return nil
		})
		if err != nil {
			return err
		}
		// Badger's default SyncWrites setting is false. Explicitly crossing the
		// Sync boundary before advancing the external cursor makes its claim real.
		// A power loss still yields the preceding sidecar or a replayable set of
		// newer idempotent derived rows.
		if err := s.db.Sync(); err != nil {
			return fmt.Errorf("sync %s index backfill batch %d: %w", spec.name, batch, err)
		}
		if options.afterDBSync != nil {
			if err := options.afterDBSync(batch, batchComplete); err != nil {
				return err
			}
		}
		nextProgress := indexBackfillProgress{complete: batchComplete}
		if !batchComplete {
			nextProgress.cursor = lastSourceKey
		}
		if err := writeIndexBackfillProgress(badgerDir, spec.progressFile, nextProgress); err != nil {
			return fmt.Errorf("write %s index backfill progress for batch %d: %w", spec.name, batch, err)
		}
		cursor = nextProgress.cursor
		if options.afterBatch != nil {
			if err := options.afterBatch(batch, batchComplete); err != nil {
				return err
			}
		}
		if batchComplete {
			return nil
		}
	}
}

// EnsureAgentOrgsIndex backfills the one-to-many agent_orgs reverse index from
// the authoritative org_member forward index. The durable completion sidecar
// makes subsequent opens O(1). Required for upgrades from versions where the
// reverse lookup was a single-slot agent_org:<agent> and multi-org members
// existed only in the forward index.
func (s *BadgerStore) EnsureAgentOrgsIndex() error {
	return s.ensureAgentOrgsIndex(indexBackfillOptions{})
}

func (s *BadgerStore) ensureAgentOrgsIndex(options indexBackfillOptions) error {
	prefix := []byte("org_member:")
	return s.ensureIndexBackfill(indexBackfillSpec{
		name:         "agent_orgs",
		sourcePrefix: prefix,
		progressFile: agentOrgsIndexBackfillSidecar,
		derive: func(item *badger.Item) (key, value []byte, skip bool, err error) {
			suffix := string(item.Key()[len(prefix):])
			colon := strings.IndexByte(suffix, ':')
			if colon < 0 {
				// Preserve the historical backfill behavior for malformed source
				// keys: ignore them, but advance the cursor so restart can finish.
				return nil, nil, true, nil
			}
			orgID := suffix[:colon]
			agentID := suffix[colon+1:]
			return agentOrgsMemberKey(agentID, orgID), nil, false, nil
		},
	}, options)
}

// ListOrgsByName returns every organization registered with the given
// human-readable name. Names are not enforced unique on-chain — the same
// "levelup" name can map to many distinct orgIDs from different admins (or
// the same admin at different heights). Returns an empty slice (not an
// error) when no orgs match. Each entry includes orgID, name, description,
// adminAgentID, and createdHeight; createdAt is left zero because the
// authoritative timestamp lives in the offchain store, not BadgerDB.
func (s *BadgerStore) ListOrgsByName(name string) ([]OrgEntry, error) {
	if name == "" {
		return nil, fmt.Errorf("org name is required")
	}
	var entries []OrgEntry
	err := s.view(func(txn *badger.Txn) error {
		prefix := orgNamePrefix(name)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			orgID := string(it.Item().Key()[len(prefix):])
			item, getErr := txn.Get(orgKey(orgID))
			if getErr == badger.ErrKeyNotFound {
				// Stale reverse index entry pointing at a deleted org —
				// skip silently. Backfill on next open will reconcile.
				continue
			}
			if getErr != nil {
				return getErr
			}
			var (
				orgName   string
				orgDesc   string
				orgAdmin  string
				orgHeight int64
				decodeErr error
			)
			err := item.Value(func(val []byte) error {
				var offset int
				orgName, offset, decodeErr = decodeString(val, 0)
				if decodeErr != nil {
					return decodeErr
				}
				orgDesc, offset, decodeErr = decodeString(val, offset)
				if decodeErr != nil {
					return decodeErr
				}
				orgAdmin, offset, decodeErr = decodeString(val, offset)
				if decodeErr != nil {
					return decodeErr
				}
				if offset+8 > len(val) {
					return fmt.Errorf("invalid org entry: missing height")
				}
				orgHeight = int64(binary.BigEndian.Uint64(val[offset : offset+8])) // #nosec G115 -- block height fits in int64
				return nil
			})
			if err != nil {
				return err
			}
			entries = append(entries, OrgEntry{
				OrgID:         orgID,
				Name:          orgName,
				Description:   orgDesc,
				AdminAgentID:  orgAdmin,
				CreatedHeight: orgHeight,
			})
		}
		return nil
	})
	return entries, err
}

// EnsureOrgNameIndex backfills the one-to-many name→orgIDs reverse index from
// the authoritative org:* forward entries. The versioned completion sidecar
// makes subsequent opens O(1). Required for in-place upgrades from pre-v6.6.9
// binaries that didn't maintain it, so GET /v1/org/by-name resolves existing
// chain state without a reset.
func (s *BadgerStore) EnsureOrgNameIndex() error {
	return s.ensureOrgNameIndex(indexBackfillOptions{})
}

func (s *BadgerStore) ensureOrgNameIndex(options indexBackfillOptions) error {
	prefix := []byte("org:")
	return s.ensureIndexBackfill(indexBackfillSpec{
		name:         "org_name",
		sourcePrefix: prefix,
		progressFile: orgNameIndexBackfillSidecar,
		derive: func(item *badger.Item) (key, value []byte, skip bool, err error) {
			orgID := string(item.Key()[len(prefix):])
			var orgName string
			if err := item.Value(func(val []byte) error {
				name, _, decodeErr := decodeString(val, 0)
				if decodeErr != nil {
					return decodeErr
				}
				orgName = name
				return nil
			}); err != nil {
				return nil, nil, false, err
			}
			if orgName == "" {
				return nil, nil, true, nil
			}
			return orgNameKey(orgName, orgID), nil, false, nil
		},
	}, options)
}

// GetMemberClearance retrieves a member's clearance level and role.
func (s *BadgerStore) GetMemberClearance(orgID, agentID string) (clearance uint8, role string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, orgMemberKey(orgID, agentID), newVal)
	})
}

// GetAgentOrg retrieves the organization an agent belongs to (reverse lookup).
func (s *BadgerStore) GetAgentOrg(agentID string) (orgID string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
//   - expiresAt (8 bytes) + requiresApproval (1 byte) + status (length-prefixed)
//   - allowedDomains count (4 bytes) + each domain (length-prefixed)
//   - allowedDepts count (4 bytes) + each dept (length-prefixed).
func (s *BadgerStore) SetFederation(fedID string, proposerOrg, targetOrg string, allowedDomains []string, maxClearance uint8, expiresAt int64, requiresApproval bool, status string, allowedDepts ...[]string) error {
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, federationKey(fedID), val)
	})
}

// GetFederation retrieves a federation entry from BadgerDB.
func (s *BadgerStore) GetFederation(fedID string) (proposerOrg, targetOrg string, maxClearance uint8, expiresAt int64, status string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, federationKey(fedID), newVal)
	})
}

// FindFederation scans for an active federation between two orgs (either direction).
func (s *BadgerStore) FindFederation(orgA, orgB string) (fedID string, err error) {
	prefix := []byte("federation:")
	err = s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, memClassKey(memoryID), []byte{classification})
	})
}

// GetMemoryClassification retrieves a memory's classification level.
func (s *BadgerStore) GetMemoryClassification(memoryID string) (uint8, error) {
	var classification uint8
	err := s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 4+len(name)+4+len(description)+4+len(parentDept)+8)
		offset := encodeString(val, 0, name)
		offset = encodeString(val, offset, description)
		offset = encodeString(val, offset, parentDept)
		binary.BigEndian.PutUint64(val[offset:offset+8], uint64(height)) // #nosec G115 -- block height is always non-negative
		return s.txnSet(txn, deptKey(orgID, deptID), val)
	})
}

// GetDept retrieves a department's name and description from BadgerDB.
func (s *BadgerStore) GetDept(orgID, deptID string) (name, description string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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

	err := s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		val := make([]byte, 1+4+len(role)+8)
		val[0] = clearance
		encodeString(val, 1, role)
		binary.BigEndian.PutUint64(val[1+4+len(role):], uint64(height)) // #nosec G115 -- block height is always non-negative
		if err := s.txnSet(txn, deptMemberKey(orgID, deptID, agentID), val); err != nil {
			return err
		}
		// Reverse lookup: agent→dept (value = "orgID:deptID")
		return s.txnSet(txn, agentDeptKey(agentID), []byte(orgID+":"+deptID))
	})
}

// RemoveDeptMember removes a member from a department in BadgerDB.
func (s *BadgerStore) RemoveDeptMember(orgID, deptID, agentID string) error {
	return s.update(func(txn *badger.Txn) error {
		if err := s.txnDelete(txn, deptMemberKey(orgID, deptID, agentID)); err != nil {
			return err
		}
		return s.txnDelete(txn, agentDeptKey(agentID))
	})
}

// GetDeptMemberClearance retrieves a department member's clearance level and role.
func (s *BadgerStore) GetDeptMemberClearance(orgID, deptID, agentID string) (clearance uint8, role string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, deptMemberKey(orgID, deptID, agentID), newVal)
	})
}

// GetAgentDept retrieves the department an agent belongs to (reverse lookup).
// Returns orgID and deptID by splitting the stored value on ":".
func (s *BadgerStore) GetAgentDept(agentID string) (orgID, deptID string, err error) {
	err = s.view(func(txn *badger.Txn) error {
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
	err := s.view(func(txn *badger.Txn) error {
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
//
// Multi-org members: iterates every org the agent is a member of, granting
// same-org access if any of those orgs owns the domain at sufficient clearance,
// and falling back to a federation check between any agent org and the domain
// owner's orgs.
//
// postFork toggles v8.0 ancestor-walk semantics: when true, the direct-grant
// check and the domain-owner resolution both walk the dotted-domain path
// from the leaf upward (skipping shared-domain barriers), so a grant on a
// parent domain and ownership on an ancestor both cover descendant lookups.
// When false the behaviour is byte-identical to v7.1.1 — exact-match grant,
// exact-match owner lookup. Fork-gated callers pass app.postV8Fork(height)
// on the consensus path and app.IsPostV8Fork() (advisory chain-height read)
// on REST handlers.
func (s *BadgerStore) HasAccessMultiOrg(domain, agentID string, memoryClassification uint8, blockTime time.Time, postFork bool) (bool, error) {
	return s.hasAccessMultiOrg(domain, agentID, 1, memoryClassification, blockTime, postFork)
}

// HasWriteAccessMultiOrg is the write-verb sibling of HasAccessMultiOrg.
// app-v18 consensus callers use it so a read-only level-1 grant cannot submit
// memories. Write is an explicit level-2 direct/ancestor grant: org membership
// and federation clearance remain read-discovery mechanisms and cannot imply a
// write verb. Effective domain owners are handled by the consensus caller.
func (s *BadgerStore) HasWriteAccessMultiOrg(domain, agentID string, blockTime time.Time, postFork bool) (bool, error) {
	if postFork {
		return s.HasAccessOrAncestor(domain, agentID, 2, blockTime)
	}
	return s.HasAccess(domain, agentID, 2, blockTime)
}

func (s *BadgerStore) hasAccessMultiOrg(domain, agentID string, directGrantLevel, requiredClearance uint8, blockTime time.Time, postFork bool) (bool, error) {
	// Step 1: Check direct grant. Post-fork walks the dotted path so a grant
	// on a parent domain covers descendant writes; pre-fork preserves exact
	// match (v7.1.1-equivalent replay).
	var directAccess bool
	var err error
	if postFork {
		directAccess, err = s.HasAccessOrAncestor(domain, agentID, directGrantLevel, blockTime)
	} else {
		directAccess, err = s.HasAccess(domain, agentID, directGrantLevel, blockTime)
	}
	if err == nil && directAccess {
		return true, nil
	}

	// Step 2: Enumerate every org the agent is a member of.
	agentOrgs, err := s.ListAgentOrgs(agentID)
	if err != nil || len(agentOrgs) == 0 {
		// Agent not in any org — only direct grants work
		return false, nil
	}

	// Step 3: Resolve the domain owner's orgs (the owner can also be multi-org).
	// Post-fork walks the dotted path to find the nearest registered ancestor;
	// pre-fork keeps exact-match GetDomainOwner. Once the owner is resolved
	// via ancestor walk, downstream federation/clearance checks inherit
	// ancestor semantics automatically — no further changes needed below.
	var domainOwner string
	if postFork {
		owner, _, resolveErr := s.ResolveOwningAncestor(domain)
		if resolveErr != nil || owner == "" {
			return false, nil // No owned ancestor → domain treated as unregistered
		}
		domainOwner = owner
	} else {
		o, gerr := s.GetDomainOwner(domain)
		if gerr != nil {
			return false, nil // Domain doesn't exist
		}
		domainOwner = o
	}

	domainOrgs, err := s.ListAgentOrgs(domainOwner)
	if err != nil || len(domainOrgs) == 0 {
		return false, nil
	}
	domainOrgSet := make(map[string]struct{}, len(domainOrgs))
	for _, o := range domainOrgs {
		domainOrgSet[o] = struct{}{}
	}

	// Step 4: Same-org access — does any of the agent's orgs own this domain
	// at sufficient clearance? This is the path that the previous single-slot
	// implementation silently failed on whenever the agent's "primary" org
	// had been overwritten by a later AddOrgMember to a different org.
	for _, agentOrg := range agentOrgs {
		if _, sameOrg := domainOrgSet[agentOrg]; !sameOrg {
			continue
		}
		clearance, _, gerr := s.GetMemberClearance(agentOrg, agentID)
		if gerr != nil {
			continue
		}
		if clearance >= requiredClearance {
			return true, nil
		}
	}

	// Step 5: Different org — need federation agreement. Try every (agentOrg,
	// domainOrg) pairing; the first active federation that satisfies clearance
	// and dept constraints wins.
	for _, agentOrg := range agentOrgs {
		clearance, _, gerr := s.GetMemberClearance(agentOrg, agentID)
		if gerr != nil {
			continue
		}
		for _, domainOrg := range domainOrgs {
			if agentOrg == domainOrg {
				continue
			}
			ok, fedErr := s.checkFederationAccess(agentOrg, domainOrg, agentID, clearance, requiredClearance, blockTime)
			if fedErr == nil && ok {
				return true, nil
			}
		}
	}

	return false, nil
}

// checkFederationAccess evaluates a single (agentOrg, domainOrg) pair against
// any active federation between them. Extracted so HasAccessMultiOrg can fan
// out across multi-org members without nested loops blowing up the function.
func (s *BadgerStore) checkFederationAccess(agentOrg, domainOrg, agentID string, agentClearance, memoryClassification uint8, blockTime time.Time) (bool, error) {
	fedID, err := s.FindFederation(agentOrg, domainOrg)
	if err != nil {
		return false, nil
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
	return s.update(func(txn *badger.Txn) error {
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

		return s.txnSet(txn, accessLogKey(height, seq), val)
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
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, agentOnChainKey(agentID), data)
	})
}

// GetRegisteredAgent retrieves an agent's on-chain state.
func (s *BadgerStore) GetRegisteredAgent(agentID string) (*OnChainAgent, error) {
	var agent OnChainAgent
	err := s.view(func(txn *badger.Txn) error {
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
	err := s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(agentOnChainKey(agentID))
		return err
	})
	return err == nil
}

// UpdateAgentMeta updates an agent's mutable display name and bio on-chain.
// RegisteredName is the permanent on-chain identity and is NEVER modified here.
func (s *BadgerStore) UpdateAgentMeta(agentID, name, bio string) error {
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, agentOnChainKey(agentID), data)
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
	return s.update(func(txn *badger.Txn) error {
		return s.txnDelete(txn, stateKey(key))
	})
}

// PrefixKeys returns all keys in the state namespace matching the given prefix,
// sorted lexicographically. Keys are returned WITHOUT the "state:" prefix.
func (s *BadgerStore) PrefixKeys(prefix string) ([]string, error) {
	return s.PrefixKeysLimit(prefix, 0)
}

// PrefixKeysLimit is PrefixKeys with an optional deterministic upper bound.
// A non-positive limit preserves the historical unbounded behavior.
func (s *BadgerStore) PrefixKeysLimit(prefix string, limit int) ([]string, error) {
	fullPrefix := stateKey(prefix)
	var keys []string

	err := s.view(func(txn *badger.Txn) error {
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
				if limit > 0 && len(keys) >= limit {
					break
				}
			}
		}
		return nil
	})

	return keys, err
}

// ValidateAppV20ResourceBounds audits legacy consensus state before app-v20 is
// allowed to activate. New app-v20 transactions enforce the same identifier
// ceiling at decode time; this scan prevents a short valid reference from
// loading an oversized pre-fork domain/identifier and multiplying it across
// validator-stat keys inside the block-wide Badger transaction.
func (s *BadgerStore) ValidateAppV20ResourceBounds(maxIdentifierBytes, maxMetadataBytes, maxRecordBytes, maxValidatorRecords int) error {
	if maxIdentifierBytes <= 0 || maxMetadataBytes <= 0 || maxRecordBytes <= 0 || maxValidatorRecords <= 0 {
		return errors.New("app-v20 resource bounds must be positive")
	}
	singleSuffixPrefixes := [][]byte{
		[]byte("memory:"), []byte("memdomain:"), []byte("memauthor:"), []byte("memclass:"),
		[]byte("nonce:"), []byte("agent:"), []byte("validator:"), []byte("vstats:"),
		[]byte("domain:"), []byte("org:"), []byte("access_req:"), []byte("federation:"),
		[]byte("cross_fed:"), []byte("poew:"), []byte("cocommit:core:"),
		[]byte("cocommit:shared:"), []byte("cocommit:coauthors:"),
	}
	fullRecordPrefixes := [][]byte{
		[]byte("agent:"), []byte("org:"), []byte("org_member:"),
		[]byte("dept:"), []byte("dept_member:"), []byte("access_req:"),
		[]byte("federation:"), []byte("cross_fed:"),
	}
	return s.view(func(txn *badger.Txn) error {
		validatorRecords := 0
		poeWeightRecords := 0
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.HasPrefix(key, []byte("validator:")) {
				validatorRecords++
				if validatorRecords > maxValidatorRecords {
					return fmt.Errorf("legacy persisted validator set has more than %d records", maxValidatorRecords)
				}
			}
			if bytes.HasPrefix(key, []byte(poeWeightsPrefix)) && !bytes.Equal(key, poeWeightsCurrentKey) {
				poeWeightRecords++
				if poeWeightRecords > maxValidatorRecords {
					return fmt.Errorf("legacy persisted PoE weight set has more than %d records", maxValidatorRecords)
				}
			}
			if len(key) > 3*maxIdentifierBytes+128 {
				return fmt.Errorf("legacy consensus key has %d bytes, app-v20 limit %d: %q", len(key), 3*maxIdentifierBytes+128, key)
			}
			for _, prefix := range singleSuffixPrefixes {
				if bytes.HasPrefix(key, prefix) && len(key)-len(prefix) > maxIdentifierBytes {
					return fmt.Errorf("legacy consensus identifier has %d bytes, app-v20 limit %d: %q", len(key)-len(prefix), maxIdentifierBytes, key)
				}
			}
			for _, prefix := range fullRecordPrefixes {
				if !bytes.HasPrefix(key, prefix) {
					continue
				}
				if err := item.Value(func(value []byte) error {
					if len(value) > maxRecordBytes {
						return fmt.Errorf("legacy consensus record %q has %d bytes, app-v20 limit %d", key, len(value), maxRecordBytes)
					}
					return nil
				}); err != nil {
					return err
				}
				break
			}

			switch {
			case bytes.HasPrefix(key, []byte("memory:")):
				if err := item.Value(func(value []byte) error {
					hash, _, err := decodeMemoryEntry(value)
					if err != nil {
						return err
					}
					if len(hash) != 0 && len(hash) != sha256.Size {
						return fmt.Errorf("legacy memory %q has content hash length %d, app-v20 requires 0 or %d", key, len(hash), sha256.Size)
					}
					return nil
				}); err != nil {
					return err
				}
			case bytes.HasPrefix(key, []byte("memdomain:")), bytes.HasPrefix(key, []byte("memauthor:")):
				if err := item.Value(func(value []byte) error {
					if len(value) > maxIdentifierBytes {
						return fmt.Errorf("legacy consensus identifier value for %q has %d bytes, app-v20 limit %d", key, len(value), maxIdentifierBytes)
					}
					return nil
				}); err != nil {
					return err
				}
			case bytes.HasPrefix(key, []byte("domain:")):
				if err := item.Value(func(value []byte) error {
					owner, off, err := decodeString(value, 0)
					if err != nil {
						return err
					}
					parent, _, err := decodeString(value, off)
					if err != nil {
						return err
					}
					if len(owner) > maxIdentifierBytes || len(parent) > maxIdentifierBytes {
						return fmt.Errorf("legacy domain %q contains an oversized owner or parent", key)
					}
					return nil
				}); err != nil {
					return err
				}
			case bytes.HasPrefix(key, []byte("agent:")):
				if err := item.Value(func(value []byte) error {
					var agent OnChainAgent
					if err := json.Unmarshal(value, &agent); err != nil {
						return err
					}
					if len(agent.AgentID) > maxIdentifierBytes || len(agent.Name) > maxIdentifierBytes ||
						len(agent.RegisteredName) > maxIdentifierBytes || len(agent.Role) > maxIdentifierBytes ||
						len(agent.Provider) > maxIdentifierBytes || len(agent.OrgID) > maxIdentifierBytes ||
						len(agent.DeptID) > maxIdentifierBytes {
						return fmt.Errorf("legacy agent %q contains an oversized identifier", key)
					}
					if len(agent.BootBio) > maxMetadataBytes || len(agent.P2PAddress) > maxMetadataBytes ||
						len(agent.DomainAccess) > maxMetadataBytes ||
						len(agent.VisibleAgents) > maxMetadataBytes {
						return fmt.Errorf("legacy agent %q contains oversized metadata", key)
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}
		return nil
	})
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

	err := s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		for id, vp := range validators {
			key := []byte("validator:" + id)
			data, err := json.Marshal(vp)
			if err != nil {
				return fmt.Errorf("marshal validator %s: %w", id, err)
			}
			if err := s.txnSet(txn, key, data); err != nil {
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

	err := s.view(func(txn *badger.Txn) error {
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
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, key, value)
	})
}

// SetAgentPermission updates an agent's permissions on-chain.
func (s *BadgerStore) SetAgentPermission(agentID string, clearance uint8, domainAccess, visibleAgents, orgID, deptID string) error {
	return s.update(func(txn *badger.Txn) error {
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
		return s.txnSet(txn, agentOnChainKey(agentID), data)
	})
}

// ListRegisteredAgents returns all on-chain registered agents.
func (s *BadgerStore) ListRegisteredAgents() ([]OnChainAgent, error) {
	var agents []OnChainAgent
	prefix := []byte("agent:")
	err := s.view(func(txn *badger.Txn) error {
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
