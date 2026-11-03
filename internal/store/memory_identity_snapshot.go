package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/tx"
)

// memoryIdentityMode belongs to one held transaction, never to a process-wide
// cache or a caller-selected app version. Startup's full policy/index validation
// remains a prerequisite; this check does not rescan those indexes per row.
type memoryIdentityMode struct {
	raw             bool
	height          int64
	heightPresent   bool
	appliedHeight   int64
	freshAppVersion uint64
}

func exactMemorySnapshotState(txn *badger.Txn, name string, maxBytes int64) ([]byte, bool, error) {
	item, err := txn.Get(stateKey(name))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if item.ValueSize() > maxBytes {
		return nil, true, errors.New("canonical identity value exceeds its bounded representation")
	}
	value, err := item.ValueCopy(nil)
	return value, true, err
}

// memoryIdentityModeInSnapshot must be evaluated once by the owner of the
// held reader and passed to its envelope/inventory adapters. No new view is
// opened here; a scoped speculative reader observes only its own transaction.
func (s *BadgerStore) memoryIdentityModeInSnapshot() (memoryIdentityMode, error) {
	var mode memoryIdentityMode
	if s == nil || s.db == nil || s.txn == nil {
		return mode, errors.New("memory identity qualification requires a held canonical snapshot")
	}
	height, present, err := exactMemorySnapshotState(s.txn, "height", 8)
	if err != nil {
		return mode, fmt.Errorf("read finalized identity height: %w", err)
	}
	mode.heightPresent = present
	if present {
		if len(height) != 8 {
			return mode, errors.New("canonical finalized height must contain exactly8 bytes")
		}
		value := binary.BigEndian.Uint64(height)
		if value > math.MaxInt64 {
			return mode, errors.New("canonical finalized height exceeds int64")
		}
		mode.height = int64(value) // #nosec G115 -- checked above
	}
	// Bound stored JSON before existing typed decoders allocate their fields.
	for _, record := range []struct {
		key []byte
		max int64
	}{{upgradeAppliedKey("app-v30"), 1024}, {freshGenesisActivationKey(), 128 << 10}, {appV23RootKey(), 8 << 10}} {
		item, recordErr := s.txn.Get(record.key)
		if errors.Is(recordErr, badger.ErrKeyNotFound) {
			continue
		}
		if recordErr != nil {
			return mode, recordErr
		}
		if item.ValueSize() > record.max {
			return mode, errors.New("canonical identity record exceeds its bounded representation")
		}
	}
	applied, err := s.GetAppliedUpgrade("app-v30")
	if err != nil {
		return mode, fmt.Errorf("read identity upgrade: %w", err)
	}
	origin, err := s.GetFreshGenesisActivation()
	if err != nil {
		return mode, fmt.Errorf("read identity genesis origin: %w", err)
	}
	if origin != nil {
		// Reject contradictory lineage by key presence before typed decoding,
		// keeping all non-H0 lineage reads bounded independently of value size.
		if _, err := s.txn.Get(appV23GenesisActivationKey()); err == nil {
			return mode, errors.New("fresh identity origin coexists with app-v23 genesis")
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return mode, err
		}
		for version := uint64(2); version <= origin.AppVersion; version++ {
			if _, err := s.txn.Get(upgradeAppliedKey(fmt.Sprintf("app-v%d", version))); err == nil {
				return mode, errors.New("fresh identity origin coexists with an applied ancestor")
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return mode, err
			}
		}
		if err := s.ValidateFreshGenesisLineage(); err != nil {
			return mode, fmt.Errorf("validate identity origin lineage: %w", err)
		}
		root, err := s.GetAppV23Root()
		if err != nil {
			return mode, err
		}
		if root == nil || root.PrincipalID != origin.RootID || root.Scope != origin.Scope || root.BootstrapDigest != origin.BootstrapDigest {
			return mode, errors.New("identity origin does not bind canonical Root")
		}
		if rootErr := s.validateMemoryIdentityRootInSnapshot(root); rootErr != nil {
			return mode, rootErr
		}
		domain, found, err := exactMemorySnapshotState(s.txn, consensuskeys.GovernanceDelegationDomainV20, 32)
		if err != nil {
			return mode, err
		}
		want, err := hex.DecodeString(origin.Scope)
		if err != nil || !found || !bytes.Equal(domain, want) {
			return mode, errors.New("identity origin does not bind canonical chain scope")
		}
		if mode.height == 0 {
			// Missing height is a qualified H0 only after proving the exact
			// pristine origin keyspace, never a LoadState-style default.
			if err := s.ValidateFreshGenesisInitialState(); err != nil {
				return mode, fmt.Errorf("validate identity genesis H0: %w", err)
			}
		}
		mode.freshAppVersion = origin.AppVersion
		if origin.AppVersion >= 30 {
			// Lineage validation already refuses a fabricated applied30 marker.
			mode.raw = mode.height > 0
			return mode, nil
		}
	}
	if applied == nil {
		return mode, nil
	}
	if applied.Name != "app-v30" || applied.TargetAppVersion != 30 || applied.AppliedHeight <= 0 {
		return mode, errors.New("canonical identity upgrade record is invalid")
	}
	if !mode.heightPresent || mode.height < applied.AppliedHeight-1 {
		return mode, errors.New("identity upgrade is ahead of persisted finalized state")
	}
	mode.appliedHeight = applied.AppliedHeight
	mode.raw = mode.height > applied.AppliedHeight
	return mode, nil
}

// The Root JSON reader is deliberately a decoder, not a validator. These
// bounded core/history-endpoint checks complement startup's complete policy,
// credential-history digest and index validation without rescanning history.
// Root has no independent schema/version field; its origin schema is checked
// by GetFreshGenesisActivation using the canonical fresh-genesis constants.
func (s *BadgerStore) validateMemoryIdentityRootInSnapshot(root *AppV23RootState) error {
	if root == nil || s == nil || s.txn == nil {
		return errors.New("identity Root validation requires its held snapshot")
	}
	if err := validateCanonicalAgentID("identity Root principal", root.PrincipalID); err != nil {
		return err
	}
	if err := validateCanonicalAgentID("identity Root credential", root.CredentialID); err != nil {
		return err
	}
	if root.Generation == 0 {
		return errors.New("identity Root generation must be positive")
	}
	history, err := hex.DecodeString(root.HistoryDigest)
	if err != nil || len(history) != sha256.Size || hex.EncodeToString(history) != root.HistoryDigest {
		return errors.New("identity Root history digest must be canonical lowercase SHA-256")
	}
	if root.Generation == 1 {
		if root.CredentialID != root.PrincipalID {
			return errors.New("first identity Root generation must use its principal credential")
		}
		want, err := appV23NextRootHistoryDigest("", 1, root.PrincipalID)
		if err != nil || root.HistoryDigest != want {
			return errors.New("first identity Root history digest does not match its principal")
		}
	}
	for _, marker := range []struct {
		id         string
		generation uint64
	}{{root.PrincipalID, 1}, {root.CredentialID, root.Generation}} {
		item, err := s.txn.Get(appV23RootCredentialKey(marker.id))
		if err != nil {
			return fmt.Errorf("read identity Root generation marker: %w", err)
		}
		if item.ValueSize() != 8 {
			return errors.New("identity Root generation marker must contain exactly8 bytes")
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(value) != 8 || binary.BigEndian.Uint64(value) != marker.generation {
			return errors.New("identity Root generation marker does not match canonical Root")
		}
	}
	if root.CredentialID != root.PrincipalID {
		for _, key := range [][]byte{agentOnChainKey(root.CredentialID), appV23EnrollmentKey(root.CredentialID)} {
			if _, err := s.txn.Get(key); err == nil {
				return errors.New("identity Root credential collides with an ordinary principal")
			} else if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
	}
	return nil
}

func (m memoryIdentityMode) useRaw(memoryID string) bool {
	return m.raw && len(memoryID) > 0 && len(memoryID) <= tx.MaxDerivedFromMemoryIDBytes
}

// Historical non-whitespace IDs above the new admission ceiling intentionally
// retain the old reader. The new-write/public-leaf ceiling is not a read ban.
func (s *BadgerStore) memoryDisclosureForIdentityMode(memoryID string, mode memoryIdentityMode) (*MemoryDisclosureState, error) {
	if mode.useRaw(memoryID) {
		return s.GetMemoryDisclosureStateAppV30(memoryID)
	}
	return s.GetMemoryDisclosureState(memoryID)
}

// withMemoryIdentitySnapshot computes one private mode for the held reader.
// Nested envelope/auth/taint helpers receive that same reader; callers cannot
// choose a version or retain a cache across snapshots.
func (s *BadgerStore) withMemoryIdentitySnapshot(fn func(*BadgerStore, memoryIdentityMode) error) error {
	return s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
		mode, err := reader.memoryIdentityModeInSnapshot()
		if err != nil {
			return err
		}
		return fn(reader, mode)
	})
}
