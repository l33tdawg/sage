package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestTaintTargetAnchorCodecAndFootprint(t *testing.T) {
	for _, id := range []string{"target", "target:part", "target\x00part", string([]byte{0xff, 0, ':', 0x80}), strings.Repeat("x", 512)} {
		anchor := taintTargetAnchor{Target: testTaintParent(id), PayloadHash: testTaintPayloadHash("codec")}
		raw, err := encodeTaintTargetAnchor(anchor)
		require.NoError(t, err)
		decoded, err := decodeTaintTargetAnchor(raw)
		require.NoError(t, err)
		require.Equal(t, anchor, decoded)
	}
	maximum := taintTargetAnchor{Target: testTaintParent(strings.Repeat("x", 512))}
	maximum.Target.SourceChainID = strings.Repeat("s", 64)
	raw, err := encodeTaintTargetAnchor(maximum)
	require.NoError(t, err)
	require.Len(t, raw, 645)
	key := taint.TargetKey(testTaintDecisionID("maximum"))
	require.Len(t, key, 76)
	require.Equal(t, 721, len(key)+len(raw), "unframed record bytes; not a transaction-budget proof")
	_, err = decodeTaintTargetAnchor(raw)
	require.NoError(t, err)

	valid, err := encodeTaintTargetAnchor(taintTargetAnchor{Target: testTaintParent("target")})
	require.NoError(t, err)
	badVersion := bytes.Clone(valid)
	badVersion[0]++
	zeroChain := bytes.Clone(valid)
	zeroChain[1], zeroChain[2] = 0, 0
	badLength := bytes.Clone(valid)
	badLength[1], badLength[2] = 0xff, 0xff
	for _, invalid := range [][]byte{nil, {1}, badVersion, zeroChain, badLength, valid[:len(valid)-1], append(bytes.Clone(valid), 0), make([]byte, 646)} {
		_, err := decodeTaintTargetAnchor(invalid)
		require.ErrorIs(t, err, ErrTaintStateMalformed)
	}
	for _, target := range []tx.ParentRef{testTaintParent(""), testTaintParent(strings.Repeat("x", 513)), {MemoryID: "target"}} {
		_, err := encodeTaintTargetAnchor(taintTargetAnchor{Target: target})
		require.ErrorIs(t, err, ErrTaintStateMalformed)
	}
}

func TestTaintTargetRecoveryRejectsDamage(t *testing.T) {
	for _, damage := range []string{"missing anchor", "malformed anchor", "payload mismatch", "missing membership", "missing index", "nonempty index", "stale membership", "malformed membership", "malformed anchor key"} {
		t.Run(damage, func(t *testing.T) {
			s := openTaintStore(t)
			id, target := seedValidTaintState(t, s)
			require.NoError(t, s.ValidateTaintState())
			require.NoError(t, s.update(func(txn *badger.Txn) error {
				switch damage {
				case "missing anchor":
					return s.txnDelete(txn, taint.TargetKey(id))
				case "malformed anchor":
					return s.txnSet(txn, taint.TargetKey(id), []byte{2})
				case "payload mismatch":
					raw := readTaintValue(t, s, taint.TargetKey(id))
					raw[len(raw)-1] ^= 1
					return s.txnSet(txn, taint.TargetKey(id), raw)
				case "missing membership":
					return s.txnDelete(txn, taint.MembershipKey(target.MemoryID, id))
				case "missing index":
					return s.txnDelete(txn, taint.DecisionIndexKey(id, target.MemoryID))
				case "nonempty index":
					return s.txnSet(txn, taint.DecisionIndexKey(id, target.MemoryID), []byte{1})
				case "stale membership":
					raw, err := taint.EncodeMembership(taint.Membership{Generation: 2, Origin: taint.OriginTarget})
					if err != nil {
						return err
					}
					return s.txnSet(txn, taint.MembershipKey(target.MemoryID, id), raw)
				case "malformed membership":
					return s.txnSet(txn, taint.MembershipKey(target.MemoryID, id), []byte{1})
				default:
					return s.txnSet(txn, taint.TargetKey(strings.ToUpper(id)), readTaintValue(t, s, taint.TargetKey(id)))
				}
			}))
			require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
		})
	}
	t.Run("orphan anchor", func(t *testing.T) {
		s := openTaintStore(t)
		raw, err := encodeTaintTargetAnchor(taintTargetAnchor{Target: testTaintParent("orphan")})
		require.NoError(t, err)
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return s.txnSet(txn, taint.TargetKey(testTaintDecisionID("orphan")), raw)
		}))
		require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
	})
}

func cleanTaintTargetForTest(t *testing.T, s *BadgerStore, id string) {
	t.Helper()
	_, err := s.ClearTaintDecision(id)
	require.NoError(t, err)
	for range 4 {
		result, err := s.CleanupTaintDecision(id, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		if result.Status == taint.StatusClean {
			return
		}
	}
	t.Fatal("one-target cleanup did not complete within four batches")
}

func TestTaintTargetLifecycleAndExactReplay(t *testing.T) {
	for _, status := range []taint.Status{taint.StatusExpanding, taint.StatusComplete, taint.StatusCleaning, taint.StatusClean} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			s := openTaintStore(t)
			id, target := seedValidTaintState(t, s)
			anchor := readTaintValue(t, s, taint.TargetKey(id))
			switch status {
			case taint.StatusComplete:
				_, err := s.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
			case taint.StatusCleaning:
				_, err := s.ClearTaintDecision(id)
				require.NoError(t, err)
			case taint.StatusClean:
				cleanTaintTargetForTest(t, s, id)
			}
			require.Equal(t, anchor, readTaintValue(t, s, taint.TargetKey(id)))
			require.NoError(t, s.ValidateTaintState())
			before, err := s.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			revision := s.CanonicalMemoryProjectionRevision()
			created, err := s.ApplyTaintDecision("sage", id, target, testTaintPayloadHash("recovery"))
			require.NoError(t, err)
			require.False(t, created)
			for _, kind := range []string{"memory", "content", "valid different chain", "payload"} {
				changed := target
				chain := "sage"
				payload := testTaintPayloadHash("recovery")
				switch kind {
				case "memory":
					changed.MemoryID = "other"
				case "content":
					changed.ContentHash[0] ^= 1
				case "valid different chain":
					// Incoming local/target chain arguments agree, but differ from the anchor.
					chain, changed.SourceChainID = "other", "other"
				case "payload":
					payload[0] ^= 1
				}
				created, err = s.ApplyTaintDecision(chain, id, changed, payload)
				require.ErrorIs(t, err, ErrTaintConflict, kind)
				require.False(t, created)
			}
			after, err := s.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, revision, s.CanonicalMemoryProjectionRevision())
			header, found, err := s.GetTaintDecision(id)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, status, header.Status)
		})
	}
}

func TestTaintTargetReplayCannotRepairMissingOrOrphanAnchor(t *testing.T) {
	for _, damage := range []string{"missing", "malformed", "inconsistent", "orphan"} {
		t.Run(damage, func(t *testing.T) {
			s := openTaintStore(t)
			id, target := seedValidTaintState(t, s)
			require.NoError(t, s.update(func(txn *badger.Txn) error {
				switch damage {
				case "missing":
					return s.txnDelete(txn, taint.TargetKey(id))
				case "malformed":
					return s.txnSet(txn, taint.TargetKey(id), []byte{2})
				case "inconsistent":
					raw := readTaintValue(t, s, taint.TargetKey(id))
					raw[len(raw)-1] ^= 1
					return s.txnSet(txn, taint.TargetKey(id), raw)
				default:
					return s.txnDelete(txn, taint.DecisionKey(id))
				}
			}))
			before, err := s.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			revision := s.CanonicalMemoryProjectionRevision()
			created, err := s.ApplyTaintDecision("sage", id, target, testTaintPayloadHash("recovery"))
			require.ErrorIs(t, err, ErrTaintStateMalformed)
			require.False(t, created)
			after, err := s.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, revision, s.CanonicalMemoryProjectionRevision())
		})
	}
}

func TestTaintTargetRecoveryIgnoresMutableOriginAndRetainsCleaning(t *testing.T) {
	s := openTaintStore(t)
	id, target := seedValidTaintState(t, s)
	anchor := readTaintValue(t, s, taint.TargetKey(id))
	_, err := s.InheritTaintMemberships("sage", "child", []tx.ParentRef{target})
	require.NoError(t, err)
	_, err = s.InheritTaintMemberships("sage", target.MemoryID, []tx.ParentRef{testTaintParent("child")})
	require.NoError(t, err)
	membership, err := taint.DecodeMembership(readTaintValue(t, s, taint.MembershipKey(target.MemoryID, id)))
	require.NoError(t, err)
	require.Equal(t, taint.OriginAdmit, membership.Origin)
	require.NoError(t, s.ValidateTaintState())
	for _, status := range []taint.Status{taint.StatusDepthExhausted, taint.StatusWorkExhausted} {
		header, _, err := s.GetTaintDecision(id)
		require.NoError(t, err)
		header.Status = status
		raw, err := taint.EncodeHeader(header)
		require.NoError(t, err)
		require.NoError(t, s.update(func(txn *badger.Txn) error { return s.txnSet(txn, taint.DecisionKey(id), raw) }))
		require.NoError(t, s.ValidateTaintState())
	}
	_, err = s.ClearTaintDecision(id)
	require.NoError(t, err)
	// Legitimate partial cleanup can already have removed the direct target pair.
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		if err := s.txnDelete(txn, taint.MembershipKey(target.MemoryID, id)); err != nil {
			return err
		}
		return s.txnDelete(txn, taint.DecisionIndexKey(id, target.MemoryID))
	}))
	require.NoError(t, s.ValidateTaintState())
	cleanTaintTargetForTest(t, s, id)
	require.Equal(t, anchor, readTaintValue(t, s, taint.TargetKey(id)))
	require.NoError(t, s.ValidateTaintState())
}

func TestTaintTargetAtomicVisibilityAndWriteFailures(t *testing.T) {
	s := openTaintStore(t)
	id := testTaintDecisionID("atomic")
	target := testTaintParent("atomic-target")
	payload := testTaintPayloadHash("atomic")
	foundIn := func(reader *BadgerStore) bool {
		t.Helper()
		var found bool
		require.NoError(t, reader.view(func(txn *badger.Txn) error {
			_, foundValue, err := readTaintTargetTxn(txn, id)
			found = foundValue
			return err
		}))
		return found
	}
	scoped := s.BeginConsensusTransaction(nil)
	created, err := scoped.ApplyTaintDecision("sage", id, target, payload)
	require.NoError(t, err)
	require.True(t, created)
	require.True(t, foundIn(scoped))
	require.False(t, foundIn(s))
	scoped.DiscardConsensusTransaction()
	require.False(t, foundIn(s))
	for failAt := 1; failAt <= 7; failAt++ {
		scoped = s.BeginConsensusTransaction(nil)
		injected := errors.New("injected anchor transaction failure")
		scoped.writeFaultHook = func(attempt int) error {
			if attempt == failAt {
				return injected
			}
			return nil
		}
		_, err = scoped.ApplyTaintDecision("sage", id, target, payload)
		require.ErrorIs(t, err, injected)
		require.Error(t, scoped.CommitConsensusTransaction())
		require.False(t, foundIn(s))
		_, found, err := s.GetTaintDecision(id)
		require.NoError(t, err)
		require.False(t, found)
		require.NoError(t, s.ValidateTaintState())
	}
	scoped = s.BeginConsensusTransaction(nil)
	_, err = scoped.ApplyTaintDecision("sage", id, target, payload)
	require.NoError(t, err)
	require.Equal(t, 7, scoped.writeAttempts)
	revision := s.CanonicalMemoryProjectionRevision()
	require.NoError(t, scoped.CommitConsensusTransaction())
	require.True(t, foundIn(s))
	require.Greater(t, s.CanonicalMemoryProjectionRevision(), revision)
	require.NoError(t, s.ValidateTaintState())
}

func TestTaintTargetAnchorAffectsHashAndProjectionRevision(t *testing.T) {
	s := openTaintStore(t)
	index := s.BeginConsensusTransaction(nil)
	require.NoError(t, index.InitializeEmptyPublicMemoryIndex())
	require.NoError(t, index.CommitConsensusTransaction())
	id := testTaintDecisionID("hash")
	key := taint.TargetKey(id)
	raw, err := encodeTaintTargetAnchor(taintTargetAnchor{Target: testTaintParent("target")})
	require.NoError(t, err)
	require.True(t, isCanonicalMemoryProjectionKey(key))
	for _, compute := range []func() ([]byte, error){s.ComputeAppHash, s.ComputeAppHashExcludingBookkeeping, s.ComputePublicMemoryAppHash} {
		before, err := compute()
		require.NoError(t, err)
		revision := s.CanonicalMemoryProjectionRevision()
		require.NoError(t, s.update(func(txn *badger.Txn) error { return s.txnSet(txn, key, raw) }))
		inserted, err := compute()
		require.NoError(t, err)
		require.NotEqual(t, before, inserted)
		require.Greater(t, s.CanonicalMemoryProjectionRevision(), revision)
		changed := bytes.Clone(raw)
		changed[len(changed)-1] ^= 1
		require.NoError(t, s.update(func(txn *badger.Txn) error { return s.txnSet(txn, key, changed) }))
		modified, err := compute()
		require.NoError(t, err)
		require.NotEqual(t, inserted, modified)
		require.NoError(t, s.update(func(txn *badger.Txn) error { return s.txnDelete(txn, key) }))
		after, err := compute()
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
}

func TestTaintTargetSnapshotAndCanonicalTransfer(t *testing.T) {
	s := openTaintStore(t)
	index := s.BeginConsensusTransaction(nil)
	require.NoError(t, index.InitializeEmptyPublicMemoryIndex())
	require.NoError(t, index.CommitConsensusTransaction())
	id, target := seedValidTaintState(t, s)
	raw := readTaintValue(t, s, taint.TargetKey(id))
	canonicalComposite, err := s.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	// Canonical Internet transfer deliberately excludes local and unpromoted stage keys.
	excluded := [][]byte{[]byte(consensuskeys.AgentOrgsIndexBackfillProgress), append(bytes.Clone(consensuskeys.AppV23MigrationStagePrefix), "probe"...)}
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		for _, key := range excluded {
			if err := s.txnSet(txn, key, []byte("local-only")); err != nil {
				return err
			}
		}
		return nil
	}))
	wantHash, err := s.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	wantComposite, err := s.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	for _, canonical := range []bool{false, true} {
		t.Run(fmt.Sprint(canonical), func(t *testing.T) {
			var buffer bytes.Buffer
			if canonical {
				require.NoError(t, statesync.WriteCanonicalState(context.Background(), s.db, &buffer))
			} else {
				_, err := s.db.Backup(&buffer, 0)
				require.NoError(t, err)
			}
			db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			if canonical {
				require.NoError(t, statesync.RestoreCanonicalState(context.Background(), &buffer, db))
			} else {
				require.NoError(t, db.Load(&buffer, 16))
			}
			restored := &BadgerStore{db: db}
			require.Equal(t, raw, readTaintValue(t, restored, taint.TargetKey(id)))
			require.NoError(t, restored.ValidateTaintState())
			hash, err := restored.ComputeAppHashExcludingBookkeeping()
			require.NoError(t, err)
			require.Equal(t, wantHash, hash)
			composite, err := restored.ComputePublicMemoryAppHash()
			require.NoError(t, err)
			if canonical {
				require.Equal(t, canonicalComposite, composite)
			} else {
				require.Equal(t, wantComposite, composite)
			}
			require.NoError(t, restored.view(func(txn *badger.Txn) error {
				for _, key := range excluded {
					found, err := keyExistsTxn(txn, key)
					require.NoError(t, err)
					require.Equal(t, !canonical, found)
				}
				return nil
			}))
			created, err := restored.ApplyTaintDecision("sage", id, target, testTaintPayloadHash("recovery"))
			require.NoError(t, err)
			require.False(t, created)
			require.NoError(t, restored.update(func(txn *badger.Txn) error { return restored.txnDelete(txn, taint.TargetKey(id)) }))
			require.ErrorIs(t, restored.ValidateTaintState(), ErrTaintStateMalformed)
		})
	}
}
