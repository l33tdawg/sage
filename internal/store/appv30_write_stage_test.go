package store

import (
	"crypto/sha256"
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30WriteStageCommitsWithinReservation(t *testing.T) {
	base := openTaintStore(t)
	scoped := base.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)

	require.NoError(t, scoped.BeginAppV30WriteStage(1<<20))
	require.NoError(t, scoped.SetState("stage-success", []byte("kept")))
	decisionID := testTaintDecisionID("stage-success")
	created, err := scoped.ApplyTaintDecision(
		"sage",
		decisionID,
		testTaintParent("stage-success-target"),
		testTaintPayloadHash("stage-success"),
	)
	require.NoError(t, err)
	require.True(t, created)

	stats, err := scoped.CommitAppV30WriteStage()
	require.NoError(t, err)
	require.Positive(t, stats.Generated)
	require.LessOrEqual(t, stats.Generated, stats.Limit)
	require.False(t, stats.Overflowed)
	require.NoError(t, scoped.CommitConsensusTransaction())

	state, err := base.GetState("stage-success")
	require.NoError(t, err)
	require.Equal(t, []byte("kept"), state)
	header, found, err := base.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.NotZero(t, header)
}

func TestAppV30WriteStageOverflowRollsBackEveryTxWrite(t *testing.T) {
	base := openTaintStore(t)
	scoped := base.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)

	require.NoError(t, scoped.BeginAppV30WriteStage(200))
	require.NoError(t, scoped.SetState("stage-overflow", []byte("discard")))
	decisionID := testTaintDecisionID("stage-overflow")
	_, err := scoped.ApplyTaintDecision(
		"sage",
		decisionID,
		testTaintParent("stage-overflow-target"),
		testTaintPayloadHash("stage-overflow"),
	)
	require.ErrorIs(t, err, ErrAppV30WriteReservationExceeded)
	require.True(t, scoped.AppV30WriteStageOverflowed())

	require.NoError(t, scoped.AbortAppV30WriteStage())
	require.NoError(t, scoped.ConsensusTransactionError())
	state, err := scoped.GetState("stage-overflow")
	require.NoError(t, err)
	require.Nil(t, state)
	_, found, err := scoped.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.False(t, found)
	require.NoError(t, scoped.SetState("stage-after-overflow", []byte("commits")))
	require.NoError(t, scoped.CommitConsensusTransaction())

	state, err = base.GetState("stage-overflow")
	require.NoError(t, err)
	require.Nil(t, state)
	_, found, err = base.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.False(t, found)
	state, err = base.GetState("stage-after-overflow")
	require.NoError(t, err)
	require.Equal(t, []byte("commits"), state)
}

func TestAppV30WriteStageRequiresConsensusTransactionAndPositiveReservation(t *testing.T) {
	base := openTaintStore(t)
	require.ErrorContains(t, base.BeginAppV30WriteStage(1), "consensus transaction")

	scoped := base.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	require.ErrorContains(t, scoped.BeginAppV30WriteStage(0), "must be positive")
	require.NoError(t, scoped.BeginAppV30WriteStage(1))
	require.ErrorContains(t, scoped.BeginAppV30WriteStage(1), "nested")
	require.ErrorContains(t, scoped.CommitConsensusTransaction(), "active app-v30 write stage")
	require.NoError(t, scoped.AbortAppV30WriteStage())
}

func TestAppV30WriteStageRollsBackNestedCoCommitTombstoneWrites(t *testing.T) {
	base := openTaintStore(t)
	require.NoError(t, base.db.Update(func(txn *badger.Txn) error {
		return txn.Set(coCommitTombstonePromoted, []byte{1})
	}))

	scoped := base.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	require.NoError(t, scoped.BeginAppV30WriteStage(200))

	contentHash := sha256.Sum256([]byte("rollback tombstone"))
	require.NoError(t, scoped.SetMemoryHash(
		"stage-tombstone-memory",
		contentHash[:],
		"committed",
	))
	tombstoneKey := coCommitTombstoneEntryKey(contentHash[:], "stage-tombstone-memory")
	_, found, err := scoped.readTaintValueTxnForTest(scoped.txn, tombstoneKey)
	require.NoError(t, err)
	require.True(t, found)

	_, err = scoped.ApplyTaintDecision(
		"sage",
		testTaintDecisionID("stage-tombstone-overflow"),
		testTaintParent("stage-tombstone-target"),
		testTaintPayloadHash("stage-tombstone-overflow"),
	)
	require.ErrorIs(t, err, ErrAppV30WriteReservationExceeded)
	require.NoError(t, scoped.AbortAppV30WriteStage())

	_, found, err = scoped.readTaintValueTxnForTest(scoped.txn, tombstoneKey)
	require.NoError(t, err)
	require.False(t, found)
}

func TestAppV30WriteStageChargesCanonicalGeneratedKeys(t *testing.T) {
	decisionID := testTaintDecisionID("stage-key-accounting")
	parent := testTaintParent("parent:with-delimiter")
	parentID, err := tx.ParentRefIDHex(parent)
	require.NoError(t, err)
	keys := []struct {
		name string
		key  []byte
	}{
		{"decision", taint.DecisionKey(decisionID)},
		{"target-anchor", taint.TargetKey(decisionID)},
		{"membership", taint.MembershipKey("child:with-delimiter", decisionID)},
		{"decision-index", taint.DecisionIndexKey(decisionID, "child:with-delimiter")},
		{"frontier", taint.FrontierKey(decisionID, 1, 0)},
		{"revision", taint.RevisionKey()},
		{"active-count", taint.ActiveCountKey()},
		{"provenance", AppV30ProvenanceKey("child:with-delimiter")},
		{"forward-lineage", lineage.ForwardKey("child:with-delimiter", parentID)},
		{"reverse-lineage", lineage.ReverseKey(parentID, "child:with-delimiter")},
	}
	base := openTaintStore(t)
	for _, item := range keys {
		t.Run(item.name, func(t *testing.T) {
			scoped := base.BeginConsensusTransaction(nil)
			t.Cleanup(scoped.DiscardConsensusTransaction)
			value := []byte("generated-value")
			setCost := int64(len(item.key) + len(value))
			deleteCost := int64(len(item.key)) + appV30TombstoneBytes
			require.NoError(t, scoped.BeginAppV30WriteStage(setCost+deleteCost))
			require.NoError(t, scoped.update(func(txn *badger.Txn) error {
				return scoped.txnSet(txn, item.key, value)
			}))
			require.Equal(t, setCost, scoped.AppV30WriteStageStats().Generated)
			require.NoError(t, scoped.update(func(txn *badger.Txn) error {
				return scoped.txnDelete(txn, item.key)
			}))
			stats, err := scoped.CommitAppV30WriteStage()
			require.NoError(t, err)
			require.Equal(t, setCost+deleteCost, stats.Generated)
		})
	}
}

func TestAppV30WriteStageApplyReservationIncludesTargetAnchor(t *testing.T) {
	base := openTaintStore(t)
	decisionID := testTaintDecisionID("stage-target-anchor")
	target := testTaintParent("stage-target-anchor-memory")
	payloadHash := testTaintPayloadHash("stage-target-anchor")
	keys := [][]byte{
		taint.DecisionKey(decisionID),
		taint.TargetKey(decisionID),
		TaintDecisionScopeKey(decisionID),
		taint.ActiveDecisionKey(decisionID),
		taint.MembershipKey(target.MemoryID, decisionID),
		taint.DecisionIndexKey(decisionID, target.MemoryID),
		taint.FrontierKey(decisionID, 1, 0),
		taint.ActiveCountKey(),
		taint.RevisionKey(),
	}
	reference := base.BeginConsensusTransaction(nil)
	t.Cleanup(reference.DiscardConsensusTransaction)
	created, err := reference.ApplyTaintDecision("sage", decisionID, target, payloadHash)
	require.NoError(t, err)
	require.True(t, created)
	var expected int64
	for _, key := range keys {
		value, found, err := reference.readTaintValueTxnForTest(reference.txn, key)
		require.NoError(t, err)
		require.True(t, found)
		expected += int64(len(key) + len(value))
	}
	reference.DiscardConsensusTransaction()

	t.Run("exact-reservation", func(t *testing.T) {
		scoped := base.BeginConsensusTransaction(nil)
		t.Cleanup(scoped.DiscardConsensusTransaction)
		require.NoError(t, scoped.BeginAppV30WriteStage(expected))
		created, err := scoped.ApplyTaintDecision("sage", decisionID, target, payloadHash)
		require.NoError(t, err)
		require.True(t, created)
		stats, err := scoped.CommitAppV30WriteStage()
		require.NoError(t, err)
		require.Equal(t, expected, stats.Generated)
	})
	t.Run("one-byte-short-rolls-back", func(t *testing.T) {
		scoped := base.BeginConsensusTransaction(nil)
		t.Cleanup(scoped.DiscardConsensusTransaction)
		require.NoError(t, scoped.BeginAppV30WriteStage(expected-1))
		_, err := scoped.ApplyTaintDecision("sage", decisionID, target, payloadHash)
		require.ErrorIs(t, err, ErrAppV30WriteReservationExceeded)
		require.True(t, scoped.AppV30WriteStageOverflowed())
		require.NoError(t, scoped.AbortAppV30WriteStage())
		require.NoError(t, scoped.ConsensusTransactionError())
		for _, key := range keys {
			_, found, err := scoped.readTaintValueTxnForTest(scoped.txn, key)
			require.NoError(t, err)
			require.False(t, found, "rejected apply retained key %x", key)
		}
	})
}

func TestAppV30WriteStageRestoresBeforeImagesAndNestedIndexes(t *testing.T) {
	base := openTaintStore(t)
	require.NoError(t, base.db.Update(func(txn *badger.Txn) error {
		return txn.Set(coCommitTombstonePromoted, []byte{1})
	}))
	oldHash := sha256.Sum256([]byte("existing-memory"))
	newHash := sha256.Sum256([]byte("rejected-memory"))
	const memoryID = "stage-existing-memory"
	require.NoError(t, base.SetMemoryHash(memoryID, oldHash[:], "committed"))
	scoped := base.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	require.NoError(t, scoped.SetState("stage-prior-tx", []byte("earlier")))
	require.NoError(t, scoped.SetMemoryHash("stage-earlier-memory", oldHash[:], "committed"))
	before := stageRawSnapshotForTest(t, scoped)
	beforePublic := cloneStringSet(scoped.publicMemoryChanged)
	beforeProjection := scoped.canonicalMemoryProjectionMutated

	require.NoError(t, scoped.BeginAppV30WriteStage(200))
	require.NoError(t, scoped.SetState("stage-prior-tx", []byte("overwrite")))
	require.NoError(t, scoped.DeleteState("stage-prior-tx"))
	require.NoError(t, scoped.SetState("stage-prior-tx", []byte("overwrite-again")))
	require.NoError(t, scoped.SetMemoryHash(memoryID, oldHash[:], "proposed"))
	require.NoError(t, scoped.SetMemoryHash(memoryID, newHash[:], "committed"))
	_, err := scoped.ApplyTaintDecision("sage", testTaintDecisionID("reject-repeated-writes"),
		testTaintParent("rejected-target"), testTaintPayloadHash("reject-repeated-writes"))
	require.ErrorIs(t, err, ErrAppV30WriteReservationExceeded)
	require.NoError(t, scoped.AbortAppV30WriteStage())
	require.NoError(t, scoped.ConsensusTransactionError())
	require.Equal(t, before, stageRawSnapshotForTest(t, scoped))
	require.Equal(t, beforePublic, scoped.publicMemoryChanged)
	require.Equal(t, beforeProjection, scoped.canonicalMemoryProjectionMutated)
	require.NoError(t, scoped.SetState("stage-later-tx", []byte("accepted")))
	require.NoError(t, scoped.CommitConsensusTransaction())
	value, err := base.GetState("stage-prior-tx")
	require.NoError(t, err)
	require.Equal(t, []byte("earlier"), value)
	value, err = base.GetState("stage-later-tx")
	require.NoError(t, err)
	require.Equal(t, []byte("accepted"), value)
}

func TestAppV30WriteStageRestoresTaintWorkAfterOverflow(t *testing.T) {
	for _, operation := range []string{"expand", "cleanup"} {
		t.Run(operation, func(t *testing.T) {
			base := openTaintStore(t)
			target := testTaintParent("stage-work-target")
			require.NoError(t, base.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
			seedTaintChild(t, base, target, "stage-work-child")
			decisionID := testTaintDecisionID("stage-work-" + operation)
			_, err := base.ApplyTaintDecision("sage", decisionID, target, testTaintPayloadHash(decisionID))
			require.NoError(t, err)
			firstDeletedKey := taint.FrontierKey(decisionID, 1, 0)
			if operation == "cleanup" {
				_, err = base.ClearTaintDecision(decisionID)
				require.NoError(t, err)
				firstDeletedKey = taint.DecisionIndexKey(decisionID, target.MemoryID)
			}
			scoped := base.BeginConsensusTransaction(nil)
			t.Cleanup(scoped.DiscardConsensusTransaction)
			require.NoError(t, scoped.SetState("stage-prior-work", []byte("earlier")))
			before := stageRawSnapshotForTest(t, scoped)
			run := func() error {
				if operation == "expand" {
					_, err := scoped.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
					return err
				}
				_, err := scoped.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
				return err
			}
			// Admit the first queue/index deletion, then reject the next generated
			// mutation. This exercises rollback after work has already begun.
			require.NoError(t, scoped.BeginAppV30WriteStage(int64(len(firstDeletedKey))+appV30TombstoneBytes))
			require.ErrorIs(t, run(), ErrAppV30WriteReservationExceeded)
			_, found, err := scoped.readTaintValueTxnForTest(scoped.txn, firstDeletedKey)
			require.NoError(t, err)
			require.False(t, found)
			require.NoError(t, scoped.AbortAppV30WriteStage())
			require.NoError(t, scoped.ConsensusTransactionError())
			require.Equal(t, before, stageRawSnapshotForTest(t, scoped))
			require.NoError(t, scoped.BeginAppV30WriteStage(1<<20))
			require.NoError(t, run())
			_, err = scoped.CommitAppV30WriteStage()
			require.NoError(t, err)
			after := stageRawSnapshotForTest(t, scoped)
			require.NotEqual(t, before, after)
			require.NoError(t, scoped.CommitConsensusTransaction())
			require.Equal(t, after, stageRawSnapshotForTest(t, base))
		})
	}
}

func stageRawSnapshotForTest(t *testing.T, s *BadgerStore) map[string][]byte {
	t.Helper()
	rows := make(map[string][]byte)
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			rows[string(item.KeyCopy(nil))] = value
		}
		return nil
	}))
	return rows
}

func (s *BadgerStore) readTaintValueTxnForTest(
	txn *badger.Txn,
	key []byte,
) ([]byte, bool, error) {
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	value, err := item.ValueCopy(nil)
	return value, true, err
}
