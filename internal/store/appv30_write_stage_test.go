package store

import (
	"crypto/sha256"
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
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
