package store

import (
	"context"
	"crypto/sha256"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// The backfill covers exactly the records that have left proposed, the lookup
// excludes the candidate's own id, and later writes maintain the index inline.
func TestCoCommitTombstoneBackfillLookupAndMaintenance(t *testing.T) {
	source := newTestBadger(t)
	shared := sha256.Sum256([]byte("shared bytes"))
	unique := sha256.Sum256([]byte("unique bytes"))
	other := sha256.Sum256([]byte("other bytes"))
	seed := source.BeginConsensusTransaction(nil)
	require.NoError(t, seed.SetMemoryHash("deprecated-one", shared[:], "deprecated"))
	require.NoError(t, seed.SetMemoryHash("committed-one", other[:], "committed"))
	require.NoError(t, seed.SetMemoryHash("proposed-one", shared[:], "proposed"))
	require.NoError(t, seed.SetMemoryHash("proposed-unique", unique[:], "proposed"))
	require.NoError(t, seed.CommitConsensusTransaction())

	before, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	legacyBefore, err := source.ComputeAppHash()
	require.NoError(t, err)

	// A source with no promotion marker must refuse the lookup entirely: the
	// consensus caller is expected to be gated on the applied fork record.
	_, err = source.CoCommitTombstoned(shared[:], "candidate")
	require.NoError(t, err)

	require.NoError(t, source.PrepareCoCommitTombstoneStage(context.Background(), 100))
	staged, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, staged, "the staged backfill cannot move the app-v13 hash")
	legacyStaged, err := source.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, legacyBefore, legacyStaged, "nor the legacy hash")

	transaction := source.BeginConsensusTransaction(nil)
	require.Error(t, transaction.PromoteCoCommitTombstoneStage(101, before),
		"a promotion whose base height is wrong is refused")
	require.Error(t, transaction.CommitConsensusTransaction())
	transaction.DiscardConsensusTransaction()

	transaction = source.BeginConsensusTransaction(nil)
	require.NoError(t, transaction.PromoteCoCommitTombstoneStage(100, before))
	require.NoError(t, transaction.CommitConsensusTransaction())
	require.NoError(t, source.ValidateCoCommitTombstoneStage())

	tombstoned, err := source.CoCommitTombstoned(shared[:], "candidate")
	require.NoError(t, err)
	require.True(t, tombstoned, "a deprecated record tombstones its exact bytes")
	tombstoned, err = source.CoCommitTombstoned(shared[:], "deprecated-one")
	require.NoError(t, err)
	require.False(t, tombstoned, "the candidate's own id is excluded (idempotent re-send / squat reclaim)")
	tombstoned, err = source.CoCommitTombstoned(other[:], "deprecated-one")
	require.NoError(t, err)
	require.True(t, tombstoned)
	tombstoned, err = source.CoCommitTombstoned(unique[:], "candidate")
	require.NoError(t, err)
	require.False(t, tombstoned, "a proposed record does not tombstone anything")
	_, err = source.CoCommitTombstoned([]byte("short"), "candidate")
	require.Error(t, err, "a malformed hash is refused, not guessed at")

	// A status change AFTER promotion maintains the index inline.
	change := source.BeginConsensusTransaction(nil)
	require.NoError(t, change.SetMemoryStatusPreservingHash("proposed-unique", "challenged"))
	require.NoError(t, change.CommitConsensusTransaction())
	tombstoned, err = source.CoCommitTombstoned(unique[:], "candidate")
	require.NoError(t, err)
	require.True(t, tombstoned, "leaving proposed turns the entry on")

	// Going back to proposed retracts it again.
	change = source.BeginConsensusTransaction(nil)
	require.NoError(t, change.SetMemoryStatusPreservingHash("proposed-unique", "proposed"))
	require.NoError(t, change.CommitConsensusTransaction())
	tombstoned, err = source.CoCommitTombstoned(unique[:], "candidate")
	require.NoError(t, err)
	require.False(t, tombstoned, "a record back in proposed carries no tombstone")

	// A deleted record takes its entry with it. There is no public memory-delete
	// API (records are deprecated, not removed), so this drives the same
	// txnDelete hook a delete would.
	remove := source.BeginConsensusTransaction(nil)
	require.NoError(t, remove.txnDelete(remove.txn, memoryKey("committed-one")))
	require.NoError(t, remove.CommitConsensusTransaction())
	tombstoned, err = source.CoCommitTombstoned(other[:], "candidate")
	require.NoError(t, err)
	require.False(t, tombstoned, "a deleted record no longer tombstones its bytes")
}

// Every pre-promotion write is inert: no index keys exist, so a binary carrying
// the fork cannot move the hash of a block executed before it applied.
func TestCoCommitTombstoneMaintenanceIsInertBeforePromotion(t *testing.T) {
	source := newTestBadger(t)
	hash := sha256.Sum256([]byte("pre-fork bytes"))
	before, err := source.ComputeAppHash()
	require.NoError(t, err)
	write := source.BeginConsensusTransaction(nil)
	require.NoError(t, write.SetMemoryHash("pre-fork", hash[:], "deprecated"))
	require.NoError(t, write.CommitConsensusTransaction())
	after, err := source.ComputeAppHash()
	require.NoError(t, err)
	require.NotEqual(t, before, after)

	count := 0
	require.NoError(t, source.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = coCommitTombstonePrefix
		options.PrefetchValues = false
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(options.Prefix); iterator.ValidForPrefix(options.Prefix); iterator.Next() {
			count++
		}
		return nil
	}))
	require.Zero(t, count, "an unpromoted chain carries no tombstone entries at all")
}

// Boot validation refuses a promoted index whose staged copy was altered.
func TestCoCommitTombstoneValidateRefusesCorruption(t *testing.T) {
	source := newTestBadger(t)
	hash := sha256.Sum256([]byte("corruption bytes"))
	seed := source.BeginConsensusTransaction(nil)
	require.NoError(t, seed.SetMemoryHash("deprecated-one", hash[:], "deprecated"))
	require.NoError(t, seed.CommitConsensusTransaction())
	base, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, source.PrepareCoCommitTombstoneStage(context.Background(), 50))
	transaction := source.BeginConsensusTransaction(nil)
	require.NoError(t, transaction.PromoteCoCommitTombstoneStage(50, base))
	require.NoError(t, transaction.CommitConsensusTransaction())
	require.NoError(t, source.ValidateCoCommitTombstoneStage())

	entry := coCommitTombstoneStageKey(coCommitTombstoneEntryKey(hash[:], "deprecated-one"))
	require.NoError(t, source.db.Update(func(txn *badger.Txn) error {
		return txn.Set(entry, []byte("committed"))
	}))
	require.ErrorIs(t, source.ValidateCoCommitTombstoneStage(), ErrCoCommitTombstoneIndex,
		"a stage whose digest no longer matches its manifest refuses to serve")
}
