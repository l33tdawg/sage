package store

import (
	"context"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/stretchr/testify/require"
)

func TestPublicMemoryMigrationExistingLedgerBoundedAndSourceUntouched(t *testing.T) {
	source := newTestBadger(t)
	transaction := source.BeginConsensusTransaction(nil)
	for index := 0; index < publicMigrationBatch+1; index++ {
		publicTestRecord(t, transaction, fmt.Sprintf("public-%03d", index))
	}
	publicTestRecord(t, transaction, "private")
	require.NoError(t, transaction.SetMemoryClassification("private", 1))
	require.NoError(t, transaction.CommitConsensusTransaction())
	before, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	target := newTestBadger(t)
	result, err := source.BuildPublicMemoryMigration(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, uint64(publicMigrationBatch+1), result.Public)
	require.Equal(t, result.Public+1, result.Scanned)
	require.Equal(t, before, result.SourceAppHash[:])
	after, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
	for index := 0; index < publicMigrationBatch+1; index++ {
		proof, err := target.PublicMemoryBranch(fmt.Sprintf("public-%03d", index))
		require.NoError(t, err)
		require.NoError(t, VerifyPublicMemoryBranch(*proof, result.Root))
	}
	_, err = target.PublicMemoryBranch("private")
	require.Error(t, err)
	_, err = source.BuildPublicMemoryMigration(context.Background(), target)
	require.Error(t, err)
}

func TestPublicMemoryMigrationCancellationAndBadSourceNeverComplete(t *testing.T) {
	source := newTestBadger(t)
	target := newTestBadger(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := source.BuildPublicMemoryMigration(ctx, target)
	require.ErrorIs(t, err, context.Canceled)
	_, err = source.BuildPublicMemoryMigration(context.Background(), source)
	require.Error(t, err)
	transaction := source.BeginConsensusTransaction(nil)
	for index := 0; index < publicMigrationBatch+1; index++ {
		publicTestRecord(t, transaction, fmt.Sprintf("public-%03d", index))
	}
	publicTestRecord(t, transaction, "zz-incomplete")
	require.NoError(t, transaction.SetMemoryAuthorPrincipal("zz-incomplete", ""))
	require.NoError(t, transaction.CommitConsensusTransaction())
	before, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	_, err = source.BuildPublicMemoryMigration(context.Background(), target)
	require.Error(t, err)
	require.NoError(t, target.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(publicIndexPrefix + "migration-complete"))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	after, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, err = source.BuildPublicMemoryMigration(context.Background(), target)
	require.Error(t, err)
}

// TestPublicMemoryMigrationQuarantinesLegacyHashless covers the app-v28
// activation blocker found on a real chain on 2026-09-19: 53 legacy PUBLIC
// records whose historical lifecycle transitions erased the canonical content
// hash made the migration unbuildable, and because the stage is prepared
// outside the consensus transaction the activation block failed on every
// replay, leaving the node unable to start at all.
//
// The fix quarantines exactly the pre-app-v25 class and keeps every other
// inconsistency fatal, so this test asserts both halves: a historical hashless
// record is folded out of the committed set (and its hash-only sibling is still
// committed), while a hashless record carrying an app-v25 submission-height
// marker still refuses the build.
func TestPublicMemoryMigrationQuarantinesLegacyHashless(t *testing.T) {
	source := newTestBadger(t)
	transaction := source.BeginConsensusTransaction(nil)
	publicTestRecord(t, transaction, "legacy-hashless")
	require.NoError(t, transaction.SetMemoryHash("legacy-hashless", nil, "committed"))
	// The real chain's records also predate the app-v23 principal projection.
	require.NoError(t, transaction.update(func(txn *badger.Txn) error {
		return transaction.txnDelete(txn, memoryAuthorPrincipalKey("legacy-hashless"))
	}))
	publicTestRecord(t, transaction, "hashed")
	publicTestRecord(t, transaction, "modern-hashless")
	require.NoError(t, transaction.SetMemorySubmissionHeight("modern-hashless", 56126))
	require.NoError(t, transaction.SetMemoryHash("modern-hashless", nil, "committed"))
	require.NoError(t, transaction.CommitConsensusTransaction())

	// The app-v25-or-later hashless record is still a hard inconsistency, so the
	// migration must refuse before it can publish anything.
	target := newTestBadger(t)
	_, err := source.BuildPublicMemoryMigration(context.Background(), target)
	require.ErrorIs(t, err, ErrPublicMemoryIndex)
	require.NoError(t, target.db.View(func(txn *badger.Txn) error {
		_, getErr := txn.Get([]byte(publicIndexPrefix + "migration-complete"))
		require.ErrorIs(t, getErr, badger.ErrKeyNotFound)
		return nil
	}))

	// Drop the marker: now the record is historical, and the migration must build
	// over the remaining public set with the hashless records quarantined.
	require.NoError(t, source.update(func(txn *badger.Txn) error {
		return source.txnDelete(txn, memorySubmissionHeightKey("modern-hashless"))
	}))
	clean := newTestBadger(t)
	result, err := source.BuildPublicMemoryMigration(context.Background(), clean)
	require.NoError(t, err)
	require.Equal(t, uint64(3), result.Scanned)
	require.Equal(t, uint64(1), result.Public)

	proof, err := clean.PublicMemoryBranch("hashed")
	require.NoError(t, err)
	require.NoError(t, VerifyPublicMemoryBranch(*proof, result.Root))
	// A quarantined record is outside the committed public set, so the migrated
	// index holds no projection for it and therefore no proof.
	for _, quarantined := range []string{"legacy-hashless", "modern-hashless"} {
		_, branchErr := clean.PublicMemoryBranch(quarantined)
		require.ErrorIs(t, branchErr, ErrMemoryDisclosureNotFound)
	}
	// Quarantine is a commitment decision, never a delete: the canonical record
	// stays readable in the source store.
	for _, retained := range []string{"legacy-hashless", "modern-hashless"} {
		state, stateErr := source.GetMemoryDisclosureState(retained)
		require.NoError(t, stateErr)
		require.Empty(t, state.ContentHash)
	}
}

type publicMigrationContext struct {
	context.Context
	checks int
	mutate func()
}

func (ctx *publicMigrationContext) Err() error {
	ctx.checks++
	if ctx.checks == 4 {
		ctx.mutate()
	}
	return nil
}

func TestPublicMemoryMigrationUsesOneSnapshotAcrossSourceCommit(t *testing.T) {
	source := newTestBadger(t)
	transaction := source.BeginConsensusTransaction(nil)
	for index := 0; index < publicMigrationBatch+1; index++ {
		publicTestRecord(t, transaction, fmt.Sprintf("public-%03d", index))
	}
	require.NoError(t, transaction.CommitConsensusTransaction())
	before, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	ctx := &publicMigrationContext{Context: context.Background(), mutate: func() {
		require.NoError(t, source.SetMemoryStatusPreservingHash("public-032", "challenged"))
	}}
	target := newTestBadger(t)
	result, err := source.BuildPublicMemoryMigration(ctx, target)
	require.NoError(t, err)
	require.Equal(t, before, result.SourceAppHash[:])
	proof, err := target.PublicMemoryBranch("public-032")
	require.NoError(t, err)
	require.Equal(t, "committed", proof.Leaf.Status)
	current, err := source.GetMemoryDisclosureState("public-032")
	require.NoError(t, err)
	require.Equal(t, "challenged", current.Status)
}
