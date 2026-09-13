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
