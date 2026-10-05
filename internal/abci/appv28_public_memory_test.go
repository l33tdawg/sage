package abci

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

// publicMemoryTestRecord seeds one committed PUBLIC=0 record directly in the
// store, which is what the index classifies as part of the public set.
func publicMemoryTestRecord(t *testing.T, bs *store.BadgerStore, identifier string) {
	t.Helper()
	publicMemoryTestRecordWithStatus(t, bs, identifier, "committed")
}

// publicMemoryTestRecordWithStatus seeds the full projection set a PUBLIC=0
// record needs (the leaf builder refuses a partial one) at any lifecycle status.
func publicMemoryTestRecordWithStatus(t *testing.T, bs *store.BadgerStore, identifier, status string) {
	t.Helper()
	hash := sha256.Sum256([]byte("app-v28:" + identifier))
	require.NoError(t, bs.SetMemoryHash(identifier, hash[:], status))
	require.NoError(t, bs.SetMemoryAuthor(identifier, strings.Repeat("a", 64)))
	require.NoError(t, bs.SetMemoryAuthorPrincipal(identifier, "app-v28-principal"))
	require.NoError(t, bs.SetMemoryDomain(identifier, "public-test"))
	require.NoError(t, bs.SetMemoryClassification(identifier, 0))
}

func publicMemoryTestHash(identifier string) []byte {
	sum := sha256.Sum256([]byte("app-v28:" + identifier))
	return sum[:]
}

// stageAppV28Activation puts the app immediately before an app-v28 activation
// block at activationHeight, with a valid applied app-v27 predecessor and a
// committed H-1 tuple — the state every real node has when it reaches H.
func stageAppV28Activation(t *testing.T, app *SageApp, activationHeight int64) {
	t.Helper()
	// A chain that can activate app-v28 has long since passed app-v13, so the
	// narrow bookkeeping rule is the one in force until the composite rule
	// takes over at H+1.
	app.appV13AppliedHeight = 5
	app.appV20AppliedHeight = 10
	app.appV21AppliedHeight = 20
	app.appV22AppliedHeight = 30
	app.appV23AppliedHeight = 35
	app.appV24AppliedHeight = 40
	app.appV25AppliedHeight = 45
	app.appV26AppliedHeight = 50
	app.appV27AppliedHeight = 55
	app.state.Height = activationHeight - 1
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV27UpgradeName, 27, 55))
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV28UpgradeName, TargetAppVersion: 28,
		ActivationHeight: activationHeight, ProposedAt: activationHeight - 1,
	}))
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: activationHeight - 1, Time: appV23BlockTime(),
	})
}

func finalizeAndCommit(t *testing.T, app *SageApp, req *abcitypes.RequestFinalizeBlock) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	response, err := app.FinalizeBlock(context.Background(), req)
	require.NoError(t, err)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	return response
}

// The activation block stages, promotes and hashes under the app-v27 rule: the
// staged index is built before the speculative transaction, promoted inside it,
// and the composite rule only begins at H+1.
func TestAppV28ActivationPromotesIndexAndKeepsLegacyHashAtH(t *testing.T) {
	app := setupTestApp(t)
	publicMemoryTestRecord(t, app.badgerStore, "public-one")
	publicMemoryTestRecordWithStatus(t, app.badgerStore, "legacy-deprecated", "deprecated")
	stageAppV28Activation(t, app, 60)

	response := finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})

	legacy, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, legacy, response.AppHash,
		"the activation block still hashes under the app-v27 rule even though the index was promoted in it")
	require.NoError(t, app.badgerStore.ValidatePublicMemoryStage())
	require.NoError(t, app.badgerStore.ValidateCoCommitTombstoneStage(),
		"the same block must promote the co-commit tombstone backfill")
	tombstoned, err := app.badgerStore.CoCommitTombstoned(publicMemoryTestHash("legacy-deprecated"), "candidate")
	require.NoError(t, err)
	require.True(t, tombstoned, "the backfill covers records that left proposed before the fork")
	proof, err := app.badgerStore.PublicMemoryBranch("public-one")
	require.NoError(t, err)
	require.Equal(t, "committed", proof.Leaf.Status)
	require.Equal(t, int64(60), app.appV28AppliedHeight)
}

// From H+1 the AppHash is the composite of the legacy root and the public
// index, which is a different value than either half alone.
func TestAppV28CompositeAppHashFromHPlusOne(t *testing.T) {
	app := setupTestApp(t)
	publicMemoryTestRecord(t, app.badgerStore, "public-one")
	stageAppV28Activation(t, app, 60)
	activation := finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})

	response := finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 61, Time: appV23BlockTime(),
	})

	composite, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	legacy, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, composite, response.AppHash, "post-fork blocks commit the composite hash")
	require.NotEqual(t, legacy, response.AppHash, "the composite hash differs from its legacy half")
	require.NotEqual(t, activation.AppHash, composite,
		"the rule changes at H+1, not inside the activation block")
}

// A public record written in a post-fork block is folded into the promoted
// index by that block's own transaction — the only transaction that can still
// see the write as changed. This drives the same scoped store and sync helper
// the consensus path uses; the RecordChange must reach the live tree through
// the stage fallback rather than through a rebuild.
func TestAppV28IndexFollowsPostForkBlockWrites(t *testing.T) {
	app := setupTestApp(t)
	publicMemoryTestRecord(t, app.badgerStore, "public-one")
	stageAppV28Activation(t, app, 60)
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	before, err := app.badgerStore.PublicMemoryBranch("public-one")
	require.NoError(t, err)
	require.Equal(t, "committed", before.Leaf.Status)

	transaction := app.badgerStore.BeginConsensusTransaction(nil)
	working := app.cloneForAppV20Finalize(transaction)
	require.NoError(t, transaction.SetMemoryStatusPreservingHash("public-one", "challenged"))
	require.NoError(t, working.syncAppV28IndexesForBlock(61))
	require.NoError(t, transaction.CommitConsensusTransaction())

	proof, err := app.badgerStore.PublicMemoryBranch("public-one")
	require.NoError(t, err)
	require.Equal(t, "challenged", proof.Leaf.Status, "a post-fork block write must reach the live index")
	require.NotEqual(t, before.Root, proof.Root, "the index root moves with the record it covers")

	composite, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.Len(t, composite, 32)
}

// Boot validation is fail-closed: an applied v28 record whose promoted index is
// missing or corrupt refuses to serve rather than hashing a root that does not
// describe the public set.
func TestAppV28BootValidationRefusesMissingStage(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV27UpgradeName, 27, 90))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV28UpgradeName, 28, 100))
	app.appV27AppliedHeight = 90
	require.NoError(t, app.refreshAppV28Fork())
	require.ErrorContains(t, app.validateAppV28Prerequisite(), "invalid public-memory stage")
}
