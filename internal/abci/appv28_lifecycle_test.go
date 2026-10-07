package abci

import (
	"context"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

// The v28 gate shipped compiled ahead of its activation evidence, and the
// release that carried the evidence raised the auto-vote ceiling to meet it: the
// compiled and auto-vote ceilings are converged at 28, so a personal node
// advances itself across the seam the same way it did for every earlier rung.
func TestAppV28GateConvergesWithTheAutoVoteCeilingAfterItsEvidence(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(28), appV28UpgradeName)
	require.Equal(t, uint64(28), maxCompiledAppVersion)
	require.Equal(t, uint64(28), MaxSupportedAppVersion())
	require.Equal(t, maxCompiledAppVersion, MaxSupportedAppVersion(),
		"the ceiling bump is the activation switch; it lands with the evidence")
}

func TestAppV28ConstantsRefreshAndStrictBoundary(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV27UpgradeName, 27, 90))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV28UpgradeName, 28, 100))
	app.appV27AppliedHeight = 90
	require.NoError(t, app.refreshAppV28Fork())
	require.Equal(t, int64(100), app.appV28AppliedHeight)
	require.False(t, app.postAppV28Fork(100), "the activation block keeps app-v27 semantics")
	require.True(t, app.postAppV28Fork(101), "v28 rules begin strictly at H+1")
	require.Equal(t, uint64(28), app.currentAppVersion())
}

func TestAppV28RefreshRejectsAppliedRecordWithoutPredecessor(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV28UpgradeName, 28, 100))
	require.NoError(t, app.refreshAppV28Fork())
	require.Equal(t, int64(100), app.appV28AppliedHeight)
	require.ErrorContains(t, app.validateAppV28Prerequisite(), "missing active app-v27 predecessor")
}

func TestAppV28ActivationCommitsVersionAndAppliedRecord(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 10
	app.appV21AppliedHeight = 20
	app.appV22AppliedHeight = 30
	app.appV23AppliedHeight = 35
	app.appV24AppliedHeight = 40
	app.appV25AppliedHeight = 45
	app.appV26AppliedHeight = 50
	app.appV27AppliedHeight = 55
	app.state.Height = 59
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV27UpgradeName, 27, 55))
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV28UpgradeName, TargetAppVersion: 28,
		ActivationHeight: 60, ProposedAt: 59,
	}))
	// A real node reaches H with a committed H-1 tuple; the promotion is bound
	// to it, so the fixture has to produce one.
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 59, Time: appV23BlockTime(),
	})

	response, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	require.NoError(t, err)
	require.NotNil(t, response.ConsensusParamUpdates)
	require.Equal(t, uint64(28), response.ConsensusParamUpdates.Version.App)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, uint64(28), app.currentAppVersion())
	record, err := app.badgerStore.GetAppliedUpgrade(appV28UpgradeName)
	require.NoError(t, err)
	require.Equal(t, &store.AppliedUpgradeRecord{
		Name: appV28UpgradeName, TargetAppVersion: 28, AppliedHeight: 60,
	}, record)
	require.False(t, app.postAppV28Fork(60))
	require.True(t, app.postAppV28Fork(61))
}

func TestAppV28ActivationRefusesWithoutAppliedV27(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 10
	app.appV21AppliedHeight = 20
	app.appV22AppliedHeight = 30
	app.appV23AppliedHeight = 35
	app.appV24AppliedHeight = 40
	app.appV25AppliedHeight = 45
	app.appV26AppliedHeight = 50
	app.state.Height = 59
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV28UpgradeName, TargetAppVersion: 28,
		ActivationHeight: 60, ProposedAt: 59,
	}))

	_, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	require.ErrorContains(t, err, "refuse app-v28 activation")
	require.ErrorContains(t, err, "want 27")
}
