package abci

import (
	"context"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func TestAppV29AutoVoteCeilingAndStrictBoundary(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(29), appV29UpgradeName)
	require.Equal(t, uint64(29), MaxCompiledAppVersion())
	require.Equal(t, uint64(29), MaxSupportedAppVersion())

	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV28UpgradeName, 28, 100))
	app.appV28AppliedHeight = 100
	require.NoError(t, app.refreshAppV29Fork())
	require.Equal(t, int64(0), app.appV29AppliedHeight)

	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV29UpgradeName, 29, 100))
	require.NoError(t, app.refreshAppV29Fork())
	require.Equal(t, int64(100), app.appV29AppliedHeight)
	require.False(t, app.postAppV29Fork(100), "the activation block keeps app-v28 semantics")
	require.True(t, app.postAppV29Fork(101), "v29 rules begin strictly at H+1")
	require.Equal(t, uint64(29), app.currentAppVersion())
}

func TestAppV29RefreshRejectsAppliedRecordWithoutV28(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV29UpgradeName, 29, 100))
	require.NoError(t, app.refreshAppV29Fork())
	require.Equal(t, int64(100), app.appV29AppliedHeight)
	require.ErrorContains(t, app.validateAppV29Prerequisite(), "missing active app-v28 predecessor")
}

func TestAppV29ActivationCommitsVersionAndAppliedRecord(t *testing.T) {
	app := setupTestApp(t)
	publicMemoryTestRecord(t, app.badgerStore, "public-one")
	stageAppV28Activation(t, app, 60)
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 61, Time: appV23BlockTime(),
	})
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV29UpgradeName, TargetAppVersion: 29,
		ActivationHeight: 62, ProposedAt: 61,
	}))

	response, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 62, Time: appV23BlockTime(),
	})
	require.NoError(t, err)
	require.NotNil(t, response.ConsensusParamUpdates)
	require.Equal(t, uint64(29), response.ConsensusParamUpdates.Version.App)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, uint64(29), app.currentAppVersion())

	record, err := app.badgerStore.GetAppliedUpgrade(appV29UpgradeName)
	require.NoError(t, err)
	require.Equal(t, &store.AppliedUpgradeRecord{
		Name: appV29UpgradeName, TargetAppVersion: 29, AppliedHeight: 62,
	}, record)
	require.False(t, app.postAppV29Fork(62))
	require.True(t, app.postAppV29Fork(63))
}

func TestAppV29ActivationRefusesWithoutAppliedV28(t *testing.T) {
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
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV29UpgradeName, TargetAppVersion: 29,
		ActivationHeight: 60, ProposedAt: 59,
	}))

	_, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	require.ErrorContains(t, err, "refuse app-v29 activation")
	require.ErrorContains(t, err, "want 28")
}
