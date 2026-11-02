package abci

import (
	"context"
	"encoding/json"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func TestAppV31DormantBoundaryAndAutoVoteCeiling(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(31), appV31UpgradeName)
	require.Equal(t, uint64(31), MaxCompiledAppVersion())
	require.Equal(t, uint64(29), MaxSupportedAppVersion())
	app := setupTestApp(t)
	app.state.Height = 100
	app.appV30AppliedHeight = 90
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 90))
	require.NoError(t, app.refreshAppV31Fork())
	require.NoError(t, app.validateAppV31Prerequisite())
	require.Equal(t, uint64(30), app.currentAppVersion())
	require.False(t, app.postAppV31Fork(1_000_000), "compilation alone cannot activate v31")

	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV31UpgradeName, 31, 100))
	require.NoError(t, app.refreshAppV31Fork())
	require.NoError(t, app.validateAppV31Prerequisite())
	require.False(t, app.postAppV31Fork(99))
	require.False(t, app.postAppV31Fork(100), "activation H retains the prior transaction semantics")
	require.True(t, app.postAppV31Fork(101), "new transaction semantics begin only at H+1")
	require.Equal(t, uint64(31), app.currentAppVersion())
	require.Equal(t, int64(90), app.appV30AppliedHeight, "v31 must not rewrite v30 state")
}

func writeAppV31AppliedRecordForTest(t *testing.T, app *SageApp, name string, record store.AppliedUpgradeRecord) {
	t.Helper()
	wire, err := json.Marshal(record)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.DB().Update(func(transaction *badger.Txn) error {
		return transaction.Set([]byte("upgrade:applied:"+name), wire)
	}))
}

func TestAppV31RefreshRejectsMalformedAppliedRecords(t *testing.T) {
	for _, test := range []struct {
		name   string
		record store.AppliedUpgradeRecord
	}{
		{"wrong name", store.AppliedUpgradeRecord{Name: appV30UpgradeName, TargetAppVersion: 31, AppliedHeight: 100}},
		{"wrong target", store.AppliedUpgradeRecord{Name: appV31UpgradeName, TargetAppVersion: 30, AppliedHeight: 100}},
		{"zero height", store.AppliedUpgradeRecord{Name: appV31UpgradeName, TargetAppVersion: 31}},
		{"negative height", store.AppliedUpgradeRecord{Name: appV31UpgradeName, TargetAppVersion: 31, AppliedHeight: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.state.Height = 100
			app.appV31AppliedHeight = 7
			writeAppV31AppliedRecordForTest(t, app, appV31UpgradeName, test.record)
			require.ErrorContains(t, app.refreshAppV31Fork(), "invalid applied app-v31 record")
			require.Zero(t, app.appV31AppliedHeight, "invalid reload must not leave a stale live gate")
		})
	}
}

func TestAppV31RefreshAbsenceAndReadFailureClearStaleGate(t *testing.T) {
	app := setupTestApp(t)
	app.appV31AppliedHeight = 100
	require.NoError(t, app.refreshAppV31Fork())
	require.Zero(t, app.appV31AppliedHeight)
	require.NoError(t, app.badgerStore.DB().Update(func(transaction *badger.Txn) error {
		return transaction.Set([]byte("upgrade:applied:"+appV31UpgradeName), []byte("{"))
	}))
	app.appV31AppliedHeight = 100
	require.ErrorContains(t, app.refreshAppV31Fork(), "read applied app-v31 record")
	require.Zero(t, app.appV31AppliedHeight)
}

func TestAppV31RefreshRequiresStateAndAllowsOnlyActivationReplayWindow(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV31UpgradeName, 31, 100))
	state := app.state
	app.state = nil
	require.ErrorContains(t, app.refreshAppV31Fork(), "without app state")
	app.state = state
	app.state.Height = 98
	require.ErrorContains(t, app.refreshAppV31Fork(), "ahead of persisted app height 98")
	require.Zero(t, app.appV31AppliedHeight)
	app.state.Height = 99
	require.NoError(t, app.refreshAppV31Fork(), "a persisted activation record can precede replay of H")
	require.Equal(t, int64(100), app.appV31AppliedHeight)
	app.state.Height = 101
	require.NoError(t, app.refreshAppV31Fork())
}

func TestAppV31PrerequisiteRequiresExactActiveV30Record(t *testing.T) {
	for _, test := range []struct {
		name   string
		active int64
		record *store.AppliedUpgradeRecord
		want   string
	}{
		{"missing gate", 0, nil, "missing active app-v30 predecessor"},
		{"missing record", 90, nil, "invalid active app-v30 predecessor"},
		{"wrong name", 90, &store.AppliedUpgradeRecord{Name: appV29UpgradeName, TargetAppVersion: 30, AppliedHeight: 90}, "invalid active app-v30 predecessor"},
		{"wrong target", 90, &store.AppliedUpgradeRecord{Name: appV30UpgradeName, TargetAppVersion: 29, AppliedHeight: 90}, "invalid active app-v30 predecessor"},
		{"wrong height", 90, &store.AppliedUpgradeRecord{Name: appV30UpgradeName, TargetAppVersion: 30, AppliedHeight: 89}, "invalid active app-v30 predecessor"},
		{"equal activation", 100, &store.AppliedUpgradeRecord{Name: appV30UpgradeName, TargetAppVersion: 30, AppliedHeight: 100}, "must be after applied app-v30 predecessor height 100"},
		{"later predecessor", 101, &store.AppliedUpgradeRecord{Name: appV30UpgradeName, TargetAppVersion: 30, AppliedHeight: 101}, "must be after applied app-v30 predecessor height 101"},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV30AppliedHeight = test.active
			app.appV31AppliedHeight = 100
			if test.record != nil {
				writeAppV31AppliedRecordForTest(t, app, appV30UpgradeName, *test.record)
			}
			require.ErrorContains(t, app.validateAppV31Prerequisite(), test.want)
		})
	}
}

func TestAppV31CloneAndPublishKeepGateSpeculativeUntilCommit(t *testing.T) {
	app := setupTestApp(t)
	app.appV31AppliedHeight = 100
	clone := app.cloneForAppV20Finalize(app.badgerStore)
	require.Equal(t, int64(100), clone.appV31AppliedHeight)
	clone.appV31AppliedHeight = 101
	require.Equal(t, int64(100), app.appV31AppliedHeight)
	app.publishAppV20Finalize(clone)
	require.Equal(t, int64(101), app.appV31AppliedHeight)
}

func TestAppV31ActivationAndCommittedReplayPreserveVersionUpdate(t *testing.T) {
	app := setupTestApp(t)
	stageAppV28Activation(t, app, 60)
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{Height: 60, Time: appV23BlockTime()})
	finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{Height: 61, Time: appV23BlockTime()})
	for _, activation := range []struct {
		name    string
		version uint64
		height  int64
	}{{appV29UpgradeName, 29, 62}, {appV30UpgradeName, 30, 64}} {
		require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
			Name: activation.name, TargetAppVersion: activation.version,
			ActivationHeight: activation.height, ProposedAt: activation.height - 1,
		}))
		finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{Height: activation.height, Time: appV23BlockTime()})
		finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{Height: activation.height + 1, Time: appV23BlockTime()})
	}
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV31UpgradeName, TargetAppVersion: 31, ActivationHeight: 66, ProposedAt: 65,
	}))
	response, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 66, Time: appV23BlockTime()})
	require.NoError(t, err)
	require.Equal(t, uint64(31), response.ConsensusParamUpdates.Version.App)
	require.Equal(t, uint64(30), app.currentAppVersion(), "live graph must remain at v30 until Commit")
	require.Zero(t, app.appV31AppliedHeight)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, uint64(31), app.currentAppVersion())
	record, err := app.badgerStore.GetAppliedUpgrade(appV31UpgradeName)
	require.NoError(t, err)
	require.Equal(t, &store.AppliedUpgradeRecord{Name: appV31UpgradeName, TargetAppVersion: 31, AppliedHeight: 66}, record)
	_, err = app.badgerStore.GetUpgradePlan()
	require.ErrorIs(t, err, store.ErrNoUpgradePlan)
	version, found := app.appliedUpgradeTargetAtHeight(66)
	require.True(t, found, "crash replay must find dormant compiled gates above the auto-vote ceiling")
	require.Equal(t, uint64(31), version)
	response = finalizeAndCommit(t, app, &abcitypes.RequestFinalizeBlock{Height: 66, Time: appV23BlockTime()})
	require.Equal(t, uint64(31), response.ConsensusParamUpdates.Version.App, "committed replay must reproduce version.app")
	require.False(t, app.postAppV31Fork(66))
	require.True(t, app.postAppV31Fork(67))
}

func TestAppV31ActivationRefusesSkippedOrMalformedPredecessor(t *testing.T) {
	for _, test := range []struct {
		name      string
		version   uint64
		activeV30 bool
		recordV30 bool
		want      string
	}{
		{"skipped v30", 31, false, false, "want 30"},
		{"missing applied v30", 31, true, false, "invalid predecessor"},
		{"malformed target", 30, true, true, "refuse malformed app-v31 activation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.state.Height = 99
			if test.activeV30 {
				app.appV30AppliedHeight = 90
			} else {
				app.appV29AppliedHeight = 90
			}
			if test.recordV30 {
				require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 90))
			}
			require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
				Name: appV31UpgradeName, TargetAppVersion: test.version, ActivationHeight: 100, ProposedAt: 99,
			}))
			_, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 100, Time: appV23BlockTime()})
			require.ErrorContains(t, err, test.want)
			require.Zero(t, app.appV31AppliedHeight)
			record, err := app.badgerStore.GetAppliedUpgrade(appV31UpgradeName)
			require.NoError(t, err)
			require.Nil(t, record)
		})
	}
}
