package abci

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV21ForkGateAndVersionLockstep(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV21Fork(100))
	assert.Equal(t, uint64(1), app.currentAppVersion())
	assert.Equal(t, tx.CanonicalUpgradeName(21), appV21UpgradeName)
	assert.Equal(t, uint64(26), MaxSupportedAppVersion())

	app.appV21AppliedHeight = 100
	assert.False(t, app.postAppV21Fork(100), "activation block retains legacy-v17 challenge policy")
	assert.True(t, app.postAppV21Fork(101), "weighted challenge policy starts at H+1")
	assert.True(t, app.postAppV8Rules(101), "app-v21 subsumes additive app-v8 rules")
	assert.True(t, app.postAppV17Rules(101), "app-v21 subsumes app-v17 lifecycle rules")
	assert.True(t, app.postAppV19Rules(101), "app-v21 subsumes app-v19 readiness rules")
	assert.False(t, app.postAppV18Rules(101), "app-v21 must not implicitly enable the independent admin override")
	assert.Equal(t, uint64(21), app.currentAppVersion())
}

func TestReplayAppV21BootRefreshRequiresAppV20(t *testing.T) {
	t.Run("valid predecessor", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv21.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4200))
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 21, 4300))
		require.NoError(t, SaveState(bs, &AppState{Height: 4300}))

		app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.NoError(t, err)
		assert.Equal(t, int64(4200), app.appV20AppliedHeight)
		assert.Equal(t, int64(4300), app.appV21AppliedHeight)
		assert.Equal(t, uint64(21), app.currentAppVersion())
		assert.False(t, app.postAppV21Fork(4300))
		assert.True(t, app.postAppV21Fork(4301))
	})

	t.Run("missing predecessor fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv21-missing-v20.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 21, 4300))
		require.NoError(t, SaveState(bs, &AppState{Height: 4300}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "requires a valid applied app-v20 predecessor")
	})

	t.Run("non-monotonic predecessor height fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv21-bad-order.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4300))
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 21, 4200))
		require.NoError(t, SaveState(bs, &AppState{Height: 4300}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "must be after applied app-v20 height")
	})

	t.Run("malformed applied target fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv21-bad-target.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4200))
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 22, 4300))
		require.NoError(t, SaveState(bs, &AppState{Height: 4300}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "target app version 22, want 21")
	})

	t.Run("non-positive applied height fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv21-zero-height.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4200))
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 21, 0))
		require.NoError(t, SaveState(bs, &AppState{Height: 4300}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "non-positive height 0")
	})

	t.Run("impossible future applied height fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv21-future-height.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 4200))
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 21, 4302))
		require.NoError(t, SaveState(bs, &AppState{Height: 4300}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "ahead of persisted app height 4300")
	})
}

func TestReplayAppV21ActivationAndCrashReplayReemitVersion(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 1
	app.state.Height = 1
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV21UpgradeName, TargetAppVersion: 21, ActivationHeight: 2, ProposedAt: 1,
	}))

	first, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(100, 0),
	})
	require.NoError(t, err)
	require.NotNil(t, first.ConsensusParamUpdates)
	assert.Equal(t, uint64(21), first.ConsensusParamUpdates.Version.App)
	assert.Equal(t, int64(2), app.pendingAppV20Finalize.app.appV21AppliedHeight)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), app.appV21AppliedHeight)
	assert.Equal(t, uint64(21), app.currentAppVersion())

	// Replaying the activation height after the plan has been consumed derives
	// the exact version bump from the applied-upgrade audit.
	replay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(100, 0),
	})
	require.NoError(t, err)
	require.NotNil(t, replay.ConsensusParamUpdates)
	assert.Equal(t, uint64(21), replay.ConsensusParamUpdates.Version.App)
}

func TestReplayAppV21ActivationRejectsMalformedTargetBeforeMutation(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 1
	app.state.Height = 1
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV21UpgradeName, TargetAppVersion: 22, ActivationHeight: 2, ProposedAt: 1,
	}))

	_, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(100, 0),
	})
	require.ErrorContains(t, err, "malformed app-v21 activation")
	applied, getErr := app.badgerStore.GetAppliedUpgrade(appV21UpgradeName)
	require.NoError(t, getErr)
	assert.Nil(t, applied)
	assert.Zero(t, app.appV21AppliedHeight)
}
