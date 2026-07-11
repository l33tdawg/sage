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

// app-v17 gate parity (v11.5, memory-lifecycle sprint: reinstate + quorum-scaled
// challenge + stale-vote cleanup rider). Mirrors replay_appv16_test.go: pins the
// dormant default, the strict-'>' activation boundary, skip-ahead subsumption,
// the version-machinery lockstep (the class of bug that surfaced as a BLOCKER in
// the app-v16 review — a missing currentAppVersion() case ⇒ handshake halt), the
// constructor boot-restore, and the crash-replay version-bump re-emit.

// TestAppV17_ForkGateDefaultsAndSubsumption pins the dormant default, the
// strict-'>' activation boundary, that a live gate subsumes the lower additive
// rules (v8..v16), and that the version machinery reports 17.
func TestAppV17_ForkGateDefaultsAndSubsumption(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV17Fork(100), "gate dormant by default (appV17AppliedHeight == 0)")
	assert.Equal(t, uint64(1), app.currentAppVersion(), "no gate ⇒ genesis version 1")

	app.appV17AppliedHeight = 5
	assert.False(t, app.postAppV17Fork(5), "activation block itself is not past-fork (strict >)")
	assert.True(t, app.postAppV17Fork(6), "live at H_act+1")

	assert.True(t, app.postAppV8Rules(6), "app-v17 subsumes app-v8 rules")
	assert.True(t, app.postAppV11Rules(6), "app-v17 subsumes app-v11 rules")
	assert.True(t, app.postAppV15Rules(6), "app-v17 subsumes app-v15 rules")
	assert.True(t, app.postAppV16Rules(6), "app-v17 subsumes app-v16 rules")
	assert.Equal(t, uint64(17), app.currentAppVersion(), "app-v17 active ⇒ version 17 (ranks above 16)")
	assert.GreaterOrEqual(t, MaxSupportedAppVersion(), uint64(17), "binary advertises at least the v17 fork gate (no handshake-halt footgun)")
}

// TestAppV17_CanonicalNameAndVersionLockstep couples the activation-name constant
// to tx.CanonicalUpgradeName (the single source of truth plan names derive from)
// and the gate→version arm: a bare chain with only the app-v17 gate set must
// report 17, or FinalizeBlock's committed version.app=17 outruns Info() and the
// next handshake halts on a regression.
func TestAppV17_CanonicalNameAndVersionLockstep(t *testing.T) {
	assert.Equal(t, tx.CanonicalUpgradeName(17), appV17UpgradeName)
	v17 := setupTestApp(t)
	v17.appV17AppliedHeight = 100
	assert.Equal(t, uint64(17), v17.currentAppVersion(), "app-v17 active with no lower gate still reports 17 (top case)")
}

// TestReplayAppV17_SkipAheadSubsumption: a hand-crafted skip to 17 (only
// appV17AppliedHeight set) must keep the app-v8..v11 + v15 + v16 additive rules
// TRUE while the mutually-exclusive AppHash-replacement helpers (v12/v13) stay
// FALSE — else the FinalizeBlock hash rule is misselected and the AppHash
// diverges. Two replicas agree.
func TestReplayAppV17_SkipAheadSubsumption(t *testing.T) {
	mk := func() *SageApp {
		a := setupTestApp(t)
		a.appV17AppliedHeight = 100 // only the top gate; every lower field stays 0
		return a
	}
	app, replica := mk(), mk()

	assert.True(t, app.postAppV8Rules(101), "v17 subsumes app-v8 rules")
	assert.True(t, app.postAppV9Rules(101), "v17 subsumes app-v9 rules")
	assert.True(t, app.postAppV10Rules(101), "v17 subsumes app-v10 rules")
	assert.True(t, app.postAppV11Rules(101), "v17 subsumes app-v11 rules")
	assert.True(t, app.postAppV15Rules(101), "v17 subsumes app-v15 rules")
	assert.True(t, app.postAppV16Rules(101), "v17 subsumes app-v16 rules")

	assert.False(t, app.postAppV12Rules(101), "v17 does NOT imply the v12 AppHash rule")
	assert.False(t, app.postAppV13Rules(101), "v17 does NOT imply the v13 AppHash rule")

	assert.Equal(t, uint64(17), app.currentAppVersion())
	assert.Equal(t, int64(0), app.v8AppliedHeight, "skip-ahead: PoE ladder untouched")

	for _, h := range []int64{1, 100, 101, 1000} {
		assert.Equalf(t, app.postAppV8Rules(h), replica.postAppV8Rules(h), "replicas agree on v8 rules at %d", h)
		assert.Equalf(t, app.postAppV17Fork(h), replica.postAppV17Fork(h), "replicas agree on v17 gate at %d", h)
	}
}

// TestReplayAppV17_BootRefreshThroughConstructor: a node restarting on a
// post-app-v17 chain restores the activation height through the REAL
// constructor path (refreshAppV17Fork) and reports version 17.
func TestReplayAppV17_BootRefreshThroughConstructor(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "appv17-boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	require.NoError(t, bs.MarkUpgradeApplied(appV17UpgradeName, 17, 4200))

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(4200), app.appV17AppliedHeight, "constructor must restore the app-v17 activation height")
	assert.Equal(t, uint64(17), app.currentAppVersion())
	assert.False(t, app.postAppV17Fork(4200), "gate dormant at the activation block (strict >)")
	assert.True(t, app.postAppV17Fork(4201), "gate live at H_act+1")
}

// TestReplayAppV17_CrashReplayReEmitsVersionBump drives the REAL propose→activate
// path and the crash-before-Commit replay: the FinalizeBlock activation arm sets
// the height in-memory and (both original and replay) re-emits
// ConsensusParamUpdates version.app=17 from the audit trail — the arm the
// manual-height unit tests bypass.
func TestReplayAppV17_CrashReplayReEmitsVersionBump(t *testing.T) {
	app := setupTestApp(t)
	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV17UpgradeName, 17, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 11, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight

	respOrig, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respOrig.ConsensusParamUpdates)
	require.Equal(t, uint64(17), respOrig.ConsensusParamUpdates.Version.App)
	assert.Equal(t, actH, app.appV17AppliedHeight, "activation sets the app-v17 height in memory")

	respReplay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respReplay.ConsensusParamUpdates, "replayed activation must re-emit the version bump from the audit trail")
	assert.Equal(t, uint64(17), respReplay.ConsensusParamUpdates.Version.App)

	respOther, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH + 1, Time: time.Now()})
	require.NoError(t, err)
	assert.Nil(t, respOther.ConsensusParamUpdates)
}
