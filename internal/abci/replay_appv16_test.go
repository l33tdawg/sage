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
)

// app-v16 replay parity (v11.2, domainless-forget remediation). The app-v16 tests
// in appv16_domain_repair_test.go set appV16AppliedHeight directly and so never
// exercise the REAL activation path: the FinalizeBlock activation arm, the
// constructor boot-restore, the version-machinery lockstep, or skip-ahead
// subsumption. These mirror replay_appv15_test.go to close that gap — the exact
// version-machinery class of bug that surfaced as a BLOCKER during this feature's
// review (currentAppVersion() missing the 16 case ⇒ handshake halt).

// TestAppV16_ForkGateDefaultsAndSubsumption pins the dormant default, the strict-'>'
// activation boundary, that a live gate subsumes the lower additive rules (v8..v15),
// and that the version machinery reports 16.
func TestAppV16_ForkGateDefaultsAndSubsumption(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV16Fork(100), "gate dormant by default (appV16AppliedHeight == 0)")
	assert.Equal(t, uint64(1), app.currentAppVersion(), "no gate ⇒ genesis version 1")

	app.appV16AppliedHeight = 5
	assert.False(t, app.postAppV16Fork(5), "activation block itself is not past-fork (strict >)")
	assert.True(t, app.postAppV16Fork(6), "live at H_act+1")

	assert.True(t, app.postAppV8Rules(6), "app-v16 subsumes app-v8 rules")
	assert.True(t, app.postAppV11Rules(6), "app-v16 subsumes app-v11 rules")
	assert.True(t, app.postAppV15Rules(6), "app-v16 subsumes app-v15 rules")
	assert.Equal(t, uint64(16), app.currentAppVersion(), "app-v16 active ⇒ version 16 (ranks above 15)")
	assert.GreaterOrEqual(t, MaxSupportedAppVersion(), uint64(16), "binary advertises at least a v16 fork gate (no handshake-halt footgun)")
}

// TestReplayAppV16_SkipAheadSubsumption: a hand-crafted skip to 16 (only
// appV16AppliedHeight set) must keep the app-v8..v11 + v15 additive rules TRUE while
// the mutually-exclusive AppHash-replacement helpers (v12/v13) stay FALSE — else the
// FinalizeBlock hash rule is misselected and the AppHash diverges. Two replicas agree.
func TestReplayAppV16_SkipAheadSubsumption(t *testing.T) {
	mk := func() *SageApp {
		a := setupTestApp(t)
		a.appV16AppliedHeight = 100 // only the top gate; every lower field stays 0
		return a
	}
	app, replica := mk(), mk()

	assert.True(t, app.postAppV8Rules(101), "v16 subsumes app-v8 rules")
	assert.True(t, app.postAppV9Rules(101), "v16 subsumes app-v9 rules")
	assert.True(t, app.postAppV10Rules(101), "v16 subsumes app-v10 rules")
	assert.True(t, app.postAppV11Rules(101), "v16 subsumes app-v11 rules")
	assert.True(t, app.postAppV15Rules(101), "v16 subsumes app-v15 rules")

	assert.False(t, app.postAppV12Rules(101), "v16 does NOT imply the v12 AppHash rule")
	assert.False(t, app.postAppV13Rules(101), "v16 does NOT imply the v13 AppHash rule")

	assert.Equal(t, uint64(16), app.currentAppVersion())
	assert.Equal(t, int64(0), app.v8AppliedHeight, "skip-ahead: PoE ladder untouched")

	for _, h := range []int64{1, 100, 101, 1000} {
		assert.Equalf(t, app.postAppV8Rules(h), replica.postAppV8Rules(h), "replicas agree on v8 rules at %d", h)
		assert.Equalf(t, app.postAppV16Fork(h), replica.postAppV16Fork(h), "replicas agree on v16 gate at %d", h)
	}
}

// TestReplayAppV16_BootRefreshThroughConstructor: a node restarting on a post-app-v16
// chain restores the activation height through the REAL constructor path
// (refreshAppV16Fork) and reports version 16.
func TestReplayAppV16_BootRefreshThroughConstructor(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "appv16-boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	require.NoError(t, bs.MarkUpgradeApplied(appV16UpgradeName, 16, 4200))

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(4200), app.appV16AppliedHeight, "constructor must restore the app-v16 activation height")
	assert.Equal(t, uint64(16), app.currentAppVersion())
	assert.False(t, app.postAppV16Fork(4200), "gate dormant at the activation block (strict >)")
	assert.True(t, app.postAppV16Fork(4201), "gate live at H_act+1")
}

// TestReplayAppV16_CrashReplayReEmitsVersionBump drives the REAL propose→activate
// path and the crash-before-Commit replay: the FinalizeBlock activation arm sets the
// height in-memory and (both original and replay) re-emits ConsensusParamUpdates
// version.app=16 from the audit trail — the arm my manual-height unit tests bypass.
func TestReplayAppV16_CrashReplayReEmitsVersionBump(t *testing.T) {
	app := setupTestApp(t)
	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV16UpgradeName, 16, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 11, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight

	respOrig, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respOrig.ConsensusParamUpdates)
	require.Equal(t, uint64(16), respOrig.ConsensusParamUpdates.Version.App)
	assert.Equal(t, actH, app.appV16AppliedHeight, "activation sets the app-v16 height in memory")

	respReplay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respReplay.ConsensusParamUpdates, "replayed activation must re-emit the version bump from the audit trail")
	assert.Equal(t, uint64(16), respReplay.ConsensusParamUpdates.Version.App)

	respOther, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH + 1, Time: time.Now()})
	require.NoError(t, err)
	assert.Nil(t, respOther.ConsensusParamUpdates)
}
