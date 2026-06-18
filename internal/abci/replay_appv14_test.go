package abci

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/contentvalidator"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// app-v14 replay parity (v10.7.0). app-v14 is the SYMMETRIC DEACTIVATION of the
// app-v7 Layer-2 content-validation gate: a governed, replay-deterministic way
// to turn an already-activated content gate OFF at a future height H2, so the
// gate is live for exactly the window (H_act, H2] and dormant again afterward —
// both halves reproducing their committed AppHashes on replay. Unlike app-v13,
// app-v14 changes NO AppHash rule; it only toggles the content gate, so the
// FinalizeBlock hash-rule switch and snapshot/verify are untouched.
//
// These tests pin:
//   D1 WindowGateOnThenOff — a registered validator HARD-REJECTS (Code 18)
//      inside (H_act, H2] and PASSES (Code 0) for every height > H2.
//   D2 DeactivationBoundaryStrict — the deactivation block H2 itself still runs
//      gated (Code 18); enforcement stops at H2+1 (Code 0). Strict-> mirror of
//      the activation boundary.
//   D3 ActivationBoundaryUnchanged — app-v14 does not perturb the lower (app-v7)
//      boundary: gate dormant at H_act, live at H_act+1.
//   D4 InertWhenDormant — appV14AppliedHeight == 0 leaves postAppV7Fork behaving
//      exactly as before (gate live for ALL heights > H_act), so every chain
//      that has not activated app-v14 replays byte-identically.
//   D5 Determinism — the live window is a pure function of the two committed
//      activation heights: two independent apps with the same heights agree at
//      every height (the replay-safety invariant).
//   D6 VersionRankAndNoHashRuleChange — an app-v14 chain reports version 14
//      (ranked above 13), still enforces v8..v13 rules, and selects NO new
//      AppHash rule (postAppV13Rules precedence is unchanged).
//   D7 BootRefreshThroughConstructor — the deactivation height restores through
//      the REAL constructor path, not just refreshAppV14Fork in isolation.
//   D8 CrashReplayReEmitsVersionBump — a replayed app-v14 activation block
//      re-emits ConsensusParamUpdates(version.app=14) from the audit trail.

// armRejectingGate wires a content-validator registry that hard-rejects the
// "blocked" outcome_class on `domain`, arms app-v7 at hAct, and returns a
// submitter agent key. Mirrors content_gate_e2e_test.go's setup.
func armRejectingGate(t *testing.T, app *SageApp, domain string, hAct int64) (agentKey, string) {
	t.Helper()
	ak := newAgentKey(t)
	registerAgent(t, app, ak, "submitter", "member")

	reg := contentvalidator.NewContentValidatorRegistry()
	reg.RegisterContentValidator(domain, "blocked", func(rec *memory.MemoryRecord) error {
		return errors.New("stub: blocked outcome_class is rejected")
	})
	app.SetContentValidators(reg)
	app.appV7AppliedHeight = hAct // postAppV7Fork(h) true only for h > hAct

	const blockedBody = `{"schema_version":1,"outcome_class":"blocked"}`
	return ak, blockedBody
}

// TestReplayAppV14_D1_WindowGateOnThenOff is the core property: the content gate
// rejects inside (H_act, H2] and passes everywhere after H2.
func TestReplayAppV14_D1_WindowGateOnThenOff(t *testing.T) {
	app := setupTestApp(t)
	const domain = "appv14-gate-domain"
	ak, blocked := armRejectingGate(t, app, domain, 100)
	app.appV14AppliedHeight = 200 // deactivate the gate after height 200

	// Inside the window: HARD-REJECT.
	inWindow := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 150, time.Now())
	assert.Equal(t, uint32(18), inWindow.Code, "inside (H_act, H2]: gate live, blocked body must be Code 18")

	// After deactivation: PASS THROUGH (no gate).
	afterDeact := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 250, time.Now())
	assert.Equal(t, uint32(0), afterDeact.Code, "height > H2: gate deactivated, same body must pass through")
}

// TestReplayAppV14_D2_DeactivationBoundaryStrict pins the strict-> boundary at
// H2: gated AT H2, off at H2+1.
func TestReplayAppV14_D2_DeactivationBoundaryStrict(t *testing.T) {
	app := setupTestApp(t)
	const domain = "appv14-boundary-domain"
	ak, blocked := armRejectingGate(t, app, domain, 100)
	const h2 = 200
	app.appV14AppliedHeight = h2

	assert.True(t, app.postAppV7Fork(h2), "gate still live AT the deactivation block H2 (height <= H2)")
	assert.False(t, app.postAppV7Fork(h2+1), "gate OFF at H2+1 (height > H2)")

	atH2 := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), h2, time.Now())
	assert.Equal(t, uint32(18), atH2.Code, "deactivation block H2 itself still enforces (Code 18)")

	afterH2 := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), h2+1, time.Now())
	assert.Equal(t, uint32(0), afterH2.Code, "first post-deactivation block passes through (Code 0)")
}

// TestReplayAppV14_D3_ActivationBoundaryUnchanged asserts app-v14 does not
// disturb the app-v7 activation boundary.
func TestReplayAppV14_D3_ActivationBoundaryUnchanged(t *testing.T) {
	app := setupTestApp(t)
	const domain = "appv14-actboundary-domain"
	ak, blocked := armRejectingGate(t, app, domain, 100)
	app.appV14AppliedHeight = 200

	assert.False(t, app.postAppV7Fork(100), "activation block H_act: gate dormant (strict >)")
	assert.True(t, app.postAppV7Fork(101), "H_act+1: gate live")

	atHAct := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 100, time.Now())
	assert.Equal(t, uint32(0), atHAct.Code, "activation block still pre-fork: blocked body passes")
	postHAct := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, blocked), 101, time.Now())
	assert.Equal(t, uint32(18), postHAct.Code, "H_act+1: gate live, blocked body rejected")
}

// TestReplayAppV14_D4_InertWhenDormant asserts a chain that never activates
// app-v14 keeps the gate live for ALL heights past H_act — i.e. the new term is
// a true no-op so existing history replays byte-identically.
func TestReplayAppV14_D4_InertWhenDormant(t *testing.T) {
	app := setupTestApp(t)
	app.appV7AppliedHeight = 100
	require.Equal(t, int64(0), app.appV14AppliedHeight, "default: app-v14 dormant")

	assert.False(t, app.postAppV7Fork(100), "H_act: dormant")
	assert.True(t, app.postAppV7Fork(101), "H_act+1: live")
	assert.True(t, app.postAppV7Fork(1_000_000), "no deactivation: gate stays live arbitrarily far out")
}

// TestReplayAppV14_D5_Determinism asserts the live window is a pure function of
// the two committed heights — the replay-safety invariant. Two independent apps
// configured identically agree at every probed height.
func TestReplayAppV14_D5_Determinism(t *testing.T) {
	mk := func() *SageApp {
		a := setupTestApp(t)
		a.appV7AppliedHeight = 100
		a.appV14AppliedHeight = 200
		return a
	}
	a, b := mk(), mk()
	for _, h := range []int64{1, 100, 101, 150, 200, 201, 1000} {
		assert.Equalf(t, a.postAppV7Fork(h), b.postAppV7Fork(h),
			"replicas must agree on gate state at height %d", h)
	}
	// And the window shape itself is exactly (100, 200].
	assert.False(t, a.postAppV7Fork(100))
	assert.True(t, a.postAppV7Fork(101))
	assert.True(t, a.postAppV7Fork(200))
	assert.False(t, a.postAppV7Fork(201))
}

// TestReplayAppV14_D6_VersionRankAndOrthogonality asserts an app-v14 chain
// reports version 14 (ranked above 13) and that app-v14 is ORTHOGONAL to the
// v8..v13 consensus-rule chain: it only toggles the content gate + ranks the
// version, so it neither subsumes the lower rules nor selects any AppHash rule.
// Crucially app-v14 nests NO behavior inside the lower forks, so it does not
// introduce a new instance of the app-v9-without-app-v8 skip-ahead gap.
func TestReplayAppV14_D6_VersionRankAndOrthogonality(t *testing.T) {
	// (a) app-v14 alone: version 14, but the v8..v13 rules stay OFF (orthogonal —
	// app-v14 is not in their subsumption OR-chain) and NO AppHash rule is picked.
	app := setupTestApp(t)
	app.appV14AppliedHeight = 100
	assert.Equal(t, uint64(14), app.currentAppVersion(), "app-v14 ranks above app-v13")
	assert.False(t, app.postAppV8Rules(101), "app-v14 is orthogonal: it does NOT imply the app-v8 rule")
	assert.False(t, app.postAppV13Rules(101), "app-v14 does NOT imply the v13 AppHash rule (gate toggle only)")

	// (b) the realistic Sentinel shape: v13 already active AND app-v14 on top. The
	// lower rules are enforced via v13's own subsumption; version is 14.
	real := setupTestApp(t)
	real.appV13AppliedHeight = 50
	real.appV14AppliedHeight = 100
	assert.Equal(t, uint64(14), real.currentAppVersion(), "v13+v14: highest gate wins")
	assert.True(t, real.postAppV8Rules(101), "v13 subsumes app-v8 rules")
	assert.True(t, real.postAppV9Rules(101), "v13 subsumes app-v9 rules")
	assert.True(t, real.postAppV10Rules(101), "v13 subsumes app-v10 rules")
	assert.True(t, real.postAppV11Rules(101), "v13 subsumes app-v11 rules")
	assert.True(t, real.postAppV13Rules(101), "v13 AppHash rule active")
}

// TestReplayAppV14_D7_BootRefreshThroughConstructor asserts a node restarting on
// a post-app-v14 chain restores the deactivation height through the REAL
// constructor path, and reconcilePoEForkMonotonicity leaves the independent gate
// alone.
func TestReplayAppV14_D7_BootRefreshThroughConstructor(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "appv14-boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	// app-v7 armed at 376 (Sentinel's real activation height), deactivated at 99000.
	require.NoError(t, bs.MarkUpgradeApplied(appV7UpgradeName, 7, 376))
	require.NoError(t, bs.MarkUpgradeApplied(appV14UpgradeName, 14, 99000))

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(376), app.appV7AppliedHeight, "constructor must restore the app-v7 activation height")
	assert.Equal(t, int64(99000), app.appV14AppliedHeight, "constructor must restore the app-v14 deactivation height")
	assert.Equal(t, uint64(14), app.currentAppVersion())
	// The restored window is exactly (376, 99000].
	assert.True(t, app.postAppV7Fork(99000), "gate live at the deactivation block")
	assert.False(t, app.postAppV7Fork(99001), "gate off after deactivation")
	assert.Equal(t, int64(0), app.v8AppliedHeight, "independent gate: PoE ladder untouched by reconcile")
}

// TestReplayAppV14_D8_CrashReplayReEmitsVersionBump simulates the
// crash-before-Commit replay of an app-v14 activation block: the plan is already
// deleted (MarkUpgradeApplied is durable), so the replayed H2 must re-emit the
// version.app=14 bump from the audit trail — matching every non-crashed replica.
func TestReplayAppV14_D8_CrashReplayReEmitsVersionBump(t *testing.T) {
	app := setupTestApp(t)
	proposer := deterministicAgentKey(t)
	// Stage the app-v14 plan through the pre-app-v8 self-activating path.
	ptx := makeUpgradeProposeTx(t, proposer, appV14UpgradeName, 14, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 11, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight

	respOrig, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respOrig.ConsensusParamUpdates)
	require.Equal(t, uint64(14), respOrig.ConsensusParamUpdates.Version.App)
	assert.Equal(t, actH, app.appV14AppliedHeight, "activation sets the deactivation height in memory")

	// Crash before Commit; CometBFT replays H2. No plan exists anymore.
	respReplay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respReplay.ConsensusParamUpdates, "replayed activation must re-emit the version bump from the audit trail")
	assert.Equal(t, uint64(14), respReplay.ConsensusParamUpdates.Version.App)

	// A non-activation height must NOT re-emit.
	respOther, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH + 1, Time: time.Now()})
	require.NoError(t, err)
	assert.Nil(t, respOther.ConsensusParamUpdates)
}

// TestReplayAppV14_D9_DeactivationMovesAppHash is the HASH-LEVEL guard (mirrors
// the rigor of replay_appv13_test.go Q3, adapted to app-v14's property). The
// deactivation boundary is NOT hash-neutral: the Code-18 reject returns BEFORE
// SetMemoryHash, so a gated reject writes no memory:<id> key, while a deactivated
// pass does — moving the committed AppHash. D1-D8 only assert the Code/predicate
// level; this test computes actual AppHashes across the boundary so a future
// refactor that moved the gate after SetMemoryHash (or mis-ordered the predicate)
// cannot stay green while diverging a live chain.
func TestReplayAppV14_D9_DeactivationMovesAppHash(t *testing.T) {
	const domain = "appv14-hash-domain"
	const hAct = 100
	const probe = 250 // a fixed height > hAct, used identically in both worlds

	// committedHash submits the SAME blocked body at the SAME height through the
	// real consensus handler, with the content gate either LIVE or DEACTIVATED at
	// that height, and returns the resulting committed AppHash + the tx code.
	// Deterministic agent identity + fixed content => the ONLY variable across the
	// two worlds is the gate decision, so any hash difference is attributable to
	// it alone. The v13 narrow rule is in force so the hash excludes the volatile
	// bookkeeping keys and the memory:<id> write is the clean differential.
	committedHash := func(deactivateAt int64) ([]byte, uint32) {
		app := setupTestApp(t)
		app.appV13AppliedHeight = 50 // narrow, stable hash rule (excludes bookkeeping keys)
		ak := deterministicAgentKey(t)
		registerAgent(t, app, ak, "submitter", "member")
		reg := contentvalidator.NewContentValidatorRegistry()
		reg.RegisterContentValidator(domain, "blocked", func(rec *memory.MemoryRecord) error {
			return errors.New("stub: blocked outcome_class is rejected")
		})
		app.SetContentValidators(reg)
		app.appV7AppliedHeight = hAct
		if deactivateAt > 0 {
			app.appV14AppliedHeight = deactivateAt
		}
		const body = `{"schema_version":1,"outcome_class":"blocked"}`
		res := app.processMemorySubmit(makeMemorySubmitTx(t, ak, domain, body), probe, time.Now())
		h, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
		require.NoError(t, err)
		return h, res.Code
	}

	// Control: gate LIVE at probe (no deactivation) -> blocked REJECTED (Code 18), no memory: write.
	hashGated, codeGated := committedHash(0)
	require.Equal(t, uint32(18), codeGated, "control: gate live at probe -> Code 18")

	// Deactivated BEFORE probe -> blocked PASSES (Code 0) -> memory:<id> enters the AppHash.
	hashDeact, codeDeact := committedHash(probe - 1)
	require.Equal(t, uint32(0), codeDeact, "deactivated at probe -> Code 0")

	// The headline property, AT THE HASH LEVEL: holding height, agent, and body
	// identical, the deactivation decision alone moves the committed AppHash.
	assert.NotEqual(t, hashGated, hashDeact,
		"deactivation must move the committed AppHash (pass writes memory:, reject does not)")

	// Byte-identical replay: each world recomputed from scratch is bit-for-bit
	// identical, so replicas and re-syncs agree on both halves of the window.
	hashGated2, _ := committedHash(0)
	assert.Equal(t, hashGated, hashGated2, "gated world replays byte-identically")
	hashDeact2, _ := committedHash(probe - 1)
	assert.Equal(t, hashDeact, hashDeact2, "deactivated world replays byte-identically")
}
