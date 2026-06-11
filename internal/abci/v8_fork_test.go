package abci

import (
	"context"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// TestUpgradeNameConstantsAreCanonical couples the v8.x activation-name
// constants to tx.CanonicalUpgradeName — the single source of truth the
// watchdog proposer derives plan names from. If the naming scheme ever
// drifts in one place but not the other, the activation block stops
// matching plan.Name and every postV8_*Fork gate silently stays false
// (the upgrade-watchdog naming bug). Keep these locked together.
func TestUpgradeNameConstantsAreCanonical(t *testing.T) {
	assert.Equal(t, tx.CanonicalUpgradeName(2), v8UpgradeName)
	assert.Equal(t, tx.CanonicalUpgradeName(3), v8_2UpgradeName)
	assert.Equal(t, tx.CanonicalUpgradeName(4), v8_3UpgradeName)
	assert.Equal(t, tx.CanonicalUpgradeName(5), v8_4UpgradeName)
	assert.Equal(t, tx.CanonicalUpgradeName(6), v8_5UpgradeName) // "app-v6"

	// Couple the OTHER half too: the version a fork activates under (app-v<N>,
	// matched by name in FinalizeBlock) must equal the version currentAppVersion()
	// reports for that gate. Without this, the name→gate arm and the gate→version
	// arm could drift apart — a new fork half-landing silently (gate flips but
	// Info under-reports, or vice-versa).
	app := setupTestApp(t)
	assert.Equal(t, uint64(1), app.currentAppVersion(), "no gate ⇒ genesis version 1")
	app.v8AppliedHeight = 10
	assert.Equal(t, uint64(2), app.currentAppVersion(), "v8UpgradeName is app-v2")
	app.v8_2AppliedHeight = 20
	assert.Equal(t, uint64(3), app.currentAppVersion(), "v8_2UpgradeName is app-v3")
	app.v8_3AppliedHeight = 30
	assert.Equal(t, uint64(4), app.currentAppVersion(), "v8_3UpgradeName is app-v4")
	app.v8_4AppliedHeight = 40
	assert.Equal(t, uint64(5), app.currentAppVersion(), "v8_4UpgradeName is app-v5")
	app.v8_5AppliedHeight = 50
	assert.Equal(t, uint64(6), app.currentAppVersion(), "v8_5UpgradeName is app-v6")

	// app-v7 (content-validation activation) is an INDEPENDENT feature gate, not a
	// PoE-ladder member. Its version (7) is the highest, so once its gate is set
	// currentAppVersion() MUST report 7 — even on a chain where every PoE gate is
	// already set — or FinalizeBlock's committed version.app=7 outruns Info() and
	// the next handshake halts on a 7→6 regression. (The watchdog target stays at
	// 6 by design; app-v7 is governance-activated only.)
	app.appV7AppliedHeight = 60
	assert.Equal(t, uint64(7), app.currentAppVersion(), "app-v7 is the highest version — top case")

	// And it reports 7 even when the PoE gates BELOW it are unset: app-v7 can
	// activate without the PoE forks (it is excluded from monotonicity reconcile),
	// so its case cannot lean on the PoE top-down ordering.
	bare := setupTestApp(t)
	bare.appV7AppliedHeight = 60
	assert.Equal(t, uint64(7), bare.currentAppVersion(), "app-v7 active with no PoE gate still reports 7")

	// app-v11 (deterministic genesis admin + SQL-admin-bootstrap disable) is an
	// INDEPENDENT gate. Lock its canonical name and the gate→version coupling:
	// a bare chain with only the app-v11 gate set must report 11, or
	// FinalizeBlock's committed version.app=11 would outrun Info() and halt the
	// next handshake. (Watchdog target stays at 6 — app-v11 is
	// governance-activated only.)
	assert.Equal(t, tx.CanonicalUpgradeName(11), appV11UpgradeName)
	v11 := setupTestApp(t)
	v11.appV11AppliedHeight = 70
	assert.Equal(t, uint64(11), v11.currentAppVersion(), "app-v11 active with no lower gate still reports 11")

	// app-v12 (state:-key AppHash exclusion, issue #40) is an INDEPENDENT gate
	// and the highest version. Same lockstep: canonical name, and a bare chain
	// with only the app-v12 gate set must report 12 (its case ranks first in
	// currentAppVersion). (Watchdog target stays at 6 — app-v12 is
	// governance-activated only.)
	assert.Equal(t, tx.CanonicalUpgradeName(12), appV12UpgradeName)
	v12 := setupTestApp(t)
	v12.appV12AppliedHeight = 80
	assert.Equal(t, uint64(12), v12.currentAppVersion(), "app-v12 active with no lower gate still reports 12 (top case)")
}

// TestV8Fork_DefaultZero asserts a freshly-created app reports zero fork
// height and answers all post-fork predicates with false. This is the
// pre-fork (v7.1.1-equivalent) replay branch — every fork-gated handler
// must hit it on a chain that hasn't activated v8.0 yet.
func TestV8Fork_DefaultZero(t *testing.T) {
	app := setupTestApp(t)

	assert.Equal(t, int64(0), app.v8AppliedHeight, "fresh app must default to v8AppliedHeight=0")
	assert.False(t, app.postV8Fork(0))
	assert.False(t, app.postV8Fork(1_000_000))
	assert.False(t, app.IsPostV8Fork())
}

// TestV8Fork_PredicateBoundary asserts the +1 ("applied at H+1") boundary
// matches CometBFT's ConsensusParamUpdates semantics — the fork takes
// effect on the block immediately AFTER the activation block. Strict
// greater-than, not >=.
func TestV8Fork_PredicateBoundary(t *testing.T) {
	app := setupTestApp(t)
	app.v8AppliedHeight = 100

	assert.False(t, app.postV8Fork(99), "below activation: pre-fork")
	assert.False(t, app.postV8Fork(100), "at activation block: still pre-fork (H+1 semantic)")
	assert.True(t, app.postV8Fork(101), "first post-activation block: post-fork")
	assert.True(t, app.postV8Fork(1_000_000), "far future: post-fork")
}

// TestInfo_AppVersionReflectsActivatedFork asserts Info() reports the consensus
// app version matching the highest activated PoE fork, instead of a hardcoded
// 1. FinalizeBlock bumps consensus_params.version.app to plan.TargetAppVersion
// on activation (app-vN → N); a node restarting on a post-fork chain that still
// reported AppVersion=1 here would hand CometBFT an app-version regression
// against the committed consensus params. The activations are cumulative,
// mirroring a real chain progressing app-v2 → app-v5 in order.
func TestInfo_AppVersionReflectsActivatedFork(t *testing.T) {
	app := setupTestApp(t)

	info := func() uint64 {
		resp, err := app.Info(context.TODO(), &abcitypes.RequestInfo{})
		require.NoError(t, err)
		return resp.AppVersion
	}

	assert.Equal(t, uint64(1), info(), "fresh chain (no fork) reports app version 1")

	app.v8AppliedHeight = 10
	assert.Equal(t, uint64(2), info(), "app-v2 (v8.0 access-control) → version 2")

	app.v8_2AppliedHeight = 20
	assert.Equal(t, uint64(3), info(), "app-v3 (v8.2 PoE-weighted quorum) → version 3")

	app.v8_3AppliedHeight = 30
	assert.Equal(t, uint64(4), info(), "app-v4 (v8.3 PoE signals) → version 4")

	app.v8_4AppliedHeight = 40
	assert.Equal(t, uint64(5), info(), "app-v5 (v8.4 domain-factor) → version 5")

	app.v8_5AppliedHeight = 50
	assert.Equal(t, uint64(6), info(), "app-v6 (v8.5 upgrade-machinery hardening) → version 6")

	// app-v7 (content-validation activation) → version 7. This is the halt
	// fix: FinalizeBlock commits version.app=7 on activation, so Info() MUST also
	// report 7 or a restarting node hands CometBFT a 7→6 app-version regression
	// and the chain halts on the handshake (the v8.4.1/8.4.2 bug class).
	app.appV7AppliedHeight = 60
	assert.Equal(t, uint64(7), info(), "app-v7 (content-validation activation) → version 7")
}

// TestV8Fork_RefreshFromPersisted asserts refreshV8Fork pulls the height
// out of the BadgerDB audit trail. Mirrors the boot-time flow: a node
// restarting on a post-fork chain must pick up the gate without waiting
// for a fresh activation event.
func TestV8Fork_RefreshFromPersisted(t *testing.T) {
	app := setupTestApp(t)
	assert.Equal(t, int64(0), app.v8AppliedHeight, "precondition: no record yet")

	require.NoError(t, app.badgerStore.MarkUpgradeApplied(v8UpgradeName, 2, 4242))
	app.refreshV8Fork()

	assert.Equal(t, int64(4242), app.v8AppliedHeight)
	assert.True(t, app.postV8Fork(4243))
}

// TestV8Fork_RefreshIgnoresOtherUpgrades asserts that an AppliedUpgrade
// record for some OTHER upgrade name (e.g. a future v9 upgrade) does not
// flip the v8 gate. The cache is keyed strictly to "app-v2".
func TestV8Fork_RefreshIgnoresOtherUpgrades(t *testing.T) {
	app := setupTestApp(t)

	require.NoError(t, app.badgerStore.MarkUpgradeApplied("app-v3", 3, 9999))
	app.refreshV8Fork()

	assert.Equal(t, int64(0), app.v8AppliedHeight, "non-v8 upgrade record must not move the gate")
	assert.False(t, app.postV8Fork(10_000))
}

// TestV8Fork_IsPostV8ForkUsesChainHeight asserts the REST-side accessor
// reads AppState.Height (not a parameter). The off-consensus path is
// advisory — REST handlers don't carry a block height through their
// signatures, so they read the chain's last-committed height instead.
func TestV8Fork_IsPostV8ForkUsesChainHeight(t *testing.T) {
	app := setupTestApp(t)
	app.v8AppliedHeight = 50

	app.state.Height = 50
	assert.False(t, app.IsPostV8Fork(), "at activation block: pre-fork")

	app.state.Height = 51
	assert.True(t, app.IsPostV8Fork(), "first post-activation block: post-fork")
}
