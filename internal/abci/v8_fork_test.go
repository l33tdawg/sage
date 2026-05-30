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
