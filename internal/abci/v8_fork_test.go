package abci

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
