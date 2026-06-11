package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// app-v12 replay parity (issue #40). Pre-fork, the AppHash includes the
// volatile state: keys Commit's SaveState rewrites every block, so the hash
// changes on EVERY block — including empty ones — and CometBFT's
// needProofBlock forces an empty proof block each timeout_commit, overriding
// CreateEmptyBlocks=false forever. Post-fork the state: namespace is excluded
// and an idle chain's hash reaches a fixed point. These tests pin:
//   P1 PreForkIdleHashChangesEveryBlock — the legacy rule keeps churning on
//      empty blocks (the historical-replay guarantee: old blocks must keep
//      producing the old, churning hashes).
//   P2 PostForkIdleHashFixedPoint — with the gate set, consecutive empty
//      FinalizeBlock+Commit cycles produce an IDENTICAL AppHash, and a real
//      state write still moves it.
//   P3 ActivationEndToEnd — a persisted {app-v12, target 12} plan activates
//      through the real FinalizeBlock path: gate set, version bumped to 12,
//      audit record written.
//
// Per the v8.x replay-test discipline, the post-fork gate is set IN-MEMORY
// (appV12AppliedHeight=H) rather than via MarkUpgradeApplied, so the
// upgrade:applied: audit key doesn't contaminate a pre/post keyspace
// comparison. P3 covers the real activation write path separately.

// finalizeEmptyBlock drives one empty FinalizeBlock+Commit cycle (what an idle
// chain executes every block) and returns the AppHash FinalizeBlock reported.
func finalizeEmptyBlock(t *testing.T, app *SageApp, height int64) []byte {
	t.Helper()
	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: height,
		Time:   time.Unix(1700000000+height, 0), // deterministic, irrelevant to the hash
	})
	require.NoError(t, err)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	return resp.AppHash
}

// TestReplayAppV12_P1_PreForkIdleHashChangesEveryBlock pins the LEGACY rule:
// with the app-v12 gate dormant, every empty block changes the AppHash
// (Commit's SaveState rewrites state:height/state:app_hash, and the next
// FinalizeBlock hashes them). This is the churn that makes needProofBlock
// mint empty blocks forever — and the exact behavior historical blocks must
// keep replaying byte-for-byte on an un-upgraded chain.
func TestReplayAppV12_P1_PreForkIdleHashChangesEveryBlock(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV12AppliedHeight, "precondition: app-v12 gate dormant")

	// Real (non-state) consensus key so the keyspace isn't empty.
	require.NoError(t, app.badgerStore.SetNonce("agent-a", 7))

	h11 := finalizeEmptyBlock(t, app, 11)
	h12 := finalizeEmptyBlock(t, app, 12)
	h13 := finalizeEmptyBlock(t, app, 13)

	assert.NotEqual(t, h11, h12, "pre-fork: SaveState's state: writes must keep moving the hash (legacy rule)")
	assert.NotEqual(t, h12, h13, "pre-fork: every empty block changes the hash (legacy rule)")
}

// TestReplayAppV12_P2_PostForkIdleHashFixedPoint asserts the fix: with the
// gate set, an idle chain's AppHash is a fixed point across empty
// FinalizeBlock+Commit cycles (so needProofBlock stops firing and
// CreateEmptyBlocks=false finally takes effect), while a real consensus write
// still moves the hash.
func TestReplayAppV12_P2_PostForkIdleHashFixedPoint(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetNonce("agent-a", 7))
	app.appV12AppliedHeight = 10 // post-fork for height > 10

	h11 := finalizeEmptyBlock(t, app, 11)
	h12 := finalizeEmptyBlock(t, app, 12)
	h13 := finalizeEmptyBlock(t, app, 13)

	assert.Equal(t, h11, h12, "post-fork: empty blocks must not move the AppHash")
	assert.Equal(t, h12, h13, "post-fork: idle hash is a fixed point")

	// A real consensus-state write still enters the hash.
	require.NoError(t, app.badgerStore.SetNonce("agent-b", 1))
	h14 := finalizeEmptyBlock(t, app, 14)
	assert.NotEqual(t, h13, h14, "post-fork: a non-state: write must still move the AppHash")

	// And it settles again.
	h15 := finalizeEmptyBlock(t, app, 15)
	assert.Equal(t, h14, h15, "post-fork: hash settles back to a fixed point after the write")
}

// TestReplayAppV12_P2b_GateBoundary pins the strict-> activation semantic on
// the new predicate: the activation block H_act itself still hashes under the
// pre-fork rule; the exclusion takes effect at H_act+1.
func TestReplayAppV12_P2b_GateBoundary(t *testing.T) {
	app := setupTestApp(t)
	app.appV12AppliedHeight = 100

	assert.False(t, app.postAppV12Fork(99), "below activation: pre-fork")
	assert.False(t, app.postAppV12Fork(100), "at activation block: still pre-fork (H+1 semantic)")
	assert.True(t, app.postAppV12Fork(101), "first post-activation block: post-fork")
	assert.True(t, app.postAppV12Rules(101), "rules helper mirrors the gate while app-v12 is the top fork")
}

// TestReplayAppV12_P3_ActivationEndToEnd drives the REAL activation path: a
// pre-fork self-activating UpgradePropose persists an {app-v12, target 12}
// plan, FinalizeBlock at the activation height sets the gate, bumps
// consensus version.app to 12, writes the audit record, and the very next
// blocks reach the idle fixed point.
func TestReplayAppV12_P3_ActivationEndToEnd(t *testing.T) {
	app := setupTestApp(t)

	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV12UpgradeName, 12, "deadbeef", 0)
	result := app.processUpgradePropose(ptx, 11, time.Unix(0, 0))
	require.Equal(t, uint32(0), result.Code, "pre-app-v8 propose self-activates: %s", result.Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	actH := plan.ActivationHeight
	require.Greater(t, actH, int64(11))

	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: actH,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)

	assert.Equal(t, actH, app.appV12AppliedHeight, "activation block must set the app-v12 gate")
	require.NotNil(t, resp.ConsensusParamUpdates)
	require.NotNil(t, resp.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(12), resp.ConsensusParamUpdates.Version.App)
	assert.Equal(t, uint64(12), app.currentAppVersion(), "Info must report 12 post-activation")

	applied, err := app.badgerStore.GetAppliedUpgrade(appV12UpgradeName)
	require.NoError(t, err)
	require.NotNil(t, applied, "applied audit record must exist")
	assert.Equal(t, actH, applied.AppliedHeight)

	// The rule flips at H_act+1; the hash settles one block later (H_act+1
	// drops the state: keys relative to H_act, then nothing changes).
	h1 := finalizeEmptyBlock(t, app, actH+1)
	h2 := finalizeEmptyBlock(t, app, actH+2)
	h3 := finalizeEmptyBlock(t, app, actH+3)
	assert.Equal(t, h1, h2, "first post-activation empty blocks must already be a fixed point")
	assert.Equal(t, h2, h3, "idle chain stays at the fixed point after activation")
}

// TestReplayAppV12_P4_RefreshFromAuditRecord asserts a node restarting on a
// post-app-v12 chain picks the gate up from the persisted audit record (the
// boot path), without waiting for another activation.
func TestReplayAppV12_P4_RefreshFromAuditRecord(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV12UpgradeName, 12, 77))

	app.appV12AppliedHeight = 0 // simulate a fresh boot before refresh
	app.refreshAppV12Fork()
	assert.Equal(t, int64(77), app.appV12AppliedHeight, "boot refresh must restore the gate from the audit record")
	assert.Equal(t, uint64(12), app.currentAppVersion())
}

// TestCommit_RetainHeight asserts the issue-#40 pruning half: Commit reports
// RetainHeight = height - retainBlocks once the chain is past the window,
// 0 (keep everything) below it or when the window is unset.
func TestCommit_RetainHeight(t *testing.T) {
	app := setupTestApp(t)

	// Default: pruning disabled.
	_ = finalizeEmptyBlock(t, app, 11)
	resp, err := app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.RetainHeight, "unset window must not prune")

	// Window armed but chain shorter than the window: still no pruning.
	app.SetRetainBlocks(100)
	_ = finalizeEmptyBlock(t, app, 12)
	resp, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.RetainHeight, "height <= window must not prune")

	// Past the window: prune everything older than the most recent 100.
	_ = finalizeEmptyBlock(t, app, 151)
	resp, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, int64(51), resp.RetainHeight, "RetainHeight = height - retainBlocks")

	// Negative input means disabled, not a negative retain height.
	app.SetRetainBlocks(-1)
	_ = finalizeEmptyBlock(t, app, 152)
	resp, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), resp.RetainHeight, "negative window disables pruning")
}
