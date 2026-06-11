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

// app-v13 replay parity (v10.5.1). app-v13 corrects app-v12's flawed AppHash
// rule: v12 excluded the WHOLE state: prefix — silently dropping governance
// proposals/votes (state:gov:*), memory quorum votes (state:vote:*), consumed
// markers, and shared-domain sentinels from the hash. v13 excludes EXACTLY the
// three SaveState bookkeeping keys. The rules are mutually exclusive
// REPLACEMENTS selected by precedence in FinalizeBlock (v13 > v12 > legacy).
// These tests pin:
//   Q1 PostForkGovStateMovesHash — the review finding's core: under v13,
//      gov/vote/sentinel writes move the FinalizeBlock AppHash; under a
//      v12-era chain they (reproducibly, flawed-but-frozen) do not.
//   Q2 IdleFixedPointSurvives — v13 keeps the issue-#40 idle fixed point.
//   Q3 ActivationBoundaryHashLevel — H_act itself hashes under the PREVIOUS
//      rule (hash-level assertion, not just the predicate).
//   Q4 EpochBoundaryBounded — an idle chain crossing an epoch boundary
//      post-v13 re-settles to a fixed point immediately after.
//   Q5 SubsumptionAndPrecedence — a v13-only chain enforces v8..v11 rules,
//      does NOT select the v12 hash rule, and reports version 13.
//   Q6 BootRefreshThroughConstructor — the gate restores through the real
//      constructor path, not just refreshAppV13Fork in isolation.
//   Q7 CrashReplayReEmitsVersionBump — a replayed activation block re-emits
//      ConsensusParamUpdates from the audit trail (plan already deleted).

// TestReplayAppV13_Q1_PostForkGovStateMovesHash pins the corrected integrity
// property end-to-end through FinalizeBlock: consensus state under state:
// (other than the three bookkeeping keys) is committed to by the hash again.
func TestReplayAppV13_Q1_PostForkGovStateMovesHash(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetNonce("agent-a", 7))
	app.appV13AppliedHeight = 10 // post-v13 for height > 10

	h11 := finalizeEmptyBlock(t, app, 11)

	// A governance write between blocks must move the next block's hash.
	require.NoError(t, app.badgerStore.SetState("gov:proposal:p1", []byte("payload")))
	h12 := finalizeEmptyBlock(t, app, 12)
	assert.NotEqual(t, h11, h12, "v13: a state:gov:* write must move the AppHash (the v12 flaw)")

	// A memory quorum vote too.
	require.NoError(t, app.badgerStore.SetState("vote:mem1:val1", []byte("accept")))
	h13 := finalizeEmptyBlock(t, app, 13)
	assert.NotEqual(t, h12, h13, "v13: a state:vote:* write must move the AppHash")

	// Contrast: the SAME writes on a v12-era chain are invisible — flawed,
	// but frozen that way so v12-era blocks replay byte-identically.
	v12app := setupTestApp(t)
	require.NoError(t, v12app.badgerStore.SetNonce("agent-a", 7))
	v12app.appV12AppliedHeight = 10
	g11 := finalizeEmptyBlock(t, v12app, 11)
	require.NoError(t, v12app.badgerStore.SetState("gov:proposal:p1", []byte("payload")))
	g12 := finalizeEmptyBlock(t, v12app, 12)
	assert.Equal(t, g11, g12, "v12-era replay: gov writes stay invisible to the broad rule (frozen flaw)")
}

// TestReplayAppV13_Q2_IdleFixedPointSurvives asserts the corrected rule keeps
// the issue-#40 property: empty FinalizeBlock+Commit cycles are a fixed point.
func TestReplayAppV13_Q2_IdleFixedPointSurvives(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetNonce("agent-a", 7))
	// Seed gov state so the fixed point holds WITH state: consensus keys present.
	require.NoError(t, app.badgerStore.SetState("gov:proposal:p1", []byte("payload")))
	app.appV13AppliedHeight = 10

	h11 := finalizeEmptyBlock(t, app, 11)
	h12 := finalizeEmptyBlock(t, app, 12)
	h13 := finalizeEmptyBlock(t, app, 13)
	assert.Equal(t, h11, h12, "v13: empty blocks must not move the AppHash")
	assert.Equal(t, h12, h13, "v13: idle hash is a fixed point")
}

// TestReplayAppV13_Q3_ActivationBoundaryHashLevel asserts at the HASH level
// (not just the predicate) that the v13 activation block itself still hashes
// under the previously-active rule, and the first post-activation block
// switches to the narrow rule.
func TestReplayAppV13_Q3_ActivationBoundaryHashLevel(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetNonce("agent-a", 7))

	// Stage a real v13 plan through the pre-app-v8 self-activating path
	// FIRST (a post-v12 height would be admin-gated via subsumption), then
	// arm the v12 gate so the broad rule is in force well before H_act.
	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV13UpgradeName, 13, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 5, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight
	app.appV12AppliedHeight = 8 // v12 broad rule in force for heights > 8

	// Activation block: rule still v12-broad. Compare against the broad hash
	// recomputed over the post-FinalizeBlock keyspace (Commit not yet run, so
	// the keyspace is exactly what FinalizeBlock hashed).
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	wantBroad, err := app.badgerStore.ComputeAppHashExcludingState()
	require.NoError(t, err)
	assert.Equal(t, wantBroad, respAt.AppHash, "H_act must hash under the PREVIOUS (v12-broad) rule")
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)

	// First post-activation block: narrow rule.
	respNext, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH + 1, Time: time.Now()})
	require.NoError(t, err)
	wantNarrow, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	assert.Equal(t, wantNarrow, respNext.AppHash, "H_act+1 must hash under the v13 narrow rule")
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
}

// TestReplayAppV13_Q4_EpochBoundaryBounded asserts an idle chain crossing an
// epoch boundary (poe.EpochInterval=100) post-v13 immediately re-settles: the
// boundary block may write epoch state (poew:*), but the blocks after it are
// a fixed point again — the chain cannot re-enter perpetual block minting.
func TestReplayAppV13_Q4_EpochBoundaryBounded(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetNonce("agent-a", 7))
	app.appV13AppliedHeight = 290

	h299 := finalizeEmptyBlock(t, app, 299)
	_ = finalizeEmptyBlock(t, app, 300) // epoch boundary — may move the hash
	h301 := finalizeEmptyBlock(t, app, 301)
	h302 := finalizeEmptyBlock(t, app, 302)
	assert.Equal(t, h301, h302, "post-boundary blocks must re-settle to a fixed point")
	_ = h299 // the boundary itself is allowed to move (or not move) the hash
}

// TestReplayAppV13_Q5_SubsumptionAndPrecedence asserts a v13-only skip-ahead
// chain enforces every lower additive rule, does NOT select the superseded
// v12 hash rule, and reports version 13.
func TestReplayAppV13_Q5_SubsumptionAndPrecedence(t *testing.T) {
	app := setupTestApp(t)
	app.appV13AppliedHeight = 100

	assert.True(t, app.postAppV8Rules(101), "v13 subsumes app-v8 rules")
	assert.True(t, app.postAppV9Rules(101), "v13 subsumes app-v9 rules")
	assert.True(t, app.postAppV10Rules(101), "v13 subsumes app-v10 rules")
	assert.True(t, app.postAppV11Rules(101), "v13 subsumes app-v11 rules")
	assert.False(t, app.postAppV12Rules(101), "the v12 hash rule is SUPERSEDED, not subsumed — precedence selects v13")
	assert.True(t, app.postAppV13Rules(101))
	assert.False(t, app.postAppV13Rules(100), "strict-> boundary")
	assert.Equal(t, uint64(13), app.currentAppVersion())
}

// TestReplayAppV13_Q6_BootRefreshThroughConstructor asserts a node restarting
// on a post-v13 chain restores the gate through the REAL constructor path
// (NewSageAppWithStores), and that reconcilePoEForkMonotonicity leaves the
// independent v13 gate alone.
func TestReplayAppV13_Q6_BootRefreshThroughConstructor(t *testing.T) {
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "boot-refresh.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	require.NoError(t, bs.MarkUpgradeApplied(appV13UpgradeName, 13, 77))

	app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
	require.NoError(t, err)
	assert.Equal(t, int64(77), app.appV13AppliedHeight, "constructor must restore the v13 gate from the audit record")
	assert.Equal(t, uint64(13), app.currentAppVersion())
	assert.Equal(t, int64(0), app.v8AppliedHeight, "independent gate: PoE ladder untouched by reconcile")
}

// TestReplayAppV13_Q7_CrashReplayReEmitsVersionBump simulates the
// crash-before-Commit replay of an activation block: the plan is already
// deleted (MarkUpgradeApplied is durable in BadgerDB), so the replayed H_act
// must re-emit the version.app bump from the audit trail — matching what
// every non-crashed replica emitted at that height.
func TestReplayAppV13_Q7_CrashReplayReEmitsVersionBump(t *testing.T) {
	app := setupTestApp(t)
	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV13UpgradeName, 13, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 11, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	actH := plan.ActivationHeight

	// Original execution of H_act: emits the bump, deletes the plan.
	respOrig, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respOrig.ConsensusParamUpdates)
	require.Equal(t, uint64(13), respOrig.ConsensusParamUpdates.Version.App)

	// Crash before Commit; CometBFT replays H_act. No plan exists anymore.
	respReplay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, respReplay.ConsensusParamUpdates, "replayed H_act must re-emit the version bump from the audit trail")
	assert.Equal(t, uint64(13), respReplay.ConsensusParamUpdates.Version.App)

	// A non-activation height must NOT re-emit.
	respOther, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: actH + 1, Time: time.Now()})
	require.NoError(t, err)
	assert.Nil(t, respOther.ConsensusParamUpdates)
}
