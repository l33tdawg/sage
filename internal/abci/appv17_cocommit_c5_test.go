package abci

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// ---------------------------------------------------------------------------
// app-v17 (C5): co-commit squat-reclaim stale-vote hygiene. When a co-commit
// reclaims a front-run squat at memory:<sharedID>, any vote:<sharedID>:* keys the
// squat accrued as a normal proposed memory are orphaned consensus state forever
// (no consensus-path cleanup exists). The C5 rider, gated postAppV17Rules, deletes
// them inside the reclaim branch. Strictly gated ⇒ a reclaim on a pre-fork / never-
// activated chain leaves every vote key exactly as today (byte-identical replay).
// ---------------------------------------------------------------------------

// squatWithVotes writes a front-run squat at sharedID (proposed) plus a few
// vote:<sharedID>:* keys, emulating a normal proposed memory that accrued votes
// before the co-commit reclaimed the slot.
func squatWithVotes(t *testing.T, app *SageApp, sharedID string) {
	t.Helper()
	require.NoError(t, app.badgerStore.SetMemoryHash(sharedID, []byte("squat-hash"), string(memory.StatusProposed)))
	for _, v := range []string{"validatorA", "validatorB", "validatorC"} {
		require.NoError(t, app.badgerStore.SetState("vote:"+sharedID+":"+v, []byte("accept")))
	}
	keys, err := app.badgerStore.PrefixKeys("vote:" + sharedID + ":")
	require.NoError(t, err)
	require.Len(t, keys, 3, "precondition: 3 stale vote keys on the squat")
}

// TestAppV17_C5_SquatReclaimClearsStaleVotes: post-fork, a reclaim purges the
// orphaned vote:<sharedID>:* keys.
func TestAppV17_C5_SquatReclaimClearsStaleVotes(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5 // co-commit machinery live
	app.appV17AppliedHeight = 5 // C5 rider live
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")

	squatWithVotes(t, app, env.SharedID)

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "reclaim submit: %s", res.Log)

	// Reclaim succeeded (memory committed) and the stale votes are gone.
	_, st, err := app.badgerStore.GetMemoryHash(env.SharedID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), st, "squat reclaimed to committed")

	keys, err := app.badgerStore.PrefixKeys("vote:" + env.SharedID + ":")
	require.NoError(t, err)
	assert.Empty(t, keys, "C5: stale vote keys cleared on post-fork squat-reclaim")
}

// TestAppV17_C5_SquatReclaimPreForkKeepsVotes: with app-v17 DORMANT (but app-v15
// live so the co-commit still runs), the rider is skipped and the vote keys
// survive exactly as today — byte-identical replay.
func TestAppV17_C5_SquatReclaimPreForkKeepsVotes(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5 // co-commit live
	require.Equal(t, int64(0), app.appV17AppliedHeight, "precondition: app-v17 dormant")
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("n1"), "sage-b")

	squatWithVotes(t, app, env.SharedID)

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "reclaim submit: %s", res.Log)

	keys, err := app.badgerStore.PrefixKeys("vote:" + env.SharedID + ":")
	require.NoError(t, err)
	assert.Len(t, keys, 3, "pre-fork: rider inert, stale vote keys survive (byte-identical replay)")
}

// TestAppV17_C5_FreshCoCommitNoVoteKeys: a fresh (non-squat) co-commit has no
// vote keys to clear — the rider is a no-op and does not disturb the commit.
func TestAppV17_C5_FreshCoCommitNoVoteKeys(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	app.appV17AppliedHeight = 5
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("fresh"), "sage-b")

	// No squat, no votes.
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "fresh co-commit: %s", res.Log)

	_, st, err := app.badgerStore.GetMemoryHash(env.SharedID)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), st)
	keys, err := app.badgerStore.PrefixKeys("vote:" + env.SharedID + ":")
	require.NoError(t, err)
	assert.Empty(t, keys, "no vote keys on a fresh co-commit")
}

func TestAppV20_CoCommitSquatReclaimBoundsStaleVotePruning(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 1
	app.appV17AppliedHeight = 1
	app.appV20AppliedHeight = 1
	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, "family.photos", []byte("bounded"), "sage-b")

	require.NoError(t, app.badgerStore.SetMemoryHash(env.SharedID, []byte("squat-hash"), string(memory.StatusProposed)))
	const extra = 17
	for i := 0; i < maxAppV20CoCommitStaleVotePrune+extra; i++ {
		require.NoError(t, app.badgerStore.SetState(
			fmt.Sprintf("vote:%s:validator-%04d", env.SharedID, i),
			[]byte("accept"),
		))
	}

	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Zero(t, res.Code, res.Log)
	keys, err := app.badgerStore.PrefixKeys("vote:" + env.SharedID + ":")
	require.NoError(t, err)
	assert.Len(t, keys, extra, "app-v20 reclaim prunes only its deterministic per-tx allowance")
	assert.Equal(t,
		fmt.Sprintf("vote:%s:validator-%04d", env.SharedID, maxAppV20CoCommitStaleVotePrune),
		keys[0],
		"the lexicographically first bounded prefix is pruned deterministically",
	)
}
