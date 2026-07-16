package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// TestV84_EndToEnd_DomainFactorAcrossActivation is the v8.4 acceptance test
// (devnet step 6, in-process): it drives the REAL consensus pipeline — a signed
// UpgradePropose for app-v5 → FinalizeBlock activation, then real memory_submit
// + memory_vote txs into a NON-shared domain — across a pre-fork and a post-fork
// verdict, and asserts the v8.4 surface is fully behind the app-v5 gate:
//
//	pre-fork  : no memdomain:<id> key is written at submit, and a terminal
//	            verdict credits NO per-domain stats (vstats_domain:<v>:<D>).
//	activation: a real FinalizeBlock at the planned height bumps app version → 5.
//	post-fork : submit writes memdomain:<id>, and a terminal verdict credits
//	            vstats_domain:<v>:<D> for the matchers (alongside the global
//	            v8.3 accumulator) — so the per-domain expertise signal is live
//	            end-to-end through the same handlers FinalizeBlock dispatches.
func TestV84_EndToEnd_DomainFactorAcrossActivation(t *testing.T) {
	app := newAppWithLogger(t, zerolog.Nop())

	// v8.2 and v8.3 are already shipped/active on any chain that reaches v8.4;
	// set their gates directly (proven by their own suites) and activate app-v5
	// below through the real propose→FinalizeBlock path.
	app.v8_2AppliedHeight = 1
	app.v8_3AppliedHeight = 1

	vs := make([]agentKey, 4)
	for i := range vs {
		vs[i] = newAgentKey(t)
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: vs[i].id, Power: 1}))
	}
	proposer := vs[0]

	// --- Schedule the app-v5 activation via a real signed UpgradePropose. ---
	body := []byte(v8_4UpgradeName)
	pubKey, sig, bodyHash, ts := signAgentProof(t, proposer, body)
	proposeTx := &tx.ParsedTx{
		Type:           tx.TxTypeUpgradePropose,
		Nonce:          1,
		Timestamp:      time.Unix(ts, 0),
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
		UpgradePropose: &tx.UpgradePropose{
			Name:               v8_4UpgradeName,
			TargetAppVersion:   5,
			BinarySHA256:       "deadbeef",
			ProposerID:         proposer.id,
			UpgradeDelayBlocks: defaultUpgradeDelayBlocks,
		},
	}
	require.NoError(t, tx.SignTx(proposeTx, proposer.priv))
	res := app.processTx(proposeTx, 50, time.Unix(ts, 0))
	require.Equal(t, uint32(0), res.Code, "propose app-v5: %s", res.Log)

	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, plan)
	activation := plan.ActivationHeight // 50 + 200 = 250
	require.Equal(t, int64(250), activation)

	// ---------- PRE-FORK: a pwn_heap memory commits 3-of-4. ----------
	const preMem = "mem-pre-pwn"
	require.False(t, app.postV8_4Fork(100), "H=100 must be pre-fork")
	submitMemoryDomain(t, app, proposer, preMem, "pwn_heap", 100)
	castVote(t, app, vs[3], preMem, false, 100)
	castVote(t, app, vs[0], preMem, true, 100)
	castVote(t, app, vs[1], preMem, true, 100)
	castVote(t, app, vs[2], preMem, true, 100) // committing vote (all four tallied)
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, preMem))

	// Pre-fork: no memdomain: key, no per-domain crediting (replay parity — the
	// v8.4 keyspace stays empty until H>activation).
	d, err := app.badgerStore.GetMemoryDomain(preMem)
	require.NoError(t, err)
	assert.Equal(t, "", d, "pre-fork submit must NOT write memdomain:")
	for _, v := range vs {
		ec, cc := vdomStatsOf(t, app, v.id, "pwn_heap")
		assert.Equal(t, uint64(0), ec, "pre-fork: no per-domain verdict crediting")
		assert.Equal(t, uint64(0), cc)
	}

	// ---------- Activate app-v5 at H=250 via real FinalizeBlock. ----------
	respAt, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: activation,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	require.NotNil(t, respAt.ConsensusParamUpdates, "activation block must bump app version")
	require.Equal(t, uint64(5), respAt.ConsensusParamUpdates.Version.App, "app version → 5")
	require.Equal(t, activation, app.v8_4AppliedHeight, "v8.4 gate set at activation")
	require.True(t, app.postV8_4Fork(activation+1), "H>activation must be post-fork")

	// ---------- POST-FORK: a pwn_heap memory commits 3-of-4. ----------
	const postMem = "mem-post-pwn"
	h := activation + 10 // 260, post-fork
	submitMemoryDomain(t, app, proposer, postMem, "pwn_heap", h)

	// Post-fork submit records the domain on-chain.
	d, err = app.badgerStore.GetMemoryDomain(postMem)
	require.NoError(t, err)
	assert.Equal(t, "pwn_heap", d, "post-fork submit writes memdomain:")

	castVote(t, app, vs[3], postMem, false, h) // dissenter, while proposed
	castVote(t, app, vs[0], postMem, true, h)
	castVote(t, app, vs[1], postMem, true, h)
	castVote(t, app, vs[2], postMem, true, h) // committing vote, all four tallied
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, postMem))

	// Post-fork: the matchers accrue per-domain expertise in pwn_heap. The
	// per-domain accumulator counts ONLY the post-v8.4 verdict (the pre-fork
	// H=100 verdict predates the app-v5 gate), whereas the global v8.3
	// accumulator counted both verdicts — a clean demonstration that the v8.4
	// per-domain crediting is independently gated from the v8.3 global one.
	for _, v := range []agentKey{vs[0], vs[1], vs[2]} {
		ec, cc := vdomStatsOf(t, app, v.id, "pwn_heap")
		assert.Equal(t, uint64(1), ec, "%s: one per-domain verdict participation (post-v8.4 only)", v.id[:8])
		assert.Equal(t, uint64(1), cc, "%s: matched commit verdict → per-domain corr", v.id[:8])
		assert.Equal(t, uint64(2), vstatsOf(t, app, v.id).CorrCount, "%s: global corr counts both verdicts", v.id[:8])
	}
	// The dissenter participated per-domain but earned no corroboration.
	ec, cc := vdomStatsOf(t, app, vs[3].id, "pwn_heap")
	assert.Equal(t, uint64(1), ec, "dissenter participated in the pwn_heap verdict")
	assert.Equal(t, uint64(0), cc, "dissenter mismatched → no per-domain corr")

	// The signal is live end-to-end: a matcher's pwn_heap accuracy has risen
	// above the dissenter's through the real submit→vote→verdict pipeline.
	assert.Greater(t,
		domainAccuracyOf(t, app, vs[0].id, "pwn_heap"),
		domainAccuracyOf(t, app, vs[3].id, "pwn_heap"),
		"post-fork, being right in pwn_heap outscores dissenting in pwn_heap")
}
