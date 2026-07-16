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

// v8.4 replay parity. The new on-chain keys (memdomain:<id>, vstats_domain:<v>:<D>)
// are written only post-fork; ComputeAppHash streams every key's raw bytes in
// lexical order, so (a) a pre-fork chain never sees these prefixes — its AppHash
// keyspace is byte-identical to v8.3.x (proven behaviorally by the e2e's pre-fork
// assertions, and structurally by the same mechanism as v8.2's R1/R2) — and
// (b) once the keys exist they must hash deterministically on every re-read, or
// replicas replaying the post-fork history would diverge. R3 pins (b): the v8.4
// keyspace is in the digest AND stable across reads.
func TestReplayV8_4_R3_PostForkKeysHashDeterministically(t *testing.T) {
	app := newAppWithLogger(t, zerolog.Nop())
	app.v8_2AppliedHeight = 1
	app.v8_3AppliedHeight = 1
	activateV84(app, 100) // v8.4 active for H>100

	vs := make([]agentKey, 4)
	for i := range vs {
		vs[i] = newAgentKey(t)
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: vs[i].id, Power: 1}))
	}
	proposer := vs[0]

	// Snapshot the AppHash BEFORE any v8.4 key is written.
	hBefore, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	// A post-fork pwn_heap memory: submit writes memdomain:, the terminal verdict
	// writes vstats_domain:<v>:pwn_heap for all four validators.
	const memID = "mem-r3"
	const h = int64(200)
	require.True(t, app.postV8_4Fork(h))
	submitMemoryDomain(t, app, proposer, memID, "pwn_heap", h)
	castVote(t, app, vs[3], memID, false, h)
	castVote(t, app, vs[0], memID, true, h)
	castVote(t, app, vs[1], memID, true, h)
	castVote(t, app, vs[2], memID, true, h)
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID))

	// The v8.4 keys now exist on-chain.
	d, err := app.badgerStore.GetMemoryDomain(memID)
	require.NoError(t, err)
	require.Equal(t, "pwn_heap", d)
	ec, _ := vdomStatsOf(t, app, vs[0].id, "pwn_heap")
	require.Equal(t, uint64(1), ec)

	// (a) The new keys are part of the digest — AppHash moved off the pre-write snapshot.
	hAfter, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.NotEqual(t, hBefore, hAfter, "memdomain:/vstats_domain: keys must contribute to the AppHash")

	// (b) Replay-safety: ComputeAppHash over the same post-fork keyspace is
	// byte-identical on every read — no map-iteration nondeterminism leaks in.
	hReplay, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hAfter, hReplay, "ComputeAppHash must be deterministic over the v8.4 keyspace")
}

// TestReplayV8_4_R4_RealActivationBoundaryByteParity pins the replay guarantee
// at the layer R3 deliberately skips: the REAL FinalizeBlock activation arm.
// R3 (and the v8.2/v8.3 suites) activate gates in-memory to isolate keyspace
// deltas; here the gate flips the way it does on a live chain — a persisted
// upgrade plan whose ActivationHeight FinalizeBlock reaches — and the
// consensus-visible AppHash (ResponseFinalizeBlock.AppHash) is asserted at
// every block on TWO independent replicas executing identical raw tx bytes,
// exactly what nodes replaying the same committed history do.
//
// The load-bearing ordering this pins: FinalizeBlock executes the block's txs
// BEFORE the activation arm flips the gate (the "H_act itself stays pre-fork"
// edge case in docs/v8.2-PLAN.md). A domain-tagged memory_submit carried IN
// the activation block must therefore execute pre-fork — writing NO
// memdomain: key — while the byte-identical submit shape one block later
// writes it. A binary that flipped the gate before executing txs would commit
// a different AppHash at H_act and fork against every shipped node.
func TestReplayV8_4_R4_RealActivationBoundaryByteParity(t *testing.T) {
	// --- Build every tx ONCE. Both replicas execute identical raw bytes, so
	// any wall-clock material captured at signing time (agent timestamps,
	// signatures) is fixed in the "block" the way it is on a real chain. ---
	vs := make([]agentKey, 4)
	for i := range vs {
		vs[i] = newAgentKey(t)
	}
	proposer := vs[0]

	// Propose app-v5 at H=50; the 200-block chain floor puts activation at 250.
	// (50, 250, 251 all avoid epoch boundaries, so processEpoch never runs —
	// the epoch keyspace is the v8.2 suite's concern, not this test's.)
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
	proposeRaw, err := tx.EncodeTx(proposeTx)
	require.NoError(t, err)

	makeSubmitRaw := func(memoryID string, nonce uint64) []byte {
		subBody := []byte(memoryID + "pwn_heap")
		subPub, subSig, subHash, subTS := signAgentProof(t, proposer, subBody)
		parsed := &tx.ParsedTx{
			Type:      tx.TxTypeMemorySubmit,
			Nonce:     nonce,
			Timestamp: time.Unix(subTS, 0),
			MemorySubmit: &tx.MemorySubmit{
				MemoryID:        memoryID,
				MemoryType:      tx.MemoryTypeObservation,
				DomainTag:       "pwn_heap",
				ConfidenceScore: 0.8,
				Content:         "content-" + memoryID,
			},
			AgentPubKey:    subPub,
			AgentSig:       subSig,
			AgentBodyHash:  subHash,
			AgentTimestamp: subTS,
		}
		require.NoError(t, tx.SignTx(parsed, proposer.priv))
		raw, encErr := tx.EncodeTx(parsed)
		require.NoError(t, encErr)
		return raw
	}
	submitActRaw := makeSubmitRaw("mem-r4-act", 2)
	submitPostRaw := makeSubmitRaw("mem-r4-post", 3)

	// --- Two independent replicas with identical genesis state. ---
	newReplica := func() *SageApp {
		app := newAppWithLogger(t, zerolog.Nop())
		// v8.2/v8.3 are already active on any chain that reaches v8.4 (their
		// own suites prove those gates); app-v5 activates below through the
		// real plan → FinalizeBlock path.
		app.v8_2AppliedHeight = 1
		app.v8_3AppliedHeight = 1
		for _, v := range vs {
			require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: v.id, Power: 1}))
		}
		return app
	}
	replicas := []*SageApp{newReplica(), newReplica()}

	// step drives one block through BOTH replicas and asserts they commit the
	// identical consensus-visible AppHash — the value CometBFT compares.
	step := func(h int64, raws [][]byte) (*abcitypes.ResponseFinalizeBlock, []byte) {
		t.Helper()
		resps := make([]*abcitypes.ResponseFinalizeBlock, len(replicas))
		for i, app := range replicas {
			resp, fbErr := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
				Height: h,
				Time:   time.Unix(ts, 0),
				Txs:    raws,
			})
			require.NoError(t, fbErr)
			for j, r := range resp.TxResults {
				require.Equal(t, uint32(0), r.Code, "replica %d, block %d, tx %d: %s", i, h, j, r.Log)
			}
			resps[i] = resp
		}
		require.Equal(t, resps[0].AppHash, resps[1].AppHash,
			"replicas executing identical blocks must commit identical AppHash at H=%d", h)
		return resps[0], resps[0].AppHash
	}

	// ---- Block 50: the propose lands; the plan is on-chain on both. ----
	_, h50 := step(50, [][]byte{proposeRaw})
	for i, app := range replicas {
		plan, planErr := app.badgerStore.GetUpgradePlan()
		require.NoError(t, planErr)
		require.NotNil(t, plan, "replica %d: plan persisted", i)
		require.Equal(t, int64(250), plan.ActivationHeight, "replica %d", i)
	}

	// ---- Block 250 (activation): the SAME block carries a domain-tagged
	// submit. Txs execute before the activation arm, so it runs PRE-fork. ----
	respAct, hAct := step(250, [][]byte{submitActRaw})
	require.NotNil(t, respAct.ConsensusParamUpdates, "activation block must bump the app version")
	require.NotNil(t, respAct.ConsensusParamUpdates.Version)
	require.Equal(t, uint64(5), respAct.ConsensusParamUpdates.Version.App)
	for i, app := range replicas {
		require.Equal(t, int64(250), app.v8_4AppliedHeight, "replica %d: gate set at activation", i)
		d, domErr := app.badgerStore.GetMemoryDomain("mem-r4-act")
		require.NoError(t, domErr)
		assert.Equal(t, "", d,
			"replica %d: the activation block's own tx must execute pre-fork — no memdomain: key", i)
	}
	assert.NotEqual(t, h50, hAct,
		"activation block moves the AppHash (applied-upgrade record + memory keys)")

	// ---- Block 251 (post-fork): the byte-identical submit shape now writes
	// the v8.4 key. ----
	_, hPost := step(251, [][]byte{submitPostRaw})
	for i, app := range replicas {
		require.True(t, app.postV8_4Fork(251), "replica %d", i)
		d, domErr := app.badgerStore.GetMemoryDomain("mem-r4-post")
		require.NoError(t, domErr)
		assert.Equal(t, "pwn_heap", d, "replica %d: post-fork submit writes memdomain:", i)
	}
	assert.NotEqual(t, hAct, hPost, "post-fork v8.4 keys must enter the digest")

	// Replay-safety: re-reading the final keyspace reproduces the hash the
	// block committed — ResponseFinalizeBlock.AppHash IS ComputeAppHash.
	hReplay, err := ComputeAppHash(replicas[0].badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hPost, hReplay,
		"ComputeAppHash must be deterministic over the post-activation keyspace")
}
