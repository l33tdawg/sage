package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// app-v8 also closes B-NEW-2: tx outer signatures are verified in the CONSENSUS
// path (processTx), not only in CheckTx. A Byzantine block proposer can place
// txs that never passed an honest node's CheckTx; without this a forged
// UpgradePropose/GovVote bearing a victim's PublicKey (signed by the attacker)
// would execute, letting one proposer forge the 2/3 quorum app-v8 relies on.
// These tests pin the gate, its replay-safety, and the forged-vote defense.

// corruptSig returns a copy with the outer signature's first byte flipped so
// tx.VerifyTx fails while the agent proof (AgentSig) stays valid.
func corruptOuterSig(t *testing.T, ptx *tx.ParsedTx) *tx.ParsedTx {
	t.Helper()
	require.NoError(t, tx.SignTx(ptx, deterministicAgentKey(t).priv)) // give it a real (then-broken) sig
	require.NotEmpty(t, ptx.Signature)
	ptx.Signature[0] ^= 0xFF
	return ptx
}

// TestAppV8_ConsensusRejectsForgedSig_PostFork: post-fork, a tx whose outer
// signature does not match its PublicKey is rejected (Code 2) BEFORE any handler
// runs — even though its agent proof is intact.
func TestAppV8_ConsensusRejectsForgedSig_PostFork(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	ptx := makeUpgradeProposeTx(t, admin, "app-v9", 9, "", 200)
	corruptOuterSig(t, ptx) // PublicKey now = deterministicAgentKey, sig broken

	res := app.processTx(ptx, 10, time.Now()) // height 10 > gate 5 => post-fork
	assert.Equal(t, uint32(2), res.Code, "forged-signature tx must be rejected in the consensus path")
	assert.Contains(t, res.Log, "invalid tx signature")
}

// TestAppV8_ConsensusAcceptsValidSig_PostFork: a correctly-signed tx passes the
// gate and reaches its handler (here, creating the gov proposal → Code 0).
func TestAppV8_ConsensusAcceptsValidSig_PostFork(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)

	ptx := makeUpgradeProposeTx(t, admin, "app-v9", 9, "", 200)
	require.NoError(t, tx.SignTx(ptx, admin.priv)) // valid outer signature

	res := app.processTx(ptx, 10, time.Now())
	assert.NotEqual(t, uint32(2), res.Code, "validly-signed tx must pass the signature gate")
	assert.Equal(t, uint32(0), res.Code, "valid post-fork propose reaches the handler: %s", res.Log)
}

// TestAppV8_PreForkSkipsConsensusSigVerify: PRE-fork the gate is dormant, so the
// SAME forged-outer-signature tx is NOT rejected by it — it reaches the handler
// (and self-activates via the agent proof) exactly as before app-v8. Replay
// parity: historical blocks see no new reject.
func TestAppV8_PreForkSkipsConsensusSigVerify(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV8AppliedHeight, "pre-fork")

	// Build a propose with a VALID agent proof but a corrupted OUTER signature.
	ak := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, ak, "app-v8", 8, "", 200)
	require.NoError(t, tx.SignTx(ptx, ak.priv))
	ptx.Signature[0] ^= 0xFF // break the outer sig only

	res := app.processTx(ptx, 10, time.Now()) // pre-fork: gate skipped
	assert.NotEqual(t, uint32(2), res.Code, "pre-fork must NOT reject on the outer signature (gate dormant)")
	assert.Equal(t, uint32(0), res.Code, "pre-fork self-activates via the agent proof, as before app-v8")
}

// TestAppV8_ConsensusRejectsForgedGovVote_PostFork demonstrates the concrete
// attack the gate closes: a forged GovVote (victim validator's PublicKey, signed
// by the attacker) is rejected before processGovVote can count it — so a lone
// proposer cannot fabricate the 2/3 quorum.
func TestAppV8_ConsensusRejectsForgedGovVote_PostFork(t *testing.T) {
	app, _, val2, _ := setupAppV8Chain(t, 5)

	body := []byte("some-proposal-id")
	pub, sig, bh, ts := signAgentProof(t, val2, body)
	vote := &tx.ParsedTx{
		Type:           tx.TxTypeGovVote,
		GovVote:        &tx.GovVote{ProposalID: "some-proposal-id", Decision: tx.VoteDecisionAccept},
		AgentPubKey:    pub,
		AgentSig:       sig,
		AgentBodyHash:  bh,
		AgentTimestamp: ts,
	}
	require.NoError(t, tx.SignTx(vote, val2.priv))
	vote.Signature[0] ^= 0xFF // forge: break the outer signature

	res := app.processTx(vote, 20, time.Now())
	assert.Equal(t, uint32(2), res.Code, "forged GovVote must be rejected before it can be counted")
}
