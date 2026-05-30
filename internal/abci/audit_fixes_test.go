package abci

import (
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

// Regression tests for the poe-drift-audit fixes folded into v8.4:
//   - re-submit of a terminal memoryID is rejected post-v8.4 (no verdict double-
//     credit / reputation gaming), but the legacy reset-to-proposed behavior is
//     preserved pre-v8.4 for replay parity;
//   - the PoE fork gates are reconciled to be monotonic (nearest-above backfill)
//     so a version jump can't leave a higher fork active while a lower one is off.
// (The "becameTerminal only on a successful status write" fix is a defensive
// error-path gate not cleanly injectable here — covered by the happy-path quorum
// tests, which still commit+credit, plus inspection.)

// rawSubmitDomain submits through processMemorySubmit and returns the raw result
// WITHOUT asserting success, so the re-submit guard can be exercised.
func rawSubmitDomain(t *testing.T, app *SageApp, ak agentKey, memoryID, domain string, height int64) *abcitypes.ExecTxResult {
	t.Helper()
	body := []byte(memoryID + domain)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        memoryID,
			MemoryType:      tx.MemoryTypeObservation,
			DomainTag:       domain,
			ConfidenceScore: 0.8,
			Content:         "content-" + memoryID,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	return app.processMemorySubmit(parsed, height, time.Unix(height, 0))
}

func addFourValidators(t *testing.T, app *SageApp) []agentKey {
	t.Helper()
	vs := make([]agentKey, 4)
	for i := range vs {
		vs[i] = newAgentKey(t)
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: vs[i].id, Power: 1}))
	}
	return vs
}

// Post-v8.4: re-submitting a committed memoryID is rejected and credits nothing
// more — closing the double-credit reputation-gaming vector.
func TestAuditFix_ReSubmitTerminalRejectedPostV84(t *testing.T) {
	app := newAppWithLogger(t, zerolog.Nop())
	app.v8_2AppliedHeight = 1
	app.v8_3AppliedHeight = 1
	app.v8_4AppliedHeight = 1
	vs := addFourValidators(t, app)
	proposer := vs[0]
	const memID = "mem-resub"

	require.Equal(t, uint32(0), rawSubmitDomain(t, app, proposer, memID, "pwn_heap", 10).Code)
	castVote(t, app, vs[3], memID, false, 10)
	castVote(t, app, vs[0], memID, true, 10)
	castVote(t, app, vs[1], memID, true, 10)
	castVote(t, app, vs[2], memID, true, 10) // committing vote, all four tallied
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID))

	corrBefore := vstatsOf(t, app, vs[0].id).CorrCount
	require.Equal(t, uint64(1), corrBefore, "credited once on the first commit")

	// Re-submit the committed memoryID → rejected, status untouched.
	res := rawSubmitDomain(t, app, proposer, memID, "pwn_heap", 20)
	assert.NotEqual(t, uint32(0), res.Code, "re-submit of a committed memory must be rejected")
	assert.Contains(t, res.Log, "terminal")
	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID), "status stays committed")

	// A replayed vote now sees an already-committed memory (priorStatus != proposed)
	// → the verdict credit does not re-fire.
	castVote(t, app, vs[3], memID, false, 21)
	assert.Equal(t, corrBefore, vstatsOf(t, app, vs[0].id).CorrCount, "no double-credit after rejected re-submit")
}

// Pre-v8.4: the guard is off — the legacy reset-to-proposed behavior is preserved
// so v8.2/v8.3 blocks replay byte-identical.
func TestAuditFix_ReSubmitAllowedPreV84(t *testing.T) {
	app := newAppWithLogger(t, zerolog.Nop())
	app.v8_2AppliedHeight = 1
	app.v8_3AppliedHeight = 1 // v8.4 NOT active
	require.False(t, app.postV8_4Fork(20))
	vs := addFourValidators(t, app)
	proposer := vs[0]
	const memID = "mem-resub-pre"

	require.Equal(t, uint32(0), rawSubmitDomain(t, app, proposer, memID, "pwn_heap", 10).Code)
	for _, v := range vs {
		castVote(t, app, v, memID, true, 10)
	}
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID))

	res := rawSubmitDomain(t, app, proposer, memID, "pwn_heap", 20)
	assert.Equal(t, uint32(0), res.Code, "pre-v8.4: re-submit accepted (legacy behavior preserved)")
	assert.Equal(t, string(memory.StatusProposed), statusOf(t, app, memID), "pre-v8.4: re-submit resets to proposed")
}

// PoE fork gates are reconciled to be monotonic: an unset gate inherits the
// NEAREST set gate above it, so a version jump can't leave a higher fork active
// while a lower one stays off.
func TestAuditFix_PoEForkMonotonicReconcile(t *testing.T) {
	// Jump straight to app-v6: only v8_5 set → all lower gates backfill to it.
	app := setupTestApp(t)
	app.v8_5AppliedHeight = 500
	app.reconcilePoEForkMonotonicity()
	assert.Equal(t, int64(500), app.v8AppliedHeight)
	assert.Equal(t, int64(500), app.v8_2AppliedHeight)
	assert.Equal(t, int64(500), app.v8_3AppliedHeight)
	assert.Equal(t, int64(500), app.v8_4AppliedHeight)

	// Partial skip: v8_2=200 and v8_5=500 set, v8 + v8_3 + v8_4 unset. v8 must
	// inherit the NEAREST set gate above (v8_2=200), NOT the top (v8_5=500) —
	// keeps heights non-decreasing so postV8Fork ⊇ postV8_2Fork.
	app2 := setupTestApp(t)
	app2.v8_2AppliedHeight = 200
	app2.v8_5AppliedHeight = 500
	app2.reconcilePoEForkMonotonicity()
	assert.Equal(t, int64(200), app2.v8AppliedHeight, "v8 inherits nearest-above v8_2 (200), not v8_5")
	assert.Equal(t, int64(200), app2.v8_2AppliedHeight)
	assert.Equal(t, int64(500), app2.v8_3AppliedHeight)
	assert.Equal(t, int64(500), app2.v8_4AppliedHeight)
	assert.Equal(t, int64(500), app2.v8_5AppliedHeight)

	// Sequential chain (every real chain): each gate set with its own height → untouched.
	app3 := setupTestApp(t)
	app3.v8AppliedHeight, app3.v8_2AppliedHeight, app3.v8_3AppliedHeight, app3.v8_4AppliedHeight, app3.v8_5AppliedHeight = 100, 200, 300, 400, 500
	app3.reconcilePoEForkMonotonicity()
	assert.Equal(t, int64(100), app3.v8AppliedHeight)
	assert.Equal(t, int64(200), app3.v8_2AppliedHeight)
	assert.Equal(t, int64(300), app3.v8_3AppliedHeight)
	assert.Equal(t, int64(400), app3.v8_4AppliedHeight)
	assert.Equal(t, int64(500), app3.v8_5AppliedHeight)

	// No forks active → no-op.
	app4 := setupTestApp(t)
	app4.reconcilePoEForkMonotonicity()
	assert.Zero(t, app4.v8AppliedHeight)
	assert.Zero(t, app4.v8_4AppliedHeight)
	assert.Zero(t, app4.v8_5AppliedHeight)
}
