package abci

import (
	"bytes"
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// app-v17 (v11.5) — quorum-scaled two-phase challenge (C3) + memory reinstate
// (C2). C3 layers a count-based branch INSIDE the legacy TxTypeMemoryChallenge
// handler behind postAppV17Rules: with >= 2 distinct modify-verb holders on the
// memory's domain the memory is parked CHALLENGED pending a second holder's
// CONFIRM (deprecate) or any holder's REINSTATE (recommit). With <= 1 holder the
// outcome is byte-identical to the legacy one-strike deprecate. C2 adds
// TxTypeMemoryReinstate, dual-gated on postAppV17Fork.
// ---------------------------------------------------------------------------

// makeMemoryReinstateTx builds a signed-proof MemoryReinstate tx (clone of
// makeMemoryChallengeTx).
func makeMemoryReinstateTx(t *testing.T, ak agentKey, memoryID, reason string) *tx.ParsedTx {
	t.Helper()
	body := []byte(memoryID + reason)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type:            tx.TxTypeMemoryReinstate,
		MemoryReinstate: &tx.MemoryReinstate{MemoryID: memoryID, Reason: reason},
		AgentPubKey:     pubKey,
		AgentSig:        sig,
		AgentBodyHash:   bodyHash,
		AgentTimestamp:  ts,
	}
}

// ---------------------------------------------------------------------------
// ModifyVerbHolders — the deterministic quorum enumerator behind C3.
// ---------------------------------------------------------------------------

// TestAppV17_ModifyVerbHolders pins the count for 0/1/2/3 holders, ancestor-owner
// inclusion (C-D2), the level-2-is-not-modify exclusion, the shared-domain
// barrier, and the expired-grant boundary (now >= expiresAt is EXPIRED, matching
// HasAccessOrAncestor).
func TestAppV17_ModifyVerbHolders(t *testing.T) {
	app := setupTestApp(t)
	bt := time.Unix(1000, 0) // now = 1000

	count := func(domain string) []string {
		t.Helper()
		h, err := app.badgerStore.ModifyVerbHolders(domain, bt)
		require.NoError(t, err)
		return h
	}

	// 0 holders: unregistered/unowned domain, no grants.
	assert.Len(t, count("orphan"), 0, "no owner + no grants => 0 holders")
	// Shared domain is skipped entirely (cascade barrier).
	assert.Len(t, count("general"), 0, "shared domain => 0 holders")

	owner := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
	// 1 holder: the domain owner only (personal-node shape).
	assert.Equal(t, []string{owner.id}, count("hr"), "owner only => 1 holder")

	// 2 holders: + a permanent level-3 grantee.
	finance := newAgentKey(t)
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", finance.id, 3, 0, owner.id))
	assert.Len(t, count("hr"), 2, "owner + L3 grantee => 2 holders")

	// 3 holders: + a not-yet-expired level-3 grantee (expiresAt 2000 > now 1000).
	legal := newAgentKey(t)
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", legal.id, 3, 2000, owner.id))
	assert.Len(t, count("hr"), 3, "owner + 2 valid L3 grantees => 3 holders")

	// Level-2 (append) grant is NOT modify — excluded.
	appender := newAgentKey(t)
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", appender.id, 2, 0, owner.id))
	assert.Len(t, count("hr"), 3, "level-2 grant is not modify => still 3")

	// Expired L3 grant at the boundary (expiresAt == now) is excluded.
	expired := newAgentKey(t)
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", expired.id, 3, 1000, owner.id))
	assert.Len(t, count("hr"), 3, "grant expiring exactly at now is expired => still 3")

	// One second earlier the same grant would be live — boundary proof.
	assert.Len(t, count2(t, app, "hr", time.Unix(999, 0)), 4, "at now=999 the expiresAt=1000 grant is still live => 4")

	// Ancestor-owner inclusion (C-D2): a child domain inherits the parent owner
	// AND the parent's ancestor grants.
	childOwner := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr.payroll", childOwner.id, "hr", 1))
	child := count("hr.payroll")
	assert.Contains(t, child, childOwner.id, "child owner counted")
	assert.Contains(t, child, owner.id, "ancestor (hr) owner counted (C-D2)")
	assert.Contains(t, child, finance.id, "ancestor L3 grantee counted")
}

func count2(t *testing.T, app *SageApp, domain string, bt time.Time) []string {
	t.Helper()
	h, err := app.badgerStore.ModifyVerbHolders(domain, bt)
	require.NoError(t, err)
	return h
}

// ---------------------------------------------------------------------------
// C3 — challenge outcome by quorum.
// ---------------------------------------------------------------------------

// TestAppV17_ChallengePersonalNodeStillDeprecates: <= 1 modify holder => the
// legacy one-strike deprecate, byte-for-byte (no challenged state, no record).
func TestAppV17_ChallengePersonalNodeStillDeprecates(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5

	owner := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("solo", owner.id, "", 1))
	const mem = "solo-mem"
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "solo"))
	require.NoError(t, app.badgerStore.SetMemoryHash(mem, []byte("real-hash"), string(memory.StatusCommitted)))

	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "cleanup"), 10, time.Now())
	require.Equal(t, uint32(0), res.Code, "solo owner challenge succeeds: %s", res.Log)

	hash, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusDeprecated), status, "1 holder => one-strike deprecate")
	assert.Empty(t, hash, "legacy deprecate nils the hash")

	rec, err := app.badgerStore.GetChallengeRecord(mem)
	require.NoError(t, err)
	assert.Nil(t, rec, "no challenge record written on the one-strike path")
}

// TestAppV17_ChallengeParksWhenQuorum: >= 2 modify holders => CHALLENGED, the
// content hash is preserved, and the on-chain record persists the C-D3 quorum.
func TestAppV17_ChallengeParksWhenQuorum(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5

	owner := newAgentKey(t)
	finance := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", finance.id, 3, 0, owner.id))

	const mem = "hr-mem"
	realHash := []byte("original-content-hash")
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
	require.NoError(t, app.badgerStore.SetMemoryHash(mem, realHash, string(memory.StatusCommitted)))

	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "disputed"), 42, time.Now())
	require.Equal(t, uint32(0), res.Code, "2-holder challenge parks: %s", res.Log)

	hash, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusChallenged), status, ">=2 holders => challenged (not deprecated)")
	assert.Equal(t, realHash, hash, "challenge KEEPS the content hash (no husk)")

	rec, err := app.badgerStore.GetChallengeRecord(mem)
	require.NoError(t, err)
	require.NotNil(t, rec, "challenge record persisted")
	assert.Equal(t, owner.id, rec.ChallengerID)
	assert.Equal(t, "hr", rec.Domain)
	assert.Equal(t, int64(42), rec.ExecutionHeight, "C-D3: execution height persisted")
	assert.Equal(t, uint32(2), rec.QuorumCount, "C-D3: measured quorum persisted")
	assert.Equal(t, realHash, rec.PriorHash, "prior hash captured for restoration")
	assert.Equal(t, string(memory.StatusCommitted), rec.PriorStatus)
}

// TestAppV17_ChallengePreForkOneStrike (byte-identical replay): with the fork
// DORMANT, a >= 2-holder domain STILL one-strike deprecates — the C3 branch is
// fully gated, so a chain that never activates app-v17 produces the legacy state.
func TestAppV17_ChallengePreForkOneStrike(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.appV17AppliedHeight, "precondition: app-v17 dormant")

	owner := newAgentKey(t)
	finance := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", finance.id, 3, 0, owner.id))
	holders, _ := app.badgerStore.ModifyVerbHolders("hr", time.Now())
	require.Len(t, holders, 2, "precondition: 2 modify holders")

	const mem = "hr-prefork"
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
	require.NoError(t, app.badgerStore.SetMemoryHash(mem, []byte("h"), string(memory.StatusCommitted)))

	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "x"), 10, time.Now())
	require.Equal(t, uint32(0), res.Code)

	_, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusDeprecated), status, "pre-fork: 2-holder challenge STILL one-strike deprecates")
	rec, err := app.badgerStore.GetChallengeRecord(mem)
	require.NoError(t, err)
	assert.Nil(t, rec, "pre-fork: no challenge record (branch gated) => byte-identical replay")
}

// ---------------------------------------------------------------------------
// C3 — CONFIRM (second challenge on a challenged memory).
// ---------------------------------------------------------------------------

// setupChallenged parks `mem` (in domain hr with owner+finance holders) as
// challenged by `owner`, returning the two holders and the original content hash.
func setupChallenged(t *testing.T, app *SageApp) (owner, finance agentKey, mem string, hash []byte) {
	t.Helper()
	app.appV17AppliedHeight = 5
	owner = newAgentKey(t)
	finance = newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", finance.id, 3, 0, owner.id))
	mem = "hr-disputed"
	hash = []byte("committed-content-hash")
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
	require.NoError(t, app.badgerStore.SetMemoryHash(mem, hash, string(memory.StatusCommitted)))
	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "open"), 20, time.Now())
	require.Equal(t, uint32(0), res.Code, "park challenged: %s", res.Log)
	_, status, _ := app.badgerStore.GetMemoryHash(mem)
	require.Equal(t, string(memory.StatusChallenged), status)
	return owner, finance, mem, hash
}

// TestAppV17_ConfirmBySecondHolderDeprecates: a DISTINCT modify holder's second
// challenge finalizes the deprecation and clears the record.
func TestAppV17_ConfirmBySecondHolderDeprecates(t *testing.T) {
	app := setupTestApp(t)
	_, finance, mem, _ := setupChallenged(t, app)

	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, finance, mem, "confirm"), 21, time.Now())
	require.Equal(t, uint32(0), res.Code, "distinct holder confirm => deprecate: %s", res.Log)

	hash, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusDeprecated), status, "confirm => deprecated (terminal)")
	assert.Empty(t, hash, "confirm nils the hash")
	rec, err := app.badgerStore.GetChallengeRecord(mem)
	require.NoError(t, err)
	assert.Nil(t, rec, "record cleared on confirm")
}

// TestAppV17_ConfirmByOriginalChallengerRejected: the challenger cannot confirm
// their own challenge (Code 93); no state change (double-challenge is rejected).
func TestAppV17_ConfirmByOriginalChallengerRejected(t *testing.T) {
	app := setupTestApp(t)
	owner, _, mem, hash := setupChallenged(t, app)

	before, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)

	res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "self-confirm"), 22, time.Now())
	assert.Equal(t, uint32(93), res.Code, "original challenger cannot confirm/double-challenge")

	after, err := ComputeAppHash(app.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, before, after, "rejected double-challenge writes nothing (AppHash unchanged)")

	gotHash, status, _ := app.badgerStore.GetMemoryHash(mem)
	assert.Equal(t, string(memory.StatusChallenged), status, "still challenged")
	assert.Equal(t, hash, gotHash, "hash intact")
}

// ---------------------------------------------------------------------------
// C2 — REINSTATE.
// ---------------------------------------------------------------------------

// TestAppV17_ReinstateRestoresHash: a distinct holder reinstates a challenged
// memory to committed and the RESTORED hash equals the original.
func TestAppV17_ReinstateRestoresHash(t *testing.T) {
	app := setupTestApp(t)
	_, finance, mem, originalHash := setupChallenged(t, app)

	res := app.processMemoryReinstate(makeMemoryReinstateTx(t, finance, mem, "false alarm"), 23, time.Now())
	require.Equal(t, uint32(0), res.Code, "distinct holder reinstate: %s", res.Log)

	hash, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), status, "reinstate => committed")
	assert.True(t, bytes.Equal(originalHash, hash), "restored hash must equal the original (%x != %x)", hash, originalHash)

	rec, err := app.badgerStore.GetChallengeRecord(mem)
	require.NoError(t, err)
	assert.Nil(t, rec, "record cleared on reinstate")
}

// TestAppV17_ReinstateWithdrawByChallenger: the ORIGINAL challenger may always
// withdraw their own challenge via reinstate.
func TestAppV17_ReinstateWithdrawByChallenger(t *testing.T) {
	app := setupTestApp(t)
	owner, _, mem, originalHash := setupChallenged(t, app)

	res := app.processMemoryReinstate(makeMemoryReinstateTx(t, owner, mem, "withdraw"), 23, time.Now())
	require.Equal(t, uint32(0), res.Code, "original challenger may withdraw: %s", res.Log)

	hash, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), status)
	assert.Equal(t, originalHash, hash, "hash restored on withdraw")
}

// TestAppV17_ReinstateWithdrawAfterGrantRevoked pins the "always withdraw"
// promise: a grantee who opened the challenge may close it even if the grant
// that originally authorized the challenge is revoked while the dispute is
// open. The AppHash-folded ChallengerID, not current grant state, authorizes
// this one narrow withdrawal path.
func TestAppV17_ReinstateWithdrawAfterGrantRevoked(t *testing.T) {
	app := setupTestApp(t)
	app.appV17AppliedHeight = 5

	owner := newAgentKey(t)
	challenger := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("hr", challenger.id, 3, 0, owner.id))

	const mem = "hr-revoked-challenger"
	originalHash := []byte("committed-content-hash")
	require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
	require.NoError(t, app.badgerStore.SetMemoryHash(mem, originalHash, string(memory.StatusCommitted)))
	require.Equal(t, uint32(0), app.processMemoryChallenge(
		makeMemoryChallengeTx(t, challenger, mem, "open"), 20, time.Now()).Code)

	require.NoError(t, app.badgerStore.DeleteAccessGrant("hr", challenger.id))
	hasModify, err := app.badgerStore.HasAccessOrAncestor("hr", challenger.id, 3, time.Now())
	require.NoError(t, err)
	require.False(t, hasModify, "precondition: the challenger no longer holds modify")

	res := app.processMemoryReinstate(makeMemoryReinstateTx(t, challenger, mem, "withdraw"), 23, time.Now())
	require.Equal(t, uint32(0), res.Code, "original challenger may withdraw after grant revocation: %s", res.Log)

	hash, status, err := app.badgerStore.GetMemoryHash(mem)
	require.NoError(t, err)
	assert.Equal(t, string(memory.StatusCommitted), status)
	assert.Equal(t, originalHash, hash)
}

// TestAppV17_ReinstateRejections: non-challenged target, double-reinstate,
// unauthorized caller, and the pre-fork handler gate.
func TestAppV17_ReinstateRejections(t *testing.T) {
	t.Run("not challenged => Code 94", func(t *testing.T) {
		app := setupTestApp(t)
		app.appV17AppliedHeight = 5
		owner := newAgentKey(t)
		require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
		const mem = "hr-committed"
		require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
		require.NoError(t, app.badgerStore.SetMemoryHash(mem, []byte("h"), string(memory.StatusCommitted)))

		before, _ := ComputeAppHash(app.badgerStore)
		res := app.processMemoryReinstate(makeMemoryReinstateTx(t, owner, mem, "x"), 10, time.Now())
		assert.Equal(t, uint32(94), res.Code, "reinstating a non-challenged memory is rejected")
		after, _ := ComputeAppHash(app.badgerStore)
		assert.Equal(t, before, after, "rejected reinstate writes nothing")
	})

	t.Run("double reinstate => Code 94", func(t *testing.T) {
		app := setupTestApp(t)
		_, finance, mem, _ := setupChallenged(t, app)
		require.Equal(t, uint32(0), app.processMemoryReinstate(makeMemoryReinstateTx(t, finance, mem, "1"), 23, time.Now()).Code)
		// Second reinstate: memory is now committed, not challenged.
		res := app.processMemoryReinstate(makeMemoryReinstateTx(t, finance, mem, "2"), 24, time.Now())
		assert.Equal(t, uint32(94), res.Code, "double-reinstate rejected (already committed)")
	})

	t.Run("unauthorized caller => Code 92", func(t *testing.T) {
		app := setupTestApp(t)
		_, _, mem, _ := setupChallenged(t, app)
		stranger := newAgentKey(t)
		res := app.processMemoryReinstate(makeMemoryReinstateTx(t, stranger, mem, "x"), 23, time.Now())
		assert.Equal(t, uint32(92), res.Code, "non-holder cannot reinstate")
		_, status, _ := app.badgerStore.GetMemoryHash(mem)
		assert.Equal(t, string(memory.StatusChallenged), status, "still challenged")
	})

	t.Run("pre-fork handler => Code 10", func(t *testing.T) {
		app := setupTestApp(t)
		require.Equal(t, int64(0), app.appV17AppliedHeight)
		owner := newAgentKey(t)
		res := app.processMemoryReinstate(makeMemoryReinstateTx(t, owner, "any", "x"), 10, time.Now())
		assert.Equal(t, uint32(10), res.Code, "reinstate handler is inert pre-fork (byte-identical replay)")
		assert.Contains(t, res.Log, "unknown tx type")
	})
}

// TestAppV17_ReinstateCheckTxDualGate: the CheckTx gate keeps type-35 out of the
// mempool pre-fork (Code 10) and admits it post-fork — the load-bearing
// mixed-binary determinism guard, symmetric with the exec-side reject.
func TestAppV17_ReinstateCheckTxDualGate(t *testing.T) {
	app := setupTestApp(t)
	signer := newAgentKey(t)
	ptx := makeMemoryReinstateTx(t, signer, "mem-1", "withdraw")
	ptx.Nonce = 1
	require.NoError(t, tx.SignTx(ptx, signer.priv))
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)

	app.state.Height = 0
	resp, err := app.CheckTx(context.TODO(), &abcitypes.RequestCheckTx{Tx: encoded})
	require.NoError(t, err)
	assert.Equal(t, uint32(10), resp.Code, "pre-fork CheckTx rejects reinstate with Code 10")
	assert.Contains(t, resp.Log, "unknown tx type")

	app.appV17AppliedHeight = 5
	app.state.Height = 100
	resp2, err := app.CheckTx(context.TODO(), &abcitypes.RequestCheckTx{Tx: encoded})
	require.NoError(t, err)
	assert.NotEqual(t, uint32(10), resp2.Code, "post-fork CheckTx admits reinstate: %s", resp2.Log)
}

// TestAppV17_ChallengeParkGuardedToCommitted pins the priorStatus==committed guard
// on the >= 2-holder park branch: the two-phase machine must NEVER pull a
// non-committed memory into `challenged` (from which a lone holder could reinstate
// it to committed). A `deprecated` memory must stay terminally deprecated, and a
// still-`proposed` memory must not skip the validation quorum — both fall through
// to the legacy one-strike deprecate and cannot be reinstated.
func TestAppV17_ChallengeParkGuardedToCommitted(t *testing.T) {
	// Two modify-verb holders on domain hr (owner + L3 grantee) so the >= 2 arm is
	// live; without the guard both sub-cases would park CHALLENGED.
	newApp := func() (*SageApp, agentKey, agentKey) {
		app := setupTestApp(t)
		app.appV17AppliedHeight = 5
		owner := newAgentKey(t)
		finance := newAgentKey(t)
		require.NoError(t, app.badgerStore.RegisterDomain("hr", owner.id, "", 1))
		require.NoError(t, app.badgerStore.SetAccessGrant("hr", finance.id, 3, 0, owner.id))
		return app, owner, finance
	}

	t.Run("deprecated stays terminal (no resurrection)", func(t *testing.T) {
		app, owner, _ := newApp()
		const mem = "hr-dep"
		require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
		// Terminal deprecated: nil hash, memdomain: key survives (passes authz).
		require.NoError(t, app.badgerStore.SetMemoryHash(mem, nil, string(memory.StatusDeprecated)))

		res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "re-challenge"), 20, time.Now())
		require.Equal(t, uint32(0), res.Code, "challenge of a deprecated memory: %s", res.Log)

		_, status, err := app.badgerStore.GetMemoryHash(mem)
		require.NoError(t, err)
		assert.Equal(t, string(memory.StatusDeprecated), status, "deprecated is terminal — must NOT be parked challenged")

		rec, err := app.badgerStore.GetChallengeRecord(mem)
		require.NoError(t, err)
		assert.Nil(t, rec, "no challenge record — the two-phase park never opened")

		// And so it can never be reinstated to committed.
		rres := app.processMemoryReinstate(makeMemoryReinstateTx(t, owner, mem, "resurrect"), 21, time.Now())
		assert.Equal(t, uint32(94), rres.Code, "cannot reinstate a memory that was never parked challenged")
		_, status2, _ := app.badgerStore.GetMemoryHash(mem)
		assert.Equal(t, string(memory.StatusDeprecated), status2, "still deprecated after rejected reinstate")
	})

	t.Run("proposed does not bypass validation quorum", func(t *testing.T) {
		app, owner, _ := newApp()
		const mem = "hr-prop"
		require.NoError(t, app.badgerStore.SetMemoryDomain(mem, "hr"))
		// Never-voted proposed memory with attacker-chosen content.
		require.NoError(t, app.badgerStore.SetMemoryHash(mem, []byte("unvalidated"), string(memory.StatusProposed)))

		res := app.processMemoryChallenge(makeMemoryChallengeTx(t, owner, mem, "challenge"), 20, time.Now())
		require.Equal(t, uint32(0), res.Code, "challenge of a proposed memory: %s", res.Log)

		_, status, err := app.badgerStore.GetMemoryHash(mem)
		require.NoError(t, err)
		assert.Equal(t, string(memory.StatusDeprecated), status, "proposed target falls through to legacy deprecate, not parked challenged")

		rec, err := app.badgerStore.GetChallengeRecord(mem)
		require.NoError(t, err)
		assert.Nil(t, rec, "no challenge record — no path to committed via reinstate")

		rres := app.processMemoryReinstate(makeMemoryReinstateTx(t, owner, mem, "commit"), 21, time.Now())
		assert.Equal(t, uint32(94), rres.Code, "proposed content cannot be forced to committed via reinstate")
	})
}
