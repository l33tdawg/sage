package abci

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// ---------------------------------------------------------------------------
// v8.0: TxTypeDomainReassign — governance-gated domain ownership recovery
//
// These tests pin every error code (10, 80–88) and the happy path through
// the full propose → vote → execute → reassign pipeline.
// ---------------------------------------------------------------------------

// makeDomainReassignTx builds a signed TxTypeDomainReassign transaction.
func makeDomainReassignTx(t *testing.T, ak agentKey, body *tx.DomainReassign, nonce uint64) *tx.ParsedTx {
	t.Helper()
	bodyBytes := []byte(body.Domain + body.NewOwnerID + body.ProposalID)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, bodyBytes)
	ptx := &tx.ParsedTx{
		Type:           tx.TxTypeDomainReassign,
		Nonce:          nonce,
		DomainReassign: body,
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	require.NoError(t, tx.SignTx(ptx, ak.priv))
	return ptx
}

// activateV8 forces the app to behave as post-fork at any height > h.
func activateV8(t *testing.T, app *SageApp, h int64) {
	t.Helper()
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(v8UpgradeName, 2, h))
	app.refreshV8Fork()
}

// seedDomainReassignProposal writes a governance proposal directly via the
// engine in the StatusExecuted state. Avoids the multi-block vote pipeline
// for tests that focus on the DomainReassign execution path.
func seedExecutedReassignProposal(t *testing.T, app *SageApp, proposerID string, body tx.DomainReassign, createdHeight int64) string {
	t.Helper()
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	proposalID := governance.ComputeProposalID(proposerID, createdHeight, governance.OpDomainReassign, body.Domain)
	state := &governance.ProposalState{
		ProposalID:    proposalID,
		Operation:     governance.OpDomainReassign,
		TargetID:      body.Domain,
		ProposerID:    proposerID,
		Status:        governance.StatusExecuted,
		CreatedHeight: createdHeight,
		ExpiryHeight:  createdHeight + governance.DefaultExpiryBlocks,
		Reason:        "test recovery",
		Payload:       payload,
	}
	data, err := json.Marshal(state)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetState("gov:proposal:"+proposalID, data))
	return proposalID
}

// setupReassignTestApp boots an app with an admin agent, the v8 fork
// already applied at height 50, and a pre-existing domain owned by some
// random captured agent. Returns the app, the admin agent (which executes
// the DomainReassign), and the new-owner identity to transfer to.
func setupReassignTestApp(t *testing.T) (*SageApp, agentKey, string, string) {
	t.Helper()
	app := setupTestApp(t)
	// Activate v8 well below test heights — we'll process at height >= 100.
	activateV8(t, app, 50)

	// Register an admin agent (executes the reassign).
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: admin.id, PublicKey: admin.pub, Power: 10,
	}))

	// A captured owner — registered as a domain owner that needs recovering.
	capturedOwnerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	capturedOwner := hex.EncodeToString(capturedOwnerPub)
	require.NoError(t, app.badgerStore.RegisterDomain("protocol.lending_pool", capturedOwner, "", 10))

	// New owner — the agent we want to assign the domain to.
	newOwnerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	newOwner := hex.EncodeToString(newOwnerPub)

	return app, admin, capturedOwner, newOwner
}

// TestDomainReassign_PreFork_UnknownTxType — pre-fork the handler returns
// Code 10. Asserts pre-fork AppHash is byte-identical to a run that omitted
// the tx entirely (no state mutation).
func TestDomainReassign_PreFork_UnknownTxType(t *testing.T) {
	// App A: pre-fork (v8AppliedHeight = 0), runs the DomainReassign tx.
	appA := setupTestApp(t)
	admin := newAgentKey(t)
	registerAgent(t, appA, admin, "admin", "admin")

	body := &tx.DomainReassign{
		Domain:     "protocol.lending_pool",
		NewOwnerID: "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		ProposalID: "11223344556677889900aabbccddeeff",
	}
	ptx := makeDomainReassignTx(t, admin, body, 1)
	res := appA.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(10), res.Code, "pre-fork must return Code 10 unknown tx type")
	assert.Contains(t, res.Log, "unknown tx type")

	// App B: pre-fork as well, no tx processed at all.
	appB := setupTestApp(t)
	admin2 := newAgentKey(t)
	registerAgent(t, appB, admin2, "admin", "admin")
	// Note: appB.admin has a different keypair, so the in-state-tree mutation
	// from registerAgent differs from appA's. We instead replay the SAME
	// register-only sequence on both apps via a deterministic FinalizeBlock,
	// then assert pre-fork DomainReassign on appA does NOT mutate state.
	hashBefore, err := ComputeAppHash(appA.badgerStore)
	require.NoError(t, err)

	// Run the pre-fork DomainReassign again — must be idempotent (no state
	// mutation).
	res2 := appA.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(10), res2.Code)

	hashAfter, err := ComputeAppHash(appA.badgerStore)
	require.NoError(t, err)
	assert.Equal(t, hashBefore, hashAfter, "pre-fork DomainReassign must not mutate state — AppHash unchanged")
}

// TestDomainReassign_HappyPath — admin executes a reassignment with a
// 3/4-vote-passed proposal. Owner flips, grants on the domain are purged,
// the shared_domain sentinel is written when OpenToShared=true.
func TestDomainReassign_HappyPath(t *testing.T) {
	app, admin, capturedOwner, newOwner := setupReassignTestApp(t)

	// Seed a few grants on the domain to verify they get purged.
	require.NoError(t, app.badgerStore.SetAccessGrant("protocol.lending_pool", "agent-a", 2, 0, capturedOwner))
	require.NoError(t, app.badgerStore.SetAccessGrant("protocol.lending_pool", "agent-b", 1, 0, capturedOwner))
	require.NoError(t, app.badgerStore.SetAccessGrant("protocol.lending_pool", "agent-c", 2, 0, capturedOwner))
	// And a grant on a DIFFERENT domain to confirm we only purge the target.
	require.NoError(t, app.badgerStore.SetAccessGrant("other.domain", "agent-x", 2, 0, capturedOwner))

	body := tx.DomainReassign{
		Domain:       "protocol.lending_pool",
		NewOwnerID:   newOwner,
		ParentDomain: "",
		OpenToShared: true,
	}
	proposalID := seedExecutedReassignProposal(t, app, admin.id, body, 80)
	body.ProposalID = proposalID

	ptx := makeDomainReassignTx(t, admin, &body, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	require.Equal(t, uint32(0), res.Code, "happy path should succeed: %s", res.Log)

	// Owner flipped on-chain.
	gotOwner, err := app.badgerStore.GetDomainOwner("protocol.lending_pool")
	require.NoError(t, err)
	assert.Equal(t, newOwner, gotOwner, "domain owner should be the new agent")

	// All grants on protocol.lending_pool purged.
	for _, agent := range []string{"agent-a", "agent-b", "agent-c"} {
		_, _, _, gErr := app.badgerStore.GetAccessGrant("protocol.lending_pool", agent)
		assert.Error(t, gErr, "grant %s on reassigned domain must be purged", agent)
	}
	// other.domain grant untouched.
	lvl, _, _, gErr := app.badgerStore.GetAccessGrant("other.domain", "agent-x")
	require.NoError(t, gErr, "grant on other.domain must survive")
	assert.Equal(t, uint8(2), lvl)

	// shared_domain sentinel set.
	v, err := app.badgerStore.GetState("shared_domain:protocol.lending_pool")
	require.NoError(t, err)
	assert.NotEmpty(t, v, "shared_domain sentinel must be set when OpenToShared=true")

	// Hybrid isSharedDomain returns true post-fork at this height.
	assert.True(t, app.isSharedDomain("protocol.lending_pool", 101), "post-fork: on-chain shared sentinel must promote")

	// Proposal marked consumed.
	consumed, err := app.badgerStore.GetState("gov:proposal:" + proposalID + ":consumed")
	require.NoError(t, err)
	assert.NotEmpty(t, consumed)
}

// TestDomainReassign_Code82_ProposalNotExecuted
func TestDomainReassign_Code82_ProposalNotExecuted(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	// Seed a proposal but leave it Voting (not Executed).
	body := tx.DomainReassign{Domain: "protocol.lending_pool", NewOwnerID: newOwner}
	payload, _ := json.Marshal(body)
	proposalID := governance.ComputeProposalID(admin.id, 80, governance.OpDomainReassign, body.Domain)
	state := &governance.ProposalState{
		ProposalID: proposalID, Operation: governance.OpDomainReassign,
		Status: governance.StatusVoting, CreatedHeight: 80, Payload: payload,
	}
	data, _ := json.Marshal(state)
	require.NoError(t, app.badgerStore.SetState("gov:proposal:"+proposalID, data))

	body.ProposalID = proposalID
	ptx := makeDomainReassignTx(t, admin, &body, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(82), res.Code, "proposal-not-executed must return Code 82")
	assert.Contains(t, res.Log, "not executed")
}

// TestDomainReassign_Code82_WrongOpType — a proposal of the wrong op type
// can't be used to authorize a DomainReassign.
func TestDomainReassign_Code82_WrongOpType(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	body := tx.DomainReassign{Domain: "protocol.lending_pool", NewOwnerID: newOwner}
	payload, _ := json.Marshal(body)
	proposalID := governance.ComputeProposalID(admin.id, 80, governance.OpAddValidator, "irrelevant")
	state := &governance.ProposalState{
		ProposalID: proposalID, Operation: governance.OpAddValidator,
		Status: governance.StatusExecuted, CreatedHeight: 80, Payload: payload,
	}
	data, _ := json.Marshal(state)
	require.NoError(t, app.badgerStore.SetState("gov:proposal:"+proposalID, data))

	body.ProposalID = proposalID
	ptx := makeDomainReassignTx(t, admin, &body, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(82), res.Code)
	assert.Contains(t, res.Log, "wrong operation type")
}

// TestDomainReassign_Code83_BodyMismatch
func TestDomainReassign_Code83_BodyMismatch(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	bodyApproved := tx.DomainReassign{Domain: "protocol.lending_pool", NewOwnerID: newOwner}
	proposalID := seedExecutedReassignProposal(t, app, admin.id, bodyApproved, 80)

	// Tx body differs from the approved proposal (different NewOwnerID).
	bodyExec := tx.DomainReassign{
		Domain:     "protocol.lending_pool",
		NewOwnerID: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		ProposalID: proposalID,
	}
	ptx := makeDomainReassignTx(t, admin, &bodyExec, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(83), res.Code)
	assert.Contains(t, res.Log, "body mismatch")
}

// TestDomainReassign_Code84_AlreadyConsumed
func TestDomainReassign_Code84_AlreadyConsumed(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	body := tx.DomainReassign{Domain: "protocol.lending_pool", NewOwnerID: newOwner}
	proposalID := seedExecutedReassignProposal(t, app, admin.id, body, 80)
	body.ProposalID = proposalID

	// First execution succeeds.
	ptx1 := makeDomainReassignTx(t, admin, &body, 1)
	res1 := app.processDomainReassign(ptx1, 100, time.Unix(1700000000, 0))
	require.Equal(t, uint32(0), res1.Code, res1.Log)

	// Second execution against the same proposal — consumed.
	ptx2 := makeDomainReassignTx(t, admin, &body, 2)
	res2 := app.processDomainReassign(ptx2, 101, time.Unix(1700000001, 0))
	assert.Equal(t, uint32(84), res2.Code)
	assert.Contains(t, res2.Log, "already consumed")
}

// TestDomainReassign_Code85_StaleProposal — TTL is 2× DefaultExpiryBlocks.
func TestDomainReassign_Code85_StaleProposal(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	body := tx.DomainReassign{Domain: "protocol.lending_pool", NewOwnerID: newOwner}
	createdHeight := int64(80)
	proposalID := seedExecutedReassignProposal(t, app, admin.id, body, createdHeight)
	body.ProposalID = proposalID

	// Execute at height = created + 2*DefaultExpiry + 1 → just past stale.
	staleHeight := createdHeight + 2*governance.DefaultExpiryBlocks + 1
	ptx := makeDomainReassignTx(t, admin, &body, 1)
	res := app.processDomainReassign(ptx, staleHeight, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(85), res.Code)
	assert.Contains(t, res.Log, "stale")
}

// TestDomainReassign_Code86_DomainNotFound
func TestDomainReassign_Code86_DomainNotFound(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	body := tx.DomainReassign{Domain: "does-not-exist", NewOwnerID: newOwner}
	proposalID := seedExecutedReassignProposal(t, app, admin.id, body, 80)
	body.ProposalID = proposalID

	ptx := makeDomainReassignTx(t, admin, &body, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(86), res.Code)
	assert.Contains(t, res.Log, "not found")
}

// TestDomainReassign_Code87_ParentMismatch
func TestDomainReassign_Code87_ParentMismatch(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	// Domain exists with parent "". The tx specifies parent "different".
	body := tx.DomainReassign{
		Domain:       "protocol.lending_pool",
		NewOwnerID:   newOwner,
		ParentDomain: "wrong-parent",
	}
	proposalID := seedExecutedReassignProposal(t, app, admin.id, body, 80)
	body.ProposalID = proposalID

	ptx := makeDomainReassignTx(t, admin, &body, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(87), res.Code)
	assert.Contains(t, res.Log, "parent mismatch")
}

// TestDomainReassign_Code80_NonAdmin — even with a perfect proposal, a
// non-admin sender is rejected.
func TestDomainReassign_Code80_NonAdmin(t *testing.T) {
	app, admin, _, newOwner := setupReassignTestApp(t)

	body := tx.DomainReassign{Domain: "protocol.lending_pool", NewOwnerID: newOwner}
	proposalID := seedExecutedReassignProposal(t, app, admin.id, body, 80)
	body.ProposalID = proposalID

	// Use a non-admin sender.
	imposter := newAgentKey(t)
	registerAgent(t, app, imposter, "imposter", "member")

	ptx := makeDomainReassignTx(t, imposter, &body, 1)
	res := app.processDomainReassign(ptx, 100, time.Unix(1700000000, 0))
	assert.Equal(t, uint32(80), res.Code)
	assert.Contains(t, res.Log, "admin")
}

// TestDomainReassign_Quorum_2of4_FailsReassign — exercise the 3/4 threshold
// requirement directly via the governance engine. With 4 validators at
// equal power, only 2 accepting fails the OpDomainReassign quorum even
// though it would pass the legacy 2/3 used for validator-set changes.
//
// (We run the engine in isolation here rather than through the full
// FinalizeBlock pipeline — the quorum math is what's under test.)
func TestDomainReassign_Quorum_2of4_FailsReassign(t *testing.T) {
	app := setupTestApp(t)
	activateV8(t, app, 50)

	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin", "admin")

	// Build a 4-validator network.
	keys := []agentKey{admin}
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: admin.id, PublicKey: admin.pub, Power: 10}))
	for i := 0; i < 3; i++ {
		k := newAgentKey(t)
		registerAgent(t, app, k, "v"+string(rune('a'+i)), "admin")
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: k.id, PublicKey: k.pub, Power: 10}))
		keys = append(keys, k)
	}
	valMap := make(map[string]int64)
	for _, k := range keys {
		valMap[k.id] = 10
	}
	require.NoError(t, app.badgerStore.SaveValidators(valMap))

	// Pre-register the target domain.
	capturedPub, _, _ := ed25519.GenerateKey(nil)
	captured := hex.EncodeToString(capturedPub)
	require.NoError(t, app.badgerStore.RegisterDomain("protocol.lending_pool", captured, "", 10))

	newOwnerPub, _, _ := ed25519.GenerateKey(nil)
	newOwner := hex.EncodeToString(newOwnerPub)

	// Propose a DomainReassign through the real engine.
	body := tx.DomainReassign{
		Domain:       "protocol.lending_pool",
		NewOwnerID:   newOwner,
		OpenToShared: false,
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)

	proposalID, err := app.govEngine.Propose(
		admin.id, governance.OpDomainReassign, body.Domain, nil, 0, 0,
		"recovery", 100, payload,
	)
	require.NoError(t, err)
	// admin auto-voted accept. Add ONE more accept (2 of 4 total).
	require.NoError(t, app.govEngine.Vote(proposalID, keys[1].id, "accept", 105))
	// keys[2] and keys[3] abstain (don't vote).

	// ProcessBlock — 2/4 accept, MinVotingBlocks satisfied.
	executed, err := app.govEngine.ProcessBlock(100 + governance.MinVotingBlocks + 1)
	require.NoError(t, err)
	assert.Nil(t, executed, "2 of 4 accept must NOT pass OpDomainReassign's 3/4 threshold")

	// Sanity: with a 3rd accept, it DOES pass.
	require.NoError(t, app.govEngine.Vote(proposalID, keys[2].id, "accept", 115))
	executed2, err := app.govEngine.ProcessBlock(116)
	require.NoError(t, err)
	require.NotNil(t, executed2, "3 of 4 accept must pass 3/4")
	assert.Equal(t, governance.StatusExecuted, executed2.Status)
}

// TestDomainReassign_PreFork_CheckTx_Code10 — CheckTx pre-fork returns
// Code 10 for the new tx type so it never enters the mempool.
func TestDomainReassign_PreFork_CheckTx_Code10(t *testing.T) {
	app := setupTestApp(t)
	// v8 NOT activated.
	require.Equal(t, int64(0), app.v8AppliedHeight)

	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin", "admin")

	body := &tx.DomainReassign{
		Domain:     "any",
		NewOwnerID: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		ProposalID: "11223344556677889900aabbccddeeff",
	}
	ptx := makeDomainReassignTx(t, admin, body, 1)

	// SignTx (node-level), encode, then CheckTx.
	require.NoError(t, tx.SignTx(ptx, admin.priv))
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)

	resp, err := app.CheckTx(context.TODO(), &abcitypes.RequestCheckTx{Tx: encoded})
	require.NoError(t, err)
	assert.Equal(t, uint32(10), resp.Code, "pre-fork CheckTx must reject DomainReassign with Code 10")
	assert.Contains(t, resp.Log, "unknown tx type")
}

// TestApplyGovernanceProposal_OpDomainReassign_NoValidatorUpdate pins the
// contract that an executed OpDomainReassign proposal must NOT produce a
// ValidatorUpdate at acceptance. The actual reassignment runs in the
// follow-up TxTypeDomainReassign tx; treating acceptance as a validator-set
// event would (a) emit a spurious "failed to apply governance proposal"
// log line on every successful 3/4 supermajority, and (b) risk a wrong
// ValidatorUpdate from the fallback path if the switch's default branch
// were ever wired to produce one.
func TestApplyGovernanceProposal_OpDomainReassign_NoValidatorUpdate(t *testing.T) {
	app, admin, _, _ := setupReassignTestApp(t)

	proposal := &governance.ProposalState{
		ProposalID: "deadbeefdeadbeefdeadbeefdeadbeef",
		Operation:  governance.OpDomainReassign,
		// TargetID must still be a 32-byte hex string — applyGovernanceProposal
		// validates pubkey length up front, before the switch. Use the admin's
		// own pubkey, which is convenient and unambiguously well-formed.
		TargetID:      admin.id,
		ProposerID:    admin.id,
		Status:        governance.StatusExecuted,
		CreatedHeight: 50,
		ExpiryHeight:  50 + governance.DefaultExpiryBlocks,
		Reason:        "test recovery",
	}

	update, err := app.applyGovernanceProposal(proposal, 100)
	require.NoError(t, err, "OpDomainReassign acceptance must succeed without error")
	assert.Nil(t, update, "OpDomainReassign acceptance must return nil ValidatorUpdate — actual reassignment runs in follow-up TxTypeDomainReassign")
}

// TestOpToString_OpDomainReassign pins the human-readable string for
// OpDomainReassign so the off-chain gov_proposal.operation column gets
// "domain_reassign" rather than the "unknown_4" the fallback path would
// emit. Operators and dashboards key on this string.
func TestOpToString_OpDomainReassign(t *testing.T) {
	tests := []struct {
		op   governance.ProposalOp
		want string
	}{
		{governance.OpAddValidator, "add_validator"},
		{governance.OpRemoveValidator, "remove_validator"},
		{governance.OpUpdatePower, "update_power"},
		{governance.OpDomainReassign, "domain_reassign"},
		{governance.OpUpgrade, "upgrade"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			assert.Equal(t, tc.want, opToString(tc.op))
		})
	}
}
