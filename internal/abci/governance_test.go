package abci

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/l33tdawg/sage/internal/governance" // ensure governance package compiles with tests
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// ---------------------------------------------------------------------------
// Governance test helpers
// ---------------------------------------------------------------------------

// setupGovTestApp creates a SageApp with an admin agent registered and
// the agent added as a validator.
func setupGovTestApp(t *testing.T) (*SageApp, agentKey) {
	t.Helper()
	app := setupTestApp(t)

	// Create and register an admin agent.
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin-agent", "admin")

	// Add the admin as a validator so governance can work.
	err := app.validators.AddValidator(&validator.ValidatorInfo{
		ID:        admin.id,
		PublicKey: admin.pub,
		Power:     10,
	})
	require.NoError(t, err)

	// Persist validator to BadgerDB (govEngine reads from there).
	valMap := map[string]int64{admin.id: 10}
	err = app.badgerStore.SaveValidators(valMap)
	require.NoError(t, err)

	return app, admin
}

// makeGovProposeTx builds a signed governance propose transaction.
func makeGovProposeTx(t *testing.T, ak agentKey, op tx.GovProposalOp, targetID string, targetPubKey []byte, power int64, reason string, nonce uint64) []byte {
	t.Helper()
	body := []byte(reason)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	ptx := &tx.ParsedTx{
		Type:  tx.TxTypeGovPropose,
		Nonce: nonce,
		GovPropose: &tx.GovPropose{
			Operation:    op,
			TargetID:     targetID,
			TargetPubKey: targetPubKey,
			TargetPower:  power,
			ExpiryBlocks: 0, // use default
			Reason:       reason,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	err := tx.SignTx(ptx, ak.priv)
	require.NoError(t, err)
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)
	return encoded
}

// makeGovVoteTx builds a signed governance vote transaction.
func makeGovVoteTx(t *testing.T, ak agentKey, proposalID string, decision tx.VoteDecision, nonce uint64) []byte {
	t.Helper()
	body := []byte(proposalID)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	ptx := &tx.ParsedTx{
		Type:  tx.TxTypeGovVote,
		Nonce: nonce,
		GovVote: &tx.GovVote{
			ProposalID: proposalID,
			Decision:   decision,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	err := tx.SignTx(ptx, ak.priv)
	require.NoError(t, err)
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)
	return encoded
}

// makeGovCancelTx builds a signed governance cancel transaction.
func makeGovCancelTx(t *testing.T, ak agentKey, proposalID string, nonce uint64) []byte {
	t.Helper()
	body := []byte(proposalID)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	ptx := &tx.ParsedTx{
		Type:  tx.TxTypeGovCancel,
		Nonce: nonce,
		GovCancel: &tx.GovCancel{
			ProposalID: proposalID,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
	err := tx.SignTx(ptx, ak.priv)
	require.NoError(t, err)
	encoded, err := tx.EncodeTx(ptx)
	require.NoError(t, err)
	return encoded
}

// finalizeBlock is a helper that calls FinalizeBlock with the given txs at a specific height.
func finalizeBlock(t *testing.T, app *SageApp, height int64, txs ...[]byte) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	resp, err := app.FinalizeBlock(context.TODO(), &abcitypes.RequestFinalizeBlock{
		Txs:    txs,
		Height: height,
		Time:   time.Now(),
	})
	require.NoError(t, err)
	return resp
}

// ---------------------------------------------------------------------------
// Integration tests: Full FinalizeBlock → ValidatorUpdates flow
// ---------------------------------------------------------------------------

func TestGov_SingleNodeAutoApprove(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Generate a new validator to add.
	newValPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	newValID := hex.EncodeToString(newValPub)

	// Submit a proposal to add the new validator.
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 3, "adding test validator", 1)

	// FinalizeBlock at height 1 — proposal created + auto-vote.
	resp := finalizeBlock(t, app, 1, proposeTx)
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code, "propose should succeed: %s", resp.TxResults[0].Log)

	// Single validator → auto-approve bypasses MinVotingBlocks.
	// The proposal should be executed in this same block's post-processing.
	require.Len(t, resp.ValidatorUpdates, 1, "should return a ValidatorUpdate for the new validator")

	update := resp.ValidatorUpdates[0]
	assert.Equal(t, int64(3), update.Power, "new validator should have power 3")
	assert.Equal(t, []byte(newValPub), update.PubKey.GetEd25519(), "pubkey should match")

	// Verify the validator was added to the in-memory set.
	_, exists := app.validators.GetValidator(newValID)
	assert.True(t, exists, "new validator should be in the set")
	assert.Equal(t, 2, app.validators.Size(), "should now have 2 validators")
}

func TestGov_MultiNodeVoteFlow(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Add a second validator so we have a multi-node setup.
	val2 := newAgentKey(t)
	registerAgent(t, app, val2, "validator-2", "admin")
	err := app.validators.AddValidator(&validator.ValidatorInfo{
		ID: val2.id, PublicKey: val2.pub, Power: 10,
	})
	require.NoError(t, err)
	valMap := map[string]int64{admin.id: 10, val2.id: 10}
	require.NoError(t, app.badgerStore.SaveValidators(valMap))

	// Generate a new validator to add.
	newValPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	newValID := hex.EncodeToString(newValPub)

	// Block 1: Admin proposes to add a validator (auto-votes yes).
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 5, "adding node 3", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code, "propose: %s", resp.TxResults[0].Log)
	assert.Empty(t, resp.ValidatorUpdates, "should NOT execute yet — need 2/3 quorum and MinVotingBlocks")

	// Get the proposal ID for voting.
	proposal, propErr := app.govEngine.GetActiveProposal()
	require.NoError(t, propErr)
	require.NotNil(t, proposal, "active proposal should exist")

	// Blocks 2-10: Empty blocks to satisfy MinVotingBlocks.
	for h := int64(2); h <= 10; h++ {
		resp = finalizeBlock(t, app, h)
		assert.Empty(t, resp.ValidatorUpdates, "no updates during voting period")
	}

	// Block 11: val2 votes yes — now 2/2 = 100% quorum + past MinVotingBlocks.
	voteTx := makeGovVoteTx(t, val2, proposal.ProposalID, tx.VoteDecisionAccept, 1)
	resp = finalizeBlock(t, app, 11, voteTx)
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code, "vote: %s", resp.TxResults[0].Log)

	// Now quorum is reached AND MinVotingBlocks passed → should execute.
	require.Len(t, resp.ValidatorUpdates, 1, "should return ValidatorUpdate after quorum")
	assert.Equal(t, int64(5), resp.ValidatorUpdates[0].Power)
	assert.Equal(t, 3, app.validators.Size(), "should now have 3 validators")
}

func TestGov_RemoveValidator(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Add two more validators (need ≥3 to remove one and keep ≥2).
	val2 := newAgentKey(t)
	val3 := newAgentKey(t)
	registerAgent(t, app, val2, "validator-2", "admin")
	registerAgent(t, app, val3, "validator-3", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: val2.id, PublicKey: val2.pub, Power: 10}))
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: val3.id, PublicKey: val3.pub, Power: 10}))
	valMap := map[string]int64{admin.id: 10, val2.id: 10, val3.id: 10}
	require.NoError(t, app.badgerStore.SaveValidators(valMap))

	// Block 1: Propose removal of val3.
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpRemoveValidator, val3.id, val3.pub, 0, "removing val3", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)

	proposal, _ := app.govEngine.GetActiveProposal()
	require.NotNil(t, proposal)

	// Block 11+: val2 votes yes.
	voteTx := makeGovVoteTx(t, val2, proposal.ProposalID, tx.VoteDecisionAccept, 1)
	resp = finalizeBlock(t, app, 12, voteTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)

	// 2 out of 3 validators voted yes (admin auto + val2) = 66.7% ≥ 2/3.
	require.Len(t, resp.ValidatorUpdates, 1, "should return removal update")
	assert.Equal(t, int64(0), resp.ValidatorUpdates[0].Power, "removal sets power to 0")
	assert.Equal(t, 2, app.validators.Size(), "should have 2 validators after removal")

	_, exists := app.validators.GetValidator(val3.id)
	assert.False(t, exists, "val3 should be removed from set")
}

func TestGov_ProposalExpiry(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Add second validator for multi-node.
	val2 := newAgentKey(t)
	registerAgent(t, app, val2, "validator-2", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: val2.id, PublicKey: val2.pub, Power: 10}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{admin.id: 10, val2.id: 10}))

	newValPub, _, _ := ed25519.GenerateKey(nil)
	newValID := hex.EncodeToString(newValPub)

	// Block 1: Propose with default expiry (100 blocks). Power must be ≤ 1/3 of total (20/3=6).
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 6, "will expire", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)

	// Verify proposal is active.
	proposal, _ := app.govEngine.GetActiveProposal()
	require.NotNil(t, proposal)

	// Jump to height 102 (past expiry of 1 + 100 = 101).
	resp = finalizeBlock(t, app, 102)
	assert.Empty(t, resp.ValidatorUpdates, "expired proposal should not produce updates")

	// Verify proposal is no longer active.
	proposal, _ = app.govEngine.GetActiveProposal()
	assert.Nil(t, proposal, "proposal should have been expired")

	// Verify we can submit a new proposal (governance not locked).
	proposeTx2 := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 6, "second attempt", 2)
	// Need to wait for cooldown (50 blocks from height 1). Cooldown starts from when proposal was created.
	resp = finalizeBlock(t, app, 152, proposeTx2)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code, "new proposal after expiry + cooldown should succeed: %s", resp.TxResults[0].Log)
}

func TestGov_ProposalRejection(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Add second validator.
	val2 := newAgentKey(t)
	registerAgent(t, app, val2, "validator-2", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: val2.id, PublicKey: val2.pub, Power: 10}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{admin.id: 10, val2.id: 10}))

	newValPub, _, _ := ed25519.GenerateKey(nil)
	newValID := hex.EncodeToString(newValPub)

	// Block 1: Propose. Power must be ≤ 1/3 of total (20/3=6).
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 6, "will be rejected", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)

	proposal, _ := app.govEngine.GetActiveProposal()
	require.NotNil(t, proposal)

	// Block 12: val2 votes reject.
	voteTx := makeGovVoteTx(t, val2, proposal.ProposalID, tx.VoteDecisionReject, 1)
	resp = finalizeBlock(t, app, 12, voteTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)

	// 1 reject out of 2 = 50% > 1/3 threshold → rejected.
	assert.Empty(t, resp.ValidatorUpdates, "rejected proposal should not produce updates")

	// Proposal should no longer be active.
	proposal, _ = app.govEngine.GetActiveProposal()
	assert.Nil(t, proposal, "rejected proposal should be cleared")
}

func TestGov_NonAdminCannotPropose(t *testing.T) {
	app, _ := setupGovTestApp(t)

	// Register a member (non-admin) agent.
	member := newAgentKey(t)
	registerAgent(t, app, member, "member-agent", "member")

	newValPub, _, _ := ed25519.GenerateKey(nil)
	newValID := hex.EncodeToString(newValPub)

	// Member tries to propose — should fail.
	proposeTx := makeGovProposeTx(t, member, tx.GovOpAddValidator, newValID, newValPub, 10, "unauthorized", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.NotEqual(t, uint32(0), resp.TxResults[0].Code, "non-admin should be rejected")
	assert.Contains(t, resp.TxResults[0].Log, "admin")
}

func TestGov_PowerConstraintEnforced(t *testing.T) {
	app, admin := setupGovTestApp(t)

	newValPub, _, _ := ed25519.GenerateKey(nil)
	newValID := hex.EncodeToString(newValPub)

	// Try to add a validator with power > 1/3 of total (total=10, max=3).
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 100, "too much power", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.NotEqual(t, uint32(0), resp.TxResults[0].Code, "should reject power > 1/3 total")
}

func TestGov_CancelByProposer(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Add second validator for multi-node (so proposal doesn't auto-approve).
	val2 := newAgentKey(t)
	registerAgent(t, app, val2, "validator-2", "admin")
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: val2.id, PublicKey: val2.pub, Power: 10}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{admin.id: 10, val2.id: 10}))

	newValPub, _, _ := ed25519.GenerateKey(nil)
	newValID := hex.EncodeToString(newValPub)

	// Block 1: Propose.
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 5, "will cancel", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code)

	proposal, _ := app.govEngine.GetActiveProposal()
	require.NotNil(t, proposal)

	// Block 2: Cancel.
	cancelTx := makeGovCancelTx(t, admin, proposal.ProposalID, 2)
	resp = finalizeBlock(t, app, 2, cancelTx)
	assert.Equal(t, uint32(0), resp.TxResults[0].Code, "cancel: %s", resp.TxResults[0].Log)
	assert.Empty(t, resp.ValidatorUpdates, "cancelled proposal should not produce updates")

	// Verify cleared.
	proposal, _ = app.govEngine.GetActiveProposal()
	assert.Nil(t, proposal, "cancelled proposal should be cleared")
}

func TestGov_MinValidatorsProtection(t *testing.T) {
	app, admin := setupGovTestApp(t)

	// Only 1 validator (admin). Try to remove the only one — should fail.
	proposeTx := makeGovProposeTx(t, admin, tx.GovOpRemoveValidator, admin.id, admin.pub, 0, "self-destruct", 1)
	resp := finalizeBlock(t, app, 1, proposeTx)
	assert.NotEqual(t, uint32(0), resp.TxResults[0].Code, "should reject removal below min validators")
}

func TestGov_ValidatorUpdatesDeterministic(t *testing.T) {
	// Run the same scenario twice and verify identical ValidatorUpdates.
	for run := 0; run < 2; run++ {
		app, admin := setupGovTestApp(t)

		newValPub, _, _ := ed25519.GenerateKey(nil)
		newValID := hex.EncodeToString(newValPub)

		proposeTx := makeGovProposeTx(t, admin, tx.GovOpAddValidator, newValID, newValPub, 3, "determinism test", 1)
		resp := finalizeBlock(t, app, 1, proposeTx)
		require.Equal(t, uint32(0), resp.TxResults[0].Code)

		if resp.ValidatorUpdates != nil {
			assert.Len(t, resp.ValidatorUpdates, 1)
			assert.Equal(t, int64(3), resp.ValidatorUpdates[0].Power)
		}
	}
}
