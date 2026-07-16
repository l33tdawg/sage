package abci

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

const governanceReplayTestDomain = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a"

type governanceReplayHarness struct {
	badgerPath string
	sqlitePath string
	app        *SageApp
	projection *store.SQLiteStore
	admin      agentKey
	proposer   agentKey
	voter      agentKey
	candidate  agentKey
}

func newGovernanceReplayHarness(t *testing.T) *governanceReplayHarness {
	t.Helper()
	root := t.TempDir()
	h := &governanceReplayHarness{
		badgerPath: filepath.Join(root, "badger"),
		sqlitePath: filepath.Join(root, "sage.db"),
		admin:      newAgentKey(t),
		proposer:   newAgentKey(t),
		voter:      newAgentKey(t),
		candidate:  newAgentKey(t),
	}
	require.NoError(t, os.MkdirAll(h.badgerPath, 0o700))
	badgerStore, err := store.NewBadgerStore(h.badgerPath)
	require.NoError(t, err)
	require.NoError(t, badgerStore.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	seedTestGovernanceDelegationDomain(t, badgerStore)
	h.projection, err = store.NewSQLiteStore(context.Background(), h.sqlitePath)
	require.NoError(t, err)
	h.app, err = NewSageAppWithStores(badgerStore, h.projection, zerolog.Nop())
	require.NoError(t, err)

	registerAgent(t, h.app, h.admin, "operator-admin", "admin")
	registerAgent(t, h.app, h.proposer, "validator-proposer", "member")
	registerAgent(t, h.app, h.voter, "validator-voter", "member")
	for _, key := range []agentKey{h.proposer, h.voter} {
		require.NoError(t, h.app.validators.AddValidator(&validator.ValidatorInfo{
			ID: key.id, PublicKey: key.pub, Power: 10,
		}))
	}
	require.NoError(t, h.app.badgerStore.SaveValidators(map[string]int64{
		h.proposer.id: 10,
		h.voter.id:    10,
	}))
	t.Cleanup(func() {
		if h.app != nil {
			_ = h.app.Close()
		}
	})
	return h
}

func (h *governanceReplayHarness) crashAndReopen(t *testing.T) {
	t.Helper()
	require.NoError(t, h.app.badgerStore.DB().Sync())
	require.NoError(t, h.app.Close())
	h.app = nil
	h.projection = nil

	badgerStore, err := store.NewBadgerStore(h.badgerPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), h.sqlitePath)
	require.NoError(t, err)
	app, err := NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	require.NoError(t, err)
	h.app = app
	h.projection = projection
}

func marshalGovernanceReplayBody(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func encodeGovernanceReplayTx(t *testing.T, parsed *tx.ParsedTx) []byte {
	t.Helper()
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

func (h *governanceReplayHarness) delegatedAddProposal(t *testing.T, nonce uint64, blockTime time.Time, proofNonce string) []byte {
	t.Helper()
	body := marshalGovernanceReplayBody(t, map[string]any{
		"validator_id":      h.proposer.id,
		"governance_domain": governanceReplayTestDomain,
		"operation":         "add_validator",
		"target_id":         h.candidate.id,
		"target_pubkey":     h.candidate.id,
		"target_power":      5,
		"reason":            "add crash-safe validator",
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: nonce, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpAddValidator, TargetID: h.candidate.id,
			TargetPubKey: h.candidate.pub, TargetPower: 5,
			Reason: "add crash-safe validator",
		},
	}
	attachGovernanceRequestProof(
		t, parsed, h.admin, h.proposer, "POST", "/v1/governance/propose",
		body, blockTime, []byte(proofNonce),
	)
	return encodeGovernanceReplayTx(t, parsed)
}

func (h *governanceReplayHarness) delegatedVote(t *testing.T, proposalID string, nonce uint64, blockTime time.Time, proofNonce string) []byte {
	t.Helper()
	body := marshalGovernanceReplayBody(t, map[string]any{
		"validator_id":      h.voter.id,
		"governance_domain": governanceReplayTestDomain,
		"proposal_id":       proposalID,
		"decision":          "accept",
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: nonce, Timestamp: blockTime,
		GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept},
	}
	attachGovernanceRequestProof(
		t, parsed, h.admin, h.voter, "POST", "/v1/governance/vote",
		body, blockTime, []byte(proofNonce),
	)
	return encodeGovernanceReplayTx(t, parsed)
}

func (h *governanceReplayHarness) delegatedCancel(t *testing.T, proposalID string, nonce uint64, blockTime time.Time, proofNonce string) []byte {
	t.Helper()
	body := marshalGovernanceReplayBody(t, map[string]any{
		"validator_id":      h.proposer.id,
		"governance_domain": governanceReplayTestDomain,
		"proposal_id":       proposalID,
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovCancel, Nonce: nonce, Timestamp: blockTime,
		GovCancel: &tx.GovCancel{ProposalID: proposalID},
	}
	attachGovernanceRequestProof(
		t, parsed, h.admin, h.proposer, "POST", "/v1/governance/cancel",
		body, blockTime, []byte(proofNonce),
	)
	return encodeGovernanceReplayTx(t, parsed)
}

func (h *governanceReplayHarness) directVote(t *testing.T, proposalID string, nonce uint64, blockTime time.Time) []byte {
	t.Helper()
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: nonce, Timestamp: blockTime,
		GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept},
	}
	require.NoError(t, tx.SignTx(parsed, h.voter.priv))
	return encodeGovernanceReplayTx(t, parsed)
}

func (h *governanceReplayHarness) directUpgradePropose(t *testing.T, nonce uint64, blockTime time.Time) []byte {
	t.Helper()
	parsed := makeUpgradeProposeTx(t, h.admin, tx.CanonicalUpgradeName(21), 21, "", defaultUpgradeDelayBlocks)
	parsed.Nonce = nonce
	parsed.Timestamp = blockTime
	require.NoError(t, tx.SignTx(parsed, h.admin.priv))
	return encodeGovernanceReplayTx(t, parsed)
}

func (h *governanceReplayHarness) directUpgradeCancel(t *testing.T, nonce uint64, blockTime time.Time) []byte {
	t.Helper()
	parsed := makeUpgradeCancelTx(t, h.admin, tx.CanonicalUpgradeName(21), "cancel before activation")
	parsed.Nonce = nonce
	parsed.Timestamp = blockTime
	require.NoError(t, tx.SignTx(parsed, h.admin.priv))
	return encodeGovernanceReplayTx(t, parsed)
}

func finalizeGovernanceReplayBlock(t *testing.T, app *SageApp, req *abcitypes.RequestFinalizeBlock) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	resp, err := app.FinalizeBlock(context.Background(), req)
	require.NoError(t, err)
	return resp
}

func commitGovernanceReplayBlock(t *testing.T, app *SageApp) {
	t.Helper()
	_, err := app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
}

func requireGovernanceReplayResponseEqual(t *testing.T, first, replayed *abcitypes.ResponseFinalizeBlock) {
	t.Helper()
	assert.Equal(t, first.TxResults, replayed.TxResults)
	assert.Equal(t, first.ValidatorUpdates, replayed.ValidatorUpdates)
	assert.Equal(t, first.ConsensusParamUpdates, replayed.ConsensusParamUpdates)
	assert.Equal(t, first.AppHash, replayed.AppHash)
}

func (h *governanceReplayHarness) createCommittedProposal(t *testing.T) string {
	t.Helper()
	blockTime := time.Unix(10_002, 0).UTC()
	raw := h.delegatedAddProposal(t, 1, blockTime, "prop0001")
	resp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: [][]byte{raw},
	})
	require.Len(t, resp.TxResults, 1)
	require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	commitGovernanceReplayBlock(t, h.app)
	return governance.ComputeProposalID(h.proposer.id, 2, governance.OpAddValidator, h.candidate.id)
}

func TestAppV20DelegatedGovernanceProposalCrashReplayRestoresProjection(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	blockTime := time.Unix(11_002, 0).UTC()
	raw := h.delegatedAddProposal(t, 1, blockTime, "prop0002")
	req := &abcitypes.RequestFinalizeBlock{Height: 2, Time: blockTime, Txs: [][]byte{raw}}

	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	h.crashAndReopen(t)
	assert.Equal(t, int64(0), h.app.state.Height)

	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)

	proposalID := governance.ComputeProposalID(h.proposer.id, 2, governance.OpAddValidator, h.candidate.id)
	projected, err := h.projection.GetGovProposal(context.Background(), proposalID)
	require.NoError(t, err)
	assert.Equal(t, string(governance.StatusVoting), projected.Status)
	votes, err := h.projection.GetGovVotes(context.Background(), proposalID)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	assert.Equal(t, h.proposer.id, votes[0].ValidatorID)
}

func TestAppV20DelegatedGovernanceVoteCrashReplayRestoresProjection(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	proposalID := h.createCommittedProposal(t)
	blockTime := time.Unix(12_003, 0).UTC()
	raw := h.delegatedVote(t, proposalID, 1, blockTime, "vote0001")
	req := &abcitypes.RequestFinalizeBlock{Height: 3, Time: blockTime, Txs: [][]byte{raw}}

	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	h.crashAndReopen(t)
	assert.Equal(t, int64(2), h.app.state.Height)

	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)
	votes, err := h.projection.GetGovVotes(context.Background(), proposalID)
	require.NoError(t, err)
	require.Len(t, votes, 2)
}

func TestAppV20DelegatedGovernanceCancelCrashReplayRestoresProjection(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	proposalID := h.createCommittedProposal(t)
	blockTime := time.Unix(13_003, 0).UTC()
	raw := h.delegatedCancel(t, proposalID, 2, blockTime, "cncl0001")
	req := &abcitypes.RequestFinalizeBlock{Height: 3, Time: blockTime, Txs: [][]byte{raw}}

	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	h.crashAndReopen(t)
	assert.Equal(t, int64(2), h.app.state.Height)

	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)
	projected, err := h.projection.GetGovProposal(context.Background(), proposalID)
	require.NoError(t, err)
	assert.Equal(t, string(governance.StatusCancelled), projected.Status)
}

func TestAppV20DelegatedGovernanceReconfigurationCrashReplayReemitsValidatorUpdate(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	proposalID := h.createCommittedProposal(t)
	for height := int64(3); height < 12; height++ {
		resp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
			Height: height, Time: time.Unix(14_000+height, 0).UTC(),
		})
		require.Empty(t, resp.ValidatorUpdates)
		commitGovernanceReplayBlock(t, h.app)
	}

	blockTime := time.Unix(14_012, 0).UTC()
	raw := h.delegatedVote(t, proposalID, 1, blockTime, "vote0002")
	req := &abcitypes.RequestFinalizeBlock{Height: 12, Time: blockTime, Txs: [][]byte{raw}}
	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	require.Len(t, first.ValidatorUpdates, 1)
	assert.Equal(t, []byte(h.candidate.pub), first.ValidatorUpdates[0].PubKey.GetEd25519())
	assert.Equal(t, int64(5), first.ValidatorUpdates[0].Power)

	h.crashAndReopen(t)
	assert.Equal(t, int64(11), h.app.state.Height)
	_, candidateAlreadyDurable := h.app.validators.GetValidator(h.candidate.id)
	assert.False(t, candidateAlreadyDurable, "FinalizeBlock mutations stay speculative until Commit")

	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)
	projected, err := h.projection.GetGovProposal(context.Background(), proposalID)
	require.NoError(t, err)
	assert.Equal(t, string(governance.StatusExecuted), projected.Status)
	assert.NotNil(t, projected.ExecutedHeight)
	assert.Equal(t, int64(12), *projected.ExecutedHeight)

	late := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 13, Time: time.Unix(14_013, 0).UTC(), Txs: [][]byte{raw},
	})
	require.Len(t, late.TxResults, 1)
	assert.Equal(t, uint32(4), late.TxResults[0].Code, "crash recovery must not weaken ordinary later-height nonce replay rejection")
}

func TestAppV20DirectGovernanceVoteCrashReplayReemitsValidatorUpdate(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	proposalID := h.createCommittedProposal(t)
	for height := int64(3); height < 12; height++ {
		finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
			Height: height, Time: time.Unix(14_100+height, 0).UTC(),
		})
		commitGovernanceReplayBlock(t, h.app)
	}
	blockTime := time.Unix(14_112, 0).UTC()
	raw := h.directVote(t, proposalID, 1, blockTime)
	req := &abcitypes.RequestFinalizeBlock{Height: 12, Time: blockTime, Txs: [][]byte{raw}}

	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	require.Len(t, first.ValidatorUpdates, 1)
	h.crashAndReopen(t)
	assert.Equal(t, int64(11), h.app.state.Height)

	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)
	projected, err := h.projection.GetGovProposal(context.Background(), proposalID)
	require.NoError(t, err)
	assert.Equal(t, string(governance.StatusExecuted), projected.Status)
}

func TestAppV20GovernanceOutcomeOnlyCrashReplayReemitsValidatorUpdate(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	proposalID := h.createCommittedProposal(t)
	voteTime := time.Unix(14_203, 0).UTC()
	vote := h.directVote(t, proposalID, 1, voteTime)
	voteResp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 3, Time: voteTime, Txs: [][]byte{vote},
	})
	require.Zero(t, voteResp.TxResults[0].Code, voteResp.TxResults[0].Log)
	require.Empty(t, voteResp.ValidatorUpdates, "minimum voting period has not elapsed")
	commitGovernanceReplayBlock(t, h.app)
	for height := int64(4); height < 12; height++ {
		finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
			Height: height, Time: time.Unix(14_200+height, 0).UTC(),
		})
		commitGovernanceReplayBlock(t, h.app)
	}

	req := &abcitypes.RequestFinalizeBlock{Height: 12, Time: time.Unix(14_212, 0).UTC()}
	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Empty(t, first.TxResults)
	require.Len(t, first.ValidatorUpdates, 1)
	h.crashAndReopen(t)
	assert.Equal(t, int64(11), h.app.state.Height)

	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)
	projected, err := h.projection.GetGovProposal(context.Background(), proposalID)
	require.NoError(t, err)
	assert.Equal(t, string(governance.StatusExecuted), projected.Status)
}

func TestAppV20UpgradeGovernanceCrashReplayCoversProposeAndCancel(t *testing.T) {
	t.Run("propose", func(t *testing.T) {
		h := newGovernanceReplayHarness(t)
		blockTime := time.Unix(14_302, 0).UTC()
		raw := h.directUpgradePropose(t, 1, blockTime)
		req := &abcitypes.RequestFinalizeBlock{Height: 2, Time: blockTime, Txs: [][]byte{raw}}

		first := finalizeGovernanceReplayBlock(t, h.app, req)
		require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
		h.crashAndReopen(t)
		replayed := finalizeGovernanceReplayBlock(t, h.app, req)
		requireGovernanceReplayResponseEqual(t, first, replayed)
		commitGovernanceReplayBlock(t, h.app)

		proposalID := governance.ComputeProposalID(
			h.admin.id, 2, governance.OpUpgrade, tx.CanonicalUpgradeName(21),
		)
		projected, err := h.projection.GetGovProposal(context.Background(), proposalID)
		require.NoError(t, err)
		assert.Equal(t, "upgrade", projected.Operation)
		votes, err := h.projection.GetGovVotes(context.Background(), proposalID)
		require.NoError(t, err)
		require.Len(t, votes, 1)
		assert.Equal(t, h.admin.id, votes[0].ValidatorID)
	})

	t.Run("cancel", func(t *testing.T) {
		h := newGovernanceReplayHarness(t)
		require.NoError(t, h.app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
			Name: tx.CanonicalUpgradeName(21), TargetAppVersion: 21,
			ActivationHeight: 100, ProposedAt: 1, ProposerID: h.admin.id,
		}))
		blockTime := time.Unix(14_402, 0).UTC()
		raw := h.directUpgradeCancel(t, 1, blockTime)
		req := &abcitypes.RequestFinalizeBlock{Height: 2, Time: blockTime, Txs: [][]byte{raw}}

		first := finalizeGovernanceReplayBlock(t, h.app, req)
		require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
		h.crashAndReopen(t)
		plan, err := h.app.badgerStore.GetUpgradePlan()
		require.NoError(t, err)
		assert.Equal(t, tx.CanonicalUpgradeName(21), plan.Name, "uncommitted cancel must be discarded")

		replayed := finalizeGovernanceReplayBlock(t, h.app, req)
		requireGovernanceReplayResponseEqual(t, first, replayed)
		commitGovernanceReplayBlock(t, h.app)
		_, err = h.app.badgerStore.GetUpgradePlan()
		require.ErrorIs(t, err, store.ErrNoUpgradePlan)
	})
}

func TestAppV20AtomicFinalizeDoesNotAuthorizeDuplicateInOriginalBlock(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	blockTime := time.Unix(15_002, 0).UTC()
	raw := h.delegatedAddProposal(t, 1, blockTime, "prop0003")
	resp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: [][]byte{raw, raw},
	})
	require.Len(t, resp.TxResults, 2)
	assert.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	assert.Equal(t, uint32(4), resp.TxResults[1].Code, "an in-block duplicate must still fail nonce replay")
}

func TestAppV20MixedOrdinaryAndGovernanceBlockCrashReplayIsAtomic(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	blockTime := time.Unix(15_102, 0).UTC()
	ordinaryAgent := newAgentKey(t)
	ordinary := makeAgentRegisterTx(t, ordinaryAgent, "mixed-block-agent", "member", "", "test", "")
	ordinary.Nonce = 1
	require.NoError(t, tx.SignTx(ordinary, ordinaryAgent.priv))
	rawOrdinary := encodeGovernanceReplayTx(t, ordinary)
	rawGovernance := h.delegatedAddProposal(t, 1, blockTime, "prop0004")
	req := &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: [][]byte{rawOrdinary, rawGovernance},
	}

	first := finalizeGovernanceReplayBlock(t, h.app, req)
	require.Len(t, first.TxResults, 2)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	require.Zero(t, first.TxResults[1].Code, first.TxResults[1].Log)
	assert.False(t, h.app.badgerStore.IsAgentRegistered(ordinaryAgent.id), "FinalizeBlock remains speculative")

	h.crashAndReopen(t)
	assert.False(t, h.app.badgerStore.IsAgentRegistered(ordinaryAgent.id), "crash discards the complete mixed block")
	replayed := finalizeGovernanceReplayBlock(t, h.app, req)
	requireGovernanceReplayResponseEqual(t, first, replayed)
	commitGovernanceReplayBlock(t, h.app)
	assert.True(t, h.app.badgerStore.IsAgentRegistered(ordinaryAgent.id))
	ordinaryNonce, err := h.app.badgerStore.GetNonce(ordinaryAgent.id)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), ordinaryNonce)

	proposalID := governance.ComputeProposalID(h.proposer.id, 2, governance.OpAddValidator, h.candidate.id)
	_, err = h.projection.GetGovProposal(context.Background(), proposalID)
	require.NoError(t, err)
}

func TestAppV20AtomicFinalizeBudgetRejectsEntryCountBeforeMutation(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	blockTime := time.Unix(15_202, 0).UTC()
	raw := h.delegatedAddProposal(t, 1, blockTime, "prop0005")
	txs := make([][]byte, maxAppV20AtomicFinalizeEntries+1)
	for i := range txs {
		txs[i] = raw
	}
	resp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: txs,
	})
	require.Len(t, resp.TxResults, len(txs))
	for i, result := range resp.TxResults {
		assert.Equal(t, appV20AtomicFinalizeBudgetCode, result.Code, "tx %d", i)
		assert.Equal(t, appV20AtomicFinalizeBudgetLog, result.Log, "tx %d", i)
	}
	active, err := h.app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
	nonce, err := h.app.badgerStore.GetNonce(h.proposer.id)
	require.NoError(t, err)
	assert.Zero(t, nonce)
}

func TestAppV20PrepareAndProcessProposalEnforceGlobalAtomicBudget(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	blockTime := time.Unix(15_402, 0).UTC()
	governanceTx := h.delegatedAddProposal(t, 1, blockTime, "prop0006")
	ordinaryOne := encodeSelfSignedRegister(t, newAgentKey(t), "ordinary-one")
	ordinaryTwo := encodeSelfSignedRegister(t, newAgentKey(t), "ordinary-two")
	maxBytes := int64(1 << 20)

	for _, txs := range [][][]byte{
		{ordinaryOne, governanceTx, ordinaryTwo},
		{governanceTx, ordinaryOne},
		{ordinaryOne, governanceTx},
	} {
		prepared, err := h.app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
			Height: 2, Time: blockTime, MaxTxBytes: maxBytes, Txs: txs,
		})
		require.NoError(t, err)
		assert.Equal(t, txs, prepared.Txs, "mixed transaction families preserve mempool order")

		processed, err := h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
			Height: 2, Time: blockTime, Txs: txs,
		})
		require.NoError(t, err)
		assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)
	}

	governanceSize := cmttypes.ComputeProtoSizeForTxs([]cmttypes.Tx{cmttypes.Tx(governanceTx)})
	prepared, err := h.app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: governanceSize,
		Txs: [][]byte{governanceTx, governanceTx},
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{governanceTx}, prepared.Txs, "prepared proposal must not exceed protobuf MaxTxBytes")

	processed, err := h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{governanceTx},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)
	processed, err = h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{ordinaryOne, ordinaryTwo},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

	upgradePropose := h.directUpgradePropose(t, 1, blockTime)
	upgradeCancel := h.directUpgradeCancel(t, 1, blockTime)
	prepared, err = h.app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: maxBytes,
		Txs: [][]byte{ordinaryOne, upgradePropose, upgradeCancel},
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{ordinaryOne, upgradePropose, upgradeCancel}, prepared.Txs)
	processed, err = h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{ordinaryOne, upgradePropose},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)
	processed, err = h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{upgradePropose, upgradeCancel},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

	overBudget := make([][]byte, maxAppV20AtomicFinalizeEntries+1)
	for i := range overBudget {
		overBudget[i] = governanceTx
	}
	processed, err = h.app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: overBudget,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_REJECT, processed.Status)

	preV20 := setupTestApp(t)
	prePrepared, err := preV20.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: 1,
		Txs: [][]byte{ordinaryOne, governanceTx, ordinaryTwo},
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{ordinaryOne, governanceTx, ordinaryTwo}, prePrepared.Txs, "pre-app-v20 remains pass-through")
	preProcessed, err := preV20.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{ordinaryOne, governanceTx},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, preProcessed.Status)
}

func TestAppV20CeremonyBudgetStartsAfterAuthenticatedBootstrap(t *testing.T) {
	blockTime := time.Unix(15_502, 0).UTC()
	ordinary := encodeSelfSignedRegister(t, newAgentKey(t), "ceremony-ordinary")

	t.Run("target-20 proposal", func(t *testing.T) {
		app, admin := setupGovTestApp(t)
		proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
		proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
		proposal.Nonce = 1
		proposal.Timestamp = blockTime
		require.NoError(t, tx.SignTx(proposal, admin.priv))
		rawProposal := encodeGovernanceReplayTx(t, proposal)
		txs := make([][]byte, maxAppV20AtomicFinalizeEntries+1)
		txs[0] = rawProposal
		for i := 1; i < len(txs); i++ {
			txs[i] = ordinary
		}

		prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
			Height: 2, Time: blockTime, MaxTxBytes: 2 << 20, Txs: txs,
		})
		require.NoError(t, err)
		assert.Len(t, prepared.Txs, maxAppV20AtomicFinalizeEntries+1, "raw bootstrap alone must not enable strict proposal filtering")
		processed, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
			Height: 2, Time: blockTime, Txs: txs,
		})
		require.NoError(t, err)
		assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)
		finalized := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{Height: 2, Time: blockTime, Txs: txs})
		assert.NotEqual(t, uint32(0), finalized.TxResults[0].Code)
		for _, result := range finalized.TxResults[1:] {
			assert.NotEqual(t, appV20AtomicFinalizeBudgetCode, result.Code, "legacy transactions retain pre-v20 semantics before authenticated ceremony state commits")
		}
	})

	t.Run("pending target-20 plan", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
			Name: appV20UpgradeName, TargetAppVersion: 20, ActivationHeight: 100,
			GovernanceDomain: governanceReplayTestDomain, ProposedAt: 1, ProposerID: "operator",
		}))
		txs := make([][]byte, maxAppV20AtomicFinalizeEntries+1)
		for i := range txs {
			txs[i] = ordinary
		}
		prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
			Height: 2, Time: blockTime, MaxTxBytes: 2 << 20, Txs: txs,
		})
		require.NoError(t, err)
		assert.Len(t, prepared.Txs, maxAppV20AtomicFinalizeEntries)
		processed, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
			Height: 2, Time: blockTime, Txs: txs,
		})
		require.NoError(t, err)
		assert.Equal(t, abcitypes.ResponseProcessProposal_REJECT, processed.Status)
	})
}

func TestAppV20RawBootstrapDoesNotChangeLegacyMixedBlockSemantics(t *testing.T) {
	baseline := setupTestApp(t)
	withForgedBootstrap := setupTestApp(t)
	blockTime := time.Unix(15_550, 0).UTC()

	legacyTxs := make([][]byte, maxAppV20AtomicFinalizeEntries+1)
	legacyKeys := make([]agentKey, len(legacyTxs))
	for i := range legacyTxs {
		legacyKeys[i] = newAgentKey(t)
		legacyTxs[i] = encodeSelfSignedRegister(t, legacyKeys[i], fmt.Sprintf("legacy-%03d", i))
	}
	forged := newAgentKey(t)
	proposal := makeUpgradeProposeTx(t, forged, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	proposal.Nonce = 1
	proposal.Timestamp = blockTime
	require.NoError(t, tx.SignTx(proposal, forged.priv))
	rawProposal := encodeGovernanceReplayTx(t, proposal)
	mixed := append([][]byte{rawProposal}, legacyTxs...)

	baselineResp := finalizeGovernanceReplayBlock(t, baseline, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: legacyTxs,
	})
	mixedResp := finalizeGovernanceReplayBlock(t, withForgedBootstrap, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: mixed,
	})
	require.NotZero(t, mixedResp.TxResults[0].Code)
	assert.Equal(t, baselineResp.TxResults, mixedResp.TxResults[1:])
	assert.Equal(t, baselineResp.AppHash, mixedResp.AppHash, "raw target-20 must change durability only, not legacy state semantics")
	commitGovernanceReplayBlock(t, baseline)
	commitGovernanceReplayBlock(t, withForgedBootstrap)
	for _, key := range legacyKeys {
		assert.True(t, withForgedBootstrap.badgerStore.IsAgentRegistered(key.id))
	}
	marker, err := withForgedBootstrap.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker)
}

func TestAppV20AuthenticatedBootstrapRequiresDedicatedBlockThenEnablesStrictRules(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV19AppliedHeight = 1
	blockTime := time.Unix(15_560, 0).UTC()
	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	proposal.Nonce = 1
	proposal.Timestamp = blockTime
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	rawProposal := encodeGovernanceReplayTx(t, proposal)
	legacyKey := newAgentKey(t)
	legacyParsed := makeAgentRegisterTx(t, legacyKey, "mixed-legacy", "member", "", "test", "")
	legacyParsed.Nonce = 1
	require.NoError(t, tx.SignTx(legacyParsed, legacyKey.priv))
	legacyRaw := encodeGovernanceReplayTx(t, legacyParsed)
	maxBytes := int64(2 << 20)

	forged := newAgentKey(t)
	forgedProposal := makeUpgradeProposeTx(t, forged, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	forgedProposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	forgedProposal.Nonce = 1
	forgedProposal.Timestamp = blockTime
	require.NoError(t, tx.SignTx(forgedProposal, forged.priv))
	forgedRaw := encodeGovernanceReplayTx(t, forgedProposal)
	prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: maxBytes, Txs: [][]byte{legacyRaw, forgedRaw},
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{legacyRaw, forgedRaw}, prepared.Txs, "unregistered target-20 sender must not isolate or censor legacy traffic")
	processed, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{legacyRaw, forgedRaw},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

	prepared, err = app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: maxBytes, Txs: [][]byte{legacyRaw, rawProposal},
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{rawProposal}, prepared.Txs, "authenticated app-v20 bootstrap must receive a dedicated block")
	processed, err = app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{legacyRaw, rawProposal},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_REJECT, processed.Status, "peers must reject a proposer that mixes the authenticated bootstrap")
	processed, err = app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: [][]byte{rawProposal},
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

	mixed := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: [][]byte{rawProposal, legacyRaw},
	})
	require.Len(t, mixed.TxResults, 2)
	assert.Equal(t, uint32(47), mixed.TxResults[0].Code)
	assert.Contains(t, mixed.TxResults[0].Log, "only transaction")
	assert.Zero(t, mixed.TxResults[1].Code, mixed.TxResults[1].Log)
	commitGovernanceReplayBlock(t, app)
	assert.True(t, app.badgerStore.IsAgentRegistered(legacyKey.id))
	adminNonce, err := app.badgerStore.GetNonce(admin.id)
	require.NoError(t, err)
	assert.Zero(t, adminNonce, "rejected bootstrap must not burn its nonce")
	marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker)

	dedicated := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 3, Time: blockTime.Add(time.Second), Txs: [][]byte{rawProposal},
	})
	require.Len(t, dedicated.TxResults, 1)
	assert.Zero(t, dedicated.TxResults[0].Code, dedicated.TxResults[0].Log)
	assert.Nil(t, dedicated.ConsensusParamUpdates, "bootstrap cannot auto-activate in its own block")
	marker, err = app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker, "audit marker remains speculative until Commit")
	commitGovernanceReplayBlock(t, app)
	marker, err = app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Equal(t, appV20LegacyResourceAuditValue, marker)
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, appV20UpgradeName, active.TargetID)

	overBudget := make([][]byte, maxAppV20AtomicFinalizeEntries+1)
	for i := range overBudget {
		overBudget[i] = legacyRaw
	}
	processed, err = app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 4, Time: blockTime.Add(2 * time.Second), Txs: overBudget,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_REJECT, processed.Status)
	strict := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 4, Time: blockTime.Add(2 * time.Second), Txs: overBudget,
	})
	for _, result := range strict.TxResults {
		assert.Equal(t, appV20AtomicFinalizeBudgetCode, result.Code)
	}
}

func TestAppV20CooldownBootstrapDoesNotCensorLegacyTraffic(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV19AppliedHeight = 1
	blockTime := time.Unix(15_570, 0).UTC()
	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	proposal.Nonce = 1
	proposal.Timestamp = blockTime
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	rawProposal := encodeGovernanceReplayTx(t, proposal)

	ordinaryKey := newAgentKey(t)
	ordinary := makeAgentRegisterTx(t, ordinaryKey, "cooldown-legacy", "member", "", "test", "")
	ordinary.Nonce = 1
	require.NoError(t, tx.SignTx(ordinary, ordinaryKey.priv))
	rawOrdinary := encodeGovernanceReplayTx(t, ordinary)

	cooldown := make([]byte, 8)
	binary.BigEndian.PutUint64(cooldown, 2)
	require.NoError(t, app.badgerStore.SetState("gov:cooldown:"+admin.id, cooldown))
	require.ErrorContains(t, app.govEngine.CheckProposerEligibility(admin.id, 2), "cooldown")

	prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 2, Time: blockTime, MaxTxBytes: 2 << 20, Txs: [][]byte{rawProposal, rawOrdinary},
	})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{rawProposal, rawOrdinary}, prepared.Txs, "cooling-down proposer must not isolate or censor legacy traffic")
	processed, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 2, Time: blockTime, Txs: prepared.Txs,
	})
	require.NoError(t, err)
	assert.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, processed.Status)

	finalized := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: blockTime, Txs: prepared.Txs,
	})
	require.Len(t, finalized.TxResults, 2)
	assert.Equal(t, uint32(47), finalized.TxResults[0].Code)
	assert.Contains(t, finalized.TxResults[0].Log, "only transaction")
	assert.Zero(t, finalized.TxResults[1].Code, finalized.TxResults[1].Log)
	commitGovernanceReplayBlock(t, app)
	assert.True(t, app.badgerStore.IsAgentRegistered(ordinaryKey.id))
	adminNonce, err := app.badgerStore.GetNonce(admin.id)
	require.NoError(t, err)
	assert.Zero(t, adminNonce, "rejected cooldown bootstrap must not burn its nonce")
	marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker)
}

func TestAppV20AuditMarkerNotPersistedWhenGovernanceProposalCreationFails(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV19AppliedHeight = 1
	_, err := app.govEngine.Propose(
		admin.id, governance.OpUpdatePower, admin.id, nil, 10, 0,
		"occupy governance singleton", 1, nil,
	)
	require.NoError(t, err)
	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	proposal.Nonce = 1
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	raw := encodeGovernanceReplayTx(t, proposal)

	resp := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(15_570, 0).UTC(), Txs: [][]byte{raw},
	})
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, uint32(47), resp.TxResults[0].Code)
	assert.Contains(t, resp.TxResults[0].Log, "governance propose failed")
	commitGovernanceReplayBlock(t, app)
	marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker, "an authorized but rejected proposal must not certify the audit")
}

func TestAppV20ProposalCancelAndExpiryClearAuditMarker(t *testing.T) {
	t.Run("cancel then retry re-audits", func(t *testing.T) {
		app, admin := setupGovTestApp(t)
		app.appV19AppliedHeight = 1
		proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
		proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
		proposal.Nonce = 1
		require.NoError(t, tx.SignTx(proposal, admin.priv))
		rawProposal := encodeGovernanceReplayTx(t, proposal)
		created := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
			Height: 2, Time: time.Unix(15_580, 0).UTC(), Txs: [][]byte{rawProposal},
		})
		require.Zero(t, created.TxResults[0].Code, created.TxResults[0].Log)
		commitGovernanceReplayBlock(t, app)
		active, err := app.govEngine.GetActiveProposal()
		require.NoError(t, err)
		require.NotNil(t, active)

		cancel := &tx.ParsedTx{
			Type: tx.TxTypeGovCancel, Nonce: 2, Timestamp: time.Unix(15_581, 0).UTC(),
			GovCancel: &tx.GovCancel{ProposalID: active.ProposalID},
		}
		require.NoError(t, tx.SignTx(cancel, admin.priv))
		rawCancel := encodeGovernanceReplayTx(t, cancel)
		cancelled := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
			Height: 3, Time: cancel.Timestamp, Txs: [][]byte{rawCancel},
		})
		require.Zero(t, cancelled.TxResults[0].Code, cancelled.TxResults[0].Log)
		commitGovernanceReplayBlock(t, app)
		marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
		require.NoError(t, err)
		assert.Nil(t, marker)

		require.NoError(t, app.badgerStore.SetRawForTest(
			[]byte("federation:repair-required"),
			make([]byte, maxAppV20EncodedRecordBytes+1),
		))
		proposal.Nonce = 3
		require.NoError(t, tx.SignTx(proposal, admin.priv))
		retryRaw := encodeGovernanceReplayTx(t, proposal)
		retried := finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
			Height: 4, Time: time.Unix(15_582, 0).UTC(), Txs: [][]byte{retryRaw},
		})
		assert.Equal(t, uint32(47), retried.TxResults[0].Code)
		assert.Contains(t, retried.TxResults[0].Log, "readiness failed")
	})

	t.Run("expired proposal", func(t *testing.T) {
		app, admin, _, _ := setupAppV8Chain(t, 5)
		app.appV19AppliedHeight = 6
		proposalID := proposeActiveAppV20Upgrade(t, app, admin, governanceReplayTestDomain)
		active, err := app.govEngine.GetActiveProposal()
		require.NoError(t, err)
		require.NotNil(t, active)
		require.Equal(t, proposalID, active.ProposalID)
		height := active.ExpiryHeight + 1
		finalizeGovernanceReplayBlock(t, app, &abcitypes.RequestFinalizeBlock{
			Height: height, Time: time.Unix(16_000+height, 0).UTC(),
		})
		commitGovernanceReplayBlock(t, app)
		marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
		require.NoError(t, err)
		assert.Nil(t, marker)
		active, err = app.govEngine.GetActiveProposal()
		require.NoError(t, err)
		assert.Nil(t, active)
	})
}

func TestAppV20PendingPlanFreezesLegacyValidatorReconfiguration(t *testing.T) {
	app, admin := setupGovTestApp(t)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV20UpgradeName, TargetAppVersion: 20, ActivationHeight: 500,
		GovernanceDomain: governanceReplayTestDomain, ProposedAt: 1, ProposerID: admin.id,
	}))
	candidate := newAgentKey(t)
	raw := makeGovProposeTx(t, admin, tx.GovOpAddValidator, candidate.id, candidate.pub, 5, "must wait for app-v20 activation", 1)
	parsed, err := tx.DecodeTx(raw)
	require.NoError(t, err)
	result := app.processGovPropose(parsed, 2, time.Unix(15_602, 0).UTC())
	assert.Equal(t, uint32(72), result.Code)
	assert.Contains(t, result.Log, "reconfiguration is frozen")
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestAppV20AtomicPredicateFailsClosedOnUnreadableCeremonyState(t *testing.T) {
	t.Run("active proposal read error", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetState("gov:active", []byte("missing-proposal")))
		assert.True(t, app.requiresAppV20AtomicFinalizeAt(2, nil))
	})

	t.Run("upgrade plan decode error", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetRawForTest([]byte("upgrade:plan"), []byte("not-json")))
		assert.True(t, app.requiresAppV20AtomicFinalizeAt(2, nil))
	})
}

func TestAppV20AtomicFinalizeDiscardsOnGovernanceReadFailure(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	require.NoError(t, h.app.badgerStore.SetState("gov:active", []byte("missing-proposal")))

	resp, err := h.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2,
		Time:   time.Unix(15_702, 0).UTC(),
	})
	require.ErrorContains(t, err, "atomic active-proposal read failed")
	assert.Nil(t, resp)
	assert.Nil(t, h.app.pendingAppV20Finalize)
	assert.Equal(t, int64(0), h.app.state.Height, "the live application graph must remain committed-state only")
	active, readErr := h.app.badgerStore.GetState("gov:active")
	require.NoError(t, readErr)
	assert.Equal(t, []byte("missing-proposal"), active, "the discarded transaction must not rewrite corrupt durable evidence")
}

func TestAppV20AtomicFinalizeDiscardsWhenUpgradeProposalCannotApply(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV19AppliedHeight = 1
	proposalID, err := app.govEngine.Propose(
		admin.id,
		governance.OpUpgrade,
		appV20UpgradeName,
		nil,
		0,
		0,
		"malformed app-v20 proposal injected for recovery test",
		1,
		[]byte("not-json"),
	)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetState(
		appV20LegacyResourceAuditStateKey,
		appV20LegacyResourceAuditValue,
	))

	resp, finalizeErr := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2,
		Time:   time.Unix(15_752, 0).UTC(),
	})
	require.ErrorContains(t, finalizeErr, "failed to apply")
	require.ErrorContains(t, finalizeErr, "decode app-v20 upgrade proposal payload")
	assert.Nil(t, resp)
	assert.Nil(t, app.pendingAppV20Finalize)
	assert.Equal(t, int64(0), app.state.Height)

	proposal, loadErr := app.govEngine.LoadProposal(proposalID)
	require.NoError(t, loadErr)
	assert.Equal(t, governance.StatusVoting, proposal.Status,
		"discard must not commit StatusExecuted without the matching upgrade plan")
	active, activeErr := app.govEngine.GetActiveProposal()
	require.NoError(t, activeErr)
	require.NotNil(t, active)
	assert.Equal(t, proposalID, active.ProposalID)
	_, planErr := app.badgerStore.GetUpgradePlan()
	require.ErrorIs(t, planErr, store.ErrNoUpgradePlan)
}

func TestAppV20AtomicFinalizeDiscardsOnUpgradePlanReadFailure(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	require.NoError(t, h.app.badgerStore.SetRawForTest([]byte("upgrade:plan"), []byte("not-json")))

	resp, err := h.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2,
		Time:   time.Unix(15_802, 0).UTC(),
	})
	require.ErrorContains(t, err, "atomic upgrade-plan read failed")
	assert.Nil(t, resp)
	assert.Nil(t, h.app.pendingAppV20Finalize)
	assert.Equal(t, int64(0), h.app.state.Height)
	_, readErr := h.app.badgerStore.GetUpgradePlan()
	require.Error(t, readErr, "the corrupt durable plan remains available for operator repair")
}

func TestAppV20AtomicFinalizeDiscardsOnEpochStatsReadFailure(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	corruptValidator := h.proposer.id
	require.NoError(t, h.app.badgerStore.SetRawForTest(
		[]byte("vstats:"+corruptValidator),
		[]byte("bad"),
	))

	resp, err := h.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 100,
		Time:   time.Unix(15_900, 0).UTC(),
	})
	require.ErrorContains(t, err, "atomic epoch processing failed")
	assert.Nil(t, resp)
	assert.Nil(t, h.app.pendingAppV20Finalize)
	assert.Equal(t, int64(0), h.app.state.Height)
	assert.Zero(t, h.app.state.EpochNum, "the speculative epoch number must not publish")
	_, readErr := h.app.badgerStore.GetValidatorStats(corruptValidator)
	require.Error(t, readErr)
}

func TestAppV20CeremonyEpochIgnoresUnboundedStaleValidatorStats(t *testing.T) {
	app, _ := setupGovTestApp(t)
	app.appV19AppliedHeight = 1
	require.NoError(t, app.badgerStore.SetState(
		appV20LegacyResourceAuditStateKey,
		appV20LegacyResourceAuditValue,
	))
	require.NoError(t, app.badgerStore.SetRawForTest(
		[]byte("vstats:removed-validator"),
		[]byte("bad"),
	))

	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 100,
		Time:   time.Unix(15_950, 0).UTC(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, app.pendingAppV20Finalize)
	commitGovernanceReplayBlock(t, app)
	assert.Equal(t, int64(100), app.state.Height)
	_, staleErr := app.badgerStore.GetValidatorStats("removed-validator")
	require.Error(t, staleErr, "bounded current-roster processing must not rewrite stale corrupt evidence")
}
