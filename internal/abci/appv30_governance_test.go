package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func setupAppV30TaintGovernance(
	t *testing.T,
) (*SageApp, agentKey, tx.ParentRef, string) {
	t.Helper()
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-taint-governance", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	const chainID = "app-v30-taint-governance-chain"
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(chainID))
	activateAppV30ForMemorySubmitTest(t, app, 20)

	contentHash := sha256.Sum256([]byte("governance taint target"))
	target := tx.ParentRef{
		SourceChainID: chainID,
		MemoryID:      "governance-taint-target",
		ContentHash:   contentHash,
	}
	require.NoError(t, app.badgerStore.SetMemoryHash(
		target.MemoryID,
		target.ContentHash[:],
		"committed",
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain(target.MemoryID, "general"))
	require.NoError(t, app.badgerStore.SetMemoryAuthor(target.MemoryID, root.id))
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(
		target.MemoryID,
		rootState.PrincipalID,
	))
	require.NoError(t, app.badgerStore.SetMemoryClassification(
		target.MemoryID,
		uint8(store.ClearanceInternal),
	))
	return app, root, target, chainID
}

func appV30ApplyPayload(target tx.ParentRef) tx.TaintApplyPayload {
	return tx.TaintApplyPayload{
		Version:           tx.TaintApplyPayloadVersion,
		Action:            tx.TaintActionApply,
		TargetChainID:     target.SourceChainID,
		TargetMemoryID:    target.MemoryID,
		TargetContentHash: append([]byte(nil), target.ContentHash[:]...),
		ScopeKind:         tx.TaintScopeDirect,
		ReasonCategory:    1,
		EvidenceDigest:    sha256.New().Sum(nil),
	}
}

func TestAppV30MemoryTaintProposeUsesExplicitVotes(t *testing.T) {
	for _, dashboard := range []bool{false, true} {
		op, err := governanceProposalOpFromRequest("memory_taint", dashboard)
		require.NoError(t, err)
		require.Equal(t, tx.GovOpMemoryTaint, op)
	}
	app, root, target, _ := setupAppV30TaintGovernance(t)
	apply := appV30ApplyPayload(target)
	payload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)

	body := []byte("taint proposal")
	pubKey, sig, bodyHash, timestamp := signAgentProof(t, root, body)
	parsed := &tx.ParsedTx{
		Type:  tx.TxTypeGovPropose,
		Nonce: 1,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpMemoryTaint,
			TargetID:  decisionID,
			Payload:   payload,
			Reason:    "taint proposal",
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: timestamp,
	}
	require.NoError(t, tx.SignTx(parsed, root.priv))

	result := app.processGovPropose(parsed, 20, appV23BlockTime())
	require.Equal(t, uint32(0), result.Code, result.Log)
	proposal, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, proposal)
	require.Equal(t, governance.OpMemoryTaint, proposal.Operation)
	require.Equal(t, decisionID, proposal.TargetID)
	votes, err := app.govEngine.GetProposalVotes(proposal.ProposalID)
	require.NoError(t, err)
	require.Empty(t, votes, "op-12 proposals require explicit validator votes")
}

func TestAppV30MemoryTaintApplyAndClearExecuteAtProposalBoundary(t *testing.T) {
	app, _, target, _ := setupAppV30TaintGovernance(t)
	apply := appV30ApplyPayload(target)
	applyPayload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(applyPayload)
	require.NoError(t, err)

	applyProposal := &governance.ProposalState{
		Operation: governance.OpMemoryTaint,
		TargetID:  decisionID,
		Payload:   applyPayload,
	}
	require.NoError(t, app.applyAppV30TaintProposal(applyProposal, 20))
	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusComplete, header.Status)
	decisions, err := app.badgerStore.GetEffectiveTaintDecisions(target.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)

	decisionBytes, err := hex.DecodeString(decisionID)
	require.NoError(t, err)
	clearEvidence := sha256.Sum256([]byte("clear evidence"))
	clear := tx.TaintClearPayload{
		Version:        tx.TaintClearPayloadVersion,
		Action:         tx.TaintActionClear,
		DecisionID:     decisionBytes,
		ReasonCategory: 2,
		EvidenceDigest: clearEvidence[:],
	}
	clearPayload, err := tx.EncodeTaintClearPayload(clear)
	require.NoError(t, err)
	clearTargetID, err := tx.TaintClearTargetID(clearPayload)
	require.NoError(t, err)
	clearProposal := &governance.ProposalState{
		Operation: governance.OpMemoryTaint,
		TargetID:  clearTargetID,
		Payload:   clearPayload,
	}
	require.NoError(t, app.applyAppV30TaintProposal(clearProposal, 21))
	header, found, err = app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusCleaning, header.Status)
	decisions, err = app.badgerStore.GetEffectiveTaintDecisions(target.MemoryID)
	require.NoError(t, err)
	require.Empty(t, decisions)
}

func TestAppV30TaintExecutionActualWritesFitReservation(t *testing.T) {
	app, _, target, _ := setupAppV30TaintGovernance(t)
	applyPayload, err := tx.EncodeTaintApplyPayload(appV30ApplyPayload(target))
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(applyPayload)
	require.NoError(t, err)
	applyProposal := &governance.ProposalState{
		Operation: governance.OpMemoryTaint,
		TargetID:  decisionID,
		Payload:   applyPayload,
	}

	applyReservation := appV30TaintExecutionEstimate(applyPayload)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	working := app.cloneForAppV20Finalize(scoped)
	require.NoError(t, working.badgerStore.BeginAppV30WriteStage(applyReservation))
	require.NoError(t, working.applyAppV30TaintProposal(applyProposal, 20))
	applyStats, err := working.badgerStore.CommitAppV30WriteStage()
	require.NoError(t, err)
	require.LessOrEqual(t, applyStats.Generated, applyReservation)
	require.NoError(t, scoped.CommitConsensusTransaction())

	decisionBytes, err := hex.DecodeString(decisionID)
	require.NoError(t, err)
	clearEvidence := sha256.Sum256([]byte("bounded clear evidence"))
	clearPayload, err := tx.EncodeTaintClearPayload(tx.TaintClearPayload{
		Version:        tx.TaintClearPayloadVersion,
		Action:         tx.TaintActionClear,
		DecisionID:     decisionBytes,
		ReasonCategory: 2,
		EvidenceDigest: clearEvidence[:],
	})
	require.NoError(t, err)
	clearTargetID, err := tx.TaintClearTargetID(clearPayload)
	require.NoError(t, err)
	clearProposal := &governance.ProposalState{
		Operation: governance.OpMemoryTaint,
		TargetID:  clearTargetID,
		Payload:   clearPayload,
	}

	clearReservation := appV30TaintExecutionEstimate(clearPayload)
	clearScoped := app.badgerStore.BeginConsensusTransaction(nil)
	clearWorking := app.cloneForAppV20Finalize(clearScoped)
	require.NoError(t, clearWorking.badgerStore.BeginAppV30WriteStage(clearReservation))
	require.NoError(t, clearWorking.applyAppV30TaintProposal(clearProposal, 21))
	clearStats, err := clearWorking.badgerStore.CommitAppV30WriteStage()
	require.NoError(t, err)
	require.LessOrEqual(t, clearStats.Generated, clearReservation)
	require.NoError(t, clearScoped.CommitConsensusTransaction())
}

func TestAppV30MemoryTaintValidatesRootAndCredentialScopes(t *testing.T) {
	app, _, target, _ := setupAppV30TaintGovernance(t)
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)

	apply := appV30ApplyPayload(target)
	apply.ScopeKind = tx.TaintScopeRootGeneration
	apply.PrincipalID = rootState.PrincipalID
	apply.GenerationFrom = 1
	apply.GenerationTo = rootState.Generation + 1
	payload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)
	_, err = app.validateAppV30TaintProposal(decisionID, nil, 0, payload, 20)
	require.ErrorContains(t, err, "future generation")

	apply = appV30ApplyPayload(target)
	apply.ScopeKind = tx.TaintScopeCredentialSet
	apply.Credentials = []string{"not-an-agent-id"}
	payload, err = tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err = tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)
	_, err = app.validateAppV30TaintProposal(decisionID, nil, 0, payload, 20)
	require.ErrorContains(t, err, "canonical agent ID")
}

func TestAppV30HistoricalOp12ProposalStaysInert(t *testing.T) {
	app, _, target, _ := setupAppV30TaintGovernance(t)
	apply := appV30ApplyPayload(target)
	payload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)

	proposal := &governance.ProposalState{
		Operation:     governance.OpMemoryTaint,
		TargetID:      decisionID,
		Payload:       payload,
		CreatedHeight: app.appV30AppliedHeight,
	}
	_, err = app.applyGovernanceProposal(proposal, 20)
	require.Error(t, err, "historical op-12 stays on the unknown-operation path")

	_, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.False(t, found)
}
