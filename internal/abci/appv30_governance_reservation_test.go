package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func prepareAppV30ReservedGovernance(t *testing.T, clear bool) (*SageApp, tx.ParentRef, string, string) {
	t.Helper()
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	app.state.Height = 19
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: root.id, PublicKey: root.pub, Power: 10,
	}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{root.id: 10}))
	// The direct governance fixture sets cached fork heights but does not run
	// app-v28 activation. Real FinalizeBlock also needs its promoted indexes
	// before it can compute the post-v28 composite AppHash.
	previousHash, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.PreparePublicMemoryStage(context.Background(), app.appV28AppliedHeight))
	require.NoError(t, app.badgerStore.PrepareCoCommitTombstoneStage(context.Background(), app.appV28AppliedHeight))
	prerequisites := app.badgerStore.BeginConsensusTransaction(nil)
	defer prerequisites.DiscardConsensusTransaction()
	require.NoError(t, prerequisites.PromotePublicMemoryStage(app.appV28AppliedHeight, previousHash))
	require.NoError(t, prerequisites.PromoteCoCommitTombstoneStage(app.appV28AppliedHeight, previousHash))
	require.NoError(t, prerequisites.CommitConsensusTransaction())
	require.NoError(t, app.badgerStore.ValidatePublicMemoryStage())
	require.NoError(t, app.badgerStore.ValidateCoCommitTombstoneStage())
	compositeHash, err := app.badgerStore.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.Len(t, compositeHash, sha256.Size)
	payload, err := tx.EncodeTaintApplyPayload(appV30ApplyPayload(target))
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)
	targetID := decisionID
	if clear {
		_, applyErr := app.badgerStore.ApplyTaintDecision(chainID, decisionID, target, sha256.Sum256(payload))
		require.NoError(t, applyErr)
		_, expandErr := app.badgerStore.ExpandTaintDecision(chainID, decisionID, taint.PerExecutionUnitBudget)
		require.NoError(t, expandErr)
		decisionBytes, decodeErr := hex.DecodeString(decisionID)
		require.NoError(t, decodeErr)
		evidence := sha256.Sum256([]byte("governance reservation clear"))
		payload, err = tx.EncodeTaintClearPayload(tx.TaintClearPayload{
			Version: tx.TaintClearPayloadVersion, Action: tx.TaintActionClear,
			DecisionID: decisionBytes, ReasonCategory: 2, EvidenceDigest: evidence[:],
		})
		require.NoError(t, err)
		targetID, err = tx.TaintClearTargetID(payload)
		require.NoError(t, err)
	}
	proposalID, err := app.govEngine.ProposeWithoutAutoVote(root.id, governance.OpMemoryTaint,
		targetID, nil, 0, governance.MinExpiryBlocks, "governance reservation", 20, payload)
	require.NoError(t, err)
	// Exercise the real voted governance execution path, not direct application.
	require.NoError(t, app.govEngine.Vote(proposalID, root.id, "accept", 20))
	return app, target, decisionID, proposalID
}

func TestAppV30GovernanceExecutionKeepsReservationThroughApply(t *testing.T) {
	for _, clear := range []bool{false, true} {
		name := "apply-and-expand"
		if clear {
			name = "clear-and-cleanup"
		}
		t.Run(name, func(t *testing.T) {
			app, _, decisionID, proposalID := prepareAppV30ReservedGovernance(t, clear)
			var working *SageApp
			var chargedUpdates int
			var maximumGenerated int64
			scoped := app.badgerStore.BeginConsensusTransaction(func(int) {
				if working == nil {
					return
				}
				stats := working.badgerStore.AppV30WriteStageStats()
				if stats.Generated > maximumGenerated {
					chargedUpdates++
					maximumGenerated = stats.Generated
				}
			})
			t.Cleanup(scoped.DiscardConsensusTransaction)
			working = app.cloneForAppV20Finalize(scoped)
			response, err := working.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{
				Height: 20, Time: appV23BlockTime(),
			})
			require.NoError(t, err)
			require.NotNil(t, response)
			require.GreaterOrEqual(t, chargedUpdates, 2, "both taint update calls must execute inside the reservation")
			require.Positive(t, maximumGenerated, "actual governance writes must be metered")
			require.Zero(t, scoped.AppV30WriteStageStats())
			proposal, err := working.govEngine.LoadProposal(proposalID)
			require.NoError(t, err)
			require.Equal(t, governance.StatusExecuted, proposal.Status)
			active, err := working.govEngine.GetActiveProposal()
			require.NoError(t, err)
			require.Nil(t, active)
			header, found, err := scoped.GetTaintDecision(decisionID)
			require.NoError(t, err)
			require.True(t, found)
			if clear {
				require.Equal(t, taint.StatusCleaning, header.Status)
				require.Equal(t, taint.CleanupQueue, header.CleanupPhase)
			} else {
				require.Equal(t, taint.StatusComplete, header.Status)
			}
			require.NoError(t, scoped.ValidateTaintState())
		})
	}
}

func TestAppV30GovernancePartialApplyRollsBackReservation(t *testing.T) {
	app, target, decisionID, proposalID := prepareAppV30ReservedGovernance(t, false)
	// A real reverse lineage edge with a deliberately missing child envelope
	// fails Expand after Apply has already written the header/target/member/frontier.
	require.NoError(t, app.badgerStore.SetMemoryLineage("missing-canonical-child", []tx.ParentRef{target}))
	beforeRevision, err := app.badgerStore.TaintRevision()
	require.NoError(t, err)
	var working *SageApp
	partialWritesObserved := false
	scoped := app.badgerStore.BeginConsensusTransaction(func(int) {
		if working != nil && working.badgerStore.AppV30WriteStageStats().Generated > 0 {
			partialWritesObserved = true
			working.pendingWrites = append(working.pendingWrites, pendingWrite{writeType: "partial-governance-mirror"})
		}
	})
	t.Cleanup(scoped.DiscardConsensusTransaction)
	working = app.cloneForAppV20Finalize(scoped)
	response, err := working.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 20, Time: appV23BlockTime(),
	})
	require.ErrorIs(t, err, store.ErrTaintStateMalformed)
	require.ErrorContains(t, err, "has no canonical memory")
	require.Nil(t, response)
	require.True(t, partialWritesObserved, "the test must reach actual charged Apply writes before failure")
	require.Empty(t, working.pendingWrites, "partial governance mirrors must not survive the failed stage")
	require.Zero(t, scoped.AppV30WriteStageStats())
	_, found, err := scoped.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.False(t, found)
	revision, err := scoped.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, beforeRevision, revision)
	activeCount, err := scoped.ActiveTaintDecisionCount()
	require.NoError(t, err)
	require.Zero(t, activeCount)
	require.NoError(t, scoped.ValidateTaintState(), "no orphan target, membership, index or frontier may remain")
	active, err := working.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, proposalID, active.ProposalID)
	require.Equal(t, governance.StatusVoting, active.Status)
	votes, err := working.govEngine.GetProposalVotes(proposalID)
	require.NoError(t, err)
	require.Len(t, votes, 1, "the original vote must remain available after rollback")
	// This only proves the savepoint leaves its store handle usable; a real
	// FinalizeBlock error still discards the entire enclosing block transaction.
	require.NoError(t, scoped.SetState("reservation-after-failed-governance", []byte("usable")))
	require.NoError(t, scoped.ConsensusTransactionError())
}
