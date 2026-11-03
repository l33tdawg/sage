package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtTypes "github.com/cometbft/cometbft/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func prepareAppV30ScopeReservationBound(t *testing.T) (*SageApp, agentKey, tx.ParentRef, []byte, string) {
	t.Helper()
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("scope reservation bounds", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	chainID := strings.Repeat("c", cmtTypes.MaxChainIDLen)
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(chainID))
	activateAppV30ForMemorySubmitTest(t, app, 20)
	app.state.Height = 19
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: root.id, PublicKey: root.pub, Power: 10}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{root.id: 10}))
	// Promote the historical v28 indexes before creating any postfork max-ID
	// records. This fixture must exercise incremental Finalize behavior, not
	// ask historical strict migration selection to adopt a wider envelope.
	previousHash, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.PreparePublicMemoryStage(context.Background(), app.appV28AppliedHeight))
	require.NoError(t, app.badgerStore.PrepareCoCommitTombstoneStage(context.Background(), app.appV28AppliedHeight))
	prerequisites := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(prerequisites.DiscardConsensusTransaction)
	require.NoError(t, prerequisites.PromotePublicMemoryStage(app.appV28AppliedHeight, previousHash))
	require.NoError(t, prerequisites.PromoteCoCommitTombstoneStage(app.appV28AppliedHeight, previousHash))
	require.NoError(t, prerequisites.CommitConsensusTransaction())
	target := tx.ParentRef{SourceChainID: chainID, MemoryID: strings.Repeat("t", taint.MaxMemoryIDBytes), ContentHash: sha256.Sum256([]byte("maximum scope reservation target"))}
	seed := func(id string, hash [sha256.Size]byte) {
		require.NoError(t, app.badgerStore.SetMemoryHash(id, hash[:], "committed"))
		require.NoError(t, app.badgerStore.SetMemoryDomain(id, "general"))
		require.NoError(t, app.badgerStore.SetMemoryAuthor(id, root.id))
		require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(id, rootState.PrincipalID))
		require.NoError(t, app.badgerStore.SetMemoryClassification(id, uint8(store.ClearanceInternal)))
	}
	seed(target.MemoryID, target.ContentHash)
	for i := 0; i < 2*taint.MaxNewMembershipsPerExec; i++ {
		prefix := fmt.Sprintf("scope-reservation-child-%04d-", i)
		id := prefix + strings.Repeat("x", taint.MaxMemoryIDBytes-len(prefix))
		seed(id, sha256.Sum256([]byte(id)))
		require.NoError(t, app.badgerStore.SetMemoryLineage(id, []tx.ParentRef{target}))
	}
	apply := appV30ApplyPayload(target)
	apply.ScopeKind = tx.TaintScopeCredentialSet
	apply.Credentials = make([]string, tx.MaxTaintCredentialSetEntries)
	for i := range apply.Credentials {
		apply.Credentials[i] = fmt.Sprintf("%064x", i+1)
	}
	apply.ReasonCategory = 255 // This field is an opaque byte, not a variable-length reason string.
	payload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)
	return app, root, target, payload, decisionID
}

func finishAppV30ScopeReservationExpansion(t *testing.T, app *SageApp, chainID, decisionID string) {
	t.Helper()
	for call := 0; call < 12; call++ {
		expanded, err := app.badgerStore.ExpandTaintDecision(chainID, decisionID, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		if expanded.Status == taint.StatusComplete {
			return
		}
	}
	t.Fatal("bounded max-ID fixture did not finish expansion")
}

func scopeReservationClearPayload(t *testing.T, decisionID string) ([]byte, string) {
	t.Helper()
	decision, err := hex.DecodeString(decisionID)
	require.NoError(t, err)
	evidence := sha256.Sum256([]byte("maximum scope cleanup"))
	payload, err := tx.EncodeTaintClearPayload(tx.TaintClearPayload{
		Version: tx.TaintClearPayloadVersion, Action: tx.TaintActionClear,
		DecisionID: decision, ReasonCategory: 255, EvidenceDigest: evidence[:],
	})
	require.NoError(t, err)
	targetID, err := tx.TaintClearTargetID(payload)
	require.NoError(t, err)
	return payload, targetID
}

func TestAppV30ScopeReservationBoundsActualApplyClearAndRelease(t *testing.T) {
	app, _, target, payload, decisionID := prepareAppV30ScopeReservationBound(t)
	applyEstimate := appV30TaintExecutionEstimate(payload)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	working := app.cloneForAppV20Finalize(scoped)
	require.NoError(t, scoped.BeginAppV30WriteStage(applyEstimate))
	require.NoError(t, working.applyAppV30TaintProposal(&governance.ProposalState{
		Operation: governance.OpMemoryTaint, TargetID: decisionID, Payload: payload,
	}, 20))
	stats, err := scoped.CommitAppV30WriteStage()
	require.NoError(t, err)
	require.Positive(t, stats.Generated)
	require.LessOrEqual(t, stats.Generated, applyEstimate)
	header, found, err := scoped.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Greater(t, header.Materialized, uint32(taint.MaxNewMembershipsPerExec/2))
	require.LessOrEqual(t, header.Materialized, uint32(taint.MaxNewMembershipsPerExec+1))
	encoded, err := taint.EncodeHeader(header)
	require.NoError(t, err)
	require.Greater(t, int64(len(taint.DecisionKey(decisionID))+len(encoded)), appV30HeaderMaxBytes,
		"only the whole reservation, not the old named header component, is a conservative bound")
	decisionScope, found, err := scoped.GetTaintDecisionScope(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, decisionScope.Credentials, tx.MaxTaintCredentialSetEntries)
	t.Logf("max-scope Apply+Expand: actual=%d reservation=%d materialized=%d header-key-value=%d", stats.Generated, applyEstimate, header.Materialized, len(taint.DecisionKey(decisionID))+len(encoded))
	require.NoError(t, scoped.CommitConsensusTransaction())
	finishAppV30ScopeReservationExpansion(t, app, target.SourceChainID, decisionID)

	clearPayload, clearTarget := scopeReservationClearPayload(t, decisionID)
	clearEstimate := appV30TaintExecutionEstimate(clearPayload)
	clearScoped := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(clearScoped.DiscardConsensusTransaction)
	clearWorking := app.cloneForAppV20Finalize(clearScoped)
	require.NoError(t, clearScoped.BeginAppV30WriteStage(clearEstimate))
	require.NoError(t, clearWorking.applyAppV30TaintProposal(&governance.ProposalState{
		Operation: governance.OpMemoryTaint, TargetID: clearTarget, Payload: clearPayload,
	}, 21))
	clearStats, err := clearScoped.CommitAppV30WriteStage()
	require.NoError(t, err)
	require.Positive(t, clearStats.Generated)
	require.LessOrEqual(t, clearStats.Generated, clearEstimate)
	header, found, err = clearScoped.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusCleaning, header.Status)
	t.Logf("max-ID Clear+Cleanup340: actual=%d reservation=%d", clearStats.Generated, clearEstimate)
	require.NoError(t, clearScoped.CommitConsensusTransaction())
	remaining := 0
	require.NoError(t, app.badgerStore.DB().View(func(txn *badger.Txn) error {
		prefix := taint.DecisionIndexPrefix(decisionID)
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		options.PrefetchValues = false
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			remaining++
		}
		return nil
	}))
	require.Equal(t, 2*taint.MaxNewMembershipsPerExec+1-appV30MaxCleanupMemberPairs, remaining,
		"the Clear execution must really delete340 max-ID membership pairs")

	clean := false
	for call := 0; call < 8; call++ {
		estimate := appV30GeneratedWriteEstimate(&tx.ParsedTx{Type: tx.TxTypeMemoryTaintWork,
			MemoryTaintWork: &tx.MemoryTaintWork{Version: tx.MemoryTaintWorkVersion, Kind: tx.MemoryTaintWorkCleanup, DecisionID: decisionID, Generation: header.Generation}})
		control := app.badgerStore.BeginConsensusTransaction(nil)
		t.Cleanup(control.DiscardConsensusTransaction)
		require.NoError(t, control.BeginAppV30WriteStage(estimate))
		cleaned, err := control.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		actual, err := control.CommitAppV30WriteStage()
		require.NoError(t, err)
		require.LessOrEqual(t, actual.Generated, estimate)
		t.Logf("cleanup phase=%d released=%t actual=%d reservation=%d", cleaned.Phase, cleaned.ActiveReleased, actual.Generated, estimate)
		require.NoError(t, control.CommitConsensusTransaction())
		if cleaned.Status == taint.StatusClean {
			require.True(t, cleaned.ActiveReleased)
			clean = true
			break
		}
	}
	require.True(t, clean)
	count, err := app.badgerStore.ActiveTaintDecisionCount()
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, app.badgerStore.ValidateTaintState())
}

func TestAppV30ScopeReservationBoundsVotedGovernanceAndPendingCarry(t *testing.T) {
	for _, clear := range []bool{false, true} {
		t.Run(fmt.Sprintf("clear_%t", clear), func(t *testing.T) {
			app, root, target, payload, decisionID := prepareAppV30ScopeReservationBound(t)
			targetID := decisionID
			if clear {
				decoded, err := tx.DecodeTaintApplyPayload(payload)
				require.NoError(t, err)
				_, err = app.badgerStore.ApplyTaintDecisionWithScope(target.SourceChainID, decisionID, target, sha256.Sum256(payload), store.TaintDecisionScopeFromPayload(*decoded))
				require.NoError(t, err)
				finishAppV30ScopeReservationExpansion(t, app, target.SourceChainID, decisionID)
				payload, targetID = scopeReservationClearPayload(t, decisionID)
			}
			proposalID, err := app.govEngine.ProposeWithoutAutoVote(root.id, governance.OpMemoryTaint,
				targetID, nil, 0, governance.MinExpiryBlocks, "bounded scope generated writes", 20, payload)
			require.NoError(t, err)
			// Before the execution vote, the real canonical carried proposal must
			// already reserve enough for its later max-scope/max-ID execution.
			carry, err := app.appV30PendingGovernanceReservation(20)
			require.NoError(t, err)
			require.Positive(t, carry)
			reservation := appV30TaintExecutionEstimate(payload)
			require.GreaterOrEqual(t, carry, reservation)
			total, err := app.appV30BlockGeneratedWriteEstimate(nil, 20)
			require.NoError(t, err)
			require.Equal(t, carry, total)
			headerBefore, existsBefore, err := app.badgerStore.GetTaintDecision(decisionID)
			require.NoError(t, err)
			if !clear {
				require.False(t, existsBefore)
			} else {
				require.Equal(t, taint.StatusComplete, headerBefore.Status)
			}
			require.NoError(t, app.govEngine.Vote(proposalID, root.id, "accept", 20))
			var working *SageApp
			var maxGenerated int64
			chargedUpdates := 0
			control := app.badgerStore.BeginConsensusTransaction(func(int) {
				if working == nil {
					return
				}
				stats := working.badgerStore.AppV30WriteStageStats()
				if stats.Generated > maxGenerated {
					require.Equal(t, reservation, stats.Limit)
					require.LessOrEqual(t, stats.Generated, reservation)
					chargedUpdates++
					maxGenerated = stats.Generated
				}
			})
			t.Cleanup(control.DiscardConsensusTransaction)
			working = app.cloneForAppV20Finalize(control)
			response, err := working.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 20, Time: appV23BlockTime()})
			require.NoError(t, err)
			require.NotNil(t, response)
			require.GreaterOrEqual(t, chargedUpdates, 2, "actual Apply+Expand or Clear+Cleanup must remain inside the same stage")
			require.Positive(t, maxGenerated)
			require.LessOrEqual(t, maxGenerated, carry)
			require.Zero(t, control.AppV30WriteStageStats())
			proposal, err := working.govEngine.LoadProposal(proposalID)
			require.NoError(t, err)
			require.Equal(t, governance.StatusExecuted, proposal.Status)
			header, found, err := control.GetTaintDecision(decisionID)
			require.NoError(t, err)
			require.True(t, found)
			if clear {
				require.Equal(t, taint.StatusCleaning, header.Status)
			} else {
				require.Greater(t, header.Materialized, uint32(taint.MaxNewMembershipsPerExec/2))
				decisionScope, found, err := control.GetTaintDecisionScope(decisionID)
				require.NoError(t, err)
				require.True(t, found)
				require.Len(t, decisionScope.Credentials, tx.MaxTaintCredentialSetEntries)
			}
			t.Logf("voted governance clear=%t generated=%d exact-reservation=%d pre-vote-carry=%d", clear, maxGenerated, reservation, carry)
			require.NoError(t, control.ValidateTaintState())
		})
	}
}
