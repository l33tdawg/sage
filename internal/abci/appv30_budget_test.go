package abci

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func makeAppV30WorkRawTx(t *testing.T, nonce uint64) []byte {
	t.Helper()
	parsed := &tx.ParsedTx{
		Type:  tx.TxTypeMemoryTaintWork,
		Nonce: nonce,
		MemoryTaintWork: &tx.MemoryTaintWork{
			Version:    tx.MemoryTaintWorkVersion,
			Kind:       tx.MemoryTaintWorkExpand,
			DecisionID: strings.Repeat("a", taint.DecisionIDBytes),
			Generation: 1,
		},
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
	}
	encoded, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return encoded
}

func TestAppV30MembershipTripleMatchesProductionCodec(t *testing.T) {
	memoryID := strings.Repeat("m", taint.MaxMemoryIDBytes)
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	membership, err := taint.EncodeMembership(taint.Membership{Generation: 1, Origin: taint.OriginExpand})
	require.NoError(t, err)
	queue, err := taint.EncodeQueueEntry(taint.QueueEntry{ParentRefID: decisionID, MemoryID: memoryID})
	require.NoError(t, err)
	actual := len(taint.MembershipKey(memoryID, decisionID)) + len(membership) +
		len(taint.DecisionIndexKey(decisionID, memoryID)) +
		len(taint.FrontierKey(decisionID, 1, 1)) + len(queue)
	require.EqualValues(t, actual, appV30MembershipTripleBytes)
}

func TestAppV30GeneratedWriteBudgetPrepareProcessAndFinalize(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 19
	app.appV20AppliedHeight = 5
	app.appV30AppliedHeight = 19
	decisionID := strings.Repeat("b", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		"sage",
		decisionID,
		tx.ParentRef{SourceChainID: "sage", MemoryID: "budget-target"},
		[32]byte{1},
	)
	require.NoError(t, err)
	require.True(t, created)

	txs := make([][]byte, 12)
	for i := range txs {
		txs[i] = makeAppV30WorkRawTx(t, uint64(i+1))
	}
	perTx := app.appV30EstimatedTxBytes(txs[0], 20)
	require.EqualValues(t, 383_076, perTx)
	require.LessOrEqual(t, perTx*10, appV30BlockWriteBudgetBytes)
	require.Greater(t, perTx*11, appV30BlockWriteBudgetBytes)

	prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height:     20,
		Time:       time.Unix(1_700_000_020, 0).UTC(),
		Txs:        txs,
		MaxTxBytes: 2_000_000,
	})
	require.NoError(t, err)
	require.Len(t, prepared.Txs, 10)

	accepted, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 20,
		Txs:    txs[:10],
	})
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, accepted.Status)
	rejected, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 20,
		Txs:    txs,
	})
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, rejected.Status)

	finalized, err := app.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 20,
		Time:   time.Unix(1_700_000_020, 0).UTC(),
		Txs:    txs,
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, len(txs))
	for _, result := range finalized.TxResults {
		require.Equal(t, appV30GeneratedWriteBudgetCode, result.Code)
		require.Equal(t, appV30GeneratedWriteBudgetLog, result.Log)
	}
	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusExpanding, header.Status)
	require.Equal(t, uint64(0), header.FrontierHead)
}

func TestAppV30WorstSubmitEstimateMatchesBenchmarkBound(t *testing.T) {
	submit := &tx.MemorySubmit{
		MemoryID: strings.Repeat("m", taint.MaxMemoryIDBytes),
	}
	for i := 0; i < taint.MaxParents; i++ {
		submit.DerivedFrom = append(submit.DerivedFrom, tx.ParentRef{
			SourceChainID: strings.Repeat("c", tx.MaxDerivedFromChainIDBytes),
			MemoryID:      strings.Repeat(string(rune('a'+i)), tx.MaxDerivedFromMemoryIDBytes),
			ContentHash:   [32]byte{byte(i)},
		})
	}
	estimate := appV30GeneratedWriteEstimate(&tx.ParsedTx{
		Type:         tx.TxTypeMemorySubmit,
		MemorySubmit: submit,
	})
	require.EqualValues(t, 80_253, estimate)
	require.LessOrEqual(t, estimate*52, appV30BlockWriteBudgetBytes)
	require.Greater(t, estimate*53, appV30BlockWriteBudgetBytes)
}

func TestAppV30ActiveGovernanceReserveCountsBeforeExecutionVote(t *testing.T) {
	app, root, target, _ := setupAppV30TaintGovernance(t)
	payload, err := tx.EncodeTaintApplyPayload(appV30ApplyPayload(target))
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)

	pubKey, sig, bodyHash, timestamp := signAgentProof(t, root, []byte("reserved taint proposal"))
	parsed := &tx.ParsedTx{
		Type:  tx.TxTypeGovPropose,
		Nonce: 1,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpMemoryTaint,
			TargetID:  decisionID,
			Payload:   payload,
			Reason:    "reserved taint proposal",
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: timestamp,
	}
	require.NoError(t, tx.SignTx(parsed, root.priv))
	result := app.processGovPropose(parsed, 20, appV23BlockTime())
	require.Equal(t, uint32(0), result.Code, result.Log)

	want := appV30TaintExecutionEstimate(payload)
	reserve, err := app.appV30ActiveGovernanceReserve(20)
	require.NoError(t, err)
	require.Equal(t, want, reserve)
	total, err := app.appV30BlockGeneratedWriteEstimate(nil, 20)
	require.NoError(t, err)
	require.Equal(t, want, total)

	txs := make([][]byte, 11)
	for i := range txs {
		txs[i] = makeAppV30WorkRawTx(t, uint64(i+1))
	}
	selected, err := app.selectAppV30Proposal(txs, 20)
	require.NoError(t, err)
	require.Len(t, selected, 9, "the active execution reserve must remain outside proposal selection")
	_, err = app.appV30BlockGeneratedWriteEstimate(txs, 20)
	require.ErrorContains(t, err, "exceed")
}

func TestAppV30WorstSubmitActualGeneratedWritesStayWithinEstimate(t *testing.T) {
	app := setupTestApp(t)
	parents := make([]tx.ParentRef, taint.MaxParents)
	for i := range parents {
		parents[i] = tx.ParentRef{
			SourceChainID: "sage",
			MemoryID:      "submit-parent-" + strings.Repeat(string(rune('a'+i)), 8),
			ContentHash:   [32]byte{byte(i + 1)},
		}
	}

	decisions := make([]string, taint.MaxActiveDecisions)
	for i := range decisions {
		decisions[i] = testTaintDecisionIDForBudget(i)
		created, err := app.badgerStore.ApplyTaintDecision(
			"sage",
			decisions[i],
			parents[0],
			[32]byte{byte(i + 1)},
		)
		require.NoError(t, err)
		require.True(t, created)
	}
	require.NoError(t, app.badgerStore.DB().Update(func(txn *badger.Txn) error {
		for _, decisionID := range decisions {
			item, err := txn.Get(taint.MembershipKey(parents[0].MemoryID, decisionID))
			require.NoError(t, err)
			value, err := item.ValueCopy(nil)
			require.NoError(t, err)
			for _, parent := range parents[1:] {
				require.NoError(t, txn.Set(taint.MembershipKey(parent.MemoryID, decisionID), value))
				require.NoError(t, txn.Set(taint.DecisionIndexKey(decisionID, parent.MemoryID), []byte{}))
			}
		}
		return nil
	}))

	childID := "submit-child-" + strings.Repeat("m", taint.MaxMemoryIDBytes-20)
	signerID := strings.Repeat("a", 64)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	require.NoError(t, scoped.BeginAppV30WriteStage(1<<20))
	require.NoError(t, scoped.SetMemoryLineage(childID, parents))
	inherited, err := scoped.InheritTaintMemberships("sage", childID, parents)
	require.NoError(t, err)
	require.Equal(t, taint.MaxActiveDecisions, inherited.Decisions)
	require.NoError(t, scoped.SetAppV30Provenance(
		childID,
		[]store.AppV30ProvenanceSigner{{
			CredentialID: signerID,
			PrincipalID:  signerID,
			Exact:        true,
		}},
	))
	stats, err := scoped.CommitAppV30WriteStage()
	require.NoError(t, err)
	require.Positive(t, stats.Generated)
	require.LessOrEqual(t, stats.Generated, int64(80_253))
}

func testTaintDecisionIDForBudget(i int) string {
	return fmt.Sprintf("%064x", i+1)
}

func TestAppV30RejectedSubmitRollsBackPendingMirrors(t *testing.T) {
	app, root, target, _ := setupAppV30TaintGovernance(t)
	const domain = "stage.rollback"
	parsed := makeMemorySubmitTx(t, root, domain, "conflicting canonical envelope")
	parsed.MemorySubmit.MemoryID = target.MemoryID
	parsed.Nonce = 1
	require.NoError(t, tx.SignTx(parsed, root.priv))

	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	working := app.cloneForAppV20Finalize(scoped)
	working.pendingWrites = []pendingWrite{{writeType: "earlier-transaction"}}
	result, err := working.processAppV30MeteredTx(parsed, 20, appV23BlockTime())
	require.NoError(t, err)
	require.Equal(t, uint32(11), result.Code, result.Log)
	require.Contains(t, result.Log, "different canonical envelope")
	_, ownerErr := scoped.GetDomainOwner(domain)
	require.Error(t, ownerErr, "the rejected transaction's domain claim must roll back")
	require.Len(t, working.pendingWrites, 1, "rolled-back domain and grant mirrors must not publish")
	require.Equal(t, "earlier-transaction", working.pendingWrites[0].writeType)

	accepted := makeMemorySubmitTx(t, root, domain, "accepted after rollback")
	accepted.Nonce = 2
	require.NoError(t, tx.SignTx(accepted, root.priv))
	result, err = working.processAppV30MeteredTx(accepted, 20, appV23BlockTime())
	require.NoError(t, err)
	require.Equal(t, uint32(0), result.Code, result.Log)
	require.Len(t, working.pendingWrites, 5, "the later accepted transaction must retain its mirrors")
	require.Equal(t, "earlier-transaction", working.pendingWrites[0].writeType)
	require.NoError(t, scoped.CommitConsensusTransaction())
}
