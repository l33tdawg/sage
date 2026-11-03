package abci

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30MeteredWorkPreservesFatalPartialMutation(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	goodChild := tx.ParentRef{SourceChainID: chainID, MemoryID: "a-valid-child", ContentHash: sha256.Sum256([]byte("valid child"))}
	require.NoError(t, app.badgerStore.SetMemoryHash(goodChild.MemoryID, goodChild.ContentHash[:], "committed"))
	require.NoError(t, app.badgerStore.SetMemoryLineage(goodChild.MemoryID, []tx.ParentRef{target}))
	require.NoError(t, app.badgerStore.SetMemoryLineage("z-missing-child", []tx.ParentRef{target}))
	decisionID := strings.Repeat("e", 64)
	_, err := app.badgerStore.ApplyTaintDecision(chainID, decisionID, target, sha256.Sum256([]byte("fatal partial work")))
	require.NoError(t, err)
	before, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	parsed := makeMemoryTaintWorkTx(t, root, &tx.MemoryTaintWork{
		Version: tx.MemoryTaintWorkVersion, Kind: tx.MemoryTaintWorkExpand,
		DecisionID: decisionID, Generation: before.Generation, FrontierHead: before.FrontierHead,
	}, 1)

	// Establish that the real fixture mutates canonical child state before
	// encountering the missing next child and poisoning the typed update.
	control := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(control.DiscardConsensusTransaction)
	controlApp := app.cloneForAppV20Finalize(control)
	result := controlApp.processMemoryTaintWork(parsed, 20)
	require.Equal(t, uint32(12), result.Code, result.Log)
	require.ErrorIs(t, control.ConsensusTransactionError(), store.ErrTaintStateMalformed)
	decisions, err := control.GetEffectiveTaintDecisions(goodChild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions, "the fixture must reach a real child mutation before failing")
	control.DiscardConsensusTransaction()

	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	working := app.cloneForAppV20Finalize(scoped)
	working.pendingWrites = []pendingWrite{{writeType: "earlier-transaction"}}
	result, err = working.processAppV30MeteredTx(parsed, 20, appV23BlockTime())
	require.ErrorIs(t, err, store.ErrTaintStateMalformed)
	require.ErrorContains(t, err, "storage failure")
	require.ErrorContains(t, err, "has no canonical memory")
	require.Nil(t, result, "a genuine partial-update poison must abort FinalizeBlock, not become Code12")
	require.Len(t, working.pendingWrites, 1)
	require.Equal(t, "earlier-transaction", working.pendingWrites[0].writeType)
	require.Zero(t, scoped.AppV30WriteStageStats())
	after, found, readErr := scoped.GetTaintDecision(decisionID)
	require.NoError(t, readErr)
	require.True(t, found)
	require.Equal(t, before, after)
	decisions, readErr = scoped.GetEffectiveTaintDecisions(goodChild.MemoryID)
	require.NoError(t, readErr)
	require.Empty(t, decisions, "the rejected partial child membership must be rolled back")
	decisions, readErr = app.badgerStore.GetEffectiveTaintDecisions(goodChild.MemoryID)
	require.NoError(t, readErr)
	require.Empty(t, decisions, "no rejected working state was published")
}
