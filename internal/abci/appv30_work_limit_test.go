package abci

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func makeAppV30WorkKindRawTx(t *testing.T, kind tx.MemoryTaintWorkKind, nonce uint64) []byte {
	t.Helper()
	parsed, err := tx.DecodeTx(makeAppV30WorkRawTx(t, nonce))
	require.NoError(t, err)
	parsed.MemoryTaintWork.Kind = kind
	if kind == tx.MemoryTaintWorkCleanup {
		parsed.MemoryTaintWork.CleanupPhase = uint8(taint.CleanupMembers)
	}
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

func TestAppV30TaintWorkLimitPrepareProcessAndFinalize(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 19
	app.appV20AppliedHeight = 5
	app.appV30AppliedHeight = 19
	txs := make([][]byte, 5)
	for i := range txs {
		kind := tx.MemoryTaintWorkExpand
		if i%2 == 1 {
			kind = tx.MemoryTaintWorkCleanup
		}
		txs[i] = makeAppV30WorkKindRawTx(t, kind, uint64(i+1))
	}
	_, err := app.appV30BlockGeneratedWriteEstimate(txs[:4], 20)
	require.NoError(t, err)
	_, err = app.appV30BlockGeneratedWriteEstimate(txs, 20)
	require.ErrorIs(t, err, errAppV30TaintWorkLimitExceeded)
	require.NotErrorIs(t, err, errAppV30GeneratedWriteBudgetExceeded)

	// The fifth work item must not prevent a later unrelated item from fitting.
	later := makeAppV30BudgetSubmitRawTx(t, 6)
	prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height: 20, Txs: append(append([][]byte(nil), txs...), later), MaxTxBytes: 2_000_000,
	})
	require.NoError(t, err)
	want := append(append([][]byte(nil), txs[:4]...), later)
	require.Equal(t, want, prepared.Txs)
	accepted, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 20, Txs: want,
	})
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, accepted.Status)
	rejected, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 20, Txs: txs,
	})
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, rejected.Status)

	// This is a proposal-policy fixture, not signed successful work execution.
	// The block-wide guard rejects before attempting the individual payloads.
	before, err := app.badgerStore.TaintRevision()
	require.NoError(t, err)
	finalized, err := app.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 20, Time: time.Unix(1_700_000_020, 0).UTC(), Txs: txs,
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 5)
	for _, result := range finalized.TxResults {
		require.Equal(t, appV30TaintWorkLimitCode, result.Code)
		require.Equal(t, appV30TaintWorkLimitLog, result.Log)
	}
	after, err := app.badgerStore.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, app.appV30WorkTxCount)
}

func TestAppV30TaintWorkLimitCountsCompletedNoOps(t *testing.T) {
	for _, kind := range []tx.MemoryTaintWorkKind{tx.MemoryTaintWorkExpand, tx.MemoryTaintWorkCleanup} {
		name := "complete expansion"
		if kind == tx.MemoryTaintWorkCleanup {
			name = "clean cleanup"
		}
		t.Run(name, func(t *testing.T) {
			app, root, target, chainID := setupAppV30TaintGovernance(t)
			decisionID := strings.Repeat("a", taint.DecisionIDBytes)
			created, err := app.badgerStore.ApplyTaintDecision(chainID, decisionID, target,
				sha256.Sum256([]byte("work limit no-op")))
			require.NoError(t, err)
			require.True(t, created)
			_, err = app.badgerStore.ExpandTaintDecision(chainID, decisionID, taint.PerExecutionUnitBudget)
			require.NoError(t, err)
			wantStatus := taint.StatusComplete
			if kind == tx.MemoryTaintWorkCleanup {
				changed, clearErr := app.badgerStore.ClearTaintDecision(decisionID)
				require.NoError(t, clearErr)
				require.True(t, changed)
				for i := 0; i < 4; i++ {
					_, cleanupErr := app.badgerStore.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
					require.NoError(t, cleanupErr)
				}
				wantStatus = taint.StatusClean
			}
			header, found, err := app.badgerStore.GetTaintDecision(decisionID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, wantStatus, header.Status)
			work := &tx.MemoryTaintWork{
				Version: tx.MemoryTaintWorkVersion, Kind: kind, DecisionID: decisionID, Generation: 1,
			}
			if kind == tx.MemoryTaintWorkCleanup {
				work.CleanupPhase = uint8(taint.CleanupMembers)
			}
			result := app.processMemoryTaintWork(makeMemoryTaintWorkTx(t, root, work, 1), 20)
			require.Equal(t, uint32(0), result.Code, result.Log)
			require.Zero(t, app.appV30WorkTxCount, "the execution counter skips these no-ops")
			txs := make([][]byte, 5)
			for i := range txs {
				txs[i] = makeAppV30WorkKindRawTx(t, kind, uint64(i+1))
			}
			_, err = app.appV30BlockGeneratedWriteEstimate(txs, 20)
			require.ErrorIs(t, err, errAppV30TaintWorkLimitExceeded)
			selected, err := app.selectAppV30Proposal(txs, 20)
			require.NoError(t, err)
			require.Equal(t, txs[:4], selected)
		})
	}
}

func TestAppV30TaintWorkLimitMalformedAndNonWork(t *testing.T) {
	app := setupTestApp(t)
	app.appV30AppliedHeight = 19
	work := makeAppV30WorkRawTx(t, 1)
	badKind := append([]byte(nil), work...)
	badKind[6] = 0xff // type + payload length + version precede the work kind.
	malformed := [][]byte{nil, work[:5], badKind}
	for _, raw := range malformed {
		_, decodeErr := tx.DecodeTx(raw)
		require.Error(t, decodeErr)
		_, isWork := app.appV30EstimatedTxResources(raw, 20)
		require.False(t, isWork)
	}
	nonWork := makeAppV30BudgetSubmitRawTx(t, 2)
	_, isWork := app.appV30EstimatedTxResources(nonWork, 20)
	require.False(t, isWork)
	txs := make([][]byte, 4, 4+len(malformed)+1)
	for i := range txs {
		txs[i] = makeAppV30WorkRawTx(t, uint64(i+1))
	}
	txs = append(txs, malformed...)
	txs = append(txs, nonWork)
	_, err := app.appV30BlockGeneratedWriteEstimate(txs, 20)
	require.NoError(t, err)
	selected, err := app.selectAppV30Proposal(txs, 20)
	require.NoError(t, err)
	require.Equal(t, txs, selected, "malformed entries keep their existing selection behavior without taking a work slot")
}

func TestAppV30TaintWorkLimitForkBoundary(t *testing.T) {
	app := setupTestApp(t)
	txs := make([][]byte, 5)
	for i := range txs {
		txs[i] = makeAppV30WorkRawTx(t, uint64(i+1))
	}
	app.appV30AppliedHeight = 19
	for _, height := range []int64{18, 19} {
		estimate, err := app.appV30BlockGeneratedWriteEstimate(txs, height)
		require.NoError(t, err)
		require.Zero(t, estimate)
		selected, err := app.selectAppV30Proposal(txs, height)
		require.NoError(t, err)
		require.Equal(t, txs, selected)
	}
	_, err := app.appV30BlockGeneratedWriteEstimate(txs, 20)
	require.ErrorIs(t, err, errAppV30TaintWorkLimitExceeded)
	app.appV30AppliedHeight = 0
	selected, err := app.selectAppV30Proposal(txs, 20)
	require.NoError(t, err)
	require.Equal(t, txs, selected)
}

func TestAppV30TaintWorkLimitPreservesStateReadErrors(t *testing.T) {
	app := setupTestApp(t)
	app.appV30AppliedHeight = 19
	app.govEngine = nil
	txs := make([][]byte, 5)
	for i := range txs {
		txs[i] = makeAppV30WorkRawTx(t, uint64(i+1))
	}
	_, estimateErr := app.appV30BlockGeneratedWriteEstimate(txs, 20)
	_, selectionErr := app.selectAppV30Proposal(txs, 20)
	for _, err := range []error{estimateErr, selectionErr} {
		require.ErrorContains(t, err, "engine is unavailable")
		require.NotErrorIs(t, err, errAppV30TaintWorkLimitExceeded)
		require.NotErrorIs(t, err, errAppV30GeneratedWriteBudgetExceeded)
	}
}
