package abci

import (
	"context"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

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
	require.EqualValues(t, 369_204, perTx)
	require.LessOrEqual(t, perTx*11, appV30BlockWriteBudgetBytes)
	require.Greater(t, perTx*12, appV30BlockWriteBudgetBytes)

	prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
		Height:     20,
		Time:       time.Unix(1_700_000_020, 0).UTC(),
		Txs:        txs,
		MaxTxBytes: 2_000_000,
	})
	require.NoError(t, err)
	require.Len(t, prepared.Txs, 11)

	accepted, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
		Height: 20,
		Txs:    txs[:11],
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
	require.EqualValues(t, 80_157, estimate)
	require.LessOrEqual(t, estimate*52, appV30BlockWriteBudgetBytes)
	require.Greater(t, estimate*53, appV30BlockWriteBudgetBytes)
}
