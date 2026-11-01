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

func makeMemoryTaintWorkTx(
	t *testing.T,
	root agentKey,
	work *tx.MemoryTaintWork,
	nonce uint64,
) *tx.ParsedTx {
	t.Helper()
	pubKey, sig, bodyHash, timestamp := signAgentProof(t, root, []byte("taint work"))
	parsed := &tx.ParsedTx{
		Type:            tx.TxTypeMemoryTaintWork,
		MemoryTaintWork: work,
		Nonce:           nonce,
		AgentPubKey:     pubKey,
		AgentSig:        sig,
		AgentBodyHash:   bodyHash,
		AgentTimestamp:  timestamp,
	}
	require.NoError(t, tx.SignTx(parsed, root.priv))
	return parsed
}

func TestMemoryTaintWorkGatesAndAdvancesExpansion(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	decisionID := strings.Repeat("e", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("taint work payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkExpand,
		DecisionID:   decisionID,
		Generation:   header.Generation,
		FrontierHead: header.FrontierHead,
		ParentRefID:  header.ParentRefID,
		Cursor:       header.LastEdge,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)

	app.appV30AppliedHeight = 30
	gated := app.processMemoryTaintWork(parsed, 20)
	require.Equal(t, uint32(10), gated.Code)
	app.appV30AppliedHeight = 19

	stale := *work
	stale.FrontierHead = 99
	staleResult := app.processMemoryTaintWork(
		makeMemoryTaintWorkTx(t, root, &stale, 2),
		20,
	)
	require.Equal(t, appV30TaintWorkStateCode, staleResult.Code)

	result := app.processMemoryTaintWork(parsed, 20)
	require.Equal(t, uint32(0), result.Code, result.Log)
	require.Equal(t, uint8(1), app.appV30WorkTxCount)
	header, found, err = app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusComplete, header.Status)
	require.Equal(t, uint64(1), header.FrontierHead)

	app.appV30WorkTxCount = maxAppV30TaintWorkPerBlock
	limited := app.processMemoryTaintWork(
		makeMemoryTaintWorkTx(t, root, work, 3),
		20,
	)
	require.Equal(t, appV30TaintWorkLimitCode, limited.Code)
}

func TestMemoryTaintWorkRunsInsideMeteredAppV30Stage(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	app.state.Height = 19
	decisionID := strings.Repeat("c", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("metered taint work payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkExpand,
		DecisionID:   decisionID,
		Generation:   1,
		FrontierHead: 0,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	working := app.cloneForAppV20Finalize(scoped)
	result, err := working.processAppV30MeteredTx(
		parsed,
		20,
		time.Unix(parsed.AgentTimestamp, 0).UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, uint32(0), result.Code, result.Log)
	require.NoError(t, scoped.CommitConsensusTransaction())

	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusComplete, header.Status)
	require.Equal(t, uint64(1), header.FrontierHead)
	require.EqualValues(t, 1, working.appV30WorkTxCount)
	require.Zero(t, working.badgerStore.AppV30WriteStageStats())
}

func TestMemoryTaintWorkAdvancesCleanup(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	decisionID := strings.Repeat("f", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("cleanup work payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	changed, err := app.badgerStore.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkCleanup,
		DecisionID:   decisionID,
		Generation:   header.Generation,
		CleanupPhase: uint8(header.CleanupPhase),
		Cursor:       header.LastEdge,
	}
	result := app.processMemoryTaintWork(
		makeMemoryTaintWorkTx(t, root, work, 1),
		20,
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
	header, found, err = app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.CleanupQueue, header.CleanupPhase)
	decisions, err := app.badgerStore.GetEffectiveTaintDecisions(target.MemoryID)
	require.NoError(t, err)
	require.Empty(t, decisions)
}

func TestMemoryTaintWorkCheckTxRejectsBeforeAppV30(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	_, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("check tx work payload")),
	)
	require.NoError(t, err)
	work := &tx.MemoryTaintWork{
		Version:    tx.MemoryTaintWorkVersion,
		Kind:       tx.MemoryTaintWorkExpand,
		DecisionID: decisionID,
		Generation: 1,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)

	app.state.Height = 19
	app.appV30AppliedHeight = 30
	response, err := app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
	require.NoError(t, err)
	require.Equal(t, uint32(10), response.Code)
	require.Contains(t, response.Log, "unknown tx type")
}
