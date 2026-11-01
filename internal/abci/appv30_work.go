package abci

import (
	"fmt"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV30TaintWorkStateCode   uint32 = 113
	appV30TaintWorkLimitCode   uint32 = 114
	maxAppV30TaintWorkPerBlock        = 4
)

func (app *SageApp) processMemoryTaintWork(
	parsedTx *tx.ParsedTx,
	height int64,
) *abcitypes.ExecTxResult {
	if !app.postAppV30Fork(height) {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	work := parsedTx.MemoryTaintWork
	if work == nil {
		return &abcitypes.ExecTxResult{Code: appV30TaintWorkStateCode, Log: "missing memory taint work payload"}
	}
	if int(app.appV30WorkTxCount) >= maxAppV30TaintWorkPerBlock {
		return &abcitypes.ExecTxResult{
			Code: appV30TaintWorkLimitCode,
			Log:  "app-v30 taint work limit: at most four expand or cleanup transactions per block",
		}
	}
	credentialID, err := app.verifyCurrentAgentIdentity(parsedTx, height)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 110, Log: "taint work: agent identity verification failed"}
	}
	if _, enrollment, _, actorErr := app.appV23Actor(credentialID); actorErr != nil || enrollment == nil {
		return appV23ControlDenied()
	}
	header, found, err := app.badgerStore.GetTaintDecision(work.DecisionID)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 12, Log: "taint work state read failed"}
	}
	if !found {
		return &abcitypes.ExecTxResult{Code: appV30TaintWorkStateCode, Log: "taint decision does not exist"}
	}

	switch work.Kind {
	case tx.MemoryTaintWorkExpand:
		if header.Status == taint.StatusComplete || header.Status == taint.StatusWorkExhausted {
			return &abcitypes.ExecTxResult{
				Code: 0,
				Log:  fmt.Sprintf("taint decision %s has no expandable work", work.DecisionID),
			}
		}
		if header.Status != taint.StatusExpanding &&
			header.Status != taint.StatusDepthExhausted {
			return &abcitypes.ExecTxResult{
				Code: appV30TaintWorkStateCode,
				Log:  "taint decision is not expanding",
			}
		}
		if work.Generation != header.Generation ||
			work.FrontierHead != header.FrontierHead ||
			work.ParentRefID != header.ParentRefID ||
			work.Cursor != header.LastEdge {
			return &abcitypes.ExecTxResult{
				Code: appV30TaintWorkStateCode,
				Log:  "taint expansion cursor is stale",
			}
		}
		localChainID := app.expectedConsensusChainIDValue()
		if localChainID == "" {
			return &abcitypes.ExecTxResult{
				Code: appV30TaintWorkStateCode,
				Log:  "local consensus chain ID is unavailable",
			}
		}
		result, err := app.badgerStore.ExpandTaintDecision(
			localChainID,
			work.DecisionID,
			taint.PerExecutionUnitBudget,
		)
		if err != nil {
			return &abcitypes.ExecTxResult{Code: 12, Log: "taint expansion failed: " + err.Error()}
		}
		app.appV30WorkTxCount++
		return &abcitypes.ExecTxResult{
			Code: 0,
			Log: fmt.Sprintf(
				"taint expansion advanced: status=%d scanned=%d materialized=%d units=%d",
				result.Status,
				result.Scanned,
				result.Materialized,
				result.Units,
			),
		}

	case tx.MemoryTaintWorkCleanup:
		if header.Status == taint.StatusClean {
			return &abcitypes.ExecTxResult{
				Code: 0,
				Log:  fmt.Sprintf("taint decision %s is clean", work.DecisionID),
			}
		}
		if header.Status != taint.StatusCleaning {
			return &abcitypes.ExecTxResult{
				Code: appV30TaintWorkStateCode,
				Log:  "taint decision is not cleaning",
			}
		}
		if work.Generation != header.Generation ||
			work.CleanupPhase != uint8(header.CleanupPhase) ||
			work.Cursor != header.LastEdge {
			return &abcitypes.ExecTxResult{
				Code: appV30TaintWorkStateCode,
				Log:  "taint cleanup cursor is stale",
			}
		}
		result, err := app.badgerStore.CleanupTaintDecision(
			work.DecisionID,
			taint.PerExecutionUnitBudget,
		)
		if err != nil {
			return &abcitypes.ExecTxResult{Code: 12, Log: "taint cleanup failed: " + err.Error()}
		}
		app.appV30WorkTxCount++
		return &abcitypes.ExecTxResult{
			Code: 0,
			Log: fmt.Sprintf(
				"taint cleanup advanced: phase=%d members=%d queue=%d units=%d",
				result.Phase,
				result.MembersDeleted,
				result.QueueDeleted,
				result.Units,
			),
		}

	default:
		return &abcitypes.ExecTxResult{Code: appV30TaintWorkStateCode, Log: "unsupported taint work kind"}
	}
}
