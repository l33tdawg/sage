package abci

import (
	"errors"
	"fmt"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV30BlockWriteBudgetBytes  int64 = 4 << 20
	appV30MembershipTripleBytes  int64 = 1_852
	appV30HeaderMaxBytes         int64 = 1_172
	appV30TargetAnchorBytes      int64 = 721
	appV30GeneratedOverheadBytes int64 = 4 << 10
	appV30SubmitOverheadBytes    int64 = 13_191
	appV30MaxCleanupMemberPairs        = 340
	// One carried taint proposal can execute after the current transactions.
	// Clear is the larger of the corrected Apply/Clear allowances.
	appV30PendingGovernanceBytes = appV30GeneratedOverheadBytes +
		int64(appV30MaxCleanupMemberPairs)*appV30MembershipTripleBytes +
		appV30HeaderMaxBytes
)

var (
	errAppV30GeneratedWriteBudgetExceeded = errors.New("app-v30 generated-write budget exceeded")
	errAppV30TaintWorkLimitExceeded       = errors.New(appV30TaintWorkLimitLog)
)

const (
	appV30GeneratedWriteBudgetCode uint32 = 115
	appV30GeneratedWriteBudgetLog         = "app-v30 block exceeds the generated-write budget"
	appV30TaintWorkLimitLog               = "app-v30 taint work limit: at most four expand or cleanup transactions per block"
)

func appV30GeneratedWriteEstimate(parsed *tx.ParsedTx) int64 {
	if parsed == nil {
		return 0
	}
	switch parsed.Type {
	case tx.TxTypeMemorySubmit:
		if parsed.MemorySubmit == nil {
			return 0
		}
		childBytes := int64(len(parsed.MemorySubmit.MemoryID))
		estimate := appV30SubmitOverheadBytes
		// One app-v30 signer marker: key + count + fixed signer tuple.
		estimate += int64(len("memprov:v30:")) + childBytes + 1 + appV30ProvenanceSignerBytes
		if len(parsed.MemorySubmit.DerivedFrom) > 0 {
			for _, ref := range parsed.MemorySubmit.DerivedFrom {
				chainBytes := int64(len(ref.SourceChainID))
				memoryBytes := int64(len(ref.MemoryID))
				forwardKey := int64(len("lineage:")) + childBytes + 1 + 64
				forwardValue := 2 + chainBytes + 2 + memoryBytes + 32
				reverseKey := int64(len("lineage-ref:")) + 64 + 1 + childBytes
				estimate += forwardKey + forwardValue + reverseKey
			}
			// Admission inheritance is bounded independently at 32 decisions.
			for i := 0; i < taint.MaxActiveDecisions; i++ {
				membershipKey := int64(len("taintm:")) + 2 + childBytes + 1 + 64
				indexKey := int64(len("taintd:")) + 64 + 1 + childBytes
				estimate += membershipKey + 11 + indexKey
			}
		}
		return estimate

	case tx.TxTypeGovPropose:
		if parsed.GovPropose == nil ||
			tx.GovProposalOp(parsed.GovPropose.Operation) != tx.GovOpMemoryTaint {
			return 0
		}
		return appV30TaintExecutionEstimate(parsed.GovPropose.Payload)

	case tx.TxTypeMemoryTaintWork:
		if parsed.MemoryTaintWork == nil {
			return 0
		}
		switch parsed.MemoryTaintWork.Kind {
		case tx.MemoryTaintWorkExpand:
			return appV30GeneratedOverheadBytes +
				int64(taint.MaxNewMembershipsPerExec)*appV30MembershipTripleBytes +
				appV30HeaderMaxBytes
		case tx.MemoryTaintWorkCleanup:
			return appV30GeneratedOverheadBytes +
				int64(appV30MaxCleanupMemberPairs)*appV30MembershipTripleBytes +
				appV30HeaderMaxBytes
		}

	case tx.TxTypeCoCommitSubmit:
		if parsed.CoCommitSubmit == nil {
			return 0
		}
		signers := int64(len(parsed.CoCommitSubmit.Coauthors))
		return appV30GeneratedOverheadBytes +
			int64(len("memprov:v30:")) +
			int64(len(parsed.CoCommitSubmit.SharedID)) +
			1 + signers*appV30ProvenanceSignerBytes
	}
	return 0
}

const appV30ProvenanceSignerBytes int64 = 64 + 64 + 8 + 1

func appV30TaintExecutionEstimate(payload []byte) int64 {
	if len(payload) < 2 {
		return 0
	}
	switch payload[1] {
	case tx.TaintActionApply:
		return appV30GeneratedOverheadBytes +
			int64(taint.MaxNewMembershipsPerExec)*appV30MembershipTripleBytes +
			appV30HeaderMaxBytes + appV30TargetAnchorBytes
	case tx.TaintActionClear:
		return appV30GeneratedOverheadBytes +
			int64(appV30MaxCleanupMemberPairs)*appV30MembershipTripleBytes +
			appV30HeaderMaxBytes
	default:
		return 0
	}
}

// appV30PendingGovernanceReservation reads only the canonical pre-block view.
// It does not forecast votes/status or execute governance while planning.
func (app *SageApp) appV30PendingGovernanceReservation(height int64) (int64, error) {
	if !app.postAppV30Fork(height) {
		return 0, nil
	}
	if app.govEngine == nil {
		return 0, fmt.Errorf("app-v30 pending governance: engine is unavailable")
	}
	proposal, err := app.govEngine.GetActiveProposal()
	if err != nil {
		return 0, fmt.Errorf("app-v30 pending governance: %w", err)
	}
	if proposal == nil || proposal.Operation != governance.OpMemoryTaint ||
		proposal.CreatedHeight <= app.appV30AppliedHeight {
		return 0, nil
	}
	return appV30PendingGovernanceBytes, nil
}

func (app *SageApp) appV30BlockGeneratedWriteEstimate(txs [][]byte, height int64) (int64, error) {
	if !app.postAppV30Fork(height) {
		return 0, nil
	}
	total, err := app.appV30PendingGovernanceReservation(height)
	if err != nil {
		return 0, err
	}
	workCount := 0
	for _, rawTx := range txs {
		estimate, isWork := app.appV30EstimatedTxResources(rawTx, height)
		if isWork {
			workCount++
			if workCount > maxAppV30TaintWorkPerBlock {
				return 0, errAppV30TaintWorkLimitExceeded
			}
		}
		if estimate > appV30BlockWriteBudgetBytes-total {
			return 0, fmt.Errorf("%w: limit %d bytes", errAppV30GeneratedWriteBudgetExceeded, appV30BlockWriteBudgetBytes)
		}
		total += estimate
	}
	return total, nil
}

func (app *SageApp) appV30EstimatedTxBytes(rawTx []byte, height int64) int64 {
	estimate, _ := app.appV30EstimatedTxResources(rawTx, height)
	return estimate
}

// appV30EstimatedTxResources counts formed work payloads independently of
// current decision status. Completed no-ops still occupy a proposal work slot.
func (app *SageApp) appV30EstimatedTxResources(rawTx []byte, height int64) (int64, bool) {
	if !app.postAppV30Fork(height) {
		return 0, false
	}
	parsed, err := tx.DecodeTx(rawTx)
	if err != nil {
		return 0, false
	}
	if parsed.Type == tx.TxTypeMemorySubmit {
		if err := tx.ActivateMemorySubmitExtensions(parsed, true, true); err != nil {
			return 0, false
		}
	}
	isWork := parsed.Type == tx.TxTypeMemoryTaintWork && parsed.MemoryTaintWork != nil &&
		(parsed.MemoryTaintWork.Kind == tx.MemoryTaintWorkExpand ||
			parsed.MemoryTaintWork.Kind == tx.MemoryTaintWorkCleanup)
	return appV30GeneratedWriteEstimate(parsed), isWork
}

// processAppV30MeteredTx gives each app-v30 state-generating transaction its
// own rollback-safe reservation inside the enclosing block transaction. The
// deterministic estimate still decides proposal admission; this boundary
// enforces the same limit against actual key/value and tombstone bytes.
func (app *SageApp) processAppV30MeteredTx(
	parsedTx *tx.ParsedTx,
	height int64,
	blockTime time.Time,
) (*abcitypes.ExecTxResult, error) {
	estimate := appV30GeneratedWriteEstimate(parsedTx)
	if !app.postAppV30Fork(height) || estimate == 0 {
		return app.processTx(parsedTx, height, blockTime), nil
	}
	if err := app.badgerStore.BeginAppV30WriteStage(estimate); err != nil {
		return nil, fmt.Errorf("begin app-v30 write reservation: %w", err)
	}
	pendingWriteCount := len(app.pendingWrites)
	var restoreSubmitCache func()
	if parsedTx.Type == tx.TxTypeMemorySubmit && parsedTx.MemorySubmit != nil {
		memoryID := memoryIDForSubmit(parsedTx.MemorySubmit, height,
			auth.PublicKeyToAgentID(parsedTx.AgentPubKey))
		restoreSubmitCache = app.SuppCache.checkpointFinalizeItem(memoryID)
	}
	stageCommitted := false
	defer func() {
		if !stageCommitted {
			app.pendingWrites = app.pendingWrites[:pendingWriteCount]
			if restoreSubmitCache != nil {
				restoreSubmitCache()
			}
		}
	}()

	result := app.processTx(parsedTx, height, blockTime)
	overflowed := app.badgerStore.AppV30WriteStageOverflowed()
	poison := app.badgerStore.ConsensusTransactionError()
	reservationExceeded := errors.Is(poison, store.ErrAppV30WriteReservationExceeded)
	if result.Code != 0 || overflowed || poison != nil {
		if err := app.badgerStore.AbortAppV30WriteStage(); err != nil {
			return nil, fmt.Errorf("rollback app-v30 transaction: %w", err)
		}
		// Abort restores the pre-stage poison snapshot. Genuine failed writes or
		// partial-update errors must still abort the enclosing block, as they did
		// before per-transaction staging. Only our typed reservation error may be
		// recovered as an ordinary transaction rejection.
		if poison != nil && !reservationExceeded {
			return nil, fmt.Errorf("app-v30 transaction storage failure: %w", poison)
		}
		if overflowed || reservationExceeded {
			return &abcitypes.ExecTxResult{
				Code: appV30GeneratedWriteBudgetCode,
				Log:  appV30GeneratedWriteBudgetLog,
			}, nil
		}
		return result, nil
	}
	if _, err := app.badgerStore.CommitAppV30WriteStage(); err != nil {
		if abortErr := app.badgerStore.AbortAppV30WriteStage(); abortErr != nil {
			return nil, fmt.Errorf(
				"abort over-budget app-v30 transaction after reservation failure: %w",
				abortErr,
			)
		}
		if app.badgerStore.AppV30WriteStageOverflowed() {
			return &abcitypes.ExecTxResult{
				Code: appV30GeneratedWriteBudgetCode,
				Log:  appV30GeneratedWriteBudgetLog,
			}, nil
		}
		return nil, fmt.Errorf("finish app-v30 write reservation: %w", err)
	}
	stageCommitted = true
	return result, nil
}

func (app *SageApp) selectAppV30Proposal(txs [][]byte, height int64) ([][]byte, error) {
	if !app.postAppV30Fork(height) {
		return txs, nil
	}
	total, err := app.appV30PendingGovernanceReservation(height)
	if err != nil {
		return nil, err
	}
	selected := make([][]byte, 0, len(txs))
	workCount := 0
	for _, rawTx := range txs {
		estimate, isWork := app.appV30EstimatedTxResources(rawTx, height)
		if isWork && workCount >= maxAppV30TaintWorkPerBlock {
			continue
		}
		if estimate > appV30BlockWriteBudgetBytes-total {
			continue
		}
		selected = append(selected, rawTx)
		total += estimate
		if isWork {
			workCount++
		}
	}
	return selected, nil
}
