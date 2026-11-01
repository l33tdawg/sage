package abci

import (
	"fmt"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV30BlockWriteBudgetBytes  int64 = 4 << 20
	appV30MembershipTripleBytes  int64 = 1_784
	appV30HeaderMaxBytes         int64 = 1_172
	appV30ScopeStateMaxBytes     int64 = 2_206
	appV30ActiveIndexBytes       int64 = 80
	appV30TombstoneBytes         int64 = 20
	appV30GeneratedOverheadBytes int64 = 4 << 10
	appV30SubmitOverheadBytes    int64 = 13_191
	appV30MaxCleanupMemberPairs        = 340
)

const (
	appV30GeneratedWriteBudgetCode uint32 = 115
	appV30GeneratedWriteBudgetLog         = "app-v30 block exceeds the generated-write budget"
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
		// Scope matching and parent inheritance can independently materialize up
		// to one admission membership for every active decision.
		estimate += appV30AdmissionMembershipEstimate(childBytes)
		if len(parsed.MemorySubmit.DerivedFrom) > 0 {
			for _, ref := range parsed.MemorySubmit.DerivedFrom {
				chainBytes := int64(len(ref.SourceChainID))
				memoryBytes := int64(len(ref.MemoryID))
				forwardKey := int64(len("lineage:")) + childBytes + 1 + 64
				forwardValue := 2 + chainBytes + 2 + memoryBytes + 32
				reverseKey := int64(len("lineage-ref:")) + 64 + 1 + childBytes
				estimate += forwardKey + forwardValue + reverseKey
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
		childBytes := int64(len(parsed.CoCommitSubmit.SharedID))
		return appV30GeneratedOverheadBytes +
			int64(len("memprov:v30:")) +
			childBytes +
			1 + signers*appV30ProvenanceSignerBytes +
			appV30AdmissionMembershipEstimate(childBytes)
	}
	return 0
}

const appV30ProvenanceSignerBytes int64 = 64 + 64 + 8 + 1

func appV30AdmissionMembershipEstimate(childBytes int64) int64 {
	var estimate int64
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		membershipKey := int64(len("taintm:")) + childBytes + 1 + 64
		indexKey := int64(len("taintd:")) + 64 + 1 + childBytes
		estimate += membershipKey + 11 + indexKey
	}
	return estimate
}

func appV30TaintExecutionEstimate(payload []byte) int64 {
	if len(payload) < 2 {
		return 0
	}
	switch payload[1] {
	case tx.TaintActionApply:
		return appV30GeneratedOverheadBytes +
			int64(taint.MaxNewMembershipsPerExec)*appV30MembershipTripleBytes +
			appV30HeaderMaxBytes + appV30ScopeStateMaxBytes + appV30ActiveIndexBytes
	case tx.TaintActionClear:
		return appV30GeneratedOverheadBytes +
			int64(appV30MaxCleanupMemberPairs)*appV30MembershipTripleBytes +
			appV30HeaderMaxBytes + appV30ActiveIndexBytes + appV30TombstoneBytes
	default:
		return 0
	}
}

// appV30ActiveGovernanceReserve bounds the one governance execution that can
// occur after this block's transactions. An op-12 proposal created in this
// block contributes the same reservation through its proposal tx; an existing
// active proposal has no transaction in the block, so it must be added here.
func (app *SageApp) appV30ActiveGovernanceReserve(height int64) (int64, error) {
	if !app.postAppV30Fork(height) {
		return 0, nil
	}
	active, err := app.govEngine.GetActiveProposal()
	if err != nil {
		return 0, fmt.Errorf("read active app-v30 governance proposal: %w", err)
	}
	if active == nil ||
		active.Operation != governance.OpMemoryTaint ||
		active.CreatedHeight <= app.appV30AppliedHeight ||
		height > active.ExpiryHeight {
		return 0, nil
	}
	if height < active.CreatedHeight+governance.MinVotingBlocks &&
		app.validators.Size() > 1 {
		return 0, nil
	}
	return appV30TaintExecutionEstimate(active.Payload), nil
}

func (app *SageApp) appV30BlockGeneratedWriteEstimate(txs [][]byte, height int64) (int64, error) {
	if !app.postAppV30Fork(height) {
		return 0, nil
	}
	total, err := app.appV30ActiveGovernanceReserve(height)
	if err != nil {
		return 0, err
	}
	if total > appV30BlockWriteBudgetBytes {
		return 0, fmt.Errorf("app-v30 generated writes exceed %d bytes", appV30BlockWriteBudgetBytes)
	}
	for _, rawTx := range txs {
		estimate := app.appV30EstimatedTxBytes(rawTx, height)
		if estimate > appV30BlockWriteBudgetBytes-total {
			return 0, fmt.Errorf("app-v30 generated writes exceed %d bytes", appV30BlockWriteBudgetBytes)
		}
		total += estimate
	}
	return total, nil
}

func (app *SageApp) appV30EstimatedTxBytes(rawTx []byte, height int64) int64 {
	if !app.postAppV30Fork(height) {
		return 0
	}
	parsed, err := tx.DecodeTx(rawTx)
	if err != nil {
		return 0
	}
	if parsed.Type == tx.TxTypeMemorySubmit {
		if err := tx.ActivateMemorySubmitExtensions(parsed, true, true); err != nil {
			return 0
		}
	}
	return appV30GeneratedWriteEstimate(parsed)
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
	stageCommitted := false
	defer func() {
		if !stageCommitted {
			app.pendingWrites = app.pendingWrites[:pendingWriteCount]
		}
	}()

	result := app.processTx(parsedTx, height, blockTime)
	overflowed := app.badgerStore.AppV30WriteStageOverflowed()
	if result.Code != 0 || overflowed {
		if err := app.badgerStore.AbortAppV30WriteStage(); err != nil {
			return nil, fmt.Errorf("rollback app-v30 transaction: %w", err)
		}
		if overflowed {
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
	selected := make([][]byte, 0, len(txs))
	total, err := app.appV30ActiveGovernanceReserve(height)
	if err != nil {
		return nil, err
	}
	if total > appV30BlockWriteBudgetBytes {
		return nil, fmt.Errorf("app-v30 generated writes exceed %d bytes", appV30BlockWriteBudgetBytes)
	}
	for _, rawTx := range txs {
		estimate := app.appV30EstimatedTxBytes(rawTx, height)
		if estimate > appV30BlockWriteBudgetBytes-total {
			continue
		}
		selected = append(selected, rawTx)
		total += estimate
	}
	return selected, nil
}
