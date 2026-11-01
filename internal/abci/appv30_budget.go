package abci

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV30BlockWriteBudgetBytes  int64 = 4 << 20
	appV30MembershipTripleBytes  int64 = 1_784
	appV30HeaderMaxBytes         int64 = 1_172
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
				membershipKey := int64(len("taintm:")) + childBytes + 1 + 64
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
		payload := parsed.GovPropose.Payload
		if len(payload) < 2 {
			return 0
		}
		switch payload[1] {
		case tx.TaintActionApply:
			return appV30GeneratedOverheadBytes +
				int64(taint.MaxNewMembershipsPerExec)*appV30MembershipTripleBytes +
				appV30HeaderMaxBytes
		case tx.TaintActionClear:
			return appV30GeneratedOverheadBytes +
				int64(appV30MaxCleanupMemberPairs)*appV30MembershipTripleBytes +
				appV30HeaderMaxBytes
		default:
			return 0
		}

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

func (app *SageApp) appV30BlockGeneratedWriteEstimate(txs [][]byte, height int64) (int64, error) {
	if !app.postAppV30Fork(height) {
		return 0, nil
	}
	var total int64
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

func (app *SageApp) selectAppV30Proposal(txs [][]byte, height int64) [][]byte {
	if !app.postAppV30Fork(height) {
		return txs
	}
	selected := make([][]byte, 0, len(txs))
	var total int64
	for _, rawTx := range txs {
		estimate := app.appV30EstimatedTxBytes(rawTx, height)
		if estimate > appV30BlockWriteBudgetBytes-total {
			continue
		}
		selected = append(selected, rawTx)
		total += estimate
	}
	return selected
}
