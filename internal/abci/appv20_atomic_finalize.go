package abci

import (
	cmttypes "github.com/cometbft/cometbft/types"
)

const (
	// Every strict post-app-v20 block commits as one Badger transaction. These
	// consensus limits bound both transaction count and raw input retained while
	// the block is evaluated. The raw-byte ceiling is intentionally independent
	// of CometBFT's protobuf MaxTxBytes limit; both must be satisfied.
	maxAppV20AtomicFinalizeEntries = 64
	maxAppV20AtomicFinalizeTxBytes = 1 << 20
)

const appV20AtomicFinalizeBudgetCode uint32 = 110

const appV20AtomicFinalizeBudgetLog = "app-v20 block exceeds the atomic-finalize budget"

// appV20AtomicFinalizeBudget is the FinalizeBlock backstop for a proposal from
// a Byzantine proposer. A block that exceeds either limit rejects every
// transaction before signature, proof, nonce, or handler state can mutate.
func appV20AtomicFinalizeBudget(txs [][]byte) bool {
	if len(txs) > maxAppV20AtomicFinalizeEntries {
		return false
	}
	totalRawBytes := 0
	for _, rawTx := range txs {
		totalRawBytes += len(rawTx)
		if totalRawBytes > maxAppV20AtomicFinalizeTxBytes {
			return false
		}
	}
	return true
}

// prepareAppV20Proposal preserves mempool order while applying the global
// app-v20 atomic-transaction budget and CometBFT's protobuf byte budget. Mixed
// governance and ordinary blocks are safe because the whole block is one
// speculative transaction until Commit.
func prepareAppV20Proposal(txs [][]byte, maxTxBytes int64) [][]byte {
	if maxTxBytes <= 0 || len(txs) == 0 {
		return nil
	}
	selected := make([][]byte, 0, len(txs))
	var protoBytes int64
	totalRawBytes := 0
	for _, rawTx := range txs {
		if len(selected) >= maxAppV20AtomicFinalizeEntries ||
			totalRawBytes+len(rawTx) > maxAppV20AtomicFinalizeTxBytes {
			break
		}
		txBytes := cmttypes.ComputeProtoSizeForTxs([]cmttypes.Tx{cmttypes.Tx(rawTx)})
		if protoBytes+txBytes > maxTxBytes {
			break
		}
		selected = append(selected, rawTx)
		protoBytes += txBytes
		totalRawBytes += len(rawTx)
	}
	return selected
}

// prepareProposalExcludingIndexes byte-packs the legacy fallback after one or
// more authenticated bootstrap transactions proved individually unable to fit
// RequestPrepareProposal.MaxTxBytes. Excluded entries remain in the mempool;
// selected entries preserve their relative order. Continue past an ordinary
// entry that does not fit so one oversized candidate cannot starve a smaller
// follower in this defensive path.
func prepareProposalExcludingIndexes(txs [][]byte, excluded map[int]struct{}, maxTxBytes int64) [][]byte {
	if maxTxBytes <= 0 || len(txs) == 0 {
		return nil
	}
	selected := make([][]byte, 0, len(txs))
	var protoBytes int64
	for i, rawTx := range txs {
		if _, skip := excluded[i]; skip {
			continue
		}
		txBytes := cmttypes.ComputeProtoSizeForTxs([]cmttypes.Tx{cmttypes.Tx(rawTx)})
		if protoBytes+txBytes > maxTxBytes {
			continue
		}
		selected = append(selected, rawTx)
		protoBytes += txBytes
	}
	return selected
}

// validateAppV20Proposal mirrors FinalizeBlock's global atomic budget for
// proposals received from peers. Transaction semantics are evaluated later;
// this check is deliberately independent of transaction family.
func validateAppV20Proposal(txs [][]byte) bool {
	return appV20AtomicFinalizeBudget(txs)
}
