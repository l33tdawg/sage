package abci

import (
	"crypto/sha256"
	"fmt"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// checkTxCommittedMemoryVote is advisory admission, not a new execution rule.
// CheckTx holds runtimeViewMu over these reads, so the envelope and submission
// height belong to one committed application view. FinalizeBlock already
// rejects these votes under app-v25 without consuming their nonce.
//
// A plain code 13 cannot resolve an indeterminate broadcast: it also covers
// missing targets, store failures and other mutable eligibility. This tagged
// subset proves that the exact signed vote cannot succeed in a later block.
// Only post-v25 submissions qualify; legacy adoption/hash repair must not be
// mistaken for a terminal ballot. Normal submissions replay without resetting
// status, adoption refuses a complete existing envelope, hash reanchor retains
// status, and challenge/reinstate/co-commit paths never reset it to proposed.
func (app *SageApp) checkTxCommittedMemoryVote(parsed *tx.ParsedTx) *abcitypes.ResponseCheckTx {
	if parsed.Type != tx.TxTypeMemoryVote || parsed.MemoryVote == nil ||
		app.state == nil || !app.postAppV25Rules(app.state.Height) {
		return nil
	}
	id := parsed.MemoryVote.MemoryID
	if id == "" {
		return nil
	}
	submitted, found, err := app.badgerStore.GetMemorySubmissionHeight(id)
	if err != nil || !found || submitted <= app.appV25AppliedHeight || submitted > app.state.Height {
		return nil
	}
	envelope, err := app.badgerStore.GetMemoryDisclosureState(id)
	if err != nil || envelope.Status != string(memory.StatusCommitted) ||
		len(envelope.ContentHash) != sha256.Size || !envelope.AuthorRecorded ||
		!envelope.DomainRecorded || !envelope.ClassificationRecorded ||
		envelope.Author == "" || envelope.Domain == "" {
		return nil
	}
	return &abcitypes.ResponseCheckTx{
		Code:      tx.CheckTxCommittedMemoryVoteCode,
		Codespace: tx.CheckTxCommittedMemoryVoteCodespace,
		Log:       fmt.Sprintf("vote rejected: canonical memory %s is committed, not proposed", id),
	}
}
