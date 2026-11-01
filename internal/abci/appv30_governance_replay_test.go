package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30HistoricalOp12RetainsLegacyFinalizePath(t *testing.T) {
	for _, tc := range []struct {
		name        string
		appV20      int64
		appV30      int64
		strictApply bool
	}{
		{name: "legacy"},
		{name: "v30 dormant", appV20: 3, strictApply: true},
		{name: "v30 activation block", appV20: 1, appV30: 4, strictApply: true},
		{name: "historical proposal after v30", appV20: 1, appV30: 3, strictApply: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, admin := setupGovTestApp(t)
			// Before app-v20, an unrecognized operation was admitted, received the
			// ordinary proposer vote, and remained inert at application. Empty
			// payloads and a validator target were legal for that historical op.
			result := app.processGovPropose(&tx.ParsedTx{
				PublicKey: admin.pub,
				GovPropose: &tx.GovPropose{
					Operation: tx.GovOpMemoryTaint,
					TargetID:  admin.id,
					Reason:    "historical unknown operation",
				},
			}, 2, time.Unix(2, 0))
			require.Zero(t, result.Code, result.Log)
			proposalID := governance.ComputeProposalID(admin.id, 2, governance.OpMemoryTaint, admin.id)
			app.appV20AppliedHeight = tc.appV20
			app.appV30AppliedHeight = tc.appV30

			response, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
				Height: 4,
				Time:   time.Unix(4, 0),
			})
			if tc.strictApply {
				// Preserve the preexisting strict-v20 application failure, rather
				// than reinterpreting this proposal through the v30 validator.
				require.ErrorContains(t, err, "unknown governance operation: 12")
				require.NotContains(t, err.Error(), "execution validation failed")
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.Empty(t, response.ValidatorUpdates)
				_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
				require.NoError(t, err)
			}
			proposal, err := app.govEngine.LoadProposal(proposalID)
			require.NoError(t, err)
			if tc.strictApply {
				require.Equal(t, governance.StatusVoting, proposal.Status, "strict execution remains atomic")
			} else {
				require.Equal(t, governance.StatusExecuted, proposal.Status)
			}
			count, err := app.badgerStore.ActiveTaintDecisionCount()
			require.NoError(t, err)
			require.Zero(t, count, "historical op12 must never create a taint decision")
		})
	}
}
