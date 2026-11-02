package abci

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30GeneratedWriteAccountingMatchesCanonicalTaintSizes(t *testing.T) {
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	memoryID := strings.Repeat("m", taint.MaxMemoryIDBytes)
	membership, err := taint.EncodeMembership(taint.Membership{
		Generation: 1, Depth: 1, Origin: taint.OriginExpand,
	})
	require.NoError(t, err)
	queue, err := taint.EncodeQueueEntry(taint.QueueEntry{
		ParentRefID: strings.Repeat("b", taint.DecisionIDBytes),
		MemoryID:    memoryID,
		Depth:       1,
	})
	require.NoError(t, err)
	parts := []int{
		len(taint.MembershipKey(memoryID, decisionID)),
		len(membership),
		len(taint.DecisionIndexKey(decisionID, memoryID)),
		len(taint.FrontierKey(decisionID, 1, 0)),
		len(queue),
	}
	require.Equal(t, []int{586, 11, 584, 89, 582}, parts)
	var total int
	for _, size := range parts {
		total += size
	}
	require.EqualValues(t, total, appV30MembershipTripleBytes)

	// The frozen target-anchor format holds one version, two uint16 lengths,
	// the bounded chain/memory IDs, and the content/payload hashes.
	anchorValueBytes := 1 + 2 + tx.MaxDerivedFromChainIDBytes +
		2 + tx.MaxDerivedFromMemoryIDBytes + 2*sha256.Size
	require.Equal(t, 645, anchorValueBytes)
	require.EqualValues(t, len(taint.TargetKey(decisionID))+anchorValueBytes, appV30TargetAnchorBytes)
	proposalEstimate := func(action byte) int64 {
		return appV30GeneratedWriteEstimate(&tx.ParsedTx{
			Type: tx.TxTypeGovPropose,
			GovPropose: &tx.GovPropose{
				Operation: tx.GovOpMemoryTaint, Payload: []byte{1, action},
			},
		})
	}
	workEstimate := appV30GeneratedWriteEstimate(&tx.ParsedTx{
		Type: tx.TxTypeMemoryTaintWork,
		MemoryTaintWork: &tx.MemoryTaintWork{
			Kind: tx.MemoryTaintWorkExpand,
		},
	})
	require.EqualValues(t, 383_076, workEstimate)
	require.EqualValues(t, 383_797, proposalEstimate(tx.TaintActionApply))
	require.EqualValues(t, appV30TargetAnchorBytes, proposalEstimate(tx.TaintActionApply)-workEstimate)
	require.EqualValues(t, 634_948, proposalEstimate(tx.TaintActionClear))
	require.Equal(t, proposalEstimate(tx.TaintActionClear), appV30PendingGovernanceBytes)
	require.GreaterOrEqual(t, appV30PendingGovernanceBytes, proposalEstimate(tx.TaintActionApply))
}

func appV30BudgetPreflightApp(t *testing.T) *SageApp {
	t.Helper()
	app := setupTestApp(t)
	app.state.Height = 20
	app.appV20AppliedHeight = 5
	app.appV30AppliedHeight = 19
	return app
}

func appV30BudgetSetActiveProposal(t *testing.T, app *SageApp, proposal governance.ProposalState) []byte {
	t.Helper()
	encoded, err := json.Marshal(proposal)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetState("gov:proposal:"+proposal.ProposalID, encoded))
	require.NoError(t, app.badgerStore.SetState("gov:active", []byte(proposal.ProposalID)))
	return encoded
}

func TestAppV30PendingGovernanceReservationPrestate(t *testing.T) {
	for _, test := range []struct {
		name    string
		active  bool
		op      governance.ProposalOp
		created int64
		status  governance.ProposalStatus
		action  byte
		want    int64
	}{
		{name: "no_active"},
		{name: "other_operation", active: true, op: governance.OpAddValidator, created: 20},
		{name: "historical_before_activation", active: true, op: governance.OpMemoryTaint, created: 18},
		{name: "historical_at_activation", active: true, op: governance.OpMemoryTaint, created: 19},
		{name: "pending_apply", active: true, op: governance.OpMemoryTaint, created: 20, status: governance.StatusVoting, action: tx.TaintActionApply, want: 634_948},
		{name: "pending_clear", active: true, op: governance.OpMemoryTaint, created: 20, status: governance.StatusVoting, action: tx.TaintActionClear, want: 634_948},
		{name: "status_not_used_to_forecast", active: true, op: governance.OpMemoryTaint, created: 20, status: governance.StatusExecuted, action: tx.TaintActionApply, want: 634_948},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := appV30BudgetPreflightApp(t)
			if test.active {
				appV30BudgetSetActiveProposal(t, app, governance.ProposalState{
					ProposalID: "budget-prestate", Operation: test.op,
					CreatedHeight: test.created, ExpiryHeight: 100,
					Status: test.status, Payload: []byte{1, test.action},
				})
			}
			reservation, err := app.appV30PendingGovernanceReservation(21)
			require.NoError(t, err)
			require.Equal(t, test.want, reservation)
			empty, err := app.appV30BlockGeneratedWriteEstimate(nil, 21)
			require.NoError(t, err)
			require.Equal(t, test.want, empty)
			// This work transaction is unrelated to the retained proposal; no
			// current GovPropose is needed for the base reservation.
			work := makeAppV30WorkRawTx(t, 1)
			withWork, err := app.appV30BlockGeneratedWriteEstimate([][]byte{work}, 21)
			require.NoError(t, err)
			require.Equal(t, test.want+383_076, withWork)
		})
	}

	t.Run("prefork_does_not_read_governance", func(t *testing.T) {
		app := appV30BudgetPreflightApp(t)
		app.govEngine = governance.NewEngine(appV30BudgetReadErrorStore{
			GovStore: app.badgerStore, err: errors.New("must not read before activation"),
		}, nil)
		work := [][]byte{makeAppV30WorkRawTx(t, 1)}
		reservation, err := app.appV30PendingGovernanceReservation(19)
		require.NoError(t, err)
		require.Zero(t, reservation)
		estimate, err := app.appV30BlockGeneratedWriteEstimate(work, 19)
		require.NoError(t, err)
		require.Zero(t, estimate)
		selected, err := app.selectAppV30Proposal(work, 19)
		require.NoError(t, err)
		require.Equal(t, work, selected)
	})
}

func TestAppV30PendingGovernanceBudgetPreflight(t *testing.T) {
	for _, action := range []byte{tx.TaintActionApply, tx.TaintActionClear} {
		t.Run(map[byte]string{tx.TaintActionApply: "apply", tx.TaintActionClear: "clear"}[action], func(t *testing.T) {
			app := appV30BudgetPreflightApp(t)
			proposal := governance.ProposalState{
				ProposalID: "carried-budget", Operation: governance.OpMemoryTaint,
				CreatedHeight: 20, ExpiryHeight: 100, Status: governance.StatusVoting,
				Payload: []byte{1, action},
			}
			before := appV30BudgetSetActiveProposal(t, app, proposal)
			txs := make([][]byte, 10)
			for i := range txs {
				txs[i] = makeAppV30WorkRawTx(t, uint64(i+1))
			}
			fit, err := app.appV30BlockGeneratedWriteEstimate(txs[:9], 21)
			require.NoError(t, err)
			require.EqualValues(t, 634_948+9*383_076, fit)
			_, err = app.appV30BlockGeneratedWriteEstimate(txs, 21)
			require.ErrorIs(t, err, errAppV30GeneratedWriteBudgetExceeded)
			prepared, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
				Height: 21, Time: time.Unix(1_700_000_021, 0).UTC(), Txs: txs, MaxTxBytes: 2_000_000,
			})
			require.NoError(t, err)
			require.Equal(t, txs[:9], prepared.Txs)
			accepted, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
				Height: 21, Txs: txs[:9],
			})
			require.NoError(t, err)
			require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, accepted.Status)
			rejected, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
				Height: 21, Txs: txs,
			})
			require.NoError(t, err)
			require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, rejected.Status)
			finalized, err := app.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{
				Height: 21, Time: time.Unix(1_700_000_021, 0).UTC(), Txs: txs,
			})
			require.NoError(t, err)
			require.Len(t, finalized.TxResults, 10)
			for _, result := range finalized.TxResults {
				require.Equal(t, appV30GeneratedWriteBudgetCode, result.Code)
				require.Equal(t, appV30GeneratedWriteBudgetLog, result.Log)
			}
			after, err := app.badgerStore.GetState("gov:proposal:" + proposal.ProposalID)
			require.NoError(t, err)
			require.Equal(t, before, after, "over-budget preflight must not execute or invalidate the carried proposal")
			active, err := app.badgerStore.GetState("gov:active")
			require.NoError(t, err)
			require.Equal(t, []byte(proposal.ProposalID), active)
		})
	}
}

type appV30BudgetReadErrorStore struct {
	governance.GovStore
	err error
}

func (s appV30BudgetReadErrorStore) GetState(string) ([]byte, error) {
	return nil, s.err
}

func TestAppV30PendingGovernanceStateErrorsFailClosed(t *testing.T) {
	backendErr := errors.New("budget test canonical state read failed")
	for _, test := range []string{"backend_read", "malformed_json", "missing_proposal", "missing_engine"} {
		t.Run(test, func(t *testing.T) {
			app := appV30BudgetPreflightApp(t)
			require.NoError(t, app.badgerStore.SetState("gov:active", []byte("broken-budget")))
			switch test {
			case "backend_read":
				app.govEngine = governance.NewEngine(appV30BudgetReadErrorStore{
					GovStore: app.badgerStore, err: backendErr,
				}, nil)
			case "malformed_json":
				require.NoError(t, app.badgerStore.SetState("gov:proposal:broken-budget", []byte("{")))
			case "missing_engine":
				app.govEngine = nil
			}
			checkError := func(err error) {
				t.Helper()
				require.Error(t, err)
				require.NotErrorIs(t, err, errAppV30GeneratedWriteBudgetExceeded)
				switch test {
				case "backend_read":
					require.ErrorIs(t, err, backendErr)
				case "malformed_json":
					var syntaxErr *json.SyntaxError
					require.ErrorAs(t, err, &syntaxErr)
				case "missing_proposal":
					require.ErrorContains(t, err, "not found")
				case "missing_engine":
					require.ErrorContains(t, err, "engine is unavailable")
				}
			}
			txs := [][]byte{makeAppV30WorkRawTx(t, 1)}
			_, err := app.PrepareProposal(context.Background(), &abcitypes.RequestPrepareProposal{
				Height: 21, Txs: txs, MaxTxBytes: 2_000_000,
			})
			checkError(err)
			_, err = app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{Height: 21, Txs: txs})
			checkError(err)
			_, err = app.finalizeBlockUncommitted(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 21, Txs: txs})
			checkError(err)
			active, err := app.badgerStore.GetState("gov:active")
			require.NoError(t, err)
			require.Equal(t, []byte("broken-budget"), active)
		})
	}
}
