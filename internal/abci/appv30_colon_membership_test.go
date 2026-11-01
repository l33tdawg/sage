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

func TestFreshV30OrdinaryColonIDCannotPoisonAnotherParentsAdmission(t *testing.T) {
	ctx := context.Background()
	app := setupTestApp(t)
	manifest, root, agent := freshGenesisFixture(t, 30, 1, true)
	const chainID = "colon-membership-reachability"
	_, err := app.InitChain(ctx, freshGenesisRequest(t, chainID, manifest, root, agent))
	require.NoError(t, err)

	// Only the committed parent corpus and authorized apply decision are store
	// fixtures. Both child submissions below use signed real FinalizeBlock and
	// Commit, including delegated-proof selection and app-v30 admission.
	parent := tx.ParentRef{SourceChainID: chainID, MemoryID: "parent", ContentHash: sha256.Sum256([]byte("parent content"))}
	victim := tx.ParentRef{SourceChainID: chainID, MemoryID: "victim", ContentHash: sha256.Sum256([]byte("victim content"))}
	for _, ref := range []tx.ParentRef{parent, victim} {
		require.NoError(t, app.badgerStore.SetMemoryHash(ref.MemoryID, ref.ContentHash[:], "committed"))
		require.NoError(t, app.badgerStore.SetMemoryDomain(ref.MemoryID, "fresh-home"))
		require.NoError(t, app.badgerStore.SetMemoryAuthor(ref.MemoryID, agent.id))
		require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(ref.MemoryID, agent.id))
		require.NoError(t, app.badgerStore.SetMemoryClassification(ref.MemoryID, 0))
	}
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(chainID, decisionID, parent, sha256.Sum256([]byte("apply fixture")))
	require.NoError(t, err)
	require.True(t, created)
	colon := makeMemorySubmitTx(t, *agent, "fresh-home", "colon child")
	colon.MemorySubmit.MemoryID = "victim:x"
	colon.MemorySubmit.DerivedFrom = []tx.ParentRef{parent}
	raw := encodeDirectH0Tx(t, colon, *agent, 1)
	finalized, err := app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 1, Time: time.Now().UTC(), Txs: [][]byte{raw}})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 1)
	require.Zero(t, finalized.TxResults[0].Code, finalized.TxResults[0].Log)
	require.Equal(t, "victim:x", string(finalized.TxResults[0].Data))
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	decisions, err := app.badgerStore.GetEffectiveTaintDecisions("victim:x")
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)
	refs, err := app.badgerStore.GetMemoryLineage("victim:x")
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{parent}, refs)

	unrelated := makeMemorySubmitTx(t, *agent, "fresh-home", "unrelated child")
	unrelated.MemorySubmit.MemoryID = "unrelated"
	derived := makeMemorySubmitTx(t, *agent, "fresh-home", "victim child")
	derived.MemorySubmit.MemoryID = "victim-child"
	derived.MemorySubmit.DerivedFrom = []tx.ParentRef{victim}
	finalized, err = app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 2, Time: time.Now().UTC(), Txs: [][]byte{
		encodeDirectH0Tx(t, unrelated, *agent, 2), encodeDirectH0Tx(t, derived, *agent, 3),
	}})
	require.NoError(t, err, "a failed admission must not halt block execution")
	require.Len(t, finalized.TxResults, 2)
	require.Zero(t, finalized.TxResults[0].Code, finalized.TxResults[0].Log)
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, int64(2), app.state.Height)
	require.Zero(t, finalized.TxResults[1].Code, finalized.TxResults[1].Log)
}
