package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// Cached judge answers may affect a node's future vote, but they and the
// locally retained source bytes must not affect execution of identical signed
// consensus input. This records actual outputs through the SQLite APIs; it
// does not run a judge, exercise an operator endpoint, or claim encryption.
func TestAppV30NodeLocalEvidenceAndJudgmentsDoNotChangeCommittedAppHash(t *testing.T) {
	ctx := context.Background()
	const chain = "app-v30-local-state"
	const domain = "fresh-home"
	const memoryID = "app-v30-local-output-memory"
	const judgeVersion = "local-output-fixture-v1"
	manifest, root, companion := freshGenesisFixture(t, 30, 4, true)
	require.NotNil(t, companion)
	signer := *companion
	apps := make([]*SageApp, 0, 2)
	projections := make([]*store.SQLiteStore, 0, 2)
	for range 2 {
		directory := t.TempDir()
		app := openFreshGenesisH0TestApp(t, filepath.Join(directory, "canonical"), filepath.Join(directory, "projection.db"))
		t.Cleanup(func() { require.NoError(t, app.Close()) })
		_, err := app.InitChain(ctx, freshGenesisRequest(t, chain, manifest, root, companion))
		require.NoError(t, err)
		require.True(t, app.IsAppV30ActiveForNextTx())
		projection, ok := app.offchainStore.(*store.SQLiteStore)
		require.True(t, ok)
		apps = append(apps, app)
		projections = append(projections, projection)
	}
	before := appV30ReplayKeyspace(t, apps[0])
	require.Equal(t, before, appV30ReplayKeyspace(t, apps[1]), "two independently opened stores have identical signed fresh origin")

	// Retention policy and the presence of pinned taint evidence genuinely
	// differ. These references are local primitive fixtures, not a claim that
	// a voted on-chain taint decision was executed in this test.
	at := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, projections[0].ConfigureTaintEvidenceDomain(ctx, domain, store.DefaultTaintEvidenceRetention(), at))
	require.NoError(t, projections[1].ConfigureTaintEvidenceDomain(ctx, domain, store.TaintEvidenceRetention{
		MaxBytes: 4096, MaxItems: 2, MaxAge: time.Hour,
	}, at))
	require.NoError(t, projections[1].DisableTaintEvidenceDomain(ctx, domain, at))
	decision := sha256.Sum256([]byte("node-local retention fixture reference"))
	decisionID := hex.EncodeToString(decision[:])
	retained := []byte("source bytes retained only by the first node")
	digest, err := projections[0].PutTaintEvidence(ctx, domain, decisionID, retained, at)
	require.NoError(t, err)
	_, err = projections[1].PutTaintEvidence(ctx, domain, decisionID, retained, at)
	require.ErrorIs(t, err, store.ErrTaintEvidenceDisabled)
	got, err := projections[0].GetTaintEvidence(ctx, domain, digest, at)
	require.NoError(t, err)
	require.Equal(t, retained, got)
	_, err = projections[1].GetTaintEvidence(ctx, domain, digest, at)
	require.ErrorIs(t, err, store.ErrTaintEvidenceNotFound)
	usageA, err := projections[0].TaintEvidenceUsage(ctx, domain)
	require.NoError(t, err)
	usageB, err := projections[1].TaintEvidenceUsage(ctx, domain)
	require.NoError(t, err)
	require.Equal(t, int64(1), usageA.Items)
	require.Equal(t, int64(1), usageA.References)
	require.Equal(t, int64(0), usageB.Items)
	require.Equal(t, int64(0), usageB.References)

	// The normal write-gate source evidence and cached judge output also
	// differ for the very memory the next signed block will submit. The
	// public store APIs permit a claim and verdict before its SQL row exists.
	sources := []string{"first node's local source text", "second node's different source text"}
	verdicts := []memory.SemanticVerdict{
		{Verdict: memory.VerdictPass, P: 0.99, Reason: "first node's cached answer"},
		{Verdict: memory.VerdictReject, P: 0.01, Reason: "second node's opposite cached answer"},
	}
	for i, projection := range projections {
		evidenceID, createErr := projection.CreateMemoryEvidence(ctx, signer.id, sources[i])
		require.NoError(t, createErr)
		require.NoError(t, projection.ClaimMemoryEvidence(ctx, evidenceID, signer.id, memoryID))
		source, found, readErr := projection.JudgeableEvidence(ctx, memoryID)
		require.NoError(t, readErr)
		require.True(t, found)
		require.Equal(t, sources[i], source)
		require.NoError(t, projection.RecordSemanticVerdict(ctx, memoryID, judgeVersion, verdicts[i]))
		verdict, found, readErr := projection.SemanticVerdict(ctx, memoryID, judgeVersion)
		require.NoError(t, readErr)
		require.True(t, found)
		require.Equal(t, verdicts[i], verdict)
	}
	require.NotEqual(t, sources[0], sources[1])
	require.NotEqual(t, verdicts[0], verdicts[1])
	for _, app := range apps {
		require.Equal(t, before, appV30ReplayKeyspace(t, app), "local writes must not publish canonical keys")
	}

	// One wire envelope is signed once, then independently checked and
	// executed by both apps. This must actually advance canonical state;
	// merely hashing the same untouched Badger database would be vacuous.
	parsed := makeMemorySubmitTx(t, signer, domain, "identical consensus content despite different local judge answers")
	parsed.MemorySubmit.MemoryID = memoryID
	parsed.MemorySubmit.Classification = tx.ClearanceInternal
	parsed.Timestamp = at
	raw := encodeDirectH0Tx(t, parsed, signer, 1)
	var expected *abcitypes.ResponseFinalizeBlock
	for _, app := range apps {
		checked, checkErr := app.CheckTx(ctx, &abcitypes.RequestCheckTx{Tx: raw})
		require.NoError(t, checkErr)
		require.Zero(t, checked.Code, checked.Log)
		finalized, finalizeErr := app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
			Height: 1, Time: at, Txs: [][]byte{raw},
		})
		require.NoError(t, finalizeErr)
		require.Len(t, finalized.TxResults, 1)
		require.Zero(t, finalized.TxResults[0].Code, finalized.TxResults[0].Log)
		require.Len(t, finalized.AppHash, sha256.Size)
		require.Equal(t, before, appV30ReplayKeyspace(t, app), "Finalize remains speculative until Commit")
		if expected == nil {
			expected = finalized
		} else {
			require.Equal(t, expected, finalized, "the entire ABCI response must be independent of local evidence and judge output")
		}
		_, commitErr := app.Commit(ctx, &abcitypes.RequestCommit{})
		require.NoError(t, commitErr)
		require.Equal(t, int64(1), app.state.Height)
		require.Equal(t, finalized.AppHash, app.state.AppHash)
		state, readErr := app.badgerStore.GetMemoryDisclosureStateAppV30(memoryID)
		require.NoError(t, readErr)
		require.NotNil(t, state)
		require.NotEqual(t, before, appV30ReplayKeyspace(t, app), "the signed transaction must produce real canonical changes")
	}
	require.Equal(t, appV30ReplayKeyspace(t, apps[0]), appV30ReplayKeyspace(t, apps[1]))
	require.Equal(t, apps[0].state.AppHash, apps[1].state.AppHash)
	for i, projection := range projections {
		verdict, found, readErr := projection.SemanticVerdict(ctx, memoryID, judgeVersion)
		require.NoError(t, readErr)
		require.True(t, found)
		require.Equal(t, verdicts[i], verdict, "Commit must not erase the local difference to manufacture equality")
		source, found, readErr := projection.JudgeableEvidence(ctx, memoryID)
		require.NoError(t, readErr)
		require.True(t, found)
		require.Equal(t, sources[i], source)
	}
}
