package abci

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// Root's authority principal is immutable: rotation changes its current signing
// credential and generation, not the principal or already-recorded authorship.
// This is the ordered consensus witness missing from the separate v23 rotation
// and v30 handler-level provenance fixtures. It does not model mempool delivery
// of a future credential, an operator handover, or a network scenario.
func TestAppV30SameBlockRootRotationPreservesSubmittedProvenance(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	canonicalPath := filepath.Join(directory, "canonical")
	projectionPath := filepath.Join(directory, "projection.db")
	app := openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
	t.Cleanup(func() {
		if app != nil {
			require.NoError(t, app.Close())
		}
	})
	const chain = "rotation-provenance-v30"
	const domain = "fresh-home"
	const beforeID = "root-submit-before-rotation"
	const afterID = "root-submit-after-rotation"
	const refusedID = "retired-root-submit-after-rotation"
	manifest, original, companion := freshGenesisFixture(t, 30, 4, true)
	_, err := app.InitChain(ctx, freshGenesisRequest(t, chain, manifest, original, companion))
	require.NoError(t, err)
	require.True(t, app.IsAppV30ActiveForNextTx())
	initialRoot, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, initialRoot)
	require.Equal(t, original.id, initialRoot.PrincipalID)
	require.Equal(t, original.id, initialRoot.CredentialID)
	require.Equal(t, uint64(1), initialRoot.Generation, "real signed fresh origin, not a manually installed generation")
	replacement := newAgentKey(t)
	at := time.Now().UTC().Truncate(time.Second)

	submit := func(signer agentKey, id string, nonce uint64) []byte {
		t.Helper()
		parsed := makeMemorySubmitTx(t, signer, domain, "immutable provenance for "+id)
		parsed.MemorySubmit.MemoryID = id
		parsed.MemorySubmit.Classification = tx.ClearanceInternal
		parsed.Timestamp = at
		return encodeDirectH0Tx(t, parsed, signer, nonce)
	}
	before := submit(original, beforeID, 1)
	rotation := &tx.RootCredentialRotate{
		ExpectedGeneration: initialRoot.Generation,
		NewCredentialID:    replacement.id,
		Scope:              initialRoot.Scope,
	}
	rotation.NewCredentialSignature = ed25519.Sign(replacement.priv,
		tx.RootCredentialRotationSignBytes(initialRoot.PrincipalID, rotation))
	pub, sig, bodyHash, stamp := signAgentProof(t, original, []byte("same-block Root credential rotation"))
	rotationTx := &tx.ParsedTx{
		Type: tx.TxTypeRootCredentialRotate, RootCredentialRotate: rotation,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: stamp,
		Timestamp: at,
	}
	rotate := encodeDirectH0Tx(t, rotationTx, original, 2)
	after := submit(replacement, afterID, 1)

	// CoCommit uses a separate provenance resolver. Preserve an ordinary
	// coauthor's own principal and generation0 as well as the new Root tuple.
	envelope, peer := buildCoCommitEnvelope(t, replacement, domain,
		[]byte("rotation-provenance-cocommit"), "sage-rotation-peer")
	envelope.CreatedAtUnix = at.Unix()
	envelope.Coauthors[0].ChainID = chain
	core := tx.CanonicalCoreBytes(envelope)
	envelope.Coauthors[0].Sig = ed25519.Sign(replacement.priv, core)
	envelope.Coauthors[1].Sig = ed25519.Sign(peer.priv, core)
	envelope.SharedID = tx.ComputeSharedID(tx.CoreHashOf(envelope), envelope.Coauthors, envelope.AgreementNonce)
	coCommit := coCommitSubmitTx(t, replacement, envelope)
	coCommit.Timestamp = at
	coCommitRaw := encodeDirectH0Tx(t, coCommit, replacement, 2)
	refused := submit(original, refusedID, 3)

	prestate := appV30ReplayKeyspace(t, app)
	finalized, err := app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
		Height: 1, Time: at, Txs: [][]byte{before, rotate, after, coCommitRaw, refused},
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 5)
	for index := range 4 {
		require.Zero(t, finalized.TxResults[index].Code, "transaction%d: %s", index, finalized.TxResults[index].Log)
	}
	// The retired Root's legacy Admin registration is refused by the
	// control-elevation gate before MemorySubmit's identity handler runs.
	require.Equal(t, uint32(110), finalized.TxResults[4].Code, "retired credential must be refused within the same ordered block")
	require.Equal(t, "access denied", finalized.TxResults[4].Log)
	require.Equal(t, prestate, appV30ReplayKeyspace(t, app), "rotation and memory writes remain speculative until Commit")
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, int64(1), app.state.Height)
	require.Len(t, finalized.AppHash, 32)
	require.Equal(t, finalized.AppHash, app.state.AppHash)
	require.NotEqual(t, prestate, appV30ReplayKeyspace(t, app))

	peerID := hex.EncodeToString(peer.pub)
	want := map[string][]store.AppV30ProvenanceSigner{
		beforeID: {{CredentialID: original.id, PrincipalID: initialRoot.PrincipalID,
			RootGeneration: initialRoot.Generation, Exact: true}},
		afterID: {{CredentialID: replacement.id, PrincipalID: initialRoot.PrincipalID,
			RootGeneration: initialRoot.Generation + 1, Exact: true}},
		envelope.SharedID: {
			{CredentialID: replacement.id, PrincipalID: initialRoot.PrincipalID,
				RootGeneration: initialRoot.Generation + 1, Exact: true},
			{CredentialID: peerID, PrincipalID: peerID, RootGeneration: 0, Exact: true},
		},
	}
	assertProvenance := func() {
		t.Helper()
		currentRoot, readErr := app.badgerStore.GetAppV23Root()
		require.NoError(t, readErr)
		require.NotNil(t, currentRoot)
		require.Equal(t, initialRoot.PrincipalID, currentRoot.PrincipalID)
		require.Equal(t, replacement.id, currentRoot.CredentialID)
		require.Equal(t, initialRoot.Generation+1, currentRoot.Generation)
		_, _, _, retiredErr := app.appV23Actor(original.id)
		require.ErrorIs(t, retiredErr, store.ErrAppV23NeedsApproval)
		for id, signers := range want {
			recorded, found, provenanceErr := app.badgerStore.GetAppV30Provenance(id)
			require.NoError(t, provenanceErr)
			require.True(t, found, id)
			require.ElementsMatch(t, signers, recorded, id)
		}
		for id, credential := range map[string]string{beforeID: original.id, afterID: replacement.id, envelope.SharedID: replacement.id} {
			author, authorErr := app.badgerStore.GetMemoryAuthor(id)
			require.NoError(t, authorErr)
			require.Equal(t, credential, author, id)
			principal, principalErr := app.badgerStore.GetMemoryAuthorPrincipal(id)
			require.NoError(t, principalErr)
			require.Equal(t, initialRoot.PrincipalID, principal, id)
		}
		_, found, provenanceErr := app.badgerStore.GetAppV30Provenance(refusedID)
		require.NoError(t, provenanceErr)
		require.False(t, found, "refused retired-credential submission must not acquire provenance")
		_, readErr = app.badgerStore.GetMemoryDisclosureStateAppV30(refusedID)
		require.ErrorIs(t, readErr, store.ErrMemoryDisclosureNotFound)
	}
	assertProvenance()
	committed := appV30ReplayKeyspace(t, app)
	committedHash := append([]byte(nil), app.state.AppHash...)
	require.NoError(t, app.Close())
	app = nil
	app = openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
	info, err := app.Info(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), info.LastBlockHeight)
	require.Equal(t, uint64(30), info.AppVersion)
	require.Equal(t, committedHash, info.LastBlockAppHash)
	require.Equal(t, committed, appV30ReplayKeyspace(t, app))
	assertProvenance()
}
