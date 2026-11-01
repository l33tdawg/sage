package statesync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestCanonicalStateRoundTripPreservesAppV30TaintLineageAndProvenance(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	source, err := store.NewBadgerStore(sourceDir)
	require.NoError(t, err)

	parent := tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      "state-sync-parent",
		ContentHash:   sha256.Sum256([]byte("state sync parent")),
	}
	child := tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      "state-sync-child",
		ContentHash:   sha256.Sum256([]byte("state sync child")),
	}
	grandchild := tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      "state-sync-grandchild",
		ContentHash:   sha256.Sum256([]byte("state sync grandchild")),
	}
	for _, ref := range []tx.ParentRef{parent, child, grandchild} {
		require.NoError(t, source.SetMemoryHash(
			ref.MemoryID,
			ref.ContentHash[:],
			"committed",
		))
	}
	require.NoError(t, source.SetMemoryLineage(child.MemoryID, []tx.ParentRef{parent}))
	require.NoError(t, source.SetMemoryLineage(grandchild.MemoryID, []tx.ParentRef{child}))
	decisionID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	created, err := source.ApplyTaintDecision(
		"sage",
		decisionID,
		parent,
		sha256.Sum256([]byte("state sync taint payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	expanded, err := source.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)

	signerID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, source.SetAppV30Provenance(child.MemoryID, []store.AppV30ProvenanceSigner{{
		CredentialID: signerID,
		PrincipalID:  signerID,
		Exact:        true,
	}}))
	sourceHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, source.CloseBadger())

	sourceDB, err := badger.Open(badger.DefaultOptions(sourceDir).WithLogger(nil))
	require.NoError(t, err)
	var stream bytes.Buffer
	require.NoError(t, WriteCanonicalState(ctx, sourceDB, &stream))
	require.NoError(t, sourceDB.Close())

	restoredDir := t.TempDir()
	restoredDB, err := badger.Open(badger.DefaultOptions(restoredDir).WithLogger(nil))
	require.NoError(t, err)
	require.NoError(t, RestoreCanonicalState(ctx, bytes.NewReader(stream.Bytes()), restoredDB))
	require.NoError(t, restoredDB.Close())

	restored, err := store.OpenBadgerStoreReadOnly(restoredDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.CloseBadger()) })
	restoredHash, err := restored.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, sourceHash, restoredHash)

	for _, memoryID := range []string{parent.MemoryID, child.MemoryID, grandchild.MemoryID} {
		decisions, err := restored.GetEffectiveTaintDecisions(memoryID)
		require.NoError(t, err)
		require.Equal(t, []string{decisionID}, decisions, memoryID)
	}
	lineage, err := restored.GetMemoryLineage(grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{child}, lineage)
	signers, found, err := restored.GetAppV30Provenance(child.MemoryID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, signerID, signers[0].CredentialID)
	require.Equal(t, signerID, signers[0].PrincipalID)
}
