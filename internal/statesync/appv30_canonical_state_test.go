package statesync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// This is an orderly local Badger reopen and canonical stream round trip,
// not process-crash recovery or a network provider/receiver state-sync test.
func TestCanonicalStateRoundTripPreservesAppV30TaintLineageAndProvenance(t *testing.T) {
	ctx := context.Background()
	sourceDir := t.TempDir()
	source, err := store.NewBadgerStore(sourceDir)
	require.NoError(t, err)
	closeSource := closeAppV30CanonicalHandleOnCleanup(t, source.CloseBadger)

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
	require.NoError(t, source.ValidateMemoryLineageParents(child.MemoryID, "sage", []tx.ParentRef{parent}))
	require.NoError(t, source.SetMemoryLineage(child.MemoryID, []tx.ParentRef{parent}))
	require.NoError(t, source.ValidateMemoryLineageParents(grandchild.MemoryID, "sage", []tx.ParentRef{child}))
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

	expectedSigners := []store.AppV30ProvenanceSigner{
		{
			CredentialID:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			PrincipalID:    "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			RootGeneration: 7,
			Exact:          true,
		},
		{
			CredentialID:   "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			PrincipalID:    "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			RootGeneration: 11,
			Exact:          true,
		},
	}
	require.NoError(t, source.SetAppV30Provenance(child.MemoryID, expectedSigners))
	sourceHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	closeSource()

	sourceDB, err := badger.Open(badger.DefaultOptions(sourceDir).WithLogger(nil))
	require.NoError(t, err)
	closeSourceDB := closeAppV30CanonicalHandleOnCleanup(t, sourceDB.Close)
	var stream bytes.Buffer
	require.NoError(t, WriteCanonicalState(ctx, sourceDB, &stream))
	closeSourceDB()

	reopened, err := store.NewBadgerStore(sourceDir)
	require.NoError(t, err)
	closeReopened := closeAppV30CanonicalHandleOnCleanup(t, reopened.CloseBadger)
	reopenedHash, err := reopened.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, sourceHash, reopenedHash, "orderly reopen must preserve the exact store hash")
	verifyAppV30StateSyncState(t, reopened, parent, child, grandchild, decisionID, expectedSigners)
	closeReopened()

	restoredDir := t.TempDir()
	restoredDB, err := badger.Open(badger.DefaultOptions(restoredDir).WithLogger(nil))
	require.NoError(t, err)
	closeRestoredDB := closeAppV30CanonicalHandleOnCleanup(t, restoredDB.Close)
	require.NoError(t, RestoreCanonicalState(ctx, bytes.NewReader(stream.Bytes()), restoredDB))
	closeRestoredDB()

	restored, err := store.OpenBadgerStoreReadOnly(restoredDir)
	require.NoError(t, err)
	closeRestored := closeAppV30CanonicalHandleOnCleanup(t, restored.CloseBadger)
	restoredHash, err := restored.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, sourceHash, restoredHash)
	verifyAppV30StateSyncState(t, restored, parent, child, grandchild, decisionID, expectedSigners)
	closeRestored()
}

func verifyAppV30StateSyncState(
	t *testing.T,
	reader *store.BadgerStore,
	parent, child, grandchild tx.ParentRef,
	decisionID string,
	expectedSigners []store.AppV30ProvenanceSigner,
) {
	t.Helper()
	for _, memoryID := range []string{parent.MemoryID, child.MemoryID, grandchild.MemoryID} {
		decisions, err := reader.GetEffectiveTaintDecisions(memoryID)
		require.NoError(t, err)
		require.Equal(t, []string{decisionID}, decisions, memoryID)
	}
	lineage, err := reader.GetMemoryLineage(grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{child}, lineage)
	signers, found, err := reader.GetAppV30Provenance(child.MemoryID)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, signers, len(expectedSigners))
	require.Equal(t, expectedSigners, signers)
}

// Each open handle registers cleanup immediately. Explicit orderly closes and
// failure cleanup share one cached close result, so no handle is closed twice.
func closeAppV30CanonicalHandleOnCleanup(t *testing.T, closeHandle func() error) func() {
	t.Helper()
	closeOnce := sync.OnceValue(closeHandle)
	closeChecked := func() {
		t.Helper()
		require.NoError(t, closeOnce())
	}
	t.Cleanup(closeChecked)
	return closeChecked
}
