package abci

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30MemoryTaintMirrorRefreshesAfterRevisionChange(t *testing.T) {
	app := setupTestApp(t)
	mirror, ok := app.offchainStore.(store.MemoryTaintMirrorStore)
	require.True(t, ok)

	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		"sage",
		decisionID,
		tx.ParentRef{SourceChainID: "sage", MemoryID: "mirror-lifecycle-target"},
		[32]byte{1},
	)
	require.NoError(t, err)
	require.True(t, created)
	app.refreshMemoryTaintMirror(context.Background())
	revision, err := mirror.MemoryTaintMirrorRevision(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), revision)

	changed, err := app.badgerStore.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	app.refreshMemoryTaintMirror(context.Background())
	revision, err = mirror.MemoryTaintMirrorRevision(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(2), revision)
}

func TestAppV30MirrorLossFallsBackToBadgerAndRebuilds(t *testing.T) {
	app, _, target, chainID := setupAppV30TaintGovernance(t)
	ctx := context.Background()
	mirror, ok := app.offchainStore.(store.MemoryTaintMirrorStore)
	require.True(t, ok)
	decisionID := strings.Repeat("d", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		[32]byte{1},
	)
	require.NoError(t, err)
	require.True(t, created)

	// Simulate a missing/rolled-back mirror update. Revision zero never matches
	// canonical taint state, so SQL callers must disable the prefilter.
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(ctx, 0, nil))
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, app.badgerStore, mirror)
	require.NoError(t, err)
	require.False(t, fresh)

	state, err := app.badgerStore.GetMemoryDisclosureState(target.MemoryID)
	require.NoError(t, err)
	record := &memory.MemoryRecord{
		MemoryID:        target.MemoryID,
		SubmittingAgent: state.Author,
		Content:         "governance taint target",
		ContentHash:     append([]byte(nil), state.ContentHash...),
		DomainTag:       state.Domain,
		Status:          memory.StatusCommitted,
	}
	_, disposition, err := app.badgerStore.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.Equal(t, store.MemoryProjectionTainted, disposition)

	app.refreshMemoryTaintMirror(ctx)
	fresh, revision, err := store.MemoryTaintMirrorFresh(ctx, app.badgerStore, mirror)
	require.NoError(t, err)
	require.True(t, fresh)
	canonicalRevision, err := app.badgerStore.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, canonicalRevision, revision)

	_, disposition, err = app.badgerStore.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.Equal(t, store.MemoryProjectionTainted, disposition)
}
