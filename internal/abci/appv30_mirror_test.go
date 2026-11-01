package abci

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

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
