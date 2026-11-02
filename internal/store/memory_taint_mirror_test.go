package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
)

func TestRebuildMemoryTaintMirrorUsesEffectiveMemberships(t *testing.T) {
	ctx := context.Background()
	canonical := openTaintStore(t)
	mirror, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "taint-mirror.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mirror.Close()) })

	target := testTaintParent("mirror-target")
	childA := seedTaintChild(t, canonical, target, "mirror-child-a")
	seedTaintChild(t, canonical, target, "mirror-child-b")
	decisionID := testTaintDecisionID("mirror")
	created, err := canonical.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash("mirror"),
	)
	require.NoError(t, err)
	require.True(t, created)
	_, err = canonical.ExpandTaintDecision(
		"sage",
		decisionID,
		taint.PerExecutionUnitBudget,
	)
	require.NoError(t, err)

	revision, count, err := RebuildMemoryTaintMirror(ctx, canonical, mirror, 3)
	require.NoError(t, err)
	require.Equal(t, 3, count)
	require.Equal(t, uint64(2), revision)
	mirrorRevision, err := mirror.MemoryTaintMirrorRevision(ctx)
	require.NoError(t, err)
	require.Equal(t, revision, mirrorRevision)

	for _, item := range []struct {
		memoryID string
		content  string
	}{
		{target.MemoryID, "tainted target"},
		{"mirror-clean", "clean memory"},
	} {
		contentHash := memory.ComputeContentHash(item.content)
		require.NoError(t, mirror.InsertMemory(ctx, &memory.MemoryRecord{
			MemoryID:        item.memoryID,
			SubmittingAgent: "agent-a",
			Content:         item.content,
			ContentHash:     contentHash,
			MemoryType:      memory.TypeFact,
			DomainTag:       "general",
			ConfidenceScore: 0.9,
			Embedding:       []float32{1, 0},
			Status:          memory.StatusCommitted,
			CreatedAt:       time.Now().UTC(),
		}))
	}
	listed, total, err := mirror.ListMemories(ctx, ListOptions{
		Limit:          10,
		ExcludeTainted: true,
	})
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, listed, 1)
	require.Equal(t, "mirror-clean", listed[0].MemoryID)
	textResults, err := mirror.SearchByText(ctx, "target", QueryOptions{
		StatusFilter:   "committed",
		ExcludeTainted: true,
		TopK:           10,
	})
	require.NoError(t, err)
	require.Empty(t, textResults)
	vectorResults, err := mirror.QuerySimilar(ctx, []float32{1, 0}, QueryOptions{
		StatusFilter:   "committed",
		ExcludeTainted: true,
		TopK:           10,
	})
	require.NoError(t, err)
	require.Len(t, vectorResults, 1)
	require.Equal(t, "mirror-clean", vectorResults[0].MemoryID)

	rows, err := mirror.conn.QueryContext(ctx,
		`SELECT memory_id FROM memory_taint_mirror ORDER BY memory_id`,
	)
	require.NoError(t, err)
	var memoryIDs []string
	for rows.Next() {
		var memoryID string
		require.NoError(t, rows.Scan(&memoryID))
		memoryIDs = append(memoryIDs, memoryID)
	}
	require.NoError(t, rows.Close())
	require.Equal(t, []string{
		childA.MemoryID,
		"mirror-child-b",
		target.MemoryID,
	}, memoryIDs)

	changed, err := canonical.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	fresh, _, err := MemoryTaintMirrorFresh(ctx, canonical, mirror)
	require.NoError(t, err)
	require.False(t, fresh)
	revision, count, err = RebuildMemoryTaintMirror(ctx, canonical, mirror, 3)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Equal(t, uint64(3), revision)
	fresh, _, err = MemoryTaintMirrorFresh(ctx, canonical, mirror)
	require.NoError(t, err)
	require.True(t, fresh)
	var mirrored int
	require.NoError(t, mirror.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memory_taint_mirror`,
	).Scan(&mirrored))
	require.Zero(t, mirrored)
}
