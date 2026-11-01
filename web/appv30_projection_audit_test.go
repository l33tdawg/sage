package web

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30CanonicalProjectionAuditTreatsEffectiveTaintAsVisibilityPolicy(t *testing.T) {
	fixture := newAppV23ProjectionRouteFixture(t, false)
	for _, id := range []string{"visible-audit", "held-audit"} {
		insertTestMemory(t, fixture.sql, id, "audit-domain")
		publishAppV23DashboardRecord(t, fixture.sql, fixture.badger, id,
			uint8(store.ClearanceInternal), true)
	}
	decisionID := applyProjectionAuditTaint(t, fixture, "held-audit")

	require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background()))
	health := fixture.badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked)
	require.True(t, health.OK)
	require.False(t, health.Quarantined)
	require.Equal(t, store.CanonicalMemoryProjectionExact, health.State)

	stats, _, projection, err := fixture.handler.requireAppV23DashboardProjectionAvailable(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, stats.TotalMemories)
	require.False(t, projection.Partial)
	rec := requestLocalProjectionRoute(t, fixture, "/v1/dashboard/stats")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"total_memories":1`)
	require.NotContains(t, rec.Body.String(), "held-audit")
	require.NotContains(t, rec.Body.String(), "hidden_count")

	// Readiness cannot change the exact record's disclosure decision.
	held, err := fixture.sql.GetMemory(context.Background(), "held-audit")
	require.NoError(t, err)
	_, disposition, err := fixture.badger.ClassifyMemoryProjection(held)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.Equal(t, store.MemoryProjectionTainted, disposition)

	cleared, err := fixture.badger.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, cleared)
	require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background()))
	stats, _, projection, err = fixture.handler.requireAppV23DashboardProjectionAvailable(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, stats.TotalMemories)
	require.False(t, projection.Partial)
}

func TestAppV30CanonicalProjectionAuditPreservesPendingFailure(t *testing.T) {
	fixture := newAppV23ProjectionRouteFixture(t, false)
	for _, id := range []string{"audit-parent", "audit-child"} {
		insertTestMemory(t, fixture.sql, id, "audit-domain")
		publishAppV23DashboardRecord(t, fixture.sql, fixture.badger, id,
			uint8(store.ClearanceInternal), true)
	}
	parentHash := sha256.Sum256([]byte("content-audit-parent"))
	require.NoError(t, fixture.badger.SetMemoryLineage("audit-child", []tx.ParentRef{{
		SourceChainID: "sage", MemoryID: "audit-parent", ContentHash: parentHash,
	}}))
	applyProjectionAuditTaint(t, fixture, "audit-parent")

	// An ancestor decision has not yet expanded to the child. Unlike a proven
	// effective hold, incomplete coverage still aborts the strict audit.
	err := fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background())
	require.ErrorIs(t, err, store.ErrMemoryProjectionTaintPending)
	require.False(t, fixture.badger.CanonicalMemoryProjectionHealth().OK)
	rec := requestLocalProjectionRoute(t, fixture, "/v1/dashboard/memory/audit-child/related")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "audit-child")
}

func TestAppV30CanonicalProjectionAuditDoesNotHideTaintedContentCorruption(t *testing.T) {
	fixture := newAppV23ProjectionRouteFixture(t, false)
	const id = "corrupt-held-audit"
	insertTestMemory(t, fixture.sql, id, "audit-domain")
	publishAppV23DashboardRecord(t, fixture.sql, fixture.badger, id,
		uint8(store.ClearanceInternal), true)
	applyProjectionAuditTaint(t, fixture, id)
	tamperAppV23ProjectionRow(t, fixture.dbPath,
		"UPDATE memories SET content = ? WHERE memory_id = ?", "tampered plaintext", id)

	held, err := fixture.sql.GetMemory(context.Background(), id)
	require.NoError(t, err)
	_, disposition, err := fixture.badger.ClassifyMemoryProjection(held)
	require.ErrorIs(t, err, store.ErrMemoryProjectionQuarantined)
	require.NotErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.Equal(t, store.MemoryProjectionQuarantined, disposition)
	require.Error(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background()))
	require.True(t, fixture.badger.CanonicalMemoryProjectionHealth().Quarantined)
	_, _, projection, err := fixture.handler.requireAppV23DashboardProjectionAudited(context.Background())
	require.NoError(t, err)
	require.True(t, projection.Partial)
}

func applyProjectionAuditTaint(t *testing.T, fixture appV23ProjectionRouteFixture, id string) string {
	t.Helper()
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	contentHash := sha256.Sum256([]byte("content-" + id))
	created, err := fixture.badger.ApplyTaintDecision("sage", decisionID,
		tx.ParentRef{SourceChainID: "sage", MemoryID: id, ContentHash: contentHash}, [32]byte{1})
	require.NoError(t, err)
	require.True(t, created)
	return decisionID
}
