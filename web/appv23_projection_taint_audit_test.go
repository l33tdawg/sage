package web

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const projectionAuditHeldID = "projection-audit-held"

func newTaintProjectionAuditFixture(t *testing.T, encrypted bool) appV23ProjectionRouteFixture {
	t.Helper()
	fixture := newAppV23ProjectionRouteFixture(t, encrypted)
	for i, id := range []string{"projection-audit-first", projectionAuditHeldID, "projection-audit-last"} {
		content := "content-" + id
		hash := sha256.Sum256([]byte(content))
		require.NoError(t, fixture.sql.InsertMemory(context.Background(), &memory.MemoryRecord{
			MemoryID: id, SubmittingAgent: "agent1", Content: content, ContentHash: hash[:],
			MemoryType: memory.TypeObservation, DomainTag: "projection-audit-domain",
			ConfidenceScore: 0.9, Status: memory.StatusCommitted,
			CreatedAt: time.Unix(1_700_000_000+int64(i), 0).UTC(),
		}))
		publishAppV23DashboardRecord(t, fixture.sql, fixture.badger, id, uint8(store.ClearanceInternal), true)
	}
	return fixture
}

func applyProjectionAuditHold(t *testing.T, fixture appV23ProjectionRouteFixture) string {
	t.Helper()
	held, err := fixture.sql.GetMemory(context.Background(), projectionAuditHeldID)
	require.NoError(t, err)
	ref := tx.ParentRef{SourceChainID: "sage", MemoryID: projectionAuditHeldID}
	copy(ref.ContentHash[:], held.ContentHash)
	decisionID := strings.Repeat("c", taint.DecisionIDBytes)
	created, err := fixture.badger.ApplyTaintDecision("sage", decisionID, ref,
		sha256.Sum256([]byte("projection audit policy hold")))
	require.NoError(t, err)
	require.True(t, created)
	return decisionID
}

func requireHealthyTaintAuditList(t *testing.T, fixture appV23ProjectionRouteFixture, path string, wantIDs []string, wantTotal int) {
	t.Helper()
	rec := requestLocalProjectionRoute(t, fixture, path)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		Memories   []*memory.MemoryRecord    `json:"memories"`
		Total      int                       `json:"total"`
		Projection *appV23ProjectionResponse `json:"projection"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ids := make([]string, 0, len(body.Memories))
	for _, record := range body.Memories {
		ids = append(ids, record.MemoryID)
	}
	require.Equal(t, wantIDs, ids)
	require.Equal(t, wantTotal, body.Total)
	require.NotNil(t, body.Projection)
	require.True(t, body.Projection.Complete)
	require.False(t, body.Projection.Partial)
	require.Zero(t, body.Projection.HiddenCount, "governed withholding is not projection quarantine")
	require.Empty(t, body.Projection.Message)
	require.NotContains(t, rec.Body.String(), "content-"+projectionAuditHeldID)
}

func TestAppV23CanonicalProjectionTaintAuditPolicy(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, subset := range []bool{false, true} {
			t.Run(fmt.Sprintf("encrypted=%t/subset=%t", encrypted, subset), func(t *testing.T) {
				fixture := newTaintProjectionAuditFixture(t, encrypted)
				if subset {
					fixture.handler.CanonicalProjectionMissingAllowedFn = func(string) bool { return false }
				}
				applyProjectionAuditHold(t, fixture)
				held, err := fixture.sql.GetMemory(context.Background(), projectionAuditHeldID)
				require.NoError(t, err)
				_, disposition, err := fixture.badger.ClassifyMemoryProjection(held)
				require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
				require.Equal(t, store.MemoryProjectionTainted, disposition)

				require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background()))
				health := fixture.badger.CanonicalMemoryProjectionHealth()
				require.True(t, health.Checked)
				require.True(t, health.OK)
				require.False(t, health.Quarantined)
				if subset {
					require.Equal(t, store.CanonicalMemoryProjectionSubset, health.State)
				} else {
					require.Equal(t, store.CanonicalMemoryProjectionExact, health.State)
				}
				stats, _, projection, err := fixture.handler.requireAppV23DashboardProjectionAvailable(context.Background())
				require.NoError(t, err)
				require.Equal(t, 2, stats.TotalMemories)
				require.True(t, projection.Complete)
				require.Zero(t, projection.HiddenCount)
				require.ErrorIs(t, fixture.handler.validateAppV23DashboardRecord(held), store.ErrMemoryProjectionTainted,
					"a healthy inventory must not authorize exact disclosure of held content")
				related := requestLocalProjectionRoute(t, fixture, "/v1/dashboard/memory/"+projectionAuditHeldID+"/related")
				require.Equal(t, http.StatusServiceUnavailable, related.Code, related.Body.String())
				require.NotContains(t, related.Body.String(), held.Content)
				if encrypted {
					requireHealthyTaintAuditList(t, fixture, "/v1/dashboard/memory/list?q=content&sort=oldest&limit=10",
						[]string{"projection-audit-first", "projection-audit-last"}, 2)
				}
			})
		}
	}
}

func TestAppV23CanonicalProjectionTaintAuditQuarantinesTamperedHeldRow(t *testing.T) {
	fixture := newTaintProjectionAuditFixture(t, false)
	applyProjectionAuditHold(t, fixture)
	tamperAppV23ProjectionRow(t, fixture.dbPath,
		"UPDATE memories SET content = ? WHERE memory_id = ?", "forged held content", projectionAuditHeldID)
	held, err := fixture.sql.GetMemory(context.Background(), projectionAuditHeldID)
	require.NoError(t, err)
	_, disposition, err := fixture.badger.ClassifyMemoryProjection(held)
	require.ErrorIs(t, err, store.ErrMemoryProjectionQuarantined)
	require.Equal(t, store.MemoryProjectionQuarantined, disposition,
		"canonical-envelope corruption must be detected before taint")
	err = fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background())
	require.ErrorIs(t, err, errAppV23DashboardProjectionUnavailable)
	health := fixture.badger.CanonicalMemoryProjectionHealth()
	require.True(t, health.Checked)
	require.True(t, health.Quarantined)
	require.Equal(t, store.CanonicalMemoryProjectionQuarantined, health.State)
	rec := requestLocalProjectionRoute(t, fixture, "/v1/dashboard/memory/list?sort=oldest&limit=10")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"partial":true`)
	require.Contains(t, rec.Body.String(), `"hidden_count":1`)
	require.NotContains(t, rec.Body.String(), "forged held content")
}

func TestAppV23CanonicalProjectionTaintAuditPreservesPendingFailure(t *testing.T) {
	fixture := newTaintProjectionAuditFixture(t, false)
	held, err := fixture.sql.GetMemory(context.Background(), projectionAuditHeldID)
	require.NoError(t, err)
	ref := tx.ParentRef{SourceChainID: "sage", MemoryID: projectionAuditHeldID}
	copy(ref.ContentHash[:], held.ContentHash)
	require.NoError(t, fixture.badger.SetMemoryLineage("projection-audit-last", []tx.ParentRef{ref}))
	applyProjectionAuditHold(t, fixture)
	child, err := fixture.sql.GetMemory(context.Background(), "projection-audit-last")
	require.NoError(t, err)
	_, disposition, err := fixture.badger.ClassifyMemoryProjection(child)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTaintPending)
	require.Equal(t, store.MemoryProjectionTaintPending, disposition)
	err = fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background())
	require.ErrorIs(t, err, store.ErrMemoryProjectionTaintPending)
	health := fixture.badger.CanonicalMemoryProjectionHealth()
	require.False(t, health.Checked)
	require.False(t, health.OK)
	rec := requestLocalProjectionRoute(t, fixture, "/v1/dashboard/memory/list?sort=oldest&limit=10")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), child.Content)
}

func TestAppV23CanonicalProjectionTaintAuditKeepsFilteredPagingCounts(t *testing.T) {
	ctx := context.Background()
	fixture := newTaintProjectionAuditFixture(t, false)
	require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(ctx))
	clearSource, exact, err := fixture.handler.appV23ProjectionIsExactForCurrentSource(ctx)
	require.NoError(t, err)
	require.True(t, exact)
	decisionID := applyProjectionAuditHold(t, fixture)
	heldSource, exact, err := fixture.handler.appV23ProjectionIsExactForCurrentSource(ctx)
	require.NoError(t, err)
	require.NotEqual(t, clearSource, heldSource, "apply must invalidate the unfiltered cached audit")
	require.False(t, exact)
	require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(ctx))
	_, exact, err = fixture.handler.appV23ProjectionIsExactForCurrentSource(ctx)
	require.NoError(t, err)
	require.False(t, exact, "healthy policy-filtered inventory cannot expose raw SQL totals")
	for offset, wantID := range []string{"projection-audit-first", "projection-audit-last"} {
		rows, total, _, _, quarantined, pageErr := fixture.handler.appV23CanonicalDashboardPageObserved(ctx,
			store.ListOptions{Limit: 1, Offset: offset, Sort: "oldest"}, "")
		require.NoError(t, pageErr)
		require.Len(t, rows, 1)
		require.Equal(t, wantID, rows[0].MemoryID)
		require.Equal(t, 2, total, "held rows must not enter the raw-SQL total")
		require.Zero(t, quarantined)
		requireHealthyTaintAuditList(t, fixture,
			fmt.Sprintf("/v1/dashboard/memory/list?sort=oldest&limit=1&offset=%d", offset), []string{wantID}, 2)
	}
	changed, err := fixture.badger.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	releasedSource, exact, err := fixture.handler.appV23ProjectionIsExactForCurrentSource(ctx)
	require.NoError(t, err)
	require.NotEqual(t, heldSource, releasedSource, "clear must invalidate the policy-filtered cached audit")
	require.False(t, exact)
	require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(ctx))
	_, exact, err = fixture.handler.appV23ProjectionIsExactForCurrentSource(ctx)
	require.NoError(t, err)
	require.True(t, exact, "the unfiltered shortcut may resume only after a fresh clear audit")
}
