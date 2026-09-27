package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/l33tdawg/sage/internal/voter"
)

const reviewTestVersion = "review-test-v1"

func heldMemory(t *testing.T, s *store.SQLiteStore, id, domain, content string, created time.Time) {
	t.Helper()
	ctx := context.Background()
	h := sha256.Sum256([]byte(content))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: id, SubmittingAgent: "agent", Content: content, ContentHash: h[:],
		MemoryType: memory.TypeFact, DomainTag: domain, ConfidenceScore: 0.9,
		Status: memory.StatusProposed, CreatedAt: created,
	}))
	require.NoError(t, s.RecordSemanticVerdict(ctx, id, reviewTestVersion, memory.SemanticVerdict{
		Verdict: memory.VerdictAbstain, P: 0.7, Reason: "held for review: lasting-memory uncertain (p=0.70)",
	}))
}

func reviewQueue(t *testing.T, router http.Handler, query string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/review-queue"+query, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	return out
}

func itemIDs(resp map[string]any) []string {
	items, _ := resp["items"].([]any)
	ids := make([]string, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.(map[string]any)["memory_id"].(string))
	}
	return ids
}

func postReview(t *testing.T, router http.Handler, id, decision string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"decision": decision})
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/memory/"+id+"/review", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestReviewQueue_GateOffSaysSo(t *testing.T) {
	h, _ := newTestHandler(t)
	resp := reviewQueue(t, testRouter(h), "")
	require.Equal(t, false, resp["gate"].(map[string]any)["enabled"])
	require.Empty(t, itemIDs(resp))
	require.Equal(t, http.StatusConflict, postReview(t, testRouter(h), "m1", "accept").Code)
}

func TestReviewDecision_RefusedWhenContentBecameUnreadable(t *testing.T) {
	ctx := context.Background()
	h, s := newTestHandler(t)
	h.SetMemoryGate(&voter.Gate{Version: reviewTestVersion})
	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "review-test"))
	v, err := vault.Open(keyPath, "review-test")
	require.NoError(t, err)
	s.SetVault(v)
	heldMemory(t, s, "m-enc", "general-notes", "The quarterly count was around two hundred units.", time.Now().UTC())
	router := testRouter(h)

	s.SetVault(nil) // the vault locks after the operator loaded the page
	rec := postReview(t, router, "m-enc", "accept")
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "cannot be read")
	_, decided, err := s.ReviewDecision(ctx, "m-enc")
	require.NoError(t, err)
	require.False(t, decided, "no decision is stored for content that cannot be read")

	s.SetVault(v) // readable again: the same request now succeeds
	rec = postReview(t, router, "m-enc", "accept")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	d, decided, err := s.ReviewDecision(ctx, "m-enc")
	require.NoError(t, err)
	require.True(t, decided)
	require.Equal(t, memory.VerdictAccept, d)
}

func TestReviewQueue_HiddenRowsNeverStarveVisibleOnes(t *testing.T) {
	h, s := newTestHandler(t)
	h.SetMemoryGate(&voter.Gate{Version: reviewTestVersion})
	router := testRouter(h)
	base := time.Now().UTC().Add(-time.Hour)
	// A hidden prefix longer than one raw page (internal domain rows are never
	// shown), then one visible held memory.
	for i := 0; i < reviewQueueRawPage+44; i++ {
		heldMemory(t, s, fmt.Sprintf("hidden-%04d", i), store.SyncAuditDomainPrefix+"peer",
			fmt.Sprintf("internal sync audit record number %d for the test", i), base.Add(time.Duration(i)*time.Millisecond))
	}
	heldMemory(t, s, "visible", "general-notes", "The quarterly count was around two hundred units.", base.Add(time.Minute))

	resp := reviewQueue(t, router, "?limit=100")
	require.Equal(t, []string{"visible"}, itemIDs(resp), "a hidden prefix must not consume the visible page")
	require.NotContains(t, resp, "next_cursor", "the source was exhausted")
}

func TestReviewQueue_ContinuationReachesPastTheScanBudget(t *testing.T) {
	h, s := newTestHandler(t)
	h.SetMemoryGate(&voter.Gate{Version: reviewTestVersion})
	router := testRouter(h)
	base := time.Now().UTC().Add(-time.Hour)
	for i := 0; i < reviewQueueScanBudget+10; i++ {
		heldMemory(t, s, fmt.Sprintf("hidden-%05d", i), store.SyncAuditDomainPrefix+"peer",
			fmt.Sprintf("internal sync audit record number %d for the test", i), base.Add(time.Duration(i)*time.Millisecond))
	}
	heldMemory(t, s, "visible", "general-notes", "The quarterly count was around two hundred units.", base.Add(time.Hour))

	first := reviewQueue(t, router, "?limit=100")
	require.Empty(t, itemIDs(first), "one request scans at most the budget")
	cursor, ok := first["next_cursor"].(string)
	require.True(t, ok, "an unfinished scan returns a continuation cursor")

	second := reviewQueue(t, router, "?limit=100&cursor="+cursor)
	require.Equal(t, []string{"visible"}, itemIDs(second), "the continuation reaches the visible held memory")
}

func TestReviewQueue_CursorSurvivesDecisions(t *testing.T) {
	for _, hidden := range []int{0, reviewQueueRawPage + 1} {
		t.Run(fmt.Sprintf("hidden=%d", hidden), func(t *testing.T) {
			f := newAppV23ProjectionRouteFixture(t, false)
			f.handler.SetMemoryGate(&voter.Gate{Version: reviewTestVersion})
			for i := 0; i < hidden; i++ {
				id := fmt.Sprintf("hidden-%03d", i)
				heldMemory(t, f.sql, id, store.SyncAuditDomainPrefix+"peer", "Internal audit "+id, time.Now().UTC())
				publishAppV23DashboardRecord(t, f.sql, f.badger, id, uint8(store.ClearanceInternal), true)
			}
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("held-%03d", i)
				heldMemory(t, f.sql, id, "notes", "A lasting fact about "+id, time.Now().UTC())
				publishAppV23DashboardRecord(t, f.sql, f.badger, id, uint8(store.ClearanceInternal), true)
			}
			primeAppV23ProjectionSnapshot(t, f)
			first := requestLocalProjectionRoute(t, f, "/v1/dashboard/memory/review-queue?limit=50")
			require.Equal(t, http.StatusOK, first.Code, first.Body.String())
			var page map[string]any
			require.NoError(t, json.Unmarshal(first.Body.Bytes(), &page))
			ids := itemIDs(page)
			require.Len(t, ids, 50)
			cursor := page["next_cursor"].(string)
			for _, id := range ids {
				req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/memory/"+id+"/review", bytes.NewBufferString(`{"decision":"accept"}`))
				markLocalCEREBRUM(f.handler, req)
				rr := httptest.NewRecorder()
				f.router.ServeHTTP(rr, req)
				require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			}
			second := requestLocalProjectionRoute(t, f, "/v1/dashboard/memory/review-queue?limit=50&cursor="+cursor)
			require.Equal(t, http.StatusOK, second.Code, second.Body.String())

			var next map[string]any
			require.NoError(t, json.Unmarshal(second.Body.Bytes(), &next))
			// A fresh request independently identifies all remaining records.
			refresh := requestLocalProjectionRoute(t, f, "/v1/dashboard/memory/review-queue?limit=50")
			var fresh map[string]any
			require.NoError(t, json.Unmarshal(refresh.Body.Bytes(), &fresh))
			require.Len(t, itemIDs(fresh), 50)
			require.Equal(t, itemIDs(fresh), itemIDs(next), "deciding the previous page must not skip remaining held memories")
		})
	}
}

func TestReviewQueue_RejectsInvalidAndStaleCursors(t *testing.T) {
	h, s := newTestHandler(t)
	h.SetMemoryGate(&voter.Gate{Version: reviewTestVersion})
	router := testRouter(h)
	for _, id := range []string{"a", "b"} {
		heldMemory(t, s, id, "notes", "A lasting fact about "+id, time.Now().UTC())
	}
	first := reviewQueue(t, router, "?limit=1")
	cursor := first["next_cursor"].(string)
	h.SetMemoryGate(&voter.Gate{Version: "new-version"})
	for _, tc := range []struct {
		cursor string
		want   int
	}{
		{"50", http.StatusBadRequest},
		{cursor, http.StatusConflict},
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/review-queue?cursor="+tc.cursor, nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		require.Equal(t, tc.want, rr.Code, rr.Body.String())
	}
}
