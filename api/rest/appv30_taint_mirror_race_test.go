package rest

import (
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

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// The wrapper injects only scheduling: the real SQLite implementation performs
// both the count and the NOT EXISTS query with the options latched by HTTP.
type taintMirrorRaceStore struct {
	*store.SQLiteStore
	beforeList func(store.ListOptions)
	queryCalls int
	sqlIDs     []string
	sqlTotal   int
}

func (s *taintMirrorRaceStore) ListMemories(
	ctx context.Context, opts store.ListOptions,
) ([]*memory.MemoryRecord, int, error) {
	s.queryCalls++
	if s.beforeList != nil {
		hook := s.beforeList
		s.beforeList = nil
		hook(opts)
	}
	rows, total, err := s.SQLiteStore.ListMemories(ctx, opts)
	s.sqlTotal = total
	for _, row := range rows {
		s.sqlIDs = append(s.sqlIDs, row.MemoryID)
	}
	return rows, total, err
}

func TestAppV30MemoryListTaintMirrorRevisionRaceDoesNotInventEmptySnapshot(t *testing.T) {
	ctx := context.Background()
	srv, canonical, readerID, ownerID, _ := setupAppV23RESTAccess(t)
	sqlite, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "mirror-race.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlite.Close()) })
	backend := &taintMirrorRaceStore{SQLiteStore: sqlite}
	srv.store = backend

	now := time.Now().UTC()
	a := appV23PagingRecord("mirror-race-a", ownerID, "owner.home", "memory a",
		memory.TypeObservation, memory.StatusCommitted, now)
	b := appV23PagingRecord("mirror-race-b", ownerID, "owner.home", "memory b",
		memory.TypeObservation, memory.StatusCommitted, now.Add(-time.Second))
	for _, record := range []*memory.MemoryRecord{a, b} {
		require.NoError(t, sqlite.InsertMemory(ctx, record))
		require.NoError(t, canonical.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	}
	apply := func(record *memory.MemoryRecord) string {
		t.Helper()
		decisionID := fmt.Sprintf("%x", sha256.Sum256([]byte("decision-"+record.MemoryID)))
		ref := tx.ParentRef{SourceChainID: "sage", MemoryID: record.MemoryID}
		copy(ref.ContentHash[:], record.ContentHash)
		created, applyErr := canonical.ApplyTaintDecision("sage", decisionID, ref,
			sha256.Sum256([]byte("payload-"+record.MemoryID)))
		require.NoError(t, applyErr)
		require.True(t, created)
		return decisionID
	}
	checkVisibility := func(aVisible, bVisible bool) {
		t.Helper()
		for _, tc := range []struct {
			record  *memory.MemoryRecord
			visible bool
		}{{a, aVisible}, {b, bVisible}} {
			disclosure, disclosureErr := srv.evaluateAppV23RecordDisclosure(readerID, tc.record, now)
			if tc.visible {
				require.NoError(t, disclosureErr)
				require.True(t, disclosure.Allowed)
			} else {
				require.ErrorIs(t, disclosureErr, store.ErrMemoryProjectionTainted)
			}
		}
		require.True(t, aVisible || bVisible, "each canonical snapshot has an eligible record")
	}

	bDecision := apply(b)
	checkVisibility(true, false)
	initialRevision, count, err := store.RebuildMemoryTaintMirror(ctx, canonical, sqlite, 3)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, canonical, sqlite)
	require.NoError(t, err)
	require.True(t, fresh)

	backend.beforeList = func(opts store.ListOptions) {
		t.Logf("HTTP latched ExcludeTainted=%t before the scheduling hook", opts.ExcludeTainted)
		changed, clearErr := canonical.ClearTaintDecision(bDecision)
		require.NoError(t, clearErr)
		require.True(t, changed)
		checkVisibility(true, true)
		apply(a)
		checkVisibility(false, true)
		current, revisionErr := canonical.TaintRevision()
		require.NoError(t, revisionErr)
		mirrored, revisionErr := sqlite.MemoryTaintMirrorRevision(ctx)
		require.NoError(t, revisionErr)
		require.Equal(t, initialRevision, mirrored)
		require.Greater(t, current, mirrored)
		t.Logf("after latch: canonical revision=%d mirror revision=%d; A denied, B eligible", current, mirrored)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=50&domain=owner.home", nil)
	req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
	out := httptest.NewRecorder()
	srv.handleListMemoriesAuth(out, req)
	require.Equal(t, http.StatusOK, out.Code, out.Body.String())
	var body struct {
		Memories   []*memory.MemoryRecord `json:"memories"`
		Total      int                    `json:"total"`
		HasMore    bool                   `json:"has_more"`
		TotalExact bool                   `json:"total_exact"`
	}
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &body))
	t.Logf("SQL diagnostics: calls=%d IDs=%v total=%d", backend.queryCalls, backend.sqlIDs, backend.sqlTotal)
	t.Logf("HTTP result: memories=%d total=%d has_more=%t total_exact=%t", len(body.Memories),
		body.Total, body.HasMore, body.TotalExact)
	require.Len(t, body.Memories, 1,
		"the completed release must leave B visible; an empty result represents no canonical snapshot")
	require.Equal(t, b.MemoryID, body.Memories[0].MemoryID)
	require.NotEqual(t, a.MemoryID, body.Memories[0].MemoryID, "currently tainted A must not be returned")
	require.Equal(t, 1, body.Total)
	require.True(t, body.TotalExact)
	require.False(t, body.HasMore)
}
