package web

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	lineageParentUUID = "794f4a15-416e-4e32-920c-5406c86662aa"
	lineageChildUUID  = "0860f8fa-6031-48a7-8590-b8a70f4151dc"
	lineageOtherUUID  = "34f08f34-e68b-48db-a711-d53f7298db4c"
)

type failingLineageContentStore struct {
	store.MemoryStore
	lineage     store.MemoryLineageStore
	blockedID   string
	parentLoads int
}

// This wrapper intentionally exposes only the established MemoryStore surface.
type legacyLineageContentStore struct {
	store.MemoryStore
	blockedID   string
	parentLoads int
}

func (s *legacyLineageContentStore) GetMemory(ctx context.Context, id string) (*memory.MemoryRecord, error) {
	if id == s.blockedID {
		s.parentLoads++
		return nil, fmt.Errorf("legacy parent content unavailable")
	}
	return s.MemoryStore.GetMemory(ctx, id)
}

func (s *failingLineageContentStore) FindMemoryParent(ctx context.Context, pointer string) (*store.MemoryParent, error) {
	return s.lineage.FindMemoryParent(ctx, pointer)
}

func (s *failingLineageContentStore) GetMemory(ctx context.Context, id string) (*memory.MemoryRecord, error) {
	if id == s.blockedID {
		s.parentLoads++
		return nil, fmt.Errorf("cannot decrypt hidden parent %s: private diagnostic", id)
	}
	return s.MemoryStore.GetMemory(ctx, id)
}

func insertLineageMemory(t *testing.T, sqlStore *store.SQLiteStore, id, agent, content, domain, pointer string) *memory.MemoryRecord {
	t.Helper()
	hash := sha256.Sum256([]byte(content))
	record := &memory.MemoryRecord{
		MemoryID: id, SubmittingAgent: agent, Content: content, ContentHash: hash[:],
		MemoryType: memory.TypeFact, DomainTag: domain, ConfidenceScore: 0.9,
		Status: memory.StatusCommitted, ParentHash: pointer, CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, sqlStore.InsertMemory(context.Background(), record))
	return record
}

func requestLineageRelated(t *testing.T, h *DashboardHandler, id, agent string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/"+id+"/related", nil)
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("id", id)
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx)
	if agent != "" {
		ctx = context.WithValue(ctx, verifiedDashboardAgentKey{}, agent)
	}
	rec := httptest.NewRecorder()
	h.handleMemoryRelated(rec, req.WithContext(ctx))
	return rec
}

func lineageGraph(t *testing.T, h *DashboardHandler, domain string, seeAll bool, agents []string) ([]graphNode, []graphEdge) {
	t.Helper()
	body, err := h.computeGraphJSON(context.Background(), "committed", domain, 100, seeAll, agents)
	require.NoError(t, err)
	var graph struct {
		Nodes []graphNode `json:"nodes"`
		Edges []graphEdge `json:"edges"`
	}
	require.NoError(t, json.Unmarshal(body, &graph))
	return graph.Nodes, graph.Edges
}

func TestMemoryLineageUsesContentHashAndLegacyID(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "correction-content-hash", true: "legacy-memory-id"}[legacy], func(t *testing.T) {
			h, sqlStore := newTestHandler(t)
			parent := insertLineageMemory(t, sqlStore, lineageParentUUID, "author", "orbital telescope", "old-domain", "")
			pointer := hex.EncodeToString(parent.ContentHash)
			if legacy {
				pointer = parent.MemoryID
			}
			insertLineageMemory(t, sqlStore, lineageChildUUID, "author", "benthic mollusc", "new-domain", pointer)
			rec := requestLineageRelated(t, h, lineageChildUUID, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var related struct {
				Related []RelatedMemory `json:"related"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &related))
			require.Len(t, related.Related, 1)
			require.Equal(t, parent.MemoryID, related.Related[0].ID)
			require.Equal(t, "chain", related.Related[0].Relation)
			require.Equal(t, 6.0, related.Related[0].Score)
			nodes, edges := lineageGraph(t, h, "", true, nil)
			require.Len(t, nodes, 2)
			require.Contains(t, edges, graphEdge{Source: lineageChildUUID, Target: lineageParentUUID, Type: "parent"})
			stored, err := sqlStore.GetMemory(context.Background(), lineageChildUUID)
			require.NoError(t, err)
			require.Equal(t, pointer, stored.ParentHash, "serving resolution must not rewrite the persisted pointer")
		})
	}
}

func TestMemoryLineageAmbiguousHashFailsClosedOutsideGraphSample(t *testing.T) {
	h, sqlStore := newTestHandler(t)
	parent := insertLineageMemory(t, sqlStore, lineageParentUUID, "author", "orbital telescope", "selected", "")
	// A duplicate outside the graph's selected domain still makes the pointer
	// ambiguous: the renderer must not choose the one parent it happens to see.
	insertLineageMemory(t, sqlStore, lineageOtherUUID, "other", parent.Content, "elsewhere", "")
	insertLineageMemory(t, sqlStore, lineageChildUUID, "author", "benthic mollusc", "selected", hex.EncodeToString(parent.ContentHash))
	rec := requestLineageRelated(t, h, lineageChildUUID, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), `"relation":"chain"`)
	nodes, edges := lineageGraph(t, h, "selected", true, nil)
	require.Len(t, nodes, 2)
	for _, edge := range edges {
		require.NotEqual(t, "parent", edge.Type)
	}
}

func TestMemoryLineageFiltersHiddenAndInternalParents(t *testing.T) {
	for _, internal := range []bool{false, true} {
		t.Run(map[bool]string{false: "hidden-author", true: "internal-domain"}[internal], func(t *testing.T) {
			h, sqlStore := newSynapseTestHandler(t)
			require.NoError(t, h.BadgerStore.RegisterAgent("caller", "caller", "member", "", "test", "", 1))
			require.NoError(t, h.BadgerStore.RegisterAgent("hidden", "hidden", "member", "", "test", "", 1))
			parentAgent, parentDomain := "hidden", "private"
			if internal {
				parentAgent, parentDomain = "caller", store.SyncAuditDomainPrefix+"lineage"
			}
			parent := insertLineageMemory(t, sqlStore, lineageParentUUID, parentAgent, "classified original", parentDomain, "")
			insertLineageMemory(t, sqlStore, lineageChildUUID, "caller", "benthic mollusc", "public", hex.EncodeToString(parent.ContentHash))
			guard := &failingLineageContentStore{MemoryStore: sqlStore, lineage: sqlStore, blockedID: parent.MemoryID}
			h.store = guard
			rec := requestLineageRelated(t, h, lineageChildUUID, "caller")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.NotContains(t, rec.Body.String(), lineageParentUUID)
			require.NotContains(t, rec.Body.String(), parent.Content)
			nodes, edges := lineageGraph(t, h, "", false, []string{"caller"})
			require.Len(t, nodes, 1)
			require.Equal(t, lineageChildUUID, nodes[0].ID)
			require.Empty(t, edges)
			require.Zero(t, guard.parentLoads, "hidden/internal parent content must never be loaded for either lineage reader")
		})
	}
}

func TestMemoryLineageRejectsQuarantinedParentProjection(t *testing.T) {
	fixture := newAppV23ProjectionRouteFixture(t, false)
	parent := insertLineageMemory(t, fixture.sql, lineageParentUUID, "author", "orbital telescope", "old-domain", "")
	insertLineageMemory(t, fixture.sql, lineageChildUUID, "author", "benthic mollusc", "new-domain", hex.EncodeToString(parent.ContentHash))
	for _, id := range []string{lineageParentUUID, lineageChildUUID} {
		publishAppV23DashboardRecord(t, fixture.sql, fixture.badger, id, uint8(store.ClearanceInternal), true)
	}
	require.NoError(t, fixture.handler.AuditAppV23CanonicalMemoryProjection(context.Background()))
	// Leave the indexed hash untouched, so the parent lookup succeeds, but make
	// the serving record disagree with its canonical committed domain.
	require.NoError(t, fixture.sql.UpdateDomainTag(context.Background(), lineageParentUUID, "forged-domain"))
	rec := requestLocalProjectionRoute(t, fixture, "/v1/dashboard/memory/"+lineageChildUUID+"/related")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), lineageParentUUID)
	require.NotContains(t, rec.Body.String(), parent.Content)
	nodes, edges := lineageGraph(t, fixture.handler, "", true, nil)
	require.Len(t, nodes, 1)
	require.Equal(t, lineageChildUUID, nodes[0].ID)
	require.Empty(t, edges)
}

func TestMemoryLineageGraphOmitsValidParentOutsideRenderedSet(t *testing.T) {
	h, sqlStore := newTestHandler(t)
	parent := insertLineageMemory(t, sqlStore, lineageParentUUID, "author", "orbital telescope", "old-domain", "")
	insertLineageMemory(t, sqlStore, lineageChildUUID, "author", "benthic mollusc", "new-domain", hex.EncodeToString(parent.ContentHash))
	guard := &failingLineageContentStore{MemoryStore: sqlStore, lineage: sqlStore, blockedID: parent.MemoryID}
	h.store = guard
	nodes, edges := lineageGraph(t, h, "new-domain", true, nil)
	require.Len(t, nodes, 1)
	require.Equal(t, lineageChildUUID, nodes[0].ID)
	require.Empty(t, edges)
	require.Zero(t, guard.parentLoads, "graph must intersect metadata with rendered records before any parent content load")

	// A readable child must remain readable if an otherwise eligible related
	// parent cannot be loaded. The diagnostic must not disclose the parent ID.
	rec := requestLineageRelated(t, h, lineageChildUUID, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, 1, guard.parentLoads)
	require.NotContains(t, rec.Body.String(), lineageParentUUID)
	require.NotContains(t, rec.Body.String(), "private diagnostic")
}

func TestMemoryLineageGraphLegacyStoreReusesRenderedParent(t *testing.T) {
	h, sqlStore := newTestHandler(t)
	insertLineageMemory(t, sqlStore, lineageParentUUID, "author", "orbital telescope", "old-domain", "")
	insertLineageMemory(t, sqlStore, lineageChildUUID, "author", "benthic mollusc", "new-domain", lineageParentUUID)
	legacy := &legacyLineageContentStore{MemoryStore: sqlStore, blockedID: lineageParentUUID}
	h.store = legacy
	_, hasLookup := h.store.(store.MemoryLineageStore)
	require.False(t, hasLookup)
	nodes, edges := lineageGraph(t, h, "", true, nil)
	require.Len(t, nodes, 2)
	require.Contains(t, edges, graphEdge{Source: lineageChildUUID, Target: lineageParentUUID, Type: "parent"})
	require.Zero(t, legacy.parentLoads, "legacy graph edges reuse rendered records without calling GetMemory")
}
