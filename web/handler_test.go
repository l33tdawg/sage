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

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

func newTestHandler(t *testing.T) (*DashboardHandler, *store.SQLiteStore) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	h := NewDashboardHandler(s)
	return h, s
}

func testRouter(h *DashboardHandler) *chi.Mux {
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	return r
}

func insertTestMemory(t *testing.T, s *store.SQLiteStore, id, domain string) {
	t.Helper()
	h := sha256.Sum256([]byte("content-" + id))
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "agent1",
		Content:         "content-" + id,
		ContentHash:     h[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       domain,
		ConfidenceScore: 0.85,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(context.Background(), rec))
}

func TestHandleListMemories(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	for i := 0; i < 5; i++ {
		insertTestMemory(t, s, fmt.Sprintf("m%d", i), "general")
	}

	req := httptest.NewRequest("GET", "/v1/dashboard/memory/list?limit=3", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, float64(5), resp["total"])
	memories := resp["memories"].([]any)
	assert.Len(t, memories, 3)
}

func TestHandleStats(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "security")
	insertTestMemory(t, s, "m2", "general")

	req := httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, float64(2), resp["total_memories"])
}

func TestHandleDeleteMemory(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	req := httptest.NewRequest("DELETE", "/v1/dashboard/memory/m1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "deleted", resp["status"])

	// Verify in store
	got, err := s.GetMemory(context.Background(), "m1")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusDeprecated, got.Status)
}

func TestHandleUpdateMemory(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	body, _ := json.Marshal(map[string]string{"domain": "security"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/memory/m1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	got, err := s.GetMemory(context.Background(), "m1")
	require.NoError(t, err)
	assert.Equal(t, "security", got.DomainTag)
}

func TestHandleUpdateMemory_MissingDomain(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/memory/m1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGraph(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")
	insertTestMemory(t, s, "m2", "general")
	insertTestMemory(t, s, "m3", "security")

	req := httptest.NewRequest("GET", "/v1/dashboard/memory/graph", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	nodes := resp["nodes"].([]any)
	assert.Len(t, nodes, 3)
	// Should have domain edges (2 general memories = 1 domain edge)
	edges := resp["edges"].([]any)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestHandleTimeline(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour).Format(time.RFC3339)
	to := now.Add(time.Hour).Format(time.RFC3339)
	url := fmt.Sprintf("/v1/dashboard/memory/timeline?from=%s&to=%s&bucket=hour", from, to)

	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Contains(t, resp, "buckets")
}
