package web

import (
	"bytes"
	"context"
	ed25519pkg "crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func newTestHandler(t *testing.T) (*DashboardHandler, *store.SQLiteStore) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	h := NewDashboardHandler(s, "test")
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

func insertTestTask(t *testing.T, s *store.SQLiteStore, id, domain, provider string) {
	t.Helper()
	h := sha256.Sum256([]byte("task-" + id))
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "author-agent",
		Content:         "[TASK] " + id,
		ContentHash:     h[:],
		MemoryType:      memory.TypeTask,
		DomainTag:       domain,
		Provider:        provider,
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		TaskStatus:      memory.TaskStatusPlanned,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(context.Background(), rec))
	require.NoError(t, s.UpdateMemoryClassification(context.Background(), id, store.ClearancePublic))
}

func signAgentGET(t *testing.T, req *http.Request, priv ed25519pkg.PrivateKey) string {
	return signAgentRequest(t, req, priv, nil)
}

func markLocalCEREBRUM(req *http.Request) {
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
}

func TestDashboardPresenceTracksLiveSSEClients(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)

	check := func(wantActive bool, wantClients int) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/ui/presence", nil)
		req.RemoteAddr = "127.0.0.1:54321"
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		var payload struct {
			Active  bool `json:"active"`
			Clients int  `json:"clients"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
		assert.Equal(t, wantActive, payload.Active)
		assert.Equal(t, wantClients, payload.Clients)
	}

	check(false, 0)
	ch := h.SSE.Subscribe()
	require.NotNil(t, ch)
	check(true, 1)
	h.SSE.Unsubscribe(ch)
	check(false, 0)

	remoteReq := httptest.NewRequest(http.MethodGet, "/ui/presence", nil)
	remoteReq.RemoteAddr = "192.0.2.10:54321"
	remoteW := httptest.NewRecorder()
	router.ServeHTTP(remoteW, remoteReq)
	assert.Equal(t, http.StatusNotFound, remoteW.Code)
}

func signAgentRequest(t *testing.T, req *http.Request, priv ed25519pkg.PrivateKey, body []byte) string {
	t.Helper()
	pub := priv.Public().(ed25519pkg.PublicKey)
	agentID := hex.EncodeToString(pub)
	ts := time.Now().Unix()
	nonce := make([]byte, 8)
	_, randErr := rand.Read(nonce)
	require.NoError(t, randErr)
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(priv, req.Method, req.URL.RequestURI(), body, ts, nonce)))
	return agentID
}

func TestDashboardAgentSignatureRejectsReplay(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	_, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)

	first := httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	signAgentGET(t, first, priv)
	second := httptest.NewRequest(http.MethodGet, first.URL.RequestURI(), nil)
	second.Header = first.Header.Clone()

	w := httptest.NewRecorder()
	router.ServeHTTP(w, first)
	require.NotEqual(t, http.StatusUnauthorized, w.Code, "first valid signature must pass authentication: %s", w.Body.String())
	w = httptest.NewRecorder()
	router.ServeHTTP(w, second)
	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
}

type transientClassificationStore struct {
	*store.SQLiteStore
	failNext atomic.Bool
}

func (s *transientClassificationStore) GetMemoryClassificationLocal(ctx context.Context, memoryID string) (int, error) {
	if s.failNext.CompareAndSwap(true, false) {
		return 0, fmt.Errorf("temporary classification lookup failure")
	}
	return s.SQLiteStore.GetMemoryClassificationLocal(ctx, memoryID)
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

func TestTaskAssignmentCrossProviderAppearsInBacklogAndInbox(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	ctx := context.Background()
	insertTestTask(t, s, "assigned-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, s.CreateAgent(ctx, &store.AgentEntry{
		AgentID: agentID, Name: "claude-code/tii-sage", RegisteredName: "claude-code/tii-sage",
		Provider: "claude-code", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/assigned-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var assigned struct {
		NotificationCreated bool `json:"notification_created"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &assigned))
	require.True(t, assigned.NotificationCreated)

	body = bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req = httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/assigned-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &assigned))
	require.False(t, assigned.NotificationCreated)

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?provider=claude-code", nil)
	require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var backlog struct {
		Tasks []struct {
			MemoryID string `json:"memory_id"`
			Assignee string `json:"assignee"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &backlog))
	require.Len(t, backlog.Tasks, 1)
	require.Equal(t, "assigned-task", backlog.Tasks[0].MemoryID)
	require.Equal(t, agentID, backlog.Tasks[0].Assignee)

	// A dashboard/session/local caller cannot forge X-Agent-ID and consume the
	// agent's notice. The subsequent signed read must still receive it.
	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	req.Header.Set("X-Agent-ID", agentID)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())

	for wantCount := 1; wantCount >= 0; wantCount-- {
		req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
		require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
		w = httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var inbox struct {
			Items []*store.AgentNotification `json:"items"`
			Count int                        `json:"count"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &inbox))
		require.Equal(t, wantCount, inbox.Count)
		if wantCount == 1 {
			require.Equal(t, "assigned-task", inbox.Items[0].TaskID)
		}
	}
}

func TestTaskBoardAllTrueRejectsSignedAgentCaller(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "public-board-task", "public-work", "codex")
	_, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true&limit=10000", nil)
	signAgentGET(t, req, priv)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "task backlog")
}

func TestTaskBoardAllTrueRequiresLocalHumanCEREBRUM(t *testing.T) {
	h, s := newTestHandler(t)
	boardHandler := http.HandlerFunc(h.handleGetTasks)
	insertTestTask(t, s, "board-task", "work", "codex")

	tests := []struct {
		name       string
		origin     string
		secFetch   string
		host       string
		wantStatus int
	}{
		{name: "same-origin browser", origin: "http://localhost:8080", secFetch: "same-origin", host: "localhost:8080", wantStatus: http.StatusOK},
		{name: "origin-less caller", host: "localhost:8080", wantStatus: http.StatusForbidden},
		{name: "cross-origin browser", origin: "https://evil.example", secFetch: "cross-site", host: "localhost:8080", wantStatus: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
			req.Host = tc.host
			if tc.name == "same-origin browser" {
				req.RemoteAddr = "127.0.0.1:54321"
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tc.secFetch)
			}
			w := httptest.NewRecorder()
			boardHandler.ServeHTTP(w, req)
			assert.Equal(t, tc.wantStatus, w.Code, w.Body.String())
		})
	}
}

func TestTaskOperatorSurfacesRejectAgentsAndUnauthenticatedLANCallers(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "guarded-task", "work", "codex")
	_, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)

	createBody := []byte(`{"content":"agent bypass","domain":"work"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/tasks", bytes.NewReader(createBody))
	req.Header.Set("Content-Type", "application/json")
	signAgentRequest(t, req, priv, createBody)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())

	req = httptest.NewRequest(http.MethodDelete, "/v1/dashboard/memory/guarded-task", nil)
	signAgentRequest(t, req, priv, nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	got, err := s.GetMemory(context.Background(), "guarded-task")
	require.NoError(t, err)
	require.NotEqual(t, memory.StatusDeprecated, got.Status)

	statusBody := []byte(`{"task_status":"done"}`)
	for _, req = range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks", nil),
		httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/guarded-task/status", bytes.NewReader(statusBody)),
	} {
		if req.Method == http.MethodPut {
			req.Header.Set("Content-Type", "application/json")
		}
		w = httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	}

	// Browser headers are not LAN authentication when encryption is disabled.
	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
	req.Header.Set("Origin", "http://192.168.1.10:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "192.168.1.10:8080"
	req.RemoteAddr = "192.168.1.20:54321"
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskBoardAllowsAuthenticatedEncryptedLANSession(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestTask(t, s, "lan-task", "work", "codex")
	h.Encrypted.Store(true)
	h.sessions.Store("valid-lan-session", time.Now().Add(time.Hour))
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
	req.Header.Set("Origin", "http://192.168.1.10:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "192.168.1.10:8080"
	req.RemoteAddr = "192.168.1.20:54321"
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "valid-lan-session"})
	w := httptest.NewRecorder()
	h.handleGetTasks(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
}

func TestCreateTaskConsensusFailureDoesNotInsertPhantom(t *testing.T) {
	h, s := newTestHandler(t)
	_, key, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.SigningKey = key
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/broadcast_tx_commit", r.URL.Path)
		_, _ = w.Write([]byte(`{"result":{"check_tx":{"code":0,"log":""},"tx_result":{"code":7,"log":"rejected"},"hash":"abc","height":"1"}}`))
	}))
	t.Cleanup(rpc.Close)
	h.CometBFTRPC = rpc.URL
	r := testRouter(h)
	body := []byte(`{"content":"must commit","domain":"work"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	stats, err := s.GetStats(context.Background())
	require.NoError(t, err)
	require.Zero(t, stats.TotalMemories, "rejected consensus task must not appear in the local board")
}

func TestCreateTaskStandaloneStoresProposedAuthoredTask(t *testing.T) {
	h, s := newTestHandler(t)
	h.SetEmbedder(&fakeEmbedder{name: "ollama", dimension: 3, ready: true, semantic: true})
	r := testRouter(h)
	body := []byte(`{"content":"standalone work","domain":"work"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var response map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	got, err := s.GetMemory(context.Background(), response["memory_id"])
	require.NoError(t, err)
	require.Equal(t, memory.StatusProposed, got.Status)
	require.Equal(t, "cerebrum", got.SubmittingAgent)
	require.Equal(t, memory.TaskStatusPlanned, got.TaskStatus)
	counts, err := s.CountMemoriesByProvider(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, counts["ollama:3"])
	require.Zero(t, counts[""], "a newly embedded dashboard task must not re-enter the repair queue")
}

func TestTaskAssignmentRejectsInactiveOrUnknownAgent(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "guarded-task", "work", "codex")
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "inactive-agent", Name: "inactive", RegisteredName: "inactive", Status: "inactive",
	}))

	for _, assignee := range []string{"missing-agent", "inactive-agent"} {
		body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, assignee))
		req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/guarded-task/assign", body)
		req.Header.Set("Content-Type", "application/json")
		markLocalCEREBRUM(req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	}
}

func TestTaskAssignmentRejectsSignedAgentCaller(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "operator-only-task", "work", "codex")
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "target-agent", Name: "target", RegisteredName: "target", Status: "active",
	}))

	pub, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	body := []byte(`{"assignee":"target-agent"}`)
	path := "/v1/dashboard/tasks/operator-only-task/assign"
	ts := time.Now().Unix()
	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", hex.EncodeToString(pub))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequest(priv, http.MethodPut, path, body, ts)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskAssignmentDoesNotBypassAgentDomainAllowlist(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "forbidden-domain-task", "work", "codex")
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "restricted-agent", Name: "restricted", RegisteredName: "restricted", Status: "active",
		DomainAccess: `[{"domain":"other","read":true,"write":true}]`,
	}))

	body := bytes.NewBufferString(`{"assignee":"restricted-agent"}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/forbidden-domain-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskNotificationRechecksRBACAfterAssignment(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "reclassified-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: agentID, Name: "agent-a", RegisteredName: "agent-a", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/reclassified-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, s.UpdateMemoryClassification(context.Background(), "reclassified-task", store.ClearanceConfidential))

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var inbox struct {
		Count int `json:"count"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &inbox))
	require.Zero(t, inbox.Count, "a notice must not reveal a task after its read access is revoked")
}

func TestTaskNotificationRejectsAgentDeactivatedAfterAssignment(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "deactivated-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: agentID, Name: "agent-a", RegisteredName: "agent-a", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/deactivated-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, s.UpdateAgentStatus(context.Background(), agentID, "inactive"))

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskNotificationTransientAuthorizationFailureDoesNotConsumeNotice(t *testing.T) {
	h, sqliteStore := newTestHandler(t)
	wrapped := &transientClassificationStore{SQLiteStore: sqliteStore}
	h.store = wrapped
	r := testRouter(h)
	insertTestTask(t, sqliteStore, "retry-auth-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, sqliteStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: agentID, Name: "agent-a", RegisteredName: "agent-a", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/retry-auth-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	wrapped.failNext.Store(true)
	for _, wantCount := range []int{0, 1} {
		req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
		signAgentGET(t, req, agentPriv)
		w = httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var inbox struct {
			Count int `json:"count"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &inbox))
		require.Equal(t, wantCount, inbox.Count)
	}
}

func TestAgentTaskStatusRequiresActiveCurrentOwnerAndCannotReopen(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "owned-task", "work", "codex")
	pubA, privA, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	pubB, privB, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentA := hex.EncodeToString(pubA)
	agentB := hex.EncodeToString(pubB)
	for id, name := range map[string]string{agentA: "agent-a", agentB: "agent-b"} {
		require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
			AgentID: id, Name: name, RegisteredName: name, Status: "active",
		}))
	}

	assignBody := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentA))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/owned-task/assign", assignBody)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	putStatus := func(priv ed25519pkg.PrivateKey, status string) *httptest.ResponseRecorder {
		t.Helper()
		body := []byte(fmt.Sprintf(`{"task_status":%q}`, status))
		path := "/v1/dashboard/tasks/owned-task/status"
		req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		signAgentRequest(t, req, priv, body)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		return rr
	}

	for _, status := range []string{"done", "dropped"} {
		rr := putStatus(privB, status)
		require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	}
	require.Equal(t, http.StatusForbidden, putStatus(privB, "planned").Code)

	require.NoError(t, s.UpdateAgentStatus(context.Background(), agentA, "inactive"))
	require.Equal(t, http.StatusForbidden, putStatus(privA, "done").Code)
	require.NoError(t, s.UpdateAgentStatus(context.Background(), agentA, "active"))
	require.Equal(t, http.StatusOK, putStatus(privA, "done").Code)
	require.Equal(t, http.StatusForbidden, putStatus(privA, "planned").Code)

	tasks, err := s.GetAllTasks(context.Background(), "work", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, memory.TaskStatusDone, tasks[0].TaskStatus)
	require.Empty(t, tasks[0].Assignee)
}

func TestHandleDeleteMemory(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	req := httptest.NewRequest("DELETE", "/v1/dashboard/memory/m1", nil)
	markLocalCEREBRUM(req)
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

	req := httptest.NewRequest("GET", "/v1/dashboard/memory/graph?status=proposed", nil)
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

func TestGraphMaxNodesDefaultsToDenseMRISample(t *testing.T) {
	t.Setenv("SAGE_GRAPH_MAX_NODES", "")
	assert.Equal(t, 2500, graphMaxNodes())

	t.Setenv("SAGE_GRAPH_MAX_NODES", "8000")
	assert.Equal(t, 8000, graphMaxNodes())
}

func TestHandleAuthCheck_NoAuth(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/auth/check", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["auth_required"])
	assert.Equal(t, true, resp["authenticated"])
}

func TestHandleAuthMiddleware_BlocksWithoutSession(t *testing.T) {
	h, _ := newTestHandler(t)
	// Simulate encryption enabled
	h.VaultKeyPath = "/tmp/fake-vault.key"
	h.Encrypted.Store(true)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "unauthorized", resp["error"])
	assert.Equal(t, true, resp["login_required"])
}

func TestHandleExport_AllMemories(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Insert more than the list endpoint's 200 cap
	for i := 0; i < 5; i++ {
		insertTestMemory(t, s, fmt.Sprintf("export-%d", i), "test-domain")
	}

	req := httptest.NewRequest("GET", "/v1/dashboard/export", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-ndjson")
	assert.Contains(t, w.Header().Get("Content-Disposition"), "sage-backup-")

	// Parse JSONL lines
	lines := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte("\n"))
	assert.Len(t, lines, 5)

	// Verify each line is valid JSON with expected fields
	for _, line := range lines {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		assert.NotEmpty(t, rec["memory_id"])
		assert.NotEmpty(t, rec["content"])
		assert.Equal(t, "test-domain", rec["domain_tag"])
		// Embeddings should be excluded
		assert.Nil(t, rec["embedding"])
	}
}

func TestHandleExport_PaginatesAcrossPages(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Insert more than the internal page size (500) to force multiple pages.
	totalRecords := 520
	for i := 0; i < totalRecords; i++ {
		insertTestMemory(t, s, fmt.Sprintf("page-%04d", i), "bulk-domain")
	}

	req := httptest.NewRequest("GET", "/v1/dashboard/export", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Parse JSONL lines — should have ALL records, not capped at 500
	lines := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte("\n"))
	assert.Len(t, lines, totalRecords)

	// Verify first and last records are valid JSON
	var first, last map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &first))
	require.NoError(t, json.Unmarshal(lines[totalRecords-1], &last))
	assert.NotEmpty(t, first["memory_id"])
	assert.NotEmpty(t, last["memory_id"])
	assert.Equal(t, "bulk-domain", first["domain_tag"])
	assert.Equal(t, "bulk-domain", last["domain_tag"])
}

func TestHandleExport_EmptyDB(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/export", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-ndjson")
	// Body should be empty (no records)
	assert.Empty(t, bytes.TrimSpace(w.Body.Bytes()))
}

func TestHandleAuthMiddleware_DynamicEncryption(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	// Encryption OFF — should allow access
	req := httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Enable encryption dynamically (simulates enabling via dashboard)
	h.Encrypted.Store(true)

	// Same request without cookie — should be blocked
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)

	// Disable encryption — should allow again
	h.Encrypted.Store(false)
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleLock_InvalidatesSession(t *testing.T) {
	h, _ := newTestHandler(t)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")
	r := testRouter(h)

	// Enable encryption and login
	body, _ := json.Marshal(map[string]string{"passphrase": "test-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Login to get session
	body, _ = json.Marshal(map[string]string{"passphrase": "test-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	cookies := w.Result().Cookies()
	require.NotEmpty(t, cookies)
	sessionCookie := cookies[0]

	// Verify session works
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Lock — invalidates the session
	req = httptest.NewRequest("POST", "/v1/dashboard/auth/lock", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	var lockResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &lockResp))
	assert.Equal(t, true, lockResp["locked"])

	// Same session cookie should now be rejected
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
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

// ---------------------------------------------------------------------------
// Network / Template tests
// ---------------------------------------------------------------------------

func TestHandleTemplates(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/network/templates", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Templates []struct {
			Name      string `json:"name"`
			Role      string `json:"role"`
			Bio       string `json:"bio"`
			Clearance int    `json:"clearance"`
			Avatar    string `json:"avatar"`
		} `json:"templates"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.GreaterOrEqual(t, len(resp.Templates), 2, "should have multiple templates")

	// Verify Coding Assistant template is present
	found := false
	for _, tmpl := range resp.Templates {
		if tmpl.Name == "Coding Assistant" {
			found = true
			assert.Equal(t, "member", tmpl.Role)
			assert.NotEmpty(t, tmpl.Bio)
			assert.Equal(t, 1, tmpl.Clearance)
			break
		}
	}
	assert.True(t, found, "Coding Assistant template should be present")
}

func TestHandleTemplatesContainsExpected(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/network/templates", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp struct {
		Templates []struct {
			Name string `json:"name"`
		} `json:"templates"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	names := make([]string, len(resp.Templates))
	for i, t := range resp.Templates {
		names[i] = t.Name
	}
	assert.Contains(t, names, "Coding Assistant")
	assert.Contains(t, names, "Voice Assistant")
	assert.Contains(t, names, "Research Agent")
	assert.Contains(t, names, "Custom")
}

// ---------------------------------------------------------------------------
// Unregistered agents test
// ---------------------------------------------------------------------------

func insertTestMemoryWithAgent(t *testing.T, s *store.SQLiteStore, id, domain, agentID string) {
	t.Helper()
	h := sha256.Sum256([]byte("content-" + id))
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: agentID,
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

func TestHandleUnregisteredAgents(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Create an agent in the dashboard
	agent := &store.AgentEntry{
		AgentID:   "registered-agent-id",
		Name:      "Registered Agent",
		Role:      "admin",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Insert memories from the registered agent
	insertTestMemoryWithAgent(t, s, "m1", "general", "registered-agent-id")

	// Insert memories from an unregistered agent
	insertTestMemoryWithAgent(t, s, "m2", "general", "orphan-agent-id")
	insertTestMemoryWithAgent(t, s, "m3", "security", "orphan-agent-id")

	req := httptest.NewRequest("GET", "/v1/dashboard/network/unregistered", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Unregistered []struct {
			AgentID     string `json:"agent_id"`
			MemoryCount int    `json:"memory_count"`
			ShortID     string `json:"short_id"`
		} `json:"unregistered"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Unregistered, 1, "should find exactly one unregistered agent")
	assert.Equal(t, "orphan-agent-id", resp.Unregistered[0].AgentID)
	assert.Equal(t, 2, resp.Unregistered[0].MemoryCount)
	assert.NotEmpty(t, resp.Unregistered[0].ShortID)
}

func TestHandleUnregisteredAgents_NoneOrphan(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Create an agent and memories from only that agent
	agent := &store.AgentEntry{
		AgentID:   "only-agent",
		Name:      "Only Agent",
		Role:      "admin",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))
	insertTestMemoryWithAgent(t, s, "m1", "general", "only-agent")

	req := httptest.NewRequest("GET", "/v1/dashboard/network/unregistered", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Unregistered []any `json:"unregistered"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Len(t, resp.Unregistered, 0, "no orphaned agents should be found")
}

// --- Agent Update Synchronous Broadcast Tests --------------------------------

func TestHandleUpdateAgent_SyncBroadcast_Success(t *testing.T) {
	// When CometBFT is available, name updates should broadcast synchronously
	// and the response should NOT contain on_chain_warning.
	var broadcastSeen bool
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcastSeen = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"result":{"code":0,"hash":"ABC123"}}`)
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, priv, _ := ed25519GenerateKey()
	h.SigningKey = priv
	r := testRouter(h)

	// Create an agent in SQLite
	agent := &store.AgentEntry{
		AgentID:   "agent-rename-1",
		Name:      "Old Name",
		Role:      "member",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Rename via PATCH
	body, _ := json.Marshal(map[string]string{"name": "New Display Name"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/network/agents/agent-rename-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "New Display Name", resp["name"])
	assert.Nil(t, resp["on_chain_warning"], "should not have warning on success")
	assert.True(t, broadcastSeen, "CometBFT broadcast should have been called")

	// Verify SQLite was also updated
	updated, err := s.GetAgent(context.Background(), "agent-rename-1")
	require.NoError(t, err)
	assert.Equal(t, "New Display Name", updated.Name)
}

func TestHandleUpdateAgent_SyncBroadcast_Failure_ReturnsWarning(t *testing.T) {
	// When CometBFT broadcast fails, the response should include on_chain_warning
	// but the SQLite update should still succeed (best-effort).
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate CometBFT being down
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, priv, _ := ed25519GenerateKey()
	h.SigningKey = priv
	r := testRouter(h)

	agent := &store.AgentEntry{
		AgentID:   "agent-rename-2",
		Name:      "Old Name",
		Role:      "member",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	body, _ := json.Marshal(map[string]string{"name": "Renamed Agent"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/network/agents/agent-rename-2", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "Renamed Agent", resp["name"])
	assert.NotNil(t, resp["on_chain_warning"], "should have warning when broadcast fails")

	// SQLite should still have the new name
	updated, err := s.GetAgent(context.Background(), "agent-rename-2")
	require.NoError(t, err)
	assert.Equal(t, "Renamed Agent", updated.Name)
}

func TestHandleUpdateAgent_NoCometBFT_NoWarning(t *testing.T) {
	// When CometBFT is not configured, no broadcast happens and no warning.
	h, s := newTestHandler(t)
	// CometBFTRPC and SigningKey left empty
	r := testRouter(h)

	agent := &store.AgentEntry{
		AgentID:   "agent-rename-3",
		Name:      "Old Name",
		Role:      "member",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	body, _ := json.Marshal(map[string]string{"name": "Renamed Without Consensus"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/network/agents/agent-rename-3", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "Renamed Without Consensus", resp["name"])
	assert.Nil(t, resp["on_chain_warning"])
}

func TestHandleUpdateAgent_PermissionsSignedByGenesisAdmin(t *testing.T) {
	var captured *tx.ParsedTx
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, decErr := hex.DecodeString(raw)
		require.NoError(t, decErr)
		captured, decErr = tx.DecodeTx(encoded)
		require.NoError(t, decErr)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"ABC123","height":"42"}}`)
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, validatorKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	adminPub, adminKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.SigningKey = validatorKey
	h.AdminSigningKey = adminKey
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-permission-1", Name: "Local Agent", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body, err := json.Marshal(map[string]any{
		"domain_access": `[{"domain":"research","read":true,"write":false}]`,
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-permission-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AgentSetPermission)
	assert.Equal(t, []byte(adminPub), captured.AgentPubKey, "permission tx must carry genesis admin proof")
	assert.NotEqual(t, validatorKey.Public(), captured.AgentPubKey, "validator key is not the RBAC admin")
}

func TestHandleUpdateAgent_AdminOverrideRejectsSignedAgent(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-override-target", Name: "Target", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))
	_, callerKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	body, err := json.Marshal(map[string]any{
		"domain_access": `[{"domain":"research","read":true,"write":false}]`,
		"admin_override": []map[string]any{{
			"domain": "research", "owner_id": strings.Repeat("a", 64), "owned_domain": "research", "level": 1,
		}},
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-override-target", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	signAgentRequest(t, req, callerKey, body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	after, err := s.GetAgent(context.Background(), "agent-override-target")
	require.NoError(t, err)
	assert.Empty(t, after.DomainAccess, "rejected agent override must not mutate local policy")
}

func TestHandleUpdateAgent_AdminOverrideRejectsNoOriginCaller(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-override-target", Name: "Target", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))
	body, err := json.Marshal(map[string]any{
		"admin_override": []map[string]any{{
			"domain": "research", "owner_id": strings.Repeat("a", 64), "owned_domain": "research", "level": 1,
		}},
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-override-target", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ed25519GenerateKey is a test helper wrapping crypto/ed25519.GenerateKey.
func ed25519GenerateKey() (ed25519PublicKey, ed25519PrivateKey, error) {
	return ed25519pkg.GenerateKey(nil)
}

// Type aliases to avoid import naming conflicts with sha256.
type ed25519PublicKey = ed25519pkg.PublicKey
type ed25519PrivateKey = ed25519pkg.PrivateKey
