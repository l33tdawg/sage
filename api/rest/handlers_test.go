package rest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
)

// --- Mock stores -------------------------------------------------------------

type mockMemoryStore struct {
	memories       map[string]*memory.MemoryRecord
	votes          map[string][]*store.ValidationVote
	corroborations map[string][]*store.Corroboration
	pendingRecords []*memory.MemoryRecord
}

func newMockMemoryStore() *mockMemoryStore {
	return &mockMemoryStore{
		memories:       make(map[string]*memory.MemoryRecord),
		votes:          make(map[string][]*store.ValidationVote),
		corroborations: make(map[string][]*store.Corroboration),
	}
}

func (m *mockMemoryStore) InsertMemory(_ context.Context, record *memory.MemoryRecord) error {
	m.memories[record.MemoryID] = record
	return nil
}

func (m *mockMemoryStore) GetMemory(_ context.Context, memoryID string) (*memory.MemoryRecord, error) {
	rec, ok := m.memories[memoryID]
	if !ok {
		return nil, fmt.Errorf("memory not found: %s", memoryID)
	}
	return rec, nil
}

func (m *mockMemoryStore) UpdateStatus(_ context.Context, memoryID string, status memory.MemoryStatus, now time.Time) error {
	rec, ok := m.memories[memoryID]
	if !ok {
		return fmt.Errorf("memory not found: %s", memoryID)
	}
	rec.Status = status
	return nil
}

func (m *mockMemoryStore) QuerySimilar(_ context.Context, embedding []float32, opts store.QueryOptions) ([]*memory.MemoryRecord, error) {
	results := make([]*memory.MemoryRecord, 0, len(m.memories))
	for _, rec := range m.memories {
		results = append(results, rec)
	}
	if opts.TopK > 0 && len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	return results, nil
}

func (m *mockMemoryStore) SearchByText(_ context.Context, query string, opts store.QueryOptions) ([]*memory.MemoryRecord, error) {
	return nil, nil
}

func (m *mockMemoryStore) InsertTriples(_ context.Context, memoryID string, triples []memory.KnowledgeTriple) error {
	return nil
}

func (m *mockMemoryStore) InsertVote(_ context.Context, vote *store.ValidationVote) error {
	m.votes[vote.MemoryID] = append(m.votes[vote.MemoryID], vote)
	return nil
}

func (m *mockMemoryStore) GetVotes(_ context.Context, memoryID string) ([]*store.ValidationVote, error) {
	return m.votes[memoryID], nil
}

func (m *mockMemoryStore) InsertChallenge(_ context.Context, _ *store.ChallengeEntry) error {
	return nil
}

func (m *mockMemoryStore) InsertCorroboration(_ context.Context, corr *store.Corroboration) error {
	m.corroborations[corr.MemoryID] = append(m.corroborations[corr.MemoryID], corr)
	return nil
}

func (m *mockMemoryStore) GetCorroborations(_ context.Context, memoryID string) ([]*store.Corroboration, error) {
	return m.corroborations[memoryID], nil
}

func (m *mockMemoryStore) GetPendingByDomain(_ context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error) {
	return m.pendingRecords, nil
}

func (m *mockMemoryStore) ListMemories(_ context.Context, opts store.ListOptions) ([]*memory.MemoryRecord, int, error) {
	results := make([]*memory.MemoryRecord, 0, len(m.memories))
	for _, rec := range m.memories {
		results = append(results, rec)
	}
	return results, len(results), nil
}

func (m *mockMemoryStore) GetStats(_ context.Context) (*store.StoreStats, error) {
	return &store.StoreStats{TotalMemories: len(m.memories)}, nil
}

func (m *mockMemoryStore) GetTimeline(_ context.Context, from, to time.Time, domain string, bucket string) ([]store.TimelineBucket, error) {
	return nil, nil
}

func (m *mockMemoryStore) DeleteMemory(_ context.Context, memoryID string) error {
	if rec, ok := m.memories[memoryID]; ok {
		rec.Status = memory.StatusDeprecated
	}
	return nil
}

func (m *mockMemoryStore) UpdateDomainTag(_ context.Context, memoryID string, domain string) error {
	if rec, ok := m.memories[memoryID]; ok {
		rec.DomainTag = domain
	}
	return nil
}

func (m *mockMemoryStore) UpdateMemoryAgent(_ context.Context, _ string, _ string) error {
	return nil
}

func (m *mockMemoryStore) GetTagsBatch(_ context.Context, _ []string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (m *mockMemoryStore) UpdateTaskStatus(_ context.Context, memoryID string, taskStatus memory.TaskStatus) error {
	if rec, ok := m.memories[memoryID]; ok {
		rec.TaskStatus = taskStatus
		return nil
	}
	return fmt.Errorf("task not found: %s", memoryID)
}

func (m *mockMemoryStore) LinkMemories(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockMemoryStore) GetLinkedMemories(_ context.Context, _ string) ([]memory.MemoryLink, error) {
	return nil, nil
}

func (m *mockMemoryStore) GetOpenTasks(_ context.Context, _ string, _ string) ([]*memory.MemoryRecord, error) {
	var tasks []*memory.MemoryRecord
	for _, rec := range m.memories {
		if rec.MemoryType == memory.TypeTask && rec.IsOpenTask() {
			tasks = append(tasks, rec)
		}
	}
	return tasks, nil
}

func (m *mockMemoryStore) GetAllTasks(_ context.Context, _ string, _ int) ([]*memory.MemoryRecord, error) {
	var tasks []*memory.MemoryRecord
	for _, rec := range m.memories {
		if rec.MemoryType == memory.TypeTask {
			tasks = append(tasks, rec)
		}
	}
	return tasks, nil
}

func (m *mockMemoryStore) SetTags(_ context.Context, _ string, _ []string) error { return nil }
func (m *mockMemoryStore) GetTags(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (m *mockMemoryStore) ListAllTags(_ context.Context) ([]store.TagCount, error) {
	return nil, nil
}
func (m *mockMemoryStore) ListMemoriesByTag(_ context.Context, _ string, _, _ int) ([]*memory.MemoryRecord, int, error) {
	return nil, 0, nil
}
func (m *mockMemoryStore) FindByContentHash(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (m *mockMemoryStore) Close() error { return nil }

type mockScoreStore struct {
	scores map[string]*store.ValidatorScore
}

func newMockScoreStore() *mockScoreStore {
	return &mockScoreStore{
		scores: make(map[string]*store.ValidatorScore),
	}
}

func (m *mockScoreStore) GetScore(_ context.Context, validatorID string) (*store.ValidatorScore, error) {
	s, ok := m.scores[validatorID]
	if !ok {
		return nil, fmt.Errorf("validator score not found: %s", validatorID)
	}
	return s, nil
}

func (m *mockScoreStore) UpdateScore(_ context.Context, score *store.ValidatorScore) error {
	m.scores[score.ValidatorID] = score
	return nil
}

func (m *mockScoreStore) GetAllScores(_ context.Context) ([]*store.ValidatorScore, error) {
	result := make([]*store.ValidatorScore, 0, len(m.scores))
	for _, s := range m.scores {
		result = append(result, s)
	}
	return result, nil
}

func (m *mockScoreStore) InsertEpochScore(_ context.Context, epoch *store.EpochScore) error {
	return nil
}

// --- Test helpers ------------------------------------------------------------

// newTestServer creates a server with mock stores and a fake CometBFT RPC.
func newTestServer(t *testing.T, cometbftURL string) (*Server, *mockMemoryStore, *mockScoreStore) {
	t.Helper()
	memStore := newMockMemoryStore()
	scoreStore := newMockScoreStore()
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)
	logger := zerolog.Nop()

	srv := NewServer(cometbftURL, memStore, scoreStore, nil, health, logger, embedding.NewClient("", ""))
	return srv, memStore, scoreStore
}

// signedRequest creates an authenticated HTTP request.
func signedRequest(t *testing.T, method, path string, body []byte) (*http.Request, string) {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	// Sign with full path including query params — matches middleware verification.
	ts := time.Now().Unix()
	sig := auth.SignRequest(priv, method, path, body, ts)
	agentID := auth.PublicKeyToAgentID(pub)

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))

	return req, agentID
}

// --- Tests -------------------------------------------------------------------

func TestHealthEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")

	var resp map[string]interface{}
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "healthy", resp["status"])
}

func TestReadyEndpoint(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]interface{}
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "ready", resp["status"])
}

func TestSubmitMemory(t *testing.T) {
	// Set up a fake CometBFT RPC server that returns broadcast_tx_commit format.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{
				"check_tx":  map[string]interface{}{"code": 0, "log": ""},
				"tx_result": map[string]interface{}{"code": 0, "data": "", "log": "memory submitted"},
				"hash":      "ABCDEF1234567890",
				"height":    "1",
			},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{
		"content": "Test memory content",
		"memory_type": "fact",
		"domain_tag": "crypto",
		"confidence_score": 0.85
	}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusCreated, rr.Code)

	var resp SubmitMemoryResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.MemoryID)
	assert.Equal(t, "ABCDEF1234567890", resp.TxHash)
	assert.Equal(t, "proposed", resp.Status)

	// Note: memory is no longer stored directly by the REST handler.
	// It is written by ABCI Commit after consensus finalizes the block.
	// This test verifies the REST layer correctly broadcasts and returns.
}

func TestSubmitMemory_ValidationErrors(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	tests := []struct {
		name string
		body string
	}{
		{"empty content", `{"content":"","memory_type":"fact","domain_tag":"crypto","confidence_score":0.5}`},
		{"invalid type", `{"content":"test","memory_type":"invalid","domain_tag":"crypto","confidence_score":0.5}`},
		{"missing domain", `{"content":"test","memory_type":"fact","domain_tag":"","confidence_score":0.5}`},
		{"confidence > 1", `{"content":"test","memory_type":"fact","domain_tag":"crypto","confidence_score":1.5}`},
		{"confidence < 0", `{"content":"test","memory_type":"fact","domain_tag":"crypto","confidence_score":-0.1}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", []byte(tt.body))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Header().Get("Content-Type"), "application/problem+json")
		})
	}
}

func TestQueryMemory(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	// Pre-populate a memory.
	memStore.memories["test-id"] = &memory.MemoryRecord{
		MemoryID:        "test-id",
		SubmittingAgent: "agent-1",
		Content:         "Test content",
		ContentHash:     []byte{1, 2, 3},
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-24 * time.Hour),
	}

	embedding := make([]float32, 1536)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{
		Embedding: embedding,
		TopK:      10,
	})

	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/query", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp QueryMemoryResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, 1, resp.TotalCount)
	assert.Equal(t, "test-id", resp.Results[0].MemoryID)
}

func TestGetMemory(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	// Build the signed request first so we know the agent ID
	req, agentID := signedRequest(t, http.MethodGet, "/v1/memory/mem-123", nil)

	memStore.memories["mem-123"] = &memory.MemoryRecord{
		MemoryID:        "mem-123",
		SubmittingAgent: agentID, // Must match requesting agent for RBAC isolation
		Content:         "Detailed memory content",
		ContentHash:     []byte{0xAA, 0xBB},
		MemoryType:      memory.TypeObservation,
		DomainTag:       "vuln_intel",
		ConfidenceScore: 0.75,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp MemoryDetailResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "mem-123", resp.MemoryID)
	assert.Equal(t, "observation", resp.MemoryType)
	assert.Equal(t, hex.EncodeToString([]byte{0xAA, 0xBB}), resp.ContentHash)
}

func TestGetMemory_NotFound(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	req, _ := signedRequest(t, http.MethodGet, "/v1/memory/nonexistent", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/problem+json")
}

func TestVoteMemory(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{
				"code": 0,
				"hash": "VOTEHASH123",
				"log":  "",
			},
		})
	}))
	defer cometMock.Close()

	srv, memStore, _ := newTestServer(t, cometMock.URL)

	// Pre-populate a memory to vote on.
	memStore.memories["vote-target"] = &memory.MemoryRecord{
		MemoryID:        "vote-target",
		SubmittingAgent: "agent-1",
		Content:         "Memory to vote on",
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.8,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	body := []byte(`{"decision":"accept","rationale":"Looks correct"}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/vote-target/vote", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp VoteResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, "VOTEHASH123", resp.TxHash)
}

func TestVoteMemory_InvalidDecision(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	memStore.memories["vote-target"] = &memory.MemoryRecord{
		MemoryID: "vote-target",
		Status:   memory.StatusProposed,
	}

	body := []byte(`{"decision":"maybe"}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/vote-target/vote", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestForgetMemory_Success(t *testing.T) {
	var capturedTxHex string
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTxHex = r.URL.Query().Get("tx")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "FORGETHASH", "log": ""},
		})
	}))
	defer cometMock.Close()

	srv, memStore, _ := newTestServer(t, cometMock.URL)
	memStore.memories["target"] = &memory.MemoryRecord{
		MemoryID:   "target",
		MemoryType: memory.TypeObservation,
		DomainTag:  "general",
		Status:     memory.StatusCommitted,
		CreatedAt:  time.Now(),
	}

	body := []byte(`{"reason":"duplicate of target-2"}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/target/forget", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	var resp ForgetResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "FORGETHASH", resp.TxHash)
	assert.NotEmpty(t, capturedTxHex, "broadcast should have been invoked")
}

func TestForgetMemory_DefaultReasonWhenOmitted(t *testing.T) {
	// The endpoint accepts an empty body and substitutes a default reason —
	// unlike challenge which requires a non-empty reason.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "FORGETHASH2"},
		})
	}))
	defer cometMock.Close()

	srv, memStore, _ := newTestServer(t, cometMock.URL)
	memStore.memories["target"] = &memory.MemoryRecord{
		MemoryID: "target",
		Status:   memory.StatusCommitted,
	}

	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/target/forget", []byte(`{}`))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
}

func TestForgetMemory_NotFound(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/missing/forget", []byte(`{}`))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
}

func TestGetAgent(t *testing.T) {
	srv, _, scoreStore := newTestServer(t, "")

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	agentID := auth.PublicKeyToAgentID(pub)

	scoreStore.scores[agentID] = &store.ValidatorScore{
		ValidatorID:   agentID,
		CurrentWeight: 0.42,
		VoteCount:     10,
	}

	ts := time.Now().Unix()
	sig := auth.SignRequest(priv, http.MethodGet, "/v1/agent/me", nil, ts)

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me", nil)
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp AgentProfileResponse
	err = json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, agentID, resp.AgentID)
	assert.Equal(t, 0.42, resp.PoEWeight)
	assert.Equal(t, int64(10), resp.VoteCount)
}

func TestGetAgent_NoScore(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	req, agentID := signedRequest(t, http.MethodGet, "/v1/agent/me", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp AgentProfileResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Equal(t, agentID, resp.AgentID)
	assert.Equal(t, float64(0), resp.PoEWeight)
}

func TestRFC7807Errors(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	// Send request with empty body to submit endpoint.
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/problem+json")

	var problem map[string]interface{}
	err := json.Unmarshal(rr.Body.Bytes(), &problem)
	require.NoError(t, err)
	assert.Contains(t, problem, "type")
	assert.Contains(t, problem, "title")
	assert.Contains(t, problem, "status")
	assert.Contains(t, problem, "detail")
}

func TestGetPending(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	memStore.pendingRecords = []*memory.MemoryRecord{
		{
			MemoryID:        "pending-1",
			SubmittingAgent: "agent-1",
			Content:         "Pending memory 1",
			ContentHash:     []byte{1, 2, 3},
			MemoryType:      memory.TypeFact,
			DomainTag:       "crypto",
			ConfidenceScore: 0.7,
			Status:          memory.StatusProposed,
			CreatedAt:       time.Now(),
		},
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/validator/pending?domain_tag=crypto", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp PendingMemoriesResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Memories, 1)
	assert.Equal(t, "pending-1", resp.Memories[0].MemoryID)
}

func TestGetEpoch(t *testing.T) {
	srv, _, scoreStore := newTestServer(t, "")

	scoreStore.scores["validator-1"] = &store.ValidatorScore{
		ValidatorID:   "validator-1",
		CurrentWeight: 0.5,
		VoteCount:     25,
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/validator/epoch", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp EpochResponse
	err := json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Scores, 1)
}

// --- Mock AgentStore for domain access tests ---------------------------------

type mockAgentStore struct {
	agents map[string]*store.AgentEntry
}

func newMockAgentStore() *mockAgentStore {
	return &mockAgentStore{agents: make(map[string]*store.AgentEntry)}
}

func (m *mockAgentStore) GetAgent(_ context.Context, agentID string) (*store.AgentEntry, error) {
	a, ok := m.agents[agentID]
	if !ok {
		return nil, fmt.Errorf("agent not found: %s", agentID)
	}
	return a, nil
}

func (m *mockAgentStore) ListAgents(_ context.Context) ([]*store.AgentEntry, error) {
	return nil, nil
}

func (m *mockAgentStore) CreateAgent(_ context.Context, _ *store.AgentEntry) error { return nil }
func (m *mockAgentStore) UpdateAgent(_ context.Context, _ *store.AgentEntry) error { return nil }
func (m *mockAgentStore) RemoveAgent(_ context.Context, _ string) error            { return nil }
func (m *mockAgentStore) UpdateAgentStatus(_ context.Context, _, _ string) error    { return nil }
func (m *mockAgentStore) UpdateAgentLastSeen(_ context.Context, _ string, _ time.Time) error {
	return nil
}
func (m *mockAgentStore) BackfillFirstSeen(_ context.Context, _ string, _ time.Time) error {
	return nil
}
func (m *mockAgentStore) AcquireRedeployLock(_ context.Context, _, _ string, _ time.Duration) error {
	return nil
}
func (m *mockAgentStore) ReleaseRedeployLock(_ context.Context) error                  { return nil }
func (m *mockAgentStore) GetRedeployLock(_ context.Context) (*store.RedeploymentLock, error) {
	return nil, nil
}
func (m *mockAgentStore) InsertRedeployLog(_ context.Context, _ *store.RedeploymentLogEntry) error {
	return nil
}
func (m *mockAgentStore) GetRedeployLog(_ context.Context, _ string) ([]*store.RedeploymentLogEntry, error) {
	return nil, nil
}
func (m *mockAgentStore) UpdateRedeployLog(_ context.Context, _ int64, _, _ string) error {
	return nil
}
func (m *mockAgentStore) RotateAgentKey(_ context.Context, _ string) (string, []byte, error) {
	return "", nil, nil
}
func (m *mockAgentStore) ReassignMemories(_ context.Context, _, _ string) (int64, error) {
	return 0, nil
}
func (m *mockAgentStore) ListAgentTags(_ context.Context, _ string) ([]store.TagCount, error) {
	return nil, nil
}
func (m *mockAgentStore) ReassignMemoriesByTag(_ context.Context, _, _, _ string) (int64, error) {
	return 0, nil
}
func (m *mockAgentStore) ReassignMemoriesByDomain(_ context.Context, _, _, _ string) (int64, error) {
	return 0, nil
}
func (m *mockAgentStore) GetAgentByName(_ context.Context, name string) (*store.AgentEntry, error) {
	for _, a := range m.agents {
		if a.Name == name {
			return a, nil
		}
	}
	return nil, nil
}

// --- Domain Access Read Enforcement Tests ------------------------------------

func TestQueryMemory_ReadAccess_AdminBypasses(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	memStore.memories["m1"] = &memory.MemoryRecord{
		MemoryID:        "m1",
		SubmittingAgent: "agent-1",
		Content:         "secret stuff",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "restricted",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "restricted", TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Register agent as admin — should bypass domain check
	agentSt.agents[agentID] = &store.AgentEntry{
		AgentID:      agentID,
		Role:         "admin",
		DomainAccess: `[{"domain":"other","read":false,"write":false}]`,
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.TotalCount)
}

func TestQueryMemory_ReadAccess_ObserverAllowed(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	memStore.memories["m1"] = &memory.MemoryRecord{
		MemoryID:        "m1",
		SubmittingAgent: "agent-1",
		Content:         "observable",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "public",
		ConfidenceScore: 0.8,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "public", TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Observer with no domain_access — should be allowed to read
	agentSt.agents[agentID] = &store.AgentEntry{
		AgentID: agentID,
		Role:    "observer",
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestQueryMemory_ReadAccess_MemberAllowed(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	memStore.memories["m1"] = &memory.MemoryRecord{
		MemoryID:        "m1",
		SubmittingAgent: "agent-1",
		Content:         "allowed content",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "crypto", TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Member with read access to "crypto"
	agentSt.agents[agentID] = &store.AgentEntry{
		AgentID:      agentID,
		Role:         "member",
		DomainAccess: `[{"domain":"crypto","read":true,"write":false}]`,
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.TotalCount)
}

func TestQueryMemory_ReadAccess_MemberDenied(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "secret", TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Member with read:false on "secret"
	agentSt.agents[agentID] = &store.AgentEntry{
		AgentID:      agentID,
		Role:         "member",
		DomainAccess: `[{"domain":"secret","read":false,"write":false}]`,
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Contains(t, rr.Body.String(), "read access")
}

func TestQueryMemory_ReadAccess_MemberDomainNotInList(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "unlisted", TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Member with domain_access that doesn't include "unlisted" — deny (allowlist model)
	agentSt.agents[agentID] = &store.AgentEntry{
		AgentID:      agentID,
		Role:         "member",
		DomainAccess: `[{"domain":"crypto","read":true,"write":true}]`,
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Contains(t, rr.Body.String(), "read access")
}

func TestQueryMemory_ReadAccess_NoDomainAllowed(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	memStore.memories["m1"] = &memory.MemoryRecord{
		MemoryID:        "m1",
		SubmittingAgent: "agent-1",
		Content:         "cross domain",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	// Empty domain tag — cross-domain query, should be allowed even for restricted members
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	agentSt.agents[agentID] = &store.AgentEntry{
		AgentID:      agentID,
		Role:         "member",
		DomainAccess: `[{"domain":"crypto","read":false,"write":false}]`,
	}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	// Empty domain tag skips the check entirely
	assert.Equal(t, http.StatusOK, rr.Code)
}

// --- Agent Register Name Reconciliation Tests --------------------------------

func TestAgentRegister_Idempotent_ReturnsOnChainName(t *testing.T) {
	// When SQLite and on-chain names match, register returns the on-chain name.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "TX123"},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	// Set up BadgerStore with an on-chain agent
	badgerDir := t.TempDir()
	bs, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs

	// Set up mock AgentStore (SQLite) with the same name
	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	// Create a signed request
	body := []byte(`{"name":"claude-code/sage","boot_bio":"test","provider":"claude-code"}`)
	req, agentID := signedRequest(t, http.MethodPost, "/v1/agent/register", body)

	// Pre-register the agent on-chain with the same name
	require.NoError(t, bs.RegisterAgent(agentID, "claude-code/sage", "member", "test", "claude-code", "", 1))
	agentSt.agents[agentID] = &store.AgentEntry{AgentID: agentID, Name: "claude-code/sage"}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "already_registered", resp["status"])
	assert.Equal(t, "claude-code/sage", resp["name"])
	// on_chain_height is the block-height field on the already_registered
	// path; earlier versions mis-named it "registered_at" (causing SDK
	// pydantic validation errors because the model declared a string type
	// while the server sent an int). Guard against regression.
	_, hasLegacy := resp["registered_at"]
	assert.False(t, hasLegacy, "response must not use the legacy registered_at key")
	height, ok := resp["on_chain_height"]
	require.True(t, ok, "response must include on_chain_height")
	// JSON unmarshal gives float64 for numbers.
	assert.Equal(t, float64(1), height)
}

func TestAgentRegister_Reconcile_SQLiteNameDiffersFromOnChain(t *testing.T) {
	// When SQLite has a different name (admin renamed via GUI), the register
	// endpoint should return the SQLite name and fire a reconciliation tx.
	var broadcastCalled bool
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcastCalled = true
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "RECONCILE_TX"},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	badgerDir := t.TempDir()
	bs, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs

	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	body := []byte(`{"name":"claude-code/sage","boot_bio":"test","provider":"claude-code"}`)
	req, agentID := signedRequest(t, http.MethodPost, "/v1/agent/register", body)

	// On-chain: old auto-generated name
	require.NoError(t, bs.RegisterAgent(agentID, "claude-code/sage", "member", "test", "claude-code", "", 1))
	// SQLite: admin renamed to "My Coding Assistant"
	agentSt.agents[agentID] = &store.AgentEntry{AgentID: agentID, Name: "My Coding Assistant"}

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "already_registered", resp["status"])
	// Should return the SQLite (display) name, not the on-chain name
	assert.Equal(t, "My Coding Assistant", resp["name"])
	// Should have broadcast a reconciliation tx
	assert.True(t, broadcastCalled, "expected reconciliation tx to be broadcast")
}

func TestAgentRegister_Reconcile_NoAgentStore(t *testing.T) {
	// When agentStore is nil (no SQLite), reconciliation is skipped gracefully.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "TX123"},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	badgerDir := t.TempDir()
	bs, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs
	srv.agentStore = nil // Explicitly nil

	body := []byte(`{"name":"claude-code/sage","boot_bio":"test","provider":"claude-code"}`)
	req, agentID := signedRequest(t, http.MethodPost, "/v1/agent/register", body)

	require.NoError(t, bs.RegisterAgent(agentID, "old-auto-name", "member", "test", "claude-code", "", 1))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "already_registered", resp["status"])
	// Without agentStore, falls back to the on-chain name
	assert.Equal(t, "old-auto-name", resp["name"])
}
