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
	var results []*memory.MemoryRecord
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

	srv := NewServer(cometbftURL, memStore, scoreStore, nil, health, logger)
	return srv, memStore, scoreStore
}

// signedRequest creates an authenticated HTTP request.
func signedRequest(t *testing.T, method, path string, body []byte) (*http.Request, string) {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	ts := time.Now().Unix()
	sig := auth.SignRequest(priv, body, ts)
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
	// Set up a fake CometBFT RPC server.
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{
				"code": 0,
				"hash": "ABCDEF1234567890",
				"log":  "",
			},
		})
	}))
	defer cometMock.Close()

	srv, memStore, _ := newTestServer(t, cometMock.URL)

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

	// Verify the memory was stored.
	assert.Len(t, memStore.memories, 1)
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

	memStore.memories["mem-123"] = &memory.MemoryRecord{
		MemoryID:        "mem-123",
		SubmittingAgent: "agent-1",
		Content:         "Detailed memory content",
		ContentHash:     []byte{0xAA, 0xBB},
		MemoryType:      memory.TypeObservation,
		DomainTag:       "vuln_intel",
		ConfidenceScore: 0.75,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	req, _ := signedRequest(t, http.MethodGet, "/v1/memory/mem-123", nil)
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
	sig := auth.SignRequest(priv, nil, ts)

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
