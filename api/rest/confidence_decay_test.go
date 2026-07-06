package rest

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// committedFact is a small seeding helper for the decay tests.
func committedFact(id, content string, conf float64, created time.Time) *memory.MemoryRecord {
	return &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "agent-x",
		Content:         content,
		ContentHash:     []byte(id),
		MemoryType:      memory.TypeFact,
		DomainTag:       "general",
		ConfidenceScore: conf,
		Status:          memory.StatusCommitted,
		CreatedAt:       created,
	}
}

func postJSON(t *testing.T, srv *Server, path string, body any) QueryMemoryResponse {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := signedRequest(t, http.MethodPost, path, raw)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	return resp
}

func resultsByID(resp QueryMemoryResponse) map[string]*MemoryResult {
	m := make(map[string]*MemoryResult, len(resp.Results))
	for _, r := range resp.Results {
		m[r.MemoryID] = r
	}
	return m
}

// TestQueryMemory_MinConfidenceFiltersOnDecayedNotStored is the core regression
// guard: min_confidence must be enforced against the DECAYED confidence (the value
// serialized in confidence_score), not the raw stored column. An aged memory whose
// stored confidence sits above the floor but whose decayed confidence has fallen
// below it must be dropped; a returned memory reports the decayed value in
// confidence_score and the stored value in initial_confidence. It also pins the
// wiring: the handler moves min_confidence onto the store's DecayFloor and zeroes
// the decay-blind stored-column MinConfidence.
func TestQueryMemory_MinConfidenceFiltersOnDecayedNotStored(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	agedCreated := time.Now().Add(-130 * 24 * time.Hour)
	memStore.memories["aged"] = committedFact("aged", "an old but once-confident fact", 0.95, agedCreated)
	memStore.memories["fresh"] = committedFact("fresh", "a current fact", 0.95, time.Now())

	agedDecayed := memory.ComputeConfidence(0.95, agedCreated, time.Now(), 0, "general")
	require.Less(t, agedDecayed, 0.70, "test setup: aged memory must decay below the floor (got %.4f)", agedDecayed)

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 10,
	})
	byID := resultsByID(resp)

	assert.NotContains(t, byID, "aged", "aged memory decayed below min_confidence and must be filtered out")
	require.Contains(t, byID, "fresh", "fresh memory is above the floor and must be returned")

	fresh := byID["fresh"]
	require.NotNil(t, fresh.InitialConfidence, "initial_confidence must be present for local memories")
	assert.InDelta(t, 0.95, *fresh.InitialConfidence, 1e-9, "initial_confidence must be the STORED value")
	assert.InDelta(t, memory.ComputeConfidence(0.95, fresh.CreatedAt, time.Now(), 0, "general"),
		fresh.ConfidenceScore, 1e-3, "confidence_score must be the DECAYED value")
	assert.GreaterOrEqual(t, fresh.ConfidenceScore, 0.70, "returned results must satisfy the min_confidence floor")

	// The floor must reach the store as a DECAYED floor, not the decay-blind
	// stored-column filter (guards a regression that forgets to zero MinConfidence).
	assert.InDelta(t, 0.70, memStore.lastQueryOpts.DecayFloor, 1e-9, "handler must set DecayFloor")
	assert.Zero(t, memStore.lastQueryOpts.MinConfidence, "handler must zero the stored-column MinConfidence")
	assert.False(t, memStore.lastQueryOpts.DecayNow.IsZero(), "handler must pin DecayNow for serialize-parity")
}

// TestQueryMemory_MinConfidenceKeepsCorroborationBoosted guards the other
// direction: a memory whose STORED confidence is below the floor but whose decayed
// confidence is lifted ABOVE it by the corroboration boost must be returned. The
// decay-blind stored-column filter used to drop exactly these.
func TestQueryMemory_MinConfidenceKeepsCorroborationBoosted(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	boosted := &memory.MemoryRecord{
		MemoryID: "boosted", SubmittingAgent: "agent-x", Content: "low stored, heavily corroborated",
		ContentHash: []byte("boosted"), MemoryType: memory.TypeInference, DomainTag: "general",
		ConfidenceScore: 0.65, Status: memory.StatusCommitted, CreatedAt: time.Now(),
	}
	memStore.memories["boosted"] = boosted
	memStore.corroborations["boosted"] = []*store.Corroboration{
		{MemoryID: "boosted", AgentID: "agent-a"},
		{MemoryID: "boosted", AgentID: "agent-b"},
	}

	boostedDecayed := memory.ComputeConfidence(0.65, boosted.CreatedAt, time.Now(), 2, "general")
	require.Greater(t, boostedDecayed, 0.70, "test setup: boost must lift decayed above the floor (got %.4f)", boostedDecayed)

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 10,
	})
	require.Len(t, resp.Results, 1, "boosted memory must survive the decayed floor despite a sub-floor stored value")
	got := resp.Results[0]
	assert.Equal(t, "boosted", got.MemoryID)
	require.NotNil(t, got.InitialConfidence)
	assert.InDelta(t, 0.65, *got.InitialConfidence, 1e-9, "initial_confidence is the stored (sub-floor) value")
	assert.GreaterOrEqual(t, got.ConfidenceScore, 0.70, "confidence_score is the boosted decayed value, above the floor")
	assert.Equal(t, 2, got.CorroborationCount)
}

// TestSearchMemory_MinConfidenceFiltersOnDecayed pins the same decayed-floor
// contract for POST /v1/memory/search (finding: search had no coverage).
func TestSearchMemory_MinConfidenceFiltersOnDecayed(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["aged"] = committedFact("aged", "an aged widget note", 0.95, time.Now().Add(-130*24*time.Hour))
	memStore.memories["fresh"] = committedFact("fresh", "a fresh widget note", 0.95, time.Now())

	resp := postJSON(t, srv, "/v1/memory/search", SearchMemoryRequest{Query: "widget", MinConfidence: 0.70, TopK: 10})
	byID := resultsByID(resp)
	assert.NotContains(t, byID, "aged", "aged memory below the decayed floor must be filtered from /search")
	require.Contains(t, byID, "fresh")
	for _, r := range resp.Results {
		assert.GreaterOrEqual(t, r.ConfidenceScore, 0.70, "every /search result must satisfy the floor")
	}
}

// TestHybridSearchMemory_MinConfidenceFiltersOnDecayed pins it for /v1/memory/hybrid.
func TestHybridSearchMemory_MinConfidenceFiltersOnDecayed(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["aged"] = committedFact("aged", "aged hybrid note", 0.95, time.Now().Add(-130*24*time.Hour))
	memStore.memories["fresh"] = committedFact("fresh", "fresh hybrid note", 0.95, time.Now())

	resp := postJSON(t, srv, "/v1/memory/hybrid", HybridSearchMemoryRequest{
		Query: "hybrid", Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 10,
	})
	byID := resultsByID(resp)
	assert.NotContains(t, byID, "aged", "aged memory below the decayed floor must be filtered from /hybrid")
	require.Contains(t, byID, "fresh")
	for _, r := range resp.Results {
		assert.GreaterOrEqual(t, r.ConfidenceScore, 0.70)
	}
}

// TestQueryMemory_MinConfidenceRefillsTopK proves the store drops sub-floor records
// BEFORE the top-K trim, so top_k is filled from qualifying records rather than
// truncated (the store-side fix for the over-fetch/under-fill regression).
func TestQueryMemory_MinConfidenceRefillsTopK(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["aged"] = committedFact("aged", "aged", 0.95, time.Now().Add(-130*24*time.Hour))
	memStore.memories["fresh"] = committedFact("fresh", "fresh", 0.95, time.Now())

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 1,
	})
	require.Len(t, resp.Results, 1, "top_k=1 must be filled by the one qualifying record, not eaten by the dropped aged one")
	assert.Equal(t, "fresh", resp.Results[0].MemoryID)
}

// TestQueryMemory_MinConfidenceDefaultTopK guards the floor-with-omitted-top_k
// shape: a qualifying memory must still come back (a regression here is a total
// recall outage for that request shape).
func TestQueryMemory_MinConfidenceDefaultTopK(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["fresh"] = committedFact("fresh", "current", 0.95, time.Now())

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, // no TopK
	})
	require.Len(t, resp.Results, 1, "a floored query without top_k must still return qualifying results")
	assert.Equal(t, "fresh", resp.Results[0].MemoryID)
}

// TestQueryMemory_NoMinConfidenceFastPathUnchanged pins the fast path: with no
// floor, aged decayed-low memories are still returned (confidence_score shows the
// decay, initial_confidence the stored value), the store is NOT asked to filter,
// and the caller's top_k reaches the store un-inflated.
func TestQueryMemory_NoMinConfidenceFastPathUnchanged(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	agedCreated := time.Now().Add(-130 * 24 * time.Hour)
	memStore.memories["aged"] = committedFact("aged", "aged", 0.95, agedCreated)
	memStore.memories["fresh"] = committedFact("fresh", "fresh", 0.95, time.Now())

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, TopK: 10, // no MinConfidence
	})
	byID := resultsByID(resp)
	require.Contains(t, byID, "aged", "with no floor, an aged decayed-low memory must still be returned")
	aged := byID["aged"]
	require.NotNil(t, aged.InitialConfidence)
	assert.InDelta(t, 0.95, *aged.InitialConfidence, 1e-9)
	assert.Less(t, aged.ConfidenceScore, 0.70, "confidence_score still reflects decay even without a floor")

	assert.Zero(t, memStore.lastQueryOpts.DecayFloor, "no floor => store must not be asked to decay-filter")
	assert.Equal(t, 10, memStore.lastQueryOpts.TopK, "caller's top_k must reach the store un-inflated")
}

// TestQueryMemory_OpenTaskNotDecayedOutByFloor pins that open tasks (which the
// domain model exempts from decay) are not silently dropped by min_confidence as
// they age, and serialize their non-decaying stored confidence.
func TestQueryMemory_OpenTaskNotDecayedOutByFloor(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["task"] = &memory.MemoryRecord{
		MemoryID: "task", SubmittingAgent: "agent-x", Content: "an aging open task",
		ContentHash: []byte("task"), MemoryType: memory.TypeTask, TaskStatus: memory.TaskStatusPlanned,
		DomainTag: "general", ConfidenceScore: 0.90, Status: memory.StatusCommitted,
		CreatedAt: time.Now().Add(-200 * 24 * time.Hour), // would decay a normal memory well below the floor
	}

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{
		Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 10,
	})
	require.Len(t, resp.Results, 1, "an open task must not be decayed out of a min_confidence recall")
	assert.Equal(t, "task", resp.Results[0].MemoryID)
	assert.InDelta(t, 0.90, resp.Results[0].ConfidenceScore, 1e-9, "open tasks report their non-decaying stored confidence")
}

// TestRecall_FailsClosedOnCorroborationErrorUnderFloor: if the serialization
// corroboration-count fetch fails while a floor is active, EACH recall handler must
// fail closed (500) rather than serialize a boost-less score that could dip below the
// floor the caller queried with. Without a floor it degrades gracefully (200). Tested
// on all three endpoints because the fail-closed guard is an independent copy in each.
func TestRecall_FailsClosedOnCorroborationErrorUnderFloor(t *testing.T) {
	cases := []struct {
		name, path     string
		floor, noFloor any
	}{
		{"query", "/v1/memory/query",
			QueryMemoryRequest{Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 10},
			QueryMemoryRequest{Embedding: []float32{0.1, 0.2, 0.3}, TopK: 10}},
		{"search", "/v1/memory/search",
			SearchMemoryRequest{Query: "current", MinConfidence: 0.70, TopK: 10},
			SearchMemoryRequest{Query: "current", TopK: 10}},
		{"hybrid", "/v1/memory/hybrid",
			HybridSearchMemoryRequest{Query: "current", Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, TopK: 10},
			HybridSearchMemoryRequest{Query: "current", Embedding: []float32{0.1, 0.2, 0.3}, TopK: 10}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, memStore, _ := newTestServer(t, "")
			memStore.memories["fresh"] = committedFact("fresh", "current fact", 0.95, time.Now())
			memStore.corrCountErr = errors.New("corroboration table unavailable")

			raw, _ := json.Marshal(tc.floor)
			req, _ := signedRequest(t, http.MethodPost, tc.path, raw)
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			assert.Equal(t, http.StatusInternalServerError, rr.Code, "%s must fail closed under a floor", tc.name)

			raw2, _ := json.Marshal(tc.noFloor)
			req2, _ := signedRequest(t, http.MethodPost, tc.path, raw2)
			rr2 := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr2, req2)
			assert.Equal(t, http.StatusOK, rr2.Code, "%s without a floor must degrade, not fail", tc.name)
		})
	}
}

// TestQueryMemory_InitialConfidenceZeroSerializes pins that a legitimate stored 0.0
// still emits initial_confidence (pointer, not an omitempty float that would drop it).
func TestQueryMemory_InitialConfidenceZeroSerializes(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["zero"] = committedFact("zero", "stored at zero", 0.0, time.Now())

	resp := postJSON(t, srv, "/v1/memory/query", QueryMemoryRequest{Embedding: []float32{0.1, 0.2, 0.3}, TopK: 10})
	byID := resultsByID(resp)
	require.Contains(t, byID, "zero")
	require.NotNil(t, byID["zero"].InitialConfidence, "a stored 0.0 must still serialize initial_confidence")
	assert.Equal(t, 0.0, *byID["zero"].InitialConfidence)
}

// TestFederatedRecall_DropsBelowFloorAndOmitsInitialConfidence pins both federation
// contracts introduced here: peer results below the floor are dropped on the
// requesting side (so the min_confidence guarantee holds across the bridge), and
// federated results omit initial_confidence (nil — only the peer's decayed value is
// known).
func TestFederatedRecall_DropsBelowFloorAndOmitsInitialConfidence(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local"] = committedFact("local", "local knowledge", 0.95, time.Now())

	fed := &fakeFederation{outcomes: []federation.PeerRecallOutcome{
		{ChainID: "chain-b", Results: []*federation.MemoryResult{
			{MemoryID: "remote-low", DomainTag: "general", ConfidenceScore: 0.50, Status: "committed", SourceChainID: "chain-b"},
			{MemoryID: "remote-ok", DomainTag: "general", ConfidenceScore: 0.85, Status: "committed", SourceChainID: "chain-b"},
		}},
	}}
	srv.SetFederation(fed)

	raw, _ := json.Marshal(QueryMemoryRequest{Embedding: []float32{0.1, 0.2, 0.3}, MinConfidence: 0.70, Federated: true, TopK: 10})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/query", raw)
	srv.SetNodeOperatorID(agentID)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	byID := resultsByID(resp)
	assert.NotContains(t, byID, "remote-low", "a federated result below the floor must be dropped on the requesting side")
	require.Contains(t, byID, "remote-ok", "a federated result above the floor must be merged")
	assert.Nil(t, byID["remote-ok"].InitialConfidence, "federated results omit initial_confidence")
}
