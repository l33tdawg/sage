package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/memory"
)

// fakeFederation implements FederationService with canned outcomes so the
// merge path is testable without TLS or a peer chain.
type fakeFederation struct {
	outcomes []federation.PeerRecallOutcome
	calls    int
	lastReq  *federation.QueryRequest
}

func (f *fakeFederation) FanOutRecall(_ context.Context, _ []string, qr *federation.QueryRequest) []federation.PeerRecallOutcome {
	f.calls++
	f.lastReq = qr
	return f.outcomes
}

func (f *fakeFederation) DeliverReceipts(context.Context, string, int64, int64) map[string]federation.DeliveryResult {
	return nil
}
func (f *fakeFederation) StoreRemoteCA(string, []byte) ([]byte, error) { return nil, errors.New("na") }
func (f *fakeFederation) PeerStatus(context.Context, string) (*federation.StatusResponse, error) {
	return nil, errors.New("na")
}
func (f *fakeFederation) LocalChainID() string { return "chain-local" }

func TestFederatedRecallMergesRemoteResults(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "local knowledge",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}

	fed := &fakeFederation{outcomes: []federation.PeerRecallOutcome{
		{
			ChainID: "chain-b",
			Results: []*federation.MemoryResult{{
				MemoryID:        "remote-1",
				SubmittingAgent: "deadbeef@chain-b",
				Content:         "remote knowledge",
				DomainTag:       "shared",
				ConfidenceScore: 0.8,
				Status:          "committed",
				CreatedAt:       time.Now().Add(-2 * time.Hour),
				SourceChainID:   "chain-b",
			}},
		},
		{ChainID: "chain-dead", Err: errors.New("peer unreachable")},
	}}
	srv.SetFederation(fed)

	body, _ := json.Marshal(HybridSearchMemoryRequest{Query: "knowledge", Embedding: []float32{0.1, 0.2}, Federated: true})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/hybrid", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	require.Equal(t, 1, fed.calls, "federation fan-out should run exactly once")
	assert.Equal(t, federation.ModeHybrid, fed.lastReq.Mode)
	assert.Equal(t, "knowledge", fed.lastReq.Query)

	// Local + remote merged; remote stamped with provenance.
	require.Equal(t, 2, resp.TotalCount)
	var remote *MemoryResult
	for _, r := range resp.Results {
		if r.MemoryID == "remote-1" {
			remote = r
		}
	}
	require.NotNil(t, remote, "remote result missing from merged response")
	assert.Equal(t, "chain-b", remote.SourceChainID)
	assert.Equal(t, "deadbeef@chain-b", remote.SubmittingAgent)

	// Failed peer disclosed, never silently dropped.
	require.NotNil(t, resp.Federation)
	assert.ElementsMatch(t, []string{"chain-b", "chain-dead"}, resp.Federation.Queried)
	assert.Equal(t, 1, resp.Federation.Merged)
	assert.Contains(t, resp.Federation.Errors["chain-dead"], "unreachable")
}

func TestRecallWithoutOptInSkipsFederation(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "purely local",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}
	fed := &fakeFederation{}
	srv.SetFederation(fed)

	body, _ := json.Marshal(SearchMemoryRequest{Query: "purely local"})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/search", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 0, fed.calls, "federation must not run without opt-in")
	assert.Nil(t, resp.Federation)
	for _, r := range resp.Results {
		assert.Empty(t, r.SourceChainID, "local results must carry no source_chain_id")
	}
}

func TestFederatedRecallWithoutTransportIsNoop(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "no transport wired",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}
	// No SetFederation: a federated=true request degrades to local-only.
	body, _ := json.Marshal(HybridSearchMemoryRequest{Query: "transport", Embedding: []float32{0.1, 0.2}, Federated: true})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/hybrid", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Nil(t, resp.Federation)
	assert.Equal(t, 1, resp.TotalCount)
}
