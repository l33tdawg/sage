package rest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// rbacMockMemoryStore wraps mockMemoryStore and honors QueryOptions.SubmittingAgents
// + DomainTag filters. The base mock returns all memories regardless of filter,
// which hides RBAC-filter regressions. These tests need real filter behavior.
type rbacMockMemoryStore struct {
	*mockMemoryStore
}

func newRBACMockMemoryStore() *rbacMockMemoryStore {
	return &rbacMockMemoryStore{mockMemoryStore: newMockMemoryStore()}
}

func (m *rbacMockMemoryStore) QuerySimilar(_ context.Context, embedding []float32, opts store.QueryOptions) ([]*memory.MemoryRecord, error) {
	results := []*memory.MemoryRecord{}
	for _, rec := range m.memories {
		if opts.DomainTag != "" && rec.DomainTag != opts.DomainTag {
			continue
		}
		if len(opts.SubmittingAgents) > 0 {
			match := false
			for _, a := range opts.SubmittingAgents {
				if rec.SubmittingAgent == a {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		results = append(results, rec)
	}
	if opts.TopK > 0 && len(results) > opts.TopK {
		results = results[:opts.TopK]
	}
	return results, nil
}

func (m *rbacMockMemoryStore) ListMemories(_ context.Context, opts store.ListOptions) ([]*memory.MemoryRecord, int, error) {
	results := []*memory.MemoryRecord{}
	for _, rec := range m.memories {
		if opts.DomainTag != "" && rec.DomainTag != opts.DomainTag {
			continue
		}
		if len(opts.SubmittingAgents) > 0 {
			match := false
			for _, a := range opts.SubmittingAgents {
				if rec.SubmittingAgent == a {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		if opts.SubmittingAgent != "" && rec.SubmittingAgent != opts.SubmittingAgent {
			continue
		}
		results = append(results, rec)
	}
	return results, len(results), nil
}

// newRBACTestServer builds a Server with a real BadgerStore + a filter-honoring
// mock memory store. Returns the server, the stores, and the cleanup closure.
func newRBACTestServer(t *testing.T) (*Server, *rbacMockMemoryStore, *store.BadgerStore, *mockAgentStore) {
	t.Helper()
	srv, _, _ := newTestServer(t, "")

	memStore := newRBACMockMemoryStore()
	srv.store = memStore

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	agentSt := newMockAgentStore()
	srv.agentStore = agentSt

	return srv, memStore, bs, agentSt
}

// seedMemory inserts a committed memory into the mock store.
func seedMemory(t *testing.T, memStore *rbacMockMemoryStore, id, submitter, domain, content string) {
	t.Helper()
	memStore.memories[id] = &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: submitter,
		Content:         content,
		ContentHash:     []byte(id),
		MemoryType:      memory.TypeObservation,
		DomainTag:       domain,
		ConfidenceScore: 0.85,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}
}

// ---------------------------------------------------------------------------
// Bug 1: visible_agents = "*" must grant cross-agent visibility
// ---------------------------------------------------------------------------

func TestRBACBug1_VisibleAgentsWildcard_GrantsSeeAll(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	// Build a signed query so we know the caller's agent_id up-front.
	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "foo.bar", TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Pre-register the caller on-chain with visible_agents = "*"
	require.NoError(t, bs.RegisterAgent(callerID, "agent-a", "member", "", "test", "", 1))
	require.NoError(t, bs.SetAgentPermission(callerID, 1, "", "*", "", ""))

	// Another agent's memory in the same domain, no domain owner registered
	seedMemory(t, memStore, "m1", "other-agent", "foo.bar", "cross-agent memory")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.TotalCount, "caller with visible_agents=\"*\" must see other agent's memory")
}

func TestRBACBug1_NoWildcard_StaysIsolated(t *testing.T) {
	// Negative control: without visible_agents="*", the caller must NOT see other agents' memories.
	// Pre-condition: the domain must be registered — unregistered domains are treated as open
	// for backward compat (commit 8ccb1b5), so agent isolation only kicks in on owned domains.
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "foo.bar", TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Register the caller with no special visibility and no grants.
	require.NoError(t, bs.RegisterAgent(callerID, "agent-a", "member", "", "test", "", 1))

	// Owner agent registered + owns the domain (simulating what auto-register does on submit).
	ownerID := "0000000000000000000000000000000000000000000000000000000000000001"
	require.NoError(t, bs.RegisterAgent(ownerID, "owner", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterDomain("foo.bar", ownerID, "", 1))
	require.NoError(t, bs.SetAccessGrant("foo.bar", ownerID, 2, 0, ownerID))

	// Owner's memory in the domain — the caller must not see it without wildcard or grant.
	seedMemory(t, memStore, "m1", ownerID, "foo.bar", "private memory")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.TotalCount, "caller without visible_agents=\"*\" must not see owner's memory")
}

// ---------------------------------------------------------------------------
// Bug 2: domain-filtered queries must not be stricter than unfiltered
// An agent that wrote a memory to a domain must be able to read it back
// via a domain-filtered query.
// ---------------------------------------------------------------------------

func TestRBACBug2_WriterCanQueryOwnDomain(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "calibration.sqli", TopK: 10})
	req, writerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Writer is registered on-chain.
	require.NoError(t, bs.RegisterAgent(writerID, "writer", "member", "", "test", "", 1))

	// Simulate the side-effect of processMemorySubmit on a previously-unregistered domain:
	// domain is registered with writer as owner, AND writer has level-2 grant.
	require.NoError(t, bs.RegisterDomain("calibration.sqli", writerID, "", 2))
	require.NoError(t, bs.SetAccessGrant("calibration.sqli", writerID, 2, 0, writerID))

	// The writer's own memory in that domain.
	seedMemory(t, memStore, "m1", writerID, "calibration.sqli", "calibration fact")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "writer must not hit 403 on own domain; body=%s", rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.TotalCount, "writer must see own memory in domain-filtered query")
}

func TestRBACBug2_UnregisteredDomain_NoACLEnforcement(t *testing.T) {
	// Unowned domains should allow queries without 403 — no access policy means open visibility.
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "unowned.domain", TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	require.NoError(t, bs.RegisterAgent(callerID, "caller", "member", "", "test", "", 1))

	// Memory exists in unowned domain, submitted by caller.
	seedMemory(t, memStore, "m1", callerID, "unowned.domain", "free content")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "unowned domain must not return 403; body=%s", rr.Body.String())
}

// ---------------------------------------------------------------------------
// Bug 3: grant_access must provide BOTH auth AND visibility
// A grantee with a level-1 (read) grant on a domain should see the granter's
// memories in that domain, not just pass the auth gate.
// ---------------------------------------------------------------------------

func TestRBACBug3_GrantProvidesVisibility(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	// We need a known granter (A) and use the signed request's derived ID as the grantee (B).
	granterID := "0000000000000000000000000000000000000000000000000000000000000001"

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "calibration.sqli", TopK: 10})
	req, granteeID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Both agents registered.
	require.NoError(t, bs.RegisterAgent(granterID, "granter", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterAgent(granteeID, "grantee", "member", "", "test", "", 1))

	// Granter owns the domain and has full access.
	require.NoError(t, bs.RegisterDomain("calibration.sqli", granterID, "", 2))
	require.NoError(t, bs.SetAccessGrant("calibration.sqli", granterID, 2, 0, granterID))

	// Grantee has a read-level grant on the same domain.
	require.NoError(t, bs.SetAccessGrant("calibration.sqli", granteeID, 1, 0, granterID))

	// Granter's memory in the domain.
	seedMemory(t, memStore, "m1", granterID, "calibration.sqli", "shared knowledge")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.TotalCount, "grantee with level-1 grant must see granter's memory in the domain")
}

// TestRBACBug1_PUTPermissionEndpoint_AcceptsWildcard verifies the REST handler
// itself correctly builds and broadcasts a TxTypeAgentSetPermission with
// visible_agents="*". We can't run the full ABCI here so we intercept the
// broadcast via a CometBFT mock and inspect the tx body.
func TestRBACBug1_PUTPermissionEndpoint_AcceptsWildcard(t *testing.T) {
	// Capture the broadcast request so we can inspect the tx body.
	var capturedTxHex string
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTxHex = r.URL.Query().Get("tx")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "TX_WILDCARD"},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	// Build PUT request with only visible_agents="*" (matches the levelup SDK call).
	body := []byte(`{"visible_agents":"*"}`)
	targetID := "1111111111111111111111111111111111111111111111111111111111111111"
	req, _ := signedRequest(t, http.MethodPut, "/v1/agent/"+targetID+"/permission", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "PUT /permission must return 200; body=%s", rr.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "permissions_updated", resp["status"])
	assert.Equal(t, targetID, resp["agent_id"])

	// Broadcast was called with a tx body that carries the wildcard verbatim.
	require.NotEmpty(t, capturedTxHex, "broadcast should have been invoked")

	// Decode the captured tx and assert visible_agents is the bare "*".
	txBytes, err := hex.DecodeString(capturedTxHex[2:]) // strip 0x prefix
	require.NoError(t, err)
	parsed, err := tx.DecodeTx(txBytes)
	require.NoError(t, err)
	require.NotNil(t, parsed.AgentSetPermission, "tx must carry AgentSetPermission payload")
	assert.Equal(t, targetID, parsed.AgentSetPermission.AgentID)
	assert.Equal(t, "*", parsed.AgentSetPermission.VisibleAgents, "tx must carry the wildcard verbatim, not a JSON-array variant")
}

func TestRBACBug3_NoGrant_StaysIsolated(t *testing.T) {
	// Negative control: without a grant, the grantee must NOT see the granter's memories.
	// Current design returns 200 with 0 memories (checkDomainAccess is permissive when the
	// caller has no explicit DomainAccess list, so agent isolation filters results instead).
	srv, memStore, bs, _ := newRBACTestServer(t)

	granterID := "0000000000000000000000000000000000000000000000000000000000000001"

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "calibration.sqli", TopK: 10})
	req, outsiderID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	require.NoError(t, bs.RegisterAgent(granterID, "granter", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterAgent(outsiderID, "outsider", "member", "", "test", "", 1))

	require.NoError(t, bs.RegisterDomain("calibration.sqli", granterID, "", 2))
	require.NoError(t, bs.SetAccessGrant("calibration.sqli", granterID, 2, 0, granterID))

	// Outsider has NO grant. Granter has memory in the domain.
	seedMemory(t, memStore, "m1", granterID, "calibration.sqli", "secret")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.TotalCount, "outsider without grant must not see granter's memory")
}
