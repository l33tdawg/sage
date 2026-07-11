package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

// ---------------------------------------------------------------------------
// Credential / authZ hardening (v10.1.1) on the broader read surface.
//   - /v1/agents + /v1/agent/{id} must never serialize claim_token (a one-time
//     credential exchangeable for the agent key seed), and must strip per-agent
//     ACL topology from non-privileged callers.
//   - /v1/pipe/{id} must only reveal a pipe payload to a party or operator/admin.
// ---------------------------------------------------------------------------

func agentReadRouterAs(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	if callerID != "" {
		r.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
			})
		})
	}
	r.Get("/v1/agents", s.handleListRegisteredAgents)
	r.Get("/v1/agent/{id}", s.handleGetRegisteredAgent)
	return r
}

func seedAgentWithSecrets(id string) *store.AgentEntry {
	return &store.AgentEntry{
		AgentID:       id,
		Name:          "blue",
		Role:          "member",
		Status:        "active",
		ClaimToken:    "CLAIMSECRET",
		DomainAccess:  `[{"domain":"red-internal","read":true}]`,
		VisibleAgents: "*",
		BundlePath:    "/Users/operator/.sage/bundles/" + id + "/sage-agent-blue.zip",
	}
}

// TestListRegisteredAgents_StripsCredentials: the unauthenticated /v1/agents
// must not leak claim_token or per-agent ACL topology.
func TestListRegisteredAgents_StripsCredentials(t *testing.T) {
	agentID := "00000000000000000000000000000000000000000000000000000000000000a1"
	mock := &mockAgentStore{agents: map[string]*store.AgentEntry{agentID: seedAgentWithSecrets(agentID)}}
	s := &Server{agentStore: mock, logger: zerolog.Nop()}

	rr := httptest.NewRecorder()
	agentReadRouterAs(s, "").ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/agents", nil))
	require.Equal(t, http.StatusOK, rr.Code)

	body := rr.Body.String()
	assert.NotContains(t, body, "CLAIMSECRET", "claim_token must never be exposed publicly")
	assert.NotContains(t, body, "red-internal", "domain_access topology must not be exposed publicly")
	assert.NotContains(t, body, ".sage/bundles", "bundle_path (server key-bundle layout) must not be exposed")

	var resp struct {
		Agents []store.AgentEntry `json:"agents"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.Agents, 1)
	assert.Empty(t, resp.Agents[0].ClaimToken)
	assert.Empty(t, resp.Agents[0].DomainAccess)
	assert.Empty(t, resp.Agents[0].VisibleAgents)
	assert.Empty(t, resp.Agents[0].BundlePath)
	assert.Equal(t, "blue", resp.Agents[0].Name, "non-sensitive fields stay")
}

// TestGetRegisteredAgent_NonPrivilegedStripped: a different (non-admin) caller
// gets claim_token AND ACL topology stripped.
func TestGetRegisteredAgent_NonPrivilegedStripped(t *testing.T) {
	agentID := "00000000000000000000000000000000000000000000000000000000000000a1"
	caller := "00000000000000000000000000000000000000000000000000000000000000b2"
	mock := &mockAgentStore{agents: map[string]*store.AgentEntry{
		agentID: seedAgentWithSecrets(agentID),
		caller:  {AgentID: caller, Role: "member"},
	}}
	s := &Server{agentStore: mock, logger: zerolog.Nop()}

	rr := httptest.NewRecorder()
	agentReadRouterAs(s, caller).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/agent/"+agentID, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	var a store.AgentEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &a))
	assert.Empty(t, a.ClaimToken, "claim_token always stripped")
	assert.Empty(t, a.DomainAccess, "ACL topology stripped from a non-privileged caller")
	assert.Empty(t, a.VisibleAgents)
}

// TestGetRegisteredAgent_SelfSeesACLButNotClaimToken: the agent itself sees its
// own ACL fields, but claim_token is STILL stripped (never needed via reads).
func TestGetRegisteredAgent_SelfSeesACLButNotClaimToken(t *testing.T) {
	agentID := "00000000000000000000000000000000000000000000000000000000000000a1"
	mock := &mockAgentStore{agents: map[string]*store.AgentEntry{agentID: seedAgentWithSecrets(agentID)}}
	s := &Server{agentStore: mock, logger: zerolog.Nop()}

	rr := httptest.NewRecorder()
	agentReadRouterAs(s, agentID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/agent/"+agentID, nil))
	require.Equal(t, http.StatusOK, rr.Code)

	var a store.AgentEntry
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &a))
	assert.Empty(t, a.ClaimToken, "claim_token stripped even for self")
	assert.Equal(t, `[{"domain":"red-internal","read":true}]`, a.DomainAccess, "self sees own ACL")
	assert.Equal(t, "*", a.VisibleAgents)
}

// --- pipe authorization -----------------------------------------------------

func newPipeServer(t *testing.T) (*Server, *store.SQLiteStore) {
	t.Helper()
	dir := t.TempDir()
	memStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(dir, "pipe.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = memStore.Close() })
	return &Server{store: memStore, agentStore: memStore, logger: zerolog.Nop()}, memStore
}

func pipeRouterAs(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Post("/v1/pipe/send", s.handlePipeSend)
	r.Get("/v1/pipe/inbox", s.handlePipeInbox)
	r.Get("/v1/pipe/{pipe_id}", s.handlePipeStatus)
	r.Put("/v1/pipe/{pipe_id}/claim", s.handlePipeClaim)
	r.Put("/v1/pipe/{pipe_id}/result", s.handlePipeResult)
	return r
}

func TestPipeStatus_Authorization(t *testing.T) {
	s, memStore := newPipeServer(t)
	from := "00000000000000000000000000000000000000000000000000000000000000f1"
	to := "00000000000000000000000000000000000000000000000000000000000000d2"
	require.NoError(t, memStore.InsertPipeline(context.Background(), &store.PipelineMessage{
		PipeID: "pipe-1", FromAgent: from, ToAgent: to, Intent: "ask", Payload: "PIPE PAYLOAD SECRET", Status: "pending",
	}))

	get := func(caller string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		pipeRouterAs(s, caller).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/pipe/pipe-1", nil))
		return rr
	}

	// Sender and recipient can read it.
	for _, party := range []string{from, to} {
		rr := get(party)
		require.Equal(t, http.StatusOK, rr.Code, "party must read the pipe")
		assert.Contains(t, rr.Body.String(), "PIPE PAYLOAD SECRET")
	}

	// An unrelated caller gets 404 and no payload.
	stranger := "00000000000000000000000000000000000000000000000000000000000000c3"
	other := get(stranger)
	require.Equal(t, http.StatusNotFound, other.Code, "non-party must not read the pipe")
	assert.NotContains(t, other.Body.String(), "PIPE PAYLOAD SECRET")

	// Anti-enumeration: requesting the SAME id "pipe-1" against a store where it
	// is absent must yield a byte-identical 404, so the exists-but-forbidden
	// response reveals no existence bit.
	s2, _ := newPipeServer(t) // fresh store, no pipe-1
	missing := httptest.NewRecorder()
	pipeRouterAs(s2, stranger).ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/v1/pipe/pipe-1", nil))
	require.Equal(t, http.StatusNotFound, missing.Code)
	assert.Equal(t, missing.Body.String(), other.Body.String(),
		"exists-but-forbidden and genuine-not-found 404 bodies must be identical for the same id")
}

func TestPipeClaimAndResult_Authorization(t *testing.T) {
	s, memStore := newPipeServer(t)
	from := "00000000000000000000000000000000000000000000000000000000000000f1"
	to := "00000000000000000000000000000000000000000000000000000000000000d2"
	stranger := "00000000000000000000000000000000000000000000000000000000000000c3"
	require.NoError(t, memStore.InsertPipeline(context.Background(), &store.PipelineMessage{
		PipeID: "pipe-claim-1", FromAgent: from, ToAgent: to, Intent: "ask", Payload: "PIPE PAYLOAD SECRET", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}))

	claim := func(caller string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		pipeRouterAs(s, caller).ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-claim-1/claim", nil))
		return rr
	}

	rr := claim(from)
	require.Equal(t, http.StatusNotFound, rr.Code, "sender may read results, but cannot claim recipient work")
	assert.NotContains(t, rr.Body.String(), "PIPE PAYLOAD SECRET")
	msg, err := memStore.GetPipeline(context.Background(), "pipe-claim-1")
	require.NoError(t, err)
	assert.Equal(t, "pending", msg.Status)
	assert.Empty(t, msg.ClaimedBy)

	rr = claim(stranger)
	require.Equal(t, http.StatusNotFound, rr.Code, "non-party cannot claim")
	msg, err = memStore.GetPipeline(context.Background(), "pipe-claim-1")
	require.NoError(t, err)
	assert.Equal(t, "pending", msg.Status)

	rr = claim(to)
	require.Equal(t, http.StatusOK, rr.Code)
	msg, err = memStore.GetPipeline(context.Background(), "pipe-claim-1")
	require.NoError(t, err)
	assert.Equal(t, "claimed", msg.Status)
	assert.Equal(t, to, msg.ClaimedBy)

	resultBody := strings.NewReader(`{"result":"forged"}`)
	rr = httptest.NewRecorder()
	pipeRouterAs(s, stranger).ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-claim-1/result", resultBody))
	require.Equal(t, http.StatusNotFound, rr.Code, "non-party cannot submit result or learn pipe exists")
	msg, err = memStore.GetPipeline(context.Background(), "pipe-claim-1")
	require.NoError(t, err)
	assert.Equal(t, "claimed", msg.Status)
	assert.Empty(t, msg.Result)
	assert.Empty(t, msg.JournalID)

	resultBody = strings.NewReader(`{"result":"sender forged"}`)
	rr = httptest.NewRecorder()
	pipeRouterAs(s, from).ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-claim-1/result", resultBody))
	require.Equal(t, http.StatusConflict, rr.Code, "sender can read but cannot complete another agent's claim")
	msg, err = memStore.GetPipeline(context.Background(), "pipe-claim-1")
	require.NoError(t, err)
	assert.Equal(t, "claimed", msg.Status)
	assert.Empty(t, msg.Result)
	assert.Empty(t, msg.JournalID, "failed result must not auto-journal")

	resultBody = strings.NewReader(`{"result":"done"}`)
	rr = httptest.NewRecorder()
	pipeRouterAs(s, to).ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/v1/pipe/pipe-claim-1/result", resultBody))
	require.Equal(t, http.StatusOK, rr.Code)
	msg, err = memStore.GetPipeline(context.Background(), "pipe-claim-1")
	require.NoError(t, err)
	assert.Equal(t, "completed", msg.Status)
	assert.Equal(t, "done", msg.Result)
	assert.NotEmpty(t, msg.JournalID)
}
