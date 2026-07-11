package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

// tokenOperatorID is the node operator identity injected by newTokenServer's
// default router, so the token-mechanics tests run as a privileged caller.
const tokenOperatorID = "0000000000000000000000000000000000000000000000000000000000000001"

// newTokenServer wires a real SQLite store + Server for HTTP-level tests. The
// returned handler injects the node operator as the authenticated caller (the
// real ed25519 auth is exercised separately in handlers_test.go); the token
// authZ gate itself is covered by tokenRouterAs with non-operator callers.
func newTokenServer(t *testing.T) (*Server, http.Handler) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tokens.db")
	memStore, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = memStore.Close() })

	s := &Server{
		store:          memStore,
		agentStore:     memStore,
		logger:         zerolog.Nop(),
		nodeOperatorID: tokenOperatorID,
	}
	return s, tokenRouterAs(s, tokenOperatorID)
}

// tokenRouterAs mounts the token routes with callerID injected as the
// authenticated agent identity, so tests can act as the operator or any agent.
func tokenRouterAs(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Post("/v1/mcp/tokens", s.handleMCPTokenIssue)
	r.Get("/v1/mcp/tokens", s.handleMCPTokenList)
	r.Delete("/v1/mcp/tokens/{id}", s.handleMCPTokenRevoke)
	return r
}

func TestMCPTokenIssue_Success(t *testing.T) {
	_, h := newTokenServer(t)

	pubkey := tokenOperatorID
	body := []byte(`{"agent_id":"` + pubkey + `","name":"chatgpt"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code)

	var resp MCPTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp.ID)
	assert.NotEmpty(t, resp.Token, "token must be returned exactly once")
	assert.Equal(t, "chatgpt", resp.Name)
	assert.Equal(t, pubkey, resp.AgentID)
	assert.Contains(t, resp.UseHint, "Bearer")
}

func TestMCPTokenIssue_BadAgentID(t *testing.T) {
	_, h := newTokenServer(t)

	cases := []struct{ body, want string }{
		{`{"agent_id":""}`, "agent_id is required"},
		{`{"agent_id":"too-short"}`, "64-char hex"},
		{`{"agent_id":"` + strings.Repeat("z", 64) + `"}`, "hex-encoded"},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens", bytes.NewBufferString(c.body))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code, "body=%s", c.body)
		assert.Contains(t, rr.Body.String(), c.want, "body=%s", c.body)
	}
}

func TestMCPTokenList_EmptyThenPopulated(t *testing.T) {
	_, h := newTokenServer(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/tokens", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var listResp struct {
		Tokens []MCPTokenSummary `json:"tokens"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&listResp))
	assert.Empty(t, listResp.Tokens)

	// Issue one, then list again.
	pubkey := tokenOperatorID
	body := []byte(`{"agent_id":"` + pubkey + `","name":"cursor"}`)
	issueReq := httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens", bytes.NewReader(body))
	issueRR := httptest.NewRecorder()
	h.ServeHTTP(issueRR, issueReq)
	require.Equal(t, http.StatusCreated, issueRR.Code)

	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest(http.MethodGet, "/v1/mcp/tokens", nil))
	require.Equal(t, http.StatusOK, rr2.Code)
	var listResp2 struct {
		Tokens []MCPTokenSummary `json:"tokens"`
	}
	require.NoError(t, json.NewDecoder(rr2.Body).Decode(&listResp2))
	require.Len(t, listResp2.Tokens, 1)
	assert.Equal(t, "cursor", listResp2.Tokens[0].Name)
	assert.Equal(t, pubkey, listResp2.Tokens[0].AgentID)
}

func TestMCPTokenRevoke(t *testing.T) {
	_, h := newTokenServer(t)

	pubkey := tokenOperatorID
	body := []byte(`{"agent_id":"` + pubkey + `"}`)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens", bytes.NewReader(body)))
	require.Equal(t, http.StatusCreated, rr.Code)

	var resp MCPTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))

	// Revoke it.
	revokeRR := httptest.NewRecorder()
	h.ServeHTTP(revokeRR, httptest.NewRequest(http.MethodDelete, "/v1/mcp/tokens/"+resp.ID, nil))
	assert.Equal(t, http.StatusNoContent, revokeRR.Code)

	// List should now show it as revoked.
	listRR := httptest.NewRecorder()
	h.ServeHTTP(listRR, httptest.NewRequest(http.MethodGet, "/v1/mcp/tokens", nil))
	var listResp struct {
		Tokens []MCPTokenSummary `json:"tokens"`
	}
	require.NoError(t, json.NewDecoder(listRR.Body).Decode(&listResp))
	require.Len(t, listResp.Tokens, 1)
	assert.False(t, listResp.Tokens[0].RevokedAt.IsZero())
}

func TestMCPTokenRevoke_NotFound(t *testing.T) {
	_, h := newTokenServer(t)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/v1/mcp/tokens/no-such-id", nil))
	assert.Equal(t, http.StatusNotFound, rr.Code)
}

// --- AuthZ gate (v10.1.1) ---------------------------------------------------

// A bearer cannot honestly execute as an arbitrary agent because HTTP MCP
// holds only the node operator's signing key.
func TestMCPToken_NonOperatorSelfMintDenied(t *testing.T) {
	s, _ := newTokenServer(t)
	self := strings.Repeat("d", 64)
	h := tokenRouterAs(s, self)

	body := []byte(`{"agent_id":"` + self + `","name":"mine"}`)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens", bytes.NewReader(body)))
	assert.Equal(t, http.StatusBadRequest, rr.Code, "non-operator identity must not be mislabeled; body=%s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "node operator")
}

// TestMCPToken_MintForOtherDenied: a non-operator agent may NOT mint a token
// impersonating a different agent_id.
func TestMCPToken_MintForOtherDenied(t *testing.T) {
	s, _ := newTokenServer(t)
	caller := strings.Repeat("d", 64)
	other := strings.Repeat("e", 64)
	h := tokenRouterAs(s, caller)

	body := []byte(`{"agent_id":"` + other + `","name":"impersonation"}`)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens", bytes.NewReader(body)))
	assert.Equal(t, http.StatusBadRequest, rr.Code, "minting for another agent_id must be denied")
}

// TestMCPToken_ListScopedToCaller: a non-operator agent sees only its own
// tokens, not every agent's.
func TestMCPToken_ListScopedToCaller(t *testing.T) {
	s, opH := newTokenServer(t)
	agentA := strings.Repeat("a", 64)

	// Operator mints an operator-scoped token.
	rr := httptest.NewRecorder()
	opH.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens",
		bytes.NewReader([]byte(`{"agent_id":"`+tokenOperatorID+`","name":"t"}`))))
	require.Equal(t, http.StatusCreated, rr.Code)

	// agentA lists — it must not see the operator token.
	rr = httptest.NewRecorder()
	tokenRouterAs(s, agentA).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/mcp/tokens", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	var resp struct {
		Tokens []MCPTokenSummary `json:"tokens"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Empty(t, resp.Tokens, "non-operator must not see operator-scoped tokens")
}

// TestMCPToken_RevokeOtherDenied: a non-operator agent cannot revoke a token it
// does not own.
func TestMCPToken_RevokeOtherDenied(t *testing.T) {
	s, opH := newTokenServer(t)
	agentB := strings.Repeat("b", 64)

	// Operator mints a token for the only identity HTTP MCP can execute as.
	issueRR := httptest.NewRecorder()
	opH.ServeHTTP(issueRR, httptest.NewRequest(http.MethodPost, "/v1/mcp/tokens",
		bytes.NewReader([]byte(`{"agent_id":"`+tokenOperatorID+`","name":"t"}`))))
	require.Equal(t, http.StatusCreated, issueRR.Code)
	var issued MCPTokenIssueResponse
	require.NoError(t, json.NewDecoder(issueRR.Body).Decode(&issued))

	// agentB tries to revoke agentA's token.
	rr := httptest.NewRecorder()
	tokenRouterAs(s, agentB).ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/v1/mcp/tokens/"+issued.ID, nil))
	assert.Equal(t, http.StatusForbidden, rr.Code, "revoking another agent's token must be denied")
}
