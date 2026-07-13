package rest

// HTTP MCP token-management endpoints.
//
// These endpoints sit under /v1/mcp/tokens and use ed25519 admin auth (NOT
// the bearer-token auth that gates the actual MCP transport). The flow is:
//
//   1. Operator (a Claude Code agent or human via dashboard) calls
//      POST /v1/mcp/tokens with name + the local node-operator agent_id. Server returns a one-shot
//      token string + token ID. Show ONCE — never readable again.
//   2. External MCP client (ChatGPT, Cursor, etc.) sends that token in
//      Authorization: Bearer <token> on every /v1/mcp/sse or
//      /v1/mcp/streamable request.
//   3. Server hashes the bearer, looks up by SHA-256 digest, sets the agent
//      identity in the request context for downstream tool handlers.
//   4. To rotate or revoke, call DELETE /v1/mcp/tokens/{id}.

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

// mcpTokenStore is the subset of SQLiteStore that the token endpoints use.
// Defined as an interface so handler tests can stub it without spinning up
// a SQLite DB.
type mcpTokenStore interface {
	IssueMCPToken(ctx context.Context, name, issuerID, legacyAgentID, provider string) (*store.MCPTokenIssue, error)
	ListMCPTokens(ctx context.Context) ([]*store.MCPToken, error)
	RevokeMCPToken(ctx context.Context, id string) error
}

// ConfigureMCPTokenIdentityRegistrar wires the shared token issuer to the
// standard AgentRegister broadcaster. OAuth and wizard issuance use the same
// SQLiteStore, so configuring it once covers all three entry points.
func (s *Server) ConfigureMCPTokenIdentityRegistrar(ts *store.SQLiteStore) {
	ts.SetMCPTokenIdentityRegistrar(func(ctx context.Context, pub ed25519.PublicKey, priv ed25519.PrivateKey, name, provider string) error {
		_, _, err := s.registerMintedAgentIdentity(ctx, pub, priv, name, provider)
		return err
	})
}

// MCPTokenIssueRequest is the JSON body for POST /v1/mcp/tokens.
type MCPTokenIssueRequest struct {
	Name    string `json:"name"`     // human label, e.g. "chatgpt-laptop"
	AgentID string `json:"agent_id"` // hex-encoded ed25519 pubkey of the agent identity to mint for
}

// MCPTokenIssueResponse is what the client sees ONCE and only once.
type MCPTokenIssueResponse struct {
	ID        string    `json:"id"` // public token ID, used for revoke
	Name      string    `json:"name"`
	AgentID   string    `json:"agent_id"`
	Token     string    `json:"token"` // base64url(32 random bytes) — only readable here
	CreatedAt time.Time `json:"created_at"`
	UseHint   string    `json:"use_hint"` // human pointer at how to use the token
}

// MCPTokenSummary mirrors store.MCPToken but never includes the token value.
type MCPTokenSummary struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	AgentID    string    `json:"agent_id"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsedAt time.Time `json:"last_used_at,omitempty"`
	RevokedAt  time.Time `json:"revoked_at,omitempty"`
}

// handleMCPTokenIssue mints a new bearer token. The plaintext token is shown
// exactly once in the response — we only persist its SHA-256 digest.
func (s *Server) handleMCPTokenIssue(w http.ResponseWriter, r *http.Request) {
	ts, ok := s.store.(mcpTokenStore)
	if !ok {
		writeJSONError(w, http.StatusServiceUnavailable, "mcp tokens unsupported on this backend")
		return
	}

	var req MCPTokenIssueRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.AgentID = strings.TrimSpace(req.AgentID)
	if req.AgentID == "" {
		writeJSONError(w, http.StatusBadRequest, "agent_id is required")
		return
	}
	// Sanity: agent_id should look like a hex-encoded ed25519 pubkey (64 chars).
	if len(req.AgentID) != 64 {
		writeJSONError(w, http.StatusBadRequest, "agent_id must be a 64-char hex-encoded ed25519 public key")
		return
	}
	if _, hexErr := hex.DecodeString(req.AgentID); hexErr != nil {
		writeJSONError(w, http.StatusBadRequest, "agent_id must be hex-encoded")
		return
	}
	if s.nodeOperatorID == "" {
		writeJSONError(w, http.StatusServiceUnavailable, "node operator identity unavailable — MCP tokens cannot be issued")
		return
	}

	// Token issuance is exact-node-operator-only. Allowing an arbitrary
	// self-signed, not-yet-registered caller to mint creates an unlimited Sybil
	// issuance oracle and bypasses the identity-keyed rate limiter.
	callerID := middleware.ContextAgentID(r.Context())
	if callerID == "" || callerID != s.nodeOperatorID {
		writeJSONError(w, http.StatusForbidden, "MCP token issuance requires the exact node operator identity")
		return
	}
	if req.AgentID != s.nodeOperatorID {
		writeJSONError(w, http.StatusBadRequest, "HTTP MCP tokens run as the local node operator; agent_id must match the node operator identity")
		return
	}

	issued, err := ts.IssueMCPToken(r.Context(), req.Name, callerID, s.nodeOperatorID, "mcp-token")
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to persist token: "+err.Error())
		return
	}

	resp := MCPTokenIssueResponse{
		ID:        issued.ID,
		Name:      req.Name,
		AgentID:   issued.AgentID,
		Token:     issued.Token,
		CreatedAt: issued.CreatedAt,
		UseHint:   "Set Authorization: Bearer <token> on requests to /v1/mcp/sse or /v1/mcp/streamable. Vault-backed tokens sign as their own on-chain identity. SAVE THIS TOKEN NOW — it is never shown again.",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// handleMCPTokenList returns issued tokens as summaries (no token values).
func (s *Server) handleMCPTokenList(w http.ResponseWriter, r *http.Request) {
	ts, ok := s.store.(mcpTokenStore)
	if !ok {
		writeJSONError(w, http.StatusServiceUnavailable, "mcp tokens unsupported on this backend")
		return
	}

	rows, err := ts.ListMCPTokens(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list tokens: "+err.Error())
		return
	}

	// Scope to the caller's own tokens unless operator/admin — a regular agent
	// must not enumerate every agent's token metadata.
	callerID := middleware.ContextAgentID(r.Context())
	privileged := s.callerIsOperatorOrAdmin(r.Context(), callerID)

	out := make([]MCPTokenSummary, 0, len(rows))
	for _, t := range rows {
		if !privileged && t.IssuerID != callerID {
			continue
		}
		out = append(out, MCPTokenSummary{
			ID:         t.ID,
			Name:       t.Name,
			AgentID:    t.AgentID,
			CreatedAt:  t.CreatedAt,
			LastUsedAt: t.LastUsedAt,
			RevokedAt:  t.RevokedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"tokens": out})
}

// handleMCPTokenRevoke marks a token as revoked. Idempotent.
func (s *Server) handleMCPTokenRevoke(w http.ResponseWriter, r *http.Request) {
	ts, ok := s.store.(mcpTokenStore)
	if !ok {
		writeJSONError(w, http.StatusServiceUnavailable, "mcp tokens unsupported on this backend")
		return
	}

	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "token id is required")
		return
	}

	// AuthZ: only the token's owning agent or an operator/admin may revoke it.
	callerID := middleware.ContextAgentID(r.Context())
	if !s.callerIsOperatorOrAdmin(r.Context(), callerID) {
		rows, listErr := ts.ListMCPTokens(r.Context())
		if listErr != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to verify token ownership")
			return
		}
		owned := false
		for _, t := range rows {
			if t.ID == id && t.IssuerID == callerID {
				owned = true
				break
			}
		}
		if !owned {
			writeJSONError(w, http.StatusForbidden, "may only revoke your own tokens unless operator/admin")
			return
		}
	}

	if err := ts.RevokeMCPToken(r.Context(), id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSONError(w, http.StatusNotFound, "token not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to revoke token: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// writeJSONError emits a {"error":"..."} JSON body with the given HTTP status.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
