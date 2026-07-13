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
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

// mcpTokenStore is the subset of SQLiteStore that the token endpoints use.
// Defined as an interface so handler tests can stub it without spinning up
// a SQLite DB.
type mcpTokenStore interface {
	InsertMCPToken(ctx context.Context, id, name, agentID, tokenSHA256 string) error
	InsertMCPTokenWithIdentity(ctx context.Context, id, name, agentID, tokenSHA256, tokenPubHex string, tokenPriv ed25519.PrivateKey) error
	DeleteMCPToken(ctx context.Context, id string) error
	ListMCPTokens(ctx context.Context) ([]*store.MCPToken, error)
	RevokeMCPToken(ctx context.Context, id string) error
	// VaultActive reports whether the store can seal a per-token signing key. When
	// false the endpoint issues a legacy keyless token (sign-as-operator).
	VaultActive() bool
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

	// KEYED issuance is possible only when the store can SEAL a per-token signing
	// key. Without an active vault we fall back to the legacy keyless model, whose
	// token can only run as the node operator.
	keyed := ts.VaultActive()

	// LEGACY keyless: the token signs as the node operator, so the requested
	// identity must BE the operator or the bearer label would misreport who acted.
	// Enforced up front (pre-v11.8 ordering) so keyless callers get this precise
	// error before the ownership check below.
	if !keyed && req.AgentID != s.nodeOperatorID {
		writeJSONError(w, http.StatusBadRequest, "HTTP MCP tokens run as the local node operator; agent_id must match the node operator identity")
		return
	}

	// AuthZ: a bearer token grants the holder an on-chain identity on the MCP
	// transport, so a caller may only mint a token for ITS OWN agent_id — unless
	// it is the node operator or an admin. Without this gate any registered agent
	// could mint a token impersonating any other agent.
	callerID := middleware.ContextAgentID(r.Context())
	if req.AgentID != callerID && !s.callerIsOperatorOrAdmin(r.Context(), callerID) {
		writeJSONError(w, http.StatusForbidden, "may only mint a token for your own agent_id unless operator/admin")
		return
	}

	// Generate 32 random bytes → base64url-encoded bearer secret. This is the
	// transport credential, kept distinct from the per-token SIGNING identity
	// below.
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to generate token")
		return
	}
	tokenStr := base64.RawURLEncoding.EncodeToString(raw)

	digest := sha256.Sum256([]byte(tokenStr))
	digestHex := hex.EncodeToString(digest[:])

	id := uuid.NewString()

	// KEYED path: mint a per-token ed25519 signing key so the token acts as its
	// OWN on-chain identity instead of collapsing to the node operator (the v11.8
	// per-token identity model). Key generation happens HERE, off-consensus —
	// never inside FinalizeBlock.
	if keyed {
		tokenPub, tokenPriv, genErr := ed25519.GenerateKey(rand.Reader)
		if genErr != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate signing identity")
			return
		}
		mintedID := hex.EncodeToString(tokenPub)
		if insErr := ts.InsertMCPTokenWithIdentity(r.Context(), id, req.Name, mintedID, digestHex, mintedID, tokenPriv); insErr != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to persist token: "+insErr.Error())
			return
		}

		// Register the minted identity on-chain SYNCHRONOUSLY but bounded. A slow
		// block must not hang issuance, so past the wait we return the token and a
		// background retry finishes. Only a DEFINITIVE (non-timeout) rejection
		// rolls back the freshly-inserted row so no half-provisioned keyed token
		// lingers.
		regCh := make(chan error, 1)
		go func() {
			_, _, rErr := s.registerMintedAgentIdentity(tokenPub, tokenPriv, req.Name, "mcp-token")
			regCh <- rErr
		}()
		select {
		case rErr := <-regCh:
			if rErr != nil {
				if isDefinitiveRegisterRejection(rErr) {
					_ = ts.DeleteMCPToken(r.Context(), id)
					writeJSONError(w, http.StatusBadGateway, "failed to register token identity on-chain: "+rErr.Error())
					return
				}
				// Transient (timeout/transport) — keep the token, finish async.
				s.retryRegisterMintedIdentity(tokenPub, tokenPriv, req.Name, "mcp-token")
			}
		case <-time.After(tokenIdentityRegisterWait):
			// Still pending after the bounded wait: return the token now and let a
			// background goroutine resolve the outstanding registration.
			go func() {
				if rErr := <-regCh; rErr != nil && !isDefinitiveRegisterRejection(rErr) {
					s.retryRegisterMintedIdentity(tokenPub, tokenPriv, req.Name, "mcp-token")
				}
			}()
		}

		resp := MCPTokenIssueResponse{
			ID:        id,
			Name:      req.Name,
			AgentID:   mintedID,
			Token:     tokenStr,
			CreatedAt: time.Now().UTC(),
			UseHint:   "Set Authorization: Bearer <token> on requests to /v1/mcp/sse or /v1/mcp/streamable. This token signs as its own on-chain identity. SAVE THIS TOKEN NOW — it is never shown again.",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// LEGACY keyless path: no vault to seal a signing key, so the token runs as
	// the node operator (identity already gated to the operator above).
	if err := ts.InsertMCPToken(r.Context(), id, req.Name, req.AgentID, digestHex); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to persist token: "+err.Error())
		return
	}

	resp := MCPTokenIssueResponse{
		ID:        id,
		Name:      req.Name,
		AgentID:   req.AgentID,
		Token:     tokenStr,
		CreatedAt: time.Now().UTC(),
		UseHint:   "Set Authorization: Bearer <token> on requests to /v1/mcp/sse or /v1/mcp/streamable. SAVE THIS TOKEN NOW — it is never shown again.",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

// isDefinitiveRegisterRejection reports whether a registration broadcast error
// is a hard consensus/mempool rejection (roll back the token) versus a transient
// timeout/transport failure (retry in the background). Only CheckTx /
// FinalizeBlock rejections are treated as definitive, so a token is never
// deleted on a mere slow block or connection blip.
func isDefinitiveRegisterRejection(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "rejected in CheckTx") || strings.Contains(msg, "rejected in FinalizeBlock")
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
		effectiveAgentID := s.nodeOperatorID
		if effectiveAgentID == "" {
			effectiveAgentID = t.AgentID
		}
		if !privileged && effectiveAgentID != callerID {
			continue
		}
		out = append(out, MCPTokenSummary{
			ID:         t.ID,
			Name:       t.Name,
			AgentID:    effectiveAgentID,
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
			effectiveAgentID := s.nodeOperatorID
			if effectiveAgentID == "" {
				effectiveAgentID = t.AgentID
			}
			if t.ID == id && effectiveAgentID == callerID {
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
