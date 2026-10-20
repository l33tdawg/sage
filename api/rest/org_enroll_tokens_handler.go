package rest

// Org enrollment-token management: the operator issuance surface for
// POST /v1/org/{org_id}/enroll (see org_enroll_handler.go).
//
//	POST   /v1/org/{org_id}/enroll-tokens       issue a single-use token (plaintext shown once)
//	GET    /v1/org/{org_id}/enroll-tokens       list the org's tokens (never the plaintext)
//	DELETE /v1/org/{org_id}/enroll-tokens/{id}  revoke a not-yet-redeemed token
//
// These are ed25519-signed admin calls. Authorization follows the app-v22
// organization-membership precedent: only a global admin may change who can
// join an org, and under app-v23 the outer appV23LocalAdminBoundary already
// confines that authority to this machine. The org must exist on chain — tokens
// are never minted for an org consensus does not recognise.
//
// The tokens are redeemed by the enroll endpoint, not here. Revoking blocks a
// node that has not yet redeemed; it never rewrites an enrollment that already
// committed (the orgpolicy outcome record is authoritative for those).

import (
	"context"
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

// orgEnrollTokenAdminStore is the subset of SQLiteStore the management
// endpoints use; an interface so handler tests can stub it, mirroring
// mcpTokenStore. It is resolved from s.store at request time, exactly as the
// mcp-token endpoints do.
type orgEnrollTokenAdminStore interface {
	IssueOrgEnrollmentToken(ctx context.Context, orgID, ownerID, nodeLabel, role, orgSignerPub, keyEpoch string) (*store.OrgEnrollmentTokenIssue, error)
	ListOrgEnrollmentTokens(ctx context.Context, orgID string) ([]*store.OrgEnrollmentToken, error)
	RevokeOrgEnrollmentToken(ctx context.Context, id string) error
}

// orgEnrollRoles are the org roles a token may admit a node as. "admin" is
// deliberately excluded: org administration is granted through the on-chain
// membership path, never by a bearer token.
var orgEnrollRoles = map[string]bool{"member": true, "observer": true}

// OrgEnrollTokenIssueRequest is the JSON body for POST /v1/org/{org_id}/enroll-tokens.
type OrgEnrollTokenIssueRequest struct {
	NodeLabel    string `json:"node_label"`          // operator label for the intended node
	Role         string `json:"role"`                // org role the node is admitted as ("member" if empty)
	OrgSignerPub string `json:"org_signer_pub"`      // hex ed25519 org policy signing key the first policy must verify under
	KeyEpoch     string `json:"key_epoch,omitempty"` // optional pinned key epoch of the first policy
}

// OrgEnrollTokenIssueResponse is shown ONCE; the plaintext is not recoverable.
type OrgEnrollTokenIssueResponse struct {
	ID        string    `json:"id"` // public token ID, used for revoke
	OrgID     string    `json:"org_id"`
	OwnerID   string    `json:"owner_id"`
	NodeLabel string    `json:"node_label"`
	Role      string    `json:"role"`
	Token     string    `json:"token"`
	IssuedAt  time.Time `json:"issued_at"`
	UseHint   string    `json:"use_hint"`
}

// OrgEnrollTokenSummary mirrors store.OrgEnrollmentToken; it never carries a token value.
type OrgEnrollTokenSummary struct {
	ID           string    `json:"id"`
	OrgID        string    `json:"org_id"`
	OwnerID      string    `json:"owner_id"`
	NodeLabel    string    `json:"node_label"`
	Role         string    `json:"role"`
	OrgSignerPub string    `json:"org_signer_pub"`
	KeyEpoch     string    `json:"key_epoch,omitempty"`
	IssuedAt     time.Time `json:"issued_at"`
	RevokedAt    time.Time `json:"revoked_at,omitempty"`
}

// orgEnrollTokenPreflight runs the checks shared by all three endpoints:
// backend support, global-admin authorization, and on-chain org existence.
// It writes the error response and returns ok=false on failure.
func (s *Server) orgEnrollTokenPreflight(w http.ResponseWriter, r *http.Request) (orgEnrollTokenAdminStore, string, bool) {
	ts, ok := s.store.(orgEnrollTokenAdminStore)
	if !ok {
		writeJSONError(w, http.StatusServiceUnavailable, "org enrollment tokens unsupported on this backend")
		return nil, "", false
	}
	if s.badgerStore == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "consensus state store unavailable")
		return nil, "", false
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return nil, "", false
	}
	if !s.callerIsGlobalAdmin(r) {
		writeJSONError(w, http.StatusForbidden, "org enrollment tokens require a global admin")
		return nil, "", false
	}
	if _, _, err := s.badgerStore.GetOrg(orgID); err != nil {
		writeJSONError(w, http.StatusNotFound, "org not found")
		return nil, "", false
	}
	return ts, orgID, true
}

// handleOrgEnrollTokenIssue mints a single-use enrollment token for a named node.
func (s *Server) handleOrgEnrollTokenIssue(w http.ResponseWriter, r *http.Request) {
	ts, orgID, ok := s.orgEnrollTokenPreflight(w, r)
	if !ok {
		return
	}

	var req OrgEnrollTokenIssueRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.NodeLabel = strings.TrimSpace(req.NodeLabel)
	req.Role = strings.TrimSpace(req.Role)
	req.OrgSignerPub = strings.TrimSpace(req.OrgSignerPub)
	req.KeyEpoch = strings.TrimSpace(req.KeyEpoch)
	if req.Role == "" {
		req.Role = "member"
	}
	if req.NodeLabel == "" {
		writeJSONError(w, http.StatusBadRequest, "node_label is required")
		return
	}
	if !orgEnrollRoles[req.Role] {
		writeJSONError(w, http.StatusBadRequest, "role must be one of: member, observer")
		return
	}
	if len(req.OrgSignerPub) != 64 {
		writeJSONError(w, http.StatusBadRequest, "org_signer_pub must be a 64-char hex-encoded ed25519 public key")
		return
	}
	if _, err := hex.DecodeString(req.OrgSignerPub); err != nil {
		writeJSONError(w, http.StatusBadRequest, "org_signer_pub must be hex-encoded")
		return
	}

	ownerID := middleware.ContextAgentID(r.Context())
	issued, err := ts.IssueOrgEnrollmentToken(r.Context(), orgID, ownerID, req.NodeLabel, req.Role, req.OrgSignerPub, req.KeyEpoch)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to persist enrollment token")
		return
	}

	writeJSON(w, http.StatusCreated, OrgEnrollTokenIssueResponse{
		ID:        issued.ID,
		OrgID:     issued.OrgID,
		OwnerID:   issued.OwnerID,
		NodeLabel: issued.NodeLabel,
		Role:      issued.Role,
		Token:     issued.Token,
		IssuedAt:  issued.IssuedAt,
		UseHint:   "Give this token to the node out-of-band; it redeems it once at POST /v1/org/" + orgID + "/enroll. SAVE THIS TOKEN NOW — it is never shown again.",
	})
}

// handleOrgEnrollTokenList returns the org's tokens as summaries (no token values).
func (s *Server) handleOrgEnrollTokenList(w http.ResponseWriter, r *http.Request) {
	ts, orgID, ok := s.orgEnrollTokenPreflight(w, r)
	if !ok {
		return
	}
	rows, err := ts.ListOrgEnrollmentTokens(r.Context(), orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list enrollment tokens")
		return
	}
	out := make([]OrgEnrollTokenSummary, 0, len(rows))
	for _, t := range rows {
		out = append(out, OrgEnrollTokenSummary{
			ID:           t.ID,
			OrgID:        t.OrgID,
			OwnerID:      t.OwnerID,
			NodeLabel:    t.NodeLabel,
			Role:         t.Role,
			OrgSignerPub: t.OrgSignerPub,
			KeyEpoch:     t.KeyEpoch,
			IssuedAt:     t.IssuedAt,
			RevokedAt:    t.RevokedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tokens": out})
}

// handleOrgEnrollTokenRevoke revokes a token of this org. Idempotent. A token
// ID from another org reports not-found, so the path org is always authoritative.
func (s *Server) handleOrgEnrollTokenRevoke(w http.ResponseWriter, r *http.Request) {
	ts, orgID, ok := s.orgEnrollTokenPreflight(w, r)
	if !ok {
		return
	}
	id := strings.TrimSpace(chi.URLParam(r, "id"))
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "token id is required")
		return
	}

	rows, err := ts.ListOrgEnrollmentTokens(r.Context(), orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to verify token org")
		return
	}
	inOrg := false
	for _, t := range rows {
		if t.ID == id {
			inOrg = true
			break
		}
	}
	if !inOrg {
		writeJSONError(w, http.StatusNotFound, "token not found")
		return
	}

	if err := ts.RevokeOrgEnrollmentToken(r.Context(), id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSONError(w, http.StatusNotFound, "token not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to revoke enrollment token")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
