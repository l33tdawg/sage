package rest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request types -----------------------------------------------------------

// OrgRegisterReq is the JSON body for POST /v1/org/register.
type OrgRegisterReq struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// OrgAddMemberReq is the JSON body for POST /v1/org/{org_id}/member.
type OrgAddMemberReq struct {
	AgentID   string `json:"agent_id"`
	Clearance int    `json:"clearance,omitempty"`
	Role      string `json:"role,omitempty"`
}

// OrgSetClearanceReq is the JSON body for POST /v1/org/{org_id}/clearance.
type OrgSetClearanceReq struct {
	AgentID   string `json:"agent_id"`
	Clearance int    `json:"clearance"`
}

// FederationProposeReq is the JSON body for POST /v1/federation/propose.
type FederationProposeReq struct {
	TargetOrgID      string   `json:"target_org_id"`
	AllowedDomains   []string `json:"allowed_domains,omitempty"`
	AllowedDepts     []string `json:"allowed_depts,omitempty"`
	MaxClearance     int      `json:"max_clearance,omitempty"`
	ExpiresAt        int64    `json:"expires_at,omitempty"`
	RequiresApproval bool     `json:"requires_approval,omitempty"`
}

// FederationRevokeReq is the JSON body for POST /v1/federation/{fed_id}/revoke.
type FederationRevokeReq struct {
	Reason string `json:"reason,omitempty"`
}

// --- Organization Handlers ---------------------------------------------------

// handleOrgRegister handles POST /v1/org/register.
func (s *Server) handleOrgRegister(w http.ResponseWriter, r *http.Request) {
	var req OrgRegisterReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org name", "name is required")
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Deterministic org ID from agent pubkey + name.
	orgIDHash := sha256.Sum256([]byte(agentID + req.Name))
	orgID := hex.EncodeToString(orgIDHash[:16])

	orgTx := &tx.ParsedTx{
		Type:      tx.TxTypeOrgRegister,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		OrgRegister: &tx.OrgRegister{
			OrgID:       orgID,
			Name:        req.Name,
			Description: req.Description,
			AdminAgent:  agentID,
		},
	}

	embedAgentAuth(r.Context(), orgTx)

	err = tx.SignTx(orgTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign org register tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(orgTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode org register tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast org register tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "registered",
		"org_id":  orgID,
		"tx_hash": txHash,
	})
}

// handleGetOrg handles GET /v1/org/{org_id}.
func (s *Server) handleGetOrg(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	if s.orgStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org store unavailable",
			"Organization storage is not configured.")
		return
	}

	org, err := s.orgStore.GetOrg(r.Context(), orgID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Organization not found",
			fmt.Sprintf("No organization found with ID %s", orgID))
		return
	}

	writeJSON(w, http.StatusOK, org)
}

// handleGetOrgByName handles GET /v1/org/by-name/{name}.
//
// Org names are NOT enforced unique on-chain — the same human-readable name
// can map to multiple distinct orgIDs registered by different admins (or
// the same admin at different heights). This endpoint returns every match
// so the caller can disambiguate. An empty result is a valid answer and
// returns HTTP 200 with `{"orgs": []}` rather than 404 — the SDK uses that
// signal to raise a clear "no such org" error.
func (s *Server) handleGetOrgByName(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org name", "name path parameter is required")
		return
	}

	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}

	entries, err := s.badgerStore.ListOrgsByName(name)
	if err != nil {
		s.logger.Error().Err(err).Str("name", name).Msg("failed to list orgs by name")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query organizations by name.")
		return
	}

	orgs := make([]map[string]string, 0, len(entries))
	for _, e := range entries {
		orgs = append(orgs, map[string]string{
			"org_id":         e.OrgID,
			"name":           e.Name,
			"admin_agent_id": e.AdminAgentID,
			"description":    e.Description,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{"orgs": orgs})
}

// handleListOrgMembers handles GET /v1/org/{org_id}/members.
func (s *Server) handleListOrgMembers(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	if s.orgStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org store unavailable",
			"Organization storage is not configured.")
		return
	}

	members, err := s.orgStore.GetOrgMembers(r.Context(), orgID)
	if err != nil {
		s.logger.Error().Err(err).Str("org_id", orgID).Msg("failed to get org members")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query organization members.")
		return
	}

	writeJSON(w, http.StatusOK, members)
}

// handleOrgAddMember handles POST /v1/org/{org_id}/member.
func (s *Server) handleOrgAddMember(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	var req OrgAddMemberReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.AgentID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "agent_id is required")
		return
	}
	if req.Clearance == 0 {
		req.Clearance = 1 // Default to INTERNAL
	}
	if req.Role == "" {
		req.Role = "member"
	}

	addTx := &tx.ParsedTx{
		Type:      tx.TxTypeOrgAddMember,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		OrgAddMember: &tx.OrgAddMember{
			OrgID:     orgID,
			AgentID:   req.AgentID,
			Clearance: tx.ClearanceLevel(req.Clearance), // #nosec G115 -- validated small int
			Role:      req.Role,
		},
	}

	embedAgentAuth(r.Context(), addTx)

	err = tx.SignTx(addTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign org add member tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(addTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode org add member tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast org add member tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "added",
		"tx_hash": txHash,
	})
}

// handleOrgRemoveMember handles DELETE /v1/org/{org_id}/member/{agent_id}.
func (s *Server) handleOrgRemoveMember(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	agentToRemove := chi.URLParam(r, "agent_id")
	if orgID == "" || agentToRemove == "" {
		writeProblem(w, http.StatusBadRequest, "Missing parameters", "org_id and agent_id path parameters are required")
		return
	}

	removeTx := &tx.ParsedTx{
		Type:      tx.TxTypeOrgRemoveMember,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		OrgRemoveMember: &tx.OrgRemoveMember{
			OrgID:   orgID,
			AgentID: agentToRemove,
		},
	}

	embedAgentAuth(r.Context(), removeTx)

	err := tx.SignTx(removeTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign org remove member tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(removeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode org remove member tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast org remove member tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "removed",
		"tx_hash": txHash,
	})
}

// handleOrgSetClearance handles POST /v1/org/{org_id}/clearance.
func (s *Server) handleOrgSetClearance(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	var req OrgSetClearanceReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.AgentID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "agent_id is required")
		return
	}

	clearanceTx := &tx.ParsedTx{
		Type:      tx.TxTypeOrgSetClearance,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		OrgSetClearance: &tx.OrgSetClearance{
			OrgID:     orgID,
			AgentID:   req.AgentID,
			Clearance: tx.ClearanceLevel(req.Clearance), // #nosec G115 -- validated small int
		},
	}

	embedAgentAuth(r.Context(), clearanceTx)

	err = tx.SignTx(clearanceTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign org set clearance tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(clearanceTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode org set clearance tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast org set clearance tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "updated",
		"tx_hash": txHash,
	})
}

// --- Federation Handlers -----------------------------------------------------

// handleFederationPropose handles POST /v1/federation/propose.
func (s *Server) handleFederationPropose(w http.ResponseWriter, r *http.Request) {
	var req FederationProposeReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.TargetOrgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing target org ID", "target_org_id is required")
		return
	}
	if req.MaxClearance == 0 {
		req.MaxClearance = 2 // Default to CONFIDENTIAL
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up proposer's org from on-chain state.
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}
	proposerOrg, err := s.badgerStore.GetAgentOrg(agentID)
	if err != nil {
		writeProblem(w, http.StatusForbidden, "Not in an organization",
			"You must belong to an organization to propose federations")
		return
	}

	proposeTx := &tx.ParsedTx{
		Type:      tx.TxTypeFederationPropose,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		FederationPropose: &tx.FederationPropose{
			ProposerOrgID:    proposerOrg,
			TargetOrgID:      req.TargetOrgID,
			AllowedDomains:   req.AllowedDomains,
			AllowedDepts:     req.AllowedDepts,
			MaxClearance:     tx.ClearanceLevel(req.MaxClearance), // #nosec G115 -- validated small int
			ExpiresAt:        req.ExpiresAt,
			RequiresApproval: req.RequiresApproval,
		},
	}

	embedAgentAuth(r.Context(), proposeTx)

	err = tx.SignTx(proposeTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign federation propose tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(proposeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode federation propose tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast federation propose tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "proposed",
		"tx_hash": txHash,
	})
}

// handleFederationApprove handles POST /v1/federation/{fed_id}/approve.
func (s *Server) handleFederationApprove(w http.ResponseWriter, r *http.Request) {
	fedID := chi.URLParam(r, "fed_id")
	if fedID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing federation ID", "fed_id path parameter is required")
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up approver's org from on-chain state.
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}
	approverOrg, err := s.badgerStore.GetAgentOrg(agentID)
	if err != nil {
		writeProblem(w, http.StatusForbidden, "Not in an organization",
			"You must belong to an organization to approve federations")
		return
	}

	approveTx := &tx.ParsedTx{
		Type:      tx.TxTypeFederationApprove,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		FederationApprove: &tx.FederationApprove{
			FederationID:  fedID,
			ApproverOrgID: approverOrg,
		},
	}

	embedAgentAuth(r.Context(), approveTx)

	err = tx.SignTx(approveTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign federation approve tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(approveTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode federation approve tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast federation approve tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "approved",
		"tx_hash": txHash,
	})
}

// handleFederationRevoke handles POST /v1/federation/{fed_id}/revoke.
func (s *Server) handleFederationRevoke(w http.ResponseWriter, r *http.Request) {
	fedID := chi.URLParam(r, "fed_id")
	if fedID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing federation ID", "fed_id path parameter is required")
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up revoker's org from on-chain state.
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}
	revokerOrg, err := s.badgerStore.GetAgentOrg(agentID)
	if err != nil {
		writeProblem(w, http.StatusForbidden, "Not in an organization",
			"You must belong to an organization to revoke federations")
		return
	}

	var req FederationRevokeReq
	// Body is optional for revoke.
	_ = decodeJSON(r, &req)

	revokeTx := &tx.ParsedTx{
		Type:      tx.TxTypeFederationRevoke,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		FederationRevoke: &tx.FederationRevoke{
			FederationID: fedID,
			RevokerOrgID: revokerOrg,
			Reason:       req.Reason,
		},
	}

	embedAgentAuth(r.Context(), revokeTx)

	err = tx.SignTx(revokeTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign federation revoke tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(revokeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode federation revoke tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast federation revoke tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "revoked",
		"tx_hash": txHash,
	})
}

// handleGetFederation handles GET /v1/federation/{fed_id}.
func (s *Server) handleGetFederation(w http.ResponseWriter, r *http.Request) {
	fedID := chi.URLParam(r, "fed_id")
	if fedID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing federation ID", "fed_id path parameter is required")
		return
	}

	if s.orgStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org store unavailable",
			"Organization storage is not configured.")
		return
	}

	fed, err := s.orgStore.GetFederation(r.Context(), fedID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Federation not found",
			fmt.Sprintf("No federation found with ID %s", fedID))
		return
	}

	writeJSON(w, http.StatusOK, fed)
}

// handleListFederations handles GET /v1/federation/active/{org_id}.
func (s *Server) handleListFederations(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	if s.orgStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org store unavailable",
			"Organization storage is not configured.")
		return
	}

	feds, err := s.orgStore.GetActiveFederations(r.Context(), orgID)
	if err != nil {
		s.logger.Error().Err(err).Str("org_id", orgID).Msg("failed to get federations")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query federations.")
		return
	}

	writeJSON(w, http.StatusOK, feds)
}
