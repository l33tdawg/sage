package rest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
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
	AgentID string `json:"agent_id"`
	// Pointer so an explicit clearance of 0 (ClearancePublic — a valid level
	// that gates reads) is distinguishable from an omitted field. A bare int
	// can't tell "0" from "absent", which silently escalated PUBLIC members to
	// INTERNAL. Mirrors the *int pattern on the agent-permission endpoint
	// (see Bug 2, v6.8.4 hotfix, api/rest/agent_handler.go).
	Clearance *int   `json:"clearance,omitempty"`
	Role      string `json:"role,omitempty"`
}

// OrgSetClearanceReq is the JSON body for POST /v1/org/{org_id}/clearance.
type OrgSetClearanceReq struct {
	AgentID   string `json:"agent_id"`
	Clearance int    `json:"clearance"`
}

// FederationProposeReq is the JSON body for POST /v1/federation/propose.
type FederationProposeReq struct {
	ProposerOrgID    string   `json:"proposer_org_id,omitempty"`
	TargetOrgID      string   `json:"target_org_id"`
	AllowedDomains   []string `json:"allowed_domains,omitempty"`
	AllowedDepts     []string `json:"allowed_depts,omitempty"`
	MaxClearance     *int     `json:"max_clearance,omitempty"`
	ExpiresAt        int64    `json:"expires_at,omitempty"`
	RequiresApproval bool     `json:"requires_approval,omitempty"`
}

// FederationRevokeReq is the JSON body for POST /v1/federation/{fed_id}/revoke.
type FederationRevokeReq struct {
	Reason string `json:"reason,omitempty"`
}

// --- Organization Handlers ---------------------------------------------------

func (s *Server) callerIsGlobalAdmin(r *http.Request) bool {
	if s.badgerStore == nil {
		return false
	}
	agentID := middleware.ContextAgentID(r.Context())
	if s.isPostV23ForNextTx() {
		return s.callerIsOperatorOrAdmin(r.Context(), agentID)
	}
	agent, err := s.badgerStore.GetRegisteredAgent(agentID)
	return err == nil && agent != nil && agent.Role == "admin"
}

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

	credentialID := middleware.ContextAgentID(r.Context())
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 organization registration requires a global admin.")
		return
	}
	adminID := credentialID
	if s.isPostV23ForNextTx() {
		adminID, err = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if err != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current Root policy state could not be resolved.")
			return
		}
	}

	// Deterministic org ID from agent pubkey + name.
	orgIDHash := sha256.Sum256([]byte(adminID + req.Name))
	orgID := hex.EncodeToString(orgIDHash[:16])

	orgTx := &tx.ParsedTx{
		Type: tx.TxTypeOrgRegister,
		OrgRegister: &tx.OrgRegister{
			OrgID:       orgID,
			Name:        req.Name,
			Description: req.Description,
			AdminAgent:  adminID,
		},
	}

	s.embedAgentAuth(r.Context(), orgTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), orgTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "org register", err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "registered",
		"org_id":  orgID,
		"tx_hash": txHash,
	})
}

// handleGetOrg handles GET /v1/org/{org_id}.
//
// Chain-authoritative — same pattern as handleGetDomain (v7.5.3). Owner,
// name, parent and created_height come from BadgerDB; the off-chain
// orgStore mirror is only consulted to enrich created_at (and serves as
// a divergence sensor: a WARN logs when the mirror disagrees with chain).
// Returning the mirror's admin_agent_id directly would let stale rows
// (chain reset that didn't drop accessStore tables) tell callers they own
// an org the chain doesn't recognise, producing Code-54 "is not admin of
// org" rejections on the next add_org_member.
func (s *Server) handleGetOrg(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Chain store unavailable",
			"On-chain state is not configured.")
		return
	}

	name, description, adminAgent, height, err := s.badgerStore.GetOrgWithMeta(orgID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Organization not found",
			fmt.Sprintf("No organization found with ID %s", orgID))
		return
	}

	resp := &store.OrgEntry{
		OrgID:         orgID,
		Name:          name,
		Description:   description,
		AdminAgentID:  adminAgent,
		CreatedHeight: height,
	}

	if s.orgStore != nil {
		if mirror, mirrorErr := s.orgStore.GetOrg(r.Context(), orgID); mirrorErr == nil && mirror != nil {
			if mirror.AdminAgentID != "" && mirror.AdminAgentID != adminAgent {
				s.logger.Warn().Str("org_id", orgID).Str("chain_admin", adminAgent).Str("mirror_admin", mirror.AdminAgentID).Msg("orgStore mirror disagrees with chain on org admin — serving chain value")
			}
			resp.CreatedAt = mirror.CreatedAt
			if resp.Description == "" {
				resp.Description = mirror.Description
			}
		}
	}

	writeJSON(w, http.StatusOK, resp)
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
//
// Reads from Badger (chain-authoritative) and uses the off-chain orgStore
// mirror only to enrich created_at on each member. A row that exists in
// the mirror but not on chain is silently dropped — surfacing it would
// mislead callers into believing they have admin access the chain won't
// honour (Code-54 rejections on add_org_member / set_clearance / etc.).
func (s *Server) handleListOrgMembers(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Chain store unavailable",
			"On-chain state is not configured.")
		return
	}

	chainMembers, err := s.badgerStore.ListOrgMembers(orgID)
	if err != nil {
		s.logger.Error().Err(err).Str("org_id", orgID).Msg("failed to list org members from chain")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query organization members.")
		return
	}

	mirrorByAgent := map[string]*store.OrgMemberEntry{}
	if s.orgStore != nil {
		if mirror, mirrorErr := s.orgStore.GetOrgMembers(r.Context(), orgID); mirrorErr == nil {
			for _, m := range mirror {
				mirrorByAgent[m.AgentID] = m
			}
		}
	}

	out := make([]*store.OrgMemberEntry, 0, len(chainMembers))
	for i := range chainMembers {
		m := chainMembers[i]
		isRoot, rootErr := s.appV23IsRootIdentity(m.AgentID)
		if rootErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current CEREBRUM Root state is unavailable.")
			return
		}
		if isRoot {
			continue
		}
		if mm := mirrorByAgent[m.AgentID]; mm != nil {
			m.CreatedAt = mm.CreatedAt
			m.RemovedAt = mm.RemovedAt
		}
		out = append(out, &m)
	}

	writeJSON(w, http.StatusOK, out)
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
	if s.rejectAppV23RootAgentTarget(w, req.AgentID) {
		return
	}
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 organization membership changes require a global admin.")
		return
	}
	// Only a missing clearance falls back to the safe INTERNAL default; an
	// explicit 0 (ClearancePublic) is honored verbatim.
	clearance := 1
	if req.Clearance != nil {
		clearance = *req.Clearance
	}
	if req.Role == "" {
		req.Role = "member"
	}

	addTx := &tx.ParsedTx{
		Type: tx.TxTypeOrgAddMember,
		OrgAddMember: &tx.OrgAddMember{
			OrgID:     orgID,
			AgentID:   req.AgentID,
			Clearance: tx.ClearanceLevel(clearance), // #nosec G115 -- validated small int
			Role:      req.Role,
		},
	}

	s.embedAgentAuth(r.Context(), addTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), addTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "org add member", err)
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
	if s.rejectAppV23RootAgentTarget(w, agentToRemove) {
		return
	}
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 organization membership changes require a global admin.")
		return
	}

	removeTx := &tx.ParsedTx{
		Type: tx.TxTypeOrgRemoveMember,
		OrgRemoveMember: &tx.OrgRemoveMember{
			OrgID:   orgID,
			AgentID: agentToRemove,
		},
	}

	s.embedAgentAuth(r.Context(), removeTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), removeTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "org remove member", err)
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
	if s.rejectAppV23RootAgentTarget(w, req.AgentID) {
		return
	}
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 organization clearance changes require a global admin.")
		return
	}

	clearanceTx := &tx.ParsedTx{
		Type: tx.TxTypeOrgSetClearance,
		OrgSetClearance: &tx.OrgSetClearance{
			OrgID:     orgID,
			AgentID:   req.AgentID,
			Clearance: tx.ClearanceLevel(req.Clearance), // #nosec G115 -- validated small int
		},
	}

	s.embedAgentAuth(r.Context(), clearanceTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), clearanceTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "org set clearance", err)
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
	maxClearance := 2 // Omitted defaults to CONFIDENTIAL; explicit PUBLIC (0) is preserved.
	if req.MaxClearance != nil {
		if *req.MaxClearance < 0 || *req.MaxClearance > int(tx.ClearanceTopSecret) {
			writeProblem(w, http.StatusBadRequest, "Invalid max clearance", "max_clearance must be between 0 and 4.")
			return
		}
		maxClearance = *req.MaxClearance
	}
	if req.ExpiresAt < 0 || (req.ExpiresAt > 0 && req.ExpiresAt <= time.Now().Unix()) {
		writeProblem(w, http.StatusBadRequest, "Invalid expiry", "expires_at must be zero or in the future.")
		return
	}
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 federation changes require a global admin.")
		return
	}

	credentialID := middleware.ContextAgentID(r.Context())
	globalAdmin := s.isPostV23ForNextTx() && s.callerIsGlobalAdmin(r)
	policyID := credentialID
	if s.isPostV23ForNextTx() {
		policyID, err = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if err != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current Root policy state could not be resolved.")
			return
		}
	}

	// Resolve the proposer's org from on-chain state. Multi-org callers may
	// select an exact membership; omission preserves the legacy primary-org
	// default for API compatibility.
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}
	proposerOrg := req.ProposerOrgID
	if proposerOrg != "" {
		if !globalAdmin {
			member, memberErr := s.badgerStore.IsAgentInOrg(policyID, proposerOrg)
			if memberErr != nil || !member {
				writeProblem(w, http.StatusForbidden, "Not in proposer organization",
					"You must belong to proposer_org_id to propose its federation.")
				return
			}
		}
	} else {
		proposerOrg, err = s.badgerStore.GetAgentOrg(policyID)
		if err != nil {
			if globalAdmin {
				writeProblem(w, http.StatusBadRequest, "Missing proposer organization",
					"proposer_org_id is required when the global Admin is not a member of an organization.")
			} else {
				writeProblem(w, http.StatusForbidden, "Not in an organization",
					"You must belong to an organization to propose federations")
			}
			return
		}
	}

	proposeTx := &tx.ParsedTx{
		Type: tx.TxTypeFederationPropose,
		FederationPropose: &tx.FederationPropose{
			ProposerOrgID:    proposerOrg,
			TargetOrgID:      req.TargetOrgID,
			AllowedDomains:   req.AllowedDomains,
			AllowedDepts:     req.AllowedDepts,
			MaxClearance:     tx.ClearanceLevel(maxClearance), // #nosec G115 -- validated 0..4
			ExpiresAt:        req.ExpiresAt,
			RequiresApproval: req.RequiresApproval,
		},
	}

	s.embedAgentAuth(r.Context(), proposeTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), proposeTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "federation propose", err)
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
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 federation changes require a global admin.")
		return
	}

	credentialID := middleware.ContextAgentID(r.Context())
	globalAdmin := s.isPostV23ForNextTx() && s.callerIsGlobalAdmin(r)
	policyID := credentialID
	if s.isPostV23ForNextTx() {
		var policyErr error
		policyID, policyErr = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if policyErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current Root policy state could not be resolved.")
			return
		}
	}

	// Resolve the authoritative target from the stored agreement. A legacy
	// primary-org lookup is insufficient for multi-org agents and can produce a
	// transaction that REST reports as accepted but consensus rejects.
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}
	_, approverOrg, _, _, _, err := s.badgerStore.GetFederation(fedID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Federation not found", "The federation agreement does not exist.")
		return
	}
	if !globalAdmin {
		member, memberErr := s.badgerStore.IsAgentInOrg(policyID, approverOrg)
		if memberErr != nil || !member {
			writeProblem(w, http.StatusForbidden, "Not in target organization",
				"You must belong to the federation's target organization to approve it.")
			return
		}
	}

	approveTx := &tx.ParsedTx{
		Type: tx.TxTypeFederationApprove,
		FederationApprove: &tx.FederationApprove{
			FederationID:  fedID,
			ApproverOrgID: approverOrg,
		},
	}

	s.embedAgentAuth(r.Context(), approveTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), approveTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "federation approve", err)
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
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 federation changes require a global admin.")
		return
	}

	credentialID := middleware.ContextAgentID(r.Context())
	globalAdmin := s.isPostV23ForNextTx() && s.callerIsGlobalAdmin(r)
	policyID := credentialID
	if s.isPostV23ForNextTx() {
		var policyErr error
		policyID, policyErr = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if policyErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current Root policy state could not be resolved.")
			return
		}
	}

	// Resolve both authoritative sides from the stored agreement and choose a
	// deterministic exact membership (proposer first). This matches consensus
	// for multi-org callers and rejects unrelated global admins before broadcast.
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "State store unavailable",
			"On-chain state store is not configured.")
		return
	}
	proposerOrg, targetOrg, _, _, _, err := s.badgerStore.GetFederation(fedID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Federation not found", "The federation agreement does not exist.")
		return
	}
	revokerOrg := ""
	if globalAdmin {
		revokerOrg = proposerOrg
	} else if inProposer, _ := s.badgerStore.IsAgentInOrg(policyID, proposerOrg); inProposer {
		revokerOrg = proposerOrg
	} else if inTarget, _ := s.badgerStore.IsAgentInOrg(policyID, targetOrg); inTarget {
		revokerOrg = targetOrg
	}
	if revokerOrg == "" {
		writeProblem(w, http.StatusForbidden, "Not in federation organization",
			"You must belong to either side of the federation to revoke it.")
		return
	}

	var req FederationRevokeReq
	// Body is optional for revoke.
	_ = decodeJSON(r, &req)

	revokeTx := &tx.ParsedTx{
		Type: tx.TxTypeFederationRevoke,
		FederationRevoke: &tx.FederationRevoke{
			FederationID: fedID,
			RevokerOrgID: revokerOrg,
			Reason:       req.Reason,
		},
	}

	s.embedAgentAuth(r.Context(), revokeTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), revokeTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "federation revoke", err)
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
