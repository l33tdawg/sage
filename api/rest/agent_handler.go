package rest

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// handleAgentRegister handles POST /v1/agent/register.
// Builds a TxTypeAgentRegister and broadcasts via CometBFT.
// Idempotent: returns existing record if already registered.
func (s *Server) handleAgentRegister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name       string `json:"name"`
		Role       string `json:"role"`
		BootBio    string `json:"boot_bio"`
		Provider   string `json:"provider"`
		P2PAddress string `json:"p2p_address"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing name", "name is required.")
		return
	}
	if req.Role == "" {
		req.Role = "member"
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Idempotent: check if already registered on-chain
	if s.badgerStore != nil && s.badgerStore.IsAgentRegistered(agentID) {
		existing, err := s.badgerStore.GetRegisteredAgent(agentID)
		if err == nil {
			name := existing.Name

			// Self-healing: if the dashboard (SQLite) has a different name than on-chain
			// (e.g. admin renamed via GUI but the CometBFT broadcast failed), reconcile
			// by pushing the SQLite name to on-chain state.
			if s.agentStore != nil {
				if sqliteAgent, agErr := s.agentStore.GetAgent(r.Context(), agentID); agErr == nil && sqliteAgent.Name != existing.Name {
					name = sqliteAgent.Name
					s.reconcileAgentName(agentID, sqliteAgent.Name, existing.BootBio)
				}
			}

			writeJSON(w, http.StatusOK, map[string]any{
				"agent_id":        existing.AgentID,
				"name":            name,
				"registered_name": existing.RegisteredName,
				"role":            existing.Role,
				"provider":        existing.Provider,
				"status":          "already_registered",
				"on_chain_height": existing.RegisteredAt,
			})
			return
		}
	}

	registerTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentRegister,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		AgentRegister: &tx.AgentRegister{
			AgentID:    agentID,
			Name:       req.Name,
			Role:       req.Role,
			BootBio:    req.BootBio,
			Provider:   req.Provider,
			P2PAddress: req.P2PAddress,
		},
	}

	embedAgentAuth(r.Context(), registerTx)

	if err := tx.SignTx(registerTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(registerTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	// broadcast_tx_commit (not _sync) so the response includes the block
	// height. Clients use on_chain_height as a trivial "did this actually
	// land on-chain?" check — surfacing it on first registration means
	// the first register_agent call doesn't come back with height=None
	// (prior behaviour) and then height=<N> only on the idempotent
	// re-registration path. SDK callers were reading the height=None as
	// a version-drift signal; with the fix both code paths surface it.
	txHash, height, err := s.broadcastTxCommitWithHeight(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent register tx")
		writeProblem(w, broadcastErrorStatus(err), "Broadcast error", err.Error())
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("agent", agentID, "", fmt.Sprintf("Agent %q registered (%s)", req.Name, req.Role), nil)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"agent_id":        agentID,
		"name":            req.Name,
		"registered_name": req.Name,
		"role":            req.Role,
		"provider":        req.Provider,
		"status":          "registered",
		"tx_hash":         txHash,
		"on_chain_height": height,
	})
}

// handleAgentUpdate handles PUT /v1/agent/update.
// Self-update only — agent can only update its own metadata.
func (s *Server) handleAgentUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name    string `json:"name"`
		BootBio string `json:"boot_bio"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	updateTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentUpdate,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    req.Name,
			BootBio: req.BootBio,
		},
	}

	embedAgentAuth(r.Context(), updateTx)

	if err := tx.SignTx(updateTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(updateTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent update tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id": agentID,
		"name":     req.Name,
		"status":   "updated",
		"tx_hash":  txHash,
	})
}

// reconcileAgentName pushes the SQLite (display) name to on-chain state via an
// AgentUpdate transaction. This self-heals the split-brain where a GUI rename
// updated SQLite but the CometBFT broadcast silently failed.
func (s *Server) reconcileAgentName(agentID, name, bio string) {
	updateTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentUpdate,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: agentID,
			Name:    name,
			BootBio: bio,
		},
	}
	if err := tx.SignTx(updateTx, s.signingKey); err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).Msg("reconcile: failed to sign agent name update")
		return
	}
	encoded, err := tx.EncodeTx(updateTx)
	if err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).Msg("reconcile: failed to encode agent name update")
		return
	}
	if _, err := s.broadcastTx(encoded); err != nil {
		s.logger.Warn().Err(err).Str("agent_id", agentID).Msg("reconcile: failed to broadcast agent name update")
		return
	}
	s.logger.Info().Str("agent_id", agentID).Str("name", name).Msg("reconciled agent name: on-chain updated to match display name")
}

// handleAgentSetPermission handles PUT /v1/agent/{id}/permission.
//
// Auth model (v6.6.9 — see also processAgentSetPermission in
// internal/abci/app.go which is the consensus-side source of truth):
//
//   - Self-set: the caller is the target agent.
//   - Global admin: caller's on-chain Role == "admin" (legacy
//     deployment-admin identity established at genesis bootstrap or
//     initial register).
//   - Org admin: caller is an org member with role="admin" in any org the
//     target also belongs to.
//
// Anything else is rejected with HTTP 403 BEFORE broadcasting the tx, and
// the ABCI handler re-checks the same auth model on-chain so the gate
// holds even when REST is bypassed (direct CometBFT broadcast, GUI, etc).
//
// Prior to v6.6.9 this endpoint:
//  1. used broadcast_tx_sync (only checks CheckTx code, not FinalizeBlock),
//  2. had no REST-side auth check, and
//  3. the ABCI handler hard-gated on global Role=="admin",
//
// so a non-admin caller would get 200 + a real tx_hash for a tx that
// FinalizeBlock then rejected with code=67 — the SQL row was never
// updated, but the API said success. That silent-failure is the bug fixed
// here (Bug B from the v6.6.8 Level Up follow-up).
func (s *Server) handleAgentSetPermission(w http.ResponseWriter, r *http.Request) {
	targetID := chi.URLParam(r, "id")
	if targetID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "id path parameter is required.")
		return
	}

	var req struct {
		Clearance     *int    `json:"clearance"`
		DomainAccess  *string `json:"domain_access"`
		VisibleAgents *string `json:"visible_agents"`
		OrgID         string  `json:"org_id"`
		DeptID        string  `json:"dept_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Pre-flight RBAC: bail out fast with 403 so callers don't get a
	// 200+tx_hash for a write the chain will refuse. The ABCI handler
	// re-verifies this in consensus — REST is just the user-friendly
	// fail-fast path, not the trust boundary.
	if s.badgerStore != nil {
		callerID := middleware.ContextAgentID(r.Context())
		if callerID == "" {
			writeProblem(w, http.StatusUnauthorized, "Authentication required", "agent identity required to set permissions.")
			return
		}
		if !s.callerCanSetPermission(callerID, targetID) {
			writeProblem(w, http.StatusForbidden, "Access denied",
				"caller is not authorized to set permissions on this agent: must be self, a global admin, or an org admin of the target's org.")
			return
		}
	}

	clearance := uint8(1)
	if req.Clearance != nil {
		clearance = uint8(*req.Clearance) // #nosec G115 -- validated small int 0-4
	}
	domainAccess := ""
	if req.DomainAccess != nil {
		domainAccess = *req.DomainAccess
	}
	visibleAgents := ""
	if req.VisibleAgents != nil {
		visibleAgents = *req.VisibleAgents
	}

	permTx := &tx.ParsedTx{
		Type:      tx.TxTypeAgentSetPermission,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		AgentSetPermission: &tx.AgentSetPermission{
			AgentID:       targetID,
			Clearance:     clearance,
			DomainAccess:  domainAccess,
			VisibleAgents: visibleAgents,
			OrgID:         req.OrgID,
			DeptID:        req.DeptID,
		},
	}

	embedAgentAuth(r.Context(), permTx)

	if err := tx.SignTx(permTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(permTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	// Use broadcast_tx_commit (NOT _sync) so a FinalizeBlock rejection is
	// surfaced as an error to the REST caller. Sync only inspects CheckTx
	// (signature/nonce) and would happily return a tx_hash for a tx that
	// the consensus handler later rejects — the v6.6.8-and-prior silent
	// failure mode.
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent set permission tx")
		writeProblem(w, broadcastErrorStatus(err), "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id": targetID,
		"status":   "permissions_updated",
		"tx_hash":  txHash,
	})
}

// callerCanSetPermission mirrors the consensus-side auth check in
// processAgentSetPermission. Keep these two implementations in sync —
// the ABCI handler is the trust boundary, this is the fail-fast UX layer.
func (s *Server) callerCanSetPermission(callerID, targetID string) bool {
	if callerID == "" {
		return false
	}
	// 1. Self-set.
	if callerID == targetID {
		return true
	}
	// 2. Global admin (legacy deployment-admin identity).
	if caller, err := s.badgerStore.GetRegisteredAgent(callerID); err == nil && caller != nil && caller.Role == "admin" {
		return true
	}
	// 3. Org admin in any org the target belongs to.
	orgs, err := s.badgerStore.ListAgentOrgs(targetID)
	if err != nil {
		return false
	}
	for _, orgID := range orgs {
		_, role, mErr := s.badgerStore.GetMemberClearance(orgID, callerID)
		if mErr == nil && role == "admin" {
			return true
		}
	}
	return false
}

// handleGetRegisteredAgent handles GET /v1/agent/{id}.
// Reads from offchain store (no tx broadcast needed).
func (s *Server) handleGetRegisteredAgent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "id path parameter is required.")
		return
	}

	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}

	agent, err := s.agentStore.GetAgent(r.Context(), id)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Agent not found", fmt.Sprintf("No agent found with ID %s.", id))
		return
	}

	writeJSON(w, http.StatusOK, agent)
}

// handleListRegisteredAgents handles GET /v1/agents.
// Lists all registered agents from offchain store.
func (s *Server) handleListRegisteredAgents(w http.ResponseWriter, r *http.Request) {
	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent store unavailable", "Agent store not configured.")
		return
	}

	agents, err := s.agentStore.ListAgents(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "List error", err.Error())
		return
	}
	if agents == nil {
		agents = make([]*store.AgentEntry, 0)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agents": agents,
		"total":  len(agents),
	})
}
