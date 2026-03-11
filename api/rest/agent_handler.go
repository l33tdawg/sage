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
			writeJSON(w, http.StatusOK, map[string]any{
				"agent_id":      existing.AgentID,
				"name":          existing.Name,
				"role":          existing.Role,
				"provider":      existing.Provider,
				"status":        "already_registered",
				"registered_at": existing.RegisteredAt,
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

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent register tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("agent", agentID, "", fmt.Sprintf("Agent %q registered (%s)", req.Name, req.Role), nil)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"agent_id": agentID,
		"name":     req.Name,
		"role":     req.Role,
		"provider": req.Provider,
		"status":   "registered",
		"tx_hash":  txHash,
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

// handleAgentSetPermission handles PUT /v1/agent/{id}/permission.
// Admin only — sets clearance, domain access, visible agents on target agent.
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

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast agent set permission tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id": targetID,
		"status":   "permissions_updated",
		"tx_hash":  txHash,
	})
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
