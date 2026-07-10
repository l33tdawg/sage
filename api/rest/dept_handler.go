package rest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request types -----------------------------------------------------------

// DeptRegisterReq is the JSON body for POST /v1/org/{org_id}/dept.
type DeptRegisterReq struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	ParentDept  string `json:"parent_dept,omitempty"`
}

// DeptAddMemberReq is the JSON body for POST /v1/org/{org_id}/dept/{dept_id}/member.
type DeptAddMemberReq struct {
	AgentID string `json:"agent_id"`
	// Pointer so an explicit clearance of 0 (ClearancePublic — a valid level
	// that gates reads) is distinguishable from an omitted field. A bare int
	// can't tell "0" from "absent", which silently escalated PUBLIC members to
	// INTERNAL. Mirrors the *int pattern on the agent-permission endpoint
	// (see Bug 2, v6.8.4 hotfix, api/rest/agent_handler.go).
	Clearance *int   `json:"clearance,omitempty"`
	Role      string `json:"role,omitempty"`
}

// --- Department Handlers -----------------------------------------------------

// handleDeptRegister handles POST /v1/org/{org_id}/dept.
func (s *Server) handleDeptRegister(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	if orgID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing org ID", "org_id path parameter is required")
		return
	}

	var req DeptRegisterReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Name == "" {
		writeProblem(w, http.StatusBadRequest, "Missing department name", "name is required")
		return
	}

	// Deterministic dept ID from org ID + name.
	deptIDHash := sha256.Sum256([]byte(orgID + req.Name))
	deptID := hex.EncodeToString(deptIDHash[:8])

	deptTx := &tx.ParsedTx{
		Type:      tx.TxTypeDeptRegister,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		DeptRegister: &tx.DeptRegister{
			OrgID:       orgID,
			DeptID:      deptID,
			DeptName:    req.Name,
			Description: req.Description,
			ParentDept:  req.ParentDept,
		},
	}

	s.embedAgentAuth(r.Context(), deptTx)

	err = tx.SignTx(deptTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign dept register tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(deptTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode dept register tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast dept register tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "registered",
		"dept_id": deptID,
		"tx_hash": txHash,
	})
}

// handleGetDept handles GET /v1/org/{org_id}/dept/{dept_id}.
func (s *Server) handleGetDept(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	deptID := chi.URLParam(r, "dept_id")
	if orgID == "" || deptID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing parameters", "org_id and dept_id path parameters are required")
		return
	}

	if s.orgStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org store unavailable",
			"Organization storage is not configured.")
		return
	}

	dept, err := s.orgStore.GetDept(r.Context(), orgID, deptID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Department not found",
			fmt.Sprintf("No department found with ID %s in org %s", deptID, orgID))
		return
	}

	writeJSON(w, http.StatusOK, dept)
}

// handleListOrgDepts handles GET /v1/org/{org_id}/depts.
func (s *Server) handleListOrgDepts(w http.ResponseWriter, r *http.Request) {
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

	depts, err := s.orgStore.GetOrgDepts(r.Context(), orgID)
	if err != nil {
		s.logger.Error().Err(err).Str("org_id", orgID).Msg("failed to get org departments")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query organization departments.")
		return
	}

	writeJSON(w, http.StatusOK, depts)
}

// handleDeptAddMember handles POST /v1/org/{org_id}/dept/{dept_id}/member.
func (s *Server) handleDeptAddMember(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	deptID := chi.URLParam(r, "dept_id")
	if orgID == "" || deptID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing parameters", "org_id and dept_id path parameters are required")
		return
	}

	var req DeptAddMemberReq
	err := decodeJSON(r, &req)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.AgentID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing agent ID", "agent_id is required")
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
		Type:      tx.TxTypeDeptAddMember,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		DeptAddMember: &tx.DeptAddMember{
			OrgID:     orgID,
			DeptID:    deptID,
			AgentID:   req.AgentID,
			Clearance: tx.ClearanceLevel(clearance), // #nosec G115 -- validated small int
			Role:      req.Role,
		},
	}

	s.embedAgentAuth(r.Context(), addTx)

	err = tx.SignTx(addTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign dept add member tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(addTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode dept add member tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast dept add member tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"status":  "added",
		"tx_hash": txHash,
	})
}

// handleDeptRemoveMember handles DELETE /v1/org/{org_id}/dept/{dept_id}/member/{agent_id}.
func (s *Server) handleDeptRemoveMember(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	deptID := chi.URLParam(r, "dept_id")
	agentToRemove := chi.URLParam(r, "agent_id")
	if orgID == "" || deptID == "" || agentToRemove == "" {
		writeProblem(w, http.StatusBadRequest, "Missing parameters", "org_id, dept_id, and agent_id path parameters are required")
		return
	}

	removeTx := &tx.ParsedTx{
		Type:      tx.TxTypeDeptRemoveMember,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		DeptRemoveMember: &tx.DeptRemoveMember{
			OrgID:   orgID,
			DeptID:  deptID,
			AgentID: agentToRemove,
		},
	}

	s.embedAgentAuth(r.Context(), removeTx)

	err := tx.SignTx(removeTx, s.signingKey)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to sign dept remove member tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(removeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode dept remove member tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast dept remove member tx")
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "removed",
		"tx_hash": txHash,
	})
}

// handleListDeptMembers handles GET /v1/org/{org_id}/dept/{dept_id}/members.
func (s *Server) handleListDeptMembers(w http.ResponseWriter, r *http.Request) {
	orgID := chi.URLParam(r, "org_id")
	deptID := chi.URLParam(r, "dept_id")
	if orgID == "" || deptID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing parameters", "org_id and dept_id path parameters are required")
		return
	}

	if s.orgStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org store unavailable",
			"Organization storage is not configured.")
		return
	}

	members, err := s.orgStore.GetDeptMembers(r.Context(), orgID, deptID)
	if err != nil {
		s.logger.Error().Err(err).Str("org_id", orgID).Str("dept_id", deptID).Msg("failed to get dept members")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query department members.")
		return
	}

	writeJSON(w, http.StatusOK, members)
}
