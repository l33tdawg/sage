package rest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/store"
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
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 department registration requires a global admin.")
		return
	}

	// Deterministic dept ID from org ID + name.
	deptIDHash := sha256.Sum256([]byte(orgID + req.Name))
	deptID := hex.EncodeToString(deptIDHash[:8])

	deptTx := &tx.ParsedTx{
		Type: tx.TxTypeDeptRegister,
		DeptRegister: &tx.DeptRegister{
			OrgID:       orgID,
			DeptID:      deptID,
			DeptName:    req.Name,
			Description: req.Description,
			ParentDept:  req.ParentDept,
		},
	}

	s.embedAgentAuth(r.Context(), deptTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), deptTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "dept register", err)
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
	if s.rejectAppV23RootAgentTarget(w, req.AgentID) {
		return
	}
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 department membership changes require a global admin.")
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
		Type: tx.TxTypeDeptAddMember,
		DeptAddMember: &tx.DeptAddMember{
			OrgID:     orgID,
			DeptID:    deptID,
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
		s.writeConsensusTxError(w, stage, "dept add member", err)
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
	if s.rejectAppV23RootAgentTarget(w, agentToRemove) {
		return
	}
	if s.isPostV22ForNextTx() && !s.callerIsGlobalAdmin(r) {
		writeProblem(w, http.StatusForbidden, "Access denied", "app-v22 department membership changes require a global admin.")
		return
	}

	removeTx := &tx.ParsedTx{
		Type: tx.TxTypeDeptRemoveMember,
		DeptRemoveMember: &tx.DeptRemoveMember{
			OrgID:   orgID,
			DeptID:  deptID,
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
		s.writeConsensusTxError(w, stage, "dept remove member", err)
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

	filtered := make([]*store.DeptMemberEntry, 0, len(members))
	for _, member := range members {
		if member == nil {
			continue
		}
		isRoot, rootErr := s.appV23IsRootIdentity(member.AgentID)
		if rootErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current CEREBRUM Root state is unavailable.")
			return
		}
		if isRoot {
			continue
		}
		filtered = append(filtered, member)
	}
	writeJSON(w, http.StatusOK, filtered)
}
