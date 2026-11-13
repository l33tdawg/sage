package rest

// Explicit local Root JOIN override (RFC #443 Phase A2).
//
//	PUT /v1/org/{org_id}/join-overrides  apply a Root-signed override revision
//	GET /v1/org/{org_id}/join-overrides  list overrides and retained usage
//
// The record itself is signed by the exact current Root credential. The HTTP
// caller must also be that local Root; ordinary agents and retired Root
// credentials cannot create or inspect fleet exceptions.

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

type OrgJoinOverrideUpdateRequest struct {
	Override *orgpolicy.JoinOverride `json:"override"`
}

type OrgJoinOverrideView struct {
	Override orgpolicy.JoinOverride      `json:"override"`
	Current  bool                        `json:"current"`
	Usage    []orgpolicy.JoinOverrideUse `json:"usage"`
}

type OrgJoinOverridesResponse struct {
	Overrides []OrgJoinOverrideView `json:"overrides"`
}

func (s *Server) handleOrgJoinOverridePut(w http.ResponseWriter, r *http.Request) {
	rootID, rootPub, ok := s.requireCurrentLocalRoot(w, r)
	if !ok {
		return
	}
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	var req OrgJoinOverrideUpdateRequest
	if !decodeCappedJSON(w, r, orgpolicy.MaxJoinOverrideRequestBytes, &req) {
		return
	}
	if req.Override == nil {
		writeJSONError(w, http.StatusBadRequest, "override is required")
		return
	}
	if req.Override.OrgID != orgID {
		writeJSONError(w, http.StatusBadRequest, "override org does not match this org")
		return
	}
	managed, err := s.orgIsManaged(r, orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org enrollment state unavailable")
		return
	}
	if !managed {
		writeJSONError(w, http.StatusConflict, "this node has no fleet enrollment outcome for the org")
		return
	}
	accepted, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if accepted.Revision == 0 {
		writeJSONError(w, http.StatusConflict, "this node has no accepted policy for the org")
		return
	}
	if verifyErr := req.Override.Verify(rootPub); verifyErr != nil {
		writeJSONError(w, http.StatusForbidden, verifyErr.Error())
		return
	}
	if req.Override.RootCredentialID != rootID {
		writeJSONError(w, http.StatusForbidden, "override is not signed by the current Root credential")
		return
	}
	now := orgPolicyNow()
	if !req.Override.CurrentAt(now) {
		writeJSONError(w, http.StatusForbidden, "override is expired or not yet valid")
		return
	}
	applied, decision, err := ps.ApplyJoinOverride(*req.Override, rootPub)
	if err != nil {
		switch {
		case errors.Is(err, orgpolicy.ErrJoinOverrideStateChanged):
			writeJSONError(w, http.StatusConflict, "join override changed concurrently; reload and retry")
		case errors.Is(err, orgpolicy.ErrJoinOverrideInvalid):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to persist join override")
		}
		return
	}
	switch decision {
	case orgpolicy.DecisionApplied, orgpolicy.DecisionNoOpSameDigest:
		if decision == orgpolicy.DecisionApplied && s.OnEvent != nil {
			s.OnEvent("governance", "", "",
				"Root JOIN override applied for peer "+applied.PeerChainID,
				map[string]any{
					"action":             "org_join_override_applied",
					"org_id":             applied.OrgID,
					"peer_agent_pub":     applied.PeerAgentPub,
					"peer_chain_id":      applied.PeerChainID,
					"override_revision":  applied.Revision,
					"root_credential_id": applied.RootCredentialID,
					"expires_at":         applied.ExpiresAt,
				})
		}
	case orgpolicy.DecisionRejectedRollback:
		writeJSONError(w, http.StatusConflict, "override revision regresses the current override")
		return
	case orgpolicy.DecisionRejectedEquivocation:
		writeJSONError(w, http.StatusConflict, "the same override revision has different content")
		return
	}
	usage, err := ps.LoadJoinOverrideUsage(orgID, applied.PeerAgentPub, applied.PeerChainID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "join override usage unavailable")
		return
	}
	writeJSON(w, http.StatusOK, OrgJoinOverrideView{
		Override: applied, Current: applied.CurrentAt(now), Usage: usage,
	})
}

func (s *Server) handleOrgJoinOverrideList(w http.ResponseWriter, r *http.Request) {
	rootID, rootPub, ok := s.requireCurrentLocalRoot(w, r)
	if !ok {
		return
	}
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	managed, err := s.orgIsManaged(r, orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org enrollment state unavailable")
		return
	}
	if !managed {
		writeJSONError(w, http.StatusNotFound, "this node has no fleet enrollment outcome for the org")
		return
	}
	overrides, err := ps.ListJoinOverrides(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "join override state unavailable")
		return
	}
	now := orgPolicyNow()
	resp := OrgJoinOverridesResponse{Overrides: make([]OrgJoinOverrideView, 0, len(overrides))}
	for _, override := range overrides {
		usage, err := ps.LoadJoinOverrideUsage(orgID, override.PeerAgentPub, override.PeerChainID)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "join override usage unavailable")
			return
		}
		resp.Overrides = append(resp.Overrides, OrgJoinOverrideView{
			Override: override,
			Current: override.CurrentAt(now) &&
				override.RootCredentialID == rootID &&
				override.Verify(rootPub) == nil,
			Usage: usage,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) requireCurrentLocalRoot(w http.ResponseWriter, r *http.Request) (string, []byte, bool) {
	if s.badgerStore == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "consensus Root state is unavailable")
		return "", nil, false
	}
	root, err := s.badgerStore.GetAppV23Root()
	if err != nil || root == nil || root.CredentialID == "" {
		writeJSONError(w, http.StatusServiceUnavailable, "current CEREBRUM Root is unavailable")
		return "", nil, false
	}
	if !isAppV23LoopbackControlRequest(r) {
		writeJSONError(w, http.StatusForbidden, "join overrides are available only through localhost on this machine")
		return "", nil, false
	}
	if middleware.ContextAgentID(r.Context()) != root.CredentialID {
		writeJSONError(w, http.StatusForbidden, "current local CEREBRUM Root is required")
		return "", nil, false
	}
	pub, err := auth.AgentIDToPublicKey(root.CredentialID)
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "current CEREBRUM Root credential is invalid")
		return "", nil, false
	}
	return root.CredentialID, pub, true
}
