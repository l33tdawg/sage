package rest

// Org roster delivery (RFC #443 Phase A).
//
//	POST /v1/org/{org_id}/roster  deliver a newer signed org roster to this node
//	GET  /v1/org/{org_id}/roster  the accepted roster state, body and any notice
//
// Like policy delivery, authority comes from the roster signature, not the
// request signer: only a roster signed by a key currently authorized under the
// node's accepted policy trust root (enrollment pin + rotation chain) can change
// anything, and its scope must equal the accepted policy scope. Ordering,
// equivocation and card-regression are enforced by orgpolicy.ApplyRoster in one
// durable write. A roster never activates a link: it only records which signed
// cards the org currently vouches for.

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

// OrgRosterUpdateRequest is the JSON body for POST /v1/org/{org_id}/roster.
type OrgRosterUpdateRequest struct {
	Roster       *orgpolicy.Roster               `json:"roster"`
	Attestations []orgpolicy.RotationAttestation `json:"attestations,omitempty"`
}

// OrgRosterUpdateResponse reports the ordering decision and resulting state.
type OrgRosterUpdateResponse struct {
	Decision orgpolicy.Decision      `json:"decision"`
	Accepted orgpolicy.RosterState   `json:"accepted"`
	Notice   *orgpolicy.RosterNotice `json:"notice,omitempty"`
}

// OrgRosterStatusResponse is the operator view of the accepted roster.
type OrgRosterStatusResponse struct {
	Accepted orgpolicy.RosterState   `json:"accepted"`
	Current  bool                    `json:"current"`
	Roster   *orgpolicy.Roster       `json:"roster,omitempty"`
	Notice   *orgpolicy.RosterNotice `json:"notice,omitempty"`
}

func (s *Server) handleOrgRosterUpdate(w http.ResponseWriter, r *http.Request) {
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
	var req OrgRosterUpdateRequest
	if !decodeCappedJSON(w, r, orgpolicy.MaxRosterRequestBytes, &req) {
		return
	}
	if req.Roster == nil {
		writeJSONError(w, http.StatusBadRequest, "roster is required")
		return
	}
	if req.Roster.OrgID != orgID {
		writeJSONError(w, http.StatusBadRequest, "roster org does not match this org")
		return
	}
	prev, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if prev.Revision == 0 {
		writeJSONError(w, http.StatusConflict, "this node is not enrolled in the org; a roster needs an accepted policy")
		return
	}
	authority, ok := resolveOrgSigners(w, prev, orgID, req.Attestations)
	if !ok {
		return
	}
	signer, err := orgpolicy.AuthorizedSignerForRoster(req.Roster, authority.signers)
	if err != nil {
		writeJSONError(w, http.StatusForbidden, "roster is not signed by the org's currently authorized key/epoch: "+err.Error())
		return
	}
	accepted, decision, notice, err := ps.ApplyRoster(req.Roster, signer.PublicKey, prev)
	if err != nil {
		switch {
		case errors.Is(err, orgpolicy.ErrRosterPolicyChanged):
			writeJSONError(w, http.StatusConflict, "accepted org policy changed during delivery; re-resolve the signer and retry")
		case errors.Is(err, orgpolicy.ErrRosterStateConflict):
			writeJSONError(w, http.StatusConflict, "accepted roster changed during delivery; reload and retry")
		case errors.Is(err, orgpolicy.ErrRosterCardRegression):
			writeJSONError(w, http.StatusConflict, err.Error())
		case errors.Is(err, orgpolicy.ErrRosterInvalid):
			writeJSONError(w, http.StatusBadRequest, "roster rejected: "+err.Error())
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to apply org roster")
		}
		return
	}
	resp := OrgRosterUpdateResponse{Decision: decision, Accepted: accepted, Notice: notice}
	switch decision {
	case orgpolicy.DecisionApplied, orgpolicy.DecisionNoOpSameDigest:
		writeJSON(w, http.StatusOK, resp)
	case orgpolicy.DecisionRejectedEquivocation:
		if s.OnEvent != nil && notice != nil {
			s.OnEvent("governance", "", "",
				"Org roster equivocation: org "+orgID+" signed conflicting "+notice.Kind+" content",
				map[string]any{
					"action":          "org_roster_equivocation",
					"kind":            notice.Kind,
					"org_id":          notice.OrgID,
					"revision":        notice.Revision,
					"agent_pub":       notice.AgentPub,
					"card_rev":        notice.CardRev,
					"accepted_digest": notice.AcceptedDigest,
					"conflict_digest": notice.ConflictDigest,
				})
		}
		writeJSON(w, http.StatusConflict, resp)
	default: // DecisionRejectedRollback
		writeJSON(w, http.StatusConflict, resp)
	}
}

func (s *Server) handleOrgRosterStatus(w http.ResponseWriter, r *http.Request) {
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
	st, body, err := ps.LoadRoster(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org roster state unavailable")
		return
	}
	notice, hasNotice, err := ps.LoadRosterNotice(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org roster state unavailable")
		return
	}
	if st.Revision == 0 && !hasNotice {
		writeJSONError(w, http.StatusNotFound, "this node holds no roster for the org")
		return
	}
	resp := OrgRosterStatusResponse{Accepted: st, Current: st.CurrentAt(orgPolicyNow()), Roster: body}
	if hasNotice {
		resp.Notice = notice
	}
	writeJSON(w, http.StatusOK, resp)
}
