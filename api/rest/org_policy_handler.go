package rest

// Org policy updates and the operator-visible equivocation surface (RFC #443
// point 3).
//
//	POST /v1/org/{org_id}/policy  deliver a newer signed org policy to this node
//	GET  /v1/org/{org_id}/policy  the accepted state plus any equivocation notice
//
// Authority comes from the policy signature, not from the request signer: any
// authenticated caller may deliver a policy (a peer relaying it, or the owner),
// because only a policy validly signed by the org's current key can change
// anything. The trust root is the key that signed the policy this node already
// accepted (established at enrollment); a rotation to a successor key must be
// proven by an attestation chain rooted there.
//
// Ordering is enforced by orgpolicy.ApplyPolicy: a strictly newer revision is
// applied in one durable write, an identical re-delivery is a no-op, an older
// revision is refused as a rollback, and the SAME revision with a DIFFERENT
// digest — both validly signed — is equivocation. Equivocation never overwrites
// the accepted policy: the notice is persisted durably (so it survives a
// restart and is served by GET) and raised live to the dashboard as a
// governance event through OnEvent.

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

// OrgPolicyUpdateRequest is the JSON body for POST /v1/org/{org_id}/policy.
type OrgPolicyUpdateRequest struct {
	Policy *orgpolicy.Policy `json:"policy"`
	// Attestations is the optional rotation chain, rooted at the signer of the
	// currently accepted policy, proving a successor key now signs.
	Attestations []orgpolicy.RotationAttestation `json:"attestations,omitempty"`
}

// OrgPolicyUpdateResponse reports the ordering decision and resulting state.
type OrgPolicyUpdateResponse struct {
	Decision     orgpolicy.Decision            `json:"decision"`
	Accepted     orgpolicy.AcceptedState       `json:"accepted"`
	Equivocation *orgpolicy.EquivocationNotice `json:"equivocation,omitempty"`
}

// OrgPolicyStatusResponse is the operator view of an org's policy on this node.
type OrgPolicyStatusResponse struct {
	Accepted     orgpolicy.AcceptedState       `json:"accepted"`
	Current      bool                          `json:"current"`    // within its validity window now
	Operations   []orgpolicy.GateResult        `json:"operations"` // what may proceed now (RFC #443 point 4)
	Equivocation *orgpolicy.EquivocationNotice `json:"equivocation,omitempty"`
}

// orgPolicyNow is the clock for org-policy freshness; a variable so tests can
// move time without signing policies against the wall clock.
var orgPolicyNow = func() int64 { return time.Now().Unix() }

// writeOrgPolicyHold applies the per-operation offline/expiry gate for an org
// this node holds a fleet policy for. It writes a 409 and returns true when op
// must hold. An org with no fleet policy on this node is not fleet-managed and
// is left on its existing path untouched, so the gate never locks out a brain
// that was never enrolled.
func (s *Server) writeOrgPolicyHold(w http.ResponseWriter, orgID string, op orgpolicy.Operation) bool {
	ps := s.orgPolicyStore(context.Background())
	if ps == nil || orgID == "" {
		return false
	}
	accepted, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Org policy state unavailable",
			"The org fleet policy could not be read; refusing to decide without it.")
		return true
	}
	if accepted.Revision == 0 {
		return false
	}
	if g := orgpolicy.Gate(accepted, op, orgPolicyNow(), false); !g.Allowed {
		writeProblem(w, http.StatusConflict, "Held by org policy",
			"Org "+orgID+": "+g.Reason+". Deliver a current signed org policy to proceed.")
		return true
	}
	return false
}

// handleOrgPolicyUpdate admits a delivered policy against the accepted state.
func (s *Server) handleOrgPolicyUpdate(w http.ResponseWriter, r *http.Request) {
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
	var req OrgPolicyUpdateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Policy == nil {
		writeJSONError(w, http.StatusBadRequest, "policy is required")
		return
	}
	if req.Policy.OrgID != orgID {
		writeJSONError(w, http.StatusBadRequest, "policy org does not match this org")
		return
	}

	prev, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if prev.Revision == 0 {
		// The first policy is only ever established by enrollment, where the
		// owner's token pins the signer. Accepting one here would let any caller
		// choose the trust root.
		writeJSONError(w, http.StatusConflict, "this node is not enrolled in the org; the first policy is set by enrollment")
		return
	}

	root, err := auth.AgentIDToPublicKey(prev.IssuerKeyID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "accepted policy has an invalid issuer key")
		return
	}
	authorized, err := orgpolicy.ResolveSigner(root, req.Attestations)
	if err != nil {
		writeJSONError(w, http.StatusForbidden, "rotation chain is not rooted at the accepted policy signer")
		return
	}

	accepted, decision, notice, err := ps.ApplyPolicy(req.Policy, authorized)
	if err != nil {
		if req.Policy.Verify(authorized) != nil {
			writeJSONError(w, http.StatusForbidden, "policy is not signed by the org's current key")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to apply org policy")
		return
	}

	resp := OrgPolicyUpdateResponse{Decision: decision, Accepted: accepted, Equivocation: notice}
	switch decision {
	case orgpolicy.DecisionApplied, orgpolicy.DecisionNoOpSameDigest:
		writeJSON(w, http.StatusOK, resp)
	case orgpolicy.DecisionRejectedEquivocation:
		if s.OnEvent != nil && notice != nil {
			s.OnEvent("governance", "", "",
				"Org policy equivocation: org "+orgID+" signed two different policies at the same revision",
				map[string]any{
					"action":          "org_policy_equivocation",
					"org_id":          notice.OrgID,
					"revision":        notice.Revision,
					"accepted_digest": notice.AcceptedDigest,
					"conflict_digest": notice.ConflictDigest,
					"issuer_key_id":   notice.IssuerKeyID,
					"key_epoch":       notice.KeyEpoch,
				})
		}
		writeJSON(w, http.StatusConflict, resp)
	default: // DecisionRejectedRollback
		writeJSON(w, http.StatusConflict, resp)
	}
}

// handleOrgPolicyStatus returns the accepted state and the durable equivocation
// notice (if any), so the alarm stays visible after a restart.
func (s *Server) handleOrgPolicyStatus(w http.ResponseWriter, r *http.Request) {
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
	accepted, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if accepted.Revision == 0 {
		writeJSONError(w, http.StatusNotFound, "this node holds no policy for the org")
		return
	}
	notice, ok, err := ps.LoadEquivocation(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	now := orgPolicyNow()
	resp := OrgPolicyStatusResponse{Accepted: accepted, Current: accepted.CurrentAt(now)}
	for _, op := range orgpolicy.Operations {
		resp.Operations = append(resp.Operations, orgpolicy.Gate(accepted, op, now, false))
	}
	if ok {
		resp.Equivocation = notice
	}
	writeJSON(w, http.StatusOK, resp)
}
