package rest

// Node-self-mint link card (RFC #443 Phase A2).
//
//	POST /v1/org/{org_id}/card  mint/update this node's signed card
//	GET  /v1/org/{org_id}/card  read the current accepted card floor
//
// The request can select only card_rev and an endpoint. Agent identity, chain,
// CA pin, P2P peer ID/routes, issue time and expiry are derived from live node
// state by the federation transport.

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

type orgLinkCardMinter interface {
	MintOrgLinkCard(orgID, endpoint string, cardRev uint64, issuedAt int64) (orgpolicy.LinkCard, error)
}

// OrgCardMintRequest is deliberately smaller than LinkCard. Unknown fields are
// ignored by the decoder and can never override the minted card.
type OrgCardMintRequest struct {
	CardRev  uint64 `json:"card_rev"`
	Endpoint string `json:"endpoint"`
}

// OrgCardResponse reports the ordering decision and accepted floor.
type OrgCardResponse struct {
	Decision orgpolicy.Decision      `json:"decision"`
	Card     *orgpolicy.CardRecord   `json:"card,omitempty"`
	Current  bool                    `json:"current"`
	Notice   *orgpolicy.RosterNotice `json:"notice,omitempty"`
}

func (s *Server) handleOrgCardMint(w http.ResponseWriter, r *http.Request) {
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	if s.orgCardMinter == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "federation transport is not configured on this node")
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	var req OrgCardMintRequest
	if !decodeCappedJSON(w, r, orgpolicy.MaxLinkCardRequestBytes, &req) {
		return
	}
	req.Endpoint = strings.TrimSpace(req.Endpoint)
	if req.CardRev == 0 {
		writeJSONError(w, http.StatusBadRequest, "card_rev must be >= 1")
		return
	}
	accepted, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if accepted.Revision == 0 {
		writeJSONError(w, http.StatusConflict, "this node is not enrolled in the org; an accepted policy is required")
		return
	}
	managed, err := s.orgIsManaged(r, orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org enrollment state unavailable")
		return
	}
	if !managed {
		writeJSONError(w, http.StatusConflict, "this node has no durable enrollment outcome for the org")
		return
	}

	now := orgPolicyNow()
	card, err := s.orgCardMinter.MintOrgLinkCard(orgID, req.Endpoint, req.CardRev, now)
	if err != nil {
		if errors.Is(err, federation.ErrLinkCardRuntimeUnavailable) {
			writeJSONError(w, http.StatusServiceUnavailable, "card runtime state is unavailable: "+err.Error())
			return
		}
		writeJSONError(w, http.StatusBadRequest, "card runtime state rejected: "+err.Error())
		return
	}
	if card.OrgID != orgID {
		writeJSONError(w, http.StatusInternalServerError, "card minter returned the wrong org")
		return
	}
	localAgentPub := s.localOrgCardAgentPub()
	if localAgentPub == "" {
		writeJSONError(w, http.StatusServiceUnavailable, "local federation identity is unavailable")
		return
	}
	if card.AgentPub != localAgentPub {
		writeJSONError(w, http.StatusInternalServerError, "card minter returned the wrong local agent identity")
		return
	}
	rec, decision, notice, err := ps.ApplyLocalCard(card, localAgentPub, accepted)
	if err != nil {
		switch {
		case errors.Is(err, orgpolicy.ErrCardPolicyChanged):
			writeJSONError(w, http.StatusConflict, "accepted org policy changed while minting; retry against the current policy")
		case errors.Is(err, orgpolicy.ErrCardStateConflict):
			writeJSONError(w, http.StatusConflict, "card floor changed concurrently; reload and retry")
		case errors.Is(err, orgpolicy.ErrCardRegression):
			writeJSONError(w, http.StatusConflict, err.Error())
		case errors.Is(err, orgpolicy.ErrCardInvalid):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to persist link card")
		}
		return
	}
	resp := OrgCardResponse{
		Decision: decision,
		Card:     &rec,
		Current:  rec.Card.CurrentAt(now),
		Notice:   notice,
	}
	if decision == orgpolicy.DecisionRejectedEquivocation {
		if s.OnEvent != nil && notice != nil {
			s.OnEvent("governance", "", "",
				"Org link card equivocation: node signed different cards at the same revision",
				map[string]any{
					"action":          "org_link_card_equivocation",
					"org_id":          notice.OrgID,
					"agent_pub":       notice.AgentPub,
					"card_rev":        notice.CardRev,
					"accepted_digest": notice.AcceptedDigest,
					"conflict_digest": notice.ConflictDigest,
				})
		}
		writeJSON(w, http.StatusConflict, resp)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleOrgCardStatus(w http.ResponseWriter, r *http.Request) {
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
		writeJSONError(w, http.StatusNotFound, "this node has no enrollment outcome for the org")
		return
	}
	agentPub := s.localOrgCardAgentPub()
	if agentPub == "" {
		writeJSONError(w, http.StatusServiceUnavailable, "local federation identity is unavailable")
		return
	}
	rec, ok, err := ps.LoadCard(orgID, agentPub)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org card state unavailable")
		return
	}
	notice, hasNotice, err := ps.LoadCardNotice(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org card state unavailable")
		return
	}
	if !ok && !hasNotice {
		writeJSONError(w, http.StatusNotFound, "this node has no card for the org")
		return
	}
	resp := OrgCardResponse{Current: ok && rec.Card.CurrentAt(orgPolicyNow())}
	if ok {
		resp.Card = rec
	}
	if hasNotice {
		resp.Notice = notice
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) orgIsManaged(r *http.Request, orgID string) (bool, error) {
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		return false, nil
	}
	orgs, err := ps.ManagedOrganizations()
	if err != nil {
		return false, err
	}
	for _, org := range orgs {
		if org == orgID {
			return true, nil
		}
	}
	return false, nil
}

// localOrgCardAgentPub is supplied by the federation minter when available.
func (s *Server) localOrgCardAgentPub() string {
	type localIdentity interface {
		LocalFederationAgentPub() string
	}
	if identity, ok := s.orgCardMinter.(localIdentity); ok {
		return identity.LocalFederationAgentPub()
	}
	if identity, ok := s.federation.(localIdentity); ok {
		return identity.LocalFederationAgentPub()
	}
	return ""
}
