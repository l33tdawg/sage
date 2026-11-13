package rest

// RFC #443 Phase A2: the node-self-minted org link card, the fleet manual QR
// JOIN gate wiring, and Root-signed JOIN overrides.
//
//	POST /v1/org/{org_id}/card        mint (or re-mint) this node's own card
//	GET  /v1/org/{org_id}/card        this node's current card and any notice
//	POST /v1/federation/join-overrides store a Root-signed JOIN override
//	GET  /v1/federation/join-overrides list overrides with their usage history
//
// A card is signed by this node's own transport key. agent_pub, chain_id, the
// CA SPKI pin and the p2p routes come from local runtime state; the operator
// supplies only card_rev, the validity window and the federation endpoint
// (the same endpoint it gives HostCreate). A node mints only for an org whose
// policy it has accepted, and the write is bound to that exact accepted
// policy generation and the per-agent_pub card floor.

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

const (
	// orgCardRequestCap is the card route's request cap, checked before decode.
	orgCardRequestCap = 16 << 10
	// joinOverrideRequestCap is the override route's request cap.
	joinOverrideRequestCap = 16 << 10
	// orgCardIssueSkewSeconds bounds how far issued_at may sit from now.
	orgCardIssueSkewSeconds = 300
)

// orgCardMinter is the federation surface the card route needs.
type orgCardMinter interface {
	MintOrgLinkCard(federation.OrgLinkCardRequest) (*orgpolicy.LinkCard, error)
	TransportAgentPub() string
}

// rootCredentialSource resolves this node's current Root credential.
type rootCredentialSource interface {
	CurrentRootCredential() (string, ed25519.PublicKey, error)
}

// OrgCardMintRequest is the JSON body for POST /v1/org/{org_id}/card.
type OrgCardMintRequest struct {
	CardRev   uint64 `json:"card_rev"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
	Endpoint  string `json:"endpoint,omitempty"`
}

// OrgCardMintResponse reports the decision and the stored card.
type OrgCardMintResponse struct {
	Decision orgpolicy.Decision      `json:"decision"`
	Card     *orgpolicy.LinkCard     `json:"card,omitempty"`
	Notice   *orgpolicy.RosterNotice `json:"notice,omitempty"`
}

// OrgCardStatusResponse is this node's current card for the org.
type OrgCardStatusResponse struct {
	Card    *orgpolicy.LinkCard     `json:"card,omitempty"`
	CardRev uint64                  `json:"card_rev"`
	Digest  string                  `json:"digest,omitempty"`
	Current bool                    `json:"current"`
	Notice  *orgpolicy.RosterNotice `json:"notice,omitempty"`
}

func (s *Server) handleOrgCardMint(w http.ResponseWriter, r *http.Request) {
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	minter, ok := s.federation.(orgCardMinter)
	if s.federation == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, "the federation transport is not wired on this node")
		return
	}
	if !s.requireNodeOperator(w, r) {
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	var req OrgCardMintRequest
	if !decodeCappedJSON(w, r, orgCardRequestCap, &req) {
		return
	}
	now := orgPolicyNow()
	if req.IssuedAt < now-orgCardIssueSkewSeconds || req.IssuedAt > now+orgCardIssueSkewSeconds {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("issued_at must be within %d seconds of now", orgCardIssueSkewSeconds))
		return
	}
	prev, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if prev.Revision == 0 {
		writeJSONError(w, http.StatusConflict, "this node is not enrolled in the org; a card needs an accepted policy")
		return
	}
	card, err := minter.MintOrgLinkCard(federation.OrgLinkCardRequest{
		OrgID: orgID, CardRev: req.CardRev, IssuedAt: req.IssuedAt, ExpiresAt: req.ExpiresAt, Endpoint: req.Endpoint,
	})
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "could not mint card: "+err.Error())
		return
	}
	decision, notice, err := ps.ApplyOwnCard(card, prev, now)
	if err != nil {
		switch {
		case errors.Is(err, orgpolicy.ErrRosterPolicyChanged):
			writeJSONError(w, http.StatusConflict, "accepted org policy changed while minting; retry")
		case errors.Is(err, orgpolicy.ErrRosterStateConflict):
			writeJSONError(w, http.StatusConflict, "card state changed while minting; reload and retry")
		case errors.Is(err, orgpolicy.ErrRosterCardRegression):
			writeJSONError(w, http.StatusConflict, "card_rev is below this node's accepted card")
		case errors.Is(err, orgpolicy.ErrCardInvalid):
			writeJSONError(w, http.StatusBadRequest, "card rejected: "+err.Error())
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to store card")
		}
		return
	}
	switch decision {
	case orgpolicy.DecisionApplied, orgpolicy.DecisionNoOpSameDigest:
		writeJSON(w, http.StatusOK, OrgCardMintResponse{Decision: decision, Card: card})
	default: // DecisionRejectedEquivocation
		if s.OnEvent != nil && notice != nil {
			s.OnEvent("governance", "", "",
				"Org card equivocation: this node minted different content at card_rev "+fmt.Sprint(notice.CardRev),
				map[string]any{
					"action":          "org_card_equivocation",
					"org_id":          notice.OrgID,
					"agent_pub":       notice.AgentPub,
					"card_rev":        notice.CardRev,
					"accepted_digest": notice.AcceptedDigest,
					"conflict_digest": notice.ConflictDigest,
				})
		}
		writeJSON(w, http.StatusConflict, OrgCardMintResponse{Decision: decision, Notice: notice})
	}
}

func (s *Server) handleOrgCardStatus(w http.ResponseWriter, r *http.Request) {
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	minter, ok := s.federation.(orgCardMinter)
	if s.federation == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, "the federation transport is not wired on this node")
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	self := minter.TransportAgentPub()
	rec, hasCard, err := ps.LoadCard(orgID, self)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org card state unavailable")
		return
	}
	notice, hasNotice, err := ps.LoadCardNotice(orgID, self)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org card state unavailable")
		return
	}
	if !hasCard && !hasNotice {
		writeJSONError(w, http.StatusNotFound, "this node holds no card for the org")
		return
	}
	resp := OrgCardStatusResponse{}
	if hasCard {
		c := rec.Card
		resp.Card, resp.CardRev, resp.Digest, resp.Current = &c, rec.CardRev, rec.Digest, c.CurrentAt(orgPolicyNow())
	}
	if hasNotice {
		resp.Notice = notice
	}
	writeJSON(w, http.StatusOK, resp)
}

// JoinOverrideRequest is the JSON body for POST /v1/federation/join-overrides.
type JoinOverrideRequest struct {
	Override *orgpolicy.JoinOverride `json:"override"`
}

// JoinOverrideResponse reports the decision.
type JoinOverrideResponse struct {
	Decision orgpolicy.Decision `json:"decision"`
	Digest   string             `json:"digest"`
}

// JoinOverrideView is one listed override.
type JoinOverrideView struct {
	orgpolicy.JoinOverrideRecord
	Current bool   `json:"current"`
	Status  string `json:"status"` // usable | expired | not_current_root
}

// JoinOverrideListResponse lists overrides.
type JoinOverrideListResponse struct {
	RootCredentialID string             `json:"root_credential_id,omitempty"`
	Overrides        []JoinOverrideView `json:"overrides"`
}

func (s *Server) currentRoot() (string, ed25519.PublicKey, error) {
	src, ok := s.federation.(rootCredentialSource)
	if s.federation == nil || !ok {
		return "", nil, errors.New("the federation transport is not wired on this node")
	}
	return src.CurrentRootCredential()
}

func (s *Server) handleJoinOverrideApply(w http.ResponseWriter, r *http.Request) {
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	var req JoinOverrideRequest
	if !decodeCappedJSON(w, r, joinOverrideRequestCap, &req) {
		return
	}
	if req.Override == nil {
		writeJSONError(w, http.StatusBadRequest, "override is required")
		return
	}
	rootID, rootPub, err := s.currentRoot()
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "current Root credential unavailable: "+err.Error())
		return
	}
	decision, err := ps.ApplyJoinOverride(req.Override, rootID, rootPub, orgPolicyNow())
	if err != nil {
		switch {
		case errors.Is(err, orgpolicy.ErrJoinOverrideInvalid):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, orgpolicy.ErrJoinOverrideStateConflict):
			writeJSONError(w, http.StatusConflict, "override changed concurrently; reload and retry")
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to store override")
		}
		return
	}
	if decision == orgpolicy.DecisionApplied && s.OnEvent != nil {
		o := req.Override
		s.OnEvent("governance", "", "",
			"Fleet JOIN override stored for peer chain "+o.PeerChainID,
			map[string]any{
				"action":         "fleet_join_override_stored",
				"peer_agent_pub": o.PeerAgentPub,
				"peer_chain_id":  o.PeerChainID,
				"revision":       o.Revision,
				"expires_at":     o.ExpiresAt,
				"reason":         o.Reason,
			})
	}
	writeJSON(w, http.StatusOK, JoinOverrideResponse{Decision: decision, Digest: req.Override.Digest()})
}

func (s *Server) handleJoinOverrideList(w http.ResponseWriter, r *http.Request) {
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	recs, err := ps.ListJoinOverrides()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "join override state unavailable")
		return
	}
	rootID, _, rootErr := s.currentRoot()
	if rootErr != nil {
		rootID = ""
	}
	now := orgPolicyNow()
	resp := JoinOverrideListResponse{RootCredentialID: rootID, Overrides: make([]JoinOverrideView, 0, len(recs))}
	for _, rec := range recs {
		v := JoinOverrideView{JoinOverrideRecord: rec}
		switch {
		case rootID == "" || rec.Override.RootCredentialID != rootID:
			v.Status = "not_current_root"
		case !rec.Override.CurrentAt(now):
			v.Status = "expired"
		default:
			v.Status, v.Current = "usable", true
		}
		resp.Overrides = append(resp.Overrides, v)
	}
	writeJSON(w, http.StatusOK, resp)
}

// fleetJoinGate is the federation.JoinPeerGate backed by this node's org
// policy store. A node without an org policy backend is not fleet-managed.
// Any storage failure refuses the JOIN.
func (s *Server) fleetJoinGate(c federation.JoinGateCheck) error {
	ps := s.orgPolicyStore(context.Background())
	if ps == nil {
		return nil
	}
	now := c.Now
	if now.IsZero() {
		now = time.Now()
	}
	d, err := ps.AuthorizeJoinPeer(orgpolicy.JoinPeerRequest{
		Side: c.Side, Stage: c.Stage, SessionID: c.SessionID,
		PeerAgentPub: c.PeerAgentPub, PeerChainID: c.PeerChainID,
		RootCredentialID: c.RootCredentialID, RootPub: c.RootPub,
		Now: now.Unix(),
	})
	if err != nil {
		return fmt.Errorf("%w: org policy state unavailable: %v", federation.ErrJoinPeerNotEligible, err)
	}
	if !d.Allowed {
		return fmt.Errorf("%w: %s", federation.ErrJoinPeerNotEligible, d.Reason)
	}
	if d.OverrideRevision > 0 && s.OnEvent != nil {
		s.OnEvent("governance", "", "",
			"Fleet JOIN override used for peer chain "+c.PeerChainID,
			map[string]any{
				"action":            "fleet_join_override_used",
				"peer_agent_pub":    c.PeerAgentPub,
				"peer_chain_id":     c.PeerChainID,
				"override_revision": d.OverrideRevision,
				"side":              c.Side,
				"stage":             c.Stage,
			})
	}
	return nil
}
