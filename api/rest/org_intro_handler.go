package rest

// RFC #443 Phase B2b: owner accept and the org source for the intro wire.
//
//	POST /v1/org/{org_id}/offers/{offer_id}/accept   the owner accepts one open offer
//
// Accepting records the owner's consent bound to this node's current owner
// envelope (revision + digest). The host signs host-accept, and the guest
// signs guest-accept + guest-ack, only while that acceptance still names the
// current envelope. On the guest, accepting also starts the handshake driver;
// the host waits for the guest to connect. The route is the current local
// Root over loopback, like the envelope it binds to.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

// orgOfferAcceptRequestCap bounds the (empty) accept body.
const orgOfferAcceptRequestCap = 1 << 10

// introGuestStarter is the federation surface that runs the guest driver.
type introGuestStarter interface {
	StartIntroGuest(orgID, offerID string) error
}

// OrgOfferAcceptResponse reports the recorded acceptance.
type OrgOfferAcceptResponse struct {
	Acceptance orgpolicy.IntroAcceptance `json:"acceptance"`
	// Handshake is "started" on the guest, "waiting_for_guest" on the host.
	Handshake string `json:"handshake"`
	Reason    string `json:"reason,omitempty"`
}

// refreshIntroOffers computes this node's offers for one org, as GET
// /v1/org/{org_id}/offers does.
func (s *Server) refreshIntroOffers(ctx context.Context, ps *orgpolicy.Store, orgID string) (orgpolicy.OfferList, error) {
	src, ok := s.federation.(orgOfferSource)
	if s.federation == nil || !ok {
		return orgpolicy.OfferList{}, errors.New("the federation transport is not wired on this node")
	}
	ils := s.introducedLinks()
	if ils == nil {
		return orgpolicy.OfferList{}, errors.New("introduced-link state is not configured on this node")
	}
	agreements, err := src.OrgPeerAgreements()
	if err != nil {
		return orgpolicy.OfferList{}, fmt.Errorf("agreement state unavailable: %w", err)
	}
	rootID, rootPub, rootErr := s.currentRoot()
	if rootErr != nil {
		rootID, rootPub = "", nil // the envelope then does not cover; Ready=false says why
	}
	return ps.RefreshOffers(orgpolicy.OfferInputs{
		OrgID: orgID, OwnAgentPub: src.TransportAgentPub(), OwnChainID: src.LocalChainID(),
		RootCredentialID: rootID, RootPub: rootPub, Now: orgPolicyNow(),
		Agreement: func(chainID string) (orgpolicy.PeerAgreement, error) {
			return introducedPeerAgreement(ctx, ils, agreements, chainID)
		},
	})
}

func (s *Server) handleOrgOfferAccept(w http.ResponseWriter, r *http.Request) {
	rootID, _, ok := s.requireCurrentLocalRoot(w, r)
	if !ok {
		return
	}
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	offerID := strings.TrimSpace(chi.URLParam(r, "offer_id"))
	if orgID == "" || !offerIDPattern.MatchString(offerID) {
		writeJSONError(w, http.StatusBadRequest, "org_id and a 64 lowercase hex offer_id are required")
		return
	}
	var ignored struct{}
	if r.ContentLength != 0 && !decodeCappedJSON(w, r, orgOfferAcceptRequestCap, &ignored) {
		return
	}
	list, err := s.refreshIntroOffers(r.Context(), ps, orgID)
	if err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "could not compute offers: "+err.Error())
		return
	}
	if !list.Ready {
		writeJSONError(w, http.StatusConflict, "this node is not ready for introductions: "+list.Reason)
		return
	}
	var offer *orgpolicy.PeerOffer
	for i := range list.Offers {
		if list.Offers[i].OfferID == offerID {
			offer = &list.Offers[i]
			break
		}
	}
	switch {
	case offer == nil:
		writeJSONError(w, http.StatusNotFound, "no such open offer on this node; list offers first")
		return
	case offer.State != orgpolicy.PeerStateOffered:
		writeJSONError(w, http.StatusConflict, "the offer is "+offer.State+"; only an open offer can be accepted")
		return
	case offer.Kind != orgpolicy.OfferKindNew:
		writeJSONError(w, http.StatusNotImplemented, "accepting a rekey offer is not supported yet")
		return
	}
	env, hasEnv, err := ps.LoadEnvelope(orgID)
	if err != nil || !hasEnv {
		writeJSONError(w, http.StatusConflict, "no owner envelope for this org")
		return
	}
	acc := orgpolicy.IntroAcceptance{
		OrgID: orgID, OfferID: offerID, Kind: offer.Kind, Role: offer.Role, PeerAgentPub: offer.PeerAgentPub,
		EnvelopeRev: env.Envelope.Revision, EnvelopeDigest: env.Envelope.Digest(),
		RootCredentialID: rootID, AcceptedAt: orgPolicyNow(),
	}
	if err := ps.SaveIntroAcceptance(acc); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to record the acceptance")
		return
	}
	resp := OrgOfferAcceptResponse{Acceptance: acc, Handshake: "waiting_for_guest"}
	if offer.Role == orgpolicy.RoleGuest {
		resp.Handshake = "started"
		starter, ok := s.federation.(introGuestStarter)
		if !ok {
			resp.Handshake, resp.Reason = "not_started", "the federation transport cannot run the intro handshake"
		} else if sErr := starter.StartIntroGuest(orgID, offerID); sErr != nil {
			// The acceptance stands; the maintenance tick retries the driver.
			resp.Handshake, resp.Reason = "not_started", sErr.Error()
		}
	}
	if s.OnEvent != nil {
		s.OnEvent("governance", "", "", "Intro offer accepted for org "+orgID, map[string]any{
			"action": "fleet_intro_accepted", "org_id": orgID, "offer_id": offerID,
			"role": offer.Role, "peer_agent_pub": offer.PeerAgentPub, "envelope_revision": acc.EnvelopeRev,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// introOrgSourceAdapter is the federation.IntroOrgSource backed by the
// node-local org policy store, resolved lazily like the introduced-link source.
type introOrgSourceAdapter struct{ s *Server }

func (a introOrgSourceAdapter) IntroOffer(ctx context.Context, orgID, offerID string) (*federation.IntroOfferView, error) {
	s := a.s
	ps := s.orgPolicyStore(ctx)
	if ps == nil {
		return nil, errors.New("org policy state is not configured on this node")
	}
	list, err := s.refreshIntroOffers(ctx, ps, orgID)
	if err != nil {
		return nil, err
	}
	if !list.Ready {
		return nil, fmt.Errorf("org %q is not ready for introductions: %s", orgID, list.Reason)
	}
	var offer *orgpolicy.PeerOffer
	for i := range list.Offers {
		if list.Offers[i].OfferID == offerID {
			offer = &list.Offers[i]
			break
		}
	}
	if offer == nil {
		return nil, federation.ErrIntroOfferUnknown
	}
	st, err := ps.LoadAccepted(orgID)
	if err != nil {
		return nil, fmt.Errorf("org policy state unavailable: %w", err)
	}
	rst, roster, err := ps.LoadRoster(orgID)
	if err != nil {
		return nil, fmt.Errorf("org roster state unavailable: %w", err)
	}
	if roster == nil || rst.Revision != list.RosterRevision || rst.Digest != list.RosterDigest {
		return nil, errors.New("the accepted roster changed while reading the offer; retry")
	}
	env, hasEnv, err := ps.LoadEnvelope(orgID)
	if err != nil {
		return nil, fmt.Errorf("owner envelope state unavailable: %w", err)
	}
	if !hasEnv || env.Envelope.Revision != list.EnvelopeRev {
		return nil, errors.New("the owner envelope changed while reading the offer; retry")
	}
	src, _ := s.federation.(orgOfferSource)
	var own, peer *orgpolicy.LinkCard
	for i := range roster.Cards {
		switch roster.Cards[i].AgentPub {
		case src.TransportAgentPub():
			own = &roster.Cards[i]
		case offer.PeerAgentPub:
			peer = &roster.Cards[i]
		}
	}
	if own == nil || peer == nil {
		return nil, errors.New("the roster does not hold both cards of the offer")
	}
	v := &federation.IntroOfferView{
		OrgID: orgID, OrgRootKeyID: st.RootKeyID, OfferID: offerID, Kind: offer.Kind, Role: offer.Role,
		State: offer.State, Reason: offer.Reason, ExpiresAt: offer.ExpiresAt,
		PolicyRevision: st.Revision, PolicyDigest: st.Digest,
		RosterRevision: rst.Revision, RosterDigest: rst.Digest,
		EnvelopeRev: env.Envelope.Revision, EnvelopeDigest: env.Envelope.Digest(),
		Own: introCardOf(own), Peer: introCardOf(peer),
	}
	acc, hasAcc, err := ps.LoadIntroAcceptance(orgID, offerID)
	if err != nil {
		return nil, err
	}
	if hasAcc {
		v.Accepted, v.AcceptedEnvelopeRev, v.AcceptedEnvelopeDigest = true, acc.EnvelopeRev, acc.EnvelopeDigest
	}
	return v, nil
}

func introCardOf(c *orgpolicy.LinkCard) federation.IntroCard {
	routes := append([]string(nil), c.P2PRoutes...)
	sort.Strings(routes)
	return federation.IntroCard{
		AgentPub: c.AgentPub, ChainID: c.ChainID, Pin: c.CASPKIPin, Endpoint: c.Endpoint,
		PeerID: c.P2PPeerID, Routes: routes, CardRev: c.CardRev, CardDigest: c.Digest(),
	}
}

func (a introOrgSourceAdapter) IntroPeerPinKnown(pin string) bool {
	s := a.s
	ps := s.orgPolicyStore(context.Background())
	src, ok := s.federation.(orgOfferSource)
	if ps == nil || s.federation == nil || !ok {
		return false
	}
	orgs, err := ps.FleetOrgs()
	if err != nil {
		return false
	}
	now := orgPolicyNow()
	own := src.TransportAgentPub()
	for _, org := range orgs {
		rst, roster, rErr := ps.LoadRoster(org)
		if rErr != nil || roster == nil || !rst.CurrentAt(now) {
			continue
		}
		for i := range roster.Cards {
			c := &roster.Cards[i]
			if c.CASPKIPin == pin && c.AgentPub != own && c.CurrentAt(now) {
				return true
			}
		}
	}
	return false
}

func (a introOrgSourceAdapter) AcceptedIntroOffers(ctx context.Context) ([]federation.IntroOfferRef, error) {
	ps := a.s.orgPolicyStore(ctx)
	if ps == nil {
		return nil, nil
	}
	accs, err := ps.ListIntroAcceptances()
	if err != nil {
		return nil, err
	}
	out := make([]federation.IntroOfferRef, 0, len(accs))
	for _, acc := range accs {
		out = append(out, federation.IntroOfferRef{OrgID: acc.OrgID, OfferID: acc.OfferID})
	}
	return out, nil
}
