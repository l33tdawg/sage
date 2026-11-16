package rest

// RFC #443 Phase B1: the owner link envelope, deterministic intro offers, and
// the introduced-link diagnostics, plus the federation wiring for the
// fail-closed introduced-link ActiveAgreement hook and the envelope sharing cap.
//
//	POST /v1/org/{org_id}/envelope                       store a Root-signed owner envelope
//	GET  /v1/org/{org_id}/envelope                       the current envelope, its status and any notice
//	GET  /v1/org/{org_id}/offers                         this node's per-peer offers for the org
//	POST /v1/org/{org_id}/offers/{offer_id}/reoffer      restart an offer's local 72 h window
//	GET  /v1/federation/introduced-links                 introduced-link rows and their history
//
// Nothing here signs, broadcasts or activates a link. Phase B only accepts
// envelope mode "ask"; the intro handshake that acts on an offer is Phase B2.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	// orgEnvelopeRequestCap is the envelope route's request cap, checked before
	// decode. 64 domains of at most 512 bytes plus framing fit with headroom.
	orgEnvelopeRequestCap = 64 << 10
	// orgReofferRequestCap bounds the (empty) re-offer body.
	orgReofferRequestCap = 1 << 10
)

var offerIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// introducedLinkStore is the node-local SQLite surface for introduced links.
type introducedLinkStore interface {
	LatestIntroducedLinkByChain(ctx context.Context, remoteChainID string) (*store.IntroducedLink, error)
	LatestIntroducedLinkStates(ctx context.Context) (map[string]string, error)
	ListIntroducedLinks(ctx context.Context, remoteChainID string) ([]store.IntroducedLink, error)
	ListIntroducedLinkHistory(ctx context.Context, linkID string) ([]store.IntroducedLinkHistory, error)
}

// orgOfferSource is the federation surface the offers route needs.
type orgOfferSource interface {
	TransportAgentPub() string
	LocalChainID() string
	OrgPeerAgreements() (map[string]string, error)
}

func (s *Server) introducedLinks() introducedLinkStore {
	if s.orgPolicyState == nil {
		return nil
	}
	ils, ok := s.orgPolicyState.(introducedLinkStore)
	if !ok {
		return nil
	}
	return ils
}

// introducedLinkSourceAdapter resolves the node-local store lazily, so the
// federation hook works whichever of SetFederation / the store wiring runs
// first. A node with no node-local store has no introduced links.
type introducedLinkSourceAdapter struct{ s *Server }

func (a introducedLinkSourceAdapter) LatestIntroducedLinkByChain(ctx context.Context, remoteChainID string) (*store.IntroducedLink, error) {
	ils := a.s.introducedLinks()
	if ils == nil {
		return nil, nil
	}
	return ils.LatestIntroducedLinkByChain(ctx, remoteChainID)
}

func (a introducedLinkSourceAdapter) LatestIntroducedLinkStates(ctx context.Context) (map[string]string, error) {
	ils := a.s.introducedLinks()
	if ils == nil {
		return map[string]string{}, nil
	}
	return ils.LatestIntroducedLinkStates(ctx)
}

// introducedSharingCap is the federation.IntroducedSharingCap backed by the
// org's owner envelope under the current Root.
func (s *Server) introducedSharingCap(orgID, remoteChainID string, domains []string, clearance uint8) error {
	ps := s.orgPolicyStore(context.Background())
	if ps == nil {
		return errors.New("org policy state is not configured on this node")
	}
	rec, ok, err := ps.LoadEnvelope(orgID)
	if err != nil {
		return fmt.Errorf("owner envelope state unavailable: %w", err)
	}
	if !ok {
		return fmt.Errorf("no owner envelope for org %q", orgID)
	}
	st, err := ps.LoadAccepted(orgID)
	if err != nil {
		return fmt.Errorf("org policy state unavailable: %w", err)
	}
	rst, _, err := ps.LoadRoster(orgID)
	if err != nil {
		return fmt.Errorf("org roster state unavailable: %w", err)
	}
	rootID, rootPub, err := s.currentRoot()
	if err != nil {
		return fmt.Errorf("current Root credential unavailable: %w", err)
	}
	env := rec.Envelope
	if covered, why := env.Covers(orgpolicy.EnvelopeContext{
		RootCredentialID: rootID, RootPub: rootPub, Policy: st, RosterScope: rst.Scope, Now: orgPolicyNow(),
	}); !covered {
		return errors.New(why)
	}
	if allowed, why := env.AllowsSharing(domains, clearance); !allowed {
		return fmt.Errorf("%w: %s (link to %s)", federation.ErrSharingOutsideEnvelope, why, remoteChainID)
	}
	return nil
}

// OrgEnvelopeRequest is the JSON body for POST /v1/org/{org_id}/envelope.
type OrgEnvelopeRequest struct {
	Envelope *orgpolicy.LinkEnvelope `json:"envelope"`
}

// OrgEnvelopeResponse reports the decision.
type OrgEnvelopeResponse struct {
	Decision orgpolicy.Decision        `json:"decision"`
	Digest   string                    `json:"digest"`
	Notice   *orgpolicy.EnvelopeNotice `json:"notice,omitempty"`
}

// OrgEnvelopeStatusResponse is the current envelope for an org.
type OrgEnvelopeStatusResponse struct {
	Envelope *orgpolicy.EnvelopeRecord `json:"envelope,omitempty"`
	// Status is covering | not_covering. Reason says why it does not cover.
	Status string                    `json:"status,omitempty"`
	Reason string                    `json:"reason,omitempty"`
	Notice *orgpolicy.EnvelopeNotice `json:"notice,omitempty"`
}

func (s *Server) handleOrgEnvelopeApply(w http.ResponseWriter, r *http.Request) {
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
	var req OrgEnvelopeRequest
	if !decodeCappedJSON(w, r, orgEnvelopeRequestCap, &req) {
		return
	}
	if req.Envelope == nil {
		writeJSONError(w, http.StatusBadRequest, "envelope is required")
		return
	}
	if req.Envelope.OrgID != orgID {
		writeJSONError(w, http.StatusBadRequest, "envelope org_id does not match the route")
		return
	}
	st, err := ps.LoadAccepted(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
		return
	}
	if st.Revision == 0 {
		writeJSONError(w, http.StatusConflict, "this node is not enrolled in the org; an envelope needs an accepted policy")
		return
	}
	decision, notice, err := ps.ApplyEnvelope(req.Envelope, rootID, rootPub, st, orgPolicyNow())
	if err != nil {
		switch {
		case errors.Is(err, orgpolicy.ErrEnvelopeInvalid):
			writeJSONError(w, http.StatusBadRequest, err.Error())
		case errors.Is(err, orgpolicy.ErrEnvelopeEquivocation):
			s.emitEnvelopeEvent("fleet_envelope_equivocation", "Owner envelope equivocation for org "+orgID, req.Envelope, notice)
			writeJSON(w, http.StatusConflict, OrgEnvelopeResponse{Decision: decision, Digest: req.Envelope.Digest(), Notice: notice})
		case errors.Is(err, orgpolicy.ErrEnvelopeRollback):
			writeJSONError(w, http.StatusConflict, err.Error())
		case errors.Is(err, orgpolicy.ErrEnvelopePolicyChanged), errors.Is(err, orgpolicy.ErrEnvelopeStateConflict):
			writeJSONError(w, http.StatusConflict, err.Error()+"; reload and retry")
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to store owner envelope")
		}
		return
	}
	if decision == orgpolicy.DecisionApplied {
		s.emitEnvelopeEvent("fleet_envelope_stored", "Owner envelope stored for org "+orgID, req.Envelope, nil)
	}
	writeJSON(w, http.StatusOK, OrgEnvelopeResponse{Decision: decision, Digest: req.Envelope.Digest()})
}

func (s *Server) emitEnvelopeEvent(action, msg string, e *orgpolicy.LinkEnvelope, n *orgpolicy.EnvelopeNotice) {
	if s.OnEvent == nil {
		return
	}
	data := map[string]any{
		"action":          action,
		"org_id":          e.OrgID,
		"revision":        e.Revision,
		"allowed_domains": e.AllowedDomains,
		"max_clearance":   e.MaxClearance,
		"expires_at":      e.ExpiresAt,
	}
	if n != nil {
		data["accepted_digest"] = n.AcceptedDigest
		data["conflict_digest"] = n.ConflictDigest
	}
	s.OnEvent("governance", "", "", msg, data)
}

func (s *Server) handleOrgEnvelopeStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
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
	rec, hasEnv, err := ps.LoadEnvelope(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "owner envelope state unavailable")
		return
	}
	notice, hasNotice, err := ps.LoadEnvelopeNotice(orgID)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "owner envelope state unavailable")
		return
	}
	if !hasEnv && !hasNotice {
		writeJSONError(w, http.StatusNotFound, "this node holds no owner envelope for the org")
		return
	}
	resp := OrgEnvelopeStatusResponse{}
	if hasNotice {
		resp.Notice = notice
	}
	if hasEnv {
		resp.Envelope = rec
		resp.Status, resp.Reason = "covering", ""
		st, sErr := ps.LoadAccepted(orgID)
		rst, _, rErr := ps.LoadRoster(orgID)
		rootID, rootPub, rootErr := s.currentRoot()
		switch {
		case sErr != nil || rErr != nil:
			writeJSONError(w, http.StatusInternalServerError, "org policy state unavailable")
			return
		case rootErr != nil:
			resp.Status, resp.Reason = "not_covering", "current Root credential unavailable"
		default:
			env := rec.Envelope
			if covered, why := env.Covers(orgpolicy.EnvelopeContext{
				RootCredentialID: rootID, RootPub: rootPub, Policy: st, RosterScope: rst.Scope, Now: orgPolicyNow(),
			}); !covered {
				resp.Status, resp.Reason = "not_covering", why
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleOrgOffers(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	ps := s.orgPolicyStore(r.Context())
	if ps == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "org policy state is not configured on this node")
		return
	}
	src, ok := s.federation.(orgOfferSource)
	if s.federation == nil || !ok {
		writeJSONError(w, http.StatusNotImplemented, "the federation transport is not wired on this node")
		return
	}
	ils := s.introducedLinks()
	if ils == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "introduced-link state is not configured on this node")
		return
	}
	orgID := strings.TrimSpace(chi.URLParam(r, "org_id"))
	if orgID == "" {
		writeJSONError(w, http.StatusBadRequest, "org_id is required")
		return
	}
	pins, err := src.OrgPeerAgreements()
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "agreement state unavailable")
		return
	}
	rootID, rootPub, rootErr := s.currentRoot()
	if rootErr != nil {
		rootID, rootPub = "", nil // the envelope then does not cover; Ready=false says why
	}
	ctx := r.Context()
	list, err := ps.RefreshOffers(orgpolicy.OfferInputs{
		OrgID: orgID, OwnAgentPub: src.TransportAgentPub(), OwnChainID: src.LocalChainID(),
		RootCredentialID: rootID, RootPub: rootPub, Now: orgPolicyNow(),
		Agreement: func(chainID string) (orgpolicy.PeerAgreement, error) {
			pin, exists := pins[chainID]
			ag := orgpolicy.PeerAgreement{Exists: exists, PinHex: pin}
			row, lErr := ils.LatestIntroducedLinkByChain(ctx, chainID)
			if lErr != nil {
				return ag, lErr
			}
			if row != nil {
				ag.LinkID, ag.LinkState = row.LinkID, row.State
			}
			return ag, nil
		},
	})
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "could not compute offers: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleOrgReoffer(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
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
	if r.ContentLength != 0 && !decodeCappedJSON(w, r, orgReofferRequestCap, &ignored) {
		return
	}
	rec, err := ps.Reoffer(orgID, offerID, orgPolicyNow())
	switch {
	case errors.Is(err, orgpolicy.ErrOfferNotFound):
		writeJSONError(w, http.StatusNotFound, "no such offer on this node; list offers first")
		return
	case errors.Is(err, orgpolicy.ErrOfferStateConflict):
		writeJSONError(w, http.StatusConflict, "offer changed concurrently; reload and retry")
		return
	case err != nil:
		writeJSONError(w, http.StatusInternalServerError, "failed to re-offer")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// IntroducedLinkView is one listed row with its history.
type IntroducedLinkView struct {
	store.IntroducedLink
	History []store.IntroducedLinkHistory `json:"history"`
}

// IntroducedLinkListResponse lists introduced links.
type IntroducedLinkListResponse struct {
	Links []IntroducedLinkView `json:"links"`
}

func (s *Server) handleIntroducedLinkList(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	ils := s.introducedLinks()
	if ils == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "introduced-link state is not configured on this node")
		return
	}
	chain := strings.TrimSpace(r.URL.Query().Get("remote_chain_id"))
	if chain != "" {
		if err := federation.ValidateChainID(chain); err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid remote_chain_id")
			return
		}
	}
	rows, err := ils.ListIntroducedLinks(r.Context(), chain)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "introduced-link state unavailable")
		return
	}
	resp := IntroducedLinkListResponse{Links: make([]IntroducedLinkView, 0, len(rows))}
	for _, row := range rows {
		hist, hErr := ils.ListIntroducedLinkHistory(r.Context(), row.LinkID)
		if hErr != nil {
			writeJSONError(w, http.StatusInternalServerError, "introduced-link state unavailable")
			return
		}
		// Diagnostics never echo signed tx bytes or the full transcript.
		row.TxBytes, row.EIntro = nil, nil
		resp.Links = append(resp.Links, IntroducedLinkView{IntroducedLink: row, History: hist})
	}
	writeJSON(w, http.StatusOK, resp)
}
