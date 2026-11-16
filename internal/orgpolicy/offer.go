package orgpolicy

// Deterministic introduction offers (RFC #443 Phase B, contract v2 §5).
//
// Two enrolled nodes of the same org compute the same offer from the same
// accepted roster: the host is the lower agent_pub (bytewise over the raw 32
// bytes, which for equal-length lowercase hex is plain string order), the guest
// the higher, and
//
//	offer_id = SHA256("sage/intro-offer/v1" ‖ org_id ‖ agent_pub_host ‖
//	                  agent_pub_guest ‖ card_rev_host ‖ card_rev_guest ‖ kind)
//
// where every string is u32-length-prefixed and every card_rev is a big-endian
// u64, so no two inputs share an encoding. Card revisions are taken from the
// accepted roster's copy of each card, which both sides hold; a roster refresh
// that changes neither card gives the same id, so nothing is re-offered.
//
// Offers only describe what may be asked. Nothing here signs, broadcasts or
// activates a link. Each node keeps a local record of when it first saw an
// offer, so the 72 h expiry survives restart; the owner can re-offer, which
// restarts the local window.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

const (
	offerIDDomain = "sage/intro-offer/v1"

	// OfferKindNew offers a first link to a peer with no agreement.
	OfferKindNew = "new"
	// OfferKindRekey offers a rekey of an active introduced link whose peer
	// card changed its CA SPKI pin.
	OfferKindRekey = "rekey"

	// OfferWindowSeconds is the offer expiry (72 h).
	OfferWindowSeconds = 72 * 60 * 60

	// RoleHost / RoleGuest are this node's role toward one peer.
	RoleHost  = "host"
	RoleGuest = "guest"
)

// Per-peer link states shown by RefreshOffers.
const (
	PeerStateNone          = "none"           // not eligible / not covered; Reason says why
	PeerStateOffered       = "offered"        // an offer is open (Kind new or rekey)
	PeerStateExpired       = "expired"        // the local offer window passed; re-offer to reopen
	PeerStateInFlight      = "in_flight"      // an introduced_link row is between intent and active
	PeerStateActive        = "active"         // the introduced link is active and still covered
	PeerStateRekeyRequired = "rekey_required" // the peer card changed its CA pin
	PeerStatePauseRequired = "pause_required" // the link is active but no longer covered; Reason says why
	PeerStatePaused        = "paused"         // the link is paused
	PeerStateManual        = "manual"         // a manual (QR JOIN) agreement exists; no offer
)

// ErrOfferNotFound means no local offer record exists for the id.
var ErrOfferNotFound = errors.New("intro offer: not found")

// ErrOfferStateConflict means the offer record changed between read and write.
var ErrOfferStateConflict = errors.New("intro offer: state changed concurrently")

// IntroRoles returns (host, guest) for two distinct agent keys.
func IntroRoles(a, b string) (host, guest string, err error) {
	if !isAgentPubHex(a) || !isAgentPubHex(b) {
		return "", "", errors.New("intro roles: agent keys must be 64 lowercase hex")
	}
	switch {
	case a < b:
		return a, b, nil
	case b < a:
		return b, a, nil
	default:
		return "", "", errors.New("intro roles: a node cannot be introduced to itself")
	}
}

// OfferID computes the deterministic offer id.
func OfferID(orgID, hostPub, guestPub string, cardRevHost, cardRevGuest uint64, kind string) string {
	b := make([]byte, 0, 256)
	b = lp(b, offerIDDomain)
	b = lp(b, orgID)
	b = lp(b, hostPub)
	b = lp(b, guestPub)
	b = u64(b, cardRevHost)
	b = u64(b, cardRevGuest)
	b = lp(b, kind)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// PeerAgreement is what the caller knows about an existing link to one peer
// chain: whether a tx-33 agreement exists, its pinned CA SPKI (hex), and the
// newest introduced_link row for that chain, if any.
type PeerAgreement struct {
	Exists    bool
	PinHex    string
	LinkID    string
	LinkState string // empty when the chain has no introduced_link row
}

// OfferInputs drives RefreshOffers.
type OfferInputs struct {
	OrgID       string
	OwnAgentPub string // this node's transport key (64 lowercase hex)
	OwnChainID  string
	// RootCredentialID / RootPub are the node's current Root, for the envelope.
	RootCredentialID string
	RootPub          ed25519.PublicKey
	Now              int64
	// Agreement reports the link state for a peer chain. An error refuses the
	// whole refresh (fail closed).
	Agreement func(chainID string) (PeerAgreement, error)
}

// PeerOffer is one peer's row.
type PeerOffer struct {
	PeerAgentPub string `json:"peer_agent_pub"`
	PeerChainID  string `json:"peer_chain_id"`
	Role         string `json:"role"`
	State        string `json:"state"`
	Reason       string `json:"reason,omitempty"`
	Kind         string `json:"kind,omitempty"`
	OfferID      string `json:"offer_id,omitempty"`
	OfferedAt    int64  `json:"offered_at,omitempty"`
	ExpiresAt    int64  `json:"expires_at,omitempty"`
	HostCardRev  uint64 `json:"host_card_rev"`
	GuestCardRev uint64 `json:"guest_card_rev"`
	LinkID       string `json:"link_id,omitempty"`
	LinkState    string `json:"link_state,omitempty"`
}

// OfferList is the result for one org. When Ready is false the org-level
// preconditions fail (Reason) and Offers is empty.
type OfferList struct {
	OrgID          string      `json:"org_id"`
	Ready          bool        `json:"ready"`
	Reason         string      `json:"reason,omitempty"`
	RosterRevision uint64      `json:"roster_revision,omitempty"`
	RosterDigest   string      `json:"roster_digest,omitempty"`
	EnvelopeRev    uint64      `json:"envelope_revision,omitempty"`
	Offers         []PeerOffer `json:"offers"`
}

// OfferRecord is the local record of an observed offer.
type OfferRecord struct {
	OfferID   string `json:"offer_id"`
	OrgID     string `json:"org_id"`
	Kind      string `json:"kind"`
	PeerPub   string `json:"peer_agent_pub"`
	OfferedAt int64  `json:"offered_at"`
	ExpiresAt int64  `json:"expires_at"`
	Reoffers  uint64 `json:"reoffers"`
}

func offerKey(orgID, offerID string) string {
	return keyPrefix + "offer/" + seg(orgID) + "/" + seg(offerID)
}

func (s *Store) loadOfferRaw(orgID, offerID string) (*OfferRecord, []byte, error) {
	raw, err := s.kv.GetState(offerKey(orgID, offerID))
	if err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: load offer: %w", err)
	}
	if raw == nil {
		return nil, nil, nil
	}
	var rec OfferRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: decode offer: %w", err)
	}
	return &rec, append([]byte(nil), raw...), nil
}

// LoadOffer returns the local record of one offer.
func (s *Store) LoadOffer(orgID, offerID string) (*OfferRecord, bool, error) {
	rec, _, err := s.loadOfferRaw(orgID, offerID)
	if err != nil || rec == nil {
		return nil, false, err
	}
	return rec, true, nil
}

// observeOffer returns the offer record, creating it on first sight.
func (s *Store) observeOffer(orgID, offerID, kind, peerPub string, now int64) (*OfferRecord, error) {
	rec, _, err := s.loadOfferRaw(orgID, offerID)
	if err != nil || rec != nil {
		return rec, err
	}
	rec = &OfferRecord{
		OfferID: offerID, OrgID: orgID, Kind: kind, PeerPub: peerPub,
		OfferedAt: now, ExpiresAt: now + OfferWindowSeconds,
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: encode offer: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{{Key: offerKey(orgID, offerID), Value: nil}},
		[]KVWrite{{Key: offerKey(orgID, offerID), Value: b}},
	)
	if err != nil {
		return nil, err
	}
	if !applied {
		// Created concurrently: return the winner.
		won, _, lErr := s.loadOfferRaw(orgID, offerID)
		if lErr != nil {
			return nil, lErr
		}
		if won == nil {
			return nil, ErrOfferStateConflict
		}
		return won, nil
	}
	return rec, nil
}

// Reoffer restarts the local 72 h window of an existing offer record.
func (s *Store) Reoffer(orgID, offerID string, now int64) (*OfferRecord, error) {
	rec, raw, err := s.loadOfferRaw(orgID, offerID)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, ErrOfferNotFound
	}
	rec.OfferedAt = now
	rec.ExpiresAt = now + OfferWindowSeconds
	rec.Reoffers++
	b, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: encode offer: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{{Key: offerKey(orgID, offerID), Value: raw}},
		[]KVWrite{{Key: offerKey(orgID, offerID), Value: b}},
	)
	if err != nil {
		return nil, err
	}
	if !applied {
		return nil, ErrOfferStateConflict
	}
	return rec, nil
}

// inFlightLinkStates are the introduced_link states between intent and a
// settled outcome.
var inFlightLinkStates = map[string]bool{
	"intent": true, "signed": true, "committed": true, "bound_pending_peer": true,
}

// RefreshOffers computes this node's per-peer offers for one org from the
// accepted policy, the accepted roster, the owner envelope and the caller's
// view of existing links, and records first sight of every open offer. Any
// storage error, or an Agreement error, fails the whole refresh.
//
// Org-level preconditions (else Ready=false): an accepted policy, a current
// accepted roster that lists this node's own card, and an envelope that
// covers the org under the current Root. Per peer: its roster card is current
// and on another chain, and PeerEligible holds under this org's policy.
func (s *Store) RefreshOffers(in OfferInputs) (OfferList, error) {
	out := OfferList{OrgID: in.OrgID, Offers: []PeerOffer{}}
	if in.Agreement == nil {
		return out, errors.New("intro offers: no agreement source")
	}
	st, body, err := s.LoadAcceptedPolicy(in.OrgID)
	if err != nil {
		return out, err
	}
	if st.Revision == 0 {
		out.Reason = "no accepted org policy"
		return out, nil
	}
	rst, roster, err := s.LoadRoster(in.OrgID)
	if err != nil {
		return out, err
	}
	if roster == nil || rst.Revision == 0 {
		out.Reason = "no accepted roster"
		return out, nil
	}
	out.RosterRevision, out.RosterDigest = rst.Revision, rst.Digest
	if !rst.CurrentAt(in.Now) {
		out.Reason = "accepted roster is not current"
		return out, nil
	}
	env, _, err := s.LoadEnvelope(in.OrgID)
	if err != nil {
		return out, err
	}
	var envelope *LinkEnvelope
	if env != nil {
		envelope = &env.Envelope
		out.EnvelopeRev = env.Envelope.Revision
	}
	if ok, why := envelope.Covers(EnvelopeContext{
		RootCredentialID: in.RootCredentialID, RootPub: in.RootPub,
		Policy: st, RosterScope: rst.Scope, Now: in.Now,
	}); !ok {
		out.Reason = why
		return out, nil
	}
	var own *LinkCard
	for i := range roster.Cards {
		if roster.Cards[i].AgentPub == in.OwnAgentPub {
			own = &roster.Cards[i]
			break
		}
	}
	switch {
	case own == nil:
		out.Reason = "this node's card is not in the accepted roster"
		return out, nil
	case own.ChainID != in.OwnChainID:
		out.Reason = "the roster card for this node names a different chain id"
		return out, nil
	case !own.CurrentAt(in.Now):
		out.Reason = "this node's roster card is not current"
		return out, nil
	}
	out.Ready = true

	cards := append([]LinkCard(nil), roster.Cards...)
	sort.Slice(cards, func(i, j int) bool { return cards[i].AgentPub < cards[j].AgentPub })
	for i := range cards {
		peer := cards[i]
		if peer.AgentPub == in.OwnAgentPub {
			continue
		}
		row, rErr := s.peerOffer(in, st, body, own, &peer)
		if rErr != nil {
			return OfferList{OrgID: in.OrgID, Offers: []PeerOffer{}}, rErr
		}
		out.Offers = append(out.Offers, row)
	}
	return out, nil
}

func (s *Store) peerOffer(in OfferInputs, st AcceptedState, body *Policy, own, peer *LinkCard) (PeerOffer, error) {
	host, guest, err := IntroRoles(in.OwnAgentPub, peer.AgentPub)
	if err != nil {
		return PeerOffer{}, err
	}
	row := PeerOffer{PeerAgentPub: peer.AgentPub, PeerChainID: peer.ChainID, Role: RoleGuest}
	hostCard, guestCard := peer, own
	if host == in.OwnAgentPub {
		row.Role = RoleHost
		hostCard, guestCard = own, peer
	}
	row.HostCardRev, row.GuestCardRev = hostCard.CardRev, guestCard.CardRev

	ag, err := in.Agreement(peer.ChainID)
	if err != nil {
		return PeerOffer{}, fmt.Errorf("intro offers: agreement state for %s: %w", peer.ChainID, err)
	}
	row.LinkID, row.LinkState = ag.LinkID, ag.LinkState

	// Coverage of this peer right now.
	var covered bool
	var why string
	switch {
	case peer.ChainID == in.OwnChainID:
		covered, why = false, "peer card names this node's own chain id"
	case !peer.CurrentAt(in.Now):
		covered, why = false, "peer card is not current"
	default:
		covered, why = PeerEligible(st, body, peer.AgentPub, in.Now)
	}

	switch {
	case inFlightLinkStates[ag.LinkState]:
		row.State = PeerStateInFlight
		return row, nil
	case ag.Exists && ag.LinkState == "":
		row.State, row.Reason = PeerStateManual, "a manual agreement exists for this chain"
		return row, nil
	case ag.Exists && ag.LinkState == "paused":
		row.State = PeerStatePaused
		return row, nil
	case ag.Exists && (ag.LinkState == "active" || ag.LinkState == "rekey_required"):
		if !covered {
			row.State, row.Reason = PeerStatePauseRequired, why
			return row, nil
		}
		if ag.LinkState == "rekey_required" || ag.PinHex != peer.CASPKIPin {
			return s.openOffer(in, row, host, guest, OfferKindRekey, PeerStateRekeyRequired)
		}
		row.State = PeerStateActive
		return row, nil
	case ag.Exists:
		// The newest introduced row is terminal (aborted/revoked) but a tx-33
		// agreement still exists: no new offer while it does.
		row.State, row.Reason = PeerStateManual, "an agreement exists for this chain outside an active introduced link"
		return row, nil
	}
	if !covered {
		row.State, row.Reason = PeerStateNone, why
		return row, nil
	}
	return s.openOffer(in, row, host, guest, OfferKindNew, PeerStateOffered)
}

func (s *Store) openOffer(in OfferInputs, row PeerOffer, host, guest, kind, state string) (PeerOffer, error) {
	row.Kind = kind
	row.OfferID = OfferID(in.OrgID, host, guest, row.HostCardRev, row.GuestCardRev, kind)
	rec, err := s.observeOffer(in.OrgID, row.OfferID, kind, row.PeerAgentPub, in.Now)
	if err != nil {
		return PeerOffer{}, err
	}
	row.OfferedAt, row.ExpiresAt = rec.OfferedAt, rec.ExpiresAt
	row.State = state
	if in.Now >= rec.ExpiresAt {
		row.State = PeerStateExpired
	}
	return row, nil
}
