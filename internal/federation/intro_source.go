package federation

// The org side of the introduced-link wire (RFC #443 Phase B2b).
//
// federation stays independent of where org state lives: the REST layer
// installs an IntroOrgSource backed by the node-local org policy store (the
// same pattern as the JOIN gate and the introduced-link source). The wire
// asks it for one offer as this node sees it right now: both roster cards,
// the accepted policy and roster revisions + digests, this node's envelope
// revision + digest, and the owner's acceptance. With no source installed the
// intro routes and the handshake window refuse everything.

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// IntroCard is one roster card as the wire needs it.
type IntroCard struct {
	AgentPub   string   // 64 lowercase hex
	ChainID    string   //
	Pin        string   // 64 lowercase hex CA SPKI pin
	Endpoint   string   //
	PeerID     string   // p2p peer id, may be empty
	Routes     []string // p2p routes, sorted
	CardRev    uint64
	CardDigest string // 64 lowercase hex
}

// IntroOfferView is one offer of one org as this node sees it now.
type IntroOfferView struct {
	OrgID        string
	OrgRootKeyID string
	OfferID      string
	Kind         string // new | rekey
	Role         string // host | guest
	// State is the orgpolicy per-peer state (offered, expired, in_flight, ...).
	State     string
	Reason    string
	ExpiresAt int64

	PolicyRevision uint64
	PolicyDigest   string
	RosterRevision uint64
	RosterDigest   string
	EnvelopeRev    uint64
	EnvelopeDigest string

	Own  IntroCard
	Peer IntroCard

	// Accepted reports an owner acceptance of this offer; it is valid only
	// while it names the current envelope (AcceptedEnvelopeRev/Digest).
	Accepted               bool
	AcceptedEnvelopeRev    uint64
	AcceptedEnvelopeDigest string
}

// AcceptanceCurrent reports whether the owner acceptance names this node's
// current envelope.
func (v *IntroOfferView) AcceptanceCurrent() bool {
	return v.Accepted && v.EnvelopeRev != 0 &&
		v.AcceptedEnvelopeRev == v.EnvelopeRev && v.AcceptedEnvelopeDigest == v.EnvelopeDigest
}

// IntroOfferRef names one accepted offer.
type IntroOfferRef struct {
	OrgID   string
	OfferID string
}

// IntroOrgSource is the org state the intro wire reads. Every method must
// fail closed on a storage error.
type IntroOrgSource interface {
	// IntroOffer resolves one offer. It returns ErrIntroOfferUnknown when this
	// node holds no such offer for the org.
	IntroOffer(ctx context.Context, orgID, offerID string) (*IntroOfferView, error)
	// IntroPeerPinKnown reports whether pin (64 lowercase hex) is the CA pin
	// of a current card of another node in an accepted, current roster of an
	// org this node is enrolled in. It gates the TLS handshake only.
	IntroPeerPinKnown(pin string) bool
	// AcceptedIntroOffers lists this node's owner acceptances.
	AcceptedIntroOffers(ctx context.Context) ([]IntroOfferRef, error)
}

// ErrIntroOfferUnknown means the node holds no such offer.
var ErrIntroOfferUnknown = errors.New("intro offer: unknown")

// introState is the in-memory half of the wire: host sessions and the guest
// drivers. Its zero value is ready to use.
type introState struct {
	mu      sync.Mutex
	src     IntroOrgSource
	host    map[string]*introHostSession  // by offer id
	running map[string]context.CancelFunc // guest drivers, by offer id
	wg      sync.WaitGroup
	// confirmHold is a test seam run while a confirm owns its session.
	confirmHold func()
	// requestPublished is a test seam run after a request publishes its
	// session and before it answers.
	requestPublished func(*introHostSession)
}

// SetIntroOrgSource installs the org source for the intro wire.
func (m *Manager) SetIntroOrgSource(src IntroOrgSource) {
	m.intro.mu.Lock()
	m.intro.src = src
	m.intro.mu.Unlock()
}

func (m *Manager) introOrgSource() IntroOrgSource {
	m.intro.mu.Lock()
	defer m.intro.mu.Unlock()
	return m.intro.src
}

// introSideFor builds one transcript side from a roster card.
func introSideFor(c IntroCard, nonce string, envRev uint64, envDigest string) IntroSide {
	return IntroSide{
		ChainID: c.ChainID, AgentPub: c.AgentPub, Pin: c.Pin, Endpoint: c.Endpoint,
		Nonce: nonce, Scope: trustOnlyScopeDigestHex(), PeerID: c.PeerID,
		P2PAddrs: append([]string(nil), c.Routes...), CardRev: c.CardRev, CardDigest: c.CardDigest,
		EnvelopeRev: envRev, EnvelopeDigest: envDigest,
	}
}

func trustOnlyScopeDigestHex() string {
	d := trustOnlyJoinScope.digest()
	return hex.EncodeToString(d[:])
}

// introTranscriptFor builds E_intro from this node's view of the offer, both
// nonces, the peer's envelope evidence and the seed commitment. Both sides
// build it independently from their own views; the guest refuses a host
// transcript that differs from its own by one byte.
func introTranscriptFor(v *IntroOfferView, hostNonce, guestNonce string, peerEnvRev uint64, peerEnvDigest, seedCommit string) ([]byte, error) {
	own := introSideFor(v.Own, "", v.EnvelopeRev, v.EnvelopeDigest)
	peer := introSideFor(v.Peer, "", peerEnvRev, peerEnvDigest)
	host, guest := own, peer
	if v.Role == "guest" {
		host, guest = peer, own
	}
	host.Nonce, guest.Nonce = hostNonce, guestNonce
	tr := &IntroTranscript{
		V: IntroTranscriptVersion, Kind: v.Kind, OfferID: v.OfferID,
		OrgID: v.OrgID, OrgRootKeyID: v.OrgRootKeyID,
		PolicyRevision: v.PolicyRevision, PolicyDigest: v.PolicyDigest,
		RosterRevision: v.RosterRevision, RosterDigest: v.RosterDigest,
		Host: host, Guest: guest, SeedCommit: seedCommit,
	}
	return tr.Encode()
}

// introOpenOffer resolves an offer and requires it to be open, of kind new,
// with this node in role.
func (m *Manager) introOpenOffer(ctx context.Context, orgID, offerID, role string) (*IntroOfferView, error) {
	src := m.introOrgSource()
	if src == nil {
		return nil, errors.New("introduced links are not configured on this node")
	}
	v, err := src.IntroOffer(ctx, orgID, offerID)
	if err != nil {
		return nil, err
	}
	switch {
	case v.OfferID != offerID || v.OrgID != orgID:
		return nil, errors.New("intro offer: source returned a different offer")
	case v.Role != role:
		return nil, fmt.Errorf("intro offer: this node is the %s for this offer", v.Role)
	case v.Kind != introKindNew:
		// Rekey replaces an active binding; it lands after B2b.
		return nil, errors.New("intro offer: only kind new is supported")
	case v.State != "offered":
		why := v.State
		if v.Reason != "" {
			why += ": " + v.Reason
		}
		return nil, fmt.Errorf("intro offer is not open (%s)", why)
	case v.ExpiresAt <= time.Now().Unix():
		return nil, errors.New("intro offer has expired")
	case v.Own.ChainID != m.localChainID || v.Own.AgentPub != hex.EncodeToString(m.agentPub):
		return nil, errors.New("intro offer: this node's roster card does not name this node")
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return nil, err
	}
	if v.Own.Pin != hex.EncodeToString(ownPin) {
		return nil, errors.New("intro offer: this node's roster card pins a different CA")
	}
	return v, nil
}

// introStaleCheck re-reads the offer under the agreement lease and refuses
// the activation if anything E_intro binds has moved: the offer is no longer
// open (expired, peer no longer eligible, envelope no longer covering, an
// agreement appeared), the policy, roster, either card or this node's
// envelope changed, or the owner acceptance no longer names the current
// envelope.
func (m *Manager) introStaleCheck(orgID, offerID, role string, tr *IntroTranscript) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		v, err := m.introOpenOffer(ctx, orgID, offerID, role)
		if err != nil {
			return err
		}
		own, peer := tr.Host, tr.Guest
		if role == "guest" {
			own, peer = tr.Guest, tr.Host
		}
		switch {
		case v.PolicyRevision != tr.PolicyRevision || v.PolicyDigest != tr.PolicyDigest:
			return errors.New("policy_mismatch: the accepted org policy changed")
		case v.RosterRevision != tr.RosterRevision || v.RosterDigest != tr.RosterDigest:
			return errors.New("roster_mismatch: the accepted roster changed")
		case v.OrgRootKeyID != tr.OrgRootKeyID:
			return errors.New("the org root changed")
		case v.Own.CardRev != own.CardRev || v.Own.CardDigest != own.CardDigest:
			return errors.New("this node's card changed")
		case v.Peer.CardRev != peer.CardRev || v.Peer.CardDigest != peer.CardDigest:
			return errors.New("the peer card changed")
		case v.EnvelopeRev != own.EnvelopeRev || v.EnvelopeDigest != own.EnvelopeDigest:
			return errors.New("the owner envelope changed")
		case !v.AcceptanceCurrent():
			return errIntroNotAccepted
		}
		return nil
	}
}

// introEvidence is the stored evidence column: both sides' revisions and
// digests, as canonical JSON.
type introEvidence struct {
	PolicyRevision      uint64 `json:"policy_revision"`
	PolicyDigest        string `json:"policy_digest"`
	RosterRevision      uint64 `json:"roster_revision"`
	RosterDigest        string `json:"roster_digest"`
	HostCardRev         uint64 `json:"host_card_rev"`
	HostCardDigest      string `json:"host_card_digest"`
	GuestCardRev        uint64 `json:"guest_card_rev"`
	GuestCardDigest     string `json:"guest_card_digest"`
	HostEnvelopeRev     uint64 `json:"host_envelope_rev"`
	HostEnvelopeDigest  string `json:"host_envelope_digest"`
	GuestEnvelopeRev    uint64 `json:"guest_envelope_rev"`
	GuestEnvelopeDigest string `json:"guest_envelope_digest"`
}

func introEvidenceOf(tr *IntroTranscript) []byte {
	b, _ := json.Marshal(&introEvidence{
		PolicyRevision: tr.PolicyRevision, PolicyDigest: tr.PolicyDigest,
		RosterRevision: tr.RosterRevision, RosterDigest: tr.RosterDigest,
		HostCardRev: tr.Host.CardRev, HostCardDigest: tr.Host.CardDigest,
		GuestCardRev: tr.Guest.CardRev, GuestCardDigest: tr.Guest.CardDigest,
		HostEnvelopeRev: tr.Host.EnvelopeRev, HostEnvelopeDigest: tr.Host.EnvelopeDigest,
		GuestEnvelopeRev: tr.Guest.EnvelopeRev, GuestEnvelopeDigest: tr.Guest.EnvelopeDigest,
	})
	return b
}
