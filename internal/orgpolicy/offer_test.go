package orgpolicy

import (
	"errors"
	"strings"
	"testing"
)

func TestIntroRolesAndOfferID(t *testing.T) {
	a, b := agentHex(1), agentHex(2)
	h1, g1, err := IntroRoles(a, b)
	if err != nil {
		t.Fatal(err)
	}
	h2, g2, _ := IntroRoles(b, a)
	if h1 != h2 || g1 != g2 || h1 >= g1 {
		t.Fatalf("roles differ by argument order or host is not the lower key: %s %s / %s %s", h1, g1, h2, g2)
	}
	if _, _, err := IntroRoles(a, a); err == nil {
		t.Fatal("a node was introduced to itself")
	}
	if _, _, err := IntroRoles(strings.ToUpper(a), b); err == nil {
		t.Fatal("upper-case agent key accepted")
	}
	base := OfferID("org-acme", h1, g1, 1, 1, OfferKindNew)
	if len(base) != 64 || base != OfferID("org-acme", h1, g1, 1, 1, OfferKindNew) {
		t.Fatal("offer id is not a stable sha256")
	}
	for name, id := range map[string]string{
		"org":        OfferID("org-other", h1, g1, 1, 1, OfferKindNew),
		"roles":      OfferID("org-acme", g1, h1, 1, 1, OfferKindNew),
		"host rev":   OfferID("org-acme", h1, g1, 2, 1, OfferKindNew),
		"guest rev":  OfferID("org-acme", h1, g1, 1, 2, OfferKindNew),
		"kind":       OfferID("org-acme", h1, g1, 1, 1, OfferKindRekey),
		"rev shift":  OfferID("org-acme", h1, g1, 0, 1<<32|1, OfferKindNew),
		"org concat": OfferID("org-acm", "e"+h1, g1, 1, 1, OfferKindNew),
	} {
		if id == base {
			t.Errorf("%s does not change the offer id", name)
		}
	}
}

type agreementTable map[string]PeerAgreement

func (a agreementTable) lookup(chain string) (PeerAgreement, error) { return a[chain], nil }

func (f *phaseBFixture) storeEnvelope(t *testing.T) {
	t.Helper()
	e := f.envelope(t, 1, "", nil)
	if _, _, err := f.st.ApplyEnvelope(e, rootID(f.root), pub(f.root), f.policy, testNow); err != nil {
		t.Fatalf("store envelope: %v", err)
	}
}

func (f *phaseBFixture) inputs(own byte, now int64, ag agreementTable) OfferInputs {
	return OfferInputs{
		OrgID: "org-acme", OwnAgentPub: agentHex(own), OwnChainID: sampleCard(own, 1, nil).ChainID,
		RootCredentialID: rootID(f.root), RootPub: pub(f.root), Now: now,
		Agreement: ag.lookup,
	}
}

func offerFor(t *testing.T, l OfferList, peer string) PeerOffer {
	t.Helper()
	for _, o := range l.Offers {
		if o.PeerAgentPub == peer {
			return o
		}
	}
	t.Fatalf("no row for peer %s in %+v", peer, l)
	return PeerOffer{}
}

func TestRefreshOffersOrgPreconditions(t *testing.T) {
	f := newPhaseBFixture(t)
	// No envelope yet.
	l, err := f.st.RefreshOffers(f.inputs(1, testNow, agreementTable{}))
	if err != nil || l.Ready || !strings.Contains(l.Reason, "envelope") || len(l.Offers) != 0 {
		t.Fatalf("no envelope: %+v %v", l, err)
	}
	f.storeEnvelope(t)
	// This node's card is not in the roster.
	l, err = f.st.RefreshOffers(f.inputs(9, testNow, agreementTable{}))
	if err != nil || l.Ready || !strings.Contains(l.Reason, "not in the accepted roster") {
		t.Fatalf("own card missing: %+v %v", l, err)
	}
	// The roster expired.
	l, err = f.st.RefreshOffers(f.inputs(1, f.roster.Expiry, agreementTable{}))
	if err != nil || l.Ready || !strings.Contains(l.Reason, "roster is not current") {
		t.Fatalf("stale roster: %+v %v", l, err)
	}
	// Root rotated: the envelope no longer covers.
	in := f.inputs(1, testNow, agreementTable{})
	other := fixedKey(t, 80)
	in.RootCredentialID, in.RootPub = rootID(other), pub(other)
	if l, err = f.st.RefreshOffers(in); err != nil || l.Ready {
		t.Fatalf("rotated Root: %+v %v", l, err)
	}
	// No accepted policy at all.
	empty := NewStore(newFakeKV())
	if l, err = empty.RefreshOffers(f.inputs(1, testNow, agreementTable{})); err != nil || l.Ready || l.Reason != "no accepted org policy" {
		t.Fatalf("no policy: %+v %v", l, err)
	}
	// An agreement-source error fails the refresh.
	in = f.inputs(1, testNow, nil)
	in.Agreement = func(string) (PeerAgreement, error) { return PeerAgreement{}, errors.New("badger down") }
	if _, err = f.st.RefreshOffers(in); err == nil {
		t.Fatal("agreement error did not fail the refresh")
	}
}

// Both sides of a pair compute the same offer from the same roster.
func TestRefreshOffersBothSidesAgree(t *testing.T) {
	f := newPhaseBFixture(t)
	f.storeEnvelope(t)
	l1, err := f.st.RefreshOffers(f.inputs(1, testNow, agreementTable{}))
	if err != nil || !l1.Ready {
		t.Fatalf("node 1: %+v %v", l1, err)
	}
	// Node 2's view: same accepted policy, roster and its own envelope.
	g := newPhaseBFixture(t)
	g.storeEnvelope(t)
	l2, err := g.st.RefreshOffers(g.inputs(2, testNow+5, agreementTable{}))
	if err != nil || !l2.Ready {
		t.Fatalf("node 2: %+v %v", l2, err)
	}
	o12, o21 := offerFor(t, l1, agentHex(2)), offerFor(t, l2, agentHex(1))
	if o12.State != PeerStateOffered || o21.State != PeerStateOffered || o12.Kind != OfferKindNew {
		t.Fatalf("not offered: %+v / %+v", o12, o21)
	}
	if o12.OfferID != o21.OfferID {
		t.Fatalf("offer ids differ across the pair: %s vs %s", o12.OfferID, o21.OfferID)
	}
	if o12.Role == o21.Role {
		t.Fatalf("both sides took role %s", o12.Role)
	}
	// Node 3 is in the roster but not in the allow-list: no offer, with a reason.
	o13 := offerFor(t, l1, agentHex(3))
	if o13.State != PeerStateNone || o13.OfferID != "" || o13.Reason == "" {
		t.Fatalf("ineligible peer offered: %+v", o13)
	}
	// A refresh with an unchanged roster keeps the same id and window.
	again, _ := f.st.RefreshOffers(f.inputs(1, testNow+3600, agreementTable{}))
	if o := offerFor(t, again, agentHex(2)); o.OfferID != o12.OfferID || o.OfferedAt != o12.OfferedAt {
		t.Fatalf("refresh re-offered: %+v vs %+v", o, o12)
	}
}

func TestRefreshOffersExpiryAndReoffer(t *testing.T) {
	f := newPhaseBFixture(t)
	f.storeEnvelope(t)
	l, _ := f.st.RefreshOffers(f.inputs(1, testNow, agreementTable{}))
	o := offerFor(t, l, agentHex(2))
	if o.ExpiresAt != testNow+OfferWindowSeconds {
		t.Fatalf("expiry: %d", o.ExpiresAt)
	}
	later := testNow + OfferWindowSeconds
	// The org delivers a fresh roster with the same cards: same offer id.
	fresh := &Roster{
		OrgID: "org-acme", Scope: "fleet", Revision: 2, KeyEpoch: "epoch-1",
		IssuedAt: later - 60, NotBefore: later - 60, Expiry: later + 3*24*3600,
		Cards: f.roster.Cards,
	}
	if err := fresh.Sign(f.org); err != nil {
		t.Fatal(err)
	}
	if _, d, _, err := f.st.ApplyRoster(fresh, pub(f.org), f.policy); err != nil || d != DecisionApplied {
		t.Fatalf("fresh roster: %q %v", d, err)
	}
	l, _ = NewStore(f.kv).RefreshOffers(f.inputs(1, later, agreementTable{}))
	if got := offerFor(t, l, agentHex(2)); got.State != PeerStateExpired || got.OfferID != o.OfferID {
		t.Fatalf("after 72h (and a restart): %+v", got)
	}
	if _, err := f.st.Reoffer("org-acme", strings.Repeat("f", 64), later); !errors.Is(err, ErrOfferNotFound) {
		t.Fatalf("re-offer of an unknown id: %v", err)
	}
	rec, err := f.st.Reoffer("org-acme", o.OfferID, later)
	if err != nil || rec.Reoffers != 1 || rec.ExpiresAt != later+OfferWindowSeconds {
		t.Fatalf("re-offer: %+v %v", rec, err)
	}
	l, _ = f.st.RefreshOffers(f.inputs(1, later+1, agreementTable{}))
	if got := offerFor(t, l, agentHex(2)); got.State != PeerStateOffered || got.OfferID != o.OfferID {
		t.Fatalf("after re-offer: %+v", got)
	}
}

func TestRefreshOffersExistingLinks(t *testing.T) {
	f := newPhaseBFixture(t)
	f.storeEnvelope(t)
	peer := sampleCard(2, 1, nil)
	chain := peer.ChainID
	cases := []struct {
		name  string
		ag    PeerAgreement
		state string
		kind  string
	}{
		{"manual agreement", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin}, PeerStateManual, ""},
		{"in flight", PeerAgreement{LinkID: "L", LinkOrgID: "org-acme", LinkState: "signed"}, PeerStateInFlight, ""},
		{"active, same pin", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStateActive, ""},
		{"active, pin changed", PeerAgreement{Exists: true, PinHex: strings.Repeat("cd", 32), LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStateRekeyRequired, OfferKindRekey},
		{"rekey required", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "rekey_required"}, PeerStateRekeyRequired, OfferKindRekey},
		{"paused", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "paused"}, PeerStatePaused, ""},
		{"revoked row, agreement gone", PeerAgreement{LinkID: "L", LinkOrgID: "org-acme", LinkState: "revoked"}, PeerStateOffered, OfferKindNew},
		{"aborted row, agreement still on chain", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "aborted"}, PeerStateManual, ""},
		// A row introduced by another org is never read under this org.
		{"other org, active row", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-other", LinkState: "active"}, PeerStateManual, ""},
		{"other org, rekey row", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-other", LinkState: "rekey_required"}, PeerStateManual, ""},
		{"other org, in flight", PeerAgreement{LinkID: "L", LinkOrgID: "org-other", LinkState: "signed"}, PeerStateInFlight, ""},
		{"other org, revoked row, agreement gone", PeerAgreement{LinkID: "L", LinkOrgID: "org-other", LinkState: "revoked"}, PeerStateOffered, OfferKindNew},
		// The envelope (eng, ops; clearance 2) narrows past the agreement terms.
		{"active, domain outside envelope", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, AllowedDomains: []string{"eng", "hr"}, MaxClearance: 2, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStatePauseRequired, ""},
		{"active, clearance above envelope", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, AllowedDomains: []string{"eng"}, MaxClearance: 3, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStatePauseRequired, ""},
		{"rekey required, domain outside envelope", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, AllowedDomains: []string{"*"}, LinkID: "L", LinkOrgID: "org-acme", LinkState: "rekey_required"}, PeerStatePauseRequired, ""},
		{"active, inside envelope", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, AllowedDomains: []string{"eng.public", "ops"}, MaxClearance: 2, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStateActive, ""},
	}
	for _, tc := range cases {
		l, err := f.st.RefreshOffers(f.inputs(1, testNow, agreementTable{chain: tc.ag}))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		o := offerFor(t, l, agentHex(2))
		if o.State != tc.state || o.Kind != tc.kind {
			t.Errorf("%s: state %q kind %q, want %q %q", tc.name, o.State, o.Kind, tc.state, tc.kind)
		}
		if tc.ag.LinkOrgID == "org-other" && o.LinkID != "" {
			t.Errorf("%s: another org's link id %q leaked into this org's offers", tc.name, o.LinkID)
		}
		if tc.state == PeerStatePauseRequired && o.Reason == "" {
			t.Errorf("%s: pause_required without a reason", tc.name)
		}
	}
}

// An active link whose peer left the allow-list must be paused.
func TestRefreshOffersPauseRequired(t *testing.T) {
	f := newPhaseBFixture(t, agentHex(1)) // node 2 is not listed
	f.storeEnvelope(t)
	peer := sampleCard(2, 1, nil)
	ag := agreementTable{peer.ChainID: {Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}}
	l, err := f.st.RefreshOffers(f.inputs(1, testNow, ag))
	if err != nil {
		t.Fatal(err)
	}
	if o := offerFor(t, l, agentHex(2)); o.State != PeerStatePauseRequired || o.Reason == "" {
		t.Fatalf("delisted active peer: %+v", o)
	}
	// A peer card that expired while the roster is still current also
	// requires a pause.
	g := newPhaseBFixture(t)
	short := sampleCard(2, 2, func(c *LinkCard) { c.ExpiresAt = testNow + 3600 })
	r2 := sampleRoster(g.org, 2, sampleCard(1, 1, nil), short)
	if _, d, _, err := g.st.ApplyRoster(r2, pub(g.org), g.policy); err != nil || d != DecisionApplied {
		t.Fatalf("roster with a short card: %q %v", d, err)
	}
	g.storeEnvelope(t)
	ag2 := agreementTable{short.ChainID: {Exists: true, PinHex: short.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}}
	l, err = g.st.RefreshOffers(g.inputs(1, testNow+3600, ag2))
	if err != nil || !l.Ready {
		t.Fatalf("refresh: %+v %v", l, err)
	}
	if o := offerFor(t, l, agentHex(2)); o.State != PeerStatePauseRequired || !strings.Contains(o.Reason, "not current") {
		t.Fatalf("expired peer card: %+v", o)
	}
}

// B2c: a rekey is due when either side's card pin moved, and a paused link
// says whether it may resume without a tap.
func TestRefreshOffersB2cRekeyAndResume(t *testing.T) {
	f := newPhaseBFixture(t)
	f.storeEnvelope(t)
	peer, own := sampleCard(2, 1, nil), sampleCard(1, 1, nil)
	chain := peer.ChainID
	moved := strings.Repeat("cd", 32)
	cases := []struct {
		name      string
		ag        PeerAgreement
		state     string
		kind      string
		resumable bool
	}{
		{"own pin unchanged", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, OwnPinHex: own.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStateActive, "", false},
		{"own pin moved", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, OwnPinHex: moved, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}, PeerStateRekeyRequired, OfferKindRekey, false},
		{"paused, nothing moved", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, OwnPinHex: own.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "paused"}, PeerStatePaused, "", true},
		{"paused, peer pin moved", PeerAgreement{Exists: true, PinHex: moved, LinkID: "L", LinkOrgID: "org-acme", LinkState: "paused"}, PeerStatePaused, "", false},
		{"paused, own pin moved", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, OwnPinHex: moved, LinkID: "L", LinkOrgID: "org-acme", LinkState: "paused"}, PeerStatePaused, "", false},
		{"paused, sharing outside envelope", PeerAgreement{Exists: true, PinHex: peer.CASPKIPin, AllowedDomains: []string{"hr"}, LinkID: "L", LinkOrgID: "org-acme", LinkState: "paused"}, PeerStatePaused, "", false},
	}
	for _, tc := range cases {
		l, err := f.st.RefreshOffers(f.inputs(1, testNow, agreementTable{chain: tc.ag}))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		o := offerFor(t, l, agentHex(2))
		if o.State != tc.state || o.Kind != tc.kind || o.Resumable != tc.resumable {
			t.Errorf("%s: state %q kind %q resumable %v, want %q %q %v", tc.name, o.State, o.Kind, o.Resumable, tc.state, tc.kind, tc.resumable)
		}
		if tc.state == PeerStatePaused && !tc.resumable && o.Reason == "" {
			t.Errorf("%s: a paused link that cannot resume must say why", tc.name)
		}
	}
	// Both sides compute the same rekey offer id from the same cards.
	l1, _ := f.st.RefreshOffers(f.inputs(1, testNow, agreementTable{chain: {Exists: true, PinHex: peer.CASPKIPin, OwnPinHex: moved, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}}))
	g := newPhaseBFixture(t)
	g.storeEnvelope(t)
	l2, _ := g.st.RefreshOffers(g.inputs(2, testNow, agreementTable{own.ChainID: {Exists: true, PinHex: moved, LinkID: "L", LinkOrgID: "org-acme", LinkState: "active"}}))
	o12, o21 := offerFor(t, l1, agentHex(2)), offerFor(t, l2, agentHex(1))
	if o12.Kind != OfferKindRekey || o21.Kind != OfferKindRekey || o12.OfferID != o21.OfferID {
		t.Fatalf("rekey offers differ across the pair: %+v / %+v", o12, o21)
	}
	// A paused peer that left the allow-list is not resumable.
	h := newPhaseBFixture(t, agentHex(1))
	h.storeEnvelope(t)
	l3, _ := h.st.RefreshOffers(h.inputs(1, testNow, agreementTable{chain: {Exists: true, PinHex: peer.CASPKIPin, LinkID: "L", LinkOrgID: "org-acme", LinkState: "paused"}}))
	if o := offerFor(t, l3, agentHex(2)); o.Resumable || o.Reason == "" {
		t.Fatalf("delisted paused peer: %+v", o)
	}
}
