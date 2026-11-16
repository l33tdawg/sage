package orgpolicy

import (
	"crypto/ed25519"
	"errors"
	"strings"
	"testing"
)

// phaseBFixture seeds a current v2 policy (allow-list: nodes 1 and 2) and an
// accepted roster carrying cards for nodes 1, 2 and 3.
type phaseBFixture struct {
	kv     *fakeKV
	st     *Store
	org    ed25519.PrivateKey
	root   ed25519.PrivateKey
	policy AcceptedState
	roster *Roster
}

func newPhaseBFixture(t *testing.T, allow ...string) *phaseBFixture {
	t.Helper()
	if allow == nil {
		allow = []string{agentHex(1), agentHex(2)}
	}
	kv := newFakeKV()
	org := fixedKey(t, 50)
	p := signed(t, org, func(p *Policy) {
		p.Schema = SchemaIDV2
		p.Revision = 3
		p.IssuedAt, p.NotBefore, p.Expiry = testNow-60, testNow-60, testNow+30*24*3600
		p.Payload.PeerAllowList = allow
	})
	st := NewStore(kv)
	acc, d, _, err := st.ApplyPolicy(p, pub(org))
	if err != nil || d != DecisionApplied {
		t.Fatalf("seed policy: %q %v", d, err)
	}
	r := sampleRoster(org, 1, sampleCard(1, 1, nil), sampleCard(2, 1, nil), sampleCard(3, 1, nil))
	if _, d, _, err := st.ApplyRoster(r, pub(org), acc); err != nil || d != DecisionApplied {
		t.Fatalf("seed roster: %q %v", d, err)
	}
	return &phaseBFixture{kv: kv, st: st, org: org, root: fixedKey(t, 77), policy: acc, roster: r}
}

func (f *phaseBFixture) envelope(t *testing.T, rev uint64, prev string, mut func(*LinkEnvelope)) *LinkEnvelope {
	t.Helper()
	e := &LinkEnvelope{
		OrgID: "org-acme", OrgRootKeyID: f.policy.RootKeyID, Scope: "fleet",
		AllowedDomains: []string{"eng", "ops"}, MaxClearance: 2,
		Revision: rev, PrevDigest: prev,
		IssuedAt: testNow - 60, ExpiresAt: testNow + 90*24*3600,
	}
	if mut != nil {
		mut(e)
	}
	if err := e.Sign(f.root); err != nil {
		t.Fatalf("sign envelope: %v", err)
	}
	return e
}

func (f *phaseBFixture) ctx(now int64) EnvelopeContext {
	return EnvelopeContext{RootCredentialID: rootID(f.root), RootPub: pub(f.root), Policy: f.policy, RosterScope: "fleet", Now: now}
}

func TestEnvelopeSignVerifyAndTamper(t *testing.T) {
	f := newPhaseBFixture(t)
	e := f.envelope(t, 1, "", nil)
	if e.Schema != EnvelopeSchemaID || e.Mode != EnvelopeModeAsk {
		t.Fatalf("defaults not filled: %q %q", e.Schema, e.Mode)
	}
	if err := e.VerifyUnderRoot(rootID(f.root), pub(f.root)); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Domain order does not change the signature.
	swapped := *e
	swapped.AllowedDomains = []string{"ops", "eng"}
	if err := swapped.VerifyUnderRoot(rootID(f.root), pub(f.root)); err != nil {
		t.Fatalf("domain order changed the signature: %v", err)
	}
	for name, mut := range map[string]func(*LinkEnvelope){
		"widened domains":  func(e *LinkEnvelope) { e.AllowedDomains = []string{"*"} },
		"raised clearance": func(e *LinkEnvelope) { e.MaxClearance = 3 },
		"other scope":      func(e *LinkEnvelope) { e.Scope = "lab" },
		"later expiry":     func(e *LinkEnvelope) { e.ExpiresAt++ },
		"other revision":   func(e *LinkEnvelope) { e.Revision = 2; e.PrevDigest = strings.Repeat("0", 64) },
	} {
		c := *e
		c.AllowedDomains = append([]string(nil), e.AllowedDomains...)
		mut(&c)
		if err := c.VerifyUnderRoot(rootID(f.root), pub(f.root)); err == nil {
			t.Errorf("%s: tampered envelope verified", name)
		}
	}
	other := fixedKey(t, 78)
	if err := e.VerifyUnderRoot(rootID(other), pub(other)); err == nil {
		t.Error("envelope verified under a different Root")
	}
	if err := e.VerifyUnderRoot("", nil); err == nil {
		t.Error("envelope verified with no Root")
	}
}

func TestEnvelopeValidation(t *testing.T) {
	f := newPhaseBFixture(t)
	long := strings.Repeat("d", MaxEnvelopeDomainBytes+1)
	many := make([]string, MaxEnvelopeDomains+1)
	for i := range many {
		many[i] = "d" + itoa(i)
	}
	for name, mut := range map[string]func(*LinkEnvelope){
		"mode auto":             func(e *LinkEnvelope) { e.Mode = "auto" },
		"clearance 5":           func(e *LinkEnvelope) { e.MaxClearance = 5 },
		"window over 365d":      func(e *LinkEnvelope) { e.ExpiresAt = e.IssuedAt + MaxEnvelopeWindowSeconds + 1 },
		"rev 0":                 func(e *LinkEnvelope) { e.Revision = 0 },
		"rev 1 with prev":       func(e *LinkEnvelope) { e.PrevDigest = strings.Repeat("a", 64) },
		"rev 2 without prev":    func(e *LinkEnvelope) { e.Revision = 2 },
		"wildcard plus domain":  func(e *LinkEnvelope) { e.AllowedDomains = []string{"*", "eng"} },
		"duplicate domain":      func(e *LinkEnvelope) { e.AllowedDomains = []string{"eng", "eng"} },
		"padded domain":         func(e *LinkEnvelope) { e.AllowedDomains = []string{" eng"} },
		"control in domain":     func(e *LinkEnvelope) { e.AllowedDomains = []string{"en\x01g"} },
		"domain too long":       func(e *LinkEnvelope) { e.AllowedDomains = []string{long} },
		"too many domains":      func(e *LinkEnvelope) { e.AllowedDomains = many },
		"empty scope":           func(e *LinkEnvelope) { e.Scope = "" },
		"bad org root key":      func(e *LinkEnvelope) { e.OrgRootKeyID = "ABC" },
		"expires before issued": func(e *LinkEnvelope) { e.ExpiresAt = e.IssuedAt },
	} {
		e := &LinkEnvelope{
			OrgID: "org-acme", OrgRootKeyID: f.policy.RootKeyID, Scope: "fleet",
			AllowedDomains: []string{"eng"}, MaxClearance: 1, Revision: 1,
			IssuedAt: testNow, ExpiresAt: testNow + 3600,
		}
		mut(e)
		if err := e.Sign(f.root); err == nil {
			t.Errorf("%s: signed an invalid envelope", name)
		}
	}
	// Empty allowed_domains is a valid trust-only envelope.
	e := f.envelope(t, 1, "", func(e *LinkEnvelope) { e.AllowedDomains = nil })
	if ok, why := e.AllowsSharing(nil, 0); !ok {
		t.Fatalf("trust-only sharing refused: %s", why)
	}
	if ok, _ := e.AllowsSharing([]string{"eng"}, 0); ok {
		t.Fatal("trust-only envelope allowed a domain")
	}
}

func TestEnvelopeCoversAndAllowsSharing(t *testing.T) {
	f := newPhaseBFixture(t)
	e := f.envelope(t, 1, "", nil)
	if ok, why := e.Covers(f.ctx(testNow)); !ok {
		t.Fatalf("covers: %s", why)
	}
	var nilEnv *LinkEnvelope
	if ok, _ := nilEnv.Covers(f.ctx(testNow)); ok {
		t.Fatal("a missing envelope covered")
	}
	expired := f.ctx(e.ExpiresAt)
	if ok, _ := e.Covers(expired); ok {
		t.Fatal("expired envelope covered")
	}
	rotated := f.ctx(testNow)
	other := fixedKey(t, 79)
	rotated.RootCredentialID, rotated.RootPub = rootID(other), pub(other)
	if ok, why := e.Covers(rotated); ok || !strings.Contains(why, "current Root") {
		t.Fatalf("Root rotation did not retire the envelope: %v %s", ok, why)
	}
	wrongScope := f.ctx(testNow)
	wrongScope.RosterScope = "lab"
	if ok, _ := e.Covers(wrongScope); ok {
		t.Fatal("envelope covered a roster of another scope")
	}
	newOrgRoot := f.ctx(testNow)
	newOrgRoot.Policy.RootKeyID = strings.Repeat("e", 64)
	if ok, _ := e.Covers(newOrgRoot); ok {
		t.Fatal("envelope covered after an org root change")
	}
	noPolicy := f.ctx(testNow)
	noPolicy.Policy = AcceptedState{}
	if ok, _ := e.Covers(noPolicy); ok {
		t.Fatal("envelope covered without an accepted policy")
	}

	for _, tc := range []struct {
		domains   []string
		clearance uint8
		want      bool
	}{
		{nil, 0, true},
		{[]string{"eng"}, 2, true},
		{[]string{"eng.public", "ops"}, 1, true},
		{[]string{"hr"}, 1, false},
		{[]string{"engineering"}, 1, false}, // not a dotted child of "eng"
		{[]string{"*"}, 1, false},
		{[]string{"eng"}, 3, false},
	} {
		if got, why := e.AllowsSharing(tc.domains, tc.clearance); got != tc.want {
			t.Errorf("AllowsSharing(%v, %d) = %v (%s), want %v", tc.domains, tc.clearance, got, why, tc.want)
		}
	}
	wild := f.envelope(t, 1, "", func(e *LinkEnvelope) { e.AllowedDomains = []string{"*"} })
	if ok, _ := wild.AllowsSharing([]string{"*"}, 2); !ok {
		t.Fatal("wildcard envelope refused wildcard sharing")
	}
}

func TestApplyEnvelopeOrdering(t *testing.T) {
	f := newPhaseBFixture(t)
	rid, rpub := rootID(f.root), pub(f.root)
	e1 := f.envelope(t, 1, "", nil)
	if d, _, err := f.st.ApplyEnvelope(e1, rid, rpub, f.policy, testNow); err != nil || d != DecisionApplied {
		t.Fatalf("first: %q %v", d, err)
	}
	writes := len(f.kv.writes)
	if d, _, err := f.st.ApplyEnvelope(e1, rid, rpub, f.policy, testNow); err != nil || d != DecisionNoOpSameDigest || len(f.kv.writes) != writes {
		t.Fatalf("same digest: %q %v", d, err)
	}
	// Same revision, different content: refused, notice persisted, kept e1.
	conflict := f.envelope(t, 1, "", func(e *LinkEnvelope) { e.AllowedDomains = []string{"*"} })
	d, notice, err := f.st.ApplyEnvelope(conflict, rid, rpub, f.policy, testNow)
	if !errors.Is(err, ErrEnvelopeEquivocation) || d != DecisionRejectedEquivocation || notice == nil {
		t.Fatalf("equivocation: %q %+v %v", d, notice, err)
	}
	if rec, _, _ := NewStore(f.kv).LoadEnvelope("org-acme"); rec.Digest != e1.Digest() {
		t.Fatal("equivocating envelope replaced the stored one")
	}
	if n, ok, _ := NewStore(f.kv).LoadEnvelopeNotice("org-acme"); !ok || n.ConflictDigest != conflict.Digest() || n.AcceptedDigest != e1.Digest() {
		t.Fatalf("notice not durable: %+v", n)
	}
	// A higher revision must chain to the stored digest.
	broken := f.envelope(t, 2, strings.Repeat("0", 64), nil)
	if _, _, err := f.st.ApplyEnvelope(broken, rid, rpub, f.policy, testNow); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("broken chain: %v", err)
	}
	e2 := f.envelope(t, 2, e1.Digest(), func(e *LinkEnvelope) { e.AllowedDomains = []string{"eng"} })
	if d, _, err := f.st.ApplyEnvelope(e2, rid, rpub, f.policy, testNow); err != nil || d != DecisionApplied {
		t.Fatalf("higher: %q %v", d, err)
	}
	// Restart: rollback stays refused, history kept.
	re := NewStore(f.kv)
	if d, _, err := re.ApplyEnvelope(e1, rid, rpub, f.policy, testNow); !errors.Is(err, ErrEnvelopeRollback) || d != DecisionRejectedRollback {
		t.Fatalf("rollback after restart: %q %v", d, err)
	}
	if h, ok, _ := re.LoadEnvelopeHistory("org-acme", 1); !ok || h.Digest != e1.Digest() {
		t.Fatal("revision 1 history lost")
	}
	if h, ok, _ := re.LoadEnvelopeHistory("org-acme", 2); !ok || h.Digest != e2.Digest() {
		t.Fatal("revision 2 history missing")
	}
}

func TestApplyEnvelopePreconditions(t *testing.T) {
	f := newPhaseBFixture(t)
	rid, rpub := rootID(f.root), pub(f.root)
	other := fixedKey(t, 78)
	cases := map[string]struct {
		e      *LinkEnvelope
		rootID string
		root   ed25519.PublicKey
		policy AcceptedState
		now    int64
	}{
		"non-Root signer":    {f.envelopeSignedBy(t, other), rid, rpub, f.policy, testNow},
		"no Root resolvable": {f.envelope(t, 1, "", nil), "", nil, f.policy, testNow},
		"no accepted policy": {f.envelope(t, 1, "", nil), rid, rpub, AcceptedState{}, testNow},
		"other org":          {f.envelope(t, 1, "", func(e *LinkEnvelope) { e.OrgID = "org-other" }), rid, rpub, f.policy, testNow},
		"org root mismatch":  {f.envelope(t, 1, "", func(e *LinkEnvelope) { e.OrgRootKeyID = strings.Repeat("e", 64) }), rid, rpub, f.policy, testNow},
		"already expired":    {f.envelope(t, 1, "", nil), rid, rpub, f.policy, testNow + 91*24*3600},
		"first is not rev 1": {f.envelope(t, 2, strings.Repeat("0", 64), nil), rid, rpub, f.policy, testNow},
	}
	for name, tc := range cases {
		before := len(f.kv.writes)
		if _, _, err := f.st.ApplyEnvelope(tc.e, tc.rootID, tc.root, tc.policy, tc.now); !errors.Is(err, ErrEnvelopeInvalid) {
			t.Errorf("%s: %v", name, err)
		}
		if len(f.kv.writes) != before {
			t.Errorf("%s: wrote state", name)
		}
	}
	// A stale accepted-policy view refuses with nothing written.
	stale := f.policy
	stale.Revision = 2
	if _, _, err := f.st.ApplyEnvelope(f.envelope(t, 1, "", nil), rid, rpub, stale, testNow); !errors.Is(err, ErrEnvelopePolicyChanged) {
		t.Fatalf("stale policy view: %v", err)
	}
}

func (f *phaseBFixture) envelopeSignedBy(t *testing.T, k ed25519.PrivateKey) *LinkEnvelope {
	t.Helper()
	e := &LinkEnvelope{
		OrgID: "org-acme", OrgRootKeyID: f.policy.RootKeyID, Scope: "fleet",
		AllowedDomains: []string{"eng"}, MaxClearance: 1, Revision: 1,
		IssuedAt: testNow - 60, ExpiresAt: testNow + 3600,
	}
	if err := e.Sign(k); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return e
}

// A policy rotation between the envelope check and the CAS refuses the write.
func TestApplyEnvelopeConcurrentRotationFailsCAS(t *testing.T) {
	f := newPhaseBFixture(t)
	rid, rpub := rootID(f.root), pub(f.root)
	e1 := f.envelope(t, 1, "", nil)
	if _, _, err := f.st.ApplyEnvelope(e1, rid, rpub, f.policy, testNow); err != nil {
		t.Fatal(err)
	}
	e2 := f.envelope(t, 2, e1.Digest(), nil)
	fired := false
	f.kv.afterGet = func(k string) {
		if k == envelopeKey("org-acme") && !fired {
			fired = true
			p := signed(t, f.org, func(p *Policy) {
				p.Schema = SchemaIDV2
				p.Revision = 4
				p.IssuedAt, p.NotBefore, p.Expiry = testNow-60, testNow-60, testNow+30*24*3600
				p.Payload.PeerAllowList = []string{agentHex(1)}
			})
			if _, d, _, err := NewStore(f.kv).ApplyPolicy(p, pub(f.org)); err != nil || d != DecisionApplied {
				t.Errorf("rotate: %q %v", d, err)
			}
		}
	}
	if _, _, err := f.st.ApplyEnvelope(e2, rid, rpub, f.policy, testNow); !errors.Is(err, ErrEnvelopePolicyChanged) {
		t.Fatalf("rotation between check and CAS: %v", err)
	}
	f.kv.afterGet = nil
	if rec, _, _ := f.st.LoadEnvelope("org-acme"); rec.Envelope.Revision != 1 {
		t.Fatal("envelope written under a rotated policy")
	}
}
