package orgpolicy

import (
	"bytes"
	"crypto/ed25519"
	"strings"
	"testing"
)

const testNow = int64(1_800_000_000)

func nodeKey(b byte) ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{b}, ed25519.SeedSize))
}

func sampleCard(b byte, rev uint64, mut func(*LinkCard)) LinkCard {
	c := LinkCard{
		OrgID:     "org-acme",
		ChainID:   "node-" + string(rune('a'+b%26)) + "-chain",
		CASPKIPin: strings.Repeat("ab", 32),
		Endpoint:  "https://10.0.0.1:8443",
		CardRev:   rev,
		IssuedAt:  testNow - 60,
		ExpiresAt: testNow + 7*24*3600,
	}
	if mut != nil {
		mut(&c)
	}
	if err := c.Sign(nodeKey(b)); err != nil {
		panic(err)
	}
	return c
}

func sampleRoster(key ed25519.PrivateKey, rev uint64, cards ...LinkCard) *Roster {
	r := &Roster{
		OrgID: "org-acme", Scope: "fleet", Revision: rev, KeyEpoch: "epoch-1",
		IssuedAt: testNow - 60, NotBefore: testNow - 60, Expiry: testNow + 3*24*3600,
		Cards: cards,
	}
	if err := r.Sign(key); err != nil {
		panic(err)
	}
	return r
}

func TestLinkCardSignVerifyAndTamper(t *testing.T) {
	c := sampleCard(1, 1, nil)
	if err := c.Verify(); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if c.AgentPub != agentHex(1) {
		t.Fatal("card agent_pub is not the signer")
	}
	if !c.CurrentAt(testNow) || c.CurrentAt(c.ExpiresAt) {
		t.Fatal("card window wrong")
	}
	tampered := c
	tampered.Endpoint = "https://10.0.0.2:8443"
	if err := tampered.Verify(); err == nil {
		t.Fatal("tampered endpoint verified")
	}
	// The org (any other key) cannot mint a card for this agent.
	forged := c
	forged.Sig = ""
	if err := forged.Sign(nodeKey(2)); err != nil {
		t.Fatal(err)
	}
	if forged.AgentPub == c.AgentPub {
		t.Fatal("re-signing kept the original agent_pub")
	}
}

func TestLinkCardValidation(t *testing.T) {
	cases := map[string]func(*LinkCard){
		"zero rev":       func(c *LinkCard) { c.CardRev = 0 },
		"empty org":      func(c *LinkCard) { c.OrgID = "" },
		"upper chain":    func(c *LinkCard) { c.ChainID = "Node-A" },
		"long chain":     func(c *LinkCard) { c.ChainID = strings.Repeat("a", 51) },
		"short pin":      func(c *LinkCard) { c.CASPKIPin = "abcd" },
		"upper pin":      func(c *LinkCard) { c.CASPKIPin = strings.Repeat("AB", 32) },
		"http endpoint":  func(c *LinkCard) { c.Endpoint = "http://10.0.0.1:8443" },
		"no port":        func(c *LinkCard) { c.Endpoint = "https://10.0.0.1" },
		"path":           func(c *LinkCard) { c.Endpoint = "https://10.0.0.1:8443/x" },
		"long endpoint":  func(c *LinkCard) { c.Endpoint = "https://" + strings.Repeat("a", 250) + ".io:1" },
		"window > 30d":   func(c *LinkCard) { c.ExpiresAt = c.IssuedAt + MaxLinkCardWindowSeconds + 1 },
		"expiry<=issued": func(c *LinkCard) { c.ExpiresAt = c.IssuedAt },
		"routes no peer": func(c *LinkCard) { c.P2PRoutes = []string{"/ip4/1.2.3.4/tcp/1"} },
		"9 routes": func(c *LinkCard) {
			c.P2PPeerID = "peer"
			for i := 0; i < 9; i++ {
				c.P2PRoutes = append(c.P2PRoutes, "/ip4/1.2.3.4/tcp/"+string(rune('1'+i)))
			}
		},
		"dup route": func(c *LinkCard) { c.P2PPeerID = "peer"; c.P2PRoutes = []string{"/r", "/r"} },
	}
	for name, mut := range cases {
		c := LinkCard{
			OrgID: "org-acme", ChainID: "node-a", CASPKIPin: strings.Repeat("ab", 32),
			Endpoint: "https://10.0.0.1:8443", CardRev: 1, IssuedAt: testNow, ExpiresAt: testNow + 3600,
		}
		mut(&c)
		if err := c.Sign(nodeKey(1)); err == nil {
			t.Fatalf("%s: invalid card signed", name)
		}
	}
	ok := sampleCard(1, 1, func(c *LinkCard) {
		c.P2PPeerID = "peer"
		c.P2PRoutes = []string{"/ip4/1.2.3.4/tcp/1", "/ip4/5.6.7.8/tcp/2"}
	})
	if err := ok.Verify(); err != nil {
		t.Fatalf("valid routed card: %v", err)
	}
}

func TestRosterSignVerifyOrderIndependent(t *testing.T) {
	org := fixedKey(t, 50)
	a, b := sampleCard(1, 1, nil), sampleCard(2, 1, nil)
	r1 := sampleRoster(org, 1, a, b)
	r2 := sampleRoster(org, 1, b, a)
	if r1.Digest() != r2.Digest() {
		t.Fatal("roster digest depends on card order")
	}
	if err := r1.Verify(pub(org)); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := r1.Verify(pub(fixedKey(t, 51))); err == nil {
		t.Fatal("roster verified under the wrong org key")
	}
	r1.Cards[0].Endpoint = "https://10.9.9.9:1"
	if err := r1.Verify(pub(org)); err == nil {
		t.Fatal("roster with a tampered embedded card verified")
	}
}

func TestRosterRejections(t *testing.T) {
	org := fixedKey(t, 50)
	a := sampleCard(1, 1, nil)
	otherOrg := sampleCard(3, 1, func(c *LinkCard) { c.OrgID = "org-other" })
	sameChain := sampleCard(4, 1, func(c *LinkCard) { c.ChainID = a.ChainID })
	badSig := sampleCard(5, 1, nil)
	badSig.Sig = strings.Repeat("00", 64)
	cases := map[string]*Roster{
		"card other org": {Cards: []LinkCard{a, otherOrg}},
		"dup agent":      {Cards: []LinkCard{a, a}},
		"dup chain":      {Cards: []LinkCard{a, sameChain}},
		"bad card sig":   {Cards: []LinkCard{a, badSig}},
		"no expiry":      {Cards: []LinkCard{a}},
		"window > 7d":    {Cards: []LinkCard{a}},
	}
	for name, r := range cases {
		r.OrgID, r.Scope, r.Revision, r.KeyEpoch = "org-acme", "fleet", 1, "epoch-1"
		r.IssuedAt, r.NotBefore, r.Expiry = testNow, testNow, testNow+3600
		switch name {
		case "no expiry":
			r.Expiry = 0
		case "window > 7d":
			r.Expiry = testNow + MaxRosterWindowSeconds + 1
		}
		if err := r.Sign(org); err == nil {
			t.Fatalf("%s: invalid roster signed", name)
		}
	}
	tooMany := &Roster{OrgID: "org-acme", Scope: "fleet", Revision: 1, KeyEpoch: "e",
		IssuedAt: testNow, NotBefore: testNow, Expiry: testNow + 3600, Cards: make([]LinkCard, MaxRosterCards+1)}
	if err := tooMany.Sign(org); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("1025-card roster: %v", err)
	}
}

func TestRosterCanonicalCap(t *testing.T) {
	org := fixedKey(t, 50)
	cards := make([]LinkCard, MaxRosterCards)
	for i := range cards {
		_, sk, _ := ed25519.GenerateKey(nil)
		c := LinkCard{
			OrgID: "org-acme", ChainID: "n" + strings.Repeat("0", 4) + itoa(i), CASPKIPin: strings.Repeat("ab", 32),
			Endpoint: "https://10.0.0.1:8443", CardRev: 1, IssuedAt: testNow, ExpiresAt: testNow + 3600,
			P2PPeerID: "peer",
		}
		for j := 0; j < maxCardRoutes; j++ {
			c.P2PRoutes = append(c.P2PRoutes, "/dns4/"+strings.Repeat("r", 100)+itoa(j)+"/tcp/4001")
		}
		if err := c.Sign(sk); err != nil {
			t.Fatal(err)
		}
		cards[i] = c
	}
	r := &Roster{OrgID: "org-acme", Scope: "fleet", Revision: 1, KeyEpoch: "e",
		IssuedAt: testNow, NotBefore: testNow, Expiry: testNow + 3600, Cards: cards}
	if err := r.Sign(org); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("over-1MiB roster: canonical=%d err=%v", len(r.CanonicalBytes()), err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}

func TestAdmitRosterOrdering(t *testing.T) {
	org := fixedKey(t, 50)
	a := sampleCard(1, 1, nil)
	r3 := sampleRoster(org, 3, a)
	st, d, err := AdmitRoster(RosterState{}, r3, pub(org), "fleet")
	if err != nil || d != DecisionApplied || st.Revision != 3 {
		t.Fatalf("first: d=%q err=%v", d, err)
	}
	if _, d, _ = AdmitRoster(st, r3, pub(org), "fleet"); d != DecisionNoOpSameDigest {
		t.Fatalf("same: %q", d)
	}
	if _, d, _ = AdmitRoster(st, sampleRoster(org, 2, a), pub(org), "fleet"); d != DecisionRejectedRollback {
		t.Fatalf("rollback: %q", d)
	}
	if _, d, _ = AdmitRoster(st, sampleRoster(org, 3), pub(org), "fleet"); d != DecisionRejectedEquivocation {
		t.Fatalf("equivocation: %q", d)
	}
	if next, d, _ := AdmitRoster(st, sampleRoster(org, 4), pub(org), "fleet"); d != DecisionApplied || next.Revision != 4 {
		t.Fatalf("newer: %q", d)
	}
	if _, _, err = AdmitRoster(st, sampleRoster(org, 4), pub(org), "other-scope"); err == nil {
		t.Fatal("scope mismatch admitted")
	}
	if _, _, err = AdmitRoster(st, sampleRoster(fixedKey(t, 51), 4), pub(org), "fleet"); err == nil {
		t.Fatal("forged roster admitted")
	}
}

func TestApplyRosterPersistsAndSurvivesRestart(t *testing.T) {
	kv := newFakeKV()
	org := fixedKey(t, 50)
	a, b := sampleCard(1, 1, nil), sampleCard(2, 1, nil)
	st := NewStore(kv)
	if _, d, _, err := st.ApplyRoster(sampleRoster(org, 1, a, b), pub(org), "fleet"); err != nil || d != DecisionApplied {
		t.Fatalf("apply: d=%q err=%v", d, err)
	}
	if len(kv.writes) != 1 {
		t.Fatalf("apply wrote %d batches, want 1", len(kv.writes))
	}

	restarted := NewStore(kv)
	rs, body, err := restarted.LoadRoster("org-acme")
	if err != nil || rs.Revision != 1 || body == nil || len(body.Cards) != 2 {
		t.Fatalf("reload: rs=%+v body=%v err=%v", rs, body, err)
	}
	if h, ok, _ := restarted.LoadRosterHistory("org-acme", 1); !ok || h.Digest != rs.Digest {
		t.Fatal("history entry missing")
	}
	if rec, ok, _ := restarted.LoadCard("org-acme", a.AgentPub); !ok || rec.CardRev != 1 {
		t.Fatal("card floor missing")
	}

	// Card a leaves the roster at rev 2; its floor stays.
	a2 := sampleCard(1, 2, func(c *LinkCard) { c.Endpoint = "https://10.0.0.7:8443" })
	if _, d, _, err := restarted.ApplyRoster(sampleRoster(org, 2, b, a2), pub(org), "fleet"); err != nil || d != DecisionApplied {
		t.Fatalf("rev2: d=%q err=%v", d, err)
	}
	if _, d, _, err := restarted.ApplyRoster(sampleRoster(org, 3, b), pub(org), "fleet"); err != nil || d != DecisionApplied {
		t.Fatalf("rev3 without a: d=%q err=%v", d, err)
	}
	if rec, _, _ := restarted.LoadCard("org-acme", a.AgentPub); rec.CardRev != 2 {
		t.Fatalf("card floor after removal = %d, want 2", rec.CardRev)
	}

	// A later roster that brings back a's OLD card regresses it: whole roster
	// rejected, nothing written.
	before := len(kv.writes)
	_, _, _, err = restarted.ApplyRoster(sampleRoster(org, 4, b, a), pub(org), "fleet")
	if err != ErrRosterCardRegression {
		t.Fatalf("regression: err=%v", err)
	}
	if len(kv.writes) != before {
		t.Fatal("card regression wrote state")
	}
	if rs, _, _ := restarted.LoadRoster("org-acme"); rs.Revision != 3 {
		t.Fatalf("roster advanced on regression: %d", rs.Revision)
	}
}

func TestApplyRosterNoOpRollbackWriteNothing(t *testing.T) {
	kv := newFakeKV()
	org := fixedKey(t, 50)
	st := NewStore(kv)
	r := sampleRoster(org, 5, sampleCard(1, 1, nil))
	if _, _, _, err := st.ApplyRoster(r, pub(org), "fleet"); err != nil {
		t.Fatal(err)
	}
	n := len(kv.writes)
	if _, d, _, _ := st.ApplyRoster(r, pub(org), "fleet"); d != DecisionNoOpSameDigest {
		t.Fatalf("noop: %q", d)
	}
	if _, d, _, _ := st.ApplyRoster(sampleRoster(org, 4), pub(org), "fleet"); d != DecisionRejectedRollback {
		t.Fatalf("rollback: %q", d)
	}
	if len(kv.writes) != n {
		t.Fatal("noop/rollback wrote state")
	}
}

func TestApplyRosterEquivocationNotices(t *testing.T) {
	kv := newFakeKV()
	org := fixedKey(t, 50)
	st := NewStore(kv)
	a := sampleCard(1, 1, nil)
	if _, _, _, err := st.ApplyRoster(sampleRoster(org, 1, a), pub(org), "fleet"); err != nil {
		t.Fatal(err)
	}
	// Roster equivocation.
	_, d, notice, err := st.ApplyRoster(sampleRoster(org, 1), pub(org), "fleet")
	if err != nil || d != DecisionRejectedEquivocation || notice == nil || notice.Kind != "roster" {
		t.Fatalf("roster equivocation: d=%q notice=%+v err=%v", d, notice, err)
	}
	// Card equivocation: same card_rev, different content, in a newer roster.
	aConflict := sampleCard(1, 1, func(c *LinkCard) { c.Endpoint = "https://10.0.0.9:8443" })
	_, d, notice, err = st.ApplyRoster(sampleRoster(org, 2, aConflict), pub(org), "fleet")
	if err != nil || d != DecisionRejectedEquivocation || notice == nil || notice.Kind != "card" || notice.AgentPub != a.AgentPub {
		t.Fatalf("card equivocation: d=%q notice=%+v err=%v", d, notice, err)
	}
	if rs, _, _ := st.LoadRoster("org-acme"); rs.Revision != 1 {
		t.Fatal("card equivocation advanced the roster")
	}
	got, ok, _ := NewStore(kv).LoadRosterNotice("org-acme")
	if !ok || got.Kind != "card" {
		t.Fatalf("notice not durable: %+v", got)
	}
}
