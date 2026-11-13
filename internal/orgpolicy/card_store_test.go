package orgpolicy

import (
	"errors"
	"testing"
)

func TestApplyOwnCardOrdering(t *testing.T) {
	kv := newFakeKV()
	org := fixedKey(t, 50)
	acc := seedRosterPolicy(t, kv, org)
	st := NewStore(kv)

	c1 := sampleCard(1, 1, nil)
	if d, _, err := st.ApplyOwnCard(&c1, acc, testNow); err != nil || d != DecisionApplied {
		t.Fatalf("first card: %q %v", d, err)
	}
	writes := len(kv.writes)
	if d, _, err := st.ApplyOwnCard(&c1, acc, testNow); err != nil || d != DecisionNoOpSameDigest || len(kv.writes) != writes {
		t.Fatalf("identical card: %q %v", d, err)
	}
	moved := sampleCard(1, 1, func(c *LinkCard) { c.Endpoint = "https://10.0.0.2:8443" })
	d, notice, err := st.ApplyOwnCard(&moved, acc, testNow)
	if err != nil || d != DecisionRejectedEquivocation || notice == nil || notice.Kind != "card" {
		t.Fatalf("same rev, different content: %q %+v %v", d, notice, err)
	}
	if rec, _, _ := st.LoadCard("org-acme", c1.AgentPub); rec.Digest != c1.Digest() {
		t.Fatal("equivocating card replaced the accepted one")
	}
	if n, ok, _ := NewStore(kv).LoadCardNotice("org-acme", c1.AgentPub); !ok || n.ConflictDigest != moved.Digest() {
		t.Fatal("card notice not durable")
	}
	c2 := sampleCard(1, 2, func(c *LinkCard) { c.Endpoint = "https://10.0.0.2:8443" })
	if d, _, err := st.ApplyOwnCard(&c2, acc, testNow); err != nil || d != DecisionApplied {
		t.Fatalf("higher rev: %q %v", d, err)
	}
	// Restart: the floor holds.
	if _, _, err := NewStore(kv).ApplyOwnCard(&c1, acc, testNow); !errors.Is(err, ErrRosterCardRegression) {
		t.Fatalf("lower rev after restart: %v", err)
	}
	// The roster path shares the floor: a roster carrying the old card regresses.
	if _, _, _, err := st.ApplyRoster(sampleRoster(org, 1, c1), pub(org), acc); !errors.Is(err, ErrRosterCardRegression) {
		t.Fatalf("roster below own card floor: %v", err)
	}
}

func TestApplyOwnCardPreconditions(t *testing.T) {
	kv := newFakeKV()
	org := fixedKey(t, 50)
	st := NewStore(kv)
	c := sampleCard(1, 1, nil)
	if _, _, err := st.ApplyOwnCard(&c, AcceptedState{}, testNow); !errors.Is(err, ErrCardInvalid) {
		t.Fatalf("no accepted policy: %v", err)
	}
	acc := seedRosterPolicy(t, kv, org)
	if _, _, err := st.ApplyOwnCard(&c, acc, c.ExpiresAt); !errors.Is(err, ErrCardInvalid) {
		t.Fatalf("expired card: %v", err)
	}
	tampered := c
	tampered.ChainID = "other-chain"
	if _, _, err := st.ApplyOwnCard(&tampered, acc, testNow); !errors.Is(err, ErrCardInvalid) {
		t.Fatalf("tampered card: %v", err)
	}
	// The accepted policy moved on after the caller checked it.
	if _, d, _, err := st.ApplyPolicy(signed(t, org, func(p *Policy) { p.Revision = 4 }), pub(org)); err != nil || d != DecisionApplied {
		t.Fatal(err)
	}
	before := len(kv.writes)
	if _, _, err := st.ApplyOwnCard(&c, acc, testNow); !errors.Is(err, ErrRosterPolicyChanged) || len(kv.writes) != before {
		t.Fatalf("stale policy generation: %v", err)
	}
}

func TestApplyOwnCardConcurrentRotationFailsCAS(t *testing.T) {
	kv := newFakeKV()
	org := fixedKey(t, 50)
	acc := seedRosterPolicy(t, kv, org)
	st := NewStore(kv)
	c := sampleCard(1, 1, nil)
	// Seed a card so the hook can fire on the card read (fakeKV skips
	// afterGet for absent keys), then mint the next rev.
	if _, _, err := st.ApplyOwnCard(&c, acc, testNow); err != nil {
		t.Fatal(err)
	}
	next := sampleCard(1, 2, nil)
	fired := false
	kv.afterGet = func(k string) {
		if k == cardKey("org-acme", c.AgentPub) && !fired {
			fired = true
			if _, d, _, err := NewStore(kv).ApplyPolicy(signed(t, org, func(p *Policy) { p.Revision = 4 }), pub(org)); err != nil || d != DecisionApplied {
				t.Errorf("rotate: %q %v", d, err)
			}
		}
	}
	if _, _, err := st.ApplyOwnCard(&next, acc, testNow); !errors.Is(err, ErrRosterPolicyChanged) {
		t.Fatalf("rotation between check and CAS: %v", err)
	}
	kv.afterGet = nil
	if rec, _, _ := st.LoadCard("org-acme", c.AgentPub); rec.CardRev != 1 {
		t.Fatal("card written under a rotated policy")
	}
}
