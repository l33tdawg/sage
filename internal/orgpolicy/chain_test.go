package orgpolicy

import (
	"crypto/ed25519"
	"testing"
)

func signed(t *testing.T, key ed25519.PrivateKey, mut func(*Policy)) *Policy {
	t.Helper()
	p := samplePolicy()
	if mut != nil {
		mut(p)
	}
	if err := p.Sign(key); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return p
}

func TestAdmitFirstPolicy(t *testing.T) {
	key := fixedKey(t, 1)
	p := signed(t, key, nil)
	next, d, err := Admit(AcceptedState{}, p, pub(key))
	if err != nil || d != DecisionApplied {
		t.Fatalf("first policy: d=%q err=%v", d, err)
	}
	if next.Revision != p.Revision || next.Digest != p.Digest() || next.IssuerKeyID != p.IssuerKeyID {
		t.Fatalf("accepted state not captured: %+v", next)
	}
}

func TestAdmitNewerApplied(t *testing.T) {
	key := fixedKey(t, 1)
	prev := stateFrom(signed(t, key, nil)) // rev 3
	p := signed(t, key, func(p *Policy) { p.Revision = 4 })
	next, d, err := Admit(prev, p, pub(key))
	if err != nil || d != DecisionApplied || next.Revision != 4 {
		t.Fatalf("newer revision: d=%q rev=%d err=%v", d, next.Revision, err)
	}
}

func TestAdmitSameDigestNoop(t *testing.T) {
	key := fixedKey(t, 1)
	p := signed(t, key, nil)
	prev := stateFrom(p)
	next, d, err := Admit(prev, p, pub(key))
	if err != nil || d != DecisionNoOpSameDigest {
		t.Fatalf("same digest: d=%q err=%v", d, err)
	}
	if next != prev {
		t.Fatal("no-op must not change accepted state")
	}
}

func TestAdmitEquivocation(t *testing.T) {
	key := fixedKey(t, 1)
	p0 := signed(t, key, nil) // rev 3, checks A
	prev := stateFrom(p0)
	p1 := signed(t, key, func(p *Policy) { p.Payload.ChecksVersion = "sage-lasting/2+supported/1" }) // rev 3, different content
	next, d, err := Admit(prev, p1, pub(key))
	if err != nil || d != DecisionRejectedEquivocation {
		t.Fatalf("equivocation: d=%q err=%v", d, err)
	}
	if next != prev {
		t.Fatal("equivocation must not overwrite accepted state")
	}
	if p0.Digest() == p1.Digest() {
		t.Fatal("test setup wrong: digests should differ")
	}
	n := NewEquivocationNotice(prev, p1)
	if n.Revision != 3 || n.AcceptedDigest != prev.Digest || n.ConflictDigest != p1.Digest() || n.OrgID != "org-acme" {
		t.Fatalf("notice fields wrong: %+v", n)
	}
}

func TestAdmitRollback(t *testing.T) {
	key := fixedKey(t, 1)
	prev := stateFrom(signed(t, key, func(p *Policy) { p.Revision = 4 }))
	p := signed(t, key, func(p *Policy) { p.Revision = 3 })
	_, d, err := Admit(prev, p, pub(key))
	if err != nil || d != DecisionRejectedRollback {
		t.Fatalf("rollback: d=%q err=%v", d, err)
	}
}

func TestAdmitForgedRejected(t *testing.T) {
	// Signed by key 2, admitted under pinned key 1: Verify must fail before any
	// ordering decision, so a forgery never earns a rollback/equivocation verdict.
	p := signed(t, fixedKey(t, 2), nil)
	if _, _, err := Admit(AcceptedState{}, p, pub(fixedKey(t, 1))); err == nil {
		t.Fatal("forged policy admitted")
	}
}

func TestAdmitOrgMismatch(t *testing.T) {
	key := fixedKey(t, 1)
	prev := stateFrom(signed(t, key, nil)) // org-acme
	p := signed(t, key, func(p *Policy) { p.OrgID = "org-other"; p.Revision = 4 })
	if _, _, err := Admit(prev, p, pub(key)); err == nil {
		t.Fatal("cross-org policy admitted against an existing org state")
	}
}
