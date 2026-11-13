package orgpolicy

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func agentHex(b byte) string {
	return hex.EncodeToString(pub(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{b}, ed25519.SeedSize))))
}

func v2Policy(peers ...string) *Policy {
	p := samplePolicy()
	p.Schema = SchemaIDV2
	p.Payload.PeerAllowList = peers
	return p
}

func TestPolicyV2SignVerifyKeepsSchema(t *testing.T) {
	key := fixedKey(t, 1)
	p := v2Policy(agentHex(7), agentHex(8))
	if err := p.Sign(key); err != nil {
		t.Fatalf("sign v2: %v", err)
	}
	if p.Schema != SchemaIDV2 {
		t.Fatalf("Sign rewrote schema to %q", p.Schema)
	}
	if err := p.Verify(pub(key)); err != nil {
		t.Fatalf("verify v2: %v", err)
	}
	// Schema is bound: flipping it breaks the signature.
	p.Schema = SchemaID
	p.Payload.PeerAllowList = []string{agentHex(7), agentHex(8)}
	if err := p.Verify(pub(key)); err == nil {
		t.Fatal("schema downgrade verified")
	}
}

func TestPolicySignDefaultsToV1(t *testing.T) {
	p := samplePolicy()
	p.Schema = ""
	if err := p.Sign(fixedKey(t, 1)); err != nil {
		t.Fatal(err)
	}
	if p.Schema != SchemaID {
		t.Fatalf("empty schema signed as %q, want v1", p.Schema)
	}
}

func TestPolicyV2AllowListFormat(t *testing.T) {
	key := fixedKey(t, 1)
	good := agentHex(7)
	cases := map[string]string{
		"chain id":   "chain-a",
		"upper case": strings.ToUpper(good),
		"short":      good[:62],
		"long":       good + "00",
		"non-hex":    "z" + good[1:],
	}
	for name, entry := range cases {
		if err := v2Policy(entry).Sign(key); err == nil {
			t.Fatalf("%s: v2 accepted allow-list entry %q", name, entry)
		}
	}
	if err := v2Policy(good, good).Sign(key); err == nil {
		t.Fatal("v2 accepted a duplicate entry")
	}
}

func TestPolicyV2FullAllowListFitsCap(t *testing.T) {
	key := fixedKey(t, 1)
	peers := make([]string, maxPeerAllowList)
	for i := range peers {
		pk, _, _ := ed25519.GenerateKey(nil)
		peers[i] = hex.EncodeToString(pk)
	}
	p := v2Policy(peers...)
	if err := p.Sign(key); err != nil {
		t.Fatalf("1024-entry v2 allow-list refused: %v", err)
	}
	if err := p.Verify(pub(key)); err != nil {
		t.Fatalf("verify: %v", err)
	}
	// Past the canonical cap is still refused at Sign and Verify.
	p.Payload.ChecksVersion = strings.Repeat("x", maxCanonicalBytes)
	if err := p.Sign(key); err == nil || !strings.Contains(err.Error(), "exceeds cap") {
		t.Fatalf("over-cap policy signed: %v", err)
	}
}

func TestAllowsPeer(t *testing.T) {
	member, outsider := agentHex(7), agentHex(8)
	if ok, _ := v2Policy(member).AllowsPeer(member); !ok {
		t.Fatal("v2 member not allowed")
	}
	if ok, why := v2Policy(member).AllowsPeer(outsider); ok || why == "" {
		t.Fatal("v2 outsider allowed")
	}
	if ok, _ := v2Policy(member).AllowsPeer(strings.ToUpper(member)); ok {
		t.Fatal("upper-case spelling allowed")
	}
	v1 := samplePolicy()
	v1.Payload.PeerAllowList = []string{member}
	if ok, why := v1.AllowsPeer(member); ok || !strings.Contains(why, "v1") {
		t.Fatalf("v1 allow-list granted eligibility (ok=%v why=%q)", ok, why)
	}
	empty := samplePolicy()
	empty.Payload.PeerAllowList = nil
	if ok, _ := empty.AllowsPeer(member); ok {
		t.Fatal("empty allow-list granted eligibility")
	}
	var nilPolicy *Policy
	if ok, _ := nilPolicy.AllowsPeer(member); ok {
		t.Fatal("nil policy granted eligibility")
	}
}

func TestPeerEligible(t *testing.T) {
	key := fixedKey(t, 1)
	member := agentHex(7)
	p := signed(t, key, func(p *Policy) { p.Schema = SchemaIDV2; p.Payload.PeerAllowList = []string{member} })
	st := stateFrom(p)
	if ok, why := PeerEligible(st, p, member, 1500); !ok {
		t.Fatalf("current member not eligible: %s", why)
	}
	if ok, why := PeerEligible(st, p, member, 2500); ok || !strings.Contains(why, "current org policy") {
		t.Fatalf("expired policy granted eligibility: %s", why)
	}
	if ok, _ := PeerEligible(AcceptedState{}, p, member, 1500); ok {
		t.Fatal("no accepted policy granted eligibility")
	}
	if ok, _ := PeerEligible(st, nil, member, 1500); ok {
		t.Fatal("missing body granted eligibility")
	}
	// A different (validly signed) body than the accepted one never decides.
	other := signed(t, key, func(p *Policy) {
		p.Schema = SchemaIDV2
		p.Payload.PeerAllowList = []string{member, agentHex(9)}
	})
	if ok, why := PeerEligible(st, other, agentHex(9), 1500); ok || !strings.Contains(why, "does not match") {
		t.Fatalf("substituted body granted eligibility: %s", why)
	}
}

func TestLoadAcceptedPolicyAndBackfill(t *testing.T) {
	kv := newFakeKV()
	st := NewStore(kv)
	key := fixedKey(t, 1)
	p := signed(t, key, func(p *Policy) { p.Revision = 5 })
	if _, d, _, err := st.ApplyPolicy(p, pub(key)); err != nil || d != DecisionApplied {
		t.Fatalf("apply: d=%q err=%v", d, err)
	}
	acc, body, err := st.LoadAcceptedPolicy(p.OrgID)
	if err != nil || body == nil || body.Digest() != acc.Digest {
		t.Fatalf("body not stored with the accepted state: body=%v err=%v", body, err)
	}

	// A node that accepted before bodies were stored: drop the body, then an
	// identical re-delivery backfills it without touching the high-water mark.
	delete(kv.m, bodyKey(p.OrgID))
	accRaw := append([]byte(nil), kv.m[acceptedKey(p.OrgID)]...)
	if _, body, _ = st.LoadAcceptedPolicy(p.OrgID); body != nil {
		t.Fatal("body reported after deletion")
	}
	if _, d, _, err := st.ApplyPolicy(p, pub(key)); err != nil || d != DecisionNoOpSameDigest {
		t.Fatalf("redelivery: d=%q err=%v", d, err)
	}
	if string(kv.m[acceptedKey(p.OrgID)]) != string(accRaw) {
		t.Fatal("backfill changed the accepted high-water mark")
	}
	if _, body, _ = st.LoadAcceptedPolicy(p.OrgID); body == nil {
		t.Fatal("identical redelivery did not backfill the body")
	}

	// A body that does not match the high-water mark is never returned.
	tampered := *p
	tampered.Payload.ChecksVersion = "other"
	raw, _ := json.Marshal(&tampered)
	kv.m[bodyKey(p.OrgID)] = raw
	if _, body, _ = st.LoadAcceptedPolicy(p.OrgID); body != nil {
		t.Fatal("mismatched body returned")
	}
}
