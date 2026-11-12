package orgpolicy

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
)

// RFC #443 §7.6 before-state: a v1 policy accepts ANY non-empty, unique string
// in PeerAllowList — chain ids, mixed case, non-hex — and signs/verifies.
func TestRFC443Baseline_V1AllowListAcceptsChainIDsAndFreeText(t *testing.T) {
	key := fixedKey(t, 1)
	p := samplePolicy()
	p.Payload.PeerAllowList = []string{"chain-a", "CHAIN-B", "not hex at all"}
	if err := p.Sign(key); err != nil {
		t.Fatalf("v1 Sign rejected free-text allow-list: %v", err)
	}
	if err := p.Verify(pub(key)); err != nil {
		t.Fatalf("v1 Verify rejected free-text allow-list: %v", err)
	}
	t.Logf("schema=%q accepted allow-list=%v", p.Schema, p.Payload.PeerAllowList)
}

// RFC #443 §3 size problem: 1024 agent_pub hex entries exceed today's 16 KiB
// canonical cap; record the largest count that fits and the full-list size.
func TestRFC443Baseline_AgentPubAllowListVsCanonicalCap(t *testing.T) {
	key := fixedKey(t, 1)
	mk := func(n int) *Policy {
		p := samplePolicy()
		p.Payload.PeerAllowList = make([]string, n)
		for i := range p.Payload.PeerAllowList {
			pk, _, _ := ed25519.GenerateKey(rand.Reader)
			p.Payload.PeerAllowList[i] = hex.EncodeToString(pk)
		}
		return p
	}
	full := mk(maxPeerAllowList)
	if err := full.Sign(key); err == nil {
		t.Fatal("1024 x 64-hex allow-list fit under the canonical cap")
	}
	t.Logf("1024 agent_pub entries: canonical=%d bytes, cap=%d", len(full.CanonicalBytes()), maxCanonicalBytes)
	best := 0
	for n := 1; n <= maxPeerAllowList; n++ {
		if mk(n).Sign(key) != nil {
			break
		}
		best = n
	}
	t.Logf("largest agent_pub allow-list under %d B cap: %d", maxCanonicalBytes, best)
	if best >= maxPeerAllowList {
		t.Fatal("unexpected: full list fits")
	}
}
