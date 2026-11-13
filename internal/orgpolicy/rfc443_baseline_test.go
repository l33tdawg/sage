package orgpolicy

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"
)

// RFC #443 §7.6: a v1 policy accepts ANY non-empty, unique string in
// PeerAllowList — chain ids, mixed case, non-hex — and still signs/verifies
// (already accepted chains stay valid), but since Phase A it grants no
// eligibility.
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
	// Phase A: such a v1 list still verifies but grants no peering eligibility.
	for _, entry := range p.Payload.PeerAllowList {
		if ok, _ := p.AllowsPeer(entry); ok {
			t.Fatalf("v1 allow-list entry %q granted eligibility", entry)
		}
	}
}

// RFC #443 §3 size: before Phase A, 1024 agent_pub hex entries exceeded the
// 16 KiB canonical cap (only 235 fit). With the 96 KiB cap the full list fits.
func TestRFC443Baseline_AgentPubAllowListVsCanonicalCap(t *testing.T) {
	key := fixedKey(t, 1)
	p := samplePolicy()
	p.Schema = SchemaIDV2
	p.Payload.PeerAllowList = make([]string, maxPeerAllowList)
	for i := range p.Payload.PeerAllowList {
		pk, _, _ := ed25519.GenerateKey(rand.Reader)
		p.Payload.PeerAllowList[i] = hex.EncodeToString(pk)
	}
	if err := p.Sign(key); err != nil {
		t.Fatalf("1024 x 64-hex allow-list refused under the %d B cap: %v", maxCanonicalBytes, err)
	}
	t.Logf("1024 agent_pub entries: canonical=%d bytes, cap=%d", len(p.CanonicalBytes()), maxCanonicalBytes)
}
