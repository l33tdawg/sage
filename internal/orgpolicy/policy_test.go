package orgpolicy

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strings"
	"testing"
)

func fixedKey(tb testing.TB, b byte) ed25519.PrivateKey {
	tb.Helper()
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{b}, ed25519.SeedSize))
}

func pub(key ed25519.PrivateKey) ed25519.PublicKey {
	return key.Public().(ed25519.PublicKey)
}

func samplePolicy() *Policy {
	return &Policy{
		OrgID:     "org-acme",
		Scope:     "fleet",
		Revision:  3,
		KeyEpoch:  "epoch-1",
		IssuedAt:  1000,
		NotBefore: 1000,
		Expiry:    2000,
		Payload: Payload{
			Judge: Judge{
				ModelTag:   "sage-memory-judge:v16",
				GGUFSHA256: strings.Repeat("ab", 32),
				BlobSHA256: strings.Repeat("cd", 32),
			},
			ChecksVersion: "sage-lasting/1+supported/1",
			PeerAllowList: []string{"chain-b", "chain-a"},
			Retention:     Retention{MaxAgeDays: 90, MaxItems: 10000},
		},
	}
}

// expectedCanonical is an INDEPENDENT re-implementation of the wire framing. If
// CanonicalBytes drifts from this layout the golden test fails — the same
// cross-implementation guard internal/federation documents for its journal.
func expectedCanonical(p *Policy) []byte {
	lp := func(b []byte, s string) []byte {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s)))
		return append(append(b, n[:]...), []byte(s)...)
	}
	u64 := func(b []byte, v uint64) []byte {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], v)
		return append(b, n[:]...)
	}
	b := []byte("sage-org-policy-v1\x00")
	b = lp(b, p.Schema)
	b = lp(b, p.OrgID)
	b = lp(b, p.Scope)
	b = u64(b, p.Revision)
	b = lp(b, p.IssuerKeyID)
	b = lp(b, p.KeyEpoch)
	b = u64(b, uint64(p.IssuedAt))
	b = u64(b, uint64(p.NotBefore))
	b = u64(b, uint64(p.Expiry))
	b = lp(b, p.Payload.Judge.ModelTag)
	b = lp(b, p.Payload.Judge.GGUFSHA256)
	b = lp(b, p.Payload.Judge.BlobSHA256)
	b = lp(b, p.Payload.ChecksVersion)
	peers := append([]string(nil), p.Payload.PeerAllowList...)
	sort.Strings(peers)
	var cnt [4]byte
	binary.BigEndian.PutUint32(cnt[:], uint32(len(peers)))
	b = append(b, cnt[:]...)
	for _, pr := range peers {
		b = lp(b, pr)
	}
	b = u64(b, uint64(p.Payload.Retention.MaxAgeDays))
	b = u64(b, uint64(p.Payload.Retention.MaxItems))
	return b
}

func TestCanonicalBytesLayout(t *testing.T) {
	p := samplePolicy()
	p.Schema = SchemaID
	p.IssuerKeyID = hex.EncodeToString(pub(fixedKey(t, 1)))
	if got, want := p.CanonicalBytes(), expectedCanonical(p); !bytes.Equal(got, want) {
		t.Fatalf("canonical bytes drift:\n got  %x\n want %x", got, want)
	}
}

func TestDigestIsSHA256OfCanonical(t *testing.T) {
	p := samplePolicy()
	p.Schema = SchemaID
	p.IssuerKeyID = hex.EncodeToString(pub(fixedKey(t, 1)))
	sum := sha256.Sum256(p.CanonicalBytes())
	if p.Digest() != hex.EncodeToString(sum[:]) {
		t.Fatal("Digest is not sha256(CanonicalBytes)")
	}
}

func TestSignVerifyRoundTrip(t *testing.T) {
	key := fixedKey(t, 1)
	p := samplePolicy()
	if err := p.Sign(key); err != nil {
		t.Fatalf("sign: %v", err)
	}
	if p.Schema != SchemaID || p.IssuerKeyID != hex.EncodeToString(pub(key)) {
		t.Fatal("Sign did not stamp schema/issuer")
	}
	if err := p.Verify(pub(key)); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyWrongPinnedKey(t *testing.T) {
	p := samplePolicy()
	if err := p.Sign(fixedKey(t, 1)); err != nil {
		t.Fatal(err)
	}
	if err := p.Verify(pub(fixedKey(t, 2))); err == nil {
		t.Fatal("verify must reject a policy signed by a non-pinned key")
	}
}

func TestTamperDetected(t *testing.T) {
	key := fixedKey(t, 1)
	mutations := map[string]func(*Policy){
		"org_id":         func(p *Policy) { p.OrgID += "x" },
		"scope":          func(p *Policy) { p.Scope += "x" },
		"revision":       func(p *Policy) { p.Revision++ },
		"key_epoch":      func(p *Policy) { p.KeyEpoch += "x" },
		"issued_at":      func(p *Policy) { p.IssuedAt++ },
		"not_before":     func(p *Policy) { p.NotBefore++ },
		"expiry":         func(p *Policy) { p.Expiry++ },
		"model_tag":      func(p *Policy) { p.Payload.Judge.ModelTag += "x" },
		"gguf":           func(p *Policy) { p.Payload.Judge.GGUFSHA256 = strings.Repeat("ef", 32) },
		"blob":           func(p *Policy) { p.Payload.Judge.BlobSHA256 = strings.Repeat("ef", 32) },
		"checks_version": func(p *Policy) { p.Payload.ChecksVersion += "x" },
		"peer":           func(p *Policy) { p.Payload.PeerAllowList = []string{"chain-a", "chain-c"} },
		"max_age":        func(p *Policy) { p.Payload.Retention.MaxAgeDays++ },
		"max_items":      func(p *Policy) { p.Payload.Retention.MaxItems++ },
	}
	for name, mut := range mutations {
		p := samplePolicy()
		if err := p.Sign(key); err != nil {
			t.Fatal(err)
		}
		mut(p)
		if err := p.Verify(pub(key)); err == nil {
			t.Fatalf("tamper on %q not detected", name)
		}
	}
}

func TestPeerAllowListOrderIndependent(t *testing.T) {
	key := fixedKey(t, 1)
	p1 := samplePolicy()
	p1.Payload.PeerAllowList = []string{"chain-a", "chain-b"}
	if err := p1.Sign(key); err != nil {
		t.Fatal(err)
	}
	p2 := samplePolicy()
	p2.Payload.PeerAllowList = []string{"chain-b", "chain-a"}
	p2.Schema = p1.Schema
	p2.IssuerKeyID = p1.IssuerKeyID
	p2.Sig = p1.Sig // the p1 signature must verify p2: same canonical content
	if err := p2.Verify(pub(key)); err != nil {
		t.Fatalf("reordered peer list changed the signature: %v", err)
	}
	if p1.Digest() != p2.Digest() {
		t.Fatal("digest depends on peer order")
	}
}

func TestValidateRejections(t *testing.T) {
	key := fixedKey(t, 1)
	cases := map[string]func(*Policy){
		"bad gguf len":    func(p *Policy) { p.Payload.Judge.GGUFSHA256 = "abcd" },
		"non-hex gguf":    func(p *Policy) { p.Payload.Judge.GGUFSHA256 = strings.Repeat("zz", 32) },
		"empty checks":    func(p *Policy) { p.Payload.ChecksVersion = "" },
		"empty model":     func(p *Policy) { p.Payload.Judge.ModelTag = "" },
		"zero revision":   func(p *Policy) { p.Revision = 0 },
		"empty epoch":     func(p *Policy) { p.KeyEpoch = "" },
		"dup peer":        func(p *Policy) { p.Payload.PeerAllowList = []string{"x", "x"} },
		"expiry<nb":       func(p *Policy) { p.NotBefore = 5000; p.Expiry = 1 },
		"neg retention":   func(p *Policy) { p.Payload.Retention.MaxItems = -1 },
		"empty peer item": func(p *Policy) { p.Payload.PeerAllowList = []string{""} },
	}
	for name, mut := range cases {
		p := samplePolicy()
		mut(p)
		if err := p.Sign(key); err == nil {
			t.Fatalf("%q: Sign accepted an invalid policy", name)
		}
	}
}

func TestOversizePeerListRejected(t *testing.T) {
	p := samplePolicy()
	p.Payload.PeerAllowList = make([]string, maxPeerAllowList+1)
	for i := range p.Payload.PeerAllowList {
		p.Payload.PeerAllowList[i] = hex.EncodeToString([]byte{byte(i), byte(i >> 8)})
	}
	if err := p.Sign(fixedKey(t, 1)); err == nil {
		t.Fatal("oversize peer list accepted")
	}
}
