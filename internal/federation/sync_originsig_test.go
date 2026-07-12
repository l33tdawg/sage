package federation

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

func testSyncItem() *SyncItem {
	return &SyncItem{
		OriginChainID:   "sage-origin-abc",
		OriginMemoryID:  "mem-42",
		OriginCreatedAt: "2026-07-12T10:00:00Z",
		Domain:          "eurorack",
		Classification:  2,
		Content:         "patch notes",
		ContentHash:     "ABCDEF0123", // case-insensitive; signer lowercases
	}
}

func TestOriginSigRoundTrip(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	item := testSyncItem()
	item.OriginSig = signOriginSig(priv, item)

	if !verifyOriginSig(pub, item) {
		t.Fatalf("valid origin sig did not verify")
	}

	// content_hash is compared case-insensitively (signer lowercases): a peer
	// that re-cases the hex must still verify.
	reCased := *item
	reCased.ContentHash = "abcdef0123"
	if !verifyOriginSig(pub, &reCased) {
		t.Fatalf("re-cased content hash should still verify")
	}
}

func TestOriginSigTamperRejected(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	base := testSyncItem()
	base.OriginSig = signOriginSig(priv, base)

	mutate := func(name string, f func(*SyncItem)) {
		it := *base
		f(&it)
		if verifyOriginSig(pub, &it) {
			t.Fatalf("tampered %s verified but must not", name)
		}
	}
	mutate("classification", func(i *SyncItem) { i.Classification = 3 })
	mutate("domain", func(i *SyncItem) { i.Domain = "other" })
	mutate("content_hash", func(i *SyncItem) { i.ContentHash = "deadbeef" })
	mutate("origin_memory_id", func(i *SyncItem) { i.OriginMemoryID = "mem-43" })
	mutate("origin_chain_id", func(i *SyncItem) { i.OriginChainID = "sage-evil" })
	mutate("origin_created_at", func(i *SyncItem) { i.OriginCreatedAt = "2026-07-12T10:00:01Z" })

	// Wrong key must not verify.
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	if verifyOriginSig(otherPub, base) {
		t.Fatalf("wrong key verified")
	}

	// A relayer stripping the signature is caught by size mismatch (fail closed).
	stripped := *base
	stripped.OriginSig = nil
	if verifyOriginSig(pub, &stripped) {
		t.Fatalf("empty sig verified")
	}
	short := *base
	short.OriginSig = []byte{1, 2, 3}
	if verifyOriginSig(pub, &short) {
		t.Fatalf("short sig verified")
	}
}

// A field-boundary confusion (moving a byte between adjacent fields) must not
// collide, because the encoding is length-prefixed / injective.
func TestOriginSigInjective(t *testing.T) {
	a := originSigMessage("ab", "c", "00", "d", 0, "")
	b := originSigMessage("a", "bc", "00", "d", 0, "")
	if string(a) == string(b) {
		t.Fatalf("length-prefix boundary collision")
	}
}
