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
		MemoryType:      "observation",
		ConfidenceScore: 0.8,
		Content:         "patch notes",
		ContentHash:     "ABCDEF0123", // case-insensitive; signer lowercases
		Tags:            []string{"oscillator", "eurorack"},
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
	// The widened envelope also binds memory_type, confidence, and tags — a
	// step-6 relayer must not be able to mutate any of them (docs §4.4).
	mutate("memory_type", func(i *SyncItem) { i.MemoryType = "fact" })
	mutate("confidence_score", func(i *SyncItem) { i.ConfidenceScore = 0.1 })
	mutate("tags-add", func(i *SyncItem) { i.Tags = []string{"oscillator", "eurorack", "extra"} })
	mutate("tags-drop", func(i *SyncItem) { i.Tags = []string{"eurorack"} })

	// Tag ORDER is not material (canonicalized): re-ordering still verifies.
	reordered := *base
	reordered.Tags = []string{"eurorack", "oscillator"}
	if !verifyOriginSig(pub, &reordered) {
		t.Fatalf("tag reorder should still verify (canonicalized)")
	}

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
	a := originSigMessage(&SyncItem{OriginChainID: "ab", OriginMemoryID: "c", ContentHash: "00", Domain: "d"})
	b := originSigMessage(&SyncItem{OriginChainID: "a", OriginMemoryID: "bc", ContentHash: "00", Domain: "d"})
	if string(a) == string(b) {
		t.Fatalf("length-prefix boundary collision")
	}
}
