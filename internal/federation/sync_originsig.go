package federation

// Origin-authenticated provenance for domain sync (v11.8, docs/v11.8-PLAN.md §4.4).
//
// A SyncItem's origin_sig is the ORIGIN agent's ed25519 signature over an
// injective, domain-separated encoding of the item's provenance + content hash +
// classification. Its purpose is mesh backfill: in a multi-node group a member M
// may serve another origin's history to a joining member. Without an origin
// signature a relaying member could forge false-origin content, silently
// re-classify it, and win first-write-wins on the receiver. With it, a relayer is
// a pure cache: it can delay or withhold, but every item it serves must carry a
// signature the receiver verifies against the ORIGIN's key, so forgery and
// mis-attribution are impossible.
//
// In the pairwise star (the only sender model before groups) the origin IS the
// authenticated pushing peer, so the receiver verifies against the peer's
// authenticated agent key. Mesh backfill resolves the origin's key from the group
// roster (sync_group_member.member_agent_pubkey) — build step 6.

import (
	"crypto/ed25519"
	"encoding/binary"
	"math"
	"sort"
	"strings"
)

// originSigMessage builds the exact bytes an origin agent signs. Fields are
// length-prefixed (injective: no two distinct tuples collide) and domain-
// separated. content_hash is lowercased hex; classification is a fixed 4-byte
// big-endian int; confidence_score is the IEEE-754 bit pattern (deterministic);
// tags are canonicalized (sorted + deduped) then count-prefixed; origin_created_at
// and memory_type are verbatim wire strings.
//
// The envelope binds EVERY SyncItem field that persists on the admitted copy —
// content (via content_hash), domain, classification, memory_type, confidence,
// and tags — so a v11.8 group mesh-backfill relayer cannot mutate ANY of them and
// still present a valid signature. The receiver rebuilds this from the received
// item, and each bound field is one it already trusts end-to-end (content_hash is
// re-derived from content in validateSyncItem; tags are canonicalized there).
func originSigMessage(item *SyncItem) []byte {
	b := make([]byte, 0, 96+len(item.OriginChainID)+len(item.OriginMemoryID)+len(item.ContentHash)+len(item.Domain)+len(item.OriginCreatedAt))
	b = append(b, []byte("sage-sync-origin-v1\x00")...)
	writePart := func(s string) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s))) // #nosec G115 -- lengths bounded by SyncItem caps
		b = append(b, n[:]...)
		b = append(b, []byte(s)...)
	}
	writePart(item.OriginChainID)
	writePart(item.OriginMemoryID)
	writePart(strings.ToLower(item.ContentHash))
	writePart(item.Domain)
	var cls [4]byte
	binary.BigEndian.PutUint32(cls[:], uint32(item.Classification)) // #nosec G115 -- validated 0-4
	b = append(b, cls[:]...)
	writePart(item.OriginCreatedAt)
	writePart(item.MemoryType)
	var conf [8]byte
	binary.BigEndian.PutUint64(conf[:], math.Float64bits(item.ConfidenceScore))
	b = append(b, conf[:]...)
	// Tags: canonicalized (sort + dedupe) so sender and receiver — which
	// independently order/dedupe tags — derive identical bytes; count-prefixed
	// so the tag boundary can't be confused with the trailing fields.
	tags := append([]string(nil), item.Tags...)
	sort.Strings(tags)
	canon := tags[:0]
	for i, t := range tags {
		if i == 0 || tags[i-1] != t {
			canon = append(canon, t)
		}
	}
	var tc [4]byte
	binary.BigEndian.PutUint32(tc[:], uint32(len(canon))) // #nosec G115 -- bounded by SyncMaxTags
	b = append(b, tc[:]...)
	for _, t := range canon {
		writePart(t)
	}
	return b
}

// signOriginSig produces the origin signature over an item, using the origin
// operator agent key. Called by the sender (which is the origin) when building a
// SyncItem.
func signOriginSig(key ed25519.PrivateKey, item *SyncItem) []byte {
	return ed25519.Sign(key, originSigMessage(item))
}

// verifyOriginSig checks item.OriginSig against the origin agent's public key.
// Returns false on any size mismatch or verification failure (fail closed).
func verifyOriginSig(originPub ed25519.PublicKey, item *SyncItem) bool {
	if len(originPub) != ed25519.PublicKeySize || len(item.OriginSig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(originPub, originSigMessage(item), item.OriginSig)
}
