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
	"strings"
)

// originSigMessage builds the exact bytes an origin agent signs. Fields are
// length-prefixed (injective: no two distinct tuples collide) and domain-
// separated. content_hash is lowercased hex; classification is a fixed 4-byte
// big-endian int; origin_created_at is the verbatim wire string. The receiver
// rebuilds this from the received item, so every field bound here is one the
// receiver already trusts end-to-end (content_hash is re-derived from content in
// validateSyncItem; classification is the value the ceiling is enforced against).
func originSigMessage(originChainID, originMemoryID, contentHash, domain string, classification int, originCreatedAt string) []byte {
	b := make([]byte, 0, 64+len(originChainID)+len(originMemoryID)+len(contentHash)+len(domain)+len(originCreatedAt))
	b = append(b, []byte("sage-sync-origin-v1\x00")...)
	writePart := func(s string) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s))) // #nosec G115 -- lengths bounded by SyncItem caps
		b = append(b, n[:]...)
		b = append(b, []byte(s)...)
	}
	writePart(originChainID)
	writePart(originMemoryID)
	writePart(strings.ToLower(contentHash))
	writePart(domain)
	var cls [4]byte
	binary.BigEndian.PutUint32(cls[:], uint32(classification)) // #nosec G115 -- validated 0-4
	b = append(b, cls[:]...)
	writePart(originCreatedAt)
	return b
}

// signOriginSig produces the origin signature over an item, using the origin
// operator agent key. Called by the sender (which is the origin) when building a
// SyncItem.
func signOriginSig(key ed25519.PrivateKey, item *SyncItem) []byte {
	return ed25519.Sign(key, originSigMessage(item.OriginChainID, item.OriginMemoryID,
		item.ContentHash, item.Domain, item.Classification, item.OriginCreatedAt))
}

// verifyOriginSig checks item.OriginSig against the origin agent's public key.
// Returns false on any size mismatch or verification failure (fail closed).
func verifyOriginSig(originPub ed25519.PublicKey, item *SyncItem) bool {
	if len(originPub) != ed25519.PublicKeySize || len(item.OriginSig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(originPub, originSigMessage(item.OriginChainID, item.OriginMemoryID,
		item.ContentHash, item.Domain, item.Classification, item.OriginCreatedAt), item.OriginSig)
}
