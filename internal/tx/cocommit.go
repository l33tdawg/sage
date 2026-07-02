package tx

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

// Canonical, deterministic derivations for the co-commit (Mode 2) primitive.
// Every function here is a PURE function of its inputs — no clock, no map
// iteration, no per-node state — so two independent chains derive byte-identical
// values from the same jointly-signed envelope. This is what lets a co-committed
// memory be committed natively to each participant's own chain and cross-anchored.

// SortCoauthorsByPubKey returns a copy of cs sorted by PubKey (byte-wise) — the
// canonical order for CanonicalCoreBytes, ComputeSharedID, and on-chain storage,
// so every chain derives byte-identical values regardless of input order.
func SortCoauthorsByPubKey(cs []CoCommitCoauthor) []CoCommitCoauthor {
	out := make([]CoCommitCoauthor, len(cs))
	copy(out, cs)
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].PubKey, out[j].PubKey) < 0 })
	return out
}

// CanonicalCoreBytes is the byte-identical message every coauthor signs. It
// covers the SHARED CORE only — schema, content, memory type, domain,
// classification band, confidence, authored time, agreement nonce, and each
// coauthor's (PubKey, ChainID) in sorted order — and deliberately EXCLUDES the
// derived SharedID (circular), the per-signer Sig (a signature cannot cover
// itself), and any local overlay (status/height/local memid differ per chain by
// construction). Field order and widths mirror encodeCoCommitSubmit so the bytes
// are reproducible cross-chain and cross-language.
func CanonicalCoreBytes(s *CoCommitSubmit) []byte {
	var buf []byte
	buf = appendUint32(buf, s.SchemaVersion)
	buf = appendBytes(buf, s.ContentHash)
	buf = append(buf, byte(s.MemoryType))
	buf = appendBytes(buf, []byte(s.Domain))
	buf = append(buf, byte(s.Classification))
	buf = appendFloat64(buf, s.ConfidenceScore)
	buf = appendInt64(buf, s.CreatedAtUnix)
	buf = appendBytes(buf, s.AgreementNonce)
	for _, c := range SortCoauthorsByPubKey(s.Coauthors) {
		buf = appendBytes(buf, c.PubKey)
		buf = appendBytes(buf, []byte(c.ChainID))
		// Sig deliberately excluded — the signed bytes cannot include the signature.
	}
	// Validity window is part of the signed core (appended after the coauthor
	// loop, mirroring its trailing-optional position in encodeCoCommitSubmit) so
	// every coauthor commits to the envelope's freshness bounds (footgun E).
	buf = appendInt64(buf, s.NotBefore)
	buf = appendInt64(buf, s.NotAfter)
	return buf
}

// CoreHashOf = sha256(CanonicalCoreBytes(s)).
func CoreHashOf(s *CoCommitSubmit) []byte {
	h := sha256.Sum256(CanonicalCoreBytes(s))
	return h[:]
}

// ComputeSharedID = hex(sha256(coreHash ‖ sortedCoauthorPubKeys ‖ nonce)).
// Content-derived and HEIGHT-FREE, so every party computes it from the envelope
// alone, before anyone commits — dissolving the ordering problem.
func ComputeSharedID(coreHash []byte, coauthors []CoCommitCoauthor, nonce []byte) string {
	h := sha256.New()
	h.Write(coreHash)
	for _, c := range SortCoauthorsByPubKey(coauthors) {
		h.Write(c.PubKey)
	}
	h.Write(nonce)
	return hex.EncodeToString(h.Sum(nil))
}

// EncodeCoauthorsCanonical returns the deterministic on-chain blob for a coauthor
// set (sorted by PubKey), stored under cocommit:coauthors:<SharedID>.
func EncodeCoauthorsCanonical(cs []CoCommitCoauthor) []byte {
	return appendCoauthors(nil, SortCoauthorsByPubKey(cs))
}

// DecodeCoauthorsCanonical parses an EncodeCoauthorsCanonical blob (as stored
// under cocommit:coauthors:<SharedID>) back into the coauthor set — used by the
// attest peer-identity bind and the co-commit self-corroboration guard.
func DecodeCoauthorsCanonical(blob []byte) ([]CoCommitCoauthor, error) {
	cs, _, err := readCoauthors(blob, 0)
	if err != nil {
		return nil, err
	}
	return cs, nil
}

// EncodeCommitReceipt returns the deterministic canonical bytes of a
// CommitReceipt SANS ValSig — the bytes the emitting chain's validator signs and
// the exact bytes a peer wraps verbatim into a CoCommitAttest (footgun T: the
// receipt enters the peer chain only as these verbatim signed bytes).
func EncodeCommitReceipt(r *CommitReceipt) []byte {
	var buf []byte
	buf = appendBytes(buf, []byte(r.ChainID))
	buf = appendBytes(buf, []byte(r.SharedID))
	buf = appendBytes(buf, []byte(r.LocalMemID))
	buf = appendInt64(buf, r.Height)
	buf = appendInt64(buf, r.CommitTime)
	buf = appendBytes(buf, r.CoreHash)
	return buf
}

// DecodeCommitReceipt parses EncodeCommitReceipt bytes (sans ValSig). Used by the
// attest handler to bind the SIGNED receipt's SharedID/CoreHash/ChainID rather
// than trusting the unsigned convenience fields on the attest tx.
func DecodeCommitReceipt(data []byte) (*CommitReceipt, error) {
	r := &CommitReceipt{}
	var b []byte
	var err error
	off := 0

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	r.ChainID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	r.SharedID = string(b)

	b, off, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}
	r.LocalMemID = string(b)

	r.Height, off, err = readInt64(data, off)
	if err != nil {
		return nil, err
	}

	r.CommitTime, off, err = readInt64(data, off)
	if err != nil {
		return nil, err
	}

	r.CoreHash, _, err = readBytes(data, off)
	if err != nil {
		return nil, err
	}

	return r, nil
}
