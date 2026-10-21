// Package orgpolicy implements the signed, versioned org-policy object
// (schema "sage.org-policy.v1") that an organization's management plane
// distributes to its enrolled SAGE nodes.
//
// INVARIANT (control plane != data plane). A policy signs ADMISSION, RETENTION
// and TOPOLOGY configuration only — which judge model+checks everyone pins, who
// is ELIGIBLE to peer, how long memories are retained. It never carries, and can
// never be, a brain read/write capability: a fully compromised org signing key
// can re-point config or enroll a rogue node (bounded, logged, revocable) but
// cannot read or forge a single memory, because brain access stays authorized by
// the node's own owner key. Nothing in this package touches memory content.
//
// This package is deliberately PURE (crypto/ed25519 + stdlib only, no store, no
// HTTP): the signing/ordering/rotation/equivocation contract is fully unit
// testable in isolation. IO — persisting the accepted chain, raising the
// operator notice on equivocation — lives in the caller.
//
// The canonical encoding mirrors internal/federation/sync_journal.go's injective
// framing (4-byte big-endian length prefixes, sorted-key sets, an ASCII domain
// tag terminated by \x00) field-for-field. It is re-implemented here rather than
// imported because those helpers are unexported in package federation; the
// golden-vector test (policy_test.go) pins the exact byte layout so the two
// encoders cannot silently drift.
package orgpolicy

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"unicode/utf8"
)

const (
	// SchemaID is the wire/semantic schema identifier bound into every signature.
	SchemaID = "sage.org-policy.v1"

	// domainSep domain-separates org-policy signatures from every other ed25519
	// signature in the system (journal entries, federation requests, origin sigs,
	// rotation attestations). Format matches internal/federation: ASCII label + NUL.
	domainSep = "sage-org-policy-v1\x00"

	// maxCanonicalBytes bounds one policy's canonical form, enforced fail-closed at
	// Sign and Verify so an over-size object can neither be authored nor admitted.
	maxCanonicalBytes = 16 * 1024

	// maxPeerAllowList bounds the eligibility set so a single policy cannot be used
	// to blow the canonical-size budget via an unbounded list.
	maxPeerAllowList = 1024
)

// Judge pins the admission gate: the local memory-judge model tag plus the two
// SHA-256 digests the pinned-SHA artifact fetch (internal/ollamad/judge_model.go)
// verifies — the downloaded GGUF and the resulting Ollama blob.
type Judge struct {
	ModelTag   string `json:"model_tag"`
	GGUFSHA256 string `json:"gguf_sha256"`
	BlobSHA256 string `json:"blob_sha256"`
}

// Retention is the (non-destructive-by-default) retention configuration. A
// destructive purge driven by these bounds is gated separately on owner presence
// when the governing policy is expired (see the offline/expiry contract).
type Retention struct {
	MaxAgeDays int64 `json:"max_age_days"`
	MaxItems   int64 `json:"max_items"`
}

// Payload is admission/retention/topology ONLY (the control-plane invariant).
// PeerAllowList is ELIGIBILITY to peer; it does not itself activate sharing —
// activation stays an owner-authorized act.
type Payload struct {
	Judge         Judge     `json:"judge"`
	ChecksVersion string    `json:"checks_version"`
	PeerAllowList []string  `json:"peer_allow_list"`
	Retention     Retention `json:"retention"`
}

// Policy is the signed org-policy object. Sig is an ed25519 signature by the org
// signing key over CanonicalBytes(); it is NOT part of the canonical bytes.
type Policy struct {
	Schema      string  `json:"schema"`
	OrgID       string  `json:"org_id"`
	Scope       string  `json:"scope"`
	Revision    uint64  `json:"revision"`      // 1-based, monotonic; a node refuses revision < current
	IssuerKeyID string  `json:"issuer_key_id"` // hex ed25519 public key of the signer
	KeyEpoch    string  `json:"key_epoch"`     // orders rotations; authorization is by attestation, not by this value
	IssuedAt    int64   `json:"issued_at"`     // unix seconds
	NotBefore   int64   `json:"not_before"`    // unix seconds
	Expiry      int64   `json:"expiry"`        // unix seconds; 0 == no expiry
	Payload     Payload `json:"payload"`
	Sig         string  `json:"sig"` // hex ed25519 over CanonicalBytes()
}

// lp appends a 4-byte big-endian length prefix followed by s — the injective
// framing shared with internal/federation/sync_journal.go:lpAppend.
func lp(b []byte, s string) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(s))) // #nosec G115 -- lengths bounded by maxCanonicalBytes
	b = append(b, n[:]...)
	return append(b, []byte(s)...)
}

func u32(b []byte, v uint32) []byte {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], v)
	return append(b, n[:]...)
}

func u64(b []byte, v uint64) []byte {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], v)
	return append(b, n[:]...)
}

func i64(b []byte, v int64) []byte { return u64(b, uint64(v)) } // #nosec G115 -- two's-complement round-trips

// CanonicalBytes builds the exact bytes hashed AND signed for a policy. Every
// field an issuer attests is bound; Sig is excluded (it signs these bytes). The
// peer allow-list is sorted so the encoding is order-independent — validate()
// rejects duplicates, keeping the sorted encoding injective.
func (p *Policy) CanonicalBytes() []byte {
	b := make([]byte, 0, 256)
	b = append(b, []byte(domainSep)...)
	b = lp(b, p.Schema)
	b = lp(b, p.OrgID)
	b = lp(b, p.Scope)
	b = u64(b, p.Revision)
	b = lp(b, p.IssuerKeyID)
	b = lp(b, p.KeyEpoch)
	b = i64(b, p.IssuedAt)
	b = i64(b, p.NotBefore)
	b = i64(b, p.Expiry)
	b = lp(b, p.Payload.Judge.ModelTag)
	b = lp(b, p.Payload.Judge.GGUFSHA256)
	b = lp(b, p.Payload.Judge.BlobSHA256)
	b = lp(b, p.Payload.ChecksVersion)
	peers := append([]string(nil), p.Payload.PeerAllowList...)
	sort.Strings(peers)
	b = u32(b, uint32(len(peers))) // #nosec G115 -- bounded by maxPeerAllowList
	for _, pr := range peers {
		b = lp(b, pr)
	}
	b = i64(b, p.Payload.Retention.MaxAgeDays)
	b = i64(b, p.Payload.Retention.MaxItems)
	return b
}

// Digest is the hex SHA-256 of the canonical bytes — the stable content identity
// used for same-revision equivocation detection.
func (p *Policy) Digest() string {
	sum := sha256.Sum256(p.CanonicalBytes())
	return hex.EncodeToString(sum[:])
}

// validate enforces the structural contract independent of the signature, so an
// ill-formed object is rejected before (and after) any crypto. It does NOT check
// Sig.
func (p *Policy) validate() error {
	if p.Schema != SchemaID {
		return fmt.Errorf("org policy: schema %q != %q", p.Schema, SchemaID)
	}
	for name, s := range map[string]string{
		"org_id": p.OrgID, "scope": p.Scope, "issuer_key_id": p.IssuerKeyID,
		"key_epoch": p.KeyEpoch, "checks_version": p.Payload.ChecksVersion,
		"judge.model_tag": p.Payload.Judge.ModelTag,
	} {
		if s == "" {
			return fmt.Errorf("org policy: %s is required", name)
		}
		if !utf8.ValidString(s) {
			return fmt.Errorf("org policy: %s must be valid UTF-8", name)
		}
	}
	if p.Revision == 0 {
		return fmt.Errorf("org policy: revision must be >= 1")
	}
	if _, err := hex.DecodeString(p.IssuerKeyID); err != nil || len(p.IssuerKeyID) != 2*ed25519.PublicKeySize {
		return fmt.Errorf("org policy: issuer_key_id must be a hex ed25519 public key")
	}
	if err := validateSHA256Hex("judge.gguf_sha256", p.Payload.Judge.GGUFSHA256); err != nil {
		return err
	}
	if err := validateSHA256Hex("judge.blob_sha256", p.Payload.Judge.BlobSHA256); err != nil {
		return err
	}
	if p.Expiry != 0 && p.NotBefore != 0 && p.Expiry < p.NotBefore {
		return fmt.Errorf("org policy: expiry precedes not_before")
	}
	if p.Payload.Retention.MaxAgeDays < 0 || p.Payload.Retention.MaxItems < 0 {
		return fmt.Errorf("org policy: retention bounds must be non-negative")
	}
	if len(p.Payload.PeerAllowList) > maxPeerAllowList {
		return fmt.Errorf("org policy: peer_allow_list %d exceeds cap %d", len(p.Payload.PeerAllowList), maxPeerAllowList)
	}
	seen := make(map[string]struct{}, len(p.Payload.PeerAllowList))
	for _, pr := range p.Payload.PeerAllowList {
		if pr == "" || !utf8.ValidString(pr) {
			return fmt.Errorf("org policy: peer_allow_list entries must be non-empty UTF-8")
		}
		if _, dup := seen[pr]; dup {
			return fmt.Errorf("org policy: peer_allow_list contains duplicate %q", pr)
		}
		seen[pr] = struct{}{}
	}
	return nil
}

func validateSHA256Hex(name, s string) error {
	// A judge pin digest is mandatory and must be a 32-byte hex SHA-256.
	if len(s) != 2*sha256.Size {
		return fmt.Errorf("org policy: %s must be a 64-char hex sha256", name)
	}
	if _, err := hex.DecodeString(s); err != nil {
		return fmt.Errorf("org policy: %s must be hex: %w", name, err)
	}
	return nil
}

// Sign sets Schema/IssuerKeyID from the signing key, validates, and signs the
// canonical bytes. KeyEpoch must already be set by the caller (it is bound into
// the signature and orders rotations). Returns an error rather than producing an
// unverifiable object.
func (p *Policy) Sign(key ed25519.PrivateKey) error {
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("org policy: invalid signing key")
	}
	p.Schema = SchemaID
	p.IssuerKeyID = hex.EncodeToString(pub)
	if err := p.validate(); err != nil {
		return err
	}
	cb := p.CanonicalBytes()
	if len(cb) > maxCanonicalBytes {
		return fmt.Errorf("org policy: canonical form %d bytes exceeds cap %d", len(cb), maxCanonicalBytes)
	}
	p.Sig = hex.EncodeToString(ed25519.Sign(key, cb))
	return nil
}

// Verify checks a policy against the pinned current org signing key, fail-closed:
// the stored issuer key must equal the pinned key (a valid signature from the
// WRONG key is caught), key_epoch must be non-empty, the structure must validate,
// the canonical form must be within bounds, and the signature must verify. A
// policy signed by a ROTATED key is verified via ResolveSigner (rotation.go),
// not here — this function pins exactly one key.
func (p *Policy) Verify(pinnedPub ed25519.PublicKey) error {
	if len(pinnedPub) != ed25519.PublicKeySize {
		return fmt.Errorf("org policy: invalid pinned key")
	}
	if p.IssuerKeyID != hex.EncodeToString(pinnedPub) {
		return fmt.Errorf("org policy: issuer key does not match the pinned org key")
	}
	if err := p.validate(); err != nil {
		return err
	}
	cb := p.CanonicalBytes()
	if len(cb) > maxCanonicalBytes {
		return fmt.Errorf("org policy: canonical form %d bytes exceeds cap %d", len(cb), maxCanonicalBytes)
	}
	sig, err := hex.DecodeString(p.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(pinnedPub, cb, sig) {
		return fmt.Errorf("org policy: signature invalid")
	}
	return nil
}
