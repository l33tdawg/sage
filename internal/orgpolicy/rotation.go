package orgpolicy

import (
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// RotationSchemaID identifies a signer-rotation attestation.
const RotationSchemaID = "sage.org-policy-rotation.v1"

// rotationDomainSep domain-separates rotation attestations from policy objects
// and every other signature. ASCII label + NUL, matching the house convention.
const rotationDomainSep = "sage-org-policy-rotation-v1\x00"

const maxRotationChain = 32

// RotationAttestation authorizes a replacement org signing key. It is signed by
// the CURRENT (From) key — so authority to appoint a successor flows only from
// the key that already holds it. A larger key_epoch ALONE authorizes nothing: a
// node advances to ToKey only by verifying this attestation chained from the key
// it has pinned. key_epoch ORDERS rotations; this signed chain AUTHORIZES them.
type RotationAttestation struct {
	Schema       string `json:"schema"`
	OrgID        string `json:"org_id"`
	FromKeyID    string `json:"from_key_id"`   // hex ed25519 pub of the signer (binds + verifies this attestation)
	ToKeyID      string `json:"to_key_id"`     // hex ed25519 pub of the successor
	NewKeyEpoch  string `json:"new_key_epoch"` // epoch token the successor's policies carry
	NotBefore    int64  `json:"not_before"`    // unix seconds; successor authorized from here
	OverlapUntil int64  `json:"overlap_until"` // unix seconds; From key honored until here (0 == no overlap)
	Sig          string `json:"sig"`           // hex ed25519 by the From key over canonicalBytes()
}

// AuthorizedSigner is one key/epoch pair valid for an org at a specific time.
// KeyEpoch may be empty only for an enrollment token that deliberately leaves
// the first epoch unpinned.
type AuthorizedSigner struct {
	PublicKey ed25519.PublicKey
	KeyEpoch  string
}

func (a *RotationAttestation) canonicalBytes() []byte {
	b := make([]byte, 0, 128)
	b = append(b, []byte(rotationDomainSep)...)
	b = lp(b, a.Schema)
	b = lp(b, a.OrgID)
	b = lp(b, a.FromKeyID)
	b = lp(b, a.ToKeyID)
	b = lp(b, a.NewKeyEpoch)
	var n8 [8]byte
	binary.BigEndian.PutUint64(n8[:], uint64(a.NotBefore)) // #nosec G115
	b = append(b, n8[:]...)
	binary.BigEndian.PutUint64(n8[:], uint64(a.OverlapUntil)) // #nosec G115
	return append(b, n8[:]...)
}

func (a *RotationAttestation) validate() error {
	if a.Schema != RotationSchemaID {
		return fmt.Errorf("rotation: schema %q != %q", a.Schema, RotationSchemaID)
	}
	if a.OrgID == "" {
		return fmt.Errorf("rotation: org_id required")
	}
	for name, h := range map[string]string{"from_key_id": a.FromKeyID, "to_key_id": a.ToKeyID} {
		if _, err := hex.DecodeString(h); err != nil || len(h) != 2*ed25519.PublicKeySize {
			return fmt.Errorf("rotation: %s must be a hex ed25519 public key", name)
		}
	}
	if a.FromKeyID == a.ToKeyID {
		return fmt.Errorf("rotation: from_key_id and to_key_id are identical")
	}
	if a.NewKeyEpoch == "" {
		return fmt.Errorf("rotation: new_key_epoch required")
	}
	if a.OverlapUntil != 0 && a.OverlapUntil < a.NotBefore {
		return fmt.Errorf("rotation: overlap_until precedes not_before")
	}
	return nil
}

// Sign sets Schema and FromKeyID from the signing key, validates, and signs.
func (a *RotationAttestation) Sign(fromKey ed25519.PrivateKey) error {
	pub, ok := fromKey.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("rotation: invalid signing key")
	}
	a.Schema = RotationSchemaID
	a.FromKeyID = hex.EncodeToString(pub)
	if err := a.validate(); err != nil {
		return err
	}
	a.Sig = hex.EncodeToString(ed25519.Sign(fromKey, a.canonicalBytes()))
	return nil
}

// Verify checks the attestation is validly signed by its own FromKeyID,
// fail-closed. It does NOT establish that FromKeyID is trusted — ResolveSigner /
// AuthorizedKeys do that by chaining from the pinned key.
func (a *RotationAttestation) Verify() error {
	if err := a.validate(); err != nil {
		return err
	}
	fromPub, err := hex.DecodeString(a.FromKeyID)
	if err != nil {
		return fmt.Errorf("rotation: from_key_id not hex")
	}
	sig, err := hex.DecodeString(a.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(ed25519.PublicKey(fromPub), a.canonicalBytes(), sig) {
		return fmt.Errorf("rotation: signature invalid")
	}
	return nil
}

// chainStep verifies one attestation links from `cur` and returns the successor
// public key. A broken link (not signed by cur) is an error regardless of time.
func chainStep(cur ed25519.PublicKey, a RotationAttestation) (ed25519.PublicKey, error) {
	if err := a.Verify(); err != nil {
		return nil, err
	}
	if a.FromKeyID != hex.EncodeToString(cur) {
		return nil, fmt.Errorf("rotation: attestation not signed by the current authorized key")
	}
	toPub, err := hex.DecodeString(a.ToKeyID)
	if err != nil {
		return nil, fmt.Errorf("rotation: to_key_id not hex")
	}
	return ed25519.PublicKey(toPub), nil
}

// ResolveSigner folds a rotation chain from the pinned key and returns the final
// authorized signer. Every attestation must be validly signed by its predecessor
// (rooted at pinnedPub); otherwise it errors. Time/overlap are ignored here — use
// AuthorizedKeys for the time-aware set. With no attestations the pinned key is
// returned unchanged.
func ResolveSigner(pinnedPub ed25519.PublicKey, atts []RotationAttestation) (ed25519.PublicKey, error) {
	if len(pinnedPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("rotation: invalid pinned key")
	}
	cur := pinnedPub
	for i := range atts {
		next, err := chainStep(cur, atts[i])
		if err != nil {
			return nil, fmt.Errorf("rotation chain link %d: %w", i, err)
		}
		cur = next
	}
	return cur, nil
}

// AuthorizedKeys returns the set of org signing keys valid at `now`, honoring
// each attestation's NotBefore (the successor is not authorized before it) and
// OverlapUntil (the predecessor stays honored until it, covering in-flight
// policies during handover). The chain must be rooted at pinnedPub and fully
// linked. The returned slice always contains exactly one "current" key plus any
// predecessor still inside an open overlap window; a caller admits a policy under
// whichever listed key verifies it.
func AuthorizedKeys(pinnedPub ed25519.PublicKey, atts []RotationAttestation, now int64) ([]ed25519.PublicKey, error) {
	if len(pinnedPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("rotation: invalid pinned key")
	}
	cur := pinnedPub
	var overlaps []ed25519.PublicKey
	for i := range atts {
		next, err := chainStep(cur, atts[i])
		if err != nil {
			return nil, fmt.Errorf("rotation chain link %d: %w", i, err)
		}
		if now < atts[i].NotBefore {
			// This rotation has not taken effect yet; cur stays authoritative and
			// no later rotation can be active either (chain is ordered).
			return dedupeKeys(append(overlaps, cur)), nil
		}
		// Rotation active: predecessor stays valid only while inside its overlap window.
		if atts[i].OverlapUntil != 0 && now <= atts[i].OverlapUntil {
			overlaps = append(overlaps, cur)
		}
		cur = next
	}
	return dedupeKeys(append(overlaps, cur)), nil
}

// AuthorizedSigners is the time-aware, org-bound signer set used by enrollment
// and policy delivery. It validates every attestation's org and ordering, keeps
// a predecessor only during its explicit overlap, and never authorizes a
// successor before that successor's NotBefore.
func AuthorizedSigners(pinnedPub ed25519.PublicKey, orgID, currentEpoch string, atts []RotationAttestation, now int64) ([]AuthorizedSigner, error) {
	if len(pinnedPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("rotation: invalid pinned key")
	}
	if orgID == "" {
		return nil, fmt.Errorf("rotation: org_id required")
	}
	if len(atts) > maxRotationChain {
		return nil, fmt.Errorf("rotation: chain length %d exceeds cap %d", len(atts), maxRotationChain)
	}
	cur := AuthorizedSigner{PublicKey: append(ed25519.PublicKey(nil), pinnedPub...), KeyEpoch: currentEpoch}
	out := []AuthorizedSigner{cur}
	var lastNotBefore int64
	for i := range atts {
		att := &atts[i]
		if att.OrgID != orgID {
			return nil, fmt.Errorf("rotation chain link %d: org_id %q does not match %q", i, att.OrgID, orgID)
		}
		nextPub, err := chainStep(cur.PublicKey, *att)
		if err != nil {
			return nil, fmt.Errorf("rotation chain link %d: %w", i, err)
		}
		if i > 0 && att.NotBefore < lastNotBefore {
			return nil, fmt.Errorf("rotation chain link %d: not_before regresses", i)
		}
		lastNotBefore = att.NotBefore
		if now < att.NotBefore {
			return dedupeSigners(out), nil
		}
		if att.OverlapUntil != 0 && now <= att.OverlapUntil {
			out = append(out, cur)
		} else {
			out = removeSigner(out, cur)
		}
		if cur.KeyEpoch != "" && att.NewKeyEpoch == cur.KeyEpoch {
			return nil, fmt.Errorf("rotation chain link %d: new_key_epoch does not advance", i)
		}
		cur = AuthorizedSigner{
			PublicKey: nextPub,
			KeyEpoch:  att.NewKeyEpoch,
		}
		out = append(out, cur)
	}
	return dedupeSigners(out), nil
}

// AuthorizedSignerForPolicy requires both an authorized key and the matching key
// epoch. An empty authorized epoch acts as a wildcard only for the initial
// enrollment policy.
func AuthorizedSignerForPolicy(p *Policy, signers []AuthorizedSigner) (AuthorizedSigner, error) {
	var lastErr error
	for _, signer := range signers {
		if signer.KeyEpoch != "" && p.KeyEpoch != signer.KeyEpoch {
			continue
		}
		if err := p.Verify(signer.PublicKey); err == nil {
			return signer, nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return AuthorizedSigner{}, lastErr
	}
	return AuthorizedSigner{}, fmt.Errorf("org policy: issuer key/epoch is not currently authorized")
}

// VerifyPolicyUnderAuthorizedSigners is the verification-only form of
// AuthorizedSignerForPolicy.
func VerifyPolicyUnderAuthorizedSigners(p *Policy, signers []AuthorizedSigner) error {
	_, err := AuthorizedSignerForPolicy(p, signers)
	return err
}

func dedupeKeys(keys []ed25519.PublicKey) []ed25519.PublicKey {
	seen := make(map[string]struct{}, len(keys))
	out := make([]ed25519.PublicKey, 0, len(keys))
	for _, k := range keys {
		h := hex.EncodeToString(k)
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, k)
	}
	return out
}

func dedupeSigners(signers []AuthorizedSigner) []AuthorizedSigner {
	seen := make(map[string]struct{}, len(signers))
	out := make([]AuthorizedSigner, 0, len(signers))
	for _, signer := range signers {
		key := hex.EncodeToString(signer.PublicKey) + "\x00" + signer.KeyEpoch
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		signer.PublicKey = append(ed25519.PublicKey(nil), signer.PublicKey...)
		out = append(out, signer)
	}
	return out
}

func removeSigner(signers []AuthorizedSigner, remove AuthorizedSigner) []AuthorizedSigner {
	removeKey := hex.EncodeToString(remove.PublicKey) + "\x00" + remove.KeyEpoch
	out := signers[:0]
	for _, signer := range signers {
		key := hex.EncodeToString(signer.PublicKey) + "\x00" + signer.KeyEpoch
		if key != removeKey {
			out = append(out, signer)
		}
	}
	return out
}
