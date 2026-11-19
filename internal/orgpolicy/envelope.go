package orgpolicy

// Owner link envelope (RFC #443 Phase B, contract v2 §4).
//
// The envelope is the node owner's standing, signed answer to "which org links
// may this node be asked to accept, and with at most what sharing". It never
// activates anything by itself: Phase B only accepts mode "ask", so every
// introduced link still needs an explicit owner acceptance. It is per node and
// per org, signed by the node's CURRENT Root credential, revisioned and hash
// chained (prev_digest), and re-verified at every use against the Root that is
// current at that moment, so a Root rotation retires every envelope the old
// Root signed.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	// EnvelopeSchemaID is bound into every envelope signature.
	EnvelopeSchemaID = "sage.org-link-envelope.v1"

	envelopeDomainSep = "sage-org-link-envelope-v1\x00"

	// EnvelopeModeAsk is the only mode Phase B accepts: every introduced link
	// needs an explicit owner acceptance.
	EnvelopeModeAsk = "ask"

	// MaxEnvelopeWindowSeconds bounds expires_at - issued_at (365 days).
	MaxEnvelopeWindowSeconds = 365 * 24 * 60 * 60

	// MaxEnvelopeDomains and MaxEnvelopeDomainBytes match the tx-33 sharing
	// bounds (federation maxSharingDomains / maxSharingDomainLen), so an
	// envelope can express every sharing snapshot a link could carry and no
	// more.
	MaxEnvelopeDomains     = 64
	MaxEnvelopeDomainBytes = 512

	// MaxEnvelopeClearance is store.ClearanceTopSecret. orgpolicy does not
	// import internal/store; the federation cap check compares against the
	// agreement's own clearance.
	MaxEnvelopeClearance = 4

	// maxEnvelopeIDBytes bounds org_id and scope so the whole envelope stays
	// well inside its request cap.
	maxEnvelopeIDBytes = 256
)

// ErrEnvelopeInvalid wraps an envelope that fails structure, signature, Root
// binding, org binding or revision ordering. It is a client error.
var ErrEnvelopeInvalid = errors.New("link envelope: invalid")

// ErrEnvelopeStateConflict means the envelope record (or the accepted policy it
// is bound to) changed between read and write; reload and retry.
var ErrEnvelopeStateConflict = errors.New("link envelope: state changed concurrently")

// LinkEnvelope is the Root-signed owner envelope for one org on this node.
type LinkEnvelope struct {
	Schema string `json:"schema"`
	OrgID  string `json:"org_id"`
	// OrgRootKeyID pins the org root the envelope was written against: the
	// accepted policy's root_key_id. An org root change makes the envelope stop
	// covering anything until the owner re-signs it.
	OrgRootKeyID string `json:"org_root_key_id"`
	// Scope is the roster scope the envelope covers.
	Scope string `json:"scope"`
	// AllowedDomains caps the outbound sharing any introduced link of this org
	// may carry. Empty means trust-only links. "*" is valid only alone.
	AllowedDomains []string `json:"allowed_domains"`
	MaxClearance   uint8    `json:"max_clearance"`
	Mode           string   `json:"mode"`
	Revision       uint64   `json:"revision"` // monotonic per org on this node
	// PrevDigest is the digest of the envelope this revision replaces, or empty
	// for the first envelope of the org. It chains the envelope history.
	PrevDigest       string `json:"prev_digest"`
	IssuedAt         int64  `json:"issued_at"`
	ExpiresAt        int64  `json:"expires_at"`
	RootCredentialID string `json:"root_credential_id"` // hex ed25519 of the signing Root
	Sig              string `json:"sig"`
}

// CanonicalBytes is the exact signed encoding. Domains are sorted first, so a
// signature never depends on input order.
func (e *LinkEnvelope) CanonicalBytes() []byte {
	domains := append([]string(nil), e.AllowedDomains...)
	sort.Strings(domains)
	b := make([]byte, 0, 512)
	b = append(b, []byte(envelopeDomainSep)...)
	b = lp(b, e.Schema)
	b = lp(b, e.OrgID)
	b = lp(b, e.OrgRootKeyID)
	b = lp(b, e.Scope)
	b = u32(b, uint32(len(domains))) // #nosec G115 -- validate bounds the count
	for _, d := range domains {
		b = lp(b, d)
	}
	b = append(b, e.MaxClearance)
	b = lp(b, e.Mode)
	b = u64(b, e.Revision)
	b = lp(b, e.PrevDigest)
	b = i64(b, e.IssuedAt)
	b = i64(b, e.ExpiresAt)
	b = lp(b, e.RootCredentialID)
	return b
}

// Digest is the hex sha256 of the canonical bytes plus the signature.
func (e *LinkEnvelope) Digest() string {
	h := sha256.New()
	h.Write(e.CanonicalBytes())
	h.Write([]byte(e.Sig))
	return hex.EncodeToString(h.Sum(nil))
}

func (e *LinkEnvelope) validate() error {
	switch {
	case e.Schema != EnvelopeSchemaID:
		return fmt.Errorf("schema must be %q", EnvelopeSchemaID)
	case strings.TrimSpace(e.OrgID) == "" || len(e.OrgID) > maxEnvelopeIDBytes || !utf8.ValidString(e.OrgID):
		return fmt.Errorf("org_id is required (valid UTF-8, at most %d bytes)", maxEnvelopeIDBytes)
	case !isAgentPubHex(e.OrgRootKeyID):
		return errors.New("org_root_key_id must be 64 lowercase hex")
	case strings.TrimSpace(e.Scope) == "" || len(e.Scope) > maxEnvelopeIDBytes || !utf8.ValidString(e.Scope):
		return fmt.Errorf("scope is required (valid UTF-8, at most %d bytes)", maxEnvelopeIDBytes)
	case e.Mode != EnvelopeModeAsk:
		return fmt.Errorf("mode must be %q", EnvelopeModeAsk)
	case e.MaxClearance > MaxEnvelopeClearance:
		return fmt.Errorf("max_clearance must be at most %d", MaxEnvelopeClearance)
	case e.Revision == 0:
		return errors.New("revision must be at least 1")
	case e.Revision == 1 && e.PrevDigest != "":
		return errors.New("prev_digest must be empty at revision 1")
	case e.Revision > 1 && !isLowerHex(e.PrevDigest):
		return errors.New("prev_digest must be the 64 lowercase hex digest of the previous envelope")
	case e.IssuedAt <= 0 || e.ExpiresAt <= e.IssuedAt:
		return errors.New("expires_at must be after a positive issued_at")
	case e.ExpiresAt-e.IssuedAt > MaxEnvelopeWindowSeconds:
		return errors.New("envelope window exceeds 365 days")
	case !isAgentPubHex(e.RootCredentialID):
		return errors.New("root_credential_id must be 64 lowercase hex")
	}
	return validateEnvelopeDomains(e.AllowedDomains)
}

// validateEnvelopeDomains applies the tx-33 sharing-snapshot rules.
func validateEnvelopeDomains(domains []string) error {
	if len(domains) > MaxEnvelopeDomains {
		return fmt.Errorf("allowed_domains is capped at %d entries", MaxEnvelopeDomains)
	}
	sorted := append([]string(nil), domains...)
	sort.Strings(sorted)
	for i, d := range sorted {
		if d == "" || len(d) > MaxEnvelopeDomainBytes || strings.TrimSpace(d) != d {
			return fmt.Errorf("allowed_domains entries must be non-empty, unpadded tags of at most %d bytes", MaxEnvelopeDomainBytes)
		}
		for _, r := range d {
			if unicode.IsControl(r) {
				return errors.New("allowed_domains entry contains control characters")
			}
		}
		if i > 0 && sorted[i-1] == d {
			return fmt.Errorf("allowed_domains contains duplicate %q", d)
		}
	}
	if len(sorted) > 1 && sorted[0] == "*" {
		return errors.New("wildcard must be the only allowed domain")
	}
	return nil
}

// Sign fills Schema and Mode (if empty), RootCredentialID and Sig using the
// Root key.
func (e *LinkEnvelope) Sign(root ed25519.PrivateKey) error {
	if e.Schema == "" {
		e.Schema = EnvelopeSchemaID
	}
	if e.Mode == "" {
		e.Mode = EnvelopeModeAsk
	}
	pub, ok := root.Public().(ed25519.PublicKey)
	if !ok {
		return errors.New("link envelope: bad root key")
	}
	e.RootCredentialID = hex.EncodeToString(pub)
	if err := e.validate(); err != nil {
		return fmt.Errorf("link envelope: %w", err)
	}
	e.Sig = hex.EncodeToString(ed25519.Sign(root, e.CanonicalBytes()))
	return nil
}

// VerifyUnderRoot checks structure and that the envelope was signed by exactly
// the Root credential rootID / rootPub.
func (e *LinkEnvelope) VerifyUnderRoot(rootID string, rootPub ed25519.PublicKey) error {
	if err := e.validate(); err != nil {
		return err
	}
	if len(rootPub) != ed25519.PublicKeySize || hex.EncodeToString(rootPub) != rootID {
		return errors.New("current Root credential is unavailable")
	}
	if e.RootCredentialID != rootID {
		return errors.New("envelope was not signed by the current Root credential")
	}
	sig, err := hex.DecodeString(e.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return errors.New("signature is malformed")
	}
	if !ed25519.Verify(rootPub, e.CanonicalBytes(), sig) {
		return errors.New("signature does not verify under the current Root credential")
	}
	return nil
}

// CurrentAt reports whether now is inside [issued_at, expires_at).
func (e *LinkEnvelope) CurrentAt(now int64) bool {
	return now >= e.IssuedAt && now < e.ExpiresAt
}

// EnvelopeContext is what an envelope is checked against at use time.
type EnvelopeContext struct {
	RootCredentialID string
	RootPub          ed25519.PublicKey
	Policy           AcceptedState // the accepted org policy
	RosterScope      string        // the accepted roster's scope
	Now              int64
}

// Covers reports whether e currently covers links of its org: it verifies under
// the current Root, is inside its window, names the accepted policy's org root,
// and its scope is the accepted roster's scope. Every deny carries a reason.
func (e *LinkEnvelope) Covers(c EnvelopeContext) (bool, string) {
	if e == nil {
		return false, "no owner envelope for this org"
	}
	if err := e.VerifyUnderRoot(c.RootCredentialID, c.RootPub); err != nil {
		return false, "owner envelope: " + err.Error()
	}
	if !e.CurrentAt(c.Now) {
		return false, "owner envelope is not current (expired or not yet valid)"
	}
	if c.Policy.Revision == 0 || c.Policy.OrgID != e.OrgID {
		return false, "no accepted org policy for the envelope's org"
	}
	if c.Policy.RootKeyID != e.OrgRootKeyID {
		return false, "owner envelope names a different org root than the accepted policy"
	}
	if c.RosterScope == "" || c.RosterScope != e.Scope {
		return false, "owner envelope scope does not match the accepted roster scope"
	}
	return true, ""
}

// AllowsSharing reports whether an outbound sharing snapshot (domains, at the
// agreement's clearance) stays inside the envelope. Every requested domain must
// be covered by an allowed domain with the agreement-scope semantics: "*"
// covers everything, and an allowed "hr" covers "hr" and "hr.public". A "*"
// request needs a "*" envelope.
func (e *LinkEnvelope) AllowsSharing(domains []string, clearance uint8) (bool, string) {
	if clearance > e.MaxClearance {
		return false, fmt.Sprintf("agreement clearance %d exceeds the owner envelope's max_clearance %d", clearance, e.MaxClearance)
	}
	for _, d := range domains {
		if !envelopeDomainCovers(e.AllowedDomains, d) {
			return false, fmt.Sprintf("domain %q is outside the owner envelope", d)
		}
	}
	return true, ""
}

func envelopeDomainCovers(allowed []string, domain string) bool {
	for _, a := range allowed {
		if a == "*" {
			return true
		}
		if domain == "*" || a == "" || domain == "" {
			continue
		}
		if a == domain || strings.HasPrefix(domain, a+".") {
			return true
		}
	}
	return false
}
