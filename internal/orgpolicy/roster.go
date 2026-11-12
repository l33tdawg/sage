package orgpolicy

// Org roster (RFC #443 Phase A): the org's signed, revisioned list of current
// fleet link cards. It is its own signed type (not a policy field) with its own
// revision, admitted with exactly the policy's ordering semantics
// (AdmitWithAuthority): verify first, then rollback / same-digest no-op /
// equivocation / apply. Every embedded card must verify under its own
// agent_pub, so the org relays cards but cannot forge one; one bad card
// rejects the whole roster (no partial apply).

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"unicode/utf8"
)

const (
	// RosterSchemaID is bound into every roster signature.
	RosterSchemaID = "sage.org-roster.v1"

	rosterDomainSep = "sage-org-roster-v1\x00"

	// MaxRosterBytes bounds one roster's canonical form.
	MaxRosterBytes = 1 << 20
	// MaxRosterRequestBytes caps a roster delivery body before decode: the
	// canonical cap plus JSON framing, hex signatures and rotation attestations.
	MaxRosterRequestBytes = 2 << 20
	// MaxRosterCards bounds the card count.
	MaxRosterCards = 1024
	// MaxRosterWindowSeconds bounds expiry - not_before (7 days).
	MaxRosterWindowSeconds = 7 * 24 * 60 * 60
)

// Roster is the org-signed roster. Sig is by the org signing key over
// CanonicalBytes().
type Roster struct {
	Schema      string     `json:"schema"`
	OrgID       string     `json:"org_id"`
	Scope       string     `json:"scope"`
	Revision    uint64     `json:"revision"`
	IssuerKeyID string     `json:"issuer_key_id"`
	KeyEpoch    string     `json:"key_epoch"`
	IssuedAt    int64      `json:"issued_at"`
	NotBefore   int64      `json:"not_before"`
	Expiry      int64      `json:"expiry"` // required; a roster always expires
	Cards       []LinkCard `json:"cards"`
	Sig         string     `json:"sig"`
}

// sortedCards returns the cards ordered by agent_pub (validate rejects
// duplicates, keeping the encoding injective and order-independent).
func (r *Roster) sortedCards() []LinkCard {
	cards := append([]LinkCard(nil), r.Cards...)
	sort.Slice(cards, func(i, j int) bool { return cards[i].AgentPub < cards[j].AgentPub })
	return cards
}

// CanonicalBytes binds every roster field and, per card, the card's own
// canonical bytes and signature.
func (r *Roster) CanonicalBytes() []byte {
	b := make([]byte, 0, 1024)
	b = append(b, []byte(rosterDomainSep)...)
	b = lp(b, r.Schema)
	b = lp(b, r.OrgID)
	b = lp(b, r.Scope)
	b = u64(b, r.Revision)
	b = lp(b, r.IssuerKeyID)
	b = lp(b, r.KeyEpoch)
	b = i64(b, r.IssuedAt)
	b = i64(b, r.NotBefore)
	b = i64(b, r.Expiry)
	cards := r.sortedCards()
	b = u32(b, uint32(len(cards))) // #nosec G115 -- bounded by MaxRosterCards
	for i := range cards {
		b = lp(b, string(cards[i].CanonicalBytes()))
		b = lp(b, cards[i].Sig)
	}
	return b
}

// Digest is the hex SHA-256 of the canonical bytes.
func (r *Roster) Digest() string {
	sum := sha256.Sum256(r.CanonicalBytes())
	return hex.EncodeToString(sum[:])
}

func (r *Roster) validate() error {
	if r.Schema != RosterSchemaID {
		return fmt.Errorf("org roster: schema %q != %q", r.Schema, RosterSchemaID)
	}
	for name, s := range map[string]string{
		"org_id": r.OrgID, "scope": r.Scope, "issuer_key_id": r.IssuerKeyID, "key_epoch": r.KeyEpoch,
	} {
		if s == "" || !utf8.ValidString(s) {
			return fmt.Errorf("org roster: %s is required", name)
		}
	}
	if r.Revision == 0 {
		return fmt.Errorf("org roster: revision must be >= 1")
	}
	if !isAgentPubHex(r.IssuerKeyID) {
		return fmt.Errorf("org roster: issuer_key_id must be a hex ed25519 public key")
	}
	if r.Expiry == 0 {
		return fmt.Errorf("org roster: expiry is required")
	}
	start := r.NotBefore
	if start == 0 {
		start = r.IssuedAt
	}
	if start <= 0 || r.Expiry <= start {
		return fmt.Errorf("org roster: expiry must follow not_before")
	}
	if r.Expiry-start > MaxRosterWindowSeconds {
		return fmt.Errorf("org roster: validity window exceeds 7 days")
	}
	if len(r.Cards) > MaxRosterCards {
		return fmt.Errorf("org roster: %d cards exceeds cap %d", len(r.Cards), MaxRosterCards)
	}
	agents := make(map[string]struct{}, len(r.Cards))
	chains := make(map[string]struct{}, len(r.Cards))
	for i := range r.Cards {
		c := &r.Cards[i]
		if err := c.Verify(); err != nil {
			return fmt.Errorf("org roster: card %d: %w", i, err)
		}
		if c.OrgID != r.OrgID {
			return fmt.Errorf("org roster: card %d belongs to org %q", i, c.OrgID)
		}
		if _, dup := agents[c.AgentPub]; dup {
			return fmt.Errorf("org roster: duplicate agent_pub %s", c.AgentPub)
		}
		agents[c.AgentPub] = struct{}{}
		if _, dup := chains[c.ChainID]; dup {
			return fmt.Errorf("org roster: duplicate chain_id %q", c.ChainID)
		}
		chains[c.ChainID] = struct{}{}
	}
	return nil
}

// Sign sets Schema/IssuerKeyID from the org key, validates and signs.
func (r *Roster) Sign(key ed25519.PrivateKey) error {
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("org roster: invalid signing key")
	}
	r.Schema = RosterSchemaID
	r.IssuerKeyID = hex.EncodeToString(pub)
	if err := r.validate(); err != nil {
		return err
	}
	cb := r.CanonicalBytes()
	if len(cb) > MaxRosterBytes {
		return fmt.Errorf("org roster: canonical form %d bytes exceeds cap %d", len(cb), MaxRosterBytes)
	}
	r.Sig = hex.EncodeToString(ed25519.Sign(key, cb))
	return nil
}

// Verify checks the roster fail-closed under the pinned org key, including every
// embedded card under its own agent key.
func (r *Roster) Verify(pinnedPub ed25519.PublicKey) error {
	if len(pinnedPub) != ed25519.PublicKeySize {
		return fmt.Errorf("org roster: invalid pinned key")
	}
	if r.IssuerKeyID != hex.EncodeToString(pinnedPub) {
		return fmt.Errorf("org roster: issuer key does not match the authorized org key")
	}
	if err := r.validate(); err != nil {
		return err
	}
	cb := r.CanonicalBytes()
	if len(cb) > MaxRosterBytes {
		return fmt.Errorf("org roster: canonical form %d bytes exceeds cap %d", len(cb), MaxRosterBytes)
	}
	sig, err := hex.DecodeString(r.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(pinnedPub, cb, sig) {
		return fmt.Errorf("org roster: signature invalid")
	}
	return nil
}

// CurrentAt reports whether the roster is inside its validity window.
func (r *Roster) CurrentAt(now int64) bool {
	if r.NotBefore != 0 && now < r.NotBefore {
		return false
	}
	return now < r.Expiry
}

// AuthorizedSignerForRoster resolves which currently authorized org signer (see
// AuthorizedSigners) signed r, honoring the signer's pinned key epoch.
func AuthorizedSignerForRoster(r *Roster, signers []AuthorizedSigner) (AuthorizedSigner, error) {
	var lastErr error
	for _, signer := range signers {
		if signer.KeyEpoch != "" && r.KeyEpoch != signer.KeyEpoch {
			continue
		}
		if err := r.Verify(signer.PublicKey); err == nil {
			return signer, nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return AuthorizedSigner{}, lastErr
	}
	return AuthorizedSigner{}, fmt.Errorf("org roster: issuer key/epoch is not currently authorized")
}

// RosterState is the node's persisted roster high-water mark. The zero value
// (Revision == 0) means no roster accepted.
type RosterState struct {
	OrgID       string `json:"org_id"`
	Scope       string `json:"scope"`
	Revision    uint64 `json:"revision"`
	Digest      string `json:"digest"`
	IssuerKeyID string `json:"issuer_key_id"`
	KeyEpoch    string `json:"key_epoch"`
	NotBefore   int64  `json:"not_before"`
	Expiry      int64  `json:"expiry"`
	Cards       int    `json:"cards"`
}

func rosterStateFrom(r *Roster) RosterState {
	return RosterState{
		OrgID: r.OrgID, Scope: r.Scope, Revision: r.Revision, Digest: r.Digest(),
		IssuerKeyID: r.IssuerKeyID, KeyEpoch: r.KeyEpoch,
		NotBefore: r.NotBefore, Expiry: r.Expiry, Cards: len(r.Cards),
	}
}

// CurrentAt reports whether the accepted roster is inside its window.
func (st RosterState) CurrentAt(now int64) bool {
	if st.Revision == 0 {
		return false
	}
	if st.NotBefore != 0 && now < st.NotBefore {
		return false
	}
	return now < st.Expiry
}

// AdmitRoster is the pure ordering decision for a delivered roster, with the
// policy's semantics. It verifies r under authorizedPub before ordering, so a
// rollback or equivocation verdict always concerns a genuinely org-signed
// roster. scope must equal the node's accepted policy scope.
func AdmitRoster(prev RosterState, r *Roster, authorizedPub ed25519.PublicKey, scope string) (RosterState, Decision, error) {
	if err := r.Verify(authorizedPub); err != nil {
		return prev, "", err
	}
	if r.Scope != scope {
		return prev, "", fmt.Errorf("org roster: scope %q does not match the accepted policy scope %q", r.Scope, scope)
	}
	if prev.Revision == 0 {
		return rosterStateFrom(r), DecisionApplied, nil
	}
	if r.OrgID != prev.OrgID {
		return prev, "", fmt.Errorf("org roster: org_id %q does not match accepted %q", r.OrgID, prev.OrgID)
	}
	switch {
	case r.Revision < prev.Revision:
		return prev, DecisionRejectedRollback, nil
	case r.Revision == prev.Revision:
		if r.Digest() == prev.Digest {
			return prev, DecisionNoOpSameDigest, nil
		}
		return prev, DecisionRejectedEquivocation, nil
	default:
		return rosterStateFrom(r), DecisionApplied, nil
	}
}
