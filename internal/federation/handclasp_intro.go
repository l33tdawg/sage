package federation

// Org-introduced handshake transcript (RFC #443 Phase B2, contract v2 §1–§2).
//
// E_intro carries today's enrollment fields (chain ids, pins, endpoints, scope
// digests, nonces, seed commitment, P2P routes) plus the org evidence both
// sides admitted: org id and root key, policy and roster revision + digest
// (one value each: both sides must hold the same ones, else the session is
// refused before E_intro exists), each side's card and envelope revision +
// digest, the deterministic offer id and the kind. It is stored as canonical
// JSON (the introduced_link e_intro column) and attested under its own domain
// tag, so a manual-JOIN E can never be confused with an introduced one.
//
// The pair seed itself is never part of the stored transcript: only its
// commitment is, so the record can be kept after the seed is zeroized.

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
)

// Domain tags for the introduced path (NUL-terminated in hashed input).
const (
	tagIntroE           = "sage/handclasp-intro/E/v1"
	tagIntroHostAccept  = "sage/handclasp-intro/host-accept/v1"
	tagIntroGuestAccept = "sage/handclasp-intro/guest-accept/v1"
	tagIntroGuestAck    = "sage/handclasp-intro/guest-ack/v1"

	// IntroTranscriptVersion is the only accepted transcript version.
	IntroTranscriptVersion = 1
	// MaxIntroTranscriptBytes bounds the canonical transcript. Two sides of
	// at most 8 routes each fit comfortably.
	MaxIntroTranscriptBytes = 16 << 10

	introKindNew   = "new"
	introKindRekey = "rekey"

	maxIntroRoutes      = 8
	maxIntroEndpointLen = 255
	maxIntroChainIDLen  = 50
	maxIntroPeerIDLen   = 128
	maxIntroRouteLen    = 512
	maxIntroOrgIDLen    = 128
)

// IntroSignature names one of the three acceptance signatures over E_intro.
type IntroSignature int

const (
	// IntroHostAccept is the host owner's acceptance.
	IntroHostAccept IntroSignature = iota + 1
	// IntroGuestAccept is the guest owner's acceptance.
	IntroGuestAccept
	// IntroGuestAck is the guest's acknowledgement of the host acceptance.
	IntroGuestAck
)

func (s IntroSignature) tag() (string, bool) {
	switch s {
	case IntroHostAccept:
		return tagIntroHostAccept, true
	case IntroGuestAccept:
		return tagIntroGuestAccept, true
	case IntroGuestAck:
		return tagIntroGuestAck, true
	}
	return "", false
}

// IntroSide is one side's transport identity and org evidence.
type IntroSide struct {
	ChainID  string   `json:"chain_id"`
	AgentPub string   `json:"agent_pub"` // 64 lowercase hex, the card's agent key
	Pin      string   `json:"pin"`       // 64 lowercase hex, CA SPKI pin from the card
	Endpoint string   `json:"endpoint"`
	Nonce    string   `json:"nonce"` // 32 lowercase hex (16 bytes)
	Scope    string   `json:"scope"` // 64 lowercase hex, ScopeDigest of the side's JOIN scope
	PeerID   string   `json:"peer_id,omitempty"`
	P2PAddrs []string `json:"p2p_addrs,omitempty"` // sorted, de-duplicated

	CardRev        uint64 `json:"card_rev"`
	CardDigest     string `json:"card_digest"`
	EnvelopeRev    uint64 `json:"envelope_rev"`
	EnvelopeDigest string `json:"envelope_digest"`
}

// IntroTranscript is E_intro.
type IntroTranscript struct {
	V       int    `json:"v"`
	Kind    string `json:"kind"`
	OfferID string `json:"offer_id"`

	OrgID        string `json:"org_id"`
	OrgRootKeyID string `json:"org_root_key_id"`

	PolicyRevision uint64 `json:"policy_revision"`
	PolicyDigest   string `json:"policy_digest"`
	RosterRevision uint64 `json:"roster_revision"`
	RosterDigest   string `json:"roster_digest"`

	Host  IntroSide `json:"host"`
	Guest IntroSide `json:"guest"`

	SeedCommit string `json:"seed_commit"` // 64 lowercase hex, SeedCommit(seed)
}

var (
	introHex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
	introHex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// Canonicalize sorts and de-duplicates the route lists in place. Every other
// field is already single-valued.
func (t *IntroTranscript) Canonicalize() {
	for _, s := range []*IntroSide{&t.Host, &t.Guest} {
		if len(s.P2PAddrs) == 0 {
			s.P2PAddrs = nil
			continue
		}
		r := append([]string(nil), s.P2PAddrs...)
		sort.Strings(r)
		s.P2PAddrs = dedupSorted(r)
	}
}

func (s IntroSide) validate(which string) error {
	switch {
	case s.ChainID == "" || len(s.ChainID) > maxIntroChainIDLen:
		return fmt.Errorf("%s chain_id must be 1..%d bytes", which, maxIntroChainIDLen)
	case !introHex64.MatchString(s.AgentPub):
		return fmt.Errorf("%s agent_pub must be 64 lowercase hex", which)
	case !introHex64.MatchString(s.Pin):
		return fmt.Errorf("%s pin must be 64 lowercase hex", which)
	case s.Endpoint == "" || len(s.Endpoint) > maxIntroEndpointLen:
		return fmt.Errorf("%s endpoint must be 1..%d bytes", which, maxIntroEndpointLen)
	case !introHex32.MatchString(s.Nonce):
		return fmt.Errorf("%s nonce must be 32 lowercase hex", which)
	case !introHex64.MatchString(s.Scope):
		return fmt.Errorf("%s scope must be 64 lowercase hex", which)
	case len(s.PeerID) > maxIntroPeerIDLen:
		return fmt.Errorf("%s peer_id exceeds %d bytes", which, maxIntroPeerIDLen)
	case len(s.P2PAddrs) > maxIntroRoutes:
		return fmt.Errorf("%s carries more than %d routes", which, maxIntroRoutes)
	case s.CardRev == 0 || !introHex64.MatchString(s.CardDigest):
		return fmt.Errorf("%s card revision and digest are required", which)
	case s.EnvelopeRev == 0 || !introHex64.MatchString(s.EnvelopeDigest):
		return fmt.Errorf("%s envelope revision and digest are required", which)
	}
	for i, r := range s.P2PAddrs {
		if r == "" || len(r) > maxIntroRouteLen {
			return fmt.Errorf("%s route %d must be 1..%d bytes", which, i, maxIntroRouteLen)
		}
		if i > 0 && s.P2PAddrs[i-1] >= r {
			return fmt.Errorf("%s routes must be sorted and unique", which)
		}
	}
	return nil
}

// Validate checks the transcript is complete and canonical. It does not
// check the evidence against local state; the write path does that.
func (t *IntroTranscript) Validate() error {
	switch {
	case t.V != IntroTranscriptVersion:
		return fmt.Errorf("transcript version %d is not supported", t.V)
	case t.Kind != introKindNew && t.Kind != introKindRekey:
		return errors.New("kind must be new or rekey")
	case !introHex64.MatchString(t.OfferID):
		return errors.New("offer_id must be 64 lowercase hex")
	case t.OrgID == "" || len(t.OrgID) > maxIntroOrgIDLen:
		return fmt.Errorf("org_id must be 1..%d bytes", maxIntroOrgIDLen)
	case !introHex64.MatchString(t.OrgRootKeyID):
		return errors.New("org_root_key_id must be 64 lowercase hex")
	case t.PolicyRevision == 0 || !introHex64.MatchString(t.PolicyDigest):
		return errors.New("policy revision and digest are required")
	case t.RosterRevision == 0 || !introHex64.MatchString(t.RosterDigest):
		return errors.New("roster revision and digest are required")
	case !introHex64.MatchString(t.SeedCommit):
		return errors.New("seed_commit must be 64 lowercase hex")
	}
	if err := t.Host.validate("host"); err != nil {
		return err
	}
	if err := t.Guest.validate("guest"); err != nil {
		return err
	}
	switch {
	case t.Host.ChainID == t.Guest.ChainID:
		return errors.New("host and guest are the same chain")
	case t.Host.AgentPub == t.Guest.AgentPub:
		return errors.New("host and guest are the same agent")
	case t.Host.AgentPub > t.Guest.AgentPub:
		// Contract v2 §5: host = the lower agent_pub. Lowercase hex orders
		// like the raw bytes.
		return errors.New("host must be the lower agent_pub")
	}
	return nil
}

// Encode validates the transcript and returns its canonical bytes.
func (t *IntroTranscript) Encode() ([]byte, error) {
	c := *t
	c.Canonicalize()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("intro transcript: %w", err)
	}
	b, err := json.Marshal(&c)
	if err != nil {
		return nil, fmt.Errorf("intro transcript: %w", err)
	}
	if len(b) > MaxIntroTranscriptBytes {
		return nil, fmt.Errorf("intro transcript: exceeds %d bytes", MaxIntroTranscriptBytes)
	}
	return b, nil
}

// DecodeIntroTranscript parses stored or received transcript bytes. They must
// be exactly the canonical encoding: unknown fields, non-canonical order or
// whitespace, and trailing bytes are refused.
func DecodeIntroTranscript(b []byte) (*IntroTranscript, error) {
	if len(b) == 0 || len(b) > MaxIntroTranscriptBytes {
		return nil, fmt.Errorf("intro transcript: size must be 1..%d bytes", MaxIntroTranscriptBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var t IntroTranscript
	if err := dec.Decode(&t); err != nil {
		return nil, fmt.Errorf("intro transcript: %w", err)
	}
	canon, err := t.Encode()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canon, b) {
		return nil, errors.New("intro transcript: not the canonical encoding")
	}
	return &t, nil
}

// IntroAttestation is E_intro's 32-byte attestation:
// SHA256(tagIntroE \0 canonical bytes).
func IntroAttestation(canonical []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(tagIntroE))
	h.Write([]byte{0x00})
	h.Write(canonical)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// SeedCommitHex is the transcript form of SeedCommit.
func SeedCommitHex(seed []byte) string {
	c := SeedCommit(seed)
	return hex.EncodeToString(c[:])
}

func introSigMsg(kind IntroSignature, e [32]byte) ([]byte, error) {
	tag, ok := kind.tag()
	if !ok {
		return nil, errors.New("unknown intro signature kind")
	}
	msg := make([]byte, 0, len(tag)+1+len(e))
	msg = append(msg, tag...)
	msg = append(msg, 0x00)
	msg = append(msg, e[:]...)
	return msg, nil
}

// SignIntro signs E_intro under the tag for kind with the signer's card agent
// key.
func SignIntro(key ed25519.PrivateKey, kind IntroSignature, e [32]byte) ([]byte, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("intro signature: bad key")
	}
	msg, err := introSigMsg(kind, e)
	if err != nil {
		return nil, err
	}
	return ed25519.Sign(key, msg), nil
}

// VerifyIntro verifies one acceptance signature against the agent_pub (hex)
// that the signer's roster card binds.
func VerifyIntro(agentPubHex string, kind IntroSignature, e [32]byte, sig []byte) bool {
	if !introHex64.MatchString(agentPubHex) || len(sig) != ed25519.SignatureSize {
		return false
	}
	pub, err := hex.DecodeString(agentPubHex)
	if err != nil {
		return false
	}
	msg, err := introSigMsg(kind, e)
	if err != nil {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(pub), msg, sig)
}
