package orgpolicy

// Link card (RFC #443 Phase A): a node's own signed statement of how a fleet
// peer can reach and pin it. The node signs it with its OWN agent key, so the
// org can relay a card but can never mint or alter one. A card carries no
// authority by itself: eligibility comes from the org policy allow-list and
// activation stays an owner act.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"unicode/utf8"
)

const (
	// LinkCardSchemaID is bound into every card signature.
	LinkCardSchemaID = "sage.org-link-card.v1"

	linkCardDomainSep = "sage-org-link-card-v1\x00"

	// MaxLinkCardBytes bounds one card's canonical form.
	MaxLinkCardBytes = 4 * 1024
	// MaxLinkCardRequestBytes preserves the agreed 16 KiB create/update cap.
	MaxLinkCardRequestBytes = 16 * 1024
	// MaxLinkCardWindowSeconds bounds expires_at - issued_at (30 days).
	MaxLinkCardWindowSeconds = 30 * 24 * 60 * 60

	maxCardRoutes     = 8
	maxCardRouteBytes = 512
	maxCardEndpoint   = 255
	maxCardChainID    = 50
	maxCardPeerID     = 128
)

// cardChainIDPattern is internal/federation's chainIDPattern narrowed to the
// 50-byte consensus chain-id bound, so a card can never name a chain id
// federation would refuse.
var cardChainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,49}$`)

// LinkCard is the node-signed card. Sig is an ed25519 signature by AgentPub over
// CanonicalBytes(); it is not part of the canonical bytes.
type LinkCard struct {
	Schema    string   `json:"schema"`
	OrgID     string   `json:"org_id"`
	AgentPub  string   `json:"agent_pub"`   // hex ed25519, the enrollee key; the signer
	ChainID   string   `json:"chain_id"`    // the node's own chain id
	CASPKIPin string   `json:"ca_spki_pin"` // hex sha256 of the node CA SPKI
	Endpoint  string   `json:"endpoint"`    // https://host:port of the federation listener
	P2PPeerID string   `json:"p2p_peer_id,omitempty"`
	P2PRoutes []string `json:"p2p_routes,omitempty"`
	CardRev   uint64   `json:"card_rev"` // monotonic per agent_pub
	IssuedAt  int64    `json:"issued_at"`
	ExpiresAt int64    `json:"expires_at"`
	Sig       string   `json:"sig"`
}

// CanonicalBytes is the exact signed encoding. Routes are sorted (validate
// rejects duplicates, keeping the encoding injective).
func (c *LinkCard) CanonicalBytes() []byte {
	b := make([]byte, 0, 512)
	b = append(b, []byte(linkCardDomainSep)...)
	b = lp(b, c.Schema)
	b = lp(b, c.OrgID)
	b = lp(b, c.AgentPub)
	b = lp(b, c.ChainID)
	b = lp(b, c.CASPKIPin)
	b = lp(b, c.Endpoint)
	b = lp(b, c.P2PPeerID)
	routes := append([]string(nil), c.P2PRoutes...)
	sort.Strings(routes)
	b = u32(b, uint32(len(routes))) // #nosec G115 -- bounded by maxCardRoutes
	for _, r := range routes {
		b = lp(b, r)
	}
	b = u64(b, c.CardRev)
	b = i64(b, c.IssuedAt)
	b = i64(b, c.ExpiresAt)
	return b
}

// Digest is the hex SHA-256 of the canonical bytes.
func (c *LinkCard) Digest() string {
	sum := sha256.Sum256(c.CanonicalBytes())
	return hex.EncodeToString(sum[:])
}

func (c *LinkCard) validate() error {
	if c.Schema != LinkCardSchemaID {
		return fmt.Errorf("link card: schema %q != %q", c.Schema, LinkCardSchemaID)
	}
	if c.OrgID == "" || !utf8.ValidString(c.OrgID) {
		return fmt.Errorf("link card: org_id is required")
	}
	if !isAgentPubHex(c.AgentPub) {
		return fmt.Errorf("link card: agent_pub must be 64 lowercase hex")
	}
	if len(c.ChainID) > maxCardChainID || !cardChainIDPattern.MatchString(c.ChainID) || c.ChainID == "." || c.ChainID == ".." {
		return fmt.Errorf("link card: invalid chain_id")
	}
	if len(c.CASPKIPin) != 2*sha256.Size || !isLowerHex(c.CASPKIPin) {
		return fmt.Errorf("link card: ca_spki_pin must be 64 lowercase hex")
	}
	if err := validateCardEndpoint(c.Endpoint); err != nil {
		return err
	}
	if len(c.P2PPeerID) > maxCardPeerID || !utf8.ValidString(c.P2PPeerID) {
		return fmt.Errorf("link card: invalid p2p_peer_id")
	}
	if len(c.P2PRoutes) > maxCardRoutes {
		return fmt.Errorf("link card: %d p2p routes exceeds cap %d", len(c.P2PRoutes), maxCardRoutes)
	}
	if len(c.P2PRoutes) > 0 && c.P2PPeerID == "" {
		return fmt.Errorf("link card: p2p routes require p2p_peer_id")
	}
	seen := make(map[string]struct{}, len(c.P2PRoutes))
	for _, r := range c.P2PRoutes {
		if r == "" || len(r) > maxCardRouteBytes || !utf8.ValidString(r) {
			return fmt.Errorf("link card: invalid p2p route")
		}
		if _, dup := seen[r]; dup {
			return fmt.Errorf("link card: duplicate p2p route")
		}
		seen[r] = struct{}{}
	}
	if c.CardRev == 0 {
		return fmt.Errorf("link card: card_rev must be >= 1")
	}
	if c.IssuedAt <= 0 || c.ExpiresAt <= c.IssuedAt {
		return fmt.Errorf("link card: expires_at must follow issued_at")
	}
	if c.ExpiresAt-c.IssuedAt > MaxLinkCardWindowSeconds {
		return fmt.Errorf("link card: validity window exceeds 30 days")
	}
	return nil
}

func validateCardEndpoint(endpoint string) error {
	if endpoint == "" || len(endpoint) > maxCardEndpoint {
		return fmt.Errorf("link card: endpoint must be 1..%d bytes", maxCardEndpoint)
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/") {
		return fmt.Errorf("link card: endpoint must be https://host:port")
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" {
		return fmt.Errorf("link card: endpoint must carry an explicit port")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("link card: endpoint port out of range")
	}
	return nil
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return len(s) > 0
}

// Sign sets Schema and AgentPub from the node key, validates and signs.
func (c *LinkCard) Sign(key ed25519.PrivateKey) error {
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("link card: invalid signing key")
	}
	c.Schema = LinkCardSchemaID
	c.AgentPub = hex.EncodeToString(pub)
	if err := c.validate(); err != nil {
		return err
	}
	cb := c.CanonicalBytes()
	if len(cb) > MaxLinkCardBytes {
		return fmt.Errorf("link card: canonical form %d bytes exceeds cap %d", len(cb), MaxLinkCardBytes)
	}
	c.Sig = hex.EncodeToString(ed25519.Sign(key, cb))
	return nil
}

// Verify checks the card fail-closed under its own AgentPub: structure, size
// and signature. It does not check freshness (see CurrentAt).
func (c *LinkCard) Verify() error {
	if err := c.validate(); err != nil {
		return err
	}
	cb := c.CanonicalBytes()
	if len(cb) > MaxLinkCardBytes {
		return fmt.Errorf("link card: canonical form %d bytes exceeds cap %d", len(cb), MaxLinkCardBytes)
	}
	pub, _ := hex.DecodeString(c.AgentPub)
	sig, err := hex.DecodeString(c.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize || !ed25519.Verify(ed25519.PublicKey(pub), cb, sig) {
		return fmt.Errorf("link card: signature invalid")
	}
	return nil
}

// CurrentAt reports whether the card is inside its validity window at now.
func (c *LinkCard) CurrentAt(now int64) bool {
	return now >= c.IssuedAt && now < c.ExpiresAt
}
