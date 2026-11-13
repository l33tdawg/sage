package orgpolicy

// Manual QR JOIN gate (RFC #443 Phase A2).
//
// A node is fleet-managed once it holds a stored EnrollmentOutcome. On such a
// node a manual QR JOIN is a new peering, so it needs the peer to be eligible
// under every org the node is enrolled in (PeerEligible: a current policy for
// OpNewPeering, the stored body IS the accepted policy, and its v2 allow-list
// names the peer's agent key). A node with no enrollment keeps its existing
// JOIN behaviour unchanged.
//
// When the peer is not eligible, a local override can let one specific peer
// through. An override is signed by the node's CURRENT Root credential, names
// the peer's agent key and chain id, carries a reason, a revision and an
// expiry (at most 30 days), and every use is appended to its history. It is
// re-verified at every use against the Root that is current at that moment,
// so a Root rotation retires every override the old Root signed.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// KVLister is the optional key-listing surface the JOIN gate needs to find
// enrollment records and overrides. The node-local SQLite adapter implements
// it. A KV without it cannot answer "is this node fleet-managed", so the gate
// fails closed on such a KV.
type KVLister interface {
	// ListStateKeys returns every key that starts with prefix, ascending. It
	// must fail rather than truncate when more than limit keys match.
	ListStateKeys(prefix string, limit int) ([]string, error)
}

const (
	// JoinOverrideSchemaID is bound into every override signature.
	JoinOverrideSchemaID = "sage.join-override.v1"

	joinOverrideDomainSep = "sage-join-override-v1\x00"

	// MaxJoinOverrideWindowSeconds bounds expires_at - issued_at (30 days).
	MaxJoinOverrideWindowSeconds = 30 * 24 * 60 * 60
	// MaxJoinOverrideReasonBytes bounds the human reason.
	MaxJoinOverrideReasonBytes = 512
	// MaxJoinOverrideUses bounds the persisted usage history per peer; the
	// oldest entries are dropped first. TotalUses keeps the full count.
	MaxJoinOverrideUses = 256

	maxEnrollmentRecords = 1024
	maxJoinOverrides     = 4096
)

// overrideChainIDPattern is internal/federation's JOIN chain-id pattern, so an
// override can name any chain id a manual QR JOIN accepts.
var overrideChainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// ErrJoinOverrideInvalid wraps an override that fails structure, signature,
// Root binding or revision ordering. It is a client error.
var ErrJoinOverrideInvalid = errors.New("join override: invalid")

// ErrJoinOverrideStateConflict means the override record changed between read
// and write; reload and retry.
var ErrJoinOverrideStateConflict = errors.New("join override: state changed concurrently")

// JoinOverride is a Root-signed, expiring local permission for one manual QR
// JOIN peer that the org policy does not make eligible.
type JoinOverride struct {
	Schema           string `json:"schema"`
	PeerAgentPub     string `json:"peer_agent_pub"` // 64 lowercase hex
	PeerChainID      string `json:"peer_chain_id"`
	Reason           string `json:"reason"`
	Revision         uint64 `json:"revision"` // monotonic per peer_agent_pub
	IssuedAt         int64  `json:"issued_at"`
	ExpiresAt        int64  `json:"expires_at"`
	RootCredentialID string `json:"root_credential_id"` // hex ed25519 of the signing Root
	Sig              string `json:"sig"`
}

// CanonicalBytes is the exact signed encoding.
func (o *JoinOverride) CanonicalBytes() []byte {
	b := make([]byte, 0, 256+len(o.Reason))
	b = append(b, []byte(joinOverrideDomainSep)...)
	b = lp(b, o.Schema)
	b = lp(b, o.PeerAgentPub)
	b = lp(b, o.PeerChainID)
	b = lp(b, o.Reason)
	b = u64(b, o.Revision)
	b = u64(b, uint64(o.IssuedAt))  // #nosec G115 -- validate rejects negatives
	b = u64(b, uint64(o.ExpiresAt)) // #nosec G115 -- validate rejects negatives
	b = lp(b, o.RootCredentialID)
	return b
}

// Digest is the hex sha256 of the canonical bytes plus the signature.
func (o *JoinOverride) Digest() string {
	h := sha256.New()
	h.Write(o.CanonicalBytes())
	h.Write([]byte(o.Sig))
	return hex.EncodeToString(h.Sum(nil))
}

func (o *JoinOverride) validate() error {
	switch {
	case o.Schema != JoinOverrideSchemaID:
		return fmt.Errorf("schema must be %q", JoinOverrideSchemaID)
	case !isAgentPubHex(o.PeerAgentPub):
		return errors.New("peer_agent_pub must be 64 lowercase hex")
	case !overrideChainIDPattern.MatchString(o.PeerChainID):
		return errors.New("peer_chain_id is not a valid chain id")
	case strings.TrimSpace(o.Reason) == "":
		return errors.New("reason is required")
	case len(o.Reason) > MaxJoinOverrideReasonBytes || !utf8.ValidString(o.Reason):
		return fmt.Errorf("reason must be valid UTF-8 of at most %d bytes", MaxJoinOverrideReasonBytes)
	case o.Revision == 0:
		return errors.New("revision must be at least 1")
	case o.IssuedAt <= 0 || o.ExpiresAt <= o.IssuedAt:
		return errors.New("expires_at must be after a positive issued_at")
	case o.ExpiresAt-o.IssuedAt > MaxJoinOverrideWindowSeconds:
		return errors.New("override window exceeds 30 days")
	case !isAgentPubHex(o.RootCredentialID):
		return errors.New("root_credential_id must be 64 lowercase hex")
	}
	return nil
}

// Sign fills Schema (if empty), RootCredentialID and Sig using the Root key.
func (o *JoinOverride) Sign(root ed25519.PrivateKey) error {
	if o.Schema == "" {
		o.Schema = JoinOverrideSchemaID
	}
	pub, ok := root.Public().(ed25519.PublicKey)
	if !ok {
		return errors.New("join override: bad root key")
	}
	o.RootCredentialID = hex.EncodeToString(pub)
	if err := o.validate(); err != nil {
		return fmt.Errorf("join override: %w", err)
	}
	o.Sig = hex.EncodeToString(ed25519.Sign(root, o.CanonicalBytes()))
	return nil
}

// VerifyUnderRoot checks structure and that the override was signed by
// exactly the Root credential rootID / rootPub.
func (o *JoinOverride) VerifyUnderRoot(rootID string, rootPub ed25519.PublicKey) error {
	if err := o.validate(); err != nil {
		return err
	}
	if len(rootPub) != ed25519.PublicKeySize || hex.EncodeToString(rootPub) != rootID {
		return errors.New("current Root credential is unavailable")
	}
	if o.RootCredentialID != rootID {
		return errors.New("override was not signed by the current Root credential")
	}
	sig, err := hex.DecodeString(o.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return errors.New("signature is malformed")
	}
	if !ed25519.Verify(rootPub, o.CanonicalBytes(), sig) {
		return errors.New("signature does not verify under the current Root credential")
	}
	return nil
}

// CurrentAt reports whether now is inside [issued_at, expires_at).
func (o *JoinOverride) CurrentAt(now int64) bool {
	return now >= o.IssuedAt && now < o.ExpiresAt
}

// JoinOverrideUse is one recorded use of an override.
type JoinOverrideUse struct {
	Revision    uint64 `json:"revision"`
	Side        string `json:"side"`  // host | guest
	Stage       string `json:"stage"` // request | confirm
	SessionID   string `json:"session_id"`
	PeerChainID string `json:"peer_chain_id"`
	At          int64  `json:"at"`
}

// JoinOverrideRecord is the persisted override plus its usage history.
type JoinOverrideRecord struct {
	Override  JoinOverride      `json:"override"`
	Digest    string            `json:"digest"`
	StoredAt  int64             `json:"stored_at"`
	TotalUses uint64            `json:"total_uses"`
	Uses      []JoinOverrideUse `json:"uses"`
}

const enrollPrefix = keyPrefix + "enroll/"
const overridePrefix = keyPrefix + "join-override/"

func overrideKey(peerAgentPub string) string { return overridePrefix + seg(peerAgentPub) }

func (s *Store) lister() (KVLister, error) {
	l, ok := s.kv.(KVLister)
	if !ok {
		return nil, errors.New("orgpolicy store: key listing is unavailable")
	}
	return l, nil
}

// FleetOrgs returns the sorted, de-duplicated org ids this node holds an
// EnrollmentOutcome for. An empty result means the node is not fleet-managed.
func (s *Store) FleetOrgs() ([]string, error) {
	l, err := s.lister()
	if err != nil {
		return nil, err
	}
	keys, err := l.ListStateKeys(enrollPrefix, maxEnrollmentRecords)
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: list enrollments: %w", err)
	}
	seen := map[string]bool{}
	var orgs []string
	for _, k := range keys {
		raw, gErr := s.kv.GetState(k)
		if gErr != nil {
			return nil, fmt.Errorf("orgpolicy store: load enrollment: %w", gErr)
		}
		if raw == nil {
			continue
		}
		var out EnrollmentOutcome
		if uErr := json.Unmarshal(raw, &out); uErr != nil {
			return nil, fmt.Errorf("orgpolicy store: decode enrollment: %w", uErr)
		}
		if out.OrgID == "" {
			return nil, errors.New("orgpolicy store: enrollment record has no org id")
		}
		if !seen[out.OrgID] {
			seen[out.OrgID] = true
			orgs = append(orgs, out.OrgID)
		}
	}
	sort.Strings(orgs)
	return orgs, nil
}

func (s *Store) loadOverrideRaw(peerAgentPub string) (*JoinOverrideRecord, []byte, error) {
	raw, err := s.kv.GetState(overrideKey(peerAgentPub))
	if err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: load join override: %w", err)
	}
	if raw == nil {
		return nil, nil, nil
	}
	var rec JoinOverrideRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: decode join override: %w", err)
	}
	return &rec, append([]byte(nil), raw...), nil
}

// LoadJoinOverride returns the stored override record for a peer.
func (s *Store) LoadJoinOverride(peerAgentPub string) (*JoinOverrideRecord, bool, error) {
	rec, _, err := s.loadOverrideRaw(peerAgentPub)
	if err != nil || rec == nil {
		return nil, false, err
	}
	return rec, true, nil
}

// ListJoinOverrides returns every stored override record, ordered by peer key.
func (s *Store) ListJoinOverrides() ([]JoinOverrideRecord, error) {
	l, err := s.lister()
	if err != nil {
		return nil, err
	}
	keys, err := l.ListStateKeys(overridePrefix, maxJoinOverrides)
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: list join overrides: %w", err)
	}
	out := make([]JoinOverrideRecord, 0, len(keys))
	for _, k := range keys {
		raw, gErr := s.kv.GetState(k)
		if gErr != nil {
			return nil, fmt.Errorf("orgpolicy store: load join override: %w", gErr)
		}
		if raw == nil {
			continue
		}
		var rec JoinOverrideRecord
		if uErr := json.Unmarshal(raw, &rec); uErr != nil {
			return nil, fmt.Errorf("orgpolicy store: decode join override: %w", uErr)
		}
		out = append(out, rec)
	}
	return out, nil
}

// ApplyJoinOverride stores a Root-signed override after verifying it under the
// current Root (rootID / rootPub). Ordering per peer_agent_pub:
//   - a higher revision replaces the stored override (its usage history is
//     kept, so the record of past uses survives a re-issue);
//   - the same revision and digest is a no-op;
//   - the same revision with a different digest, or a lower revision, is
//     refused (ErrJoinOverrideInvalid) and nothing is written.
//
// An override that is already expired at now is refused too: storing it could
// never authorize anything.
func (s *Store) ApplyJoinOverride(o *JoinOverride, rootID string, rootPub ed25519.PublicKey, now int64) (Decision, error) {
	if err := o.VerifyUnderRoot(rootID, rootPub); err != nil {
		return "", fmt.Errorf("%w: %v", ErrJoinOverrideInvalid, err)
	}
	if now >= o.ExpiresAt {
		return "", fmt.Errorf("%w: override is already expired", ErrJoinOverrideInvalid)
	}
	prev, raw, err := s.loadOverrideRaw(o.PeerAgentPub)
	if err != nil {
		return "", err
	}
	digest := o.Digest()
	rec := JoinOverrideRecord{Override: *o, Digest: digest, StoredAt: now}
	if prev != nil {
		switch {
		case o.Revision == prev.Override.Revision && digest == prev.Digest:
			return DecisionNoOpSameDigest, nil
		case o.Revision == prev.Override.Revision:
			return "", fmt.Errorf("%w: revision %d already holds a different override", ErrJoinOverrideInvalid, o.Revision)
		case o.Revision < prev.Override.Revision:
			return "", fmt.Errorf("%w: revision %d is below the stored revision %d", ErrJoinOverrideInvalid, o.Revision, prev.Override.Revision)
		}
		rec.TotalUses = prev.TotalUses
		rec.Uses = prev.Uses
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("orgpolicy store: encode join override: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{{Key: overrideKey(o.PeerAgentPub), Value: raw}},
		[]KVWrite{{Key: overrideKey(o.PeerAgentPub), Value: b}},
	)
	if err != nil {
		return "", err
	}
	if !applied {
		return "", ErrJoinOverrideStateConflict
	}
	return DecisionApplied, nil
}

// JoinPeerRequest is one JOIN gate check.
type JoinPeerRequest struct {
	Side         string // host | guest
	Stage        string // request | confirm
	SessionID    string
	PeerAgentPub string // 64 lowercase hex
	PeerChainID  string
	// RootCredentialID / RootPub identify the node's current Root credential.
	// Only needed when an override is consulted; empty fails any override.
	RootCredentialID string
	RootPub          ed25519.PublicKey
	Now              int64
}

// JoinPeerDecision is the gate's answer.
type JoinPeerDecision struct {
	FleetManaged bool     `json:"fleet_managed"`
	Allowed      bool     `json:"allowed"`
	Reason       string   `json:"reason,omitempty"`
	OrgIDs       []string `json:"org_ids,omitempty"`
	// OverrideRevision is set when an override, not the policy, let the peer in.
	OverrideRevision uint64 `json:"override_revision,omitempty"`
}

// AuthorizeJoinPeer is the manual QR JOIN gate. A node with no enrollment is
// allowed unchanged. A fleet-managed node allows the peer only if every
// enrolled org's accepted policy makes it eligible, or a current override
// signed by the current Root names this exact peer and chain; an override use
// is recorded before the decision is returned (if recording fails, the JOIN
// is refused). Any storage error is returned and must be treated as a refusal.
func (s *Store) AuthorizeJoinPeer(req JoinPeerRequest) (JoinPeerDecision, error) {
	orgs, err := s.FleetOrgs()
	if err != nil {
		return JoinPeerDecision{}, err
	}
	if len(orgs) == 0 {
		return JoinPeerDecision{Allowed: true}, nil
	}
	d := JoinPeerDecision{FleetManaged: true, OrgIDs: orgs}
	var denials []string
	for _, org := range orgs {
		st, body, lErr := s.LoadAcceptedPolicy(org)
		if lErr != nil {
			return JoinPeerDecision{}, lErr
		}
		if ok, why := PeerEligible(st, body, req.PeerAgentPub, req.Now); !ok {
			denials = append(denials, fmt.Sprintf("org %q: %s", org, why))
		}
	}
	if len(denials) == 0 {
		d.Allowed = true
		return d, nil
	}
	policyReason := "peer is not eligible under the org policy: " + strings.Join(denials, "; ")

	rec, raw, err := s.loadOverrideRaw(req.PeerAgentPub)
	if err != nil {
		return JoinPeerDecision{}, err
	}
	if rec == nil {
		d.Reason = policyReason
		return d, nil
	}
	o := rec.Override
	switch vErr := o.VerifyUnderRoot(req.RootCredentialID, req.RootPub); {
	case vErr != nil:
		d.Reason = policyReason + "; override refused: " + vErr.Error()
		return d, nil
	case !o.CurrentAt(req.Now):
		d.Reason = policyReason + "; override refused: not current (expired or not yet valid)"
		return d, nil
	case o.PeerAgentPub != req.PeerAgentPub || o.PeerChainID != req.PeerChainID:
		d.Reason = policyReason + "; override refused: it names a different peer chain"
		return d, nil
	}
	rec.TotalUses++
	rec.Uses = append(rec.Uses, JoinOverrideUse{
		Revision: o.Revision, Side: req.Side, Stage: req.Stage,
		SessionID: req.SessionID, PeerChainID: req.PeerChainID, At: req.Now,
	})
	if n := len(rec.Uses); n > MaxJoinOverrideUses {
		rec.Uses = append([]JoinOverrideUse(nil), rec.Uses[n-MaxJoinOverrideUses:]...)
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return JoinPeerDecision{}, fmt.Errorf("orgpolicy store: encode join override: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{{Key: overrideKey(req.PeerAgentPub), Value: raw}},
		[]KVWrite{{Key: overrideKey(req.PeerAgentPub), Value: b}},
	)
	if err != nil {
		return JoinPeerDecision{}, err
	}
	if !applied {
		return JoinPeerDecision{}, ErrJoinOverrideStateConflict
	}
	d.Allowed = true
	d.OverrideRevision = o.Revision
	d.Reason = "allowed by Root override: " + o.Reason
	return d, nil
}
