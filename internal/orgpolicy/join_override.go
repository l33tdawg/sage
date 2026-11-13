package orgpolicy

// Explicit local Root override for fleet-managed QR JOIN (RFC #443 Phase A2).
//
// The organization policy is the normal eligibility source. When it denies a
// peer, the node owner may make one explicit, expiring exception signed by the
// current CEREBRUM Root credential. Root handover invalidates every older
// override without rewriting it: every use re-resolves the current Root and
// verifies the override under that exact credential. Successful uses are kept
// in a bounded, idempotent audit history.

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
	"time"
	"unicode/utf8"
)

const (
	// JoinOverrideSchemaID is bound into every Root override signature.
	JoinOverrideSchemaID = "sage.org-join-override.v1"

	joinOverrideDomainSep = "sage-org-join-override-v1\x00"

	// MaxJoinOverrideWindowSeconds bounds the exception lifetime.
	MaxJoinOverrideWindowSeconds = 30 * 24 * 60 * 60
	// MaxJoinOverrideReasonBytes bounds the human audit reason.
	MaxJoinOverrideReasonBytes = 1024
	// MaxJoinOverrideRequestBytes bounds one signed override create/update.
	MaxJoinOverrideRequestBytes = 16 * 1024
	// MaxJoinOverrideUseHistory bounds each peer override's retained audit
	// history. The oldest entries are dropped only after the cap is reached.
	MaxJoinOverrideUseHistory = 1024
)

var joinOverrideChainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

var (
	ErrJoinOverrideStateChanged = errors.New("org join override: state changed concurrently")
	ErrJoinOverrideInvalid      = errors.New("org join override: invalid")
)

// JoinOverride is one explicit Root exception for an exact peer identity.
// Sig is an ed25519 signature by RootCredentialID over CanonicalBytes().
type JoinOverride struct {
	Schema           string `json:"schema"`
	OrgID            string `json:"org_id"`
	PeerAgentPub     string `json:"peer_agent_pub"`
	PeerChainID      string `json:"peer_chain_id"`
	Reason           string `json:"reason"`
	IssuedAt         int64  `json:"issued_at"`
	ExpiresAt        int64  `json:"expires_at"`
	Revision         uint64 `json:"revision"`
	RootCredentialID string `json:"root_credential_id"`
	Sig              string `json:"sig"`
}

// CanonicalBytes is the exact signed encoding.
func (o *JoinOverride) CanonicalBytes() []byte {
	b := make([]byte, 0, 256)
	b = append(b, []byte(joinOverrideDomainSep)...)
	b = lp(b, o.Schema)
	b = lp(b, o.OrgID)
	b = lp(b, o.PeerAgentPub)
	b = lp(b, o.PeerChainID)
	b = lp(b, o.Reason)
	b = i64(b, o.IssuedAt)
	b = i64(b, o.ExpiresAt)
	b = u64(b, o.Revision)
	b = lp(b, o.RootCredentialID)
	return b
}

// Digest identifies the signed override content, excluding Sig.
func (o *JoinOverride) Digest() string {
	sum := sha256.Sum256(o.CanonicalBytes())
	return hex.EncodeToString(sum[:])
}

func (o *JoinOverride) validate() error {
	if o.Schema != JoinOverrideSchemaID {
		return fmt.Errorf("%w: schema %q != %q", ErrJoinOverrideInvalid, o.Schema, JoinOverrideSchemaID)
	}
	if strings.TrimSpace(o.OrgID) == "" || !utf8.ValidString(o.OrgID) {
		return fmt.Errorf("%w: org_id is required", ErrJoinOverrideInvalid)
	}
	if !isAgentPubHex(o.PeerAgentPub) {
		return fmt.Errorf("%w: peer_agent_pub must be 64 lowercase hex", ErrJoinOverrideInvalid)
	}
	if !joinOverrideChainIDPattern.MatchString(o.PeerChainID) || o.PeerChainID == "." || o.PeerChainID == ".." {
		return fmt.Errorf("%w: invalid peer_chain_id", ErrJoinOverrideInvalid)
	}
	reason := strings.TrimSpace(o.Reason)
	if reason == "" || len(o.Reason) > MaxJoinOverrideReasonBytes || !utf8.ValidString(o.Reason) {
		return fmt.Errorf("%w: reason must be non-empty UTF-8 up to %d bytes", ErrJoinOverrideInvalid, MaxJoinOverrideReasonBytes)
	}
	for _, r := range o.Reason {
		if r < 0x20 && r != '\t' && r != '\n' || r == 0x7f {
			return fmt.Errorf("%w: reason contains a control character", ErrJoinOverrideInvalid)
		}
	}
	if o.IssuedAt <= 0 || o.ExpiresAt <= o.IssuedAt {
		return fmt.Errorf("%w: expires_at must follow issued_at", ErrJoinOverrideInvalid)
	}
	if o.ExpiresAt-o.IssuedAt > MaxJoinOverrideWindowSeconds {
		return fmt.Errorf("%w: validity window exceeds 30 days", ErrJoinOverrideInvalid)
	}
	if o.Revision == 0 {
		return fmt.Errorf("%w: revision must be >= 1", ErrJoinOverrideInvalid)
	}
	if !isAgentPubHex(o.RootCredentialID) {
		return fmt.Errorf("%w: root_credential_id must be 64 lowercase hex", ErrJoinOverrideInvalid)
	}
	return nil
}

// Sign sets the schema and Root credential from the signing key and signs the
// canonical fields. IssuedAt and ExpiresAt must already be set by the caller.
func (o *JoinOverride) Sign(rootKey ed25519.PrivateKey) error {
	pub, ok := rootKey.Public().(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: invalid Root signing key", ErrJoinOverrideInvalid)
	}
	o.Schema = JoinOverrideSchemaID
	o.RootCredentialID = hex.EncodeToString(pub)
	if err := o.validate(); err != nil {
		return err
	}
	o.Sig = hex.EncodeToString(ed25519.Sign(rootKey, o.CanonicalBytes()))
	return nil
}

// Verify checks structure and the signature under the exact current Root key.
// Freshness is checked separately by CurrentAt.
func (o *JoinOverride) Verify(rootPub ed25519.PublicKey) error {
	if len(rootPub) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: invalid Root key", ErrJoinOverrideInvalid)
	}
	if o.RootCredentialID != hex.EncodeToString(rootPub) {
		return fmt.Errorf("%w: override is not signed by the current Root credential", ErrJoinOverrideInvalid)
	}
	if err := o.validate(); err != nil {
		return err
	}
	sig, err := hex.DecodeString(o.Sig)
	if err != nil || len(sig) != ed25519.SignatureSize ||
		!ed25519.Verify(rootPub, o.CanonicalBytes(), sig) {
		return fmt.Errorf("%w: Root signature invalid", ErrJoinOverrideInvalid)
	}
	return nil
}

// CurrentAt reports whether the override is active at now.
func (o *JoinOverride) CurrentAt(now int64) bool {
	return now >= o.IssuedAt && now < o.ExpiresAt
}

func joinOverrideKey(orgID, peerAgentPub, peerChainID string) string {
	return keyPrefix + "join-override/" + seg(orgID) + "/" + seg(peerAgentPub) + "/" + seg(peerChainID)
}

func joinOverrideUseKey(orgID, peerAgentPub, peerChainID string) string {
	return keyPrefix + "join-override-use/" + seg(orgID) + "/" + seg(peerAgentPub) + "/" + seg(peerChainID)
}

// KeyLister is the optional read-only range surface backed by the node-local
// KV. Production SQLite implements it; a KV without listing can still serve
// every exact-key operation but cannot enumerate fleet-managed orgs/overrides.
type KeyLister interface {
	ListKeys(prefix string) ([]string, error)
}

// ManagedOrganizations returns every org with a durable EnrollmentOutcome.
// The enrollment row, not a policy alone, is the fleet-management marker.
func (s *Store) ManagedOrganizations() ([]string, error) {
	lister, ok := s.kv.(KeyLister)
	if !ok {
		return nil, errors.New("orgpolicy store: state backend cannot enumerate enrollment outcomes")
	}
	keys, err := lister.ListKeys(keyPrefix + "enroll/")
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: list enrollment outcomes: %w", err)
	}
	orgs := make(map[string]struct{})
	for _, key := range keys {
		raw, err := s.kv.GetState(key)
		if err != nil {
			return nil, fmt.Errorf("orgpolicy store: load enrollment outcome: %w", err)
		}
		if raw == nil {
			continue
		}
		var outcome EnrollmentOutcome
		if err := json.Unmarshal(raw, &outcome); err != nil {
			return nil, fmt.Errorf("orgpolicy store: decode enrollment outcome: %w", err)
		}
		if outcome.OrgID != "" {
			orgs[outcome.OrgID] = struct{}{}
		}
	}
	out := make([]string, 0, len(orgs))
	for org := range orgs {
		out = append(out, org)
	}
	sort.Strings(out)
	return out, nil
}

// LoadJoinOverride returns the current override for one exact peer.
func (s *Store) LoadJoinOverride(orgID, peerAgentPub, peerChainID string) (*JoinOverride, error) {
	raw, err := s.kv.GetState(joinOverrideKey(orgID, peerAgentPub, peerChainID))
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: load join override: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	var out JoinOverride
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("orgpolicy store: decode join override: %w", err)
	}
	return &out, nil
}

// ApplyJoinOverride verifies an override under rootPub and persists monotonic
// revisions for its exact peer identity.
func (s *Store) ApplyJoinOverride(o JoinOverride, rootPub ed25519.PublicKey) (JoinOverride, Decision, error) {
	if err := o.Verify(rootPub); err != nil {
		return JoinOverride{}, "", err
	}
	raw, err := s.kv.GetState(joinOverrideKey(o.OrgID, o.PeerAgentPub, o.PeerChainID))
	if err != nil {
		return JoinOverride{}, "", fmt.Errorf("orgpolicy store: load join override: %w", err)
	}
	if raw != nil {
		var prev JoinOverride
		if decodeErr := json.Unmarshal(raw, &prev); decodeErr != nil {
			return JoinOverride{}, "", fmt.Errorf("orgpolicy store: decode join override: %w", decodeErr)
		}
		switch {
		case o.Revision < prev.Revision:
			return prev, DecisionRejectedRollback, nil
		case o.Revision == prev.Revision && o.Digest() != prev.Digest():
			return prev, DecisionRejectedEquivocation, nil
		case o.Revision == prev.Revision:
			return prev, DecisionNoOpSameDigest, nil
		}
	}
	next, err := json.Marshal(o)
	if err != nil {
		return JoinOverride{}, "", fmt.Errorf("orgpolicy store: encode join override: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{{Key: joinOverrideKey(o.OrgID, o.PeerAgentPub, o.PeerChainID), Value: raw}},
		[]KVWrite{{Key: joinOverrideKey(o.OrgID, o.PeerAgentPub, o.PeerChainID), Value: next}},
	)
	if err != nil {
		return JoinOverride{}, "", err
	}
	if !applied {
		currentRaw, loadErr := s.kv.GetState(joinOverrideKey(o.OrgID, o.PeerAgentPub, o.PeerChainID))
		if loadErr != nil {
			return JoinOverride{}, "", loadErr
		}
		if currentRaw == nil {
			return JoinOverride{}, "", ErrJoinOverrideStateChanged
		}
		var current JoinOverride
		if decodeErr := json.Unmarshal(currentRaw, &current); decodeErr != nil {
			return JoinOverride{}, "", fmt.Errorf("orgpolicy store: decode join override: %w", decodeErr)
		}
		switch {
		case o.Revision < current.Revision:
			return current, DecisionRejectedRollback, nil
		case o.Revision == current.Revision && o.Digest() != current.Digest():
			return current, DecisionRejectedEquivocation, nil
		case o.Revision == current.Revision:
			return current, DecisionNoOpSameDigest, nil
		default:
			return JoinOverride{}, "", ErrJoinOverrideStateChanged
		}
	}
	return o, DecisionApplied, nil
}

// ListJoinOverrides returns all current overrides for orgID in peer order.
func (s *Store) ListJoinOverrides(orgID string) ([]JoinOverride, error) {
	lister, ok := s.kv.(KeyLister)
	if !ok {
		return nil, errors.New("orgpolicy store: state backend cannot enumerate join overrides")
	}
	prefix := keyPrefix + "join-override/" + seg(orgID) + "/"
	keys, err := lister.ListKeys(prefix)
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: list join overrides: %w", err)
	}
	out := make([]JoinOverride, 0, len(keys))
	for _, key := range keys {
		raw, err := s.kv.GetState(key)
		if err != nil {
			return nil, fmt.Errorf("orgpolicy store: load join override: %w", err)
		}
		if raw == nil {
			continue
		}
		var o JoinOverride
		if err := json.Unmarshal(raw, &o); err != nil {
			return nil, fmt.Errorf("orgpolicy store: decode join override: %w", err)
		}
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PeerAgentPub != out[j].PeerAgentPub {
			return out[i].PeerAgentPub < out[j].PeerAgentPub
		}
		return out[i].PeerChainID < out[j].PeerChainID
	})
	return out, nil
}

// JoinOverrideUse is one audit row for a JOIN gate that admitted this peer
// because of the override rather than ordinary policy eligibility.
type JoinOverrideUse struct {
	UsedAt           int64  `json:"used_at"`
	OrgID            string `json:"org_id"`
	PeerAgentPub     string `json:"peer_agent_pub"`
	PeerChainID      string `json:"peer_chain_id"`
	OverrideRevision uint64 `json:"override_revision"`
	RootCredentialID string `json:"root_credential_id"`
	SessionID        string `json:"session_id"`
	Role             string `json:"role"`
	Phase            string `json:"phase"`
}

func (u JoinOverrideUse) validate() error {
	if u.UsedAt <= 0 || u.OrgID == "" || !isAgentPubHex(u.PeerAgentPub) ||
		!joinOverrideChainIDPattern.MatchString(u.PeerChainID) || u.OverrideRevision == 0 ||
		!isAgentPubHex(u.RootCredentialID) || strings.TrimSpace(u.SessionID) == "" {
		return fmt.Errorf("%w: incomplete usage row", ErrJoinOverrideInvalid)
	}
	if u.Role != "host" && u.Role != "guest" {
		return fmt.Errorf("%w: role must be host or guest", ErrJoinOverrideInvalid)
	}
	if u.Phase != "request" && u.Phase != "approval" && u.Phase != "commit" {
		return fmt.Errorf("%w: phase must be request, approval, or commit", ErrJoinOverrideInvalid)
	}
	return nil
}

func (s *Store) loadJoinOverrideUsageRaw(orgID, peerAgentPub, peerChainID string) ([]JoinOverrideUse, []byte, error) {
	raw, err := s.kv.GetState(joinOverrideUseKey(orgID, peerAgentPub, peerChainID))
	if err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: load join override usage: %w", err)
	}
	if raw == nil {
		return nil, nil, nil
	}
	var out []JoinOverrideUse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: decode join override usage: %w", err)
	}
	return out, raw, nil
}

// LoadJoinOverrideUsage returns the retained use history for one peer.
func (s *Store) LoadJoinOverrideUsage(orgID, peerAgentPub, peerChainID string) ([]JoinOverrideUse, error) {
	out, _, err := s.loadJoinOverrideUsageRaw(orgID, peerAgentPub, peerChainID)
	if out == nil && err == nil {
		out = []JoinOverrideUse{}
	}
	return out, err
}

// RecordJoinOverrideUse appends one successful override use. The event is
// idempotent for (session_id, role, phase, revision), and the write only lands
// if the exact override revision that authorized the decision is still current.
func (s *Store) RecordJoinOverrideUse(expected JoinOverride, use JoinOverrideUse) error {
	if use.OrgID != expected.OrgID || use.PeerAgentPub != expected.PeerAgentPub ||
		use.PeerChainID != expected.PeerChainID || use.OverrideRevision != expected.Revision ||
		use.RootCredentialID != expected.RootCredentialID {
		return fmt.Errorf("%w: usage row does not match the authorizing override", ErrJoinOverrideInvalid)
	}
	if err := use.validate(); err != nil {
		return err
	}
	for attempt := 0; attempt < 3; attempt++ {
		currentRaw, err := s.kv.GetState(joinOverrideKey(use.OrgID, use.PeerAgentPub, use.PeerChainID))
		if err != nil {
			return fmt.Errorf("orgpolicy store: load join override: %w", err)
		}
		if currentRaw == nil {
			return ErrJoinOverrideStateChanged
		}
		var current JoinOverride
		if decodeErr := json.Unmarshal(currentRaw, &current); decodeErr != nil {
			return fmt.Errorf("orgpolicy store: decode join override: %w", decodeErr)
		}
		if current.Revision != expected.Revision || current.Digest() != expected.Digest() {
			return ErrJoinOverrideStateChanged
		}
		history, historyRaw, err := s.loadJoinOverrideUsageRaw(use.OrgID, use.PeerAgentPub, use.PeerChainID)
		if err != nil {
			return err
		}
		for _, prior := range history {
			if prior.SessionID == use.SessionID && prior.Role == use.Role &&
				prior.Phase == use.Phase && prior.OverrideRevision == use.OverrideRevision {
				return nil
			}
		}
		history = append(history, use)
		if len(history) > MaxJoinOverrideUseHistory {
			history = append([]JoinOverrideUse(nil), history[len(history)-MaxJoinOverrideUseHistory:]...)
		}
		next, err := json.Marshal(history)
		if err != nil {
			return fmt.Errorf("orgpolicy store: encode join override usage: %w", err)
		}
		applied, err := s.kv.CompareAndSetStatesAtomic(
			[]KVCondition{
				{Key: joinOverrideKey(use.OrgID, use.PeerAgentPub, use.PeerChainID), Value: currentRaw},
				{Key: joinOverrideUseKey(use.OrgID, use.PeerAgentPub, use.PeerChainID), Value: historyRaw},
			},
			[]KVWrite{{
				Key:   joinOverrideUseKey(use.OrgID, use.PeerAgentPub, use.PeerChainID),
				Value: next,
			}},
		)
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
	}
	return ErrJoinOverrideStateChanged
}

// CurrentRoot is the exact committed Root credential used to verify an
// override at decision time.
type CurrentRoot struct {
	CredentialID string
	PublicKey    ed25519.PublicKey
}

// JoinGateCheck is one gate decision at a specific QR JOIN phase.
type JoinGateCheck struct {
	PeerAgentPub string
	PeerChainID  string
	SessionID    string
	Role         string
	Phase        string
}

// JoinGateDecision reports whether fleet policy applies and whether this check
// was admitted normally, by override, or not at all.
type JoinGateDecision struct {
	Managed          bool   `json:"managed"`
	Allowed          bool   `json:"allowed"`
	Reason           string `json:"reason,omitempty"`
	OrgID            string `json:"org_id,omitempty"`
	OverrideRevision uint64 `json:"override_revision,omitempty"`
}

// JoinGate evaluates the exact same policy/override path at every JOIN phase.
// Nodes with no EnrollmentOutcome are deliberately untouched.
type JoinGate struct {
	Store       *Store
	CurrentRoot func() (CurrentRoot, error)
	Now         func() int64
}

// Evaluate applies PeerEligible across every fleet-managed org, then considers
// an exact current Root override. Any state read or audit-write failure is
// returned to the JOIN caller and fails closed.
func (g JoinGate) Evaluate(check JoinGateCheck) (JoinGateDecision, error) {
	if g.Store == nil {
		return JoinGateDecision{}, errors.New("org join gate: store unavailable")
	}
	if !isAgentPubHex(check.PeerAgentPub) ||
		!joinOverrideChainIDPattern.MatchString(check.PeerChainID) ||
		strings.TrimSpace(check.SessionID) == "" ||
		(check.Role != "host" && check.Role != "guest") ||
		(check.Phase != "request" && check.Phase != "approval" && check.Phase != "commit") {
		return JoinGateDecision{}, errors.New("org join gate: invalid decision context")
	}
	orgs, err := g.Store.ManagedOrganizations()
	if err != nil {
		return JoinGateDecision{}, err
	}
	if len(orgs) == 0 {
		return JoinGateDecision{Managed: false, Allowed: true}, nil
	}
	now := time.Now().Unix()
	if g.Now != nil {
		now = g.Now()
	}
	firstDeny := "peer is not eligible under this node's fleet policy"
	for _, orgID := range orgs {
		accepted, policy, err := g.Store.LoadAcceptedPolicy(orgID)
		if err != nil {
			return JoinGateDecision{}, err
		}
		if accepted.Revision == 0 {
			continue
		}
		ok, reason := PeerEligible(accepted, policy, check.PeerAgentPub, now)
		if ok {
			return JoinGateDecision{Managed: true, Allowed: true, OrgID: orgID}, nil
		}
		if reason != "" {
			firstDeny = reason
		}
	}

	var root *CurrentRoot
	overrideReason := ""
	for _, orgID := range orgs {
		override, err := g.Store.LoadJoinOverride(orgID, check.PeerAgentPub, check.PeerChainID)
		if err != nil {
			return JoinGateDecision{}, err
		}
		if override == nil {
			continue
		}
		if !override.CurrentAt(now) {
			overrideReason = "Root override is expired or not yet valid"
			continue
		}
		if root == nil {
			if g.CurrentRoot == nil {
				return JoinGateDecision{}, errors.New("org join gate: current Root resolver unavailable")
			}
			resolved, err := g.CurrentRoot()
			if err != nil {
				return JoinGateDecision{}, err
			}
			root = &resolved
		}
		if override.RootCredentialID != root.CredentialID {
			overrideReason = "Root override is not signed by the current Root credential"
			continue
		}
		if err := override.Verify(root.PublicKey); err != nil {
			overrideReason = "Root override signature is not valid under the current Root credential"
			continue
		}
		if err := g.Store.RecordJoinOverrideUse(*override, JoinOverrideUse{
			UsedAt: now, OrgID: orgID, PeerAgentPub: check.PeerAgentPub,
			PeerChainID: check.PeerChainID, OverrideRevision: override.Revision,
			RootCredentialID: root.CredentialID, SessionID: check.SessionID,
			Role: check.Role, Phase: check.Phase,
		}); err != nil {
			return JoinGateDecision{}, err
		}
		return JoinGateDecision{
			Managed: true, Allowed: true, OrgID: orgID,
			OverrideRevision: override.Revision,
		}, nil
	}
	if overrideReason != "" {
		firstDeny = overrideReason
	}
	return JoinGateDecision{Managed: true, Allowed: false, Reason: firstDeny}, nil
}
