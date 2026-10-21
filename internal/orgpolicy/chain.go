package orgpolicy

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Decision is the outcome of admitting a fetched policy against the node's
// currently-accepted state. Only DecisionApplied changes state. The caller acts
// on each: persist on Applied, do nothing on NoOpSameDigest, and RAISE THE
// OPERATOR NOTICE on RejectedEquivocation (and, less urgently, log a rejected
// rollback).
type Decision string

const (
	// DecisionApplied: a strictly newer, validly-signed revision — accept and persist.
	DecisionApplied Decision = "applied"
	// DecisionNoOpSameDigest: the same revision with an identical digest — an
	// idempotent re-fetch. It MUST NOT extend expiry or authority; the caller does nothing.
	DecisionNoOpSameDigest Decision = "noop_same_digest"
	// DecisionRejectedRollback: a validly-signed but stale revision (< current).
	// Rollback protection; no state change.
	DecisionRejectedRollback Decision = "rejected_rollback"
	// DecisionRejectedEquivocation: the SAME revision carrying a DIFFERENT digest,
	// both validly signed by the authorized org key. This is the dangerous case —
	// the signer equivocated (or its key is compromised). Never a silent overwrite:
	// no state change, and the caller raises an owner-visible notice.
	DecisionRejectedEquivocation Decision = "rejected_equivocation"
)

// AcceptedState is the node's persisted high-water mark for an org's policy. The
// zero value (Revision == 0) means "no policy accepted yet". Persisted across
// restart and rotation so rollback/equivocation stays rejected after a restart.
type AcceptedState struct {
	OrgID        string `json:"org_id"`
	Scope        string `json:"scope"`
	Revision     uint64 `json:"revision"`
	Digest       string `json:"digest"`
	KeyEpoch     string `json:"key_epoch"`
	IssuerKeyID  string `json:"issuer_key_id"`
	RootKeyID    string `json:"root_key_id"`
	RootKeyEpoch string `json:"root_key_epoch"`
	// RotationChainJSON persists the signed root-to-current rotation history so an
	// in-flight policy signed by an overlapping predecessor remains verifiable
	// after the successor was accepted and across restart.
	RotationChainJSON string `json:"-"`
	// NotBefore/Expiry carry the accepted policy's validity window so the
	// per-operation offline/expiry gate (gate.go) can be evaluated from the
	// persisted state alone, without re-fetching the policy body.
	NotBefore int64 `json:"not_before"`
	Expiry    int64 `json:"expiry"` // 0 == no expiry
}

type acceptedStateWire struct {
	OrgID         string                `json:"org_id"`
	Scope         string                `json:"scope"`
	Revision      uint64                `json:"revision"`
	Digest        string                `json:"digest"`
	KeyEpoch      string                `json:"key_epoch"`
	IssuerKeyID   string                `json:"issuer_key_id"`
	RootKeyID     string                `json:"root_key_id"`
	RootKeyEpoch  string                `json:"root_key_epoch"`
	RotationChain []RotationAttestation `json:"rotation_chain,omitempty"`
	NotBefore     int64                 `json:"not_before"`
	Expiry        int64                 `json:"expiry"`
}

// MarshalJSON publishes the persisted rotation history as an array rather than
// exposing its internal canonical JSON string.
func (st AcceptedState) MarshalJSON() ([]byte, error) {
	chain, err := st.RotationHistory()
	if err != nil {
		return nil, err
	}
	return json.Marshal(acceptedStateWire{
		OrgID:         st.OrgID,
		Scope:         st.Scope,
		Revision:      st.Revision,
		Digest:        st.Digest,
		KeyEpoch:      st.KeyEpoch,
		IssuerKeyID:   st.IssuerKeyID,
		RootKeyID:     st.RootKeyID,
		RootKeyEpoch:  st.RootKeyEpoch,
		RotationChain: chain,
		NotBefore:     st.NotBefore,
		Expiry:        st.Expiry,
	})
}

// UnmarshalJSON restores both the current high-water mark and its complete
// rotation history from the wire representation.
func (st *AcceptedState) UnmarshalJSON(data []byte) error {
	var wire acceptedStateWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	chainJSON := ""
	if len(wire.RotationChain) > 0 {
		raw, err := json.Marshal(wire.RotationChain)
		if err != nil {
			return err
		}
		chainJSON = string(raw)
	}
	*st = AcceptedState{
		OrgID:             wire.OrgID,
		Scope:             wire.Scope,
		Revision:          wire.Revision,
		Digest:            wire.Digest,
		KeyEpoch:          wire.KeyEpoch,
		IssuerKeyID:       wire.IssuerKeyID,
		RootKeyID:         wire.RootKeyID,
		RootKeyEpoch:      wire.RootKeyEpoch,
		RotationChainJSON: chainJSON,
		NotBefore:         wire.NotBefore,
		Expiry:            wire.Expiry,
	}
	return nil
}

func stateFrom(p *Policy) AcceptedState {
	return NewAcceptedStateWithAuthority(p, PolicyAuthority{
		AuthorizedKey: mustPolicyKey(p),
		RootKeyID:     p.IssuerKeyID,
		RootKeyEpoch:  p.KeyEpoch,
	})
}

func mustPolicyKey(p *Policy) ed25519.PublicKey {
	raw, _ := hex.DecodeString(p.IssuerKeyID)
	return ed25519.PublicKey(raw)
}

// PolicyAuthority is the fully resolved authority for one delivered policy.
// RotationChain is the complete root-to-current chain, not only the delta in
// the current request.
type PolicyAuthority struct {
	AuthorizedKey ed25519.PublicKey
	RootKeyID     string
	RootKeyEpoch  string
	RotationChain []RotationAttestation
}

// NewAcceptedStateWithAuthority builds the high-water mark for a policy under
// the supplied trust root and complete rotation history.
func NewAcceptedStateWithAuthority(p *Policy, authority PolicyAuthority) AcceptedState {
	chainJSON := ""
	if len(authority.RotationChain) > 0 {
		raw, _ := json.Marshal(authority.RotationChain)
		chainJSON = string(raw)
	}
	return AcceptedState{
		OrgID:             p.OrgID,
		Scope:             p.Scope,
		Revision:          p.Revision,
		Digest:            p.Digest(),
		KeyEpoch:          p.KeyEpoch,
		IssuerKeyID:       p.IssuerKeyID,
		RootKeyID:         authority.RootKeyID,
		RootKeyEpoch:      authority.RootKeyEpoch,
		RotationChainJSON: chainJSON,
		NotBefore:         p.NotBefore,
		Expiry:            p.Expiry,
	}
}

// RotationHistory decodes the persisted root-to-current rotation chain.
func (st AcceptedState) RotationHistory() ([]RotationAttestation, error) {
	if st.RotationChainJSON == "" || st.RotationChainJSON == "null" {
		return nil, nil
	}
	var chain []RotationAttestation
	if err := json.Unmarshal([]byte(st.RotationChainJSON), &chain); err != nil {
		return nil, fmt.Errorf("org policy: decode rotation history: %w", err)
	}
	return chain, nil
}

// Admit decides whether p may replace prev, given the signer the caller has
// already resolved as authorized (the pinned org key, or a rotated key proven by
// a verified attestation chain — see ResolveSigner / AuthorizedKeys in
// rotation.go). Admit is PURE: it performs no IO and raises no alarm. It verifies
// p fail-closed under authorizedPub BEFORE any ordering decision, so a rollback
// or equivocation verdict always concerns a genuinely org-signed object, never a
// forgery.
//
// On DecisionApplied the returned AcceptedState is what the caller persists (in
// one atomic write together with any enrollment/consume record). On every other
// decision the returned state equals prev unchanged.
func Admit(prev AcceptedState, p *Policy, authorizedPub ed25519.PublicKey) (AcceptedState, Decision, error) {
	authority := PolicyAuthority{AuthorizedKey: authorizedPub}
	if prev.Revision == 0 {
		authority.RootKeyID = hex.EncodeToString(authorizedPub)
		authority.RootKeyEpoch = p.KeyEpoch
	} else {
		authority.RootKeyID = prev.RootKeyID
		authority.RootKeyEpoch = prev.RootKeyEpoch
		chain, err := prev.RotationHistory()
		if err != nil {
			return prev, "", err
		}
		authority.RotationChain = chain
	}
	return AdmitWithAuthority(prev, p, authority)
}

// AdmitWithAuthority performs the same ordering decision with persisted trust
// root and complete rotation history.
func AdmitWithAuthority(prev AcceptedState, p *Policy, authority PolicyAuthority) (AcceptedState, Decision, error) {
	authorizedPub := authority.AuthorizedKey
	if err := p.Verify(authorizedPub); err != nil {
		return prev, "", err
	}
	// First policy ever: accept unconditionally (subject to the Verify above).
	if prev.Revision == 0 {
		return NewAcceptedStateWithAuthority(p, authority), DecisionApplied, nil
	}
	if p.OrgID != prev.OrgID {
		return prev, "", fmt.Errorf("org policy: org_id %q does not match accepted %q", p.OrgID, prev.OrgID)
	}
	if p.Scope != prev.Scope {
		return prev, "", fmt.Errorf("org policy: scope %q does not match accepted %q", p.Scope, prev.Scope)
	}
	switch {
	case p.Revision < prev.Revision:
		return prev, DecisionRejectedRollback, nil
	case p.Revision == prev.Revision:
		if p.Digest() == prev.Digest {
			return prev, DecisionNoOpSameDigest, nil
		}
		return prev, DecisionRejectedEquivocation, nil
	default:
		return NewAcceptedStateWithAuthority(p, authority), DecisionApplied, nil
	}
}

// EquivocationNotice is the owner-visible payload the caller persists + broadcasts
// when Admit returns DecisionRejectedEquivocation. It names the LAYER and the
// conflict — org, revision, the two digests, the issuer epoch — never a peer and
// never raw bytes. The caller maps this onto the operator-notice store and the
// Cerebrum SSE event.
type EquivocationNotice struct {
	OrgID          string `json:"org_id"`
	Revision       uint64 `json:"revision"`
	AcceptedDigest string `json:"accepted_digest"`
	ConflictDigest string `json:"conflict_digest"`
	IssuerKeyID    string `json:"issuer_key_id"`
	KeyEpoch       string `json:"key_epoch"`
}

// NewEquivocationNotice builds the notice for a conflicting policy p against the
// accepted state prev. Caller invokes it only on DecisionRejectedEquivocation.
func NewEquivocationNotice(prev AcceptedState, p *Policy) EquivocationNotice {
	return EquivocationNotice{
		OrgID:          prev.OrgID,
		Revision:       prev.Revision,
		AcceptedDigest: prev.Digest,
		ConflictDigest: p.Digest(),
		IssuerKeyID:    p.IssuerKeyID,
		KeyEpoch:       p.KeyEpoch,
	}
}
