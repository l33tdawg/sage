package orgpolicy

import (
	"crypto/ed25519"
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
	OrgID       string `json:"org_id"`
	Revision    uint64 `json:"revision"`
	Digest      string `json:"digest"`
	KeyEpoch    string `json:"key_epoch"`
	IssuerKeyID string `json:"issuer_key_id"`
	// NotBefore/Expiry carry the accepted policy's validity window so the
	// per-operation offline/expiry gate (gate.go) can be evaluated from the
	// persisted state alone, without re-fetching the policy body.
	NotBefore int64 `json:"not_before"`
	Expiry    int64 `json:"expiry"` // 0 == no expiry
}

func stateFrom(p *Policy) AcceptedState {
	return AcceptedState{
		OrgID:       p.OrgID,
		Revision:    p.Revision,
		Digest:      p.Digest(),
		KeyEpoch:    p.KeyEpoch,
		IssuerKeyID: p.IssuerKeyID,
		NotBefore:   p.NotBefore,
		Expiry:      p.Expiry,
	}
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
	if err := p.Verify(authorizedPub); err != nil {
		return prev, "", err
	}
	// First policy ever: accept unconditionally (subject to the Verify above).
	if prev.Revision == 0 {
		return stateFrom(p), DecisionApplied, nil
	}
	if p.OrgID != prev.OrgID {
		return prev, "", fmt.Errorf("org policy: org_id %q does not match accepted %q", p.OrgID, prev.OrgID)
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
		return stateFrom(p), DecisionApplied, nil
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
