package orgpolicy

// Durable owner envelope state (RFC #443 Phase B, contract v2 §4).
//
// One compare-and-set transaction per admission writes the current envelope
// record and a per-revision history entry, conditioned on the exact accepted
// policy bytes the envelope was checked against and on the envelope record the
// caller read. An equivocation (same revision, different digest) is refused and
// leaves a durable notice. Restart reloads all of it.

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// ErrEnvelopeRollback means a lower revision than the stored one was offered.
var ErrEnvelopeRollback = errors.New("link envelope: revision is below the stored envelope")

// ErrEnvelopeEquivocation means the same revision was offered with a different
// digest. The stored envelope is kept and a notice is persisted.
var ErrEnvelopeEquivocation = errors.New("link envelope: revision already holds a different envelope")

// ErrEnvelopePolicyChanged means the accepted org policy is not the one the
// caller checked the envelope against. Nothing is written.
var ErrEnvelopePolicyChanged = errors.New("link envelope: accepted org policy changed")

func envelopeKey(orgID string) string { return keyPrefix + "envelope/" + seg(orgID) }
func envelopeHistKey(orgID string, rev uint64) string {
	return keyPrefix + "envelope-hist/" + seg(orgID) + "/" + strconv.FormatUint(rev, 10)
}
func envelopeNoticeKey(orgID string) string { return keyPrefix + "envelope-notice/" + seg(orgID) }

// EnvelopeRecord is the stored envelope with its digest.
type EnvelopeRecord struct {
	Envelope LinkEnvelope `json:"envelope"`
	Digest   string       `json:"digest"`
	StoredAt int64        `json:"stored_at"`
}

// EnvelopeNotice is the durable record of an envelope equivocation.
type EnvelopeNotice struct {
	OrgID          string `json:"org_id"`
	Revision       uint64 `json:"revision"`
	AcceptedDigest string `json:"accepted_digest"`
	ConflictDigest string `json:"conflict_digest"`
	At             int64  `json:"at"`
}

func (s *Store) loadEnvelopeRaw(orgID string) (*EnvelopeRecord, []byte, error) {
	raw, err := s.kv.GetState(envelopeKey(orgID))
	if err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: load envelope: %w", err)
	}
	if raw == nil {
		return nil, nil, nil
	}
	var rec EnvelopeRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: decode envelope: %w", err)
	}
	return &rec, append([]byte(nil), raw...), nil
}

// LoadEnvelope returns the current envelope record for an org.
func (s *Store) LoadEnvelope(orgID string) (*EnvelopeRecord, bool, error) {
	rec, _, err := s.loadEnvelopeRaw(orgID)
	if err != nil || rec == nil {
		return nil, false, err
	}
	return rec, true, nil
}

// LoadEnvelopeHistory returns the envelope record stored at one revision.
func (s *Store) LoadEnvelopeHistory(orgID string, rev uint64) (*EnvelopeRecord, bool, error) {
	raw, err := s.kv.GetState(envelopeHistKey(orgID, rev))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load envelope history: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var rec EnvelopeRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode envelope history: %w", err)
	}
	return &rec, true, nil
}

// LoadEnvelopeNotice returns the persisted equivocation notice for an org.
func (s *Store) LoadEnvelopeNotice(orgID string) (*EnvelopeNotice, bool, error) {
	raw, err := s.kv.GetState(envelopeNoticeKey(orgID))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load envelope notice: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var n EnvelopeNotice
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode envelope notice: %w", err)
	}
	return &n, true, nil
}

// ApplyEnvelope admits a Root-signed owner envelope for e.OrgID. policy must be
// the accepted policy state the caller read; the write is conditioned on its
// exact stored bytes, so a policy rotation in between refuses with nothing
// written.
//
// Checks: verified under the current Root (rootID / rootPub), not already
// expired at now, org_root_key_id equals the accepted policy's root key.
// Ordering per org:
//   - no stored envelope: the revision must be 1 (prev_digest empty);
//   - higher revision: prev_digest must be the stored envelope's digest;
//   - same revision and digest: no-op;
//   - same revision, different digest: refused, notice persisted
//     (ErrEnvelopeEquivocation);
//   - lower revision: refused (ErrEnvelopeRollback).
func (s *Store) ApplyEnvelope(e *LinkEnvelope, rootID string, rootPub ed25519.PublicKey, policy AcceptedState, now int64) (Decision, *EnvelopeNotice, error) {
	if err := e.VerifyUnderRoot(rootID, rootPub); err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrEnvelopeInvalid, err)
	}
	if policy.Revision == 0 || policy.OrgID != e.OrgID {
		return "", nil, fmt.Errorf("%w: no accepted org policy for %q", ErrEnvelopeInvalid, e.OrgID)
	}
	if e.OrgRootKeyID != policy.RootKeyID {
		return "", nil, fmt.Errorf("%w: org_root_key_id does not match the accepted policy's root key", ErrEnvelopeInvalid)
	}
	if now >= e.ExpiresAt {
		return "", nil, fmt.Errorf("%w: envelope is already expired", ErrEnvelopeInvalid)
	}
	current, policyRaw, err := s.loadAcceptedWithRaw(e.OrgID)
	if err != nil {
		return "", nil, err
	}
	if current != policy {
		return "", nil, ErrEnvelopePolicyChanged
	}
	prev, raw, err := s.loadEnvelopeRaw(e.OrgID)
	if err != nil {
		return "", nil, err
	}
	digest := e.Digest()
	policyCond := KVCondition{Key: acceptedKey(e.OrgID), Value: policyRaw}
	envCond := KVCondition{Key: envelopeKey(e.OrgID), Value: raw}
	if prev == nil {
		if e.Revision != 1 {
			return "", nil, fmt.Errorf("%w: the first envelope for an org must be revision 1", ErrEnvelopeInvalid)
		}
	} else {
		switch {
		case e.Revision == prev.Envelope.Revision && digest == prev.Digest:
			return DecisionNoOpSameDigest, nil, nil
		case e.Revision == prev.Envelope.Revision:
			n := EnvelopeNotice{
				OrgID: e.OrgID, Revision: e.Revision,
				AcceptedDigest: prev.Digest, ConflictDigest: digest, At: now,
			}
			b, mErr := json.Marshal(n)
			if mErr != nil {
				return "", nil, fmt.Errorf("orgpolicy store: encode envelope notice: %w", mErr)
			}
			applied, wErr := s.kv.CompareAndSetStatesAtomic(
				[]KVCondition{policyCond, envCond},
				[]KVWrite{{Key: envelopeNoticeKey(e.OrgID), Value: b}},
			)
			if wErr != nil {
				return "", nil, wErr
			}
			if !applied {
				return "", nil, s.envelopeCASFailure(e.OrgID, policyRaw)
			}
			return DecisionRejectedEquivocation, &n, ErrEnvelopeEquivocation
		case e.Revision < prev.Envelope.Revision:
			return DecisionRejectedRollback, nil, ErrEnvelopeRollback
		case e.PrevDigest != prev.Digest:
			return "", nil, fmt.Errorf("%w: prev_digest does not chain to the stored envelope (revision %d)", ErrEnvelopeInvalid, prev.Envelope.Revision)
		}
	}
	rec := EnvelopeRecord{Envelope: *e, Digest: digest, StoredAt: now}
	b, err := json.Marshal(rec)
	if err != nil {
		return "", nil, fmt.Errorf("orgpolicy store: encode envelope: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{policyCond, envCond, {Key: envelopeHistKey(e.OrgID, e.Revision), Value: nil}},
		[]KVWrite{
			{Key: envelopeKey(e.OrgID), Value: b},
			{Key: envelopeHistKey(e.OrgID, e.Revision), Value: b},
		},
	)
	if err != nil {
		return "", nil, err
	}
	if !applied {
		return "", nil, s.envelopeCASFailure(e.OrgID, policyRaw)
	}
	return DecisionApplied, nil, nil
}

func (s *Store) envelopeCASFailure(orgID string, policyRaw []byte) error {
	_, now, err := s.loadAcceptedWithRaw(orgID)
	if err == nil && string(now) != string(policyRaw) {
		return ErrEnvelopePolicyChanged
	}
	return ErrEnvelopeStateConflict
}
