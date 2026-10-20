package orgpolicy

// Persistence for the org-policy accepted chain and the retry-safe enrollment
// outcome. This file keeps the package's PURE discipline: it depends on a small
// KV abstraction (GetState + SetStatesAtomic) rather than importing
// internal/store, so the ordering/equivocation/enrollment durability contract
// stays unit-testable in isolation (an in-memory fake KV) with no badger in the
// build. The caller (api/rest) adapts *store.BadgerStore to KV — a one-line
// bridge from orgpolicy.KVWrite to store.StateWrite — and nothing else here
// knows about badger.
//
// Durability model (answers RFC #443 point 2, retry-safe enrollment):
//   - The enrollment OUTCOME record IS the single-use consume marker. Keyed by
//     token_id, it records the enrollee pubkey that consumed the token plus the
//     replayable result (bound identity + first policy). Its existence means
//     "consumed, with this recoverable outcome".
//   - Consume + bound identity + first policy + accepted high-water mark commit
//     in ONE SetStatesAtomic transaction (one badger Update + one Sync). There is
//     no two-phase window: either the whole outcome is durable or nothing is.
//   - Crash BEFORE commit: nothing written, the token is still unconsumed, a
//     retry re-verifies and commits cleanly. Crash AFTER commit but before the
//     HTTP response: the outcome is durable, and the idempotent retry (same
//     token_id + same enrollee pubkey) replays it — a lost response never strands
//     the node behind a token it cannot reuse.
//
// Accepted-chain model (point 3, policy ordering & equivocation):
//   - The accepted (revision, digest, key_epoch, issuer) high-water mark is
//     persisted per org and reloaded on restart, so rollback and equivocation
//     stay rejected across a restart.
//   - An equivocation (same revision, different digest) never overwrites the
//     accepted state; it persists a DURABLE owner-visible notice (so the alarm
//     survives a restart) and returns it for the live operator surface to raise.

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// KVWrite is one key/value update applied as part of an atomic batch. It mirrors
// store.StateWrite field-for-field; the caller bridges the two.
type KVWrite struct {
	Key   string
	Value []byte
}

// KV is the minimal durable key/value surface this package needs. The contract
// matches *store.BadgerStore exactly:
//   - GetState returns (nil, nil) for an absent key (never a sentinel error).
//   - SetStatesAtomic persists every write in ONE transaction and fsyncs before
//     returning; a returned error means NONE of the writes are durable (a sync
//     error is the one exception the store documents, and the caller fail-stops).
type KV interface {
	GetState(key string) ([]byte, error)
	SetStatesAtomic(writes []KVWrite) error
}

const keyPrefix = "orgpolicy/"

// seg hex-encodes a dynamic key segment so an arbitrary org_id / token_id can
// never inject a path separator or collide with another key's structure.
func seg(s string) string { return hex.EncodeToString([]byte(s)) }

func acceptedKey(orgID string) string { return keyPrefix + "accepted/" + seg(orgID) }
func enrollKey(tokenID string) string { return keyPrefix + "enroll/" + seg(tokenID) }
func equivKey(orgID string) string    { return keyPrefix + "equiv/" + seg(orgID) }

// EnrollmentOutcome is the durable, replayable result of a first enrollment. It
// is written exactly once per token (atomically with the accepted state) and
// returned verbatim on an idempotent retry. The result is fully derivable from
// BoundIdentity + FirstPolicy, as RFC #443 point 2 requires; both are stored so
// a replay re-delivers the identical first policy without re-contacting anyone.
type EnrollmentOutcome struct {
	TokenID       string        `json:"token_id"`
	EnrolleePub   string        `json:"enrollee_pub"` // hex ed25519; the identity that consumed the token
	OrgID         string        `json:"org_id"`
	BoundIdentity string        `json:"bound_identity"` // owner id + node label + org role, bound at redeem time
	FirstPolicy   *Policy       `json:"first_policy"`   // the delivered first policy (re-verifiable on replay)
	Accepted      AcceptedState `json:"accepted"`       // high-water mark this enrollment establishes
	ConsumedAt    int64         `json:"consumed_at"`    // unix seconds
}

// NewEnrollmentOutcome builds a commit-ready outcome from a verified first
// policy. The accepted high-water mark and OrgID are derived from the policy
// here (stateFrom is unexported by design), so a caller outside this package
// cannot desynchronize the persisted accepted state from the stored first
// policy. first must be non-nil for a usable outcome; CommitFirstEnrollment
// rejects a nil first policy, so a nil here yields an outcome that fails to
// commit rather than a panic.
func NewEnrollmentOutcome(tokenID, enrolleePub, boundIdentity string, first *Policy, consumedAt int64) EnrollmentOutcome {
	out := EnrollmentOutcome{
		TokenID:       tokenID,
		EnrolleePub:   enrolleePub,
		BoundIdentity: boundIdentity,
		FirstPolicy:   first,
		ConsumedAt:    consumedAt,
	}
	if first != nil {
		out.OrgID = first.OrgID
		out.Accepted = stateFrom(first)
	}
	return out
}

// Store binds the KV to the orgpolicy persistence operations. It holds no state
// of its own; every method reads/writes through kv.
type Store struct{ kv KV }

// NewStore wraps a KV (e.g. an adapter over *store.BadgerStore).
func NewStore(kv KV) *Store { return &Store{kv: kv} }

// LoadAccepted returns the persisted high-water mark for orgID, or the zero
// AcceptedState (Revision == 0) when the org has no accepted policy yet.
func (s *Store) LoadAccepted(orgID string) (AcceptedState, error) {
	var st AcceptedState
	raw, err := s.kv.GetState(acceptedKey(orgID))
	if err != nil {
		return st, fmt.Errorf("orgpolicy store: load accepted: %w", err)
	}
	if raw == nil {
		return st, nil // no policy accepted yet
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return AcceptedState{}, fmt.Errorf("orgpolicy store: decode accepted: %w", err)
	}
	return st, nil
}

// LoadEnrollment returns the durable outcome for tokenID, reporting ok=false when
// the token has never been consumed. The caller uses it to make redemption
// idempotent: if ok and the stored EnrolleePub equals the presenting node's
// pubkey, replay the stored outcome; if ok and it differs, the token was already
// consumed by another identity and must be refused.
func (s *Store) LoadEnrollment(tokenID string) (*EnrollmentOutcome, bool, error) {
	raw, err := s.kv.GetState(enrollKey(tokenID))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load enrollment: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var out EnrollmentOutcome
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode enrollment: %w", err)
	}
	return &out, true, nil
}

// LoadEquivocation returns the most recent persisted equivocation notice for
// orgID (ok=false if none). It is the durable backing for the operator-visible
// surface, so the alarm is still shown after a node restart.
func (s *Store) LoadEquivocation(orgID string) (*EquivocationNotice, bool, error) {
	raw, err := s.kv.GetState(equivKey(orgID))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load equivocation: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var n EquivocationNotice
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode equivocation: %w", err)
	}
	return &n, true, nil
}

// CommitFirstEnrollment persists a verified first enrollment in a SINGLE atomic
// write: the consume/outcome record (keyed by token_id) and the org accepted
// high-water mark, together. The caller MUST have already (a) verified the first
// policy under the authorized signer and (b) checked LoadEnrollment to honor
// idempotency — this method unconditionally writes, so a second call for the
// same token with a different outcome would overwrite and is a caller bug.
//
// outcome.Accepted must equal stateFrom(outcome.FirstPolicy); CommitFirstEnrollment
// enforces that so the persisted high-water mark can never disagree with the
// stored first policy.
func (s *Store) CommitFirstEnrollment(outcome EnrollmentOutcome) error {
	if outcome.FirstPolicy == nil {
		return fmt.Errorf("orgpolicy store: enrollment outcome has no first policy")
	}
	if outcome.TokenID == "" || outcome.EnrolleePub == "" || outcome.OrgID == "" {
		return fmt.Errorf("orgpolicy store: enrollment outcome missing token_id/enrollee/org")
	}
	if outcome.OrgID != outcome.FirstPolicy.OrgID {
		return fmt.Errorf("orgpolicy store: enrollment org %q != first policy org %q", outcome.OrgID, outcome.FirstPolicy.OrgID)
	}
	want := stateFrom(outcome.FirstPolicy)
	if outcome.Accepted != want {
		return fmt.Errorf("orgpolicy store: accepted state does not match first policy")
	}
	recBytes, err := json.Marshal(outcome)
	if err != nil {
		return fmt.Errorf("orgpolicy store: encode enrollment outcome: %w", err)
	}
	accBytes, err := json.Marshal(outcome.Accepted)
	if err != nil {
		return fmt.Errorf("orgpolicy store: encode accepted state: %w", err)
	}
	// ONE transaction: consume marker (== outcome) and accepted high-water mark.
	return s.kv.SetStatesAtomic([]KVWrite{
		{Key: enrollKey(outcome.TokenID), Value: recBytes},
		{Key: acceptedKey(outcome.OrgID), Value: accBytes},
	})
}

// ApplyPolicy admits a fetched policy against the persisted accepted state and
// persists the consequence, returning the resulting accepted state and decision.
// It does NOT enforce NotBefore/Expiry — per-operation offline/expiry gating
// (RFC #443 point 4) is layered on top by the caller; ApplyPolicy is purely the
// ordering + equivocation persistence step.
//
//   - DecisionApplied            → the new accepted state is persisted (one write).
//   - DecisionNoOpSameDigest      → nothing is written (an idempotent re-fetch must
//     not extend expiry or authority).
//   - DecisionRejectedRollback    → nothing is written.
//   - DecisionRejectedEquivocation→ a durable owner-visible notice is persisted
//     (accepted state is left UNCHANGED) and returned
//     for the live operator surface to raise.
//
// authorized is the signer the caller resolved as current (the pinned org key,
// or a rotated successor proven by a verified attestation chain — see rotation.go).
func (s *Store) ApplyPolicy(p *Policy, authorized ed25519.PublicKey) (AcceptedState, Decision, *EquivocationNotice, error) {
	prev, err := s.LoadAccepted(p.OrgID)
	if err != nil {
		return AcceptedState{}, "", nil, err
	}
	next, decision, err := Admit(prev, p, authorized)
	if err != nil {
		return prev, "", nil, err
	}
	switch decision {
	case DecisionApplied:
		accBytes, mErr := json.Marshal(next)
		if mErr != nil {
			return prev, "", nil, fmt.Errorf("orgpolicy store: encode accepted state: %w", mErr)
		}
		if wErr := s.kv.SetStatesAtomic([]KVWrite{{Key: acceptedKey(p.OrgID), Value: accBytes}}); wErr != nil {
			return prev, "", nil, wErr
		}
		return next, decision, nil, nil
	case DecisionRejectedEquivocation:
		notice := NewEquivocationNotice(prev, p)
		nBytes, mErr := json.Marshal(notice)
		if mErr != nil {
			return prev, "", nil, fmt.Errorf("orgpolicy store: encode equivocation notice: %w", mErr)
		}
		if wErr := s.kv.SetStatesAtomic([]KVWrite{{Key: equivKey(p.OrgID), Value: nBytes}}); wErr != nil {
			return prev, "", nil, wErr
		}
		return prev, decision, &notice, nil
	default: // NoOpSameDigest, RejectedRollback — no state change
		return prev, decision, nil, nil
	}
}
