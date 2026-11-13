package orgpolicy

// Node-local link-card admission (RFC #443 Phase A2).
//
// A card is minted and signed by the enrolled node itself. The node-local
// store therefore verifies the card under its own agent_pub and binds the
// write to both the exact accepted-policy generation and the current card
// floor. The same card/{org}/{agent_pub} record is used by roster admission, so
// a self-minted card can never move the floor backwards or evade roster
// equivocation checks.

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrCardPolicyChanged means the accepted org policy changed after the
	// caller derived and signed the card.
	ErrCardPolicyChanged = errors.New("org link card: accepted org policy changed since the card was minted")

	// ErrCardStateConflict means another card update changed the floor between
	// the read and the compare-and-set write.
	ErrCardStateConflict = errors.New("org link card: card floor changed concurrently")

	// ErrCardRegression means the submitted card_rev is below the accepted
	// floor for this agent_pub.
	ErrCardRegression = errors.New("org link card: card_rev regresses the accepted card floor")

	// ErrCardInvalid wraps a structurally invalid or self-signature-failing
	// card. It is a caller error.
	ErrCardInvalid = errors.New("org link card: invalid")
)

// ApplyLocalCard admits a card minted by this node for expectedAgentPub.
//
// The card's structure and signature are verified before any state is read.
// The write is conditioned on the exact accepted-policy bytes supplied by the
// caller and the exact current card-floor bytes. A policy swap therefore
// returns ErrCardPolicyChanged instead of letting a card signed for the old
// generation land under the new one. A concurrent same-revision write either
// wins the CAS, becomes an idempotent no-op, or is surfaced as an equivocation
// notice; it can never produce a torn card record.
func (s *Store) ApplyLocalCard(card LinkCard, expectedAgentPub string, policy AcceptedState) (CardRecord, Decision, *RosterNotice, error) {
	if err := card.Verify(); err != nil {
		return CardRecord{}, "", nil, fmt.Errorf("%w: %v", ErrCardInvalid, err)
	}
	if card.AgentPub != expectedAgentPub {
		return CardRecord{}, "", nil, fmt.Errorf("%w: card agent_pub %s does not match local node %s", ErrCardInvalid, card.AgentPub, expectedAgentPub)
	}
	if policy.Revision == 0 || policy.OrgID != card.OrgID {
		return CardRecord{}, "", nil, fmt.Errorf("%w: no accepted policy for %q", ErrCardInvalid, card.OrgID)
	}

	current, policyRaw, err := s.loadAcceptedWithRaw(card.OrgID)
	if err != nil {
		return CardRecord{}, "", nil, err
	}
	if current != policy {
		return CardRecord{}, "", nil, ErrCardPolicyChanged
	}

	prev, cardRaw, err := s.loadCardRaw(card.OrgID, card.AgentPub)
	if err != nil {
		return CardRecord{}, "", nil, err
	}
	digest := card.Digest()
	if prev != nil {
		switch {
		case card.CardRev < prev.CardRev:
			return *prev, DecisionRejectedRollback, nil, ErrCardRegression
		case card.CardRev == prev.CardRev && digest == prev.Digest:
			return *prev, DecisionNoOpSameDigest, nil, nil
		case card.CardRev == prev.CardRev:
			notice := &RosterNotice{
				Kind: "card", OrgID: card.OrgID, AgentPub: card.AgentPub,
				CardRev: card.CardRev, AcceptedDigest: prev.Digest, ConflictDigest: digest,
			}
			if noticeErr := s.persistCardNotice(
				[]KVCondition{
					{Key: acceptedKey(card.OrgID), Value: policyRaw},
					{Key: cardKey(card.OrgID, card.AgentPub), Value: cardRaw},
				},
				*notice,
			); noticeErr != nil {
				return *prev, "", nil, noticeErr
			}
			return *prev, DecisionRejectedEquivocation, notice, nil
		}
	}

	next := CardRecord{CardRev: card.CardRev, Digest: digest, Card: card}
	cardBytes, err := json.Marshal(next)
	if err != nil {
		return CardRecord{}, "", nil, fmt.Errorf("orgpolicy store: encode card: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{
			{Key: acceptedKey(card.OrgID), Value: policyRaw},
			{Key: cardKey(card.OrgID, card.AgentPub), Value: cardRaw},
		},
		[]KVWrite{{Key: cardKey(card.OrgID, card.AgentPub), Value: cardBytes}},
	)
	if err != nil {
		return CardRecord{}, "", nil, err
	}
	if !applied {
		return s.classifyCardCASFailure(card, policyRaw)
	}
	return next, DecisionApplied, nil, nil
}

func (s *Store) persistCardNotice(conds []KVCondition, notice RosterNotice) error {
	b, err := json.Marshal(notice)
	if err != nil {
		return fmt.Errorf("orgpolicy store: encode card notice: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		conds,
		[]KVWrite{{Key: rosterNoticeKey(notice.OrgID), Value: b}},
	)
	if err != nil {
		return err
	}
	if !applied {
		return s.cardCASFailure(notice.OrgID, notice.AgentPub, conds[0].Value)
	}
	return nil
}

func (s *Store) cardCASFailure(orgID, agentPub string, policyRaw []byte) error {
	_, nowPolicy, err := s.loadAcceptedWithRaw(orgID)
	if err == nil && string(nowPolicy) != string(policyRaw) {
		return ErrCardPolicyChanged
	}
	if err == nil {
		if _, current, loadErr := s.loadCardRaw(orgID, agentPub); loadErr != nil {
			return loadErr
		} else if current == nil {
			return ErrCardStateConflict
		}
	}
	return ErrCardStateConflict
}

// classifyCardCASFailure turns a lost compare-and-set into the same semantic
// outcomes a serialized second request would have observed. In particular, two
// same-revision cards raced by concurrent requests become a durable
// equivocation notice instead of only a generic retry error.
func (s *Store) classifyCardCASFailure(card LinkCard, expectedPolicyRaw []byte) (CardRecord, Decision, *RosterNotice, error) {
	_, policyRaw, err := s.loadAcceptedWithRaw(card.OrgID)
	if err != nil {
		return CardRecord{}, "", nil, err
	}
	if string(policyRaw) != string(expectedPolicyRaw) {
		return CardRecord{}, "", nil, ErrCardPolicyChanged
	}
	prev, cardRaw, err := s.loadCardRaw(card.OrgID, card.AgentPub)
	if err != nil {
		return CardRecord{}, "", nil, err
	}
	if prev == nil {
		return CardRecord{}, "", nil, ErrCardStateConflict
	}
	digest := card.Digest()
	switch {
	case card.CardRev < prev.CardRev:
		return *prev, DecisionRejectedRollback, nil, ErrCardRegression
	case card.CardRev == prev.CardRev && digest == prev.Digest:
		return *prev, DecisionNoOpSameDigest, nil, nil
	case card.CardRev == prev.CardRev:
		notice := &RosterNotice{
			Kind: "card", OrgID: card.OrgID, AgentPub: card.AgentPub,
			CardRev: card.CardRev, AcceptedDigest: prev.Digest, ConflictDigest: digest,
		}
		if err := s.persistCardNotice(
			[]KVCondition{
				{Key: acceptedKey(card.OrgID), Value: policyRaw},
				{Key: cardKey(card.OrgID, card.AgentPub), Value: cardRaw},
			},
			*notice,
		); err != nil {
			return *prev, "", nil, err
		}
		return *prev, DecisionRejectedEquivocation, notice, nil
	default:
		return *prev, DecisionRejectedRollback, nil, ErrCardRegression
	}
}

// LoadCardNotice returns the most recent durable roster/card notice for orgID.
// The notice kind distinguishes a card-floor equivocation from a roster one.
func (s *Store) LoadCardNotice(orgID string) (*RosterNotice, bool, error) {
	return s.LoadRosterNotice(orgID)
}
