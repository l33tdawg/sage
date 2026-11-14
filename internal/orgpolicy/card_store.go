package orgpolicy

// Node-self-minted link card (RFC #443 Phase A2). Each enrolled node signs its
// own card; this file persists it into the same per-agent_pub card floor the
// roster store uses (card/{org}/{agent_pub}), under the same rules:
//
//   - a higher card_rev replaces the stored card;
//   - the same card_rev and digest is a no-op;
//   - the same card_rev with a different digest is card equivocation: refused,
//     with a durable notice;
//   - a lower card_rev is refused.
//
// Every write is conditioned on the exact stored accepted-policy bytes the
// caller authorized against, so a card is never minted under a policy
// generation that changed in between, and the floor survives restarts.

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrCardInvalid wraps a card that fails verification, freshness or the
// accepted-policy precondition. It is a client error.
var ErrCardInvalid = errors.New("org card: invalid")

func cardNoticeKey(orgID, agentPub string) string {
	return keyPrefix + "card-notice/" + seg(orgID) + "/" + seg(agentPub)
}

// LoadCardNotice returns the latest durable equivocation notice for a card.
func (s *Store) LoadCardNotice(orgID, agentPub string) (*RosterNotice, bool, error) {
	raw, err := s.kv.GetState(cardNoticeKey(orgID, agentPub))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load card notice: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var n RosterNotice
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode card notice: %w", err)
	}
	return &n, true, nil
}

// ApplyOwnCard admits the node's own freshly signed card. policy is the
// accepted state the caller checked; if the stored accepted policy is no
// longer exactly that, nothing is written (ErrRosterPolicyChanged).
func (s *Store) ApplyOwnCard(c *LinkCard, policy AcceptedState, now int64) (Decision, *RosterNotice, error) {
	if policy.Revision == 0 || policy.OrgID != c.OrgID {
		return "", nil, fmt.Errorf("%w: no accepted org policy for %q", ErrCardInvalid, c.OrgID)
	}
	if err := c.Verify(); err != nil {
		return "", nil, fmt.Errorf("%w: %v", ErrCardInvalid, err)
	}
	if !c.CurrentAt(now) {
		return "", nil, fmt.Errorf("%w: card is not current", ErrCardInvalid)
	}
	current, policyRaw, err := s.loadAcceptedWithRaw(c.OrgID)
	if err != nil {
		return "", nil, err
	}
	if current != policy {
		return "", nil, ErrRosterPolicyChanged
	}
	rec, cardRaw, err := s.loadCardRaw(c.OrgID, c.AgentPub)
	if err != nil {
		return "", nil, err
	}
	conds := []KVCondition{
		{Key: acceptedKey(c.OrgID), Value: policyRaw},
		{Key: cardKey(c.OrgID, c.AgentPub), Value: cardRaw},
	}
	digest := c.Digest()
	if rec != nil {
		switch {
		case c.CardRev < rec.CardRev:
			return "", nil, ErrRosterCardRegression
		case c.CardRev == rec.CardRev && digest == rec.Digest:
			return DecisionNoOpSameDigest, nil, nil
		case c.CardRev == rec.CardRev:
			notice := RosterNotice{
				Kind: "card", OrgID: c.OrgID, Revision: policy.Revision, AgentPub: c.AgentPub,
				CardRev: c.CardRev, AcceptedDigest: rec.Digest, ConflictDigest: digest,
			}
			b, mErr := json.Marshal(notice)
			if mErr != nil {
				return "", nil, fmt.Errorf("orgpolicy store: encode card notice: %w", mErr)
			}
			applied, wErr := s.kv.CompareAndSetStatesAtomic(conds,
				[]KVWrite{{Key: cardNoticeKey(c.OrgID, c.AgentPub), Value: b}})
			if wErr != nil {
				return "", nil, wErr
			}
			if !applied {
				return "", nil, s.rosterCASFailure(c.OrgID, policyRaw)
			}
			return DecisionRejectedEquivocation, &notice, nil
		}
	}
	recBytes, err := json.Marshal(CardRecord{CardRev: c.CardRev, Digest: digest, Card: *c})
	if err != nil {
		return "", nil, fmt.Errorf("orgpolicy store: encode card: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(conds,
		[]KVWrite{{Key: cardKey(c.OrgID, c.AgentPub), Value: recBytes}})
	if err != nil {
		return "", nil, err
	}
	if !applied {
		return "", nil, s.rosterCASFailure(c.OrgID, policyRaw)
	}
	return DecisionApplied, nil, nil
}
