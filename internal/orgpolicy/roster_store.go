package orgpolicy

// Durable roster + card state (RFC #443 Phase A), node-local through the same
// KV as the policy chain. One compare-and-set transaction per admission:
//
//   - the roster high-water mark, the accepted roster body and a history entry;
//   - per agent_pub, the highest card ever accepted (kept even after a card
//     leaves the roster, so a later roster can never regress it);
//   - durable owner-visible notices for roster or card equivocation.
//
// Restart reloads all of it, so rollback, equivocation and card regression stay
// rejected across restarts.

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// ErrRosterCardRegression means a delivered roster carried a card whose
// card_rev is lower than one already accepted for that agent_pub. The whole
// roster is rejected.
var ErrRosterCardRegression = errors.New("org roster: card_rev regresses an accepted card")

// ErrRosterStateConflict means the roster or card state changed between read
// and the compare-and-set write; reload and retry.
var ErrRosterStateConflict = errors.New("org roster: accepted roster changed concurrently")

func rosterKey(orgID string) string     { return keyPrefix + "roster/" + seg(orgID) }
func rosterBodyKey(orgID string) string { return keyPrefix + "roster-body/" + seg(orgID) }
func rosterNoticeKey(orgID string) string {
	return keyPrefix + "roster-notice/" + seg(orgID)
}
func rosterHistKey(orgID string, rev uint64) string {
	return keyPrefix + "roster-hist/" + seg(orgID) + "/" + strconv.FormatUint(rev, 10)
}
func cardKey(orgID, agentPub string) string {
	return keyPrefix + "card/" + seg(orgID) + "/" + seg(agentPub)
}

// CardRecord is the highest accepted card for one agent_pub.
type CardRecord struct {
	CardRev uint64   `json:"card_rev"`
	Digest  string   `json:"digest"`
	Card    LinkCard `json:"card"`
}

// RosterNotice is the durable owner-visible notice for a rejected roster:
// Kind "roster" (same revision, different digest) or "card" (a roster carried
// a card whose card_rev equals an accepted one with a different digest).
type RosterNotice struct {
	Kind           string `json:"kind"`
	OrgID          string `json:"org_id"`
	Revision       uint64 `json:"revision"`
	AgentPub       string `json:"agent_pub,omitempty"`
	CardRev        uint64 `json:"card_rev,omitempty"`
	AcceptedDigest string `json:"accepted_digest"`
	ConflictDigest string `json:"conflict_digest"`
	IssuerKeyID    string `json:"issuer_key_id"`
}

func (s *Store) loadRosterRaw(orgID string) (RosterState, []byte, error) {
	var st RosterState
	raw, err := s.kv.GetState(rosterKey(orgID))
	if err != nil {
		return st, nil, fmt.Errorf("orgpolicy store: load roster: %w", err)
	}
	if raw == nil {
		return st, nil, nil
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return RosterState{}, nil, fmt.Errorf("orgpolicy store: decode roster: %w", err)
	}
	return st, append([]byte(nil), raw...), nil
}

// LoadRoster returns the roster high-water mark and the accepted roster body.
// The body is returned only if it matches the high-water mark (nil otherwise).
func (s *Store) LoadRoster(orgID string) (RosterState, *Roster, error) {
	st, _, err := s.loadRosterRaw(orgID)
	if err != nil || st.Revision == 0 {
		return st, nil, err
	}
	raw, err := s.kv.GetState(rosterBodyKey(orgID))
	if err != nil {
		return st, nil, fmt.Errorf("orgpolicy store: load roster body: %w", err)
	}
	if raw == nil {
		return st, nil, nil
	}
	var r Roster
	if err := json.Unmarshal(raw, &r); err != nil {
		return st, nil, fmt.Errorf("orgpolicy store: decode roster body: %w", err)
	}
	if r.OrgID != st.OrgID || r.Revision != st.Revision || r.Digest() != st.Digest {
		return st, nil, nil
	}
	return st, &r, nil
}

// LoadRosterNotice returns the most recent persisted roster notice.
func (s *Store) LoadRosterNotice(orgID string) (*RosterNotice, bool, error) {
	raw, err := s.kv.GetState(rosterNoticeKey(orgID))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load roster notice: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var n RosterNotice
	if err := json.Unmarshal(raw, &n); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode roster notice: %w", err)
	}
	return &n, true, nil
}

// LoadRosterHistory returns the accepted roster state recorded for revision rev.
func (s *Store) LoadRosterHistory(orgID string, rev uint64) (*RosterState, bool, error) {
	raw, err := s.kv.GetState(rosterHistKey(orgID, rev))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load roster history: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var st RosterState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode roster history: %w", err)
	}
	return &st, true, nil
}

// LoadCard returns the highest accepted card for agentPub in orgID.
func (s *Store) LoadCard(orgID, agentPub string) (*CardRecord, bool, error) {
	rec, _, err := s.loadCardRaw(orgID, agentPub)
	if err != nil || rec == nil {
		return nil, false, err
	}
	return rec, true, nil
}

func (s *Store) loadCardRaw(orgID, agentPub string) (*CardRecord, []byte, error) {
	raw, err := s.kv.GetState(cardKey(orgID, agentPub))
	if err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: load card: %w", err)
	}
	if raw == nil {
		return nil, nil, nil
	}
	var rec CardRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, nil, fmt.Errorf("orgpolicy store: decode card: %w", err)
	}
	return &rec, append([]byte(nil), raw...), nil
}

// ApplyRoster admits a delivered roster against the persisted state and
// persists the consequence. authorized is the org signer the caller resolved
// from the accepted policy's trust root and rotation chain; scope is the
// accepted policy scope.
//
//   - DecisionApplied: roster state + body + history + raised card floors, in
//     one compare-and-set write. A card whose card_rev is below an accepted one
//     rejects the whole roster (ErrRosterCardRegression, nothing written); a
//     card at an accepted card_rev with a different digest is card
//     equivocation (notice persisted, roster rejected).
//   - DecisionNoOpSameDigest / DecisionRejectedRollback: nothing written.
//   - DecisionRejectedEquivocation: roster notice persisted, state unchanged.
func (s *Store) ApplyRoster(r *Roster, authorized ed25519.PublicKey, scope string) (RosterState, Decision, *RosterNotice, error) {
	prev, raw, err := s.loadRosterRaw(r.OrgID)
	if err != nil {
		return RosterState{}, "", nil, err
	}
	next, decision, err := AdmitRoster(prev, r, authorized, scope)
	if err != nil {
		return prev, "", nil, err
	}
	rosterCond := KVCondition{Key: rosterKey(r.OrgID), Value: raw}
	switch decision {
	case DecisionApplied:
		conds := []KVCondition{rosterCond}
		var writes []KVWrite
		for i := range r.Cards {
			c := r.Cards[i]
			rec, cardRaw, cErr := s.loadCardRaw(r.OrgID, c.AgentPub)
			if cErr != nil {
				return prev, "", nil, cErr
			}
			conds = append(conds, KVCondition{Key: cardKey(r.OrgID, c.AgentPub), Value: cardRaw})
			digest := c.Digest()
			if rec != nil {
				switch {
				case c.CardRev < rec.CardRev:
					return prev, "", nil, ErrRosterCardRegression
				case c.CardRev == rec.CardRev && digest != rec.Digest:
					notice := RosterNotice{
						Kind: "card", OrgID: r.OrgID, Revision: r.Revision, AgentPub: c.AgentPub,
						CardRev: c.CardRev, AcceptedDigest: rec.Digest, ConflictDigest: digest,
						IssuerKeyID: r.IssuerKeyID,
					}
					if pErr := s.persistRosterNotice(rosterCond, notice); pErr != nil {
						return prev, "", nil, pErr
					}
					return prev, DecisionRejectedEquivocation, &notice, nil
				case c.CardRev == rec.CardRev:
					continue // identical card already accepted
				}
			}
			recBytes, mErr := json.Marshal(CardRecord{CardRev: c.CardRev, Digest: digest, Card: c})
			if mErr != nil {
				return prev, "", nil, fmt.Errorf("orgpolicy store: encode card: %w", mErr)
			}
			writes = append(writes, KVWrite{Key: cardKey(r.OrgID, c.AgentPub), Value: recBytes})
		}
		stBytes, mErr := json.Marshal(next)
		if mErr != nil {
			return prev, "", nil, fmt.Errorf("orgpolicy store: encode roster state: %w", mErr)
		}
		bodyBytes, mErr := json.Marshal(r)
		if mErr != nil {
			return prev, "", nil, fmt.Errorf("orgpolicy store: encode roster body: %w", mErr)
		}
		writes = append(writes,
			KVWrite{Key: rosterKey(r.OrgID), Value: stBytes},
			KVWrite{Key: rosterBodyKey(r.OrgID), Value: bodyBytes},
			KVWrite{Key: rosterHistKey(r.OrgID, r.Revision), Value: stBytes},
		)
		applied, wErr := s.kv.CompareAndSetStatesAtomic(conds, writes)
		if wErr != nil {
			return prev, "", nil, wErr
		}
		if !applied {
			return prev, "", nil, ErrRosterStateConflict
		}
		return next, decision, nil, nil
	case DecisionRejectedEquivocation:
		notice := RosterNotice{
			Kind: "roster", OrgID: prev.OrgID, Revision: prev.Revision,
			AcceptedDigest: prev.Digest, ConflictDigest: r.Digest(), IssuerKeyID: r.IssuerKeyID,
		}
		if pErr := s.persistRosterNotice(rosterCond, notice); pErr != nil {
			return prev, "", nil, pErr
		}
		return prev, decision, &notice, nil
	default:
		return prev, decision, nil, nil
	}
}

func (s *Store) persistRosterNotice(rosterCond KVCondition, n RosterNotice) error {
	b, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("orgpolicy store: encode roster notice: %w", err)
	}
	applied, err := s.kv.CompareAndSetStatesAtomic(
		[]KVCondition{rosterCond},
		[]KVWrite{{Key: rosterNoticeKey(n.OrgID), Value: b}},
	)
	if err != nil {
		return err
	}
	if !applied {
		return ErrRosterStateConflict
	}
	return nil
}
