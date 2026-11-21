package orgpolicy

// Owner acceptance of an intro offer (RFC #443 Phase B2b, contract v2 §2).
//
// An acceptance is the owner's local consent to one offer, bound to the exact
// owner envelope (revision + digest) it was given under. The host signs
// host-accept, and the guest signs guest-accept + guest-ack, only while an
// acceptance exists and still names the node's current envelope: a new
// envelope revision needs a new tap. Acceptances are node-local, survive
// restart, and are never relayed.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// IntroAcceptance is the local record of an owner's acceptance of one offer.
type IntroAcceptance struct {
	OrgID          string `json:"org_id"`
	OfferID        string `json:"offer_id"`
	Kind           string `json:"kind"`
	Role           string `json:"role"`
	PeerAgentPub   string `json:"peer_agent_pub"`
	EnvelopeRev    uint64 `json:"envelope_revision"`
	EnvelopeDigest string `json:"envelope_digest"`
	// RootCredentialID is the Root that accepted.
	RootCredentialID string `json:"root_credential_id"`
	AcceptedAt       int64  `json:"accepted_at"`
}

// maxIntroAcceptances bounds a listing (one per roster peer at most).
const maxIntroAcceptances = 4096

func introAcceptPrefix() string { return keyPrefix + "intro-accept/" }

func introAcceptKey(orgID, offerID string) string {
	return introAcceptPrefix() + seg(orgID) + "/" + seg(offerID)
}

func (a *IntroAcceptance) validate() error {
	switch {
	case a.OrgID == "":
		return errors.New("intro acceptance: org_id is required")
	case !isAgentPubHex(a.OfferID):
		return errors.New("intro acceptance: offer_id must be 64 lowercase hex")
	case a.Kind != OfferKindNew && a.Kind != OfferKindRekey:
		return errors.New("intro acceptance: kind must be new or rekey")
	case a.Role != RoleHost && a.Role != RoleGuest:
		return errors.New("intro acceptance: role must be host or guest")
	case !isAgentPubHex(a.PeerAgentPub):
		return errors.New("intro acceptance: peer_agent_pub must be 64 lowercase hex")
	case a.EnvelopeRev == 0 || !isAgentPubHex(a.EnvelopeDigest):
		return errors.New("intro acceptance: envelope revision and digest are required")
	case a.AcceptedAt <= 0:
		return errors.New("intro acceptance: accepted_at is required")
	}
	return nil
}

// SaveIntroAcceptance records (or rebinds) the owner's acceptance of an offer.
// Re-accepting under a newer envelope replaces the record.
func (s *Store) SaveIntroAcceptance(a IntroAcceptance) error {
	if err := a.validate(); err != nil {
		return err
	}
	b, err := json.Marshal(&a)
	if err != nil {
		return fmt.Errorf("orgpolicy store: encode intro acceptance: %w", err)
	}
	return s.kv.SetStatesAtomic([]KVWrite{{Key: introAcceptKey(a.OrgID, a.OfferID), Value: b}})
}

// LoadIntroAcceptance returns the acceptance of one offer, if any.
func (s *Store) LoadIntroAcceptance(orgID, offerID string) (*IntroAcceptance, bool, error) {
	raw, err := s.kv.GetState(introAcceptKey(orgID, offerID))
	if err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: load intro acceptance: %w", err)
	}
	if raw == nil {
		return nil, false, nil
	}
	var a IntroAcceptance
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, false, fmt.Errorf("orgpolicy store: decode intro acceptance: %w", err)
	}
	if a.OrgID != orgID || a.OfferID != offerID {
		return nil, false, errors.New("orgpolicy store: intro acceptance does not match its key")
	}
	return &a, true, nil
}

// ListIntroAcceptances returns every recorded acceptance.
func (s *Store) ListIntroAcceptances() ([]IntroAcceptance, error) {
	l, err := s.lister()
	if err != nil {
		return nil, err
	}
	keys, err := l.ListStateKeys(introAcceptPrefix(), maxIntroAcceptances)
	if err != nil {
		return nil, fmt.Errorf("orgpolicy store: list intro acceptances: %w", err)
	}
	out := make([]IntroAcceptance, 0, len(keys))
	for _, k := range keys {
		if !strings.HasPrefix(k, introAcceptPrefix()) {
			continue
		}
		raw, gErr := s.kv.GetState(k)
		if gErr != nil {
			return nil, fmt.Errorf("orgpolicy store: load intro acceptance: %w", gErr)
		}
		if raw == nil {
			continue
		}
		var a IntroAcceptance
		if uErr := json.Unmarshal(raw, &a); uErr != nil {
			return nil, fmt.Errorf("orgpolicy store: decode intro acceptance: %w", uErr)
		}
		if introAcceptKey(a.OrgID, a.OfferID) != k {
			continue
		}
		out = append(out, a)
	}
	return out, nil
}
