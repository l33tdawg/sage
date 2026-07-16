package federation

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/l33tdawg/sage/internal/tx"
)

const (
	// This tx-33 compatibility bridge must match the app-v20 consensus
	// collection/identifier bounds so it cannot construct a transaction that is
	// guaranteed to fail at delivery. The independent capability/RBAC plane is
	// not constrained by these legacy treaty fields.
	maxSharingDomains   = 64
	maxSharingDomainLen = 512
)

// canonicalSharingDomains validates a complete outbound-sharing snapshot and
// returns a detached, deterministically ordered copy. Empty is a valid
// snapshot (share nothing). "*" is valid only as the sole entry because it
// already covers every concrete domain.
func canonicalSharingDomains(raw []string) ([]string, error) {
	if len(raw) > maxSharingDomains {
		return nil, fmt.Errorf("sharing is capped at %d domains", maxSharingDomains)
	}
	out := append([]string{}, raw...)
	sort.Strings(out)
	for i, domain := range out {
		if domain == "" || len(domain) > maxSharingDomainLen || strings.TrimSpace(domain) != domain {
			return nil, fmt.Errorf("shared domains must be non-empty, unpadded tags of at most %d bytes", maxSharingDomainLen)
		}
		for _, r := range domain {
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("shared domain contains control characters")
			}
		}
		if i > 0 && out[i-1] == domain {
			return nil, fmt.Errorf("sharing snapshot contains duplicate domain %q", domain)
		}
	}
	if len(out) > 1 && out[0] == "*" {
		return nil, fmt.Errorf("wildcard sharing must be the only domain")
	}
	return out, nil
}

// UpdateAgreementSharing atomically replaces this node's unilateral outbound
// domain grant for remoteChainID. Pairing/trust material and every other treaty
// term are copied from the current active agreement; the peer does not need to
// update its own independent outbound grant or re-pair.
func (m *Manager) UpdateAgreementSharing(remoteChainID string, domains []string) (*SharingUpdateResult, error) {
	canonical, err := canonicalSharingDomains(domains)
	if err != nil {
		return nil, err
	}

	m.agreementMutationMu.Lock()
	defer m.agreementMutationMu.Unlock()
	// Legacy tx-33 terms are still the effective read policy until a peer is
	// upgraded to directional RBAC. Take the same write side used by peer
	// handlers before resolving the current terms and retain it through the
	// committed replacement. This makes a narrowing linearizable with any
	// response already serving the old agreement generation.
	if ss := m.syncStore(); ss != nil {
		policyUnlock := ss.LockSyncPolicyWrite()
		defer policyUnlock()
	}

	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	terms := &tx.CrossFedTerms{
		RemoteChainID:  agreement.RemoteChainID,
		Endpoint:       agreement.Endpoint,
		PeerPubKey:     append([]byte(nil), agreement.PeerPubKey...),
		MaxClearance:   tx.ClearanceLevel(agreement.MaxClearance),
		AllowedDomains: canonical,
		AllowedDepts:   append([]string(nil), agreement.AllowedDepts...),
		ExpiresAt:      agreement.ExpiresAt,
		Status:         "active",
	}
	txHash, err := m.broadcastCrossFedSetLocked(terms)
	if err != nil {
		return nil, fmt.Errorf("update federation sharing: %w", err)
	}
	return &SharingUpdateResult{
		Domains:      append([]string{}, canonical...),
		MaxClearance: agreement.MaxClearance,
		TxHash:       txHash,
	}, nil
}
