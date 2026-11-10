package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// FederatedGuestStore is intentionally narrower than SQLiteStore. Federation
// transport can consume signed guest capabilities but cannot create groups or
// infer consensus-local membership.
type FederatedGuestStore interface {
	ListFederatedGroupGuests(ctx context.Context, remoteChainID, remoteAgentID string) ([]store.FederatedGroupGuest, error)
	// ListFederatedGroupGuestsForGroup backs the group projection: a federated
	// agent should appear where its group appears, not only in a per-identity
	// inventory.
	ListFederatedGroupGuestsForGroup(ctx context.Context, groupID string) ([]store.FederatedGroupGuest, error)
}

type FederatedQueryChallengeStore interface {
	FederationV23SchemaReady(ctx context.Context) error
	IssueFederatedQueryChallenge(ctx context.Context, challenge store.FederatedQueryChallenge, now time.Time) error
	ConsumeFederatedQueryChallenge(ctx context.Context, challenge store.FederatedQueryChallenge, now time.Time) error
}

// LocalGroupDomainResolver is implemented by the local group authority. A
// guest link is useful only when the referenced local group currently covers
// the exact queried domain.
type LocalGroupDomainResolver interface {
	FederatedGuestGroupAllowsDomain(ctx context.Context, groupID, domain string) (bool, error)
}

type agreementBindingV23 struct {
	Version        int                              `json:"version"`
	LocalChainID   string                           `json:"local_chain_id"`
	RemoteChainID  string                           `json:"remote_chain_id"`
	PeerAgentID    string                           `json:"peer_agent_id"`
	PeerCAPin      string                           `json:"peer_ca_pin"`
	PolicyEpoch    string                           `json:"policy_epoch"`
	PolicyRevision int64                            `json:"policy_revision"`
	PolicyPaused   bool                             `json:"policy_paused"`
	PolicyDomains  []store.PeerRBACDomainPermission `json:"policy_domains"`
	MaxClearance   uint8                            `json:"max_clearance"`
	ExpiresAt      int64                            `json:"expires_at"`
	Domains        []string                         `json:"domains"`
}

// v23BindingReady is the single runtime admission gate for both capability
// advertisement and query handling. A historical tx-33 agreement alone is not
// a v23 authorization binding.
func (m *Manager) v23BindingReady(ctx context.Context, agreement *store.CrossFedRecord, peerAgentID string) (*store.PeerRBACPolicy, error) {
	if m.postV23ForNextTx == nil || !m.postV23ForNextTx() {
		return nil, fmt.Errorf("app-v23 is not active")
	}
	if agreement == nil || agreement.RemoteChainID == "" {
		return nil, fmt.Errorf("active federation agreement is required")
	}
	if m.syncStore() == nil || m.federatedGuestStore == nil ||
		m.localGroupResolver == nil || m.queryChallengeStore == nil || m.badger == nil {
		return nil, fmt.Errorf("federation v23 authorization backends are unavailable")
	}
	if err := m.queryChallengeStore.FederationV23SchemaReady(ctx); err != nil {
		return nil, fmt.Errorf("federation v23 schema is not ready: %w", err)
	}
	if !m.seedEstablished(agreement.RemoteChainID) || len(m.seedCandidates(agreement.RemoteChainID)) == 0 {
		return nil, fmt.Errorf("federation v23 agreement seed is not established and unlocked")
	}
	if _, err := auth.AgentIDToPublicKey(peerAgentID); err != nil {
		return nil, fmt.Errorf("peer operator identity is invalid: %w", err)
	}
	policy, err := m.getPeerRBACPolicyForAgreement(ctx, agreement)
	if err != nil {
		return nil, err
	}
	if policy == nil || policy.PeerAgentID != peerAgentID ||
		policy.PolicyVersion != store.CurrentPeerRBACPolicyVersion ||
		policy.PolicyEpoch == "" || policy.RemoteCAPin != hex.EncodeToString(agreement.PeerPubKey) ||
		policy.Revision <= 0 {
		return nil, fmt.Errorf("federation v23 peer policy is incomplete or stale")
	}
	return policy, nil
}

// agreementBindingDigestV23 binds guest links and query envelopes to the exact
// active peer operator plus the current ceremony/policy generation.
func (m *Manager) agreementBindingDigestV23(ctx context.Context, agreement *store.CrossFedRecord, peerAgentID string) (string, error) {
	policy, err := m.v23BindingReady(ctx, agreement, peerAgentID)
	if err != nil {
		return "", err
	}
	binding := agreementBindingV23{
		Version:        FederationProtocolV23,
		LocalChainID:   m.localChainID,
		RemoteChainID:  agreement.RemoteChainID,
		PeerAgentID:    peerAgentID,
		PeerCAPin:      policy.RemoteCAPin,
		PolicyEpoch:    policy.PolicyEpoch,
		PolicyRevision: policy.Revision,
		PolicyPaused:   policy.Paused,
		PolicyDomains:  append([]store.PeerRBACDomainPermission(nil), policy.Domains...),
		MaxClearance:   agreement.MaxClearance,
		ExpiresAt:      agreement.ExpiresAt,
		Domains:        append([]string(nil), agreement.AllowedDomains...),
	}
	sort.Strings(binding.Domains)
	sort.Slice(binding.PolicyDomains, func(i, j int) bool {
		return binding.PolicyDomains[i].Domain < binding.PolicyDomains[j].Domain
	})
	body, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

// attestLocalFederatedAgent produces the source-side ordinary-agent fact carried
// under the authenticated peer envelope. The destination cannot inspect
// another chain's local RBAC tables, while the nested proof still binds the
// original caller and exact query independently.
func (m *Manager) attestLocalFederatedAgent(agentID string) (uint8, bool, error) {
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		return 0, false, errors.New("local federation agent policy is unavailable")
	}
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer ownerUnlock()
	return m.attestLocalFederatedAgentLocked(agentID)
}

// attestLocalFederatedAgentLocked is the non-locking form for bounded
// federation side effects that already hold the global sync-policy -> Badger
// authorization read order. Keeping the final attestation and outbound request
// inside that lease prevents an enrollment, suspension, or clearance mutation
// from committing after the source fact is checked but before disclosure.
func (m *Manager) attestLocalFederatedAgentLocked(agentID string) (uint8, bool, error) {
	if m.badger == nil {
		return 0, false, errors.New("local federation agent policy is unavailable")
	}
	eligible, err := m.localFederatedGuestAgentEligible(agentID)
	if err != nil || !eligible {
		return 0, false, err
	}
	enrollment, err := m.badger.GetAppV23Enrollment(agentID)
	if err != nil || enrollment == nil || !enrollment.Active {
		return 0, false, err
	}
	return enrollment.Clearance, true, nil
}

// authorizeFederatedPeerRead applies the directional federation export model.
// Peer Read policy selects exported domains. Any active ordinary agent attested
// by the source SAGE may read that export up to the lower source/agreement
// classification ceiling. Explicit linked-reader rows remain a compatibility
// path for older peers; they are not mirrored groups and never narrow the new
// peer-export contract.
func (m *Manager) authorizeFederatedPeerRead(ctx context.Context, peer *peerIdentity, agreement *store.CrossFedRecord, agentID, domain, sourceAuthorizationModel string, sourceAgentEligible bool, sourceAgentMaxClassification uint8) (uint8, error) {
	if m.federatedGuestStore == nil || m.localGroupResolver == nil {
		return 0, fmt.Errorf("federated guest authorization is unavailable")
	}
	domainCeiling, err := m.peerDomainExportCeiling(ctx, peer, agreement, domain)
	if err != nil {
		return 0, err
	}
	switch sourceAuthorizationModel {
	case "":
		legacyCeiling, legacyErr := m.authorizeFederatedGuestRead(ctx, peer, agreement, agentID, domain)
		if legacyErr != nil {
			return 0, legacyErr
		}
		if legacyCeiling > domainCeiling {
			legacyCeiling = domainCeiling
		}
		return legacyCeiling, nil
	case SourceAuthorizationPeerExportV1:
		if !sourceAgentEligible || agentID == peer.AgentID {
			return 0, errors.New("peer-export authorization requires an attested ordinary source agent")
		}
		ceiling := sourceAgentMaxClassification
		if ceiling > agreement.MaxClearance {
			ceiling = agreement.MaxClearance
		}
		if ceiling > domainCeiling {
			ceiling = domainCeiling
		}
		return ceiling, nil
	default:
		return 0, errors.New("unsupported federation source authorization model")
	}
}

// authorizeFederatedGuestRead retains the exact explicit linked-reader gate
// used by linked messaging and control-plane tests. Federation recall uses
// authorizeFederatedPeerRead so a missing override means the peer export's
// default Read, not a missing mirrored group.
func (m *Manager) authorizeFederatedGuestRead(ctx context.Context, peer *peerIdentity, agreement *store.CrossFedRecord, agentID, domain string) (uint8, error) {
	if m.federatedGuestStore == nil || m.localGroupResolver == nil {
		return 0, fmt.Errorf("federated guest authorization is unavailable")
	}
	guests, err := m.federatedGuestStore.ListFederatedGroupGuests(ctx, peer.ChainID, agentID)
	if err != nil {
		return 0, fmt.Errorf("list federated guest links: %w", err)
	}
	return m.authorizeFederatedGuestRows(ctx, peer, agreement, agentID, domain, guests)
}

func (m *Manager) authorizeFederatedGuestRows(ctx context.Context, peer *peerIdentity, agreement *store.CrossFedRecord, agentID, domain string, guests []store.FederatedGroupGuest) (uint8, error) {
	digest, err := m.agreementBindingDigestV23(ctx, agreement, peer.AgentID)
	if err != nil {
		return 0, err
	}
	var ceiling uint8
	found := false
	for _, guest := range guests {
		if store.VerifyFederatedGroupGuest(guest) != nil ||
			guest.State != store.FederatedGuestStateActive ||
			guest.RemoteChainID != peer.ChainID ||
			guest.RemoteAgentID != agentID ||
			!m.federatedGuestAuthorityActive(guest) ||
			guest.AgreementBindingDigest != digest {
			continue
		}
		allowed, resolveErr := m.localGroupResolver.FederatedGuestGroupAllowsDomain(ctx, guest.GroupID, domain)
		if resolveErr != nil {
			return 0, fmt.Errorf("resolve local guest group: %w", resolveErr)
		}
		if !allowed {
			continue
		}
		if !found || guest.MaxClassification > ceiling {
			ceiling = guest.MaxClassification
		}
		found = true
	}
	if !found {
		return 0, fmt.Errorf("remote agent has no active guest link for this domain and agreement generation")
	}
	if ceiling > agreement.MaxClearance {
		ceiling = agreement.MaxClearance
	}
	return ceiling, nil
}

// federatedGuestAuthorityActive separates authority to CREATE/MUTATE a row
// from authority already frozen into a valid signed row. A promoted Admin is a
// revocable delegate, so its rows remain conditional on its current role,
// enrollment, generation, and policy projection. Root is the stable CEREBRUM
// authority: an exact credential signature remains valid provenance after a
// forward-only handover because every Root credential generation is
// permanently reserved in consensus history. Retired credentials still cannot
// prepare, elevate, or commit a new mutation: those entry points continue to
// require localAdminSignerActive for the authenticated caller.
func (m *Manager) federatedGuestAuthorityActive(
	guest store.FederatedGroupGuest,
) bool {
	if m.badger == nil {
		return false
	}
	root, err := m.badger.GetAppV23Root()
	if err != nil || root == nil {
		return false
	}
	historicalGeneration, wasRoot, err :=
		m.badger.GetAppV23RootCredentialGeneration(guest.AuthorizedBy)
	if err != nil {
		return false
	}
	switch guest.AuthorityKind {
	case "":
		// Rolling compatibility for pre-release rows: only an immutable Root
		// history signature survives without lifecycle metadata. A legacy
		// delegated-Admin row must be explicitly rebound.
		return wasRoot
	case store.FederatedGuestAuthorityRoot:
		return wasRoot &&
			guest.AuthorityRootGeneration == historicalGeneration
	case store.FederatedGuestAuthorityAdmin:
		if wasRoot ||
			guest.AuthorityRootGeneration != root.Generation ||
			!m.localAdminSignerActive(guest.AuthorizedBy) {
			return false
		}
		enrollment, enrollmentErr :=
			m.badger.GetAppV23Enrollment(guest.AuthorizedBy)
		role, roleErr := m.badger.GetAppV23Role(guest.AuthorizedBy)
		return enrollmentErr == nil && roleErr == nil &&
			enrollment != nil && role != nil &&
			enrollment.Revision == guest.AuthorityEnrollmentRevision &&
			role.Revision == guest.AuthorityRoleRevision
	default:
		return false
	}
}

// localAdminSignerActive evaluates the current consensus-local identity on
// every disclosure. A demoted/deactivated Admin's previously signed node-local
// guest rows stop authorizing immediately. Historical Root provenance is
// handled separately by federatedGuestAuthorityActive and never makes a
// retired credential an active caller.
func (m *Manager) localAdminSignerActive(agentID string) bool {
	if m.badger == nil {
		return false
	}
	root, err := m.badger.GetAppV23Root()
	if err != nil || root == nil {
		return false
	}
	policyID := agentID
	if agentID == root.CredentialID {
		policyID = root.PrincipalID
	} else if agentID == root.PrincipalID && root.CredentialID != root.PrincipalID {
		return false
	}
	enrollment, err := m.badger.GetAppV23Enrollment(policyID)
	if err != nil || enrollment == nil || !enrollment.Active ||
		enrollment.RootGeneration != root.Generation {
		return false
	}
	role, err := m.badger.GetAppV23Role(policyID)
	if err != nil || role == nil {
		return false
	}
	agent, err := m.badger.GetRegisteredAgent(policyID)
	return err == nil && agent != nil &&
		role.Role == store.AppV23RoleAdmin &&
		agent.Role == role.Role &&
		agent.Clearance == enrollment.Clearance &&
		agent.Capabilities == enrollment.Capabilities &&
		m.badger.ValidateCurrentAgentPolicy(role.Role, enrollment.Profile,
			enrollment.Capabilities, enrollment.Clearance) == nil
}
