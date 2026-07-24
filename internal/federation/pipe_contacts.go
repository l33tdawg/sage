package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	pipeContactMinAgentPrefix = 8
	pipeContactPrefixStep     = 4
	pipeContactNodeSlugMax    = 32
)

var (
	ErrPipeContactChanged     = errors.New("federated pipe contact changed")
	ErrPipeContactUnavailable = errors.New("federated pipe contact is unavailable")
)

type pipeContactAggregate struct {
	agentID string
	domains []PipeContactDomain
}

// buildPipeContactGrant derives the finite agent-address projection for one
// already-authenticated peer. A shared-domain owner is always eligible; every
// other active local agent is included only when the live RBAC read check says
// it may read that exact shared subtree. It runs beneath handleStatus's policy
// read lease and intentionally does not advertise a send capability: every
// contact remains default-off until its local operator enables acceptance.
func (m *Manager) buildPipeContactGrant(ctx context.Context, peer *peerIdentity, policy *store.PeerRBACPolicy) (*PipeContactGrant, error) {
	if policy == nil {
		return nil, nil
	}
	if peer == nil || peer.Agreement == nil || peer.ChainID == "" || peer.AgentID == "" {
		return nil, fmt.Errorf("authenticated peer identity is incomplete")
	}
	if policy.RemoteChainID != peer.ChainID || policy.PeerAgentID != peer.AgentID {
		return nil, fmt.Errorf("pipe contact policy binding does not match authenticated peer")
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		return nil, fmt.Errorf("pipe contacts require SQLite and consensus domain state")
	}

	agents, err := ss.ListAgents(ctx)
	if err != nil {
		return nil, fmt.Errorf("list local agents for pipe contacts: %w", err)
	}
	agentByID := make(map[string]*store.AgentEntry, len(agents))
	for _, agent := range agents {
		if agent != nil && agent.AgentID != "" {
			key := agent.AgentID
			if isCanonicalAgentID(key) {
				key = strings.ToLower(key)
			}
			agentByID[key] = agent
		}
	}

	byAgent := make(map[string]*pipeContactAggregate)
	now := time.Now()
	postV8Access := m.postV8ForAccess != nil && m.postV8ForAccess()
	for _, permission := range policy.Domains {
		if !permission.Read && !permission.Copy {
			continue
		}
		owner, owningDomain, resolveErr := m.badger.ResolveOwningAncestor(permission.Domain)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve pipe contact owner for %q: %w", permission.Domain, resolveErr)
		}
		if owner == "" || owningDomain == "" {
			continue
		}
		if isCanonicalAgentID(owner) {
			owner = strings.ToLower(owner)
		}
		// TxTypeDomainReassign may explicitly mark an otherwise registered
		// domain as shared/no-single-owner. ResolveOwningAncestor deliberately
		// handles the reserved-name barrier; the sentinel is checked here too so
		// the contact projection never guesses an owner for an open-shared leaf.
		shared, stateErr := m.badger.GetState("shared_domain:" + owningDomain)
		if stateErr != nil {
			return nil, fmt.Errorf("read shared-domain marker for %q: %w", owningDomain, stateErr)
		}
		if len(shared) != 0 {
			continue
		}
		metaOwner, _, ownerHeight, metaErr := m.badger.GetDomainOwnerAndMeta(owningDomain)
		if metaErr != nil {
			return nil, fmt.Errorf("read pipe contact owner metadata for %q: %w", owningDomain, metaErr)
		}
		if isCanonicalAgentID(metaOwner) {
			metaOwner = strings.ToLower(metaOwner)
		}
		if metaOwner != owner {
			return nil, fmt.Errorf("pipe contact owner changed while resolving %q", owningDomain)
		}

		contactDomain := PipeContactDomain{
			Domain:       permission.Domain,
			OwningDomain: owningDomain,
			OwnerHeight:  ownerHeight,
		}
		eligible := map[string]struct{}{owner: {}}
		for agentID, agent := range agentByID {
			if agent == nil || agent.Status != "active" || agent.RemovedAt != nil || agentID == owner {
				continue
			}
			// A federated inbox requires SAGE's ordinary level-1 Read verb.
			// Passing classification 0 would admit any same-org member that can
			// view PUBLIC memories, even though it has no domain Read capability.
			// Level-2 Write is naturally included because it satisfies this bar.
			allowed, accessErr := m.badger.HasAccessMultiOrg(permission.Domain, agentID, 1, now, postV8Access)
			if accessErr != nil {
				return nil, fmt.Errorf("check pipe contact access for %q on %q: %w", agentID, permission.Domain, accessErr)
			}
			if allowed {
				eligible[agentID] = struct{}{}
			}
		}
		for agentID := range eligible {
			agg := byAgent[agentID]
			if agg == nil {
				agg = &pipeContactAggregate{agentID: agentID, domains: make([]PipeContactDomain, 0, 1)}
				byAgent[agentID] = agg
			}
			agg.domains = append(agg.domains, contactDomain)
		}
	}
	acceptances := map[string]string{}
	if policy.Revision > 0 {
		acceptances, err = ss.GetFederatedPipeContactAcceptances(ctx, *policy)
		if err != nil {
			return nil, fmt.Errorf("read pipe contact acceptance: %w", err)
		}
	}

	agentIDs := make([]string, 0, len(byAgent))
	for agentID := range byAgent {
		agentIDs = append(agentIDs, agentID)
	}
	sort.Strings(agentIDs)
	prefixes := uniqueAgentPrefixes(agentIDs)
	nodeHandle := pipeContactNodeHandle(m.NetworkName(), m.localChainID)

	contacts := make([]PipeContact, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		agg := byAgent[agentID]
		sort.Slice(agg.domains, func(i, j int) bool {
			if agg.domains[i].Domain == agg.domains[j].Domain {
				return agg.domains[i].OwningDomain < agg.domains[j].OwningDomain
			}
			return agg.domains[i].Domain < agg.domains[j].Domain
		})

		contact := PipeContact{
			AgentID:   agentID,
			Available: false,
			Accepting: false,
			Domains:   agg.domains,
		}
		if agent := agentByID[agentID]; agent != nil {
			name := agent.Name
			if name == "" {
				name = agent.RegisteredName
			}
			contact.DisplayName = sanitizeName(name)
			contact.RegisteredName = sanitizeName(agent.RegisteredName)
			contact.Provider = sanitizeName(agent.Provider)
			contact.Available = agent.Status == "active" && agent.RemovedAt == nil
		}
		if prefix := prefixes[agentID]; prefix != "" {
			contact.Address = agentID + "@" + m.localChainID
			contact.Handle = "#" + nodeHandle + "/" + prefix
			contact.ContactID, err = m.pipeContactID(peer, policy, contact)
			if err != nil {
				return nil, err
			}
			contact.Accepting = acceptances[agentID] == contact.ContactID
		}
		contacts = append(contacts, contact)
	}

	grant := &PipeContactGrant{
		Version:     PipeContactVersion,
		AgreementID: m.pipeContactAgreementID(peer, policy),
		Paused:      policy.Paused,
		Contacts:    contacts,
	}
	// Revision is an authorization barrier, not a cosmetic cache version.
	// DisplayName and Handle may change when an operator renames an agent or
	// network; neither is a trust anchor and neither may invalidate queued or
	// already-claimed work. ContactID, exact agent identity, domains,
	// availability, acceptance and Pause are the authorization inputs.
	type revisionContact struct {
		AgentID   string              `json:"agent_id"`
		ContactID string              `json:"contact_id"`
		Available bool                `json:"available"`
		Accepting bool                `json:"accepting"`
		Domains   []PipeContactDomain `json:"domains"`
	}
	revisionContacts := make([]revisionContact, 0, len(contacts))
	for _, contact := range contacts {
		revisionContacts = append(revisionContacts, revisionContact{
			AgentID: contact.AgentID, ContactID: contact.ContactID,
			Available: contact.Available, Accepting: contact.Accepting,
			Domains: contact.Domains,
		})
	}
	revisionInput := struct {
		LocalChainID string                `json:"local_chain_id"`
		PeerChainID  string                `json:"peer_chain_id"`
		PeerAgentID  string                `json:"peer_agent_id"`
		PolicyEpoch  string                `json:"policy_epoch"`
		RemoteCAPin  string                `json:"remote_ca_pin"`
		PolicyRev    int64                 `json:"policy_revision"`
		GrantVersion int                   `json:"grant_version"`
		AgreementID  string                `json:"agreement_id"`
		Paused       bool                  `json:"paused"`
		Contacts     []revisionContact     `json:"contacts"`
		Agreement    *store.CrossFedRecord `json:"agreement"`
	}{
		LocalChainID: m.localChainID,
		PeerChainID:  peer.ChainID,
		PeerAgentID:  peer.AgentID,
		PolicyEpoch:  policy.PolicyEpoch,
		RemoteCAPin:  policy.RemoteCAPin,
		PolicyRev:    policy.Revision,
		GrantVersion: grant.Version,
		AgreementID:  grant.AgreementID,
		Paused:       grant.Paused,
		Contacts:     revisionContacts,
		Agreement:    peer.Agreement,
	}
	encoded, err := json.Marshal(revisionInput)
	if err != nil {
		return nil, fmt.Errorf("encode pipe contact revision: %w", err)
	}
	sum := sha256.Sum256(encoded)
	grant.Revision = hex.EncodeToString(sum[:])
	return grant, nil
}

// pipeContactAuthorizationRevision binds one event to exactly one visible
// contact's current authorization state. The grant's aggregate Revision remains
// the whole-snapshot cache key; using it here would let an unrelated agent being
// paused, disabled, or made unavailable permanently fail queued work for this
// unchanged target. Reversible target state is included so Off/On and
// Pause/Resume return to the same revision, while ContactID carries the exact
// agreement, policy and owner/domain generation.
func pipeContactAuthorizationRevision(grant *PipeContactGrant, contact *PipeContact) string {
	if grant == nil || contact == nil {
		return ""
	}
	encoded, _ := json.Marshal(struct {
		Version     int    `json:"version"`
		AgreementID string `json:"agreement_id"`
		ContactID   string `json:"contact_id"`
		AgentID     string `json:"agent_id"`
		Paused      bool   `json:"paused"`
		Available   bool   `json:"available"`
		Accepting   bool   `json:"accepting"`
	}{
		Version: grant.Version, AgreementID: grant.AgreementID,
		ContactID: contact.ContactID, AgentID: contact.AgentID,
		Paused: grant.Paused, Available: contact.Available, Accepting: contact.Accepting,
	})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func (m *Manager) pipeContactAgreementID(peer *peerIdentity, policy *store.PeerRBACPolicy) string {
	input := struct {
		Version      int                   `json:"version"`
		LocalChainID string                `json:"local_chain_id"`
		PeerChainID  string                `json:"peer_chain_id"`
		PeerAgentID  string                `json:"peer_agent_id"`
		PolicyEpoch  string                `json:"policy_epoch"`
		RemoteCAPin  string                `json:"remote_ca_pin"`
		Agreement    *store.CrossFedRecord `json:"agreement"`
	}{PipeContactVersion, m.localChainID, peer.ChainID, peer.AgentID, policy.PolicyEpoch, policy.RemoteCAPin, peer.Agreement}
	encoded, _ := json.Marshal(input)
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

// pipeContactID binds an acceptance toggle to identity and authorization
// inputs, not display state. Pause, agent availability, names and aliases are
// intentionally excluded so a temporary stop/resume or rename preserves the
// operator's choice. The delivery path will still require available+accepting
// under a freshly re-derived contact.
func (m *Manager) pipeContactID(peer *peerIdentity, policy *store.PeerRBACPolicy, contact PipeContact) (string, error) {
	input := struct {
		Version       int                   `json:"version"`
		LocalChainID  string                `json:"local_chain_id"`
		PeerChainID   string                `json:"peer_chain_id"`
		PeerAgentID   string                `json:"peer_agent_id"`
		PolicyEpoch   string                `json:"policy_epoch"`
		RemoteCAPin   string                `json:"remote_ca_pin"`
		PolicyVersion int                   `json:"policy_version"`
		PolicyRev     int64                 `json:"policy_revision"`
		Agreement     *store.CrossFedRecord `json:"agreement"`
		AgentID       string                `json:"agent_id"`
		Domains       []PipeContactDomain   `json:"domains"`
	}{
		Version: PipeContactVersion, LocalChainID: m.localChainID,
		PeerChainID: peer.ChainID, PeerAgentID: peer.AgentID,
		PolicyEpoch: policy.PolicyEpoch, RemoteCAPin: policy.RemoteCAPin,
		PolicyVersion: policy.PolicyVersion, PolicyRev: policy.Revision,
		Agreement: peer.Agreement, AgentID: contact.AgentID, Domains: contact.Domains,
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", fmt.Errorf("encode pipe contact id: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// LocalPipeContacts returns this node's current shared-domain RBAC contact
// projection for one locally managed connection. It is the CEREBRUM-facing
// form of the same data advertised to the authenticated peer by
// /fed/v1/status.
func (m *Manager) LocalPipeContacts(ctx context.Context, remoteChainID string) (*PipeContactGrant, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, fmt.Errorf("pipe contacts require the SQLite store backend")
	}
	unlock := ss.LockSyncPolicyRead()
	defer unlock()
	if m.badger == nil {
		return nil, fmt.Errorf("pipe contacts require consensus domain state")
	}
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer ownerUnlock()
	contactUnlock := ss.LockAgentContactRead()
	defer contactUnlock()
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	policy, err := m.getPeerRBACPolicyForAgreement(ctx, agreement)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, fmt.Errorf("connection has no exact peer RBAC snapshot")
	}
	peer := &peerIdentity{ChainID: remoteChainID, AgentID: policy.PeerAgentID, Agreement: agreement}
	return m.buildPipeContactGrant(ctx, peer, policy)
}

// SetPipeContactAcceptance changes one local agent's inbound work-request
// consent. The current contact is checked before the store mutation and again
// afterwards, so an ownership transfer, RBAC revoke, or availability change
// racing the click cannot silently enable a stale recipient.
func (m *Manager) SetPipeContactAcceptance(ctx context.Context, remoteChainID, localAgentID, contactID string, accepting bool) (*PipeContactGrant, error) {
	current, err := m.LocalPipeContacts(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	found := false
	for _, contact := range current.Contacts {
		if contact.AgentID != localAgentID {
			continue
		}
		found = true
		if contact.ContactID == "" || contact.ContactID != contactID {
			return nil, ErrPipeContactChanged
		}
		if accepting && !contact.Available {
			return nil, ErrPipeContactUnavailable
		}
		break
	}
	if !found {
		return nil, ErrPipeContactChanged
	}

	policy, err := m.GetPeerRBACPolicy(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, ErrPipeContactChanged
	}
	ss := m.syncStore()
	if acceptanceErr := ss.SetBoundFederatedPipeContactAcceptance(ctx, *policy, localAgentID, contactID, accepting); acceptanceErr != nil {
		return nil, acceptanceErr
	}

	updated, err := m.LocalPipeContacts(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	for _, contact := range updated.Contacts {
		if contact.AgentID == localAgentID && contact.ContactID == contactID && contact.Accepting == accepting {
			return updated, nil
		}
	}
	return nil, ErrPipeContactChanged
}

// uniqueAgentPrefixes returns the shortest collision-free even-step prefix for
// each valid 64-hex agent ID. Friendly prefixes remain display-only; invalid
// legacy IDs get no routable address or handle.
func uniqueAgentPrefixes(agentIDs []string) map[string]string {
	lengths := make(map[string]int, len(agentIDs))
	for _, agentID := range agentIDs {
		if isCanonicalAgentID(agentID) {
			lengths[agentID] = pipeContactMinAgentPrefix
		}
	}
	for {
		groups := make(map[string][]string, len(lengths))
		for agentID, length := range lengths {
			if length > len(agentID) {
				length = len(agentID)
			}
			groups[strings.ToLower(agentID[:length])] = append(groups[strings.ToLower(agentID[:length])], agentID)
		}
		changed := false
		for _, ids := range groups {
			if len(ids) < 2 {
				continue
			}
			for _, agentID := range ids {
				if lengths[agentID] < len(agentID) {
					lengths[agentID] += pipeContactPrefixStep
					if lengths[agentID] > len(agentID) {
						lengths[agentID] = len(agentID)
					}
					changed = true
				}
			}
		}
		if !changed {
			break
		}
	}
	out := make(map[string]string, len(lengths))
	for agentID, length := range lengths {
		out[agentID] = strings.ToLower(agentID[:length])
	}
	return out
}

func isCanonicalAgentID(agentID string) bool {
	if len(agentID) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(agentID)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == agentID
}

func pipeContactNodeHandle(networkName, chainID string) string {
	source := strings.ToLower(sanitizeName(networkName))
	var b strings.Builder
	lastDash := false
	for _, r := range source {
		isASCIIAlphaNum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isASCIIAlphaNum {
			if b.Len() >= pipeContactNodeSlugMax {
				break
			}
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 && b.Len() < pipeContactNodeSlugMax {
			b.WriteByte('-')
			lastDash = true
		}
	}
	slug := strings.Trim(b.String(), "-")
	if slug == "" {
		slug = "sage"
	}
	fingerprint := sha256.Sum256([]byte(chainID))
	return slug + "-" + hex.EncodeToString(fingerprint[:4])
}
