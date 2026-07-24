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
	// A lookup returns at most 20 contacts, but authorization must be checked
	// before a result is exposed. Bound the name candidate set too, so an
	// authenticated peer cannot force a full roster × policy-domain scan while
	// holding the revocation leases.
	maxPipeContactLookupCandidates = 64
	// Legacy peers cannot ask for a named lookup and still expect the v1 status
	// envelope. Sample a deterministic active-agent subset while always keeping
	// owners eligible, rather than constructing an unbounded roster merely to
	// discard it at the wire cap.
	maxPipeContactStatusCandidates = 128
)

var (
	ErrPipeContactChanged     = errors.New("federated pipe contact changed")
	ErrPipeContactUnavailable = errors.New("federated pipe contact is unavailable")
)

type pipeContactAggregate struct {
	agentID string
	domains []PipeContactDomain
}

// buildPipeContactStatusGrant is the bounded legacy v1 status projection.
// Modern callers request compact status and use targeted lookup instead; this
// keeps an old peer from forcing full local roster × policy evaluation under
// the federation snapshot leases.
func (m *Manager) buildPipeContactStatusGrant(ctx context.Context, peer *peerIdentity, policy *store.PeerRBACPolicy) (*PipeContactGrant, error) {
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
	agents, err := ss.ListPipeContactStatusCandidates(ctx, maxPipeContactStatusCandidates)
	if err != nil {
		return nil, fmt.Errorf("list bounded local agents for pipe contacts: %w", err)
	}
	return m.buildPipeContactGrantForCandidates(ctx, peer, policy, agents, nil, true, nil, true)
}

// buildPipeContactGrantForCandidates is the common projection builder. Status
// supplies the complete local roster for legacy compatibility. Targeted lookup
// supplies a short selector-derived candidate list and does not automatically
// add every owner, because doing so would leak nonmatching contacts.
func (m *Manager) buildPipeContactGrantForCandidates(ctx context.Context, peer *peerIdentity, policy *store.PeerRBACPolicy, agents []*store.AgentEntry, candidateIDs []string, includeOwners bool, handleOverrides map[string]string, selectedAcceptances bool) (*PipeContactGrant, error) {
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
	var err error
	agentByID := make(map[string]*store.AgentEntry, len(agents))
	candidateIDSet := make(map[string]struct{}, len(agents)+len(candidateIDs))
	for _, agent := range agents {
		if agent != nil && agent.AgentID != "" {
			key := agent.AgentID
			if isCanonicalAgentID(key) {
				key = strings.ToLower(key)
			}
			agentByID[key] = agent
			candidateIDSet[key] = struct{}{}
		}
	}
	for _, agentID := range candidateIDs {
		if isCanonicalAgentID(agentID) {
			agentID = strings.ToLower(agentID)
		}
		if agentID != "" {
			candidateIDSet[agentID] = struct{}{}
		}
	}

	byAgent := make(map[string]*pipeContactAggregate)
	ownerIDs := make(map[string]struct{})
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
		if includeOwners {
			ownerIDs[owner] = struct{}{}
		}

		contactDomain := PipeContactDomain{
			Domain:       permission.Domain,
			OwningDomain: owningDomain,
			OwnerHeight:  ownerHeight,
		}
		eligible := make(map[string]struct{})
		if includeOwners {
			eligible[owner] = struct{}{}
		} else if _, selected := candidateIDSet[owner]; selected {
			eligible[owner] = struct{}{}
		}
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
	// The bounded legacy status candidate query does not necessarily contain
	// every effective owner. Owners are nevertheless always visible to an
	// authorized peer, so fetch missing metadata in a small-projection batched
	// query before materializing contacts. In particular, never call GetAgent
	// here: it computes memory counts and can turn a policy's 1,024-domain cap
	// into repeated full-table work while the federation snapshot leases are held.
	if includeOwners && len(ownerIDs) != 0 {
		missingOwners := make([]string, 0, len(ownerIDs))
		for owner := range ownerIDs {
			if _, alreadyLoaded := agentByID[owner]; !alreadyLoaded {
				missingOwners = append(missingOwners, owner)
			}
		}
		if len(missingOwners) != 0 {
			owners, ownerErr := ss.GetPipeContactAgents(ctx, missingOwners)
			if ownerErr != nil {
				return nil, fmt.Errorf("load pipe contact owners: %w", ownerErr)
			}
			for _, owner := range owners {
				if owner == nil || owner.AgentID == "" {
					continue
				}
				ownerID := owner.AgentID
				if isCanonicalAgentID(ownerID) {
					ownerID = strings.ToLower(ownerID)
				}
				agentByID[ownerID] = owner
			}
		}
	}
	acceptances := map[string]string{}
	if policy.Revision > 0 {
		if !selectedAcceptances {
			acceptances, err = ss.GetFederatedPipeContactAcceptances(ctx, *policy)
		} else {
			acceptances, err = ss.GetFederatedPipeContactAcceptancesForAgents(ctx, *policy, canonicalPipeContactAgentIDs(byAgent))
		}
		if err != nil {
			return nil, fmt.Errorf("read pipe contact acceptance: %w", err)
		}
	}

	agentIDs := agentIDsFromAggregates(byAgent)
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
			if handle := handleOverrides[agentID]; handle != "" {
				contact.Handle = handle
			}
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

func agentIDsFromAggregates(byAgent map[string]*pipeContactAggregate) []string {
	agentIDs := make([]string, 0, len(byAgent))
	for agentID := range byAgent {
		agentIDs = append(agentIDs, agentID)
	}
	return agentIDs
}

func canonicalPipeContactAgentIDs(byAgent map[string]*pipeContactAggregate) []string {
	all := agentIDsFromAggregates(byAgent)
	canonical := all[:0]
	for _, agentID := range all {
		if isCanonicalAgentID(agentID) {
			canonical = append(canonical, agentID)
		}
	}
	return canonical
}

// buildPipeContactLookupGrant first narrows the local candidate set with a
// bounded SQLite query, then evaluates the normal live Badger RBAC rules only
// for those candidates. It intentionally trades an unbounded fuzzy-name scan
// for deterministic first-candidate behavior; exact address routing remains a
// point lookup and is never subject to the name candidate cap.
func (m *Manager) buildPipeContactLookupGrant(ctx context.Context, peer *peerIdentity, policy *store.PeerRBACPolicy, req PipeContactLookupRequest) (*PipeContactGrant, int, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, 0, fmt.Errorf("pipe contacts require SQLite")
	}
	var (
		agents          []*store.AgentEntry
		candidateIDs    []string
		handleOverrides map[string]string
		err             error
	)
	if req.Name != "" {
		agents, err = ss.FindPipeContactLookupCandidates(ctx, req.Name, maxPipeContactLookupCandidates)
	} else if agentID, chainID := splitPipeAddress(req.Target); chainID != "" {
		if chainID != m.localChainID {
			grant, buildErr := m.buildPipeContactGrantForCandidates(ctx, peer, policy, nil, nil, false, nil, true)
			return grant, 0, buildErr
		}
		candidateIDs = append(candidateIDs, agentID)
		agents, err = ss.GetPipeContactAgents(ctx, []string{agentID})
	} else if strings.HasPrefix(req.Target, "#") {
		prefix, ok := pipeContactHandlePrefix(req.Target, pipeContactNodeHandle(m.NetworkName(), m.localChainID))
		if !ok {
			grant, buildErr := m.buildPipeContactGrantForCandidates(ctx, peer, policy, nil, nil, false, nil, true)
			return grant, 0, buildErr
		}
		agents, err = ss.FindPipeContactAgentsByIDPrefix(ctx, prefix, maxPipeContactLookupCandidates)
		handleOverrides = make(map[string]string, len(agents))
		for _, agent := range agents {
			if agent != nil {
				handleOverrides[strings.ToLower(agent.AgentID)] = req.Target
			}
		}
	} else if isCanonicalAgentID(req.Target) {
		candidateIDs = append(candidateIDs, strings.ToLower(req.Target))
		agents, err = ss.GetPipeContactAgents(ctx, []string{req.Target})
	} else {
		agents, err = ss.FindPipeContactLookupCandidates(ctx, req.Target, maxPipeContactLookupCandidates)
		if err == nil {
			filtered := agents[:0]
			for _, agent := range agents {
				if agent != nil && strings.EqualFold(pipeContactAgentDisplayName(agent), req.Target) {
					filtered = append(filtered, agent)
				}
			}
			agents = filtered
		}
	}
	if err != nil {
		return nil, 0, fmt.Errorf("find pipe contact candidates: %w", err)
	}
	grant, err := m.buildPipeContactGrantForCandidates(ctx, peer, policy, agents, candidateIDs, false, handleOverrides, true)
	if err != nil {
		return nil, 0, err
	}
	filtered, total := filterPipeContactLookup(grant, req)
	return filtered, total, nil
}

func pipeContactAgentDisplayName(agent *store.AgentEntry) string {
	if agent == nil {
		return ""
	}
	if agent.Name != "" {
		return agent.Name
	}
	return agent.RegisteredName
}

func pipeContactHandlePrefix(handle, nodeHandle string) (string, bool) {
	prefix := strings.TrimPrefix(handle, "#"+nodeHandle+"/")
	if prefix == handle || len(prefix) < pipeContactMinAgentPrefix || len(prefix) > 64 {
		return "", false
	}
	for _, char := range prefix {
		if !((char >= 'a' && char <= 'f') || (char >= '0' && char <= '9')) {
			return "", false
		}
	}
	return prefix, true
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

// LocalPipeContacts returns the bounded CEREBRUM administrative projection for
// one locally managed connection. The browser must not make an unbounded roster
// × policy-domain scan under revocation locks; callers that are changing one
// contact add that exact agent to this bounded view internally.
func (m *Manager) LocalPipeContacts(ctx context.Context, remoteChainID string) (*PipeContactGrant, error) {
	return m.localPipeContacts(ctx, remoteChainID, "")
}

// LocalPipeContactsForAgent augments the bounded administrative view with one
// exact local recipient. It gives the operator a safe way to enable a shared
// reader outside the default sample without turning the dashboard into a full
// roster query. The returned contact ID is still derived live and must be sent
// back unchanged by SetPipeContactAcceptance.
func (m *Manager) LocalPipeContactsForAgent(ctx context.Context, remoteChainID, localAgentID string) (*PipeContactGrant, error) {
	if !isCanonicalAgentID(localAgentID) {
		return nil, fmt.Errorf("local agent id must be a canonical 64-hex Ed25519 public key")
	}
	return m.localPipeContacts(ctx, remoteChainID, localAgentID)
}

func (m *Manager) localPipeContacts(ctx context.Context, remoteChainID, selectedAgentID string) (*PipeContactGrant, error) {
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
	agents, err := ss.ListPipeContactStatusCandidates(ctx, maxPipeContactStatusCandidates)
	if err != nil {
		return nil, fmt.Errorf("list bounded local agents for pipe contacts: %w", err)
	}
	candidateIDs := []string(nil)
	if selectedAgentID != "" {
		candidateIDs = []string{selectedAgentID}
		selected, selectedErr := ss.GetPipeContactAgents(ctx, candidateIDs)
		if selectedErr != nil {
			return nil, fmt.Errorf("load selected local pipe contact: %w", selectedErr)
		}
		agents = append(agents, selected...)
	}
	return m.buildPipeContactGrantForCandidates(ctx, peer, policy, agents, candidateIDs, true, nil, true)
}

// SetPipeContactAcceptance changes one local agent's inbound work-request
// consent. The current contact is checked before the store mutation and again
// afterwards, so an ownership transfer, RBAC revoke, or availability change
// racing the click cannot silently enable a stale recipient.
func (m *Manager) SetPipeContactAcceptance(ctx context.Context, remoteChainID, localAgentID, contactID string, accepting bool) (*PipeContactGrant, error) {
	current, err := m.localPipeContacts(ctx, remoteChainID, localAgentID)
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

	updated, err := m.localPipeContacts(ctx, remoteChainID, localAgentID)
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
