package federation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	linkedMessageDirectoryVersion          = 1
	maxLinkedMessageDirectoryRequestBytes  = 1 << 20
	maxLinkedMessageDirectoryResponseBytes = 512 << 10
	maxLinkedMessageDirectoryNameBytes     = 512
	maxLinkedMessageDirectoryResults       = 20
	minLinkedMessageDirectoryNameRunes     = 2
)

// linkedMessageGuestInventory is deliberately separate from
// FederatedGuestStore. Query-only test stores and alternate read backends do
// not gain an inventory capability merely because exact guest lookup exists.
type linkedMessageGuestInventory interface {
	ListFederatedGuestIdentities(context.Context) ([]store.FederatedGuestIdentity, error)
}

// LinkedMessageDirectoryRequest is a peer-authenticated, caller-scoped name
// query. guest_to_member asks the link host to derive exact eligible members
// for SourceAgentID. member_to_guest carries only host-signed relations the
// caller already owns; it never asks the receiver to enumerate its roster.
type LinkedMessageDirectoryRequest struct {
	Version       int                      `json:"version"`
	Direction     string                   `json:"direction"`
	SourceAgentID string                   `json:"source_agent_id"`
	Name          string                   `json:"name"`
	Limit         int                      `json:"limit"`
	Relations     []*LinkedMessageRelation `json:"relations,omitempty"`
}

type LinkedMessageDirectoryEntry struct {
	AgentID         string                 `json:"agent_id"`
	DisplayName     string                 `json:"display_name,omitempty"`
	RegisteredName  string                 `json:"registered_name,omitempty"`
	Provider        string                 `json:"provider,omitempty"`
	Address         string                 `json:"address"`
	ConsentRevision int64                  `json:"consent_revision"`
	Relation        *LinkedMessageRelation `json:"relation"`
}

type LinkedMessageDirectoryResponse struct {
	Version       int                           `json:"version"`
	ChainID       string                        `json:"chain_id"`
	Direction     string                        `json:"direction"`
	SourceAgentID string                        `json:"source_agent_id"`
	Entries       []LinkedMessageDirectoryEntry `json:"entries"`
}

// LinkedMessageDirectoryResult is the local, already-authorized projection
// consumed by REST. It intentionally contains no relation bytes, group IDs,
// consent revisions, domains, presence bit, delivery state, or read state.
type LinkedMessageDirectoryResult struct {
	Contacts []PipeContact `json:"contacts"`
}

func validLinkedMessageDirectoryName(name string) bool {
	return name != "" && len(name) <= maxLinkedMessageDirectoryNameBytes &&
		utf8.ValidString(name) &&
		utf8.RuneCountInString(name) >= minLinkedMessageDirectoryNameRunes
}

func linkedDirectoryContactFromAgent(agent *store.AgentEntry, chainID string) PipeContact {
	if agent == nil {
		return PipeContact{}
	}
	displayName := agent.Name
	if displayName == "" {
		displayName = agent.RegisteredName
	}
	return PipeContact{
		AgentID:           strings.ToLower(agent.AgentID),
		DisplayName:       sanitizeName(displayName),
		RegisteredName:    sanitizeName(agent.RegisteredName),
		Provider:          sanitizeName(agent.Provider),
		Address:           strings.ToLower(agent.AgentID) + "@" + chainID,
		AuthorizationMode: LinkedMessageAuthorizationMode,
		Available:         false,
		Accepting:         false,
		Domains:           []PipeContactDomain{},
	}
}

func linkedDirectoryEntryFromAgent(
	agent *store.AgentEntry,
	chainID string,
	relation *LinkedMessageRelation,
	consentRevision int64,
) LinkedMessageDirectoryEntry {
	contact := linkedDirectoryContactFromAgent(agent, chainID)
	return LinkedMessageDirectoryEntry{
		AgentID: contact.AgentID, DisplayName: contact.DisplayName,
		RegisteredName: contact.RegisteredName, Provider: contact.Provider,
		Address: contact.Address, ConsentRevision: consentRevision,
		Relation: relation,
	}
}

func validateLinkedDirectoryEntryMetadata(
	chainID string,
	entry LinkedMessageDirectoryEntry,
) error {
	if !isCanonicalAgentID(entry.AgentID) ||
		entry.AgentID != strings.ToLower(entry.AgentID) ||
		entry.Address != entry.AgentID+"@"+chainID ||
		entry.ConsentRevision <= 0 || entry.Relation == nil ||
		len(entry.DisplayName) > maxLinkedMessageDirectoryNameBytes ||
		len(entry.RegisteredName) > maxLinkedMessageDirectoryNameBytes ||
		len(entry.Provider) > maxLinkedMessageDirectoryNameBytes {
		return ErrFederatedPipeInvalid
	}
	for _, value := range []string{
		entry.DisplayName, entry.RegisteredName, entry.Provider,
	} {
		if sanitizeName(value) != value {
			return ErrFederatedPipeInvalid
		}
	}
	return nil
}

// ValidateLinkedMessageDirectoryResult is the REST package's defensive shape
// check. Authority has already been verified by Manager, but a fake or future
// adapter still cannot turn malformed metadata into a contactable result.
func ValidateLinkedMessageDirectoryResult(
	remoteChainID string,
	result *LinkedMessageDirectoryResult,
) error {
	if result == nil || ValidateChainID(remoteChainID) != nil ||
		len(result.Contacts) > maxLinkedMessageDirectoryResults {
		return ErrFederatedPipeInvalid
	}
	seen := make(map[string]struct{}, len(result.Contacts))
	for _, contact := range result.Contacts {
		if !isCanonicalAgentID(contact.AgentID) ||
			contact.AgentID != strings.ToLower(contact.AgentID) ||
			contact.Address != contact.AgentID+"@"+remoteChainID ||
			contact.ContactID != "" || contact.Handle != "" ||
			contact.AuthorizationMode != LinkedMessageAuthorizationMode ||
			len(contact.Domains) != 0 || contact.Available || contact.Accepting ||
			len(contact.DisplayName) > maxLinkedMessageDirectoryNameBytes ||
			len(contact.RegisteredName) > maxLinkedMessageDirectoryNameBytes ||
			len(contact.Provider) > maxLinkedMessageDirectoryNameBytes {
			return ErrFederatedPipeInvalid
		}
		if _, duplicate := seen[contact.AgentID]; duplicate {
			return ErrFederatedPipeInvalid
		}
		seen[contact.AgentID] = struct{}{}
		for _, value := range []string{
			contact.DisplayName, contact.RegisteredName, contact.Provider,
		} {
			if sanitizeName(value) != value {
				return ErrFederatedPipeInvalid
			}
		}
	}
	return nil
}

func linkedDirectoryUnavailable(w http.ResponseWriter) {
	// Every missing/denied/malformed tuple has one response. A peer cannot use
	// this route to distinguish an absent name from a revoked group or consent.
	httpError(w, http.StatusNotFound, "linked message directory unavailable")
}

func decodeLinkedMessageDirectoryRequest(
	w http.ResponseWriter,
	r *http.Request,
) (*LinkedMessageDirectoryRequest, bool) {
	var req LinkedMessageDirectoryRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxLinkedMessageDirectoryRequestBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&req); err != nil {
		linkedDirectoryUnavailable(w)
		return nil, false
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		linkedDirectoryUnavailable(w)
		return nil, false
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Version != linkedMessageDirectoryVersion ||
		(req.Direction != LinkedMessageGuestToMember &&
			req.Direction != LinkedMessageMemberToGuest) ||
		!validLinkedMessageDirectoryName(req.Name) ||
		req.Limit < 1 || req.Limit > maxLinkedMessageDirectoryResults ||
		!isCanonicalAgentID(req.SourceAgentID) ||
		req.SourceAgentID != strings.ToLower(req.SourceAgentID) ||
		len(req.Relations) > store.MaxFederatedGuestIdentities {
		linkedDirectoryUnavailable(w)
		return nil, false
	}
	if req.Direction == LinkedMessageGuestToMember && len(req.Relations) != 0 {
		linkedDirectoryUnavailable(w)
		return nil, false
	}
	if req.Direction == LinkedMessageMemberToGuest && len(req.Relations) == 0 {
		linkedDirectoryUnavailable(w)
		return nil, false
	}
	return &req, true
}

func boundedLinkedMessageDirectoryResponse(
	response LinkedMessageDirectoryResponse,
) (*LinkedMessageDirectoryResponse, error) {
	bounded := response
	bounded.Entries = make([]LinkedMessageDirectoryEntry, 0, len(response.Entries))
	for _, entry := range response.Entries {
		candidate := bounded
		candidate.Entries = append(
			append([]LinkedMessageDirectoryEntry(nil), bounded.Entries...), entry,
		)
		encoded, err := json.Marshal(&candidate)
		if err != nil {
			return nil, err
		}
		if len(encoded) > maxLinkedMessageDirectoryResponseBytes {
			break
		}
		bounded.Entries = candidate.Entries
	}
	return &bounded, nil
}

// handleLinkedMessageDirectory performs bounded name matching only after
// peerAuth has authenticated the exact remote node. It returns no total or
// truncation bit because either would leak hidden linked principals.
func (m *Manager) handleLinkedMessageDirectory(
	w http.ResponseWriter,
	r *http.Request,
) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	req, ok := decodeLinkedMessageDirectoryRequest(w, r)
	if !ok {
		return
	}
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		linkedDirectoryUnavailable(w)
		return
	}
	lookupCtx, cancel := context.WithTimeout(
		r.Context(), pipeContactLookupQueryTimeout,
	)
	candidates, err := ss.FindPipeContactLookupCandidates(
		lookupCtx, req.Name, maxPipeContactLookupCandidates,
	)
	cancel()
	if err != nil {
		linkedDirectoryUnavailable(w)
		return
	}

	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	contactUnlock := ss.LockAgentContactRead()
	defer policyUnlock()
	defer ownerUnlock()
	defer contactUnlock()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		linkedDirectoryUnavailable(w)
		return
	}
	policy, _, err := m.currentLinkedMessagePolicy(
		r.Context(), agreement, peer.AgentID,
	)
	if err != nil {
		linkedDirectoryUnavailable(w)
		return
	}

	entries := make([]LinkedMessageDirectoryEntry, 0, req.Limit)
	switch req.Direction {
	case LinkedMessageGuestToMember:
		for _, candidate := range candidates {
			if len(entries) >= req.Limit || candidate == nil {
				break
			}
			relation, _, buildErr := m.buildHostedLinkedRelation(
				r.Context(), peer, agreement, req.Direction,
				req.SourceAgentID, strings.ToLower(candidate.AgentID),
			)
			if buildErr != nil {
				continue
			}
			contact := linkedDirectoryContactFromAgent(candidate, m.localChainID)
			exact, partial := pipeContactMatchesName(req.Name, contact)
			if !exact && !partial {
				continue
			}
			entries = append(entries, linkedDirectoryEntryFromAgent(
				candidate, m.localChainID, relation,
				relation.ReceiverConsentRevision,
			))
		}
	case LinkedMessageMemberToGuest:
		relations := make(map[string]*LinkedMessageRelation, len(req.Relations))
		for _, relation := range req.Relations {
			if relation == nil ||
				relation.Direction != LinkedMessageMemberToGuest ||
				relation.HostChainID != peer.ChainID ||
				relation.PeerChainID != m.localChainID ||
				relation.SourceAgentID != req.SourceAgentID ||
				relation.TargetAgentID != relation.Guest.RemoteAgentID ||
				validateLinkedMessageRelation(relation, peer.AgentID) != nil {
				linkedDirectoryUnavailable(w)
				return
			}
			if _, duplicate := relations[relation.TargetAgentID]; duplicate {
				linkedDirectoryUnavailable(w)
				return
			}
			relations[relation.TargetAgentID] = relation
		}
		for _, candidate := range candidates {
			if len(entries) >= req.Limit || candidate == nil {
				break
			}
			targetID := strings.ToLower(candidate.AgentID)
			relation := relations[targetID]
			if relation == nil || !m.linkedMessageLocalAgentEligible(targetID) {
				continue
			}
			consent, consentErr := m.receiverLinkedMessageConsent(
				r.Context(), policy, req.SourceAgentID, targetID,
			)
			if consentErr != nil {
				continue
			}
			contact := linkedDirectoryContactFromAgent(candidate, m.localChainID)
			exact, partial := pipeContactMatchesName(req.Name, contact)
			if !exact && !partial {
				continue
			}
			entries = append(entries, linkedDirectoryEntryFromAgent(
				candidate, m.localChainID, relation, consent.Revision,
			))
		}
	}
	response, err := boundedLinkedMessageDirectoryResponse(
		LinkedMessageDirectoryResponse{
			Version: linkedMessageDirectoryVersion, ChainID: m.localChainID,
			Direction: req.Direction, SourceAgentID: req.SourceAgentID,
			Entries: entries,
		},
	)
	if err != nil {
		linkedDirectoryUnavailable(w)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func (m *Manager) callLinkedMessageDirectory(
	ctx context.Context,
	agreement *store.CrossFedRecord,
	req LinkedMessageDirectoryRequest,
) (*LinkedMessageDirectoryResponse, error) {
	requestCtx, cancel := context.WithTimeout(ctx, linkedMessageResolveTimeout)
	defer cancel()
	body, status, err := m.doPeerRequest(
		requestCtx, agreement, http.MethodPost,
		"/fed/v1/pipe/linked/directory", req,
	)
	if err != nil || status != http.StatusOK {
		return nil, ErrRemotePipeTargetNotFound
	}
	var response LinkedMessageDirectoryResponse
	if json.Unmarshal(body, &response) != nil ||
		response.Version != linkedMessageDirectoryVersion ||
		response.ChainID != agreement.RemoteChainID ||
		response.Direction != req.Direction ||
		response.SourceAgentID != req.SourceAgentID ||
		len(response.Entries) > req.Limit {
		return nil, ErrFederatedPipeInvalid
	}
	return &response, nil
}

func sortLinkedDirectoryContacts(name string, contacts []PipeContact) {
	sort.Slice(contacts, func(i, j int) bool {
		iExact, _ := pipeContactMatchesName(name, contacts[i])
		jExact, _ := pipeContactMatchesName(name, contacts[j])
		if iExact != jExact {
			return iExact
		}
		iName := strings.ToLower(contacts[i].DisplayName)
		jName := strings.ToLower(contacts[j].DisplayName)
		if iName != jName {
			return iName < jName
		}
		return contacts[i].AgentID < contacts[j].AgentID
	})
}

func (m *Manager) hostedDirectoryRelations(
	ctx context.Context,
	peer *peerIdentity,
	agreement *store.CrossFedRecord,
	sourceAgentID string,
) ([]*LinkedMessageRelation, error) {
	inventory, ok := m.federatedGuestStore.(linkedMessageGuestInventory)
	if !ok {
		return nil, ErrRemotePipeTargetNotFound
	}
	identities, err := inventory.ListFederatedGuestIdentities(ctx)
	if err != nil || len(identities) > store.MaxFederatedGuestIdentities {
		return nil, ErrFederatedPipeInvalid
	}
	relations := make([]*LinkedMessageRelation, 0)
	for _, identity := range identities {
		if identity.RemoteChainID != agreement.RemoteChainID {
			continue
		}
		relation, _, buildErr := m.buildHostedLinkedRelation(
			ctx, peer, agreement, LinkedMessageMemberToGuest,
			sourceAgentID, identity.RemoteAgentID,
		)
		if buildErr == nil {
			relations = append(relations, relation)
		}
	}
	sort.Slice(relations, func(i, j int) bool {
		return relations[i].TargetAgentID < relations[j].TargetAgentID
	})
	return relations, nil
}

// FindRemoteLinkedMessageContacts performs a live, caller-scoped friendly-name
// lookup over one authenticated peer. Both linked directions are checked, and
// every returned address is suitable for the existing exact
// ResolveRemoteLinkedPipeTarget path. No result is persisted or cached here.
func (m *Manager) FindRemoteLinkedMessageContacts(
	ctx context.Context,
	remoteChainID, sourceAgentID, name string,
	limit int,
) (*LinkedMessageDirectoryResult, error) {
	name = strings.TrimSpace(name)
	if !validLinkedMessageDirectoryName(name) ||
		!isCanonicalAgentID(sourceAgentID) ||
		sourceAgentID != strings.ToLower(sourceAgentID) ||
		!m.linkedMessageLocalAgentEligible(sourceAgentID) {
		return &LinkedMessageDirectoryResult{Contacts: []PipeContact{}}, nil
	}
	if limit <= 0 || limit > maxLinkedMessageDirectoryResults {
		limit = maxLinkedMessageDirectoryResults
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return &LinkedMessageDirectoryResult{Contacts: []PipeContact{}}, nil
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return &LinkedMessageDirectoryResult{Contacts: []PipeContact{}}, nil
	}
	peer := &peerIdentity{
		ChainID: remoteChainID, AgentID: peerAgentID, Agreement: agreement,
	}

	// Build host-owned offers under local leases, then release every lease
	// before the peer request. A slow direct/relay route cannot block a local
	// group, guest, consent, or agreement mutation.
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		return &LinkedMessageDirectoryResult{Contacts: []PipeContact{}}, nil
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	hosted, hostedErr := m.hostedDirectoryRelations(
		ctx, peer, agreement, sourceAgentID,
	)
	ownerUnlock()
	policyUnlock()
	if hostedErr != nil && !errors.Is(hostedErr, ErrRemotePipeTargetNotFound) {
		return nil, hostedErr
	}

	responses := make([]struct {
		direction string
		response  *LinkedMessageDirectoryResponse
	}, 0, 2)
	if len(hosted) != 0 {
		response, callErr := m.callLinkedMessageDirectory(
			ctx, agreement, LinkedMessageDirectoryRequest{
				Version:       linkedMessageDirectoryVersion,
				Direction:     LinkedMessageMemberToGuest,
				SourceAgentID: sourceAgentID, Name: name, Limit: limit,
				Relations: hosted,
			},
		)
		if callErr == nil {
			responses = append(responses, struct {
				direction string
				response  *LinkedMessageDirectoryResponse
			}{LinkedMessageMemberToGuest, response})
		}
	}
	response, callErr := m.callLinkedMessageDirectory(
		ctx, agreement, LinkedMessageDirectoryRequest{
			Version:       linkedMessageDirectoryVersion,
			Direction:     LinkedMessageGuestToMember,
			SourceAgentID: sourceAgentID, Name: name, Limit: limit,
		},
	)
	if callErr == nil {
		responses = append(responses, struct {
			direction string
			response  *LinkedMessageDirectoryResponse
		}{LinkedMessageGuestToMember, response})
	}

	hostedByTarget := make(map[string]*LinkedMessageRelation, len(hosted))
	for _, relation := range hosted {
		hostedByTarget[relation.TargetAgentID] = relation
	}
	contacts := make([]PipeContact, 0, limit)
	seen := make(map[string]struct{})
	hostedReturned := make(map[string]*LinkedMessageRelation)
	for _, result := range responses {
		for _, entry := range result.response.Entries {
			contact := PipeContact{
				AgentID: entry.AgentID, DisplayName: entry.DisplayName,
				RegisteredName: entry.RegisteredName, Provider: entry.Provider,
				Address:           entry.Address,
				AuthorizationMode: LinkedMessageAuthorizationMode,
				Available:         false, Accepting: false,
				Domains: []PipeContactDomain{},
			}
			exact, partial := pipeContactMatchesName(name, contact)
			if validateLinkedDirectoryEntryMetadata(remoteChainID, entry) != nil ||
				entry.Relation.SourceAgentID != sourceAgentID ||
				entry.Relation.TargetAgentID != entry.AgentID || (!exact && !partial) {
				return nil, ErrFederatedPipeInvalid
			}
			switch result.direction {
			case LinkedMessageGuestToMember:
				if entry.Relation.Direction != LinkedMessageGuestToMember ||
					entry.Relation.HostChainID != remoteChainID ||
					entry.Relation.PeerChainID != m.localChainID ||
					entry.Relation.ReceiverConsentRevision != entry.ConsentRevision ||
					validateLinkedMessageRelation(entry.Relation, peerAgentID) != nil {
					return nil, ErrFederatedPipeInvalid
				}
			case LinkedMessageMemberToGuest:
				original := hostedByTarget[entry.AgentID]
				if original == nil ||
					!linkedMessageRelationEqual(original, entry.Relation) {
					return nil, ErrFederatedPipeInvalid
				}
				updated := *original
				updated.ReceiverConsentRevision = entry.ConsentRevision
				if err := updated.sign(m.agentKey); err != nil {
					return nil, ErrFederatedPipeInvalid
				}
				entry.Relation = &updated
			default:
				return nil, ErrFederatedPipeInvalid
			}
			if _, duplicate := seen[entry.AgentID]; duplicate {
				continue
			}
			seen[entry.AgentID] = struct{}{}
			if result.direction == LinkedMessageMemberToGuest {
				hostedReturned[entry.AgentID] = entry.Relation
			}
			contacts = append(contacts, contact)
		}
	}

	// The remote response may race a local group/guest/policy mutation. Repeat
	// every locally-hosted relation under current leases before exposing its
	// friendly metadata. Exact resolve/send repeat both halves again.
	policyUnlock = ss.LockSyncPolicyRead()
	ownerUnlock = m.badger.LockDomainOwnershipRead()
	filtered := contacts[:0]
	for _, contact := range contacts {
		if relation := hostedReturned[contact.AgentID]; relation != nil {
			if m.revalidateHostedLinkedRelation(
				ctx, peer, agreement, relation,
			) != nil {
				continue
			}
		}
		filtered = append(filtered, contact)
	}
	ownerUnlock()
	policyUnlock()
	contacts = filtered
	currentAgreement, currentErr := m.ActiveAgreement(remoteChainID)
	if currentErr != nil || !sameAgreementGeneration(agreement, currentAgreement) ||
		!m.linkedMessageLocalAgentEligible(sourceAgentID) {
		return &LinkedMessageDirectoryResult{Contacts: []PipeContact{}}, nil
	}
	sortLinkedDirectoryContacts(name, contacts)
	if len(contacts) > limit {
		contacts = contacts[:limit]
	}
	result := &LinkedMessageDirectoryResult{Contacts: contacts}
	if ValidateLinkedMessageDirectoryResult(remoteChainID, result) != nil {
		return nil, ErrFederatedPipeInvalid
	}
	return result, nil
}
