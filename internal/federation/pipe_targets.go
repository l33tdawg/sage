package federation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/l33tdawg/sage/internal/store"
)

var (
	ErrRemotePipeTargetNotFound       = errors.New("remote pipe target not found")
	ErrRemotePipeTargetAmbiguous      = errors.New("remote pipe target is ambiguous")
	ErrRemotePipeTargetUnavailable    = errors.New("remote pipe target is unavailable")
	ErrRemotePipeTargetNotAccepting   = errors.New("remote pipe target is not accepting work requests")
	ErrRemotePipePeerUnsupported      = errors.New("remote SAGE does not support federated pipeline delivery")
	ErrRemotePipeResolutionIncomplete = errors.New("remote pipe target resolution is incomplete")
)

type remotePipeCandidate struct {
	chainID     string
	grant       *PipeContactGrant
	contact     PipeContact
	policyEpoch string
}

func pipeRoutingAgreementID(agreement *store.CrossFedRecord) string {
	if agreement == nil {
		return ""
	}
	encoded, _ := json.Marshal(struct {
		RemoteChainID  string   `json:"remote_chain_id"`
		Endpoint       string   `json:"endpoint"`
		PeerPubKey     []byte   `json:"peer_pub_key"`
		MaxClearance   uint8    `json:"max_clearance"`
		ExpiresAt      int64    `json:"expires_at"`
		AllowedDomains []string `json:"allowed_domains"`
		AllowedDepts   []string `json:"allowed_depts"`
		Status         string   `json:"status"`
	}{agreement.RemoteChainID, agreement.Endpoint, agreement.PeerPubKey, agreement.MaxClearance,
		agreement.ExpiresAt, agreement.AllowedDomains, agreement.AllowedDepts, agreement.Status})
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])
}

func hasFederatedPipelineCapability(status *StatusResponse) bool {
	return status != nil && slices.Contains(status.Capabilities, CapabilityFederatedPipeline)
}

func hasFederatedPipelineContactLookupCapability(status *StatusResponse) bool {
	return status != nil && slices.Contains(status.Capabilities, CapabilityFederatedPipelineContactLookup)
}

func validateRemotePipeContactGrant(remoteChainID string, grant *PipeContactGrant) error {
	if grant == nil || grant.Version != PipeContactVersion || !isPipeDigest(grant.AgreementID) ||
		!isPipeDigest(grant.Revision) || len(grant.Contacts) > maxPipeContactStatusContacts {
		return fmt.Errorf("invalid pipe contact snapshot")
	}
	seen := make(map[string]struct{}, len(grant.Contacts))
	for _, contact := range grant.Contacts {
		if len(contact.DisplayName) > 512 || len(contact.RegisteredName) > 512 ||
			len(contact.Provider) > 512 || len(contact.Handle) > 512 ||
			len(contact.Domains) > store.MaxPeerRBACPolicyDomains {
			return fmt.Errorf("invalid pipe contact snapshot")
		}
		if !isCanonicalAgentID(contact.AgentID) {
			// Legacy/non-agent domain owners may remain visible as unroutable
			// metadata, but they can never carry an address or authorization id.
			if contact.Address != "" || contact.Handle != "" || contact.ContactID != "" {
				return fmt.Errorf("invalid unroutable pipe contact")
			}
			continue
		}
		if contact.AgentID != strings.ToLower(contact.AgentID) || !isPipeDigest(contact.ContactID) ||
			contact.Address != contact.AgentID+"@"+remoteChainID || !strings.HasPrefix(contact.Handle, "#") {
			return fmt.Errorf("invalid routable pipe contact")
		}
		if _, duplicate := seen[contact.AgentID]; duplicate {
			return fmt.Errorf("duplicate routable pipe contact")
		}
		seen[contact.AgentID] = struct{}{}
	}
	return nil
}

// ValidateRemotePipeContactGrant validates an authenticated contact response
// before another package exposes it for discovery. It is deliberately shared
// with the send resolver so malformed peer metadata never becomes a
// contactable-looking MCP result.
func ValidateRemotePipeContactGrant(remoteChainID string, grant *PipeContactGrant) error {
	return validateRemotePipeContactGrant(remoteChainID, grant)
}

func sameRemotePipeContactBinding(a, b *store.SyncControl) bool {
	return a != nil && b != nil && a.RemoteChainID == b.RemoteChainID &&
		a.PeerAgentID == b.PeerAgentID && a.PolicyEpoch == b.PolicyEpoch &&
		a.RemoteCAPin == b.RemoteCAPin && a.BindingState == b.BindingState &&
		a.RemotePolicyVersion == b.RemotePolicyVersion && a.RemoteRevision == b.RemoteRevision &&
		a.RemotePolicyHash == b.RemotePolicyHash
}

// refreshRemotePipeContactCache replaces the encrypted routing hint only under
// the immutable agreement + sync binding captured before the status request.
// Every replacement and invalidation is conditional, so a delayed response
// from an old generation cannot relabel itself with current policy state or
// delete a newer cache row.
func (m *Manager) refreshRemotePipeContactCache(ctx context.Context, requested *store.CrossFedRecord, requestedControl *store.SyncControl, status *StatusResponse) error {
	ss := m.syncStore()
	if ss == nil || requested == nil || requestedControl == nil {
		return nil
	}
	localAgreementID := pipeRoutingAgreementID(requested)
	if !hasFederatedPipelineCapability(status) {
		return ss.DeleteFederatedPipeRemoteContactSnapshotBound(ctx, *requestedControl, localAgreementID)
	}
	if hasFederatedPipelineContactLookupCapability(status) {
		// Targeted lookup results are live-only, but a normal status request may
		// still carry a complete, revision-bound legacy v1 snapshot. Preserve (or
		// refresh) that snapshot for exact-address offline queueing. A compact
		// status has no snapshot at all, so it must neither create nor invalidate
		// the last independently authenticated legacy route hint.
		if status.PipeContacts == nil {
			return nil
		}
		return m.cacheRemotePipeContactGrant(ctx, requested, requestedControl, status.PipeContacts)
	}
	if status.PipeContacts == nil {
		return ss.DeleteFederatedPipeRemoteContactSnapshotBound(ctx, *requestedControl, localAgreementID)
	}
	return m.cacheRemotePipeContactGrant(ctx, requested, requestedControl, status.PipeContacts)
}

func (m *Manager) cacheRemotePipeContactGrant(ctx context.Context, requested *store.CrossFedRecord, requestedControl *store.SyncControl, grant *PipeContactGrant) error {
	ss := m.syncStore()
	if ss == nil || requested == nil || requestedControl == nil {
		return nil
	}
	localAgreementID := pipeRoutingAgreementID(requested)
	invalidate := func() {
		_ = ss.DeleteFederatedPipeRemoteContactSnapshotBound(context.Background(), *requestedControl, localAgreementID)
	}
	if err := validateRemotePipeContactGrant(requested.RemoteChainID, grant); err != nil {
		invalidate()
		return err
	}
	current, err := m.ActiveAgreement(requested.RemoteChainID)
	if err != nil || !sameAgreementGeneration(requested, current) {
		invalidate()
		return fmt.Errorf("federation agreement changed during pipe contact refresh")
	}
	control, err := ss.GetSyncControl(ctx, requested.RemoteChainID)
	if err != nil || !sameRemotePipeContactBinding(requestedControl, control) ||
		requestedControl.BindingState != "active" || requestedControl.RemoteCAPin != hex.EncodeToString(current.PeerPubKey) ||
		!isCanonicalAgentID(requestedControl.PeerAgentID) {
		invalidate()
		return fmt.Errorf("active pipe policy binding unavailable")
	}
	encoded, err := json.Marshal(grant)
	if err != nil {
		invalidate()
		return fmt.Errorf("encode remote pipe contacts: %w", err)
	}
	putErr := ss.PutFederatedPipeRemoteContactSnapshot(ctx, store.FederatedPipeRemoteContactSnapshot{
		RemoteChainID: requested.RemoteChainID, PeerAgentID: requestedControl.PeerAgentID,
		PolicyEpoch: requestedControl.PolicyEpoch, RemoteCAPin: requestedControl.RemoteCAPin,
		RemotePolicyVersion: requestedControl.RemotePolicyVersion, RemotePolicyRevision: requestedControl.RemoteRevision,
		RemotePolicyHash: requestedControl.RemotePolicyHash, LocalAgreementID: localAgreementID,
		RemoteAgreementID: grant.AgreementID,
		ContactRevision:   grant.Revision, Snapshot: encoded,
	})
	if putErr != nil {
		// In particular, vault-locked encryption must not leave an older snapshot
		// eligible to resurrect after unlock plus outage.
		invalidate()
	}
	return putErr
}

func (m *Manager) lookupRemotePipeContacts(ctx context.Context, agreement *store.CrossFedRecord, req *PipeContactLookupRequest) (*PipeContactLookupResponse, error) {
	if agreement == nil || req == nil {
		return nil, fmt.Errorf("pipe contact lookup is unavailable")
	}
	body, status, err := m.doPeerRequest(ctx, agreement, "POST", "/fed/v1/pipe/contacts/lookup", req)
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, fmt.Errorf("peer %s returned %d: %s", agreement.RemoteChainID, status, truncate(body, 200))
	}
	var out PipeContactLookupResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode pipe contact lookup: %w", err)
	}
	if out.Grant == nil || out.Total < 0 || out.Total < len(out.Grant.Contacts) || len(out.Grant.Contacts) > maxPipeContactLookupResults ||
		out.Truncated != (out.Total > len(out.Grant.Contacts)) {
		return nil, fmt.Errorf("invalid pipe contact lookup response")
	}
	if err := validateRemotePipeContactGrant(agreement.RemoteChainID, out.Grant); err != nil {
		return nil, err
	}
	return &out, nil
}

// FindRemotePipeContacts searches one active peer's bounded lookup endpoint.
// It is used by the local caller-filtered discovery surface after a local
// name miss; it never enumerates a remote roster.
func (m *Manager) FindRemotePipeContacts(ctx context.Context, remoteChainID, name string, limit int) (*PipeContactLookupResponse, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 512 {
		return nil, fmt.Errorf("invalid pipe contact name")
	}
	if limit <= 0 {
		limit = maxPipeContactLookupResults
	}
	if limit > maxPipeContactLookupResults {
		limit = maxPipeContactLookupResults
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	status, err := m.fetchPeerStatusForPipeLookup(ctx, agreement)
	if err != nil {
		return nil, err
	}
	return m.findRemotePipeContactsWithStatus(ctx, agreement, status, name, limit)
}

// FindRemotePipeContactsWithStatus performs the named lookup using the
// already-authenticated compact status probe made by local discovery. Keeping
// both exchanges in one per-peer fan-out avoids a second expensive status
// projection; lookup-capable results remain live-only and are not cached.
func (m *Manager) FindRemotePipeContactsWithStatus(ctx context.Context, remoteChainID string, status *StatusResponse, name string, limit int) (*PipeContactLookupResponse, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 512 {
		return nil, fmt.Errorf("invalid pipe contact name")
	}
	if limit <= 0 {
		limit = maxPipeContactLookupResults
	}
	if limit > maxPipeContactLookupResults {
		limit = maxPipeContactLookupResults
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	return m.findRemotePipeContactsWithStatus(ctx, agreement, status, name, limit)
}

func (m *Manager) findRemotePipeContactsWithStatus(ctx context.Context, agreement *store.CrossFedRecord, status *StatusResponse, name string, limit int) (*PipeContactLookupResponse, error) {
	if !hasFederatedPipelineCapability(status) {
		return nil, ErrRemotePipePeerUnsupported
	}
	if hasFederatedPipelineContactLookupCapability(status) {
		return m.lookupRemotePipeContacts(ctx, agreement, &PipeContactLookupRequest{Name: name, Limit: limit})
	}
	if err := validateRemotePipeContactGrant(agreement.RemoteChainID, status.PipeContacts); err != nil {
		return nil, err
	}
	grant, total := filterPipeContactLookup(status.PipeContacts, PipeContactLookupRequest{Name: name, Limit: limit})
	return &PipeContactLookupResponse{Grant: grant, Total: total, Truncated: total > len(grant.Contacts)}, nil
}

func (m *Manager) cachedRemotePipeContacts(ctx context.Context, agreement *store.CrossFedRecord, control *store.SyncControl) (*PipeContactGrant, error) {
	ss := m.syncStore()
	if ss == nil || agreement == nil || control == nil || control.BindingState != "active" ||
		control.RemoteCAPin != hex.EncodeToString(agreement.PeerPubKey) {
		return nil, nil
	}
	current, err := m.ActiveAgreement(agreement.RemoteChainID)
	if err != nil || !sameAgreementGeneration(agreement, current) {
		return nil, nil
	}
	snapshot, err := ss.GetFederatedPipeRemoteContactSnapshot(ctx, *control, pipeRoutingAgreementID(current))
	if err != nil || snapshot == nil {
		return nil, err
	}
	var grant PipeContactGrant
	if err := json.Unmarshal(snapshot.Snapshot, &grant); err != nil {
		return nil, fmt.Errorf("decode cached remote pipe contacts: %w", err)
	}
	if err := validateRemotePipeContactGrant(agreement.RemoteChainID, &grant); err != nil {
		return nil, err
	}
	if grant.AgreementID != snapshot.RemoteAgreementID || grant.Revision != snapshot.ContactRevision {
		return nil, fmt.Errorf("cached remote pipe contact binding is inconsistent")
	}
	return &grant, nil
}

// ResolveRemotePipeTarget resolves only the finite contact projection exposed
// by active, authenticated federation peers. It never searches arbitrary
// remote agents and never falls through to local provider/name resolution.
func (m *Manager) ResolveRemotePipeTarget(ctx context.Context, target string) (*RemotePipeTarget, error) {
	return m.resolveRemotePipeTarget(ctx, target, true)
}

// resolveRemotePipeTargetLive is used by the outbox immediately before a send.
// It deliberately forbids cache fallback: queued work may be addressed from a
// last authenticated snapshot, but payload bytes leave only after a fresh live
// status snapshot matches the stored authorization tuple.
func (m *Manager) resolveRemotePipeTargetLive(ctx context.Context, target string) (*RemotePipeTarget, error) {
	return m.resolveRemotePipeTarget(ctx, target, false)
}

func (m *Manager) resolveRemotePipeTarget(ctx context.Context, target string, allowCachedExact bool) (*RemotePipeTarget, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, ErrRemotePipeTargetNotFound
	}
	agreements := m.ActiveAgreements()
	sort.Slice(agreements, func(i, j int) bool { return agreements[i].RemoteChainID < agreements[j].RemoteChainID })
	if chain := pipeAddressChain(target); chain != "" {
		filtered := agreements[:0]
		for _, agreement := range agreements {
			if agreement.RemoteChainID == chain {
				filtered = append(filtered, agreement)
			}
		}
		agreements = filtered
	}
	if len(agreements) == 0 {
		return nil, ErrRemotePipeTargetNotFound
	}

	candidates := make([]remotePipeCandidate, 0)
	var lookupErrors []error
	unsupportedPeers := 0
	ss := m.syncStore()
	if ss == nil {
		return nil, ErrRemotePipePeerUnsupported
	}
	for i := range agreements {
		agreement := agreements[i]
		control, controlErr := ss.GetSyncControl(ctx, agreement.RemoteChainID)
		if controlErr != nil || control == nil || control.BindingState != "active" ||
			control.RemoteCAPin != hex.EncodeToString(agreement.PeerPubKey) ||
			!isCanonicalAgentID(control.PeerAgentID) {
			lookupErrors = append(lookupErrors, fmt.Errorf("%s: active pipe policy binding unavailable", agreement.RemoteChainID))
			continue
		}
		status, err := m.fetchPeerStatusForPipeLookup(ctx, &agreement)
		if err != nil {
			if allowCachedExact && pipeAddressChain(target) == agreement.RemoteChainID && errors.Is(err, ErrPeerOffline) {
				cached, cacheErr := m.cachedRemotePipeContacts(ctx, &agreement, control)
				if cacheErr == nil && cached != nil {
					for _, contact := range cached.Contacts {
						candidates = append(candidates, remotePipeCandidate{
							chainID: agreement.RemoteChainID, grant: cached, contact: contact,
							policyEpoch: control.PolicyEpoch,
						})
					}
				} else if cacheErr != nil {
					lookupErrors = append(lookupErrors, cacheErr)
				}
			}
			lookupErrors = append(lookupErrors, err)
			continue
		}
		if !hasFederatedPipelineCapability(status) {
			unsupportedPeers++
			continue
		}
		grant := status.PipeContacts
		if hasFederatedPipelineContactLookupCapability(status) {
			lookup, lookupErr := m.lookupRemotePipeContacts(ctx, &agreement, &PipeContactLookupRequest{Target: target, Limit: maxPipeContactLookupResults})
			if lookupErr != nil {
				lookupErrors = append(lookupErrors, fmt.Errorf("%s: lookup pipe contact: %w", agreement.RemoteChainID, lookupErr))
				continue
			}
			grant = lookup.Grant
		} else {
			if grantErr := validateRemotePipeContactGrant(agreement.RemoteChainID, grant); grantErr != nil {
				lookupErrors = append(lookupErrors, fmt.Errorf("%s: invalid pipe contact snapshot", agreement.RemoteChainID))
				continue
			}
			if refreshErr := m.refreshRemotePipeContactCache(ctx, &agreement, control, status); refreshErr != nil {
				lookupErrors = append(lookupErrors, fmt.Errorf("%s: cache authenticated pipe contacts: %w", agreement.RemoteChainID, refreshErr))
				continue
			}
		}
		currentControl, err := ss.GetSyncControl(ctx, agreement.RemoteChainID)
		if err != nil || !sameRemotePipeContactBinding(control, currentControl) {
			lookupErrors = append(lookupErrors, fmt.Errorf("%s: pipe policy binding changed during contact resolution", agreement.RemoteChainID))
			continue
		}
		for _, contact := range grant.Contacts {
			candidates = append(candidates, remotePipeCandidate{
				chainID: agreement.RemoteChainID, grant: grant, contact: contact,
				policyEpoch: control.PolicyEpoch,
			})
		}
	}

	matches := matchRemotePipeCandidates(target, candidates)
	if len(matches) == 0 {
		if pipeAddressChain(target) != "" && unsupportedPeers > 0 {
			return nil, ErrRemotePipePeerUnsupported
		}
		if strings.HasPrefix(target, "#") && unsupportedPeers > 0 && len(lookupErrors) == 0 {
			return nil, ErrRemotePipePeerUnsupported
		}
		if len(lookupErrors) > 0 {
			return nil, errors.Join(ErrRemotePipeResolutionIncomplete, errors.Join(lookupErrors...))
		}
		return nil, ErrRemotePipeTargetNotFound
	}
	// A full canonical address selects one chain, so unrelated peer reachability
	// cannot make it ambiguous. Handles and bare labels require a complete scan.
	if pipeAddressChain(target) == "" && len(lookupErrors) > 0 {
		return nil, errors.Join(ErrRemotePipeResolutionIncomplete, errors.Join(lookupErrors...))
	}
	if len(matches) != 1 {
		choices := make([]string, 0, len(matches))
		for _, match := range matches {
			choices = append(choices, match.contact.Address)
		}
		sort.Strings(choices)
		return nil, fmt.Errorf("%w: choose one of %s", ErrRemotePipeTargetAmbiguous, strings.Join(choices, ", "))
	}
	match := matches[0]
	if match.grant.Paused {
		return nil, ErrRemotePipeTargetUnavailable
	}
	if !match.contact.Available {
		return nil, ErrRemotePipeTargetUnavailable
	}
	if !match.contact.Accepting {
		return nil, ErrRemotePipeTargetNotAccepting
	}
	if !isCanonicalAgentID(match.contact.AgentID) || match.contact.ContactID == "" || match.contact.Address == "" {
		return nil, ErrRemotePipeTargetUnavailable
	}
	return &RemotePipeTarget{
		ChainID: match.chainID, AgentID: match.contact.AgentID, ContactID: match.contact.ContactID,
		ContactRevision: pipeContactAuthorizationRevision(match.grant, &match.contact), PolicyEpoch: match.policyEpoch,
		AgreementID: match.grant.AgreementID, Address: match.contact.Address,
		Handle: match.contact.Handle, DisplayName: match.contact.DisplayName,
		Domains: append([]PipeContactDomain(nil), match.contact.Domains...),
	}, nil
}

func matchRemotePipeCandidates(target string, candidates []remotePipeCandidate) []remotePipeCandidate {
	target = strings.TrimSpace(target)
	addressAgent, addressChain := splitPipeAddress(target)
	out := make([]remotePipeCandidate, 0, 1)
	for _, candidate := range candidates {
		contact := candidate.contact
		matched := false
		switch {
		case addressChain != "":
			matched = candidate.chainID == addressChain && strings.EqualFold(contact.AgentID, addressAgent) && contact.Address == contact.AgentID+"@"+candidate.chainID
		case strings.HasPrefix(target, "#"):
			matched = strings.EqualFold(contact.Handle, target)
		default:
			matched = strings.EqualFold(contact.AgentID, target) || strings.EqualFold(contact.DisplayName, target)
		}
		if matched {
			out = append(out, candidate)
		}
	}
	return out
}

func splitPipeAddress(target string) (agentID, chainID string) {
	idx := strings.LastIndex(target, "@")
	if idx <= 0 || idx == len(target)-1 {
		return "", ""
	}
	agentID, chainID = target[:idx], target[idx+1:]
	if !isCanonicalAgentID(agentID) || ValidateChainID(chainID) != nil {
		return "", ""
	}
	return strings.ToLower(agentID), chainID
}

func pipeAddressChain(target string) string {
	_, chainID := splitPipeAddress(target)
	return chainID
}
