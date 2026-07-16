package federation

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// ResolvePeerOperatorAgentID resolves the remote operator identity frozen by
// JOIN. Fresh links use sync_control.peer_agent_id. Legacy guest-side rows may
// safely reuse the remote controller identity; legacy host-side rows require an
// exact two-member enrollment group whose epoch and remote CA pin still match.
func (m *Manager) ResolvePeerOperatorAgentID(ctx context.Context, remoteChainID string) (string, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return "", err
	}
	return m.resolvePeerOperatorAgentID(ctx, agreement)
}

func (m *Manager) resolvePeerOperatorAgentID(ctx context.Context, agreement *store.CrossFedRecord) (string, error) {
	ss := m.syncStore()
	if ss == nil {
		return "", fmt.Errorf("peer RBAC requires the SQLite store backend")
	}
	if agreement == nil {
		return "", fmt.Errorf("active federation agreement is required")
	}
	control, err := ss.GetSyncControl(ctx, agreement.RemoteChainID)
	if err != nil {
		return "", fmt.Errorf("read peer operator binding: %w", err)
	}
	wantPin := hex.EncodeToString(agreement.PeerPubKey)
	if control == nil || control.BindingState != "active" || control.RemoteChainID != agreement.RemoteChainID ||
		control.PolicyEpoch == "" || control.RemoteCAPin != wantPin {
		return "", fmt.Errorf("connection has no active peer operator binding matching its trust anchor")
	}
	localAgentID := hex.EncodeToString(m.agentPub)

	if control.PeerAgentID != "" {
		switch control.Role {
		case "guest":
			if control.ControllerChainID != agreement.RemoteChainID || control.ControllerAgentID != control.PeerAgentID {
				return "", fmt.Errorf("guest peer operator binding conflicts with the frozen controller")
			}
		case "host":
			if control.ControllerChainID != m.localChainID || control.ControllerAgentID != localAgentID {
				return "", fmt.Errorf("host peer operator binding conflicts with the local controller")
			}
		default:
			return "", fmt.Errorf("invalid sync control role %q", control.Role)
		}
		if _, keyErr := auth.AgentIDToPublicKey(control.PeerAgentID); keyErr != nil {
			return "", fmt.Errorf("frozen peer operator id is invalid: %w", keyErr)
		}
		return control.PeerAgentID, nil
	}

	// Safe legacy guest fallback: on the guest, the controller has always been
	// the remote host, so its ceremony-frozen controller key is the peer key.
	if control.Role == "guest" {
		if control.ControllerChainID != agreement.RemoteChainID || control.ControllerAgentID == "" {
			return "", fmt.Errorf("legacy guest binding does not name the remote controller")
		}
		if _, keyErr := auth.AgentIDToPublicKey(control.ControllerAgentID); keyErr != nil {
			return "", fmt.Errorf("legacy guest controller id is invalid: %w", keyErr)
		}
		return control.ControllerAgentID, nil
	}

	// A legacy host row names only the local controller. Recover the guest key
	// solely from the deterministic pairwise enrollment group, never by guessing
	// from an arbitrary group/member row.
	if control.Role != "host" || control.ControllerChainID != m.localChainID || control.ControllerAgentID != localAgentID {
		return "", fmt.Errorf("legacy host binding does not name the local controller")
	}
	groupID := pairwiseGroupID(m.localChainID, agreement.RemoteChainID, control.PolicyEpoch)
	group, err := ss.GetSyncGroup(ctx, groupID)
	if err != nil {
		return "", fmt.Errorf("read legacy pairwise group: %w", err)
	}
	if group == nil || group.GroupID != groupID || group.Epoch != control.PolicyEpoch ||
		group.ControllerChainID != m.localChainID || group.ControllerAgentPubkey != localAgentID {
		return "", fmt.Errorf("legacy host peer identity cannot be recovered from an exact pairwise group")
	}
	members, err := ss.ListSyncGroupMembers(ctx, groupID)
	if err != nil {
		return "", fmt.Errorf("read legacy pairwise roster: %w", err)
	}
	if len(members) != 2 {
		return "", fmt.Errorf("legacy pairwise group has %d members, want exactly 2", len(members))
	}
	var remoteAgentID string
	for _, member := range members {
		if member.MemberState != store.GroupMemberActive || member.LeftRevision != 0 {
			return "", fmt.Errorf("legacy pairwise group contains a non-active member")
		}
		switch member.MemberChainID {
		case m.localChainID:
			if member.MemberAgentPubkey != localAgentID {
				return "", fmt.Errorf("legacy pairwise group local identity mismatch")
			}
		case agreement.RemoteChainID:
			if member.CAPin != wantPin || member.MemberAgentPubkey == "" {
				return "", fmt.Errorf("legacy pairwise group remote trust binding mismatch")
			}
			remoteAgentID = member.MemberAgentPubkey
		default:
			return "", fmt.Errorf("legacy pairwise group contains unexpected chain %q", member.MemberChainID)
		}
	}
	if _, err := auth.AgentIDToPublicKey(remoteAgentID); err != nil {
		return "", fmt.Errorf("legacy pairwise peer operator id is invalid: %w", err)
	}
	return remoteAgentID, nil
}

// GetPeerRBACPolicy returns nil for a legacy/unconfigured connection. A
// configured empty policy is returned non-nil and means explicit deny-all.
func (m *Manager) GetPeerRBACPolicy(ctx context.Context, remoteChainID string) (*store.PeerRBACPolicy, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	return m.getPeerRBACPolicyForAgreement(ctx, agreement)
}

// getPeerRBACPolicyForAgreement is the request-path variant of
// GetPeerRBACPolicy. The authenticated peer middleware has already resolved the
// exact active agreement, so re-reading it through Manager.badger is both
// redundant and unsafe for deliberately store-light handler tests. Identity,
// CA-pin, epoch and policy-version checks still run against the supplied frozen
// agreement and sync_control binding; this is not a weaker lookup.
func (m *Manager) getPeerRBACPolicyForAgreement(ctx context.Context, agreement *store.CrossFedRecord) (*store.PeerRBACPolicy, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, nil // non-SQLite deployments remain legacy/unconfigured
	}
	if agreement == nil || agreement.RemoteChainID == "" {
		return nil, fmt.Errorf("active federation agreement is required")
	}
	remoteChainID := agreement.RemoteChainID
	policy, err := ss.GetPeerRBACPolicy(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		control, controlErr := ss.GetSyncControl(ctx, remoteChainID)
		if controlErr != nil {
			return nil, controlErr
		}
		v3 := control != nil && (control.PolicyVersion >= SyncPolicyVersionPeerRBAC ||
			control.RemotePolicyVersion >= SyncPolicyVersionPeerRBAC)
		if !v3 {
			return nil, nil
		}
		// A v3 marker means this is a trust-only/PeerRBAC link, never a legacy
		// agreement. Missing policy persistence is explicit deny-all, but only after
		// the complete frozen JOIN binding is verified. A partial/corrupt v3 control
		// row must error instead of falling back to mutable tx-33 authority.
		peerAgentID, resolveErr := m.resolvePeerOperatorAgentID(ctx, agreement)
		if resolveErr != nil {
			return nil, resolveErr
		}
		if control.BindingState != "active" || control.PeerAgentID == "" ||
			!m.peerRBACSyncBinding(control, agreement, peerAgentID) {
			return nil, fmt.Errorf("%w: incomplete or stale v3 sync binding", store.ErrPeerRBACBindingMismatch)
		}
		return &store.PeerRBACPolicy{
			RemoteChainID: remoteChainID,
			PeerAgentID:   peerAgentID,
			PolicyEpoch:   control.PolicyEpoch,
			RemoteCAPin:   control.RemoteCAPin,
			PolicyVersion: store.CurrentPeerRBACPolicyVersion,
			Domains:       []store.PeerRBACDomainPermission{},
		}, nil
	}
	peerAgentID, err := m.resolvePeerOperatorAgentID(ctx, agreement)
	if err != nil {
		return nil, err
	}
	if policy.PeerAgentID != peerAgentID {
		return nil, fmt.Errorf("%w: stored policy peer no longer matches the active connection", store.ErrPeerRBACBindingMismatch)
	}
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	if control == nil || control.BindingState != "active" || policy.PolicyEpoch != control.PolicyEpoch ||
		policy.RemoteCAPin != control.RemoteCAPin || control.PeerAgentID == "" ||
		!m.peerRBACSyncBinding(control, agreement, peerAgentID) {
		return nil, fmt.Errorf("%w: stored policy ceremony generation no longer matches the active connection", store.ErrPeerRBACBindingMismatch)
	}
	return policy, nil
}

// ReplacePeerRBACPolicy atomically installs this node's complete directional
// grant for a peer. The remote operator binding comes only from the authenticated
// JOIN artifacts; callers cannot choose or replace it.
func (m *Manager) ReplacePeerRBACPolicy(ctx context.Context, remoteChainID string, domains []store.PeerRBACDomainPermission) (*store.PeerRBACPolicy, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, fmt.Errorf("peer RBAC requires the SQLite store backend")
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil || control == nil || control.BindingState != "active" {
		return nil, fmt.Errorf("connection has no active peer RBAC ceremony binding")
	}
	if control.PeerAgentID == "" {
		// v11.8 guest rows already name the remote controller; host rows can be
		// recovered only from the exact ceremony-era two-member roster. Resolve
		// above performs those checks. Freeze the result once so subsequent v3
		// Read/Copy enforcement never has to fall back or force a re-pair.
		if err := ss.FreezeSyncControlPeerAgent(ctx, remoteChainID, control.PolicyEpoch, control.RemoteCAPin, peerAgentID); err != nil {
			return nil, fmt.Errorf("freeze recovered peer operator binding: %w", err)
		}
		control.PeerAgentID = peerAgentID
	}
	if control.PeerAgentID != peerAgentID {
		return nil, fmt.Errorf("%w: active sync peer differs from the recovered operator", store.ErrPeerRBACBindingMismatch)
	}
	return ss.ReplaceBoundPeerRBACPolicy(ctx, store.PeerRBACPolicy{
		RemoteChainID: remoteChainID,
		PeerAgentID:   peerAgentID,
		PolicyEpoch:   control.PolicyEpoch,
		RemoteCAPin:   control.RemoteCAPin,
		PolicyVersion: store.CurrentPeerRBACPolicyVersion,
		Domains:       domains,
	})
}

// initializePeerRBACPolicy is part of fresh JOIN activation, not a mutable
// permission update. It deliberately discards any retired snapshot left behind
// as a managed-grant cleanup identity, then binds a present, empty deny-all
// snapshot to the newly frozen peer operator. The legacy tx-33 envelope is
// empty too, so even a failure between delete and replace remains deny-all.
func (m *Manager) initializePeerRBACPolicy(ctx context.Context, remoteChainID string) error {
	ss := m.syncStore()
	if ss == nil {
		return fmt.Errorf("peer RBAC requires the SQLite store backend")
	}
	peerAgentID, err := m.ResolvePeerOperatorAgentID(ctx, remoteChainID)
	if err != nil {
		return err
	}
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil || control == nil || control.BindingState != "active" {
		return fmt.Errorf("connection has no active peer RBAC ceremony binding")
	}
	if deleteErr := ss.DeletePeerRBACPolicy(ctx, remoteChainID); deleteErr != nil {
		return fmt.Errorf("clear retired peer RBAC snapshot: %w", deleteErr)
	}
	_, err = ss.ReplaceBoundPeerRBACPolicy(ctx, store.PeerRBACPolicy{
		RemoteChainID: remoteChainID,
		PeerAgentID:   peerAgentID,
		PolicyEpoch:   control.PolicyEpoch,
		RemoteCAPin:   control.RemoteCAPin,
		PolicyVersion: store.CurrentPeerRBACPolicyVersion,
		Domains:       []store.PeerRBACDomainPermission{},
	})
	if err != nil {
		return fmt.Errorf("install initial peer RBAC deny policy: %w", err)
	}
	return nil
}

func peerRBACAllows(policy *store.PeerRBACPolicy, domain string, permission func(store.PeerRBACDomainPermission) bool) bool {
	if policy == nil || domain == "" {
		return false
	}
	for _, grant := range policy.Domains {
		if permission(grant) && DomainAllowed([]string{grant.Domain}, domain) {
			return true
		}
	}
	return false
}

func peerRBACAllowsRead(policy *store.PeerRBACPolicy, domain string) bool {
	return peerRBACAllows(policy, domain, func(grant store.PeerRBACDomainPermission) bool { return grant.Read })
}

func peerRBACGrantFromPolicy(policy *store.PeerRBACPolicy) *PeerRBACGrant {
	if policy == nil {
		return nil
	}
	domains := make([]PeerRBACDomainGrant, 0, len(policy.Domains))
	for _, permission := range policy.Domains {
		domains = append(domains, PeerRBACDomainGrant{
			Domain: permission.Domain,
			Read:   permission.Read || permission.Copy,
			// v11.9 reserves the field but does not advertise reusable ordinary
			// AccessGrant state as a connection-scoped federation capability.
			Write: false,
			Copy:  permission.Copy,
		})
	}
	return &PeerRBACGrant{PolicyVersion: policy.PolicyVersion, Domains: domains}
}
