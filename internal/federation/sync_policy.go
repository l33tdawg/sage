package federation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"unicode"

	"github.com/l33tdawg/sage/internal/store"
)

type SyncPolicyRequest struct {
	Version          int      `json:"version"`
	Epoch            string   `json:"epoch"`
	Revision         int64    `json:"revision"`
	Domains          []string `json:"domains,omitempty"`
	PublishDomains   []string `json:"publish_domains,omitempty"`
	SubscribeDomains []string `json:"subscribe_domains,omitempty"`
}

type SyncPolicyResponse struct {
	Status   string `json:"status"`
	Revision int64  `json:"revision"`
}

type HostSyncPolicyResult struct {
	Version  int      `json:"version"`
	Revision int64    `json:"revision"`
	Domains  []string `json:"domains"`
	State    string   `json:"state"`
}

const (
	SyncPolicyVersionLegacy      = 1
	SyncPolicyVersionDirectional = 2
	SyncPolicyVersionPeerRBAC    = 3
)

type DirectionalSyncPolicyResult struct {
	Version          int      `json:"version"`
	Revision         int64    `json:"revision"`
	PublishDomains   []string `json:"publish_domains"`
	SubscribeDomains []string `json:"subscribe_domains"`
	State            string   `json:"state"`
}

// canonicalSyncDomainsFormat validates only the host-controlled copy-policy
// syntax. In v2 the two tx-33 scopes are independent outbound grants, so the
// policy may legitimately name a domain shared by either side and MUST NOT be
// intersected with the receiver's outbound grant.
func canonicalSyncDomainsFormat(raw []string) ([]string, error) {
	if len(raw) > 100 {
		return nil, fmt.Errorf("sync policy is capped at 100 domains")
	}
	out := append([]string(nil), raw...)
	sort.Strings(out)
	for i, domain := range out {
		if domain == "" || domain == "*" || len(domain) > 128 || strings.TrimSpace(domain) != domain {
			return nil, fmt.Errorf("sync domains must be concrete, non-empty tags")
		}
		for _, r := range domain {
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("sync domain contains control characters")
			}
		}
		if i > 0 && out[i-1] == domain {
			return nil, fmt.Errorf("sync policy contains duplicate domains")
		}
	}
	return out, nil
}

// canonicalSyncDomains preserves the v1 bilateral treaty check for legacy
// links. New host-managed links use canonicalSyncDomainsFormat instead.
func canonicalSyncDomains(raw []string, agreement *store.CrossFedRecord) ([]string, error) {
	out, err := canonicalSyncDomainsFormat(raw)
	if err != nil {
		return nil, err
	}
	for _, domain := range out {
		if agreement == nil || !DomainAllowed(agreement.AllowedDomains, domain) {
			return nil, fmt.Errorf("domain %q is outside the federation treaty", domain)
		}
	}
	return out, nil
}

func syncPolicyHashVersion(version int, epoch, controller string, revision int64, domains []string) string {
	h := sha256.New()
	if version == SyncPolicyVersionDirectional {
		h.Write([]byte("sage-sync-policy-v2\x00"))
	} else {
		h.Write([]byte("sage-sync-policy-v1\x00"))
	}
	writePolicyPart := func(value string) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(value))) // #nosec G115 -- inputs are bounded
		h.Write(n[:])
		h.Write([]byte(value))
	}
	writePolicyPart(epoch)
	writePolicyPart(controller)
	var rev [8]byte
	binary.BigEndian.PutUint64(rev[:], uint64(revision)) // #nosec G115 -- revision must be positive
	h.Write(rev[:])
	for _, domain := range domains {
		writePolicyPart(domain)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func directionalSyncPolicyHash(epoch, publisher string, revision int64, publish, subscribe []string) string {
	h := sha256.New()
	h.Write([]byte("sage-sync-policy-v3\x00"))
	write := func(value string) {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(value))) // #nosec G115 -- policy fields are bounded
		h.Write(n[:])
		h.Write([]byte(value))
	}
	write(epoch)
	write(publisher)
	var rev [8]byte
	binary.BigEndian.PutUint64(rev[:], uint64(revision)) // #nosec G115 -- revision is positive
	h.Write(rev[:])
	write("publish")
	for _, domain := range publish {
		write(domain)
	}
	write("subscribe")
	for _, domain := range subscribe {
		write(domain)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func syncPolicyEpoch(e [32]byte) string {
	h := sha256.New()
	h.Write([]byte("sage-sync-policy-epoch-v1\x00"))
	h.Write(e[:])
	return hex.EncodeToString(h.Sum(nil))
}

func (m *Manager) handleSyncPolicy(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	ss := m.syncStore()
	if peer == nil || ss == nil {
		httpError(w, http.StatusNotImplemented, "sync policy unavailable")
		return
	}
	var req SyncPolicyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
		(req.Version != SyncPolicyVersionLegacy && req.Version != SyncPolicyVersionDirectional && req.Version != SyncPolicyVersionPeerRBAC) || req.Revision <= 0 {
		httpError(w, http.StatusBadRequest, "invalid sync policy")
		return
	}
	control, err := ss.GetSyncControl(r.Context(), peer.ChainID)
	if err != nil || control == nil || control.BindingState != "active" {
		httpError(w, http.StatusForbidden, "peer is not this node's sync controller")
		return
	}
	if control.PolicyEpoch != req.Epoch || control.RemoteCAPin != hex.EncodeToString(peer.Agreement.PeerPubKey) {
		httpError(w, http.StatusForbidden, "sync controller binding mismatch")
		return
	}
	if req.Version < SyncPolicyVersionPeerRBAC &&
		(control.PolicyVersion >= SyncPolicyVersionPeerRBAC || control.RemotePolicyVersion >= SyncPolicyVersionPeerRBAC) {
		httpError(w, http.StatusConflict, "peer-RBAC sync policy cannot be downgraded")
		return
	}
	if req.Version == SyncPolicyVersionPeerRBAC {
		if !m.syncControlPeerBound(control, peer) {
			httpError(w, http.StatusForbidden, "sync peer binding mismatch")
			return
		}
		publish, pErr := canonicalSyncDomainsFormat(req.PublishDomains)
		subscribe, sErr := canonicalSyncDomainsFormat(req.SubscribeDomains)
		if pErr != nil || sErr != nil {
			httpError(w, http.StatusBadRequest, "invalid directional sync policy")
			return
		}
		hash := directionalSyncPolicyHash(req.Epoch, peer.ChainID, req.Revision, publish, subscribe)
		status, applyErr := ss.ApplyRemoteDirectionalSyncPolicy(r.Context(), peer.ChainID, req.Epoch,
			req.Version, req.Revision, hash, publish, subscribe)
		if applyErr != nil {
			httpError(w, http.StatusConflict, applyErr.Error())
			return
		}
		m.nudgeSync()
		writeJSON(w, http.StatusOK, &SyncPolicyResponse{Status: status, Revision: req.Revision})
		return
	}
	if control.Role != "guest" || control.ControllerChainID != peer.ChainID || control.ControllerAgentID != peer.AgentID {
		httpError(w, http.StatusForbidden, "peer is not this node's sync controller")
		return
	}
	var domains []string
	if req.Version == SyncPolicyVersionDirectional {
		domains, err = canonicalSyncDomainsFormat(req.Domains)
	} else {
		domains, err = canonicalSyncDomains(req.Domains, peer.Agreement)
	}
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	hash := syncPolicyHashVersion(req.Version, req.Epoch, peer.ChainID, req.Revision, domains)
	status, err := ss.ApplySyncPolicyVersion(r.Context(), peer.ChainID, req.Epoch, req.Version, req.Revision, hash, domains)
	if err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &SyncPolicyResponse{Status: status, Revision: req.Revision})
}

func (m *Manager) syncControlPeerBound(control *store.SyncControl, peer *peerIdentity) bool {
	if control == nil || peer == nil || peer.Agreement == nil || control.BindingState != "active" ||
		control.RemoteChainID != peer.ChainID || control.PeerAgentID == "" || control.PeerAgentID != peer.AgentID ||
		control.RemoteCAPin != hex.EncodeToString(peer.Agreement.PeerPubKey) {
		return false
	}
	if control.Role == "guest" {
		return control.ControllerChainID == peer.ChainID && control.ControllerAgentID == peer.AgentID
	}
	return control.Role == "host" && control.ControllerChainID == m.localChainID &&
		control.ControllerAgentID == hex.EncodeToString(m.agentPub)
}

// inboundGroupPeerBound binds group RBAC to the paired transport identity. A
// group capability is usable only across an active JOIN-frozen edge: the live
// signer must be the exact paired peer operator as well as the exact group
// member. Group RBAC is independent from direct Read/Copy scope, not from the
// identity trust ceremony beneath it.
func (m *Manager) inboundGroupPeerBound(ctx context.Context, ss *store.SQLiteStore, peer *peerIdentity) (bool, error) {
	if ss == nil || peer == nil || peer.Agreement == nil {
		return false, nil
	}
	control, err := ss.GetSyncControl(ctx, peer.ChainID)
	if err != nil {
		return false, err
	}
	if control == nil {
		return false, nil
	}
	return m.syncControlPeerBound(control, peer), nil
}

// currentInboundGroupPeerBound re-resolves the consensus agreement before
// checking the off-consensus JOIN binding. Callers use this while holding the
// sync-policy write lease immediately before a group mutation: a request may
// have authenticated under an agreement that was revoked while it waited for
// the journal/write lock, and that stale request must not commit afterward.
func (m *Manager) currentInboundGroupPeerBound(ctx context.Context, ss *store.SQLiteStore, chainID, agentID string) (bool, error) {
	agreement, err := m.ActiveAgreement(chainID)
	if err != nil {
		return false, nil
	}
	return m.inboundGroupPeerBound(ctx, ss, &peerIdentity{
		ChainID:   chainID,
		AgentID:   agentID,
		Agreement: agreement,
	})
}

// peerRBACSyncBinding is the exact direct-pair identity binding. Callers select
// v3 first from either the control version or a validated persisted PeerRBAC
// snapshot; the latter deliberately covers the crash window before a legacy
// control row is version-bumped. The active agreement, CA pin and frozen JOIN
// operator must still name the authenticated peer. Callers without a live
// request pass the peer agent frozen in sync_control; inbound callers pass the
// authenticated peer agent and catch an identity mismatch before copy policy.
func (m *Manager) peerRBACSyncBinding(control *store.SyncControl, agreement *store.CrossFedRecord, peerAgentID string) bool {
	if control == nil || agreement == nil {
		return false
	}
	return m.syncControlPeerBound(control, &peerIdentity{
		ChainID:   agreement.RemoteChainID,
		AgentID:   peerAgentID,
		Agreement: agreement,
	})
}

// SetDirectionalSyncPolicy publishes this node's independent copy grant and
// copy subscription. publish is a subset of what this node shares for live
// reads; subscribe is what this node elects to retain from the peer.
func (m *Manager) SetDirectionalSyncPolicy(ctx context.Context, remoteChainID string, rawPublish, rawSubscribe []string) (*DirectionalSyncPolicyResult, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, fmt.Errorf("domain sync requires SQLite")
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil || control == nil || control.BindingState != "active" || control.PolicyEpoch == "" ||
		control.PeerAgentID == "" || control.RemoteCAPin != hex.EncodeToString(agreement.PeerPubKey) {
		return nil, fmt.Errorf("this connection has no active directional sync binding")
	}
	publish, err := canonicalSyncDomainsFormat(rawPublish)
	if err != nil {
		return nil, err
	}
	subscribe, err := canonicalSyncDomainsFormat(rawSubscribe)
	if err != nil {
		return nil, err
	}
	// Peer RBAC is authoritative for every v3 Publish. A configured empty policy
	// and an unconfigured policy are both deny-all for Copy: otherwise a
	// subscribe-only v2->v3 migration could silently turn legacy sync_domains
	// into a new outbound Copy capability. Legacy SetHostSyncPolicy/v1-v2 paths
	// retain their tx-33 compatibility without entering this method.
	peerPolicy, err := m.GetPeerRBACPolicy(ctx, remoteChainID)
	if err != nil {
		return nil, fmt.Errorf("read peer copy permission: %w", err)
	}
	for _, domain := range publish {
		if peerPolicy == nil || !peerRBACAllows(peerPolicy, domain, func(grant store.PeerRBACDomainPermission) bool { return grant.Copy }) {
			return nil, fmt.Errorf("copy permission for %q is not granted to this peer", domain)
		}
	}
	if err := m.authorizeSyncPolicyDomains(publish); err != nil {
		return nil, err
	}
	revision := control.Revision + 1
	hash := directionalSyncPolicyHash(control.PolicyEpoch, m.localChainID, revision, publish, subscribe)
	if _, err := ss.ApplyLocalDirectionalSyncPolicy(ctx, remoteChainID, control.PolicyEpoch,
		SyncPolicyVersionPeerRBAC, revision, hash, publish, subscribe); err != nil {
		return nil, err
	}
	state := "pending"
	if err := m.deliverSyncPolicy(ctx, ss, remoteChainID); err == nil {
		state = "delivered"
	}
	m.nudgeSync()
	return &DirectionalSyncPolicyResult{Version: SyncPolicyVersionPeerRBAC, Revision: revision,
		PublishDomains: publish, SubscribeDomains: subscribe, State: state}, nil
}

func (m *Manager) SetHostSyncPolicy(ctx context.Context, remoteChainID string, raw []string) (*HostSyncPolicyResult, error) {
	ss := m.syncStore()
	if ss == nil {
		return nil, fmt.Errorf("domain sync requires SQLite")
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil || control == nil || control.BindingState != "active" || control.Role != "host" ||
		control.ControllerChainID != m.localChainID || control.ControllerAgentID != hex.EncodeToString(m.agentPub) ||
		control.RemoteCAPin != hex.EncodeToString(agreement.PeerPubKey) {
		return nil, fmt.Errorf("this connection is not controlled by the local host")
	}
	if control.PolicyVersion >= SyncPolicyVersionPeerRBAC || control.RemotePolicyVersion >= SyncPolicyVersionPeerRBAC {
		return nil, fmt.Errorf("peer-RBAC sync policy cannot be replaced by legacy host-managed sync")
	}
	domains, err := canonicalSyncDomainsFormat(raw)
	if err != nil {
		return nil, err
	}
	revision := control.Revision + 1
	hash := syncPolicyHashVersion(SyncPolicyVersionDirectional, control.PolicyEpoch, m.localChainID, revision, domains)
	if _, err := ss.ApplySyncPolicyVersion(ctx, remoteChainID, control.PolicyEpoch, SyncPolicyVersionDirectional, revision, hash, domains); err != nil {
		return nil, err
	}
	state := "pending"
	if err := m.deliverSyncPolicy(ctx, ss, remoteChainID); err == nil {
		state = "delivered"
	}
	m.nudgeSync()
	return &HostSyncPolicyResult{Version: SyncPolicyVersionDirectional, Revision: revision, Domains: domains, State: state}, nil
}

func (m *Manager) authorizeSyncPolicyDomains(domains []string) error {
	if len(domains) == 0 {
		return nil // a controller must always be able to narrow/disable egress
	}
	if m.badger == nil {
		return fmt.Errorf("domain authorization store is unavailable")
	}
	agentID := hex.EncodeToString(m.agentPub)
	if rec, err := m.badger.GetRegisteredAgent(agentID); err == nil && rec != nil && rec.Role == "admin" {
		return nil
	}
	for _, domain := range domains {
		owns, err := m.badger.IsDomainOwnerOrAncestor(domain, agentID)
		if err != nil || !owns {
			return fmt.Errorf("host operator is not admin or owner of domain %q", domain)
		}
	}
	return nil
}

// authorizeOwnerUnilateralDomain is the anti-hijack authority check for an
// OWNER-UNILATERAL domain emit (docs §8): adding/removing MY domain to/from MY
// scope is locally effective and needs no controller quorum, but the local
// operator MUST have authority over the domain. Mirrors authorizeSyncPolicyDomains'
// admin-or-owner rule (an admin, or IsDomainOwnerOrAncestor over the NEW tag), and
// — like that check's anti-hijack rule over both new and stored scope — ALSO
// asserts authority over the STORED scope when requireStoredOwner: the group must
// already record THIS node as the domain's owner_chain_id, so a re-add / remove can
// never retarget a domain another member owns in the group. gs is the current
// group projection (its domainOwner map carries the stored owner_chain_id).
func (m *Manager) authorizeOwnerUnilateralDomain(tag string, requireStoredOwner bool, gs *groupApplyState) error {
	if tag == "" {
		return fmt.Errorf("domain tag is required")
	}
	if m.badger == nil {
		return fmt.Errorf("domain authorization store is unavailable")
	}
	if requireStoredOwner {
		if gs == nil {
			return fmt.Errorf("stored-owner authorization requires group state")
		}
		if owner, ok := gs.domainOwner[tag]; !ok || owner != m.localChainID {
			return fmt.Errorf("local node is not the recorded owner of domain %q in this group", tag)
		}
	}
	agentID := hex.EncodeToString(m.agentPub)
	if rec, err := m.badger.GetRegisteredAgent(agentID); err == nil && rec != nil && rec.Role == "admin" {
		return nil
	}
	owns, err := m.badger.IsDomainOwnerOrAncestor(tag, agentID)
	if err != nil || !owns {
		return fmt.Errorf("local operator is not admin or owner of domain %q", tag)
	}
	return nil
}

func (m *Manager) deliverSyncPolicy(ctx context.Context, ss *store.SQLiteStore, remoteChainID string) error {
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil || control == nil || control.Revision <= control.DeliveredRevision ||
		(control.Role != "host" && control.PolicyVersion < SyncPolicyVersionPeerRBAC) {
		return err
	}
	var req *SyncPolicyRequest
	if control.PolicyVersion >= SyncPolicyVersionPeerRBAC {
		publish, pErr := ss.GetDirectionalSyncDomains(ctx, remoteChainID, store.SyncDirectionLocalPublish)
		if pErr != nil {
			return pErr
		}
		subscribe, sErr := ss.GetDirectionalSyncDomains(ctx, remoteChainID, store.SyncDirectionLocalSubscribe)
		if sErr != nil {
			return sErr
		}
		req = &SyncPolicyRequest{Version: SyncPolicyVersionPeerRBAC, Epoch: control.PolicyEpoch,
			Revision: control.Revision, PublishDomains: publish, SubscribeDomains: subscribe}
	} else {
		domains, err := ss.GetSyncDomains(ctx, remoteChainID)
		if err != nil {
			return err
		}
		req = &SyncPolicyRequest{Version: SyncPolicyVersionDirectional, Epoch: control.PolicyEpoch, Revision: control.Revision, Domains: domains}
	}
	push := m.syncPolicyPushFn
	if push == nil {
		push = m.SyncPolicyPush
	}
	if _, err := push(ctx, remoteChainID, req); err != nil {
		return err
	}
	return ss.MarkSyncPolicyDelivered(context.Background(), remoteChainID, control.PolicyEpoch, control.Revision)
}
