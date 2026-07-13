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
	Version  int      `json:"version"`
	Epoch    string   `json:"epoch"`
	Revision int64    `json:"revision"`
	Domains  []string `json:"domains"`
}

type SyncPolicyResponse struct {
	Status   string `json:"status"`
	Revision int64  `json:"revision"`
}

type HostSyncPolicyResult struct {
	Revision int64    `json:"revision"`
	Domains  []string `json:"domains"`
	State    string   `json:"state"`
}

func canonicalSyncDomains(raw []string, agreement *store.CrossFedRecord) ([]string, error) {
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
		if agreement == nil || !DomainAllowed(agreement.AllowedDomains, domain) {
			return nil, fmt.Errorf("domain %q is outside the federation treaty", domain)
		}
	}
	return out, nil
}

func syncPolicyHash(epoch, controller string, revision int64, domains []string) string {
	h := sha256.New()
	h.Write([]byte("sage-sync-policy-v1\x00"))
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Version != 1 || req.Revision <= 0 {
		httpError(w, http.StatusBadRequest, "invalid sync policy")
		return
	}
	control, err := ss.GetSyncControl(r.Context(), peer.ChainID)
	if err != nil || control == nil || control.BindingState != "active" || control.Role != "guest" {
		httpError(w, http.StatusForbidden, "peer is not this node's sync controller")
		return
	}
	if control.ControllerChainID != peer.ChainID || control.ControllerAgentID != peer.AgentID || control.PolicyEpoch != req.Epoch ||
		control.RemoteCAPin != hex.EncodeToString(peer.Agreement.PeerPubKey) {
		httpError(w, http.StatusForbidden, "sync controller binding mismatch")
		return
	}
	domains, err := canonicalSyncDomains(req.Domains, peer.Agreement)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	hash := syncPolicyHash(req.Epoch, peer.ChainID, req.Revision, domains)
	status, err := ss.ApplySyncPolicy(r.Context(), peer.ChainID, req.Epoch, req.Revision, hash, domains)
	if err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &SyncPolicyResponse{Status: status, Revision: req.Revision})
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
	domains, err := canonicalSyncDomains(raw, agreement)
	if err != nil {
		return nil, err
	}
	if err := m.authorizeSyncPolicyDomains(domains); err != nil {
		return nil, err
	}
	revision := control.Revision + 1
	hash := syncPolicyHash(control.PolicyEpoch, m.localChainID, revision, domains)
	if _, err := ss.ApplySyncPolicy(ctx, remoteChainID, control.PolicyEpoch, revision, hash, domains); err != nil {
		return nil, err
	}
	state := "pending"
	if err := m.deliverSyncPolicy(ctx, ss, remoteChainID); err == nil {
		state = "delivered"
	}
	m.nudgeSync()
	return &HostSyncPolicyResult{Revision: revision, Domains: domains, State: state}, nil
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
	if err != nil || control == nil || control.Role != "host" || control.Revision <= control.DeliveredRevision {
		return err
	}
	domains, err := ss.GetSyncDomains(ctx, remoteChainID)
	if err != nil {
		return err
	}
	req := &SyncPolicyRequest{Version: 1, Epoch: control.PolicyEpoch, Revision: control.Revision, Domains: domains}
	push := m.syncPolicyPushFn
	if push == nil {
		push = m.SyncPolicyPush
	}
	if _, err := push(ctx, remoteChainID, req); err != nil {
		return err
	}
	return ss.MarkSyncPolicyDelivered(context.Background(), remoteChainID, control.PolicyEpoch, control.Revision)
}
