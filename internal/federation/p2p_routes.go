package federation

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/l33tdawg/sage/internal/store"
)

// p2pRouteBinding is the immutable JOIN generation that authenticated a route
// exchange. Route persistence must still see this exact binding at its final
// write point: a delayed request/response from a revoked or replaced ceremony
// must never install routes into the replacement connection.
type p2pRouteBinding struct {
	peerAgentID        string
	agreementCAPin     string
	role               string
	controllerChainID  string
	controllerAgentID  string
	frozenPeerAgentID  string
	policyEpoch        string
	controlRemoteCAPin string
	bindingState       string
}

// currentP2PRouteBinding resolves the live agreement and its exact
// ceremony-frozen operator/control tuple. Callers hold the sync-policy read
// lease while invoking this helper; final persistence keeps that lease until
// Persist returns so PurgeSyncPeerState cannot complete ahead of a stale write.
func (m *Manager) currentP2PRouteBinding(ctx context.Context, remoteChainID string) (*store.CrossFedRecord, p2pRouteBinding, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, p2pRouteBinding{}, err
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, p2pRouteBinding{}, fmt.Errorf("p2p route exchange requires the SQLite store backend")
	}
	peerAgentID, err := m.resolvePeerOperatorAgentID(ctx, agreement)
	if err != nil {
		return nil, p2pRouteBinding{}, err
	}
	control, err := ss.GetSyncControl(ctx, remoteChainID)
	if err != nil {
		return nil, p2pRouteBinding{}, fmt.Errorf("read p2p route binding: %w", err)
	}
	if control == nil {
		return nil, p2pRouteBinding{}, fmt.Errorf("connection has no active p2p route binding")
	}
	if control.RemoteChainID != remoteChainID || control.BindingState != "active" ||
		control.PolicyEpoch == "" || control.RemoteCAPin != hex.EncodeToString(agreement.PeerPubKey) ||
		peerAgentID == "" {
		return nil, p2pRouteBinding{}, fmt.Errorf("connection p2p route binding does not match its active trust generation")
	}
	peer := &peerIdentity{ChainID: remoteChainID, AgentID: peerAgentID, Agreement: agreement}
	if control.PeerAgentID == "" || control.PeerAgentID != peerAgentID || !m.syncControlPeerBound(control, peer) {
		return nil, p2pRouteBinding{}, fmt.Errorf("connection p2p route binding does not match its frozen peer operator")
	}
	return agreement, p2pRouteBinding{
		peerAgentID:        peerAgentID,
		agreementCAPin:     hex.EncodeToString(agreement.PeerPubKey),
		role:               control.Role,
		controllerChainID:  control.ControllerChainID,
		controllerAgentID:  control.ControllerAgentID,
		frozenPeerAgentID:  control.PeerAgentID,
		policyEpoch:        control.PolicyEpoch,
		controlRemoteCAPin: control.RemoteCAPin,
		bindingState:       control.BindingState,
	}, nil
}

func validateP2PBundle(bundle JoinP2PBundle) error {
	if bundle.Protocol != "/sage/fed/1.0.0" || len(bundle.Addrs) == 0 || len(bundle.Addrs) > 4 {
		return fmt.Errorf("invalid p2p route bundle")
	}
	declared, err := peer.Decode(bundle.PeerID)
	if err != nil {
		return fmt.Errorf("invalid p2p peer id")
	}
	hasCircuit := false
	for _, raw := range bundle.Addrs {
		if len(raw) == 0 || len(raw) > 512 {
			return fmt.Errorf("invalid p2p route")
		}
		addr, err := ma.NewMultiaddr(raw)
		if err != nil {
			return fmt.Errorf("invalid p2p route: %w", err)
		}
		info, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil || info.ID != declared {
			return fmt.Errorf("p2p route peer mismatch")
		}
		hasCircuit = hasCircuit || strings.Contains(raw, "/p2p-circuit/")
	}
	if !hasCircuit {
		return fmt.Errorf("p2p route bundle has no relay fallback")
	}
	return nil
}

func (m *Manager) handleP2PRoutes(w http.ResponseWriter, r *http.Request) {
	identity := peerFromCtx(r.Context())
	hooks := m.joinP2PHooks()
	if identity == nil || hooks.Persist == nil || hooks.LocalBundle == nil {
		httpError(w, http.StatusNotImplemented, "p2p route exchange unavailable")
		return
	}
	ss := m.syncStore()
	if ss == nil || identity.Agreement == nil {
		httpError(w, http.StatusForbidden, "p2p route exchange requires the frozen peer operator")
		return
	}
	// Snapshot the ceremony generation that authenticated this request. This
	// short lease does not cover body parsing or local route discovery.
	unlock := ss.LockSyncPolicyRead()
	_, expectedBinding, err := m.currentP2PRouteBinding(r.Context(), identity.ChainID)
	unlock()
	if err != nil || identity.Agreement.RemoteChainID != identity.ChainID ||
		expectedBinding.peerAgentID == "" || identity.AgentID != expectedBinding.peerAgentID ||
		expectedBinding.agreementCAPin != hex.EncodeToString(identity.Agreement.PeerPubKey) {
		httpError(w, http.StatusForbidden, "p2p route exchange requires the frozen peer operator")
		return
	}
	var remote JoinP2PBundle
	if decodeErr := json.NewDecoder(r.Body).Decode(&remote); decodeErr != nil || validateP2PBundle(remote) != nil {
		httpError(w, http.StatusBadRequest, "invalid p2p route bundle")
		return
	}
	local, err := hooks.LocalBundle()
	if err != nil || validateP2PBundle(local) != nil {
		httpError(w, http.StatusServiceUnavailable, "local relay route is not ready")
		return
	}

	// Linearization point with revoke: re-resolve the active agreement and the
	// exact frozen JOIN tuple under the read side of the policy gate, then keep
	// the lease through Persist. A concurrent purge waits, persists its delete,
	// and finally Remove runs after this write; a completed purge makes this
	// recheck fail before any route can be re-added.
	unlock = ss.LockSyncPolicyRead()
	_, currentBinding, err := m.currentP2PRouteBinding(r.Context(), identity.ChainID)
	if err != nil || currentBinding != expectedBinding {
		unlock()
		httpError(w, http.StatusForbidden, "p2p route binding changed before persistence")
		return
	}
	if err := hooks.Persist(identity.ChainID, remote.Addrs); err != nil {
		unlock()
		httpError(w, http.StatusInternalServerError, "could not persist peer route")
		return
	}
	unlock()
	writeJSON(w, http.StatusOK, &local)
}

// ExchangeP2PRoutes upgrades a completed LAN agreement to roaming connectivity
// over the already-authenticated direct mTLS channel. Older peers simply return
// 404 and retain fully compatible LAN-only behavior.
func (m *Manager) ExchangeP2PRoutes(ctx context.Context, remoteChainID string, local JoinP2PBundle) error {
	if err := validateP2PBundle(local); err != nil {
		return err
	}
	ss := m.syncStore()
	if ss == nil {
		return fmt.Errorf("p2p route exchange requires the SQLite store backend")
	}
	// Capture one exact JOIN generation, but never hold the policy lease across
	// the network request. The response is authorized again at its write point.
	unlock := ss.LockSyncPolicyRead()
	agreement, expectedBinding, err := m.currentP2PRouteBinding(ctx, remoteChainID)
	unlock()
	if err != nil {
		return err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/p2p/routes", &local)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented {
		return fmt.Errorf("peer does not support roaming routes")
	}
	if status != http.StatusOK {
		return fmt.Errorf("peer route exchange returned %d: %s", status, truncate(body, 200))
	}
	var remote JoinP2PBundle
	if unmarshalErr := json.Unmarshal(body, &remote); unmarshalErr != nil {
		return unmarshalErr
	}
	if validationErr := validateP2PBundle(remote); validationErr != nil {
		return validationErr
	}
	hooks := m.joinP2PHooks()
	if hooks.Persist == nil {
		return fmt.Errorf("p2p route persistence unavailable")
	}
	unlock = ss.LockSyncPolicyRead()
	_, currentBinding, err := m.currentP2PRouteBinding(ctx, remoteChainID)
	if err != nil {
		unlock()
		return fmt.Errorf("p2p route binding is no longer active: %w", err)
	}
	if currentBinding != expectedBinding {
		unlock()
		return fmt.Errorf("p2p route binding changed before persistence")
	}
	err = hooks.Persist(remoteChainID, remote.Addrs)
	unlock()
	return err
}
