package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

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
	var remote JoinP2PBundle
	if err := json.NewDecoder(r.Body).Decode(&remote); err != nil || validateP2PBundle(remote) != nil {
		httpError(w, http.StatusBadRequest, "invalid p2p route bundle")
		return
	}
	local, err := hooks.LocalBundle()
	if err != nil || validateP2PBundle(local) != nil {
		httpError(w, http.StatusServiceUnavailable, "local relay route is not ready")
		return
	}
	if err := hooks.Persist(identity.ChainID, remote.Addrs); err != nil {
		httpError(w, http.StatusInternalServerError, "could not persist peer route")
		return
	}
	writeJSON(w, http.StatusOK, &local)
}

// ExchangeP2PRoutes upgrades a completed LAN agreement to roaming connectivity
// over the already-authenticated direct mTLS channel. Older peers simply return
// 404 and retain fully compatible LAN-only behavior.
func (m *Manager) ExchangeP2PRoutes(ctx context.Context, remoteChainID string, local JoinP2PBundle) error {
	if err := validateP2PBundle(local); err != nil {
		return err
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
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
	if err := json.Unmarshal(body, &remote); err != nil {
		return err
	}
	if err := validateP2PBundle(remote); err != nil {
		return err
	}
	hooks := m.joinP2PHooks()
	if hooks.Persist == nil {
		return fmt.Errorf("p2p route persistence unavailable")
	}
	return hooks.Persist(remoteChainID, remote.Addrs)
}
