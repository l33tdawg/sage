package abci

import (
	"errors"
	"net/netip"
	"strings"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/statesync"
)

const (
	bootStateSyncP2PFilterPrefix = "/p2p/filter"
	bootStateSyncP2PAddrPrefix   = bootStateSyncP2PFilterPrefix + "/addr/"
	bootStateSyncP2PIDPrefix     = bootStateSyncP2PFilterPrefix + "/id/"
	bootStateSyncP2PRejectCode   = 111
)

func isBootStateSyncP2PFilterQuery(req *abcitypes.RequestQuery) bool {
	return req != nil && (req.Path == bootStateSyncP2PFilterPrefix ||
		strings.HasPrefix(req.Path, bootStateSyncP2PFilterPrefix+"/"))
}

// queryBootStateSyncP2PFilter is CometBFT's authenticated transport admission
// callback. Comet invokes addr filtering before the secret-connection handshake
// exposes a node ID, then invokes ID filtering before adding the peer to the
// switch. Address admission is therefore deliberately limited to one valid
// unicast/loopback IP endpoint and only advances the connection to the exact ID
// check; it is never sufficient by itself.
//
// This path bypasses the normal application Query gate because a receiver must
// establish its approved provider connection while its runtime is Discovering.
// It delegates no application query and rechecks the controller's monotonic,
// rollback-latched authorization on every invocation.
func (r *BootStateSyncRuntime) queryBootStateSyncP2PFilter(req *abcitypes.RequestQuery) *abcitypes.ResponseQuery {
	approved, err := r.authorizedBootStateSyncPeerIDs()
	if err != nil {
		return rejectBootStateSyncP2PFilter("state sync P2P authorization is unavailable")
	}

	path := req.Path
	switch {
	case strings.HasPrefix(path, bootStateSyncP2PAddrPrefix):
		candidate := strings.TrimPrefix(path, bootStateSyncP2PAddrPrefix)
		if candidate == "" || strings.Contains(candidate, "/") {
			return rejectBootStateSyncP2PFilter("state sync P2P address is malformed")
		}
		address, parseErr := netip.ParseAddrPort(candidate)
		ip := address.Addr()
		if parseErr != nil || !address.IsValid() || address.Port() == 0 ||
			ip.Zone() != "" || (!ip.IsGlobalUnicast() && !ip.IsLoopback()) {
			return rejectBootStateSyncP2PFilter("state sync P2P address is malformed")
		}
		return &abcitypes.ResponseQuery{}

	case strings.HasPrefix(path, bootStateSyncP2PIDPrefix):
		candidate := strings.TrimPrefix(path, bootStateSyncP2PIDPrefix)
		if strings.Contains(candidate, "/") || !statesync.ValidCometNodeID(candidate) {
			return rejectBootStateSyncP2PFilter("state sync P2P node ID is malformed")
		}
		for _, nodeID := range approved {
			if candidate == nodeID {
				return &abcitypes.ResponseQuery{}
			}
		}
		return rejectBootStateSyncP2PFilter("state sync P2P node ID is not approved")

	default:
		return rejectBootStateSyncP2PFilter("state sync P2P filter path is invalid")
	}
}

func (r *BootStateSyncRuntime) authorizedBootStateSyncPeerIDs() ([]string, error) {
	if r == nil {
		return nil, errors.New("state sync runtime is missing")
	}
	// Do not acquire the runtime bundle lease here. PendingComet deliberately
	// holds it exclusively through durable sealing, while Comet's transport
	// admission callback must remain independently answerable. Failure is a
	// one-way phase transition, so an atomic latch is sufficient and race-free.
	if r.p2pFilterFailed.Load() {
		return nil, errors.New("state sync runtime has failed")
	}
	r.endpointsMu.RLock()
	defer r.endpointsMu.RUnlock()
	var approved []string
	switch {
	case r.serving != nil && r.receiving == nil:
		if err := r.serving.requireAuthorized(); err != nil {
			// Expiry closes the joining node's snapshot/P2P session, but must
			// not partition this provider from validators that were already in
			// the locally installed authorization. The expiry latch is sticky,
			// so this fallback can never re-admit the joining node after rollback.
			r.serving.authMu.Lock()
			expired := r.serving.expired
			r.serving.authMu.Unlock()
			if !expired || r.serving.authorization == nil {
				return nil, err
			}
			approved = r.serving.authorization.ValidatorNodeIDs()
			break
		}
		if r.serving.authorization == nil {
			return nil, errors.New("state sync serving authorization is missing")
		}
		approved = r.serving.authorization.ApprovedPeerNodeIDs()

	case r.receiving != nil && r.serving == nil:
		r.receiving.mu.Lock()
		defer r.receiving.mu.Unlock()
		if err := r.receiving.requireAuthorizedLocked(); err != nil {
			// Before sealing, expiry is terminal for the boot attempt. After
			// sealing, it closes only the transfer session: the validator mesh
			// keeps its immutable provider peer IDs for crash/reconnect safety.
			if !r.p2pFilterSealed.Load() || r.receiving.authorization == nil {
				return nil, err
			}
			approved = r.receiving.authorization.ValidatorNodeIDs()
			break
		}
		if r.receiving.authorization == nil {
			return nil, errors.New("state sync receiving authorization is missing")
		}
		approved = r.receiving.authorization.ApprovedPeerNodeIDs()

	default:
		return nil, errors.New("exactly one state sync endpoint controller must be armed")
	}
	if len(approved) == 0 {
		return nil, errors.New("state sync P2P validator allowlist is empty")
	}
	// Failure can race while requireAuthorized is blocked on the controller's
	// clock or mutex. Recheck after authorization and the private ID copy so a
	// decision computed before that one-way transition is never returned after
	// the runtime has failed.
	if r.p2pFilterFailed.Load() {
		return nil, errors.New("state sync runtime failed during P2P authorization")
	}
	return approved, nil
}

func rejectBootStateSyncP2PFilter(log string) *abcitypes.ResponseQuery {
	return &abcitypes.ResponseQuery{
		Code: bootStateSyncP2PRejectCode,
		Log:  log,
	}
}
