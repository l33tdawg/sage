package federation

// RFC #443 Phase A2: the fleet-managed manual QR JOIN gate seam and the
// node-self-minted org link card.
//
// federation stays independent of where org policy lives: the REST layer
// installs a JoinPeerGate backed by the node-local org policy store. The
// Manager calls it on both sides of the ceremony, once when the peer's agent
// key is first learned and again under the agreement lease immediately before
// the side's own tx-33. A nil gate (nodes and tests with no org policy
// backend) leaves JOIN unchanged.

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/orgpolicy"
)

// JOIN gate sides and stages, recorded in override usage history.
const (
	JoinGateSideHost    = "host"
	JoinGateSideGuest   = "guest"
	JoinGateStageReq    = "request"
	JoinGateStageApprov = "approve"
	JoinGateStageConfrm = "confirm"
)

// JoinGateCheck is one gate evaluation.
type JoinGateCheck struct {
	Side         string
	Stage        string
	SessionID    string
	PeerAgentPub string // 64 lowercase hex
	PeerChainID  string
	// RootCredentialID / RootPub are this node's current Root credential
	// (pre-app-v23: the node operator key). Empty when it cannot be resolved,
	// in which case no override can apply.
	RootCredentialID string
	RootPub          ed25519.PublicKey
	Now              time.Time
}

// JoinPeerGate refuses a JOIN peer by returning a non-nil error whose text is
// shown to the operator. It must fail closed on any internal error.
type JoinPeerGate func(JoinGateCheck) error

// ErrJoinPeerNotEligible is wrapped by gate refusals.
var ErrJoinPeerNotEligible = errors.New("fleet JOIN refused")

// SetJoinPeerGate installs the fleet JOIN gate.
func (m *Manager) SetJoinPeerGate(g JoinPeerGate) {
	m.joinGateMu.Lock()
	m.joinGate = g
	m.joinGateMu.Unlock()
}

func (m *Manager) checkJoinPeer(side, stage, sessionID string, peerAgentPub []byte, peerChainID string) error {
	m.joinGateMu.RLock()
	g := m.joinGate
	m.joinGateMu.RUnlock()
	if g == nil {
		return nil
	}
	rootID, rootPub, rootErr := m.CurrentRootCredential()
	if rootErr != nil {
		rootID, rootPub = "", nil
	}
	err := g(JoinGateCheck{
		Side:             side,
		Stage:            stage,
		SessionID:        sessionID,
		PeerAgentPub:     hex.EncodeToString(peerAgentPub),
		PeerChainID:      peerChainID,
		RootCredentialID: rootID,
		RootPub:          rootPub,
		Now:              time.Now(),
	})
	if err != nil {
		m.logger.Warn().Err(err).Str("side", side).Str("stage", stage).
			Str("peer_chain", peerChainID).Str("session", shortID(sessionID)).Msg("fleet JOIN gate refused peer")
		if errors.Is(err, ErrJoinPeerNotEligible) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrJoinPeerNotEligible, err)
	}
	return nil
}

// CurrentRootCredential returns this node's current Root credential: after
// app-v23 the committed CEREBRUM Root, before it the node operator key that
// signs federation control.
func (m *Manager) CurrentRootCredential() (string, ed25519.PublicKey, error) {
	if m.postV23ForNextTx == nil || !m.postV23ForNextTx() {
		if len(m.agentPub) != ed25519.PublicKeySize {
			return "", nil, errors.New("node operator credential is unavailable")
		}
		pub := append(ed25519.PublicKey(nil), m.agentPub...)
		return hex.EncodeToString(pub), pub, nil
	}
	if m.badger == nil {
		return "", nil, errors.New("app-v23 Root state is unavailable")
	}
	root, err := m.badger.GetAppV23Root()
	if err != nil {
		return "", nil, fmt.Errorf("read current CEREBRUM Root: %w", err)
	}
	if root == nil || strings.TrimSpace(root.CredentialID) == "" {
		return "", nil, errors.New("no committed CEREBRUM Root")
	}
	pub, err := hex.DecodeString(root.CredentialID)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return "", nil, errors.New("current CEREBRUM Root credential id is not an ed25519 key")
	}
	return root.CredentialID, ed25519.PublicKey(pub), nil
}

// TransportAgentPub is the hex ed25519 key peers see in JOIN (host_agent_id /
// guest_agent_id) and that signs this node's link card.
func (m *Manager) TransportAgentPub() string { return hex.EncodeToString(m.agentPub) }

// OrgLinkCardRequest is what the operator supplies when minting the node's
// card. Everything else comes from local runtime state.
type OrgLinkCardRequest struct {
	OrgID     string
	CardRev   uint64
	IssuedAt  int64
	ExpiresAt int64
	// Endpoint is this node's externally reachable federation listener, the
	// same value the operator gives HostCreate. Empty means p2p-only, which
	// needs a ready p2p route.
	Endpoint string
}

// MintOrgLinkCard builds and signs this node's own link card. agent_pub,
// chain_id, the CA SPKI pin and the p2p peer id and routes come from local
// runtime state and are never taken from the caller.
func (m *Manager) MintOrgLinkCard(req OrgLinkCardRequest) (*orgpolicy.LinkCard, error) {
	if len(m.agentKey) != ed25519.PrivateKeySize {
		return nil, errors.New("node transport key is unavailable")
	}
	pin, err := m.ownPin()
	if err != nil {
		return nil, err
	}
	card := &orgpolicy.LinkCard{
		OrgID:     req.OrgID,
		ChainID:   m.localChainID,
		CASPKIPin: hex.EncodeToString(pin),
		Endpoint:  req.Endpoint,
		CardRev:   req.CardRev,
		IssuedAt:  req.IssuedAt,
		ExpiresAt: req.ExpiresAt,
	}
	if hooks := m.joinP2PHooks(); hooks.LocalBundle != nil && m.transportIsEnabled() {
		if bundle, bErr := hooks.LocalBundle(); bErr == nil && bundle.PeerID != "" && len(bundle.Addrs) > 0 {
			addrs := append([]string(nil), bundle.Addrs...)
			sort.Strings(addrs)
			addrs = dedupeSorted(addrs)
			if len(addrs) > 8 {
				addrs = addrs[:8]
			}
			card.P2PPeerID = bundle.PeerID
			card.P2PRoutes = addrs
		}
	}
	if card.Endpoint == "" {
		if card.P2PPeerID == "" {
			return nil, errors.New("endpoint is required when no p2p route is ready")
		}
		card.Endpoint = joinP2POnlyEndpoint
	} else if vErr := validateJoinEndpoint(card.Endpoint); vErr != nil {
		return nil, fmt.Errorf("invalid endpoint: %w", vErr)
	}
	if err := card.Sign(m.agentKey); err != nil {
		return nil, err
	}
	return card, nil
}

func dedupeSorted(in []string) []string {
	out := in[:0]
	for i, s := range in {
		if i == 0 || s != in[i-1] {
			out = append(out, s)
		}
	}
	return out
}
