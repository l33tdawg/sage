package federation

// Local link-card source (RFC #443 Phase A2). The node, not the browser or an
// issuer-side registry, supplies every identity/transport field and signs the
// card with its own stable federation agent key.

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/l33tdawg/sage/internal/orgpolicy"
)

// ErrLinkCardRuntimeUnavailable means the node cannot currently derive a
// truthful card because federation or its live transport material is absent.
var ErrLinkCardRuntimeUnavailable = errors.New("link card runtime unavailable")

// LocalFederationAgentPub returns the stable transport identity used as the
// self-minted card's agent_pub.
func (m *Manager) LocalFederationAgentPub() string {
	return hex.EncodeToString(m.agentPub)
}

// MintOrgLinkCard derives a new card from live local state. endpoint is the only
// caller-selected input (plus the monotonic revision): it must resolve to a
// local interface and use the effective federation listener port. When P2P is
// enabled, routes and peer ID come from the live transport bundle and cannot be
// supplied by the request.
func (m *Manager) MintOrgLinkCard(orgID, endpoint string, cardRev uint64, issuedAt int64) (orgpolicy.LinkCard, error) {
	if m.transportDisabled.Load() {
		return orgpolicy.LinkCard{}, fmt.Errorf("%w: federation transport is disabled", ErrLinkCardRuntimeUnavailable)
	}
	if strings.TrimSpace(orgID) == "" {
		return orgpolicy.LinkCard{}, fmt.Errorf("link card org is required")
	}
	if issuedAt <= 0 {
		return orgpolicy.LinkCard{}, fmt.Errorf("link card issue time is invalid")
	}
	if err := validateJoinEndpoint(endpoint); err != nil {
		return orgpolicy.LinkCard{}, err
	}

	var bundle JoinP2PBundle
	if hooks := m.joinP2PHooks(); hooks.LocalBundle != nil {
		live, err := hooks.LocalBundle()
		if err != nil {
			return orgpolicy.LinkCard{}, fmt.Errorf("%w: live P2P routes unavailable: %v", ErrLinkCardRuntimeUnavailable, err)
		}
		if err := validateP2PBundle(live); err != nil {
			return orgpolicy.LinkCard{}, fmt.Errorf("%w: live P2P routes invalid: %v", ErrLinkCardRuntimeUnavailable, err)
		}
		bundle = live
	}
	if err := m.validateCardEndpoint(endpoint, len(bundle.Addrs) > 0); err != nil {
		return orgpolicy.LinkCard{}, err
	}

	pin, err := m.ownPin()
	if err != nil {
		return orgpolicy.LinkCard{}, fmt.Errorf("%w: node CA pin unavailable: %v", ErrLinkCardRuntimeUnavailable, err)
	}
	card := orgpolicy.LinkCard{
		OrgID:     orgID,
		ChainID:   m.localChainID,
		CASPKIPin: hex.EncodeToString(pin),
		Endpoint:  endpoint,
		P2PPeerID: bundle.PeerID,
		P2PRoutes: append([]string(nil), bundle.Addrs...),
		CardRev:   cardRev,
		IssuedAt:  issuedAt,
		ExpiresAt: issuedAt + orgpolicy.MaxLinkCardWindowSeconds,
	}
	if err := card.Sign(m.agentKey); err != nil {
		return orgpolicy.LinkCard{}, err
	}
	return card, nil
}

func (m *Manager) validateCardEndpoint(endpoint string, hasLiveP2P bool) error {
	if m.federationListenAddr == "" {
		return fmt.Errorf("%w: federation listener address is unavailable", ErrLinkCardRuntimeUnavailable)
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("invalid link card endpoint: %w", err)
	}
	_, listenPort, err := net.SplitHostPort(m.federationListenAddr)
	if err != nil || listenPort == "" {
		return fmt.Errorf("%w: federation listener address is invalid", ErrLinkCardRuntimeUnavailable)
	}
	if endpoint != joinP2POnlyEndpoint {
		if u.Port() != listenPort {
			return fmt.Errorf("link card endpoint port %s does not match federation listener port %s", u.Port(), listenPort)
		}
		if !linkCardHostIsLocal(u.Hostname()) {
			return fmt.Errorf("link card endpoint host must be an address on this node")
		}
	} else if !hasLiveP2P {
		return fmt.Errorf("P2P-only link card endpoint requires live P2P routes")
	}
	return nil
}

func linkCardHostIsLocal(host string) bool {
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return true
		}
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return false
		}
		for _, addr := range addrs {
			var local net.IP
			switch value := addr.(type) {
			case *net.IPNet:
				local = value.IP
			case *net.IPAddr:
				local = value.IP
			}
			if local != nil && local.Equal(ip) {
				return true
			}
		}
		return false
	}
	localName, err := os.Hostname()
	return err == nil && strings.EqualFold(host, localName)
}
