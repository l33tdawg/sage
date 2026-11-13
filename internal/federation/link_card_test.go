package federation

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/orgpolicy"
)

func TestMintOrgLinkCardDerivesIdentityPinAndLiveRoutes(t *testing.T) {
	node := newCeremonyNode(t, "node-card-aaaaaa")
	node.mgr.federationListenAddr = "0.0.0.0:18444"
	bundle := testDirectRouteBundle(t, "127.0.0.1")
	node.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return bundle, nil },
	})
	now := time.Now().Unix()
	card, err := node.mgr.MintOrgLinkCard("org-acme", "https://127.0.0.1:18444", 9, now)
	require.NoError(t, err)
	require.NoError(t, card.Verify())
	require.Equal(t, "org-acme", card.OrgID)
	require.Equal(t, "node-card-aaaaaa", card.ChainID)
	require.Equal(t, hex.EncodeToString(node.mgr.agentPub), card.AgentPub)
	require.Equal(t, hex.EncodeToString(mustOwnPin(node.mgr)), card.CASPKIPin)
	require.Equal(t, bundle.PeerID, card.P2PPeerID)
	require.Equal(t, bundle.Addrs, card.P2PRoutes)
	require.Equal(t, uint64(9), card.CardRev)
	require.Equal(t, int64(orgpolicy.MaxLinkCardWindowSeconds), card.ExpiresAt-card.IssuedAt)
}

func TestMintOrgLinkCardRejectsUnconstrainedEndpointAndRoutes(t *testing.T) {
	node := newCeremonyNode(t, "node-card-bbbbbb")
	node.mgr.federationListenAddr = "0.0.0.0:18444"
	now := time.Now().Unix()

	_, err := node.mgr.MintOrgLinkCard("org-acme", "https://127.0.0.1:19444", 1, now)
	require.ErrorContains(t, err, "does not match federation listener port")
	_, err = node.mgr.MintOrgLinkCard("org-acme", "https://8.8.8.8:18444", 1, now)
	require.Error(t, err)

	// P2P-only is transcript material only when the live transport actually has
	// routes. A caller cannot promote it by supplying a route string.
	_, err = node.mgr.MintOrgLinkCard("org-acme", joinP2POnlyEndpoint, 1, now)
	require.ErrorContains(t, err, "requires live P2P routes")

	bundle := testDirectRouteBundle(t, "127.0.0.1")
	node.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return bundle, nil },
	})
	card, err := node.mgr.MintOrgLinkCard("org-acme", joinP2POnlyEndpoint, 2, now)
	require.NoError(t, err)
	require.Equal(t, bundle.PeerID, card.P2PPeerID)
	require.Equal(t, bundle.Addrs, card.P2PRoutes)
	require.False(t, strings.Contains(card.Endpoint, "203.0.113"))
}
