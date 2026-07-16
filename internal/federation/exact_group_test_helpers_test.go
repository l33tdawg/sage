package federation

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// bindInboundGroupPeer seeds the exact active JOIN identity beneath a group
// handler test. Tests may still exercise legacy domain semantics by keeping the
// policy version at 1; only the transport identity binding is relevant here.
func bindInboundGroupPeer(t *testing.T, m *Manager, ss *store.SQLiteStore, peer *peerIdentity, role string) {
	t.Helper()
	require.NotNil(t, peer)
	require.NotNil(t, peer.Agreement)
	if len(peer.Agreement.PeerPubKey) == 0 {
		peer.Agreement.PeerPubKey = bytes.Repeat([]byte{0x7d}, 32)
	}
	peer.Agreement.RemoteChainID = peer.ChainID
	peer.Agreement.Status = "active"
	if m.badger == nil {
		attachBadger(t, m)
	}
	require.NoError(t, m.badger.SetCrossFed(
		peer.ChainID,
		peer.Agreement.Endpoint,
		peer.Agreement.PeerPubKey,
		peer.Agreement.MaxClearance,
		peer.Agreement.ExpiresAt,
		peer.Agreement.AllowedDomains,
		peer.Agreement.AllowedDepts,
		"active",
	))
	current, err := m.ActiveAgreement(peer.ChainID)
	require.NoError(t, err)
	peer.Agreement = current
	control := store.SyncControl{
		RemoteChainID: peer.ChainID,
		Role:          role,
		PeerAgentID:   peer.AgentID,
		PolicyEpoch:   "test-group-edge-" + peer.ChainID,
		RemoteCAPin:   hex.EncodeToString(peer.Agreement.PeerPubKey),
		PolicyVersion: 1,
	}
	if role == "host" {
		control.ControllerChainID = m.localChainID
		control.ControllerAgentID = hex.EncodeToString(m.agentPub)
	} else {
		control.ControllerChainID = peer.ChainID
		control.ControllerAgentID = peer.AgentID
	}
	require.NoError(t, ss.PrepareSyncControl(context.Background(), control))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), peer.ChainID, control.PolicyEpoch))
}

// bindTestPeerAgreement gives handler-unit tests the current consensus trust
// snapshot that peerAuth would have attached in production. It deliberately
// does not invent a sync_control: tests without a JOIN-era binding continue to
// exercise the true legacy treaty path.
func bindTestPeerAgreement(t *testing.T, m *Manager, peer *peerIdentity) {
	t.Helper()
	require.NotNil(t, peer)
	require.NotNil(t, peer.Agreement)
	if m.badger == nil {
		attachBadger(t, m)
	}
	if len(peer.Agreement.PeerPubKey) == 0 {
		peer.Agreement.PeerPubKey = bytes.Repeat([]byte{0x7e}, 32)
	}
	_, _, _, _, _, _, status, getErr := m.badger.GetCrossFed(peer.ChainID)
	if getErr != nil {
		require.NoError(t, m.badger.SetCrossFed(
			peer.ChainID,
			peer.Agreement.Endpoint,
			peer.Agreement.PeerPubKey,
			peer.Agreement.MaxClearance,
			peer.Agreement.ExpiresAt,
			peer.Agreement.AllowedDomains,
			peer.Agreement.AllowedDepts,
			"active",
		))
	} else if status != "active" {
		return
	}
	current, err := m.ActiveAgreement(peer.ChainID)
	if err == nil {
		peer.Agreement = current
	}
}
