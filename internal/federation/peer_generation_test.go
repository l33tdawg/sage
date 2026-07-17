package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// Consensus terms and the CA/operator can be byte-identical after a fresh
// reciprocal JOIN. The off-consensus policy epoch is therefore part of every
// authenticated request generation: work captured under E1 must not authorize
// a query, group mutation, or revoke after E2 becomes active.
func TestCapturedRequestGenerationRejectsIdenticalTermsFreshPolicyEpoch(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-peer", 4)
	agreement, err := m.ActiveAgreement("chain-peer")
	require.NoError(t, err)
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerID := hex.EncodeToString(peerPub)
	pin := hex.EncodeToString(agreement.PeerPubKey)
	activate := func(epoch string) {
		require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
			RemoteChainID: "chain-peer", Role: "guest", ControllerChainID: "chain-peer",
			ControllerAgentID: peerID, PeerAgentID: peerID, PolicyEpoch: epoch,
			RemoteCAPin: pin, PolicyVersion: SyncPolicyVersionPeerRBAC,
		}))
		require.NoError(t, ss.ActivateSyncControl(ctx, "chain-peer", epoch))
	}
	activate("epoch-e1")
	stale := &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement,
		CeremonyCaptured: true,
		Ceremony: &peerCeremonyBinding{
			PeerAgentID: peerID, PolicyEpoch: "epoch-e1", RemoteCAPin: pin, State: "active",
		},
	}

	require.NoError(t, ss.PurgeSyncPeerState(ctx, "chain-peer"))
	activate("epoch-e2")
	_, err = m.currentRequestAgreementBound(ctx, stale)
	require.ErrorContains(t, err, "ceremony generation changed")
	bound, bindErr := m.currentInboundGroupPeerBound(ctx, ss, stale)
	require.NoError(t, bindErr)
	assert.False(t, bound, "stale E1 group work became valid under identical E2 terms")

	// Use E2 in the body to prove the denial comes from the captured request
	// generation, not merely from comparing notice.PolicyEpoch to live control.
	m.broadcastFn = func([]byte) (string, int64, error) { return "must-not-broadcast", 1, nil }
	_, err = m.acceptPeerRevokeNotice(ctx, stale, RevokeNotice{PolicyEpoch: "epoch-e2"})
	require.Error(t, err)
	assert.Equal(t, "active", m.crossFedStatus("chain-peer"))
}
