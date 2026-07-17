package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// TestTrustOnlyV3MemberInviteIsIndependentFromDirectCopy proves a fresh JOIN can
// form a synchronization group without restoring its deliberately empty tx-33
// envelope or granting direct Copy/Subscribe. The exact frozen controller edge
// establishes identity; the signed group invitation is its own RBAC lane.
func TestTrustOnlyV3MemberInviteIsIndependentFromDirectCopy(t *testing.T) {
	ctx := context.Background()
	invitee := newCeremonyNode(t, "chain-m")
	ss := invitee.mgr.syncStore()
	require.NotNil(t, ss)

	controllerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	controllerID := hex.EncodeToString(controllerPub)
	controllerPin := []byte("pin-bytes-32-controller-rrrrrrrr")
	require.NoError(t, invitee.mgr.badger.SetCrossFed(
		"chain-r", "https://controller.invalid:8444", controllerPin, 4, 0,
		nil, nil, "active",
	))
	agreement, err := invitee.mgr.ActiveAgreement("chain-r")
	require.NoError(t, err)
	require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-r", Role: "guest", ControllerChainID: "chain-r",
		ControllerAgentID: controllerID, PeerAgentID: controllerID,
		PolicyEpoch: "fresh-v3-invite", RemoteCAPin: hex.EncodeToString(controllerPin),
		PolicyVersion: SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, ss.ActivateSyncControl(ctx, "chain-r", "fresh-v3-invite"))
	_, err = ss.ReplacePeerRBACPolicy(ctx, store.PeerRBACPolicy{
		RemoteChainID: "chain-r", PeerAgentID: controllerID,
		PolicyEpoch: "fresh-v3-invite", RemoteCAPin: hex.EncodeToString(controllerPin),
		PolicyVersion: store.CurrentPeerRBACPolicyVersion, Domains: []store.PeerRBACDomainPermission{},
	})
	require.NoError(t, err)
	ownPin, err := invitee.mgr.ownPin()
	require.NoError(t, err)
	payload := memberInvitePayload("chain-m", hex.EncodeToString(invitee.mgr.agentPub),
		store.GroupRoleSelectiveSync, hex.EncodeToString(ownPin))
	payload[pkSelectedDomains], err = encodeSelectedDomains([]string{"studio"})
	require.NoError(t, err)
	payload[pkInviteHead] = "frozen-roster-head"

	call := func(agentID string, p map[string]string) *httptest.ResponseRecorder {
		body, marshalErr := json.Marshal(memberInviteAcceptRequest{
			GroupID: "g-fresh-v3", ControllerPub: controllerID, Payload: p,
		})
		require.NoError(t, marshalErr)
		req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/group/member-invite/accept", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
			ChainID: "chain-r", AgentID: agentID, Agreement: agreement,
		}))
		rr := httptest.NewRecorder()
		invitee.mgr.handleMemberInviteAccept(rr, req)
		return rr
	}

	accepted := call(controllerID, payload)
	require.Equal(t, http.StatusOK, accepted.Code, accepted.Body.String())
	require.Empty(t, agreement.AllowedDomains, "fresh JOIN must remain trust-only")

	// Direct subscriptions may change independently; they neither grant nor revoke
	// the exact group invitation capability.
	_, err = ss.ApplyLocalDirectionalSyncPolicy(ctx, "chain-r", "fresh-v3-invite",
		SyncPolicyVersionPeerRBAC, 2, "local-2", nil, nil)
	require.NoError(t, err)
	stillAccepted := call(controllerID, payload)
	require.Equal(t, http.StatusOK, stillAccepted.Code, stillAccepted.Body.String())

	wrongPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	wrongOperator := call(hex.EncodeToString(wrongPub), payload)
	require.Equal(t, http.StatusForbidden, wrongOperator.Code, wrongOperator.Body.String())

	ownedPayload := make(map[string]string, len(payload))
	for key, value := range payload {
		ownedPayload[key] = value
	}
	ownedPayload[pkOwnedDomains], err = encodeSelectedDomains([]string{"not-owned"})
	require.NoError(t, err)
	unauthorizedOwner := call(controllerID, ownedPayload)
	require.Equal(t, http.StatusForbidden, unauthorizedOwner.Code, unauthorizedOwner.Body.String())
}
