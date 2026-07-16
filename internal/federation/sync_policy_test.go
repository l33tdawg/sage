package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestSyncPolicyHandlerRequiresFrozenHostAndAppliesSnapshot(t *testing.T) {
	m, ss, _ := newDrainTestManager(t)
	agreement := &store.CrossFedRecord{RemoteChainID: "chain-host", PeerPubKey: []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa"),
		AllowedDomains: []string{"shared"}, Status: "active"}
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-host", Role: "guest", ControllerChainID: "chain-host",
		ControllerAgentID: "host-agent", PolicyEpoch: "epoch", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
		PolicyVersion: SyncPolicyVersionLegacy,
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-host", "epoch"))

	call := func(agent string, revision int64, domains []string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(SyncPolicyRequest{Version: 1, Epoch: "epoch", Revision: revision, Domains: domains})
		req := httptest.NewRequest(http.MethodPut, "/fed/v1/sync/policy", bytes.NewReader(body))
		ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{ChainID: "chain-host", AgentID: agent, Agreement: agreement})
		rr := httptest.NewRecorder()
		m.handleSyncPolicy(rr, req.WithContext(ctx))
		return rr
	}

	assert.Equal(t, http.StatusForbidden, call("other-agent", 1, []string{"shared"}).Code)
	assert.Equal(t, http.StatusBadRequest, call("host-agent", 1, []string{"private"}).Code)
	assert.Equal(t, http.StatusOK, call("host-agent", 1, []string{"shared.alpha"}).Code)
	domains, err := ss.GetSyncDomains(context.Background(), "chain-host")
	require.NoError(t, err)
	assert.Equal(t, []string{"shared.alpha"}, domains)
	assert.Equal(t, http.StatusOK, call("host-agent", 1, []string{"shared.alpha"}).Code, "same revision+hash is idempotent")
	assert.Equal(t, http.StatusConflict, call("host-agent", 1, []string{"shared.beta"}).Code, "equal revision equivocation is refused")
}

func TestHostSyncPolicyPersistsAndDeliversLatestSnapshot(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-guest", 2, "shared")
	require.NoError(t, bs.RegisterAgent(hex.EncodeToString(m.agentPub), "operator", "admin", "", "test", "", 1))
	agreement, err := m.ActiveAgreement("chain-guest")
	require.NoError(t, err)
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-guest", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PolicyEpoch: "epoch-host",
		RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey), PolicyVersion: SyncPolicyVersionDirectional,
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-guest", "epoch-host"))
	var delivered *SyncPolicyRequest
	m.syncPolicyPushFn = func(_ context.Context, chain string, req *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		assert.Equal(t, "chain-guest", chain)
		copyReq := *req
		copyReq.Domains = append([]string(nil), req.Domains...)
		delivered = &copyReq
		return &SyncPolicyResponse{Status: "applied", Revision: req.Revision}, nil
	}
	result, err := m.SetHostSyncPolicy(context.Background(), "chain-guest", []string{"shared.alpha"})
	require.NoError(t, err)
	assert.Equal(t, "delivered", result.State)
	require.NotNil(t, delivered)
	assert.Equal(t, int64(1), delivered.Revision)
	control, err := ss.GetSyncControl(context.Background(), "chain-guest")
	require.NoError(t, err)
	assert.Equal(t, control.Revision, control.DeliveredRevision)
}

func TestPeerRBACPolicyPinsGuestOperatorOnHostSide(t *testing.T) {
	m, ss, _ := newDrainTestManager(t)
	agreement := &store.CrossFedRecord{RemoteChainID: "chain-guest",
		PeerPubKey: []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa"), Status: "active"}
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-guest", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: "guest-agent",
		PolicyEpoch: "epoch-v3", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-guest", "epoch-v3"))

	call := func(agent string, version int, revision int64) *httptest.ResponseRecorder {
		body, _ := json.Marshal(SyncPolicyRequest{Version: version,
			Epoch: "epoch-v3", Revision: revision})
		req := httptest.NewRequest(http.MethodPut, "/fed/v1/sync/policy", bytes.NewReader(body))
		ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
			ChainID: "chain-guest", AgentID: agent, Agreement: agreement,
		})
		rr := httptest.NewRecorder()
		m.handleSyncPolicy(rr, req.WithContext(ctx))
		return rr
	}

	assert.Equal(t, http.StatusForbidden, call("other-agent", SyncPolicyVersionPeerRBAC, 1).Code)
	assert.Equal(t, http.StatusConflict, call("guest-agent", SyncPolicyVersionLegacy, 1).Code)
	assert.Equal(t, http.StatusConflict, call("guest-agent", SyncPolicyVersionDirectional, 1).Code)
	assert.Equal(t, http.StatusOK, call("guest-agent", SyncPolicyVersionPeerRBAC, 1).Code)
}

func TestHostSyncPolicyCannotReplacePeerRBACPolicy(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-peer", 2, "shared")
	require.NoError(t, bs.RegisterAgent(hex.EncodeToString(m.agentPub), "operator", "admin", "", "test", "", 1))
	agreement, err := m.ActiveAgreement("chain-peer")
	require.NoError(t, err)
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-peer", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: strings.Repeat("ab", 32),
		PolicyEpoch: "epoch-v3", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
		PolicyVersion: SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-peer", "epoch-v3"))

	_, err = m.SetHostSyncPolicy(context.Background(), "chain-peer", []string{"shared"})
	require.ErrorContains(t, err, "cannot be replaced")
}

func TestSyncPolicyDomainAuthorizationFailsClosedAndAllowsOwner(t *testing.T) {
	m, _, bs := newDrainTestManager(t)
	agentID := hex.EncodeToString(m.agentPub)
	require.NoError(t, bs.RegisterAgent(agentID, "operator", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterDomain("owned", agentID, "", 1))

	require.NoError(t, m.authorizeSyncPolicyDomains([]string{"owned.child"}))
	assert.Error(t, m.authorizeSyncPolicyDomains([]string{"someone-elses-domain"}))
	assert.NoError(t, m.authorizeSyncPolicyDomains(nil), "disabling sync must remain possible")

	m.badger = nil
	assert.Error(t, m.authorizeSyncPolicyDomains([]string{"owned"}), "missing RBAC state must fail closed")
}

func TestDirectionalPublishRequiresPeerRBACCopy(t *testing.T) {
	ctx := context.Background()
	m, ss, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-peer", 2, "legacy")
	operatorID := hex.EncodeToString(m.agentPub)
	require.NoError(t, bs.RegisterAgent(operatorID, "operator", "admin", "", "test", "", 1))
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	agreement, err := m.ActiveAgreement("chain-peer")
	require.NoError(t, err)
	require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-peer", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: operatorID, PeerAgentID: hex.EncodeToString(peerPub),
		PolicyEpoch: "epoch-rbac", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}))
	require.NoError(t, ss.ActivateSyncControl(ctx, "chain-peer", "epoch-rbac"))
	m.syncPolicyPushFn = func(_ context.Context, _ string, req *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		return &SyncPolicyResponse{Status: "applied", Revision: req.Revision}, nil
	}

	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true, Copy: true},
	})
	require.NoError(t, err)
	result, err := m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"tii.project"}, []string{"peer.notes"})
	require.NoError(t, err, "configured PeerRBAC Copy must replace legacy tx-33 scope")
	assert.Equal(t, []string{"tii.project"}, result.PublishDomains)

	_, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"legacy"}, []string{"peer.notes"})
	require.ErrorContains(t, err, "not granted", "present PeerRBAC policy must not fall back to legacy scope")

	require.NoError(t, ss.DeletePeerRBACPolicy(ctx, "chain-peer"))
	_, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"legacy"}, nil)
	require.ErrorContains(t, err, "not granted", "v3 must never promote legacy tx-33 scope into Copy")
	_, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", nil, []string{"legacy"})
	require.NoError(t, err, "a recipient may subscribe without granting the peer Copy in the other direction")
}
