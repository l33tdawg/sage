package federation

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
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
