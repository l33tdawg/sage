package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

type pauseContractDriver struct {
	FederationJoinDriver
	policy *store.PeerRBACPolicy
	calls  []bool
}

func (d *pauseContractDriver) ResolvePeerOperatorAgentID(context.Context, string) (string, error) {
	return d.policy.PeerAgentID, nil
}
func (d *pauseContractDriver) GetPeerRBACPolicy(context.Context, string) (*store.PeerRBACPolicy, error) {
	clone := *d.policy
	clone.Domains = append([]store.PeerRBACDomainPermission(nil), d.policy.Domains...)
	return &clone, nil
}
func (d *pauseContractDriver) ReplacePeerRBACPolicy(_ context.Context, _ string, domains []store.PeerRBACDomainPermission) (*store.PeerRBACPolicy, error) {
	d.policy.Domains = append([]store.PeerRBACDomainPermission(nil), domains...)
	return d.GetPeerRBACPolicy(context.Background(), d.policy.RemoteChainID)
}
func (d *pauseContractDriver) SetPeerRBACPaused(_ context.Context, _ string, paused bool) (*store.PeerRBACPolicy, error) {
	d.calls = append(d.calls, paused)
	d.policy.Paused = paused
	return d.GetPeerRBACPolicy(context.Background(), d.policy.RemoteChainID)
}
func (d *pauseContractDriver) PeerStatus(context.Context, string) (*federation.StatusResponse, error) {
	return &federation.StatusResponse{PeerRBACGrant: &federation.PeerRBACGrant{
		PolicyVersion: federation.SyncPolicyVersionPeerRBAC, Paused: true,
		Domains: []federation.PeerRBACDomainGrant{},
	}}, nil
}

func TestNormalizeRequestedPeerPermissionsEnforcesDependencies(t *testing.T) {
	permissions, err := normalizeRequestedPeerPermissions([]store.PeerRBACDomainPermission{
		{Domain: "tii.read", Read: true},
		{Domain: "tii.copy", Copy: true},
		{Domain: "tii.none"},
	})
	require.NoError(t, err)
	require.Equal(t, []store.PeerRBACDomainPermission{
		{Domain: "tii.copy", Read: true, Copy: true},
		{Domain: "tii.read", Read: true},
	}, permissions)
}

func TestNormalizeRequestedPeerPermissionsRejectsWriteCapability(t *testing.T) {
	permissions, err := normalizeRequestedPeerPermissions([]store.PeerRBACDomainPermission{
		{Domain: "tii", Write: true},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "consensus-bound ingress capability")
	require.Nil(t, permissions)
}

func TestClonePeerPermissionsSanitizesStaleWriteAndCanonicalizesCopy(t *testing.T) {
	permissions := clonePeerPermissions([]store.PeerRBACDomainPermission{{
		Domain: "tii", Write: true, Copy: true,
	}})
	require.Equal(t, []store.PeerRBACDomainPermission{{
		Domain: "tii", Read: true, Write: false, Copy: true,
	}}, permissions)
}

func TestFederationPauseEndpointPreservesPolicyAndReportsBothDirections(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "pause.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	peerPin := bytes.Repeat([]byte{0x55}, 32)
	require.NoError(t, bs.SetCrossFed("chain-peer", "https://peer:8444", peerPin, 4, 0, nil, nil, "active"))
	driver := &pauseContractDriver{policy: &store.PeerRBACPolicy{
		RemoteChainID: "chain-peer", PeerAgentID: "peer-agent", PolicyEpoch: "epoch-pause",
		RemoteCAPin: "pin", PolicyVersion: federation.SyncPolicyVersionPeerRBAC,
		Domains: []store.PeerRBACDomainPermission{{Domain: "tii", Read: true, Copy: true}},
	}}
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.Federation = driver

	pauseReq := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/pause",
		bytes.NewBufferString(`{"paused":true}`)), "chain-peer")
	pauseReq.RemoteAddr = "127.0.0.1:4242"
	pauseReq.Header.Set("Sec-Fetch-Site", "same-origin")
	pauseRR := httptest.NewRecorder()
	h.handleFedPause(pauseRR, pauseReq)
	require.Equal(t, http.StatusOK, pauseRR.Code, pauseRR.Body.String())
	require.Equal(t, []bool{true}, driver.calls)
	require.True(t, driver.policy.Paused)
	require.Equal(t, []store.PeerRBACDomainPermission{{Domain: "tii", Read: true, Copy: true}}, driver.policy.Domains)
	var pauseBody struct {
		Paused      bool                             `json:"paused"`
		Permissions []store.PeerRBACDomainPermission `json:"permissions"`
	}
	require.NoError(t, json.NewDecoder(pauseRR.Body).Decode(&pauseBody))
	require.True(t, pauseBody.Paused)
	require.Len(t, pauseBody.Permissions, 1)

	permissionsReq := withFederationChain(httptest.NewRequest(http.MethodGet,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/permissions", nil), "chain-peer")
	permissionsReq.RemoteAddr = "127.0.0.1:4242"
	permissionsReq.Header.Set("Sec-Fetch-Site", "same-origin")
	permissionsRR := httptest.NewRecorder()
	h.handleFedPermissionsGet(permissionsRR, permissionsReq)
	require.Equal(t, http.StatusOK, permissionsRR.Code, permissionsRR.Body.String())
	var permissionsBody struct {
		LocalPaused  bool `json:"local_paused"`
		RemotePaused bool `json:"remote_paused"`
	}
	require.NoError(t, json.NewDecoder(permissionsRR.Body).Decode(&permissionsBody))
	require.True(t, permissionsBody.LocalPaused)
	require.True(t, permissionsBody.RemotePaused)

	resumeReq := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-peer/pause",
		bytes.NewBufferString(`{"paused":false}`)), "chain-peer")
	resumeReq.RemoteAddr = "127.0.0.1:4242"
	resumeReq.Header.Set("Sec-Fetch-Site", "same-origin")
	resumeRR := httptest.NewRecorder()
	h.handleFedPause(resumeRR, resumeReq)
	require.Equal(t, http.StatusOK, resumeRR.Code, resumeRR.Body.String())
	require.Equal(t, []bool{true, false}, driver.calls)
	require.False(t, driver.policy.Paused)
}
