package web

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	policy       *store.PeerRBACPolicy
	calls        []bool
	replaceCalls int
	publish      []string
	subscribe    []string
	syncErr      error
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
	d.replaceCalls++
	d.policy.Domains = append([]store.PeerRBACDomainPermission(nil), domains...)
	d.policy.Revision++
	return d.GetPeerRBACPolicy(context.Background(), d.policy.RemoteChainID)
}

func (d *pauseContractDriver) UpdateDirectionalSyncPolicy(_ context.Context, _ string, publish, subscribe *[]string) (*federation.DirectionalSyncPolicyResult, error) {
	if d.syncErr != nil {
		return nil, d.syncErr
	}
	if publish != nil {
		d.publish = append([]string(nil), (*publish)...)
	}
	if subscribe != nil {
		d.subscribe = append([]string(nil), (*subscribe)...)
	}
	return &federation.DirectionalSyncPolicyResult{
		Version: federation.SyncPolicyVersionPeerRBAC, Revision: 2,
		PublishDomains: d.publish, SubscribeDomains: d.subscribe, State: "pending",
	}, nil
}
func (d *pauseContractDriver) ReconcileDirectionalPublishToPeerRBAC(_ context.Context, _ string) (*federation.DirectionalSyncPolicyResult, error) {
	allowed := make(map[string]bool)
	for _, permission := range d.policy.Domains {
		if permission.Copy {
			allowed[permission.Domain] = true
		}
	}
	narrowed := make([]string, 0, len(d.publish))
	for _, domain := range d.publish {
		if allowed[domain] {
			narrowed = append(narrowed, domain)
		}
	}
	d.publish = narrowed
	return &federation.DirectionalSyncPolicyResult{
		Version: federation.SyncPolicyVersionPeerRBAC, Revision: 1,
		PublishDomains: d.publish, SubscribeDomains: d.subscribe, State: "pending",
	}, nil
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

type policyOnlyContractDriver struct {
	FederationJoinDriver
	policy       *store.PeerRBACPolicy
	replaceCalls int
}

func (d *policyOnlyContractDriver) ResolvePeerOperatorAgentID(context.Context, string) (string, error) {
	return d.policy.PeerAgentID, nil
}

func (d *policyOnlyContractDriver) GetPeerRBACPolicy(context.Context, string) (*store.PeerRBACPolicy, error) {
	clone := *d.policy
	clone.Domains = append([]store.PeerRBACDomainPermission(nil), d.policy.Domains...)
	return &clone, nil
}

func (d *policyOnlyContractDriver) ReplacePeerRBACPolicy(_ context.Context, _ string, domains []store.PeerRBACDomainPermission) (*store.PeerRBACPolicy, error) {
	d.replaceCalls++
	d.policy.Domains = append([]store.PeerRBACDomainPermission(nil), domains...)
	d.policy.Revision++
	return d.GetPeerRBACPolicy(context.Background(), d.policy.RemoteChainID)
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

func TestShareableDomainCatalogueKeepsCopiedFromAfterConnectionRemoval(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "copy-source.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })

	operatorID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, bs.RegisterAgent(operatorID, "operator", "admin", "", "test", "", 4))
	require.NoError(t, bs.RegisterDomain("sage-autoresearch-benchmark", operatorID, "", 1))
	require.NoError(t, ss.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID:  "chain-a",
		OriginMemoryID: "origin-1",
		LocalMemoryID:  "local-copy-1",
		DomainTag:      "sage-autoresearch-benchmark",
		Outcome:        "admitted",
	}))
	// No cross_fed agreement is installed: this exercises the post-revoke
	// catalogue, where durable copy provenance must outlive the connection.
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.NodeOperatorAgentID = operatorID

	domains, err := h.federationShareableDomains(ctx)
	require.NoError(t, err)
	require.Len(t, domains, 1)
	require.Equal(t, "sage-autoresearch-benchmark", domains[0].Domain)
	require.Equal(t, []federationDomainCopySource{{
		ChainID: "chain-a", MemoryCount: 1,
	}}, domains[0].CopySources)
}

func TestFederationPermissionSavePreflightsAndReportsCopyAlignment(t *testing.T) {
	setup := func(t *testing.T, driver FederationJoinDriver) (*DashboardHandler, *store.SQLiteStore, *store.BadgerStore) {
		t.Helper()
		ctx := context.Background()
		ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "permissions.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = ss.Close() })
		bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = bs.CloseBadger() })
		pin := bytes.Repeat([]byte{0x44}, 32)
		require.NoError(t, bs.SetCrossFed("chain-peer", "https://peer:8444", pin, 4, 0, nil, nil, "active"))
		owner := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		require.NoError(t, bs.RegisterDomain("tii.work", owner, "", 1))
		require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
			RemoteChainID: "chain-peer", Role: "host", ControllerChainID: "chain-local",
			ControllerAgentID: owner, PeerAgentID: "peer-agent", PolicyEpoch: "epoch-save",
			RemoteCAPin: "4444444444444444444444444444444444444444444444444444444444444444",
		}))
		require.NoError(t, ss.ActivateSyncControl(ctx, "chain-peer", "epoch-save"))
		_, err = ss.ApplyLocalDirectionalSyncPolicy(ctx, "chain-peer", "epoch-save",
			federation.SyncPolicyVersionPeerRBAC, 1, "local-save-1", nil, []string{"peer.offered"})
		require.NoError(t, err)
		h := NewDashboardHandler(ss, "test")
		h.BadgerStore = bs
		h.NodeOperatorAgentID = owner
		h.Federation = driver
		return h, ss, bs
	}
	request := func(h *DashboardHandler) *httptest.ResponseRecorder {
		body := bytes.NewBufferString(`{"permissions":[{"domain":"tii.work","copy":true}]}`)
		req := withFederationChain(httptest.NewRequest(http.MethodPut,
			"http://localhost/v1/dashboard/federation/connections/chain-peer/permissions", body), "chain-peer")
		markLocalCEREBRUM(req)
		rr := httptest.NewRecorder()
		h.handleFedPermissionsPut(rr, req)
		return rr
	}
	basePolicy := func() *store.PeerRBACPolicy {
		return &store.PeerRBACPolicy{
			RemoteChainID: "chain-peer", PeerAgentID: "peer-agent", PolicyEpoch: "epoch-save",
			RemoteCAPin:   "4444444444444444444444444444444444444444444444444444444444444444",
			PolicyVersion: federation.SyncPolicyVersionPeerRBAC,
		}
	}

	t.Run("missing copy alignment leaves RBAC unchanged", func(t *testing.T) {
		driver := &policyOnlyContractDriver{policy: basePolicy()}
		h, _, _ := setup(t, driver)
		rr := request(h)
		require.Equal(t, http.StatusNotImplemented, rr.Code, rr.Body.String())
		require.Zero(t, driver.replaceCalls)
		require.Contains(t, rr.Body.String(), "Permissions were not changed")
	})

	t.Run("successful alignment preserves subscribe choices", func(t *testing.T) {
		driver := &pauseContractDriver{policy: basePolicy(), subscribe: []string{"peer.offered"}}
		h, _, _ := setup(t, driver)
		rr := request(h)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		require.Equal(t, 1, driver.replaceCalls)
		require.Equal(t, []string{"tii.work"}, driver.publish)
		require.Equal(t, []string{"peer.offered"}, driver.subscribe)
	})

	t.Run("synthetic revision zero policy is persisted before alignment", func(t *testing.T) {
		driver := &pauseContractDriver{policy: basePolicy(), subscribe: []string{"peer.offered"}}
		h, _, _ := setup(t, driver)
		body := bytes.NewBufferString(`{"permissions":[]}`)
		req := withFederationChain(httptest.NewRequest(http.MethodPut,
			"http://localhost/v1/dashboard/federation/connections/chain-peer/permissions", body), "chain-peer")
		markLocalCEREBRUM(req)
		rr := httptest.NewRecorder()
		h.handleFedPermissionsPut(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		require.Equal(t, 1, driver.replaceCalls, "a synthesized deny-all policy is not a durable stored revision")
		require.Equal(t, int64(1), driver.policy.Revision)
		require.Empty(t, driver.publish)
		require.Equal(t, []string{"peer.offered"}, driver.subscribe)
	})

	t.Run("alignment failure is explicit and retryable after safe RBAC commit", func(t *testing.T) {
		driver := &pauseContractDriver{policy: basePolicy(), subscribe: []string{"peer.offered"}, syncErr: errors.New("binding changed")}
		h, _, _ := setup(t, driver)
		rr := request(h)
		require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
		require.Equal(t, 1, driver.replaceCalls)
		require.Equal(t, []store.PeerRBACDomainPermission{{Domain: "tii.work", Read: true, Copy: true}}, driver.policy.Domains)
		require.Contains(t, rr.Body.String(), "Permissions were saved")
		require.Contains(t, rr.Body.String(), "Retry Save what I share")

		getReq := withFederationChain(httptest.NewRequest(http.MethodGet,
			"http://localhost/v1/dashboard/federation/connections/chain-peer/permissions", nil), "chain-peer")
		markLocalCEREBRUM(getReq)
		getRR := httptest.NewRecorder()
		h.handleFedPermissionsGet(getRR, getReq)
		require.Equal(t, http.StatusOK, getRR.Code, getRR.Body.String())
		var getBody struct {
			CopyAlignmentPending bool `json:"copy_alignment_pending"`
		}
		require.NoError(t, json.NewDecoder(getRR.Body).Decode(&getBody))
		require.True(t, getBody.CopyAlignmentPending, "reload must keep the failed alignment retry actionable")

		driver.syncErr = nil
		retryRR := request(h)
		require.Equal(t, http.StatusOK, retryRR.Code, retryRR.Body.String())
		require.Equal(t, 1, driver.replaceCalls, "an unchanged retry must preserve the policy/contact revision")
		var retryBody struct {
			PolicyReplaced bool `json:"policy_replaced"`
		}
		require.NoError(t, json.NewDecoder(retryRR.Body).Decode(&retryBody))
		require.False(t, retryBody.PolicyReplaced)
	})
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
