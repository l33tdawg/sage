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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestSyncPolicyHandlerRequiresFrozenHostAndAppliesSnapshot(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	pin := []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	hostAgentID := newPeerOperatorID(t)
	otherAgentID := newPeerOperatorID(t)
	require.NoError(t, bs.SetCrossFed("chain-host", "https://peer:8444", pin, 4, 0, []string{"shared"}, nil, "active"))
	agreement, err := m.ActiveAgreement("chain-host")
	require.NoError(t, err)
	control := store.SyncControl{
		RemoteChainID: "chain-host", Role: "guest", ControllerChainID: "chain-host",
		ControllerAgentID: hostAgentID, PeerAgentID: hostAgentID, PolicyEpoch: "epoch", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
		PolicyVersion: SyncPolicyVersionLegacy,
	}
	require.NoError(t, ss.PrepareSyncControl(context.Background(), control))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-host", "epoch"))

	call := func(agent string, revision int64, domains []string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(SyncPolicyRequest{Version: 1, Epoch: "epoch", Revision: revision, Domains: domains})
		req := httptest.NewRequest(http.MethodPut, "/fed/v1/sync/policy", bytes.NewReader(body))
		ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
			ChainID: "chain-host", AgentID: agent, Agreement: agreement, CeremonyCaptured: true,
			Ceremony: &peerCeremonyBinding{PeerAgentID: control.PeerAgentID, PolicyEpoch: control.PolicyEpoch,
				RemoteCAPin: control.RemoteCAPin, State: "active"},
		})
		rr := httptest.NewRecorder()
		m.handleSyncPolicy(rr, req.WithContext(ctx))
		return rr
	}

	assert.Equal(t, http.StatusForbidden, call(otherAgentID, 1, []string{"shared"}).Code)
	assert.Equal(t, http.StatusBadRequest, call(hostAgentID, 1, []string{"private"}).Code)
	assert.Equal(t, http.StatusOK, call(hostAgentID, 1, []string{"shared.alpha"}).Code)
	domains, err := ss.GetSyncDomains(context.Background(), "chain-host")
	require.NoError(t, err)
	assert.Equal(t, []string{"shared.alpha"}, domains)
	assert.Equal(t, http.StatusOK, call(hostAgentID, 1, []string{"shared.alpha"}).Code, "same revision+hash is idempotent")
	assert.Equal(t, http.StatusConflict, call(hostAgentID, 1, []string{"shared.beta"}).Code, "equal revision equivocation is refused")
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
	m, ss, bs := newDrainTestManager(t)
	pin := []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	guestAgentID := newPeerOperatorID(t)
	otherAgentID := newPeerOperatorID(t)
	require.NoError(t, bs.SetCrossFed("chain-guest", "https://peer:8444", pin, 4, 0, nil, nil, "active"))
	agreement, err := m.ActiveAgreement("chain-guest")
	require.NoError(t, err)
	control := store.SyncControl{
		RemoteChainID: "chain-guest", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: guestAgentID,
		PolicyEpoch: "epoch-v3", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}
	require.NoError(t, ss.PrepareSyncControl(context.Background(), control))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-guest", "epoch-v3"))

	call := func(agent string, version int, revision int64) *httptest.ResponseRecorder {
		body, _ := json.Marshal(SyncPolicyRequest{Version: version,
			Epoch: "epoch-v3", Revision: revision})
		req := httptest.NewRequest(http.MethodPut, "/fed/v1/sync/policy", bytes.NewReader(body))
		ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
			ChainID: "chain-guest", AgentID: agent, Agreement: agreement, CeremonyCaptured: true,
			Ceremony: &peerCeremonyBinding{PeerAgentID: control.PeerAgentID, PolicyEpoch: control.PolicyEpoch,
				RemoteCAPin: control.RemoteCAPin, State: "active"},
		})
		rr := httptest.NewRecorder()
		m.handleSyncPolicy(rr, req.WithContext(ctx))
		return rr
	}

	assert.Equal(t, http.StatusForbidden, call(otherAgentID, SyncPolicyVersionPeerRBAC, 1).Code)
	assert.Equal(t, http.StatusConflict, call(guestAgentID, SyncPolicyVersionLegacy, 1).Code)
	assert.Equal(t, http.StatusConflict, call(guestAgentID, SyncPolicyVersionDirectional, 1).Code)
	assert.Equal(t, http.StatusOK, call(guestAgentID, SyncPolicyVersionPeerRBAC, 1).Code)
}

func TestStaleAuthenticatedPolicyRequestCannotMutateFreshCeremonyEpoch(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	peerID := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, m, ss, bs, "chain-peer", peerID, "host", nil, 4)
	e1, err := ss.GetSyncControl(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, e1)
	stalePeer := &peerIdentity{
		ChainID: "chain-peer", AgentID: peerID, Agreement: agreement, CeremonyCaptured: true,
		Ceremony: &peerCeremonyBinding{PeerAgentID: e1.PeerAgentID, PolicyEpoch: e1.PolicyEpoch,
			RemoteCAPin: e1.RemoteCAPin, State: e1.BindingState},
	}

	e2 := *e1
	e2.PolicyEpoch = "epoch-chain-peer-e2"
	require.NoError(t, ss.PurgeSyncPeerState(context.Background(), e1.RemoteChainID))
	require.NoError(t, ss.PrepareSyncControl(context.Background(), e2))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), e2.RemoteChainID, e2.PolicyEpoch))
	body, err := json.Marshal(SyncPolicyRequest{Version: SyncPolicyVersionPeerRBAC,
		Epoch: e2.PolicyEpoch, Revision: 1, PublishDomains: []string{"fresh.secret"}})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPut, "/fed/v1/sync/policy", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, stalePeer))
	rr := httptest.NewRecorder()
	m.handleSyncPolicy(rr, req)
	assert.Equal(t, http.StatusForbidden, rr.Code)
	current, err := ss.GetSyncControl(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, e2.PolicyEpoch, current.PolicyEpoch)
	assert.Zero(t, current.RemoteRevision, "E1-authenticated body must not advance E2's revision lane")
	domains, err := ss.GetDirectionalSyncDomains(context.Background(), "chain-peer", store.SyncDirectionRemotePublish)
	require.NoError(t, err)
	assert.Empty(t, domains)
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
	var pushedPolicies [][]string
	m.syncPolicyPushFn = func(_ context.Context, _ string, req *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		pushedPolicies = append(pushedPolicies, append([]string(nil), req.PublishDomains...))
		return &SyncPolicyResponse{Status: "applied", Revision: req.Revision}, nil
	}

	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true, Copy: true},
	})
	require.NoError(t, err)
	result, err := m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"tii.project"}, []string{"peer.notes"})
	require.NoError(t, err, "configured PeerRBAC Copy must replace legacy tx-33 scope")
	assert.Equal(t, []string{"tii.project"}, result.PublishDomains)
	require.Equal(t, [][]string{{"tii.project"}}, pushedPolicies)

	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", true)
	require.NoError(t, err)
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{
		{Domain: "tii.next", Read: true, Copy: true},
	})
	require.NoError(t, err)
	result, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"tii.next"}, []string{"peer.notes"})
	require.NoError(t, err, "the latest dormant Copy configuration must remain editable while paused")
	assert.Equal(t, []string{"tii.next"}, result.PublishDomains)
	assert.Equal(t, "pending", result.State)
	assert.Equal(t, [][]string{{"tii.project"}}, pushedPolicies,
		"paused edits must not disclose even the new domain labels")
	control, err := ss.GetSyncControl(ctx, "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, control)
	assert.Greater(t, control.Revision, control.DeliveredRevision)
	_, err = ss.ApplyRemoteDirectionalSyncPolicy(ctx, "chain-peer", "epoch-rbac",
		SyncPolicyVersionPeerRBAC, 1, "remote-paused", nil, []string{"tii.next"})
	require.NoError(t, err)
	effective, _, err := m.pairwiseEgressPolicy(ctx, ss, agreement)
	require.NoError(t, err)
	assert.Empty(t, effective, "editing while paused must not send data")
	_, err = m.SetPeerRBACPaused(ctx, "chain-peer", false)
	require.NoError(t, err)
	m.syncTick(ctx, ss)
	assert.Equal(t, [][]string{{"tii.project"}, {"tii.next"}}, pushedPolicies,
		"resume must deliver only the latest withheld snapshot")
	effective, _, err = m.pairwiseEgressPolicy(ctx, ss, agreement)
	require.NoError(t, err)
	assert.Equal(t, []string{"tii.next"}, effective, "resume must activate the latest saved Copy choice")

	_, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"legacy"}, []string{"peer.notes"})
	require.ErrorContains(t, err, "not granted", "present PeerRBAC policy must not fall back to legacy scope")

	require.NoError(t, ss.DeletePeerRBACPolicy(ctx, "chain-peer"))
	_, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", []string{"legacy"}, nil)
	require.ErrorContains(t, err, "not granted", "v3 must never promote legacy tx-33 scope into Copy")
	_, err = m.SetDirectionalSyncPolicy(ctx, "chain-peer", nil, []string{"legacy"})
	require.NoError(t, err, "a recipient may subscribe without granting the peer Copy in the other direction")
}

func preparePendingPeerRBACPolicy(t *testing.T, m *Manager, ss *store.SQLiteStore, bs *store.BadgerStore, chainID string) {
	t.Helper()
	peerID := newPeerOperatorID(t)
	configurePeerRBACConnection(t, m, ss, bs, chainID, peerID, "host", nil, 4)
	_, err := m.ReplacePeerRBACPolicy(context.Background(), chainID, []store.PeerRBACDomainPermission{
		{Domain: "shared", Read: true, Copy: true},
	})
	require.NoError(t, err)
	control, err := ss.GetSyncControl(context.Background(), chainID)
	require.NoError(t, err)
	require.NotNil(t, control)
	revision := control.Revision + 1
	publish := []string{"shared.docs"}
	hash := directionalSyncPolicyHash(control.PolicyEpoch, m.localChainID, revision, publish, nil)
	_, err = ss.ApplyLocalDirectionalSyncPolicy(context.Background(), chainID, control.PolicyEpoch,
		SyncPolicyVersionPeerRBAC, revision, hash, publish, nil)
	require.NoError(t, err)
}

func TestPauseBeforeFinalPolicyDeliveryGateWithholdsDomainLabels(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")

	gateEntered := make(chan struct{})
	releaseGate := make(chan struct{})
	m.syncPolicyFinalGateHook = func(chainID string) {
		if chainID == "chain-peer" {
			close(gateEntered)
			<-releaseGate
		}
	}
	pushCalled := false
	m.syncPolicyPushFn = func(context.Context, string, *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		pushCalled = true
		return &SyncPolicyResponse{Status: "applied"}, nil
	}

	deliveryDone := make(chan error, 1)
	go func() { deliveryDone <- m.deliverSyncPolicy(context.Background(), ss, "chain-peer") }()
	select {
	case <-gateEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("policy delivery did not reach its final pause gate")
	}
	paused, err := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
	require.NoError(t, err)
	require.True(t, paused.Paused)
	close(releaseGate)
	select {
	case err := <-deliveryDone:
		require.ErrorContains(t, err, "pause")
	case <-time.After(2 * time.Second):
		t.Fatal("withheld delivery did not return")
	}
	assert.False(t, pushCalled, "a Pause committed before the final gate must withhold all domain labels")
}

func TestBlockedPolicyDeliveryDoesNotBlockPausingAnotherPeer(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-a")
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-b")

	pushEntered := make(chan struct{})
	m.syncPolicyPushFn = func(ctx context.Context, chainID string, _ *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		if chainID == "chain-a" {
			close(pushEntered)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return &SyncPolicyResponse{Status: "applied"}, nil
	}
	deliveryCtx, cancelDelivery := context.WithCancel(context.Background())
	deliveryDone := make(chan error, 1)
	go func() { deliveryDone <- m.deliverSyncPolicy(deliveryCtx, ss, "chain-a") }()
	select {
	case <-pushEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("chain-a delivery did not start")
	}

	pauseDone := make(chan error, 1)
	go func() {
		_, err := m.SetPeerRBACPaused(context.Background(), "chain-b", true)
		pauseDone <- err
	}()
	select {
	case err := <-pauseDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("offline chain-a blocked the independent chain-b Pause control")
	}
	cancelDelivery()
	select {
	case <-deliveryDone:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled chain-a delivery did not return")
	}
}

func TestPauseCancelsInFlightSamePeerPolicyDelivery(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")

	pushEntered := make(chan struct{})
	m.syncPolicyPushFn = func(ctx context.Context, _ string, _ *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		close(pushEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	deliveryDone := make(chan error, 1)
	go func() { deliveryDone <- m.deliverSyncPolicy(context.Background(), ss, "chain-peer") }()
	select {
	case <-pushEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("same-peer policy delivery did not start")
	}

	pauseDone := make(chan error, 1)
	go func() {
		_, err := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
		pauseDone <- err
	}()
	select {
	case err := <-pauseDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Pause did not cancel and linearize behind the in-flight same-peer delivery")
	}
	select {
	case err := <-deliveryDone:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("canceled same-peer delivery did not return")
	}
	policy, err := m.GetPeerRBACPolicy(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, policy)
	assert.True(t, policy.Paused)
}

func TestResumeAndFailedGenerationMutationCannotLeaveStalePauseLatch(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")
	_, err := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
	require.NoError(t, err)

	resumeOwnsDelivery := make(chan struct{})
	releaseResume := make(chan struct{})
	m.syncPolicyPauseLeaseHook = func(chainID string) {
		if chainID != "chain-peer" {
			return
		}
		close(resumeOwnsDelivery)
		<-releaseResume
	}
	resumeDone := make(chan error, 1)
	go func() {
		_, resumeErr := m.SetPeerRBACPaused(context.Background(), "chain-peer", false)
		resumeDone <- resumeErr
	}()
	select {
	case <-resumeOwnsDelivery:
	case <-time.After(2 * time.Second):
		t.Fatal("Resume did not acquire the per-peer delivery lease")
	}

	generationReady := make(chan *SyncPolicyGenerationMutation, 1)
	go func() {
		generationReady <- m.BeginSyncPolicyGenerationMutation("chain-peer")
	}()
	lease := m.syncPolicyDeliveryLease("chain-peer")
	require.Eventually(t, func() bool {
		lease.stateMu.Lock()
		defer lease.stateMu.Unlock()
		return lease.generationBlocks == 1
	}, 2*time.Second, time.Millisecond, "generation block was not published before waiting on Resume")

	close(releaseResume)
	select {
	case resumeErr := <-resumeDone:
		require.NoError(t, resumeErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Resume did not finish after its test barrier was released")
	}
	var generation *SyncPolicyGenerationMutation
	select {
	case generation = <-generationReady:
	case <-time.After(2 * time.Second):
		t.Fatal("generation mutation did not acquire after Resume released the delivery lease")
	}
	generation.Restore()

	policy, err := m.GetPeerRBACPolicy(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, policy)
	assert.False(t, policy.Paused, "Resume must remain the persisted operator intent")
	operatorPaused, generationBlocked := lease.deliveryBlocked()
	assert.False(t, operatorPaused, "a failed generation mutation must not restore a stale Pause latch")
	assert.False(t, generationBlocked, "Restore must release its independent generation block")
}

func TestFreshGenerationActivateCannotEraseConcurrentPauseIntent(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")
	lease := m.syncPolicyDeliveryLease("chain-peer")
	generation := m.BeginSyncPolicyGenerationMutation("chain-peer")

	pauseDone := make(chan error, 1)
	go func() {
		_, pauseErr := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
		pauseDone <- pauseErr
	}()
	require.Eventually(t, func() bool {
		lease.stateMu.Lock()
		defer lease.stateMu.Unlock()
		return lease.pauseRequested && lease.pauseMutations == 1 && lease.pauseRevision > generation.pauseRevision
	}, 2*time.Second, time.Millisecond, "Pause intent was not published while the fresh generation held delivery")

	generation.Activate()
	operatorPaused, generationBlocked := lease.deliveryBlocked()
	assert.True(t, operatorPaused, "fresh-generation activation must preserve a concurrent operator Pause")
	assert.False(t, generationBlocked, "Activate must release only the generation block")

	select {
	case pauseErr := <-pauseDone:
		require.NoError(t, pauseErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Pause did not finish after generation activation released the delivery lease")
	}
	policy, err := m.GetPeerRBACPolicy(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, policy)
	assert.True(t, policy.Paused)
	operatorPaused, generationBlocked = lease.deliveryBlocked()
	assert.True(t, operatorPaused)
	assert.False(t, generationBlocked)
}

func TestFreshGenerationClearsPauseThatLinearizedBeforeItAcquiredDelivery(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")
	lease := m.syncPolicyDeliveryLease("chain-peer")

	pauseOwnsDelivery := make(chan struct{})
	releasePause := make(chan struct{})
	m.syncPolicyPauseLeaseHook = func(chainID string) {
		if chainID == "chain-peer" {
			close(pauseOwnsDelivery)
			<-releasePause
		}
	}
	pauseDone := make(chan error, 1)
	go func() {
		_, pauseErr := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
		pauseDone <- pauseErr
	}()
	select {
	case <-pauseOwnsDelivery:
	case <-time.After(2 * time.Second):
		t.Fatal("Pause did not acquire the retiring generation's delivery lease")
	}

	generationReady := make(chan *SyncPolicyGenerationMutation, 1)
	go func() { generationReady <- m.BeginSyncPolicyGenerationMutation("chain-peer") }()
	require.Eventually(t, func() bool {
		lease.stateMu.Lock()
		defer lease.stateMu.Unlock()
		return lease.generationBlocks == 1
	}, 2*time.Second, time.Millisecond, "generation did not publish its block while waiting for Pause")
	select {
	case generation := <-generationReady:
		generation.Restore()
		t.Fatal("generation acquired delivery before the older Pause completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releasePause)
	select {
	case pauseErr := <-pauseDone:
		require.NoError(t, pauseErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Pause did not finish")
	}
	var generation *SyncPolicyGenerationMutation
	select {
	case generation = <-generationReady:
	case <-time.After(2 * time.Second):
		t.Fatal("generation did not acquire after Pause released delivery")
	}
	generation.Activate()

	operatorPaused, generationBlocked := lease.deliveryBlocked()
	assert.False(t, operatorPaused, "fresh generation must not inherit Pause committed on the retiring generation")
	assert.False(t, generationBlocked)
}

func TestEarlierResumeCannotEraseLaterQueuedPauseIntent(t *testing.T) {
	m, _, bs := newDrainTestManager(t)
	ss := m.syncStore()
	require.NotNil(t, ss)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")
	_, err := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
	require.NoError(t, err)
	lease := m.syncPolicyDeliveryLease("chain-peer")

	resumeOwnsDelivery := make(chan struct{})
	releaseResume := make(chan struct{})
	pauseOwnsDelivery := make(chan struct{})
	releasePause := make(chan struct{})
	var hookCalls atomic.Int32
	m.syncPolicyPauseLeaseHook = func(chainID string) {
		if chainID != "chain-peer" {
			return
		}
		switch hookCalls.Add(1) {
		case 1:
			close(resumeOwnsDelivery)
			<-releaseResume
		case 2:
			close(pauseOwnsDelivery)
			<-releasePause
		}
	}
	resumeDone := make(chan error, 1)
	go func() {
		_, resumeErr := m.SetPeerRBACPaused(context.Background(), "chain-peer", false)
		resumeDone <- resumeErr
	}()
	select {
	case <-resumeOwnsDelivery:
	case <-time.After(2 * time.Second):
		t.Fatal("Resume did not acquire the delivery lease")
	}

	pauseDone := make(chan error, 1)
	go func() {
		_, pauseErr := m.SetPeerRBACPaused(context.Background(), "chain-peer", true)
		pauseDone <- pauseErr
	}()
	require.Eventually(t, func() bool {
		lease.stateMu.Lock()
		defer lease.stateMu.Unlock()
		return lease.pauseRequested && lease.pendingPauses == 1 && lease.pauseMutations == 2
	}, 2*time.Second, time.Millisecond, "the later Pause did not publish while Resume owned delivery")

	close(releaseResume)
	select {
	case resumeErr := <-resumeDone:
		require.NoError(t, resumeErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Resume did not finish")
	}
	select {
	case <-pauseOwnsDelivery:
	case <-time.After(2 * time.Second):
		t.Fatal("queued Pause did not acquire after Resume")
	}
	operatorPaused, generationBlocked := lease.deliveryBlocked()
	assert.True(t, operatorPaused, "Resume completion must not clear a later published Pause intent")
	assert.False(t, generationBlocked)

	close(releasePause)
	select {
	case pauseErr := <-pauseDone:
		require.NoError(t, pauseErr)
	case <-time.After(2 * time.Second):
		t.Fatal("queued Pause did not finish")
	}
	policy, err := m.GetPeerRBACPolicy(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, policy)
	assert.True(t, policy.Paused)
}

func TestFreshGenerationCannotInheritBuiltStalePolicyPayload(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	preparePendingPeerRBACPolicy(t, m, ss, bs, "chain-peer")
	e1, err := ss.GetSyncControl(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, e1)

	built := make(chan struct{})
	releaseBuilt := make(chan struct{})
	m.syncPolicyBeforePushHook = func(chainID string) {
		if chainID == "chain-peer" {
			close(built)
			<-releaseBuilt
		}
	}
	pushCalled := false
	m.syncPolicyPushFn = func(context.Context, string, *SyncPolicyRequest) (*SyncPolicyResponse, error) {
		pushCalled = true
		return &SyncPolicyResponse{Status: "applied"}, nil
	}
	deliveryDone := make(chan error, 1)
	go func() { deliveryDone <- m.deliverSyncPolicy(context.Background(), ss, "chain-peer") }()
	select {
	case <-built:
	case <-time.After(2 * time.Second):
		t.Fatal("E1 policy payload was not built")
	}

	type mutationLease struct {
		generation *SyncPolicyGenerationMutation
		unlock     func()
	}
	mutationReady := make(chan mutationLease, 1)
	go func() {
		unlock := m.LockAgreementMutation()
		generation := m.BeginSyncPolicyGenerationMutation("chain-peer")
		mutationReady <- mutationLease{generation: generation, unlock: unlock}
	}()
	select {
	case lease := <-mutationReady:
		lease.generation.Restore()
		lease.unlock()
		t.Fatal("generation mutation bypassed the in-flight E1 delivery lease")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseBuilt)
	var mutation mutationLease
	select {
	case mutation = <-mutationReady:
	case <-time.After(2 * time.Second):
		t.Fatal("generation mutation did not acquire after E1 delivery was canceled")
	}
	defer mutation.unlock()
	defer mutation.generation.Restore()
	select {
	case deliveryErr := <-deliveryDone:
		require.ErrorContains(t, deliveryErr, "generation")
	case <-time.After(2 * time.Second):
		t.Fatal("stale E1 delivery did not terminate")
	}
	assert.False(t, pushCalled, "a payload built under E1 must not be sent after E2 mutation begins")

	require.NoError(t, ss.PurgeSyncPeerState(context.Background(), "chain-peer"))
	e2 := *e1
	e2.PolicyEpoch = "epoch-chain-peer-e2"
	require.NoError(t, ss.PrepareSyncControl(context.Background(), e2))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), e2.RemoteChainID, e2.PolicyEpoch))
	mutation.generation.Activate()
	current, err := ss.GetSyncControl(context.Background(), "chain-peer")
	require.NoError(t, err)
	require.NotNil(t, current)
	assert.Equal(t, e2.PolicyEpoch, current.PolicyEpoch)
}
