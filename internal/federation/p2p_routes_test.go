package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func testRouteBundle(t *testing.T, ip string) JoinP2PBundle {
	t.Helper()
	relayKey, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	relayID, err := peer.IDFromPrivateKey(relayKey)
	require.NoError(t, err)
	peerKey, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	peerID, err := peer.IDFromPrivateKey(peerKey)
	require.NoError(t, err)
	return JoinP2PBundle{PeerID: peerID.String(), Protocol: "/sage/fed/1.0.0", Addrs: []string{
		"/ip4/" + ip + "/tcp/4001/p2p/" + relayID.String() + "/p2p-circuit/p2p/" + peerID.String(),
	}}
}

func bindP2PRouteControl(t *testing.T, m *Manager, remoteChainID, peerAgentID, epoch string) *store.CrossFedRecord {
	t.Helper()
	ss := m.syncStore()
	require.NotNil(t, ss)
	agreement, err := m.ActiveAgreement(remoteChainID)
	require.NoError(t, err)
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: remoteChainID, Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: peerAgentID,
		PolicyEpoch: epoch, RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
		PolicyVersion: SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), remoteChainID, epoch))
	return agreement
}

func TestAuthenticatedRouteExchangePersistsRemoteAndReturnsLocal(t *testing.T) {
	m, _, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "remote-chain", 4)
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerAgentID := hex.EncodeToString(peerPub)
	agreement := bindP2PRouteControl(t, m, "remote-chain", peerAgentID, "epoch-routes")
	remote := testRouteBundle(t, "203.0.113.10")
	local := testRouteBundle(t, "203.0.113.20")
	var persistedChain string
	var persisted []string
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return local, nil },
		Persist: func(chain string, addrs []string) error {
			persistedChain, persisted = chain, append([]string(nil), addrs...)
			return nil
		},
	})
	body, _ := json.Marshal(remote)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/p2p/routes", bytes.NewReader(body))
	ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "remote-chain", AgentID: peerAgentID, Agreement: agreement,
	})
	rr := httptest.NewRecorder()
	m.handleP2PRoutes(rr, req.WithContext(ctx))
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "remote-chain", persistedChain)
	assert.Equal(t, remote.Addrs, persisted)
	var got JoinP2PBundle
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &got))
	assert.Equal(t, local.PeerID, got.PeerID)
	assert.Equal(t, local.Protocol, got.Protocol)
	assert.Equal(t, local.Addrs, got.Addrs)
	assert.NotZero(t, got.Revision)
	assert.NotZero(t, got.IssuedAt)
	assert.Greater(t, got.ExpiresAt, got.IssuedAt)
}

func TestRouteExchangeRejectsAuthenticatedChainWithWrongOperator(t *testing.T) {
	m, _, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "remote-chain", 4)
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerAgentID := hex.EncodeToString(peerPub)
	agreement := bindP2PRouteControl(t, m, "remote-chain", peerAgentID, "epoch-routes")
	called := false
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return testRouteBundle(t, "203.0.113.20"), nil },
		Persist:     func(string, []string) error { called = true; return nil },
	})
	body, _ := json.Marshal(testRouteBundle(t, "203.0.113.10"))
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/p2p/routes", bytes.NewReader(body))
	ctx := context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "remote-chain", AgentID: strings.Repeat("00", 32), Agreement: agreement,
	})
	rr := httptest.NewRecorder()
	m.handleP2PRoutes(rr, req.WithContext(ctx))
	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.False(t, called)
}

func TestStaleAuthenticatedRouteRequestCannotPersistIntoFreshCeremonyEpoch(t *testing.T) {
	m, ss, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "remote-chain", 4)
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerAgentID := hex.EncodeToString(peerPub)
	agreement := bindP2PRouteControl(t, m, "remote-chain", peerAgentID, "epoch-routes-e1")
	e1, err := ss.GetSyncControl(context.Background(), "remote-chain")
	require.NoError(t, err)
	require.NotNil(t, e1)
	stalePeer := &peerIdentity{
		ChainID: "remote-chain", AgentID: peerAgentID, Agreement: agreement, CeremonyCaptured: true,
		Ceremony: &peerCeremonyBinding{PeerAgentID: e1.PeerAgentID, PolicyEpoch: e1.PolicyEpoch,
			RemoteCAPin: e1.RemoteCAPin, State: e1.BindingState},
	}
	e2 := *e1
	e2.PolicyEpoch = "epoch-routes-e2"
	require.NoError(t, ss.PurgeSyncPeerState(context.Background(), e1.RemoteChainID))
	require.NoError(t, ss.PrepareSyncControl(context.Background(), e2))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), e2.RemoteChainID, e2.PolicyEpoch))

	persisted := false
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return testRouteBundle(t, "203.0.113.20"), nil },
		Persist:     func(string, []string) error { persisted = true; return nil },
	})
	body, err := json.Marshal(testRouteBundle(t, "203.0.113.10"))
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/p2p/routes", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, stalePeer))
	rr := httptest.NewRecorder()
	m.handleP2PRoutes(rr, req)
	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.False(t, persisted, "E1-authenticated route bundle must not persist into E2")
}

func TestRouteExchangeRejectsNonActiveOrCorruptFrozenBinding(t *testing.T) {
	tests := []struct {
		name     string
		activate bool
		mutate   func(*store.SyncControl)
	}{
		{name: "pending", activate: false},
		{name: "wrong controller", activate: true, mutate: func(c *store.SyncControl) {
			c.ControllerChainID = "different-local-chain"
		}},
		{name: "wrong CA pin", activate: true, mutate: func(c *store.SyncControl) {
			c.RemoteCAPin = strings.Repeat("00", 32)
		}},
		{name: "missing frozen peer", activate: true, mutate: func(c *store.SyncControl) {
			c.PeerAgentID = ""
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, ss, bs := newDrainTestManager(t)
			seedDrainAgreement(t, bs, "remote-chain", 4)
			agreement, err := m.ActiveAgreement("remote-chain")
			require.NoError(t, err)
			peerPub, _, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			peerAgentID := hex.EncodeToString(peerPub)
			control := store.SyncControl{
				RemoteChainID: "remote-chain", Role: "host", ControllerChainID: m.localChainID,
				ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: peerAgentID,
				PolicyEpoch: "epoch-corrupt-binding", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
				PolicyVersion: SyncPolicyVersionPeerRBAC,
			}
			if tc.mutate != nil {
				tc.mutate(&control)
			}
			require.NoError(t, ss.PrepareSyncControl(context.Background(), control))
			if tc.activate {
				require.NoError(t, ss.ActivateSyncControl(context.Background(), "remote-chain", control.PolicyEpoch))
			}

			called := false
			m.SetJoinP2PHooks(JoinP2PHooks{
				LocalBundle: func() (JoinP2PBundle, error) { return testRouteBundle(t, "203.0.113.20"), nil },
				Persist:     func(string, []string) error { called = true; return nil },
			})
			body, err := json.Marshal(testRouteBundle(t, "203.0.113.10"))
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, "/fed/v1/p2p/routes", bytes.NewReader(body))
			req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
				ChainID: "remote-chain", AgentID: peerAgentID, Agreement: agreement,
			}))
			rr := httptest.NewRecorder()
			m.handleP2PRoutes(rr, req)
			assert.Equal(t, http.StatusForbidden, rr.Code)
			assert.False(t, called)
		})
	}
}

func TestInboundRoutePersistenceLinearizesWithCompletedRevoke(t *testing.T) {
	m, _, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "remote-chain", 4)
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerAgentID := hex.EncodeToString(peerPub)
	agreement := bindP2PRouteControl(t, m, "remote-chain", peerAgentID, "epoch-inbound-revoke")

	remote := testRouteBundle(t, "203.0.113.30")
	local := testRouteBundle(t, "203.0.113.31")
	persistEntered := make(chan struct{})
	releasePersist := make(chan struct{})
	removeCalled := make(chan struct{})
	var removeOnce sync.Once
	var routeMu sync.Mutex
	routePresent := false
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return local, nil },
		Persist: func(string, []string) error {
			close(persistEntered)
			<-releasePersist
			routeMu.Lock()
			routePresent = true
			routeMu.Unlock()
			return nil
		},
		Remove: func(string) error {
			routeMu.Lock()
			routePresent = false
			routeMu.Unlock()
			removeOnce.Do(func() { close(removeCalled) })
			return nil
		},
	})
	body, err := json.Marshal(remote)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/p2p/routes", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{
		ChainID: "remote-chain", AgentID: peerAgentID, Agreement: agreement,
	}))
	rr := httptest.NewRecorder()
	handlerDone := make(chan struct{})
	go func() {
		m.handleP2PRoutes(rr, req)
		close(handlerDone)
	}()
	select {
	case <-persistEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("route persistence was not reached")
	}

	// Revoke consensus first, as the real disconnect path does, then purge the
	// local capability state. Purge must wait for the in-flight persistence
	// lease; otherwise Remove can complete before the delayed Persist re-adds it.
	require.NoError(t, bs.UpdateCrossFedStatus("remote-chain", "revoked"))
	purgeStarted := make(chan struct{})
	purgeDone := make(chan struct{})
	go func() {
		close(purgeStarted)
		m.PurgeLocalFederationState("remote-chain")
		close(purgeDone)
	}()
	<-purgeStarted
	select {
	case <-removeCalled:
		close(releasePersist)
		<-handlerDone
		t.Fatal("revoke removed routes before the in-flight persistence linearized")
	case <-time.After(250 * time.Millisecond):
	}
	close(releasePersist)
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("route handler did not finish")
	}
	select {
	case <-purgeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("revoke purge did not finish after persistence released")
	}
	assert.Equal(t, http.StatusOK, rr.Code)
	routeMu.Lock()
	assert.False(t, routePresent, "completed revoke must leave no route installed")
	routeMu.Unlock()
}

func TestOutboundRouteResponseAfterCompletedRevokeCannotReAddRoute(t *testing.T) {
	a := newTestChain(t, "route-client")
	b := newTestChain(t, "route-server")
	server := startListener(t, b)
	federate(t, a, b, server.URL, nil, 4, 0)
	federate(t, b, a, "https://unused.invalid", nil, 4, 0)
	bindP2PRouteControl(t, a.mgr, b.chainID, hex.EncodeToString(b.agentPub), "epoch-client-server")
	bindP2PRouteControl(t, b.mgr, a.chainID, hex.EncodeToString(a.agentPub), "epoch-server-client")

	clientBundle := testRouteBundle(t, "203.0.113.40")
	serverBundle := testRouteBundle(t, "203.0.113.41")
	responseBlocked := make(chan struct{})
	releaseResponse := make(chan struct{})
	b.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return serverBundle, nil },
		Persist: func(string, []string) error {
			close(responseBlocked)
			<-releaseResponse
			return nil
		},
	})
	var routeMu sync.Mutex
	clientRoutePresent := false
	clientPersistCalls := 0
	a.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return clientBundle, nil },
		Persist: func(string, []string) error {
			routeMu.Lock()
			clientPersistCalls++
			clientRoutePresent = true
			routeMu.Unlock()
			return nil
		},
		Remove: func(string) error {
			routeMu.Lock()
			clientRoutePresent = false
			routeMu.Unlock()
			return nil
		},
	})

	exchangeDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { exchangeDone <- a.mgr.ExchangeP2PRoutes(ctx, b.chainID, clientBundle) }()
	select {
	case <-responseBlocked:
	case <-ctx.Done():
		t.Fatal("peer did not reach its delayed route response")
	}

	// The outbound side must not hold the policy lock during the network wait.
	// Complete the revoke before releasing the peer's valid-but-stale response.
	require.NoError(t, a.badger.UpdateCrossFedStatus(b.chainID, "revoked"))
	purgeDone := make(chan struct{})
	go func() {
		a.mgr.PurgeLocalFederationState(b.chainID)
		close(purgeDone)
	}()
	select {
	case <-purgeDone:
	case <-time.After(2 * time.Second):
		close(releaseResponse)
		t.Fatal("revoke blocked behind the outbound network request")
	}
	close(releaseResponse)
	select {
	case exchangeErr := <-exchangeDone:
		require.Error(t, exchangeErr)
		assert.Contains(t, exchangeErr.Error(), "p2p route binding is no longer active")
	case <-ctx.Done():
		t.Fatal("route exchange did not finish after response was released")
	}
	routeMu.Lock()
	assert.Zero(t, clientPersistCalls, "a response from the revoked generation must not reach Persist")
	assert.False(t, clientRoutePresent, "completed revoke must leave no route installed")
	routeMu.Unlock()
}
