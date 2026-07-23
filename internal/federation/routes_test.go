package federation

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

type closeTrackingConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeTrackingConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestRaceRouteDialsBlackholedDirectFallsBackToRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var blackholeCanceled atomic.Bool
	relayClient, relayServer := net.Pipe()
	t.Cleanup(func() {
		_ = relayClient.Close()
		_ = relayServer.Close()
	})
	start := time.Now()
	winner, err := raceRouteDials(ctx, []routeDialAttempt{
		{dial: func(ctx context.Context) (PeerRouteDialResult, error) {
			<-ctx.Done()
			blackholeCanceled.Store(true)
			return PeerRouteDialResult{}, ctx.Err()
		}},
		{delay: 25 * time.Millisecond, dial: func(context.Context) (PeerRouteDialResult, error) {
			return PeerRouteDialResult{Conn: relayClient, Kind: RouteKindRelay, Target: "relay"}, nil
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, RouteKindRelay, winner.Kind)
	assert.Less(t, time.Since(start), 300*time.Millisecond)
	require.Eventually(t, blackholeCanceled.Load, time.Second, 10*time.Millisecond)
}

func TestRaceRouteDialsPreservesDirectPreference(t *testing.T) {
	directClient, directServer := net.Pipe()
	relayClient, relayServer := net.Pipe()
	t.Cleanup(func() {
		_ = directClient.Close()
		_ = directServer.Close()
		_ = relayClient.Close()
		_ = relayServer.Close()
	})
	var relayCalled atomic.Bool
	winner, err := raceRouteDials(context.Background(), []routeDialAttempt{
		{dial: func(context.Context) (PeerRouteDialResult, error) {
			return PeerRouteDialResult{Conn: directClient, Kind: RouteKindDirect}, nil
		}},
		{delay: 100 * time.Millisecond, dial: func(context.Context) (PeerRouteDialResult, error) {
			relayCalled.Store(true)
			return PeerRouteDialResult{Conn: relayClient, Kind: RouteKindRelay}, nil
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, RouteKindDirect, winner.Kind)
	time.Sleep(125 * time.Millisecond)
	assert.False(t, relayCalled.Load(), "losing delayed relay must be canceled before dialing")
}

func TestRaceRouteDialsClosesConcurrentSuccessfulLoser(t *testing.T) {
	first, firstPeer := net.Pipe()
	second, secondPeer := net.Pipe()
	t.Cleanup(func() {
		_ = firstPeer.Close()
		_ = secondPeer.Close()
	})
	tracked := []*closeTrackingConn{{Conn: first}, {Conn: second}}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	result := make(chan PeerRouteDialResult, 1)
	errs := make(chan error, 1)
	go func() {
		winner, err := raceRouteDials(context.Background(), []routeDialAttempt{
			{dial: func(context.Context) (PeerRouteDialResult, error) {
				ready <- struct{}{}
				<-release
				return PeerRouteDialResult{Conn: tracked[0]}, nil
			}},
			{dial: func(context.Context) (PeerRouteDialResult, error) {
				ready <- struct{}{}
				<-release
				return PeerRouteDialResult{Conn: tracked[1]}, nil
			}},
		})
		if err != nil {
			errs <- err
			return
		}
		result <- winner
	}()
	<-ready
	<-ready
	close(release)
	var winner PeerRouteDialResult
	select {
	case err := <-errs:
		require.NoError(t, err)
	case winner = <-result:
	case <-time.After(time.Second):
		t.Fatal("route race did not finish")
	}
	require.Eventually(t, func() bool {
		return tracked[0].closed.Load() || tracked[1].closed.Load()
	}, time.Second, time.Millisecond)
	if winner.Conn == tracked[0] {
		assert.True(t, tracked[1].closed.Load())
		assert.False(t, tracked[0].closed.Load())
	} else {
		assert.Same(t, tracked[1], winner.Conn)
		assert.True(t, tracked[0].closed.Load())
		assert.False(t, tracked[1].closed.Load())
	}
	require.NoError(t, winner.Conn.Close())
}

func TestRaceRouteDialsClosesConnectionReturnedWithError(t *testing.T) {
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close() })
	tracked := &closeTrackingConn{Conn: conn}
	_, err := raceRouteDials(context.Background(), []routeDialAttempt{
		{dial: func(context.Context) (PeerRouteDialResult, error) {
			return PeerRouteDialResult{Conn: tracked}, errors.New("dial failed after opening")
		}},
	})
	require.ErrorContains(t, err, "dial failed after opening")
	assert.True(t, tracked.closed.Load())
}

func TestJoinTransportBlackholedFirstTargetUsesRelay(t *testing.T) {
	relayClient, relayServer := net.Pipe()
	t.Cleanup(func() {
		_ = relayClient.Close()
		_ = relayServer.Close()
	})
	transport := joinP2PHTTPTransport(nil, func(ctx context.Context, target string) (net.Conn, error) {
		if !strings.Contains(target, "/p2p-circuit/") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return relayClient, nil
	}, []string{
		"/ip4/192.0.2.1/tcp/1/p2p/direct",
		"/ip4/192.0.2.2/tcp/2/p2p/relay/p2p-circuit/p2p/destination",
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	conn, err := transport.DialTLSContext(ctx, "tcp", "unused")
	require.NoError(t, err)
	assert.Same(t, relayClient, conn)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestMountedRouterRejectsPeerAndJoinTrafficAfterRuntimeDisable(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	router := m.Router()
	m.SetTransportEnabled(false)
	for _, path := range []string{"/fed/v1/status", "/fed/v1/join/status?session_id=anything"} {
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusServiceUnavailable, rr.Code, path)
		assert.Contains(t, rr.Body.String(), "disabled", path)
	}
}

func TestLocalRouteStatusDoesNotClaimUnknownDirectRouteReady(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	status := m.LocalRouteStatus()
	assert.Equal(t, "degraded", status["state"])
	candidates, ok := status["candidates"].([]map[string]any)
	require.True(t, ok)
	assert.Empty(t, candidates)
	assert.NotContains(t, status, "selected")
	assert.Contains(t, status["message"], "No route is selected")
}

func TestPersistRouteSnapshotRejectsStaleAndConflictingRevision(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	binding := p2pRouteBinding{peerAgentID: "peer", policyEpoch: "epoch", bindingState: "active"}
	generation := routeBindingID(binding)
	current := RouteSnapshot{}
	var present bool
	var persistCalls int
	m.SetJoinP2PHooks(JoinP2PHooks{
		LoadSnapshot: func(string) (RouteSnapshot, bool) { return current, present },
		PersistSnapshot: func(_ string, snapshot RouteSnapshot) error {
			persistCalls++
			current, present = snapshot, true
			return nil
		},
	})
	now := time.Now()
	bundle := JoinP2PBundle{
		PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: []string{"one"},
		Revision: 4, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
	}
	require.NoError(t, m.persistRouteSnapshot("chain-b", binding, bundle))
	assert.Equal(t, generation, current.Generation)
	assert.Equal(t, 1, persistCalls)

	stale := bundle
	stale.Revision = 3
	require.ErrorContains(t, m.persistRouteSnapshot("chain-b", binding, stale), "stale")
	assert.Equal(t, 1, persistCalls)

	conflict := bundle
	conflict.Addrs = []string{"different"}
	require.ErrorContains(t, m.persistRouteSnapshot("chain-b", binding, conflict), "conflicting")
	assert.Equal(t, 1, persistCalls)

	require.NoError(t, m.persistRouteSnapshot("chain-b", binding, bundle), "identical revision is idempotent")
	assert.Equal(t, 1, persistCalls)
}

func TestPersistRouteSnapshotNewGenerationMayReplaceHigherOldRevision(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	oldBinding := p2pRouteBinding{peerAgentID: "peer", policyEpoch: "old", bindingState: "active"}
	newBinding := p2pRouteBinding{peerAgentID: "peer", policyEpoch: "new", bindingState: "active"}
	now := time.Now()
	current := RouteSnapshot{
		PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: []string{"old"},
		Revision: 99, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
		Generation: routeBindingID(oldBinding),
	}
	m.SetJoinP2PHooks(JoinP2PHooks{
		LoadSnapshot: func(string) (RouteSnapshot, bool) { return current, true },
		PersistSnapshot: func(_ string, snapshot RouteSnapshot) error {
			current = snapshot
			return nil
		},
	})
	require.NoError(t, m.persistRouteSnapshot("chain-b", newBinding, JoinP2PBundle{
		PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: []string{"new"},
		Revision: 1, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
	}))
	assert.Equal(t, routeBindingID(newBinding), current.Generation)
	assert.Equal(t, uint64(1), current.Revision)
}

func TestTriggerRouteRefreshRetriesLostExchange(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) {
			return JoinP2PBundle{PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: []string{"relay"}}, nil
		},
	})
	var attempts atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)
	m.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error {
		defer wg.Done()
		if attempts.Add(1) == 1 {
			return errors.New("simulated lost response")
		}
		return nil
	}
	m.triggerRouteRefresh("chain-b")
	require.Eventually(t, func() bool {
		m.routeRefreshMu.Lock()
		defer m.routeRefreshMu.Unlock()
		return !m.routeRefreshActive["chain-b"]
	}, time.Second, 10*time.Millisecond)
	m.triggerRouteRefresh("chain-b")
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route refresh retry did not run")
	}
	assert.Equal(t, int32(2), attempts.Load())
}

func TestRouteRefresherStartsOnceAndStopCancelsInflightRefresh(t *testing.T) {
	a := newTestChain(t, "route-refresh-lifecycle-a")
	b := newTestChain(t, "route-refresh-lifecycle-b")
	federate(t, a, b, "https://127.0.0.1:1", nil, 4, 0)
	a.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) {
			return JoinP2PBundle{PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: []string{"relay"}}, nil
		},
	})
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	a.mgr.routeRefreshFn = func(ctx context.Context, _ string, _ JoinP2PBundle) error {
		calls.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		return ctx.Err()
	}
	a.mgr.StartRouteRefresher(context.Background())
	a.mgr.StartRouteRefresher(context.Background())
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial route refresh did not start")
	}
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, int32(1), calls.Load())
	a.mgr.StopRouteRefresher()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("route refresher stop did not cancel its in-flight exchange")
	}
	a.mgr.StopRouteRefresher()
}

func TestDisabledTransportFailsBeforeDial(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	m.SetTransportEnabled(false)
	called := false
	m.SetPeerDialFunc(func(context.Context, string) (net.Conn, bool, error) {
		called = true
		return nil, true, errors.New("unexpected")
	})
	_, _, err := m.doPeerRequest(context.Background(), testAgreement("chain-b"), "GET", "/fed/v1/status", nil)
	require.ErrorContains(t, err, "disabled")
	assert.False(t, called)
	assert.Equal(t, RouteStateDisabled, m.RouteDiagnostics("chain-b").State)
}

func TestSecurityErrorsAreNotConnectivityFallbackEligible(t *testing.T) {
	err := x509.UnknownAuthorityError{}
	assert.True(t, isSecurityTransportError(err))
	assert.False(t, isPeerOfflineDialError(err))
	assert.False(t, isSecurityTransportError(&net.DNSError{IsTimeout: true}))
}

func testAgreement(chain string) *store.CrossFedRecord {
	return &store.CrossFedRecord{RemoteChainID: chain, Endpoint: "https://127.0.0.1:1"}
}
