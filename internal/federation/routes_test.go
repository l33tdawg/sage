package federation

import (
	"context"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
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
	}, 2*time.Second, time.Millisecond)
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

func TestJoinTransportDoesNotRetrySecurityFailure(t *testing.T) {
	var calls atomic.Int32
	transport := joinP2PHTTPTransport(nil, func(context.Context, string) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("tls: bad certificate")
	}, []string{"/ip4/192.0.2.1/tcp/1/p2p/direct"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := transport.DialTLSContext(ctx, "tcp", "unused")
	require.ErrorContains(t, err, "authentication failed")
	assert.Equal(t, int32(1), calls.Load())
	assert.Less(t, time.Since(start), 250*time.Millisecond, "security failures must not enter availability retry backoff")
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
	assert.Contains(t, status["message"], "No route is ready")
}

func TestLocalRouteStatusReportsPreparedDirectRoute(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return testDirectRouteBundle(t, "192.168.1.25"), nil },
	})
	status := m.LocalRouteStatus()
	assert.Equal(t, "ready", status["state"])
	candidates, ok := status["candidates"].([]map[string]any)
	require.True(t, ok)
	require.Len(t, candidates, 1)
	assert.Equal(t, RouteKindP2PDirect, candidates[0]["kind"])
	assert.Contains(t, status["message"], "A direct route is prepared")
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
	binding := p2pRouteBinding{peerAgentID: "peer", policyEpoch: "epoch", bindingState: "active"}
	require.Error(t, waitRouteRefresh(context.Background(), m.beginRouteRefresh(context.Background(), "chain-b", binding)))
	require.NoError(t, waitRouteRefresh(context.Background(), m.beginRouteRefresh(context.Background(), "chain-b", binding)))
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("route refresh retry did not run")
	}
	assert.Equal(t, int32(2), attempts.Load())
}

func TestRouteRefreshFailureIsVisibleInDiagnostics(t *testing.T) {
	m := &Manager{routeStatus: make(map[string]RouteDiagnostics)}
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) {
			return JoinP2PBundle{PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: []string{"relay"}}, nil
		},
	})
	m.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error {
		return errors.New("simulated route outage")
	}
	binding := p2pRouteBinding{peerAgentID: "peer", policyEpoch: "epoch", bindingState: "active"}
	require.Error(t, waitRouteRefresh(context.Background(), m.beginRouteRefresh(context.Background(), "chain-b", binding)))
	require.Eventually(t, func() bool {
		return strings.Contains(m.RouteDiagnostics("chain-b").LastError, "simulated route outage")
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, RouteStateOffline, m.RouteDiagnostics("chain-b").State)
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
	bindP2PRouteControl(t, a.mgr, b.chainID, hex.EncodeToString(b.agentPub), "route-refresh-lifecycle")
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

func newRetryTestPair(t *testing.T, endpoint string) (*testChain, *testChain, *httptest.Server, *atomic.Int32, p2pRouteBinding) {
	return newRetryTestPairWithIntercept(t, endpoint, nil)
}

func newRetryTestPairWithIntercept(t *testing.T, endpoint string, intercept func(http.ResponseWriter, *http.Request) bool) (*testChain, *testChain, *httptest.Server, *atomic.Int32, p2pRouteBinding) {
	t.Helper()
	a := newTestChain(t, "retry-a")
	b := newTestChain(t, "retry-b")
	var statusCalls atomic.Int32
	tlsCfg, err := b.mgr.ServerTLSConfig()
	require.NoError(t, err)
	router := b.mgr.Router()
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fed/v1/status" {
			statusCalls.Add(1)
		}
		if intercept != nil && intercept(w, r) {
			return
		}
		router.ServeHTTP(w, r)
	}))
	server.TLS = tlsCfg
	server.StartTLS()
	t.Cleanup(server.Close)
	if endpoint == "" {
		endpoint = server.URL
	}
	federate(t, a, b, endpoint, []string{"*"}, 4, 0)
	federate(t, b, a, "https://127.0.0.1:1", []string{"*"}, 4, 0)
	bindP2PRouteControl(t, a.mgr, b.chainID, hex.EncodeToString(b.agentPub), "retry-epoch")
	bindP2PRouteControl(t, b.mgr, a.chainID, hex.EncodeToString(a.agentPub), "retry-epoch")
	binding, err := a.mgr.routeRefreshBinding(context.Background(), b.chainID)
	require.NoError(t, err)
	return a, b, server, &statusCalls, binding
}

func installRetryRouteHooks(t *testing.T, m *Manager, chain string, binding p2pRouteBinding, expiresAt time.Time) {
	t.Helper()
	bundle := testDirectRouteBundle(t, "192.0.2.25")
	now := time.Now()
	snapshot := snapshotFromBundle(JoinP2PBundle{
		PeerID: bundle.PeerID, Protocol: bundle.Protocol, Addrs: bundle.Addrs,
		Revision: 7, IssuedAt: now.Add(-time.Hour).Unix(), ExpiresAt: expiresAt.Unix(),
	}, routeBindingID(binding))
	m.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle:  func() (JoinP2PBundle, error) { return bundle, nil },
		LoadSnapshot: func(got string) (RouteSnapshot, bool) { return snapshot, got == chain },
	})
}

func TestRetryPeerStatusDeduplicatesRefreshAndAuthenticatedReprobe(t *testing.T) {
	a, b, _, statusCalls, binding := newRetryTestPair(t, "")
	installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
	started := make(chan struct{})
	release := make(chan struct{})
	var refreshCalls atomic.Int32
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error {
		if refreshCalls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	}
	const callers = 12
	errs := make(chan error, callers)
	for range callers {
		go func() {
			status, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
			if err == nil && (status == nil || status.ChainID != b.chainID) {
				err = errors.New("retry returned the wrong authenticated peer")
			}
			errs <- err
		}()
	}
	<-started
	time.Sleep(30 * time.Millisecond)
	close(release)
	for range callers {
		require.NoError(t, <-errs)
	}
	assert.Equal(t, int32(1), refreshCalls.Load())
	assert.Equal(t, int32(1), statusCalls.Load(), "concurrent operator retries must share exactly one re-probe")
}

func TestRetryPeerStatusStaleDirectRecoversThroughExactGenerationRelay(t *testing.T) {
	a, b, server, statusCalls, binding := newRetryTestPair(t, "https://127.0.0.1:1")
	installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
	var relayDials atomic.Int32
	a.mgr.SetPeerRouteDialFunc(func(ctx context.Context, chain string, _ []string, authenticate PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		relayDials.Add(1)
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
		return PeerRouteDialResult{Conn: conn, Kind: RouteKindRelay, Target: "authenticated-test-relay"}, true, err
	})
	status, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
	require.NoError(t, err)
	require.Equal(t, b.chainID, status.ChainID)
	assert.Equal(t, int32(1), relayDials.Load())
	assert.Equal(t, int32(1), statusCalls.Load())
	assert.Equal(t, RouteKindRelay, a.mgr.RouteDiagnostics(b.chainID).State)
}

func TestRetryPeerStatusExpiredSnapshotUsesAuthenticatedBootstrap(t *testing.T) {
	a, b, _, statusCalls, binding := newRetryTestPair(t, "")
	installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(-time.Minute))
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
	status, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
	require.NoError(t, err)
	assert.Equal(t, b.chainID, status.ChainID)
	assert.Equal(t, int32(1), statusCalls.Load())
}

func TestRetryPeerStatusRejectsLegacyAndStaleGenerationRoutes(t *testing.T) {
	t.Run("legacy requires re-pair", func(t *testing.T) {
		a := newTestChain(t, "legacy-retry-a")
		b := newTestChain(t, "legacy-retry-b")
		federate(t, a, b, "https://127.0.0.1:1", nil, 4, 0)
		_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
		require.ErrorIs(t, err, ErrLegacyRouteBinding)
		require.ErrorContains(t, err, "paired again")
	})
	t.Run("stale generation never dials", func(t *testing.T) {
		a, b, _, _, binding := newRetryTestPair(t, "")
		installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
		hooks := a.mgr.joinP2PHooks()
		good, _ := hooks.LoadSnapshot(b.chainID)
		good.Generation = "different-trust-generation"
		hooks.LoadSnapshot = func(string) (RouteSnapshot, bool) { return good, true }
		a.mgr.SetJoinP2PHooks(hooks)
		a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
		var routeDials atomic.Int32
		a.mgr.SetPeerRouteDialFunc(func(context.Context, string, []string, PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
			routeDials.Add(1)
			return PeerRouteDialResult{}, true, errors.New("stale route must not be dialed")
		})
		_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
		require.NoError(t, err, "the independently pinned Direct endpoint remains safe")
		assert.Zero(t, routeDials.Load())
	})
}

func TestRetryPeerStatusIsBoundedAndSecurityFailureSkipsReprobe(t *testing.T) {
	a, b, _, statusCalls, binding := newRetryTestPair(t, "")
	installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
	started := make(chan struct{})
	release := make(chan struct{})
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error {
		close(started)
		<-release
		return errors.New("peer identity mismatch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	begin := time.Now()
	_, err := a.mgr.RetryPeerStatus(ctx, b.chainID)
	require.ErrorContains(t, err, "timed out")
	assert.Less(t, time.Since(begin), 500*time.Millisecond)
	<-started
	close(release)
	require.Eventually(t, func() bool {
		a.mgr.routeRetryMu.Lock()
		defer a.mgr.routeRetryMu.Unlock()
		return len(a.mgr.routeRetryActive) == 0
	}, 3*time.Second, time.Millisecond)
	assert.Zero(t, statusCalls.Load(), "a security-blocked refresh must not fall through to a status request")
}

func TestRetryPeerStatusFederationOffThenOnKeepsTrustEdge(t *testing.T) {
	a, b, _, _, binding := newRetryTestPair(t, "")
	installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
	a.mgr.SetTransportEnabled(false)
	_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
	require.ErrorContains(t, err, "disabled")
	a.mgr.SetTransportEnabled(true)
	status, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
	require.NoError(t, err)
	assert.Equal(t, b.chainID, status.ChainID)
}

func TestRetryPeerStatusHTTPAuthorizationDenialStopsBeforeReprobe(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var routeRequests atomic.Int32
			a, b, _, statusCalls, binding := newRetryTestPairWithIntercept(t, "", func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/fed/v1/p2p/routes" {
					return false
				}
				routeRequests.Add(1)
				http.Error(w, "policy epoch denied", status)
				return true
			})
			installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
			_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
			require.ErrorIs(t, err, ErrRouteExchangeDenied)
			assert.Equal(t, RouteRecoverySecurityBlocked, RouteRecoveryFailureCode(err))
			assert.Equal(t, int32(1), routeRequests.Load())
			assert.Zero(t, statusCalls.Load(), "an authenticated route denial must stop before status re-probe")
		})
	}
}

func TestRetryPeerStatusFreezesExactGenerationTargetsBeforeDial(t *testing.T) {
	a, b, server, _, binding := newRetryTestPair(t, "https://127.0.0.1:1")
	g := testDirectRouteBundle(t, "192.0.2.40")
	g2 := testDirectRouteBundle(t, "192.0.2.41")
	now := time.Now()
	snapshot := snapshotFromBundle(JoinP2PBundle{
		PeerID: g.PeerID, Protocol: g.Protocol, Addrs: g.Addrs,
		Revision: 10, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
	}, routeBindingID(binding))
	var snapshotMu sync.RWMutex
	a.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return g, nil },
		LoadSnapshot: func(string) (RouteSnapshot, bool) {
			snapshotMu.RLock()
			defer snapshotMu.RUnlock()
			return snapshot, true
		},
	})
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
	dialStarted := make(chan []string, 1)
	releaseDial := make(chan struct{})
	a.mgr.SetPeerRouteDialFunc(func(ctx context.Context, _ string, targets []string, _ PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		dialStarted <- append([]string(nil), targets...)
		<-releaseDial
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
		return PeerRouteDialResult{Conn: conn, Kind: RouteKindP2PDirect, Target: targets[0]}, true, err
	})
	result := make(chan error, 1)
	go func() {
		_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
		result <- err
	}()
	frozen := <-dialStarted
	snapshotMu.Lock()
	snapshot.PeerID = g2.PeerID
	snapshot.Addrs = append([]string(nil), g2.Addrs...)
	snapshot.Revision++
	snapshotMu.Unlock()
	assert.Equal(t, g.Addrs, frozen, "dial must receive the immutable G snapshot copied before the swap")
	assert.NotEqual(t, g2.Addrs, frozen, "G2 targets must never enter the G recovery request")
	close(releaseDial)
	require.NoError(t, <-result)
}

func TestRetryPeerStatusEmptyFrozenGenerationNeverReloadsNewTargets(t *testing.T) {
	a, b, _, _, binding := newRetryTestPair(t, joinP2POnlyEndpoint)
	g := testDirectRouteBundle(t, "192.0.2.42")
	g2 := testDirectRouteBundle(t, "192.0.2.43")
	now := time.Now()
	snapshot := snapshotFromBundle(JoinP2PBundle{
		PeerID: g.PeerID, Protocol: g.Protocol, Addrs: []string{},
		Revision: 11, IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix(),
	}, routeBindingID(binding))
	a.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle:  func() (JoinP2PBundle, error) { return g, nil },
		LoadSnapshot: func(string) (RouteSnapshot, bool) { return snapshot, true },
	})
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
	reloadedCurrent := false
	var attempted []string
	a.mgr.SetPeerRouteDialFunc(func(_ context.Context, _ string, targets []string, _ PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		if targets == nil {
			reloadedCurrent = true
			targets = g2.Addrs
		}
		attempted = append(attempted, targets...)
		return PeerRouteDialResult{}, true, errors.New("frozen generation has no route targets")
	})

	_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
	require.Error(t, err)
	assert.False(t, reloadedCurrent, "exact-generation empty targets must not collapse to the normal-call nil sentinel")
	assert.Empty(t, attempted, "G2 targets must never enter an empty G recovery request")
}

func TestRetryPeerStatusRejectsGenerationChangeDuringStatusResponse(t *testing.T) {
	statusStarted := make(chan struct{}, 1)
	releaseStatus := make(chan struct{})
	a, b, _, _, binding := newRetryTestPairWithIntercept(t, "", func(_ http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path != "/fed/v1/status" {
			return false
		}
		statusStarted <- struct{}{}
		<-releaseStatus
		return false
	})
	installRetryRouteHooks(t, a.mgr, b.chainID, binding, time.Now().Add(time.Hour))
	a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return nil }
	result := make(chan error, 1)
	go func() {
		_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
		result <- err
	}()
	<-statusStarted
	ss := a.mgr.syncStore()
	control, err := ss.GetSyncControl(context.Background(), b.chainID)
	require.NoError(t, err)
	require.NotNil(t, control)
	replacement := *control
	replacement.PolicyEpoch = "retry-epoch-replaced"
	require.NoError(t, ss.PurgeSyncPeerState(context.Background(), b.chainID))
	require.NoError(t, ss.PrepareSyncControl(context.Background(), replacement))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), b.chainID, replacement.PolicyEpoch))
	close(releaseStatus)
	err = <-result
	require.ErrorIs(t, err, ErrTrustGenerationChanged)
	assert.Equal(t, RouteRecoveryTrustGenerationMismatch, RouteRecoveryFailureCode(err))
}

func TestRetryPeerStatusLegacyActiveUnprovableBindingRequiresRepair(t *testing.T) {
	a := newTestChain(t, "legacy-active-a")
	b := newTestChain(t, "legacy-active-b")
	federate(t, a, b, "https://127.0.0.1:1", nil, 4, 0)
	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)
	ss := a.mgr.syncStore()
	epoch := "legacy-unfrozen"
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: b.chainID, Role: "host", ControllerChainID: a.chainID,
		ControllerAgentID: hex.EncodeToString(a.agentPub), PolicyEpoch: epoch,
		RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey), PolicyVersion: SyncPolicyVersionLegacy,
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), b.chainID, epoch))
	_, err = a.mgr.RetryPeerStatus(context.Background(), b.chainID)
	require.ErrorIs(t, err, ErrLegacyRouteBinding)
	assert.Equal(t, RouteRecoveryLegacyRepairRequired, RouteRecoveryFailureCode(err))
}

func TestRetryPeerStatusEmitsTypedOperationalDiagnostics(t *testing.T) {
	tests := []struct {
		name     string
		snapshot func(t *testing.T, binding p2pRouteBinding) (RouteSnapshot, bool)
		want     string
	}{
		{name: "missing", want: RouteRecoveryBundleMissing, snapshot: func(*testing.T, p2pRouteBinding) (RouteSnapshot, bool) { return RouteSnapshot{}, false }},
		{name: "expired", want: RouteRecoveryBundleExpired, snapshot: func(t *testing.T, binding p2pRouteBinding) (RouteSnapshot, bool) {
			bundle := testDirectRouteBundle(t, "192.0.2.50")
			return snapshotFromBundle(JoinP2PBundle{PeerID: bundle.PeerID, Protocol: bundle.Protocol, Addrs: bundle.Addrs, Revision: 1, IssuedAt: time.Now().Add(-time.Hour).Unix(), ExpiresAt: time.Now().Add(-time.Minute).Unix()}, routeBindingID(binding)), true
		}},
		{name: "stale direct", want: RouteRecoveryStaleDirect, snapshot: func(t *testing.T, binding p2pRouteBinding) (RouteSnapshot, bool) {
			bundle := testDirectRouteBundle(t, "192.0.2.51")
			return snapshotFromBundle(JoinP2PBundle{PeerID: bundle.PeerID, Protocol: bundle.Protocol, Addrs: bundle.Addrs, Revision: 1, IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix()}, routeBindingID(binding)), true
		}},
		{name: "relay unavailable", want: RouteRecoveryRelayUnavailable, snapshot: func(t *testing.T, binding p2pRouteBinding) (RouteSnapshot, bool) {
			bundle := testRouteBundle(t, "203.0.113.52")
			return snapshotFromBundle(JoinP2PBundle{PeerID: bundle.PeerID, Protocol: bundle.Protocol, Addrs: bundle.Addrs, Revision: 1, IssuedAt: time.Now().Unix(), ExpiresAt: time.Now().Add(time.Hour).Unix()}, routeBindingID(binding)), true
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b, _, _, binding := newRetryTestPair(t, "https://127.0.0.1:1")
			local := testDirectRouteBundle(t, "192.0.2.60")
			snapshot, present := tc.snapshot(t, binding)
			a.mgr.SetJoinP2PHooks(JoinP2PHooks{
				LocalBundle:  func() (JoinP2PBundle, error) { return local, nil },
				LoadSnapshot: func(string) (RouteSnapshot, bool) { return snapshot, present },
			})
			a.mgr.routeRefreshFn = func(context.Context, string, JoinP2PBundle) error { return errors.New("route refresh unavailable") }
			_, err := a.mgr.RetryPeerStatus(context.Background(), b.chainID)
			require.Error(t, err)
			assert.Equal(t, tc.want, RouteRecoveryFailureCode(err))
		})
	}
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

// TestRouteRecoveryVerdictsDoNotBlameTheRelay pins the distinction that cost an
// operator an hour: a relayed candidate that is merely slow, or that the peer
// closed mid-handshake, must be reported as a transport verdict rather than as a
// relay-availability failure. The hint only knows that a relay address exists.
func TestRouteRecoveryVerdictsDoNotBlameTheRelay(t *testing.T) {
	relayHint := RouteRecoveryRelayUnavailable
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"deadline", fmt.Errorf("refresh route: %w", context.DeadlineExceeded), RouteRecoveryTimeout},
		{"deadline text", errors.New("p2p dial: context deadline exceeded"), RouteRecoveryTimeout},
		{"handshake closed", errors.New("tls handshake: EOF"), RouteRecoveryHandshakeFailed},
		{"stream reset", errors.New("stream reset by peer"), RouteRecoveryHandshakeFailed},
		{"unclassified still uses the hint", errors.New("relay reservation not found"), RouteRecoveryRelayUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			classified := classifyRouteRecoveryError(tc.err, relayHint)
			require.Equal(t, tc.want, RouteRecoveryFailureCode(classified))
		})
	}
}

// TestRouteRefreshBudgetPrefersTheRelayBudgetForCircuitPeers proves the budget
// is chosen from the frozen candidate set, and that both halves stay
// operator-tunable for a pathological path.
func TestRouteRefreshBudgetPrefersTheRelayBudgetForCircuitPeers(t *testing.T) {
	m := &Manager{}
	direct, relay := "chain-direct", "chain-relay"
	snapshots := map[string]RouteSnapshot{
		direct: {Addrs: []string{"/ip4/192.168.30.9/tcp/51458/p2p/12D3KooWdirect"}},
		relay:  {Addrs: []string{"/ip4/65.108.81.134/tcp/4001/p2p/12D3KooWrelay/p2p-circuit/p2p/12D3KooWpeer"}},
	}
	m.SetJoinP2PHooks(JoinP2PHooks{
		LoadSnapshot: func(chain string) (RouteSnapshot, bool) {
			snapshot, ok := snapshots[chain]
			return snapshot, ok
		},
	})
	require.False(t, m.RouteRelayPreferred(direct))
	require.True(t, m.RouteRelayPreferred(relay))
	require.Equal(t, routeRefreshTimeoutDirect, m.routeRefreshBudget(direct))
	require.Equal(t, routeRefreshTimeoutRelay, m.routeRefreshBudget(relay))

	t.Setenv("SAGE_FED_RELAY_TIMEOUT_MS", "45000")
	require.Equal(t, 45*time.Second, m.routeRefreshBudget(relay))
	t.Setenv("SAGE_FED_ROUTE_TIMEOUT_MS", "12000")
	require.Equal(t, 12*time.Second, m.routeRefreshBudget(direct))
}
