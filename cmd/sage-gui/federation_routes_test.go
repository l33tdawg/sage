package main

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	libcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
	sagep2p "github.com/l33tdawg/sage/internal/p2p"
)

type trackedRouteConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *trackedRouteConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

type routedTestConn struct {
	*trackedRouteConn
	target  string
	limited bool
}

func (c *routedTestConn) P2PRoute() (string, bool) { return c.target, c.limited }

// TestDialFederationP2PRouteTargetsGivesRelayedCandidatesMoreTime pins the fix
// for the flapping relay: a relayed candidate needs several round trips (two
// network legs plus the relay's handshake) before the federation TLS handshake
// even starts, so it must not be judged with the direct 2s budget. The budgets
// are compressed here so the ordering is provable without a slow test.
func TestDialFederationP2PRouteTargetsGivesRelayedCandidatesMoreTime(t *testing.T) {
	t.Setenv("SAGE_FED_DIRECT_CANDIDATE_TIMEOUT_MS", "60")
	t.Setenv("SAGE_FED_RELAY_CANDIDATE_TIMEOUT_MS", "600")
	relayTarget := "/ip4/65.108.81.134/tcp/4001/p2p/12D3KooWrelay/p2p-circuit/p2p/12D3KooWpeer"
	directTarget := "/ip4/192.168.30.9/tcp/51458/p2p/12D3KooWdirect"

	slowDial := func(delay time.Duration) func(context.Context, string) (net.Conn, error) {
		return func(ctx context.Context, _ string) (net.Conn, error) {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
				client, peer := net.Pipe()
				t.Cleanup(func() { _ = peer.Close() })
				return client, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	// Slower than the direct budget, well inside the relay budget.
	winner, handled, err := dialFederationP2PRouteTargets(
		context.Background(), []string{relayTarget}, slowDial(200*time.Millisecond), nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, relayTarget, winner.Target)
	require.Equal(t, federation.RouteKindRelay, winner.Kind,
		"a circuit candidate must be dialed as a relay route")
	if winner.Conn != nil {
		_ = winner.Conn.Close()
	}

	// The same latency on a direct candidate must still be cut off at the
	// direct budget: the longer relay budget is not a blanket widening.
	_, handled, err = dialFederationP2PRouteTargets(
		context.Background(), []string{directTarget}, slowDial(200*time.Millisecond), nil)
	require.True(t, handled)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestDialFederationP2PRouteTargetsClosesConcurrentLoser(t *testing.T) {
	first, firstPeer := net.Pipe()
	second, secondPeer := net.Pipe()
	t.Cleanup(func() {
		_ = firstPeer.Close()
		_ = secondPeer.Close()
	})
	conns := map[string]*trackedRouteConn{
		"first":  {Conn: first},
		"second": {Conn: second},
	}
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	result := make(chan federation.PeerRouteDialResult, 1)
	errs := make(chan error, 1)
	go func() {
		winner, _, err := dialFederationP2PRouteTargets(context.Background(), []string{"first", "second"},
			func(_ context.Context, target string) (net.Conn, error) {
				ready <- struct{}{}
				<-release
				return conns[target], nil
			}, nil)
		if err != nil {
			errs <- err
			return
		}
		result <- winner
	}()
	<-ready
	<-ready
	close(release)
	var winner federation.PeerRouteDialResult
	select {
	case err := <-errs:
		require.NoError(t, err)
	case winner = <-result:
	case <-time.After(time.Second):
		t.Fatal("p2p route race did not finish")
	}
	require.Eventually(t, func() bool {
		return conns["first"].closed.Load() || conns["second"].closed.Load()
	}, time.Second, time.Millisecond)
	for target, conn := range conns {
		if winner.Conn == conn {
			assert.False(t, conn.closed.Load(), target)
			require.NoError(t, conn.Close())
		} else {
			assert.True(t, conn.closed.Load(), target)
		}
	}
}

func TestDialFederationP2PRouteTargetsClosesConnectionReturnedWithError(t *testing.T) {
	conn, peerConn := net.Pipe()
	t.Cleanup(func() { _ = peerConn.Close() })
	tracked := &trackedRouteConn{Conn: conn}
	_, handled, err := dialFederationP2PRouteTargets(context.Background(), []string{"only"},
		func(context.Context, string) (net.Conn, error) {
			return tracked, errors.New("opened then failed")
		}, nil)
	assert.True(t, handled)
	require.ErrorContains(t, err, "opened then failed")
	assert.True(t, tracked.closed.Load())
}

func TestDialFederationP2PRouteTargetsAuthenticatesBeforeChoosingWinner(t *testing.T) {
	bad, badPeer := net.Pipe()
	good, goodPeer := net.Pipe()
	t.Cleanup(func() {
		_ = badPeer.Close()
		_ = goodPeer.Close()
	})
	badTracked := &trackedRouteConn{Conn: bad}
	goodTracked := &trackedRouteConn{Conn: good}
	winner, handled, err := dialFederationP2PRouteTargets(context.Background(), []string{"bad", "good"},
		func(_ context.Context, target string) (net.Conn, error) {
			if target == "bad" {
				return badTracked, nil
			}
			return goodTracked, nil
		},
		func(_ context.Context, result federation.PeerRouteDialResult, dialErr error) (federation.PeerRouteDialResult, error) {
			if dialErr != nil {
				return result, dialErr
			}
			if result.Target == "bad" {
				_ = result.Conn.Close()
				result.Conn = nil
				return result, errors.New("pinned TLS identity mismatch")
			}
			result.Authenticated = true
			return result, nil
		})
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Same(t, goodTracked, winner.Conn)
	assert.True(t, winner.Authenticated)
	require.Eventually(t, badTracked.closed.Load, time.Second, 5*time.Millisecond)
	require.NoError(t, winner.Conn.Close())
}

func TestDialFederationP2PRouteTargetsReportsReusedLiveRelay(t *testing.T) {
	raw, peerConn := net.Pipe()
	t.Cleanup(func() { _ = peerConn.Close() })
	tracked := &trackedRouteConn{Conn: raw}
	conn := &routedTestConn{
		trackedRouteConn: tracked,
		target:           "/ip4/192.0.2.9/tcp/4001/p2p/relay/p2p-circuit/p2p/peer",
		limited:          true,
	}
	winner, handled, err := dialFederationP2PRouteTargets(
		context.Background(),
		[]string{"/ip4/127.0.0.1/tcp/4002/p2p/peer"},
		func(context.Context, string) (net.Conn, error) { return conn, nil },
		nil,
	)
	require.NoError(t, err)
	assert.True(t, handled)
	assert.Equal(t, federation.RouteKindRelay, winner.Kind)
	assert.Equal(t, conn.target, winner.Target)
	require.NoError(t, winner.Conn.Close())
}

func TestFederationRouteTraceReportsOnlyActualCandidateStarts(t *testing.T) {
	direct := "/ip4/192.0.2.10/tcp/4001/p2p/peer"
	relay := "/ip4/192.0.2.20/tcp/4001/p2p/relay/p2p-circuit/p2p/peer"
	ctx, snapshot := federation.WithPeerRouteAttemptTrace(context.Background())
	_, handled, err := dialFederationP2PRouteTargets(ctx, []string{direct, relay},
		func(context.Context, string) (net.Conn, error) { return nil, context.DeadlineExceeded }, nil)
	require.True(t, handled)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	attempts := snapshot()
	require.Len(t, attempts, 2)
	assert.Contains(t, attempts, federation.PeerRouteAttempt{Kind: federation.RouteKindP2PDirect, Target: direct, Verdict: federation.RouteRecoveryTimeout})
	assert.Contains(t, attempts, federation.PeerRouteAttempt{Kind: federation.RouteKindRelay, Target: relay, Verdict: federation.RouteRecoveryTimeout})

	// The relay head-start timer must not be reported as an attempted dial
	// when the caller's deadline ends before that candidate starts.
	shortCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	shortCtx, snapshot = federation.WithPeerRouteAttemptTrace(shortCtx)
	_, handled, err = dialFederationP2PRouteTargets(shortCtx, []string{relay},
		func(context.Context, string) (net.Conn, error) {
			t.Error("relay must not start before its delayed candidate is admitted")
			return nil, context.DeadlineExceeded
		}, nil)
	require.True(t, handled)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Empty(t, snapshot())
}

func TestExpiredPersistedFederationRouteRemainsRecoveryHintAndSynthesizesRelay(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "config.yaml"),
		[]byte("federation:\n  enabled: true\n  p2p_enabled: true\n"), 0o600))
	priv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	staleTarget := "/ip4/203.0.113.10/tcp/4001/p2p/" + id.String()
	now := time.Now()
	require.NoError(t, persistFederationRouteSnapshot("expired-chain", federation.RouteSnapshot{
		PeerID: id.String(), Protocol: string(sagep2p.FederationProtocol),
		Addrs: []string{staleTarget}, Revision: 8, IssuedAt: now.Add(-48 * time.Hour).Unix(),
		ExpiresAt: now.Add(-24 * time.Hour).Unix(), Generation: "generation",
	}))
	cfg, err := LoadConfig() // restart-style reload from durable config
	require.NoError(t, err)
	require.NotEmpty(t, cfg.Federation.P2PRelayAddrs)
	targets, err := configuredFederationRouteTargets(cfg.Federation, "expired-chain")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(targets), 2)
	assert.Equal(t, staleTarget, targets[0], "stale direct address remains a non-authorizing recovery hint")
	assert.Contains(t, targets, cfg.Federation.P2PRelayAddrs[0]+"/p2p-circuit/p2p/"+id.String())
}

func TestConfiguredFederationRouteTargetsUsesCurrentSnapshotNotLegacyMirror(t *testing.T) {
	priv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	now := time.Now()
	current := "/ip4/10.0.0.8/tcp/4001/p2p/" + id.String()
	cfg := FederationConfig{
		P2PPeers: map[string][]string{"peer": {"stale-mirror"}},
		P2PRoutes: map[string]FederationRouteSnapshot{"peer": {
			PeerID: id.String(), Protocol: string(sagep2p.FederationProtocol),
			Addrs: []string{current}, Revision: 2, IssuedAt: now.Add(-time.Minute).Unix(),
			ExpiresAt: now.Add(time.Hour).Unix(), Generation: "generation",
		}},
	}
	targets, err := configuredFederationRouteTargets(cfg, "peer")
	require.NoError(t, err)
	assert.Equal(t, []string{current}, targets)
}

func TestConfiguredFederationRouteChainIDsIncludesVersionedOnlyRoutes(t *testing.T) {
	cfg := FederationConfig{
		P2PPeers:  map[string][]string{"legacy-only": nil, "both": nil},
		P2PRoutes: map[string]FederationRouteSnapshot{"versioned-only": {}, "both": {}},
	}
	assert.Equal(t, []string{"both", "legacy-only", "versioned-only"}, configuredFederationRouteChainIDs(cfg))
}

func TestLegacyFederationRoutesSynthesizeRelayRecovery(t *testing.T) {
	remotePriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	remoteID, err := peer.IDFromPrivateKey(remotePriv)
	require.NoError(t, err)
	relayPriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	relayID, err := peer.IDFromPrivateKey(relayPriv)
	require.NoError(t, err)
	direct := "/ip4/192.168.50.250/tcp/49123/p2p/" + remoteID.String()
	relay := "/ip4/198.51.100.20/tcp/4001/p2p/" + relayID.String()
	cfg := FederationConfig{
		P2PPeers:      map[string][]string{"legacy": {direct}},
		P2PRelayAddrs: []string{relay},
	}
	targets, err := configuredFederationRouteTargets(cfg, "legacy")
	require.NoError(t, err)
	assert.Equal(t, []string{
		direct,
		relay + "/p2p-circuit/p2p/" + remoteID.String(),
	}, targets)
}

func TestLegacyFederationRoutesRejectConflictingPeerIDs(t *testing.T) {
	firstPriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	firstID, err := peer.IDFromPrivateKey(firstPriv)
	require.NoError(t, err)
	secondPriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	secondID, err := peer.IDFromPrivateKey(secondPriv)
	require.NoError(t, err)
	cfg := FederationConfig{P2PPeers: map[string][]string{"legacy": {
		"/ip4/192.168.50.10/tcp/1/p2p/" + firstID.String(),
		"/ip4/192.168.50.11/tcp/1/p2p/" + secondID.String(),
	}}}
	_, err = configuredFederationRouteTargets(cfg, "legacy")
	require.ErrorContains(t, err, "different peer ids")
}

func TestLegacyFederationRoutesRejectInvalidMultiaddr(t *testing.T) {
	cfg := FederationConfig{P2PPeers: map[string][]string{"legacy": {"not-a-multiaddr"}}}
	_, err := configuredFederationRouteTargets(cfg, "legacy")
	require.ErrorContains(t, err, "invalid peer route")
}

func TestConfiguredFederationRoutesRejectUnsafeDirectTargets(t *testing.T) {
	priv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	for _, target := range []string{
		"/dns4/localhost/tcp/4001/p2p/" + id.String(),
		"/ip4/127.0.0.1/tcp/4001/p2p/" + id.String(),
		"/ip6/fe80::1/tcp/4001/p2p/" + id.String(),
	} {
		cfg := FederationConfig{P2PPeers: map[string][]string{"legacy": {target}}}
		_, err = configuredFederationRouteTargets(cfg, "legacy")
		require.ErrorContains(t, err, "unsafe direct")
	}
}

func TestCurrentSnapshotIgnoresInvalidLegacyMirrorAndSynthesizesRelay(t *testing.T) {
	remotePriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	remoteID, err := peer.IDFromPrivateKey(remotePriv)
	require.NoError(t, err)
	relayPriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	relayID, err := peer.IDFromPrivateKey(relayPriv)
	require.NoError(t, err)
	now := time.Now()
	direct := "/ip4/10.0.0.5/tcp/4001/p2p/" + remoteID.String()
	relay := "/ip4/198.51.100.20/tcp/4001/p2p/" + relayID.String()
	cfg := FederationConfig{
		P2PPeers:      map[string][]string{"peer": {"invalid-legacy-mirror"}},
		P2PRelayAddrs: []string{relay},
		P2PRoutes: map[string]FederationRouteSnapshot{"peer": {
			PeerID: remoteID.String(), Protocol: string(sagep2p.FederationProtocol),
			Addrs: []string{direct}, Revision: 2, IssuedAt: now.Unix(),
			ExpiresAt: now.Add(time.Hour).Unix(), Generation: "generation",
		}},
	}
	targets, err := configuredFederationRouteTargets(cfg, "peer")
	require.NoError(t, err)
	assert.Equal(t, []string{direct, relay + "/p2p-circuit/p2p/" + remoteID.String()}, targets)
}

func TestConfiguredFederationRouteTargetsRejectsSnapshotPeerAndProtocolConfusion(t *testing.T) {
	firstPriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	firstID, err := peer.IDFromPrivateKey(firstPriv)
	require.NoError(t, err)
	secondPriv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	secondID, err := peer.IDFromPrivateKey(secondPriv)
	require.NoError(t, err)
	now := time.Now()
	target := "/ip4/10.0.0.5/tcp/4001/p2p/" + firstID.String()
	base := FederationRouteSnapshot{
		PeerID: secondID.String(), Protocol: string(sagep2p.FederationProtocol),
		Addrs: []string{target}, Revision: 2, IssuedAt: now.Unix(),
		ExpiresAt: now.Add(time.Hour).Unix(), Generation: "generation",
	}
	cfg := FederationConfig{P2PRoutes: map[string]FederationRouteSnapshot{"peer": base}}
	_, err = configuredFederationRouteTargets(cfg, "peer")
	require.ErrorContains(t, err, "peer id does not match")
	base.PeerID = firstID.String()
	base.Protocol = "/other/protocol/1.0.0"
	cfg.P2PRoutes["peer"] = base
	_, err = configuredFederationRouteTargets(cfg, "peer")
	require.ErrorContains(t, err, "protocol is unsupported")
}

func TestWatchFederationRouteChangesRefreshesWhenRelayAppears(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var relayReady atomic.Bool
	started := make(chan struct{})
	var startedOnce atomic.Bool
	local := func() (federation.JoinP2PBundle, error) {
		if startedOnce.CompareAndSwap(false, true) {
			close(started)
		}
		addrs := []string{"/ip4/192.168.1.25/tcp/4001/p2p/peer"}
		if relayReady.Load() {
			addrs = append(addrs, "/ip4/198.51.100.2/tcp/4001/p2p/relay/p2p-circuit/p2p/peer")
		}
		return federation.JoinP2PBundle{PeerID: "peer", Protocol: "/sage/fed/1.0.0", Addrs: addrs}, nil
	}
	refreshed := make(chan struct{}, 1)
	go watchFederationRouteChanges(ctx, 5*time.Millisecond, local, func() {
		select {
		case refreshed <- struct{}{}:
		default:
		}
	})
	<-started
	relayReady.Store(true)
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("relay-ready address change did not trigger route publication")
	}
}
