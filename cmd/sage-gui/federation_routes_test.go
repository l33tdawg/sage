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

func TestExpiredPersistedFederationRouteIsPrunedAtBoot(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("SAGE_HOME", tmp)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "config.yaml"),
		[]byte("federation:\n  enabled: true\n  p2p_enabled: true\n"), 0o600))
	priv, _, err := libcrypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	target := "/ip4/203.0.113.10/tcp/4001/p2p/" + id.String()
	now := time.Now()
	require.NoError(t, persistFederationRouteSnapshot("expired-chain", federation.RouteSnapshot{
		PeerID: id.String(), Protocol: string(sagep2p.FederationProtocol),
		Addrs: []string{target}, Revision: 8, IssuedAt: now.Add(-2 * time.Hour).Unix(),
		ExpiresAt: now.Add(-time.Hour).Unix(), Generation: "generation",
	}))
	cfg, err := LoadConfig()
	require.NoError(t, err)
	_, err = configuredFederationRouteTargets(cfg.Federation, "expired-chain", now)
	require.ErrorContains(t, err, "expired")
	expired := pruneExpiredFederationRoutes(&cfg.Federation, now)
	require.Equal(t, []string{"expired-chain"}, expired)
	for _, chain := range expired {
		require.NoError(t, persistFederationPeer(chain, nil))
	}
	reloaded, err := LoadConfig()
	require.NoError(t, err)
	assert.NotContains(t, reloaded.Federation.P2PPeers, "expired-chain")
	assert.NotContains(t, reloaded.Federation.P2PRoutes, "expired-chain")
}

func TestConfiguredFederationRouteTargetsUsesCurrentSnapshotNotLegacyMirror(t *testing.T) {
	now := time.Now()
	cfg := FederationConfig{
		P2PPeers: map[string][]string{"peer": {"stale-mirror"}},
		P2PRoutes: map[string]FederationRouteSnapshot{"peer": {
			Addrs: []string{"current"}, Revision: 2, IssuedAt: now.Add(-time.Minute).Unix(),
			ExpiresAt: now.Add(time.Hour).Unix(), Generation: "generation",
		}},
	}
	targets, err := configuredFederationRouteTargets(cfg, "peer", now)
	require.NoError(t, err)
	assert.Equal(t, []string{"current"}, targets)
}
