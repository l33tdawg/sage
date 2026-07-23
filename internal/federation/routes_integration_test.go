package federation

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startCountedFederationServer(t *testing.T, chain *testChain, requests *atomic.Int32) *httptest.Server {
	t.Helper()
	tlsCfg, err := chain.mgr.ServerTLSConfig()
	require.NoError(t, err)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		chain.mgr.Router().ServeHTTP(w, r)
	})
	server := httptest.NewUnstartedServer(handler)
	server.TLS = tlsCfg
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func dialTestServer(ctx context.Context, rawURL string) (net.Conn, error) {
	address := strings.TrimPrefix(rawURL, "https://")
	return (&net.Dialer{}).DialContext(ctx, "tcp", address)
}

func TestMutatingPeerRequestWritesOnceAfterRouteRace(t *testing.T) {
	a := newTestChain(t, "route-write-a")
	b := newTestChain(t, "route-write-b")
	var received atomic.Int32
	server := startCountedFederationServer(t, b, &received)
	// The HTTPS candidate is intentionally unreachable. The P2P seam connects
	// to the same authenticated mTLS listener after the direct head start.
	federate(t, a, b, "https://192.0.2.1:44444", nil, 4, 0)
	federate(t, b, a, "https://unused.invalid", nil, 4, 0)
	var p2pDials atomic.Int32
	a.mgr.SetPeerRouteDialFunc(func(ctx context.Context, chain string, authenticate PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		p2pDials.Add(1)
		started := time.Now()
		conn, err := dialTestServer(ctx, server.URL)
		result, err := authenticate(ctx, PeerRouteDialResult{
			Conn: conn, Kind: RouteKindRelay, Target: "test-relay", Latency: time.Since(started),
		}, err)
		return result, true, err
	})
	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	_, status, err := a.mgr.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/write", map[string]any{"mutation": "exactly-once"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotImplemented, status)
	assert.Equal(t, int32(1), p2pDials.Load())
	assert.Equal(t, int32(1), received.Load(), "route racing must not replay a mutating HTTP request")
	assert.Equal(t, RouteKindRelay, a.mgr.RouteDiagnostics(b.chainID).State)
}

func TestTLSSecurityFailureFallsBackBeforeRequestWrite(t *testing.T) {
	a := newTestChain(t, "route-security-a")
	b := newTestChain(t, "route-security-b")
	var validRequests atomic.Int32
	valid := startCountedFederationServer(t, b, &validRequests)

	// This direct candidate accepts TCP but presents an unrelated certificate.
	// It is rejected before winner selection, so the otherwise-valid pinned P2P
	// route can win without replaying any HTTP request.
	bad := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	bad.Config.ErrorLog = log.New(io.Discard, "", 0)
	bad.StartTLS()
	t.Cleanup(bad.Close)
	federate(t, a, b, bad.URL, nil, 4, 0)
	federate(t, b, a, "https://unused.invalid", nil, 4, 0)
	var p2pDials atomic.Int32
	a.mgr.SetPeerRouteDialFunc(func(ctx context.Context, chain string, authenticate PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		p2pDials.Add(1)
		conn, err := dialTestServer(ctx, valid.URL)
		result, err := authenticate(ctx, PeerRouteDialResult{Conn: conn, Kind: RouteKindRelay, Target: "valid-relay"}, err)
		return result, true, err
	})
	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, status, err := a.mgr.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/write", map[string]any{"must": "not-replay"})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotImplemented, status)
	assert.Equal(t, int32(1), p2pDials.Load())
	assert.Equal(t, int32(1), validRequests.Load(), "TLS candidate fallback must still write the HTTP request once")
	assert.Equal(t, RouteKindRelay, a.mgr.RouteDiagnostics(b.chainID).State)
}
