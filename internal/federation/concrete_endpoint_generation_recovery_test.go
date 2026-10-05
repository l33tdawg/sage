package federation

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// R2b regression. R2 fixed the P2P-only shape, where a nulled routeDial left no
// transport at all. The same assumption fails for an agreement paired with a
// CONCRETE endpoint once that host moves: the address is a real one, so nothing
// looks unroutable, but every outbound request dials a machine that no longer
// answers while the peer's own outbound traffic keeps arriving. That is the
// asymmetry operators report as "they can reach us, we cannot reach them", and
// it also blocked the route exchange that is the only thing able to replace the
// stale snapshot — so the pair could not recover on its own and the queue drained
// only during the windows when the old address happened to answer.
//
// What must NOT regress: a protected request still refuses a cross-generation
// route. The exemption is about the exchange path, not about the agreement shape.

// concreteEndpointGenerationFixture federates a -> b over a concrete endpoint
// (deliberately NOT the p2p-only sentinel) and installs a snapshot whose
// generation differs from the one the request requires.
func concreteEndpointGenerationFixture(t *testing.T) (*testChain, *testChain, *atomic.Int32, *atomic.Value) {
	t.Helper()
	a := newTestChain(t, "concrete-a")
	b := newTestChain(t, "concrete-b")
	// Port 1 on loopback refuses immediately, so a test about WHICH transports
	// were offered never waits on a routing timeout.
	const storedEndpoint = "https://127.0.0.1:1"
	federate(t, a, b, storedEndpoint, nil, 4, 0)
	federate(t, b, a, "https://unused.invalid", nil, 4, 0)

	var dials atomic.Int32
	var sawTargets atomic.Value
	sawTargets.Store([]string{})

	a.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LoadSnapshot: func(string) (RouteSnapshot, bool) {
			return RouteSnapshot{
				PeerID:     "peer-b",
				Addrs:      []string{"/dns4/relay.example/tcp/4001/p2p/peer-b"},
				Generation: "generation-OLD",
				Revision:   7,
			}, true
		},
	})
	a.mgr.SetPeerRouteDialFunc(func(_ context.Context, _ string, frozen []string, _ PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		dials.Add(1)
		sawTargets.Store(append([]string{}, frozen...))
		// Refuse the dial: this test is about whether the route layer is
		// consulted and with which targets, not about completing a handshake.
		return PeerRouteDialResult{}, true, assert.AnError
	})
	return a, b, &dials, &sawTargets
}

// The recovery path, for a peer whose stored address moved.
func TestConcreteEndpointPeerCanAttemptRouteExchangeAcrossGenerations(t *testing.T) {
	a, b, dials, sawTargets := concreteEndpointGenerationFixture(t)

	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = withRouteGeneration(ctx, "generation-NEW")

	_, _, reqErr := a.mgr.doPeerRequest(ctx, agreement, http.MethodPost, p2pRoutesExchangePath, map[string]any{})
	require.Error(t, reqErr, "the stub dialer refuses, so the request must fail")

	require.Equal(t, int32(1), dials.Load(),
		"the route exchange was never offered a transport: a concrete-endpoint peer whose address moved "+
			"cannot learn the current one, so it stays unreachable until re-paired")

	targets, _ := sawTargets.Load().([]string)
	assert.Equal(t, []string{"/dns4/relay.example/tcp/4001/p2p/peer-b"}, targets,
		"the exchange must receive the stale snapshot as a bootstrap hint")
}

// The rule that must survive: a protected request still refuses a
// cross-generation route, so the stale snapshot never enters normal traffic.
func TestConcreteEndpointProtectedRequestStillRefusesCrossGenerationRoute(t *testing.T) {
	a, b, dials, _ := concreteEndpointGenerationFixture(t)

	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = withRouteGeneration(ctx, "generation-NEW")

	_, _, reqErr := a.mgr.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/query", map[string]any{})
	require.Error(t, reqErr)

	assert.Equal(t, int32(0), dials.Load(),
		"a protected request used a route learned under a different trust generation")
}

// A moved host must not read as an offline one. When the fallback was withheld,
// the failure has to say so: the operator's next move is a route repair or a
// re-pair, not a network investigation, and the peer's own traffic may still be
// arriving the whole time.
func TestConcreteEndpointGenerationMismatchReportsTrustGenerationNotOffline(t *testing.T) {
	a, b, _, _ := concreteEndpointGenerationFixture(t)

	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = withRouteGeneration(ctx, "generation-NEW")

	_, _, reqErr := a.mgr.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/query", map[string]any{})
	require.Error(t, reqErr)

	assert.Equal(t, RouteRecoveryTrustGenerationMismatch, RouteRecoveryFailureCode(reqErr),
		"expected a trust-generation recovery code, got: %v", reqErr)
	// Still an offline-class failure for the layers that branch on it, so the
	// outbox keeps retrying rather than treating the row as terminal.
	assert.True(t, errors.Is(reqErr, ErrPeerOffline),
		"the recovery verdict must not lose the offline classification: %v", reqErr)
}

// A matching-generation snapshot is unaffected: it still pins the exact frozen
// target set, exchange path or not.
func TestConcreteEndpointMatchingGenerationStillPinsFrozenTargets(t *testing.T) {
	a := newTestChain(t, "concrete-match-a")
	b := newTestChain(t, "concrete-match-b")
	federate(t, a, b, "https://127.0.0.1:1", nil, 4, 0)

	var sawTargets atomic.Value
	sawTargets.Store([]string{})
	a.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LoadSnapshot: func(string) (RouteSnapshot, bool) {
			return RouteSnapshot{
				PeerID:     "peer-b",
				Addrs:      []string{"/dns4/current.example/tcp/4001/p2p/peer-b"},
				Generation: "generation-NEW",
				Revision:   8,
			}, true
		},
	})
	a.mgr.SetPeerRouteDialFunc(func(_ context.Context, _ string, frozen []string, _ PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
		sawTargets.Store(append([]string{}, frozen...))
		return PeerRouteDialResult{}, true, assert.AnError
	})

	agreement, err := a.mgr.ActiveAgreement(b.chainID)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = withRouteGeneration(ctx, "generation-NEW")

	_, _, _ = a.mgr.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/query", map[string]any{})

	targets, _ := sawTargets.Load().([]string)
	assert.Equal(t, []string{"/dns4/current.example/tcp/4001/p2p/peer-b"}, targets,
		"a matching-generation snapshot must still pin its exact addresses")
}
