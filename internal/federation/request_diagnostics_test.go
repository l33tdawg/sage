package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func TestPeerRequestDiagnosticRedactsSecretsAndPreservesTypedCause(t *testing.T) {
	ctx, snapshot := WithPeerRouteAttemptTrace(context.Background())
	complete := BeginPeerRouteAttempt(ctx, RouteKindRelay, "https://candidate-user:candidate-pass@relay.example:443/route?candidate-token=secret#fragment")
	complete(context.DeadlineExceeded)
	cause := &url.Error{Op: "Get", URL: "https://user:password@peer.example/status?token=query-secret", Err: context.DeadlineExceeded}
	err := newPeerRequestFailure(ctx, "https://user:password@peer.example:8443", http.MethodGet,
		"/fed/v1/status?token=query-secret#fragment", RouteRecoveryTimeout, 0, cause)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var typed *url.Error
	require.ErrorAs(t, err, &typed)
	require.Same(t, cause, typed)
	for _, secret := range []string{"password", "query-secret", "candidate-user", "candidate-pass", "candidate-token", "fragment"} {
		require.NotContains(t, err.Error(), secret)
	}
	require.Equal(t, "https://relay.example:443/route", snapshot()[0].Target)
	require.Contains(t, err.Error(), "transport_verdict=timeout")
	require.Contains(t, err.Error(), "context deadline exceeded")
	require.NotContains(t, err.Error(), "Retained work will retry", "ordinary requests do not imply a durable delivery event")
	require.Equal(t, RouteRecoveryTimeout, RouteRecoveryFailureCode(classifyRouteRecoveryError(err, RouteRecoveryRelayUnavailable)))
	diagnostic := PeerRequestFailureDiagnostic(err)
	require.NotNil(t, diagnostic)
	require.Equal(t, "https://peer.example:8443/fed/v1/status", diagnostic.Endpoint)
	diagnostic.Candidates[0].Target = "caller mutation"
	require.NotEqual(t, diagnostic.Candidates[0].Target, PeerRequestFailureDiagnostic(err).Candidates[0].Target)
}

func TestPeerRouteAttemptTraceBoundsAndCopiesLateCompletions(t *testing.T) {
	ctx, snapshot := WithPeerRouteAttemptTrace(context.Background())
	finish := make([]func(error), 0, 20)
	for i := range 20 {
		finish = append(finish, BeginPeerRouteAttempt(ctx, RouteKindP2PDirect, fmt.Sprintf("/dns4/peer-%d/tcp/443/p2p/id", i)))
	}
	before := snapshot()
	require.Len(t, before, maxPeerRouteAttemptDiagnostics)
	for _, complete := range finish {
		complete(context.DeadlineExceeded)
	}
	require.Equal(t, "started", before[0].Verdict, "a returned snapshot must not change after a late candidate completes")
	require.Equal(t, RouteRecoveryTimeout, snapshot()[0].Verdict)
}

func TestPeerRequestDiagnosticPreservesLegacyDialerRecoveryVerdicts(t *testing.T) {
	for _, tc := range []struct{ message, verdict string }{
		{"context deadline exceeded", RouteRecoveryTimeout},
		{"connection reset by peer", RouteRecoveryHandshakeFailed},
		{"EOF", RouteRecoveryHandshakeFailed},
	} {
		t.Run(tc.message, func(t *testing.T) {
			cause := fmt.Errorf("%w: p2p dial: %s", ErrPeerOffline, tc.message)
			failure := newPeerRequestFailure(context.Background(), "https://peer.example", http.MethodGet,
				"/fed/v1/status", peerTransportVerdict(cause), 0, cause)
			require.ErrorIs(t, failure, ErrPeerOffline)
			require.Equal(t, tc.verdict, PeerRequestFailureDiagnostic(failure).Verdict)
			require.Equal(t, tc.verdict, RouteRecoveryFailureCode(classifyRouteRecoveryError(failure, RouteRecoveryRelayUnavailable)))
		})
	}
}

func TestPeerStatusFailuresReportKnownEndpointWithoutPeerBodies(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		verdict string
	}{
		{"authorization", http.StatusForbidden, "peer-body-secret", "http_failure"},
		{"server failure", http.StatusServiceUnavailable, "peer-body-secret", "http_failure"},
		{"malformed response", http.StatusOK, "peer-body-secret", "invalid_response"},
		{"wrong identity", http.StatusOK, `{"chain_id":"peer-body-secret"}`, "peer_identity_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := newTestChain(t, "diag-source"), newTestChain(t, "diag-peer")
			tlsConfig, err := b.mgr.ServerTLSConfig()
			require.NoError(t, err)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/fed/v1/status", r.URL.Path)
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			server.TLS = tlsConfig
			server.StartTLS()
			t.Cleanup(server.Close)
			federate(t, a, b, server.URL, nil, 4, 0)
			federate(t, b, a, "https://unused.invalid", nil, 4, 0)
			_, err = a.mgr.PeerStatus(context.Background(), b.chainID)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "peer-body-secret")
			require.NotContains(t, err.Error(), "retained work will retry")
			diagnostic := PeerRequestFailureDiagnostic(err)
			require.NotNil(t, diagnostic)
			require.Equal(t, server.URL+"/fed/v1/status", diagnostic.Endpoint)
			require.Equal(t, tc.verdict, diagnostic.Verdict)
			require.Equal(t, tc.status, diagnostic.HTTPStatus)
			require.Len(t, diagnostic.Candidates, 1)
			require.Equal(t, "connected", diagnostic.Candidates[0].Verdict)
			route := a.mgr.RouteDiagnostics(b.chainID)
			require.Equal(t, RouteKindDirect, route.State, "an HTTP/status failure must not relabel its connected transport as offline")
			require.Equal(t, diagnostic, route.RequestFailure)
			require.NotContains(t, route.LastError, "peer-body-secret")
			if tc.status == http.StatusForbidden {
				require.Equal(t, RouteRecoverySecurityBlocked, RouteRecoveryFailureCode(classifyRouteRecoveryError(err, "")))
			}
		})
	}
}

func TestKnownPeerResultPreflightTimeoutRetainsReplyWithEndpointAndRemedy(t *testing.T) {
	a, b := newTestChain(t, "diag-reply-source"), newTestChain(t, "diag-reply-peer")
	tlsConfig, err := b.mgr.ServerTLSConfig()
	require.NoError(t, err)
	statusCalled := make(chan struct{}, 1)
	releaseStatus := make(chan struct{})
	defer close(releaseStatus)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fed/v1/status" {
			statusCalled <- struct{}{}
			select {
			case <-r.Context().Done():
			case <-releaseStatus:
			}
			return
		}
		t.Errorf("preflight must not push reply bytes to %s", r.URL.Path)
	}))
	server.TLS = tlsConfig
	server.StartTLS()
	t.Cleanup(server.Close)
	federate(t, a, b, server.URL, nil, 4, 0)
	federate(t, b, a, "https://unused.invalid", nil, 4, 0)
	activatePipePeer(t, a, b, "host")
	ss := pipeSQLite(t, a)
	control, err := ss.GetSyncControl(context.Background(), b.chainID)
	require.NoError(t, err)
	now := time.Now().UTC()
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	recipient := hex.EncodeToString(pub)
	const reply = "retained reply secret"
	msg := &store.PipelineMessage{
		PipeID: "diag-reply", FromAgent: newPeerOperatorID(t), ToAgent: recipient, SourceChainID: b.chainID,
		SourcePipeID: "remote-original", FederationPolicyEpoch: control.PolicyEpoch,
		FederationAgreementID: "agreement", FederationContactID: "contact", FederationContactRevision: "revision",
		Payload: "retained request", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, ss.InsertPipeline(context.Background(), msg))
	require.NoError(t, ss.ClaimPipeline(context.Background(), msg.PipeID, recipient))
	proof := signedPipeProof(t, priv, recipient, http.MethodPut, "/v1/pipe/"+msg.PipeID+"/result", []byte(`{"result":"retained reply secret"}`), now.Unix())
	outbox := &store.PipelineTransportOutbox{
		EventID: "diag-result-event", PipeID: msg.PipeID, RemoteChainID: b.chainID,
		EventKind: "result", PolicyEpoch: control.PolicyEpoch, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		SourceAgentID: recipient, TargetAgentID: msg.FromAgent, Proof: proof,
	}
	require.NoError(t, ss.CompleteFederatedPipelineWithTransport(context.Background(), msg.PipeID, recipient, reply, outbox))
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	err = a.mgr.preflightPipelineResultPeer(ctx, msg, outbox)
	require.ErrorIs(t, err, ErrRemotePipeResolutionIncomplete)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-statusCalled:
	default:
		t.Fatal("the recorded peer status endpoint was never attempted")
	}
	require.Contains(t, err.Error(), server.URL+"/fed/v1/status")
	require.Contains(t, err.Error(), "transport_verdict=timeout")
	require.Contains(t, err.Error(), "connection Retry")
	require.NotContains(t, err.Error(), "retained reply secret")
	a.mgr.recordPipelineDeliveryError(ss, outbox, err, false, 0)
	retained, err := ss.GetPipelineTransport(context.Background(), outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, "pending", retained.State)
	require.Contains(t, retained.LastError, server.URL+"/fed/v1/status")
	require.True(t, strings.Contains(retained.LastError, "candidate=direct"))
	record, err := ss.GetPipeline(context.Background(), msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, reply, record.Result)
}
