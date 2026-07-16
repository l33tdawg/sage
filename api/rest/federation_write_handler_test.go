package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
)

// unavailableFederationWriter proves that an injected implementation cannot
// accidentally revive the reserved route before consensus defines a
// connection-bound ingress capability.
type unavailableFederationWriter struct {
	*fakeFederation
	calls int
}

func (f *unavailableFederationWriter) WritePeer(context.Context, string, *federation.RemoteWriteRequest) (*federation.RemoteWriteResult, error) {
	f.calls++
	return &federation.RemoteWriteResult{StatusCode: http.StatusCreated}, nil
}

func TestCrossFedWriteIsOperatorOnlyAndAlwaysUnavailable(t *testing.T) {
	server, _, _ := newTestServer(t, "")
	writer := &unavailableFederationWriter{fakeFederation: &fakeFederation{}}
	server.SetFederation(writer)

	body := []byte(`{"headers":{"x_agent_id":"preview"},"body":"e30="}`)
	operatorRequest, operatorID := signedRequest(t, http.MethodPost, "/v1/federation/cross/chain-peer/write", body)
	server.SetNodeOperatorID(operatorID)
	operatorResponse := httptest.NewRecorder()
	server.Router().ServeHTTP(operatorResponse, operatorRequest)
	if operatorResponse.Code != http.StatusNotImplemented || writer.calls != 0 {
		t.Fatalf("operator response=%d calls=%d body=%s", operatorResponse.Code, writer.calls, operatorResponse.Body.String())
	}
	if !strings.Contains(operatorResponse.Body.String(), "consensus-bound ingress capability") {
		t.Fatalf("unavailable response did not explain the trust-bound requirement: %s", operatorResponse.Body.String())
	}

	nonOperatorRequest, _ := signedRequest(t, http.MethodPost, "/v1/federation/cross/chain-peer/write", body)
	server.SetNodeOperatorID(strings.Repeat("0", 64))
	nonOperatorResponse := httptest.NewRecorder()
	server.Router().ServeHTTP(nonOperatorResponse, nonOperatorRequest)
	if nonOperatorResponse.Code != http.StatusForbidden || writer.calls != 0 {
		t.Fatalf("non-operator response=%d calls=%d", nonOperatorResponse.Code, writer.calls)
	}
}

func TestCrossFedWriteDoesNotParseOrDispatchPreviewCredentials(t *testing.T) {
	server, _, _ := newTestServer(t, "")
	writer := &unavailableFederationWriter{fakeFederation: &fakeFederation{}}
	server.SetFederation(writer)

	request, operatorID := signedRequest(t, http.MethodPost, "/v1/federation/cross/chain-peer/write", []byte(`not-json ordinary-access-grant preview`))
	server.SetNodeOperatorID(operatorID)
	response := httptest.NewRecorder()
	server.Router().ServeHTTP(response, request)
	if response.Code != http.StatusNotImplemented || writer.calls != 0 {
		t.Fatalf("malformed preview envelope response=%d calls=%d body=%s", response.Code, writer.calls, response.Body.String())
	}
}
