package rest

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
)

type notifyingRevokeFederation struct {
	*fakeFederation
	called string
	result *federation.RevokeAgreementResult
}

func (f *notifyingRevokeFederation) RevokeAgreementNotifying(chain string) (*federation.RevokeAgreementResult, error) {
	f.called = chain
	return f.result, nil
}

func legacyFederationControlRouter(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Post("/v1/federation/cross", s.handleCrossFedSet)
	r.Get("/v1/federation/cross", s.handleCrossFedList)
	r.Post("/v1/federation/cross/{chain_id}/revoke", s.handleCrossFedRevoke)
	return r
}

func TestLegacyFederationControlRequiresExactNodeOperator(t *testing.T) {
	_, signingKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{logger: zerolog.Nop(), nodeOperatorID: "node-operator", signingKey: signingKey}

	routes := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/v1/federation/cross"},
		{method: http.MethodGet, path: "/v1/federation/cross"},
		{method: http.MethodPost, path: "/v1/federation/cross/chain-peer/revoke"},
	}

	for _, route := range routes {
		req := httptest.NewRequest(route.method, route.path, nil)
		rr := httptest.NewRecorder()
		legacyFederationControlRouter(s, "ordinary-signed-agent").ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("nonoperator %s %s status=%d body=%s", route.method, route.path, rr.Code, rr.Body.String())
		}
	}

	// The exact operator crosses the gate. The deliberately unwired fixture then
	// fails at the next dependency/validation guard rather than returning 403.
	for _, route := range routes {
		req := httptest.NewRequest(route.method, route.path, nil)
		rr := httptest.NewRecorder()
		legacyFederationControlRouter(s, "node-operator").ServeHTTP(rr, req)
		if rr.Code == http.StatusForbidden {
			t.Fatalf("operator did not cross federation-control gate for %s %s", route.method, route.path)
		}
	}
}

func TestRESTFederationRevokeUsesPeerNotificationWorkflowWhenAvailable(t *testing.T) {
	driver := &notifyingRevokeFederation{
		fakeFederation: &fakeFederation{},
		result: &federation.RevokeAgreementResult{
			TxHash: "tx-notify", PeerNotified: false, NoticeError: "peer was offline",
		},
	}
	s := &Server{logger: zerolog.Nop(), nodeOperatorID: "node-operator", federation: driver}
	req := httptest.NewRequest(http.MethodPost, "/v1/federation/cross/chain-peer/revoke", nil)
	rr := httptest.NewRecorder()
	legacyFederationControlRouter(s, "node-operator").ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if driver.called != "chain-peer" {
		t.Fatalf("notifier called with %q", driver.called)
	}
	var body struct {
		Status              string `json:"status"`
		TxHash              string `json:"tx_hash"`
		PeerNotified        bool   `json:"peer_notified"`
		NotificationWarning string `json:"notification_warning"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "revoked" || body.TxHash != "tx-notify" || body.PeerNotified || body.NotificationWarning != "peer was offline" {
		t.Fatalf("unexpected response: %+v", body)
	}
}
