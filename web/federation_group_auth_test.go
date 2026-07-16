package web

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFederationGroupRouteAcceptsOnlyOperatorEd25519Signature(t *testing.T) {
	h, _ := newTestHandler(t)
	operatorPub, operatorKey, _ := ed25519.GenerateKey(nil)
	h.NodeOperatorAgentID = hex.EncodeToString(operatorPub)
	router := testRouter(h)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	signAgentRequest(t, req, operatorKey, nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotImplemented {
		t.Fatalf("valid operator signature did not cross group auth boundary: %d %s", rr.Code, rr.Body.String())
	}

	_, otherKey, _ := ed25519.GenerateKey(nil)
	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	signAgentRequest(t, req, otherKey, nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("nonoperator signed request status=%d", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/groups", nil)
	req.RemoteAddr = "127.0.0.1:43210"
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("unsigned loopback request status=%d", rr.Code)
	}
}

func TestFederationGroupSurfaceRequiresVerifiedNodeOperator(t *testing.T) {
	h := &DashboardHandler{NodeOperatorAgentID: strings.Repeat("a", 64)}
	paths := []struct {
		method, path, body string
	}{
		{http.MethodGet, "/v1/dashboard/federation/groups", ""},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/domains", `{"domain_tag":"hr"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/domains/remove", `{"domain_tag":"hr"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/self-role", `{"role":"full-sync"}`},
		{http.MethodPost, "/v1/dashboard/federation/groups/g1/roster", `{"entry_type":"manifest","payload":{}}`},
	}

	for _, tc := range paths {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			call := func(req *http.Request) int {
				rr := httptest.NewRecorder()
				switch tc.path {
				case "/v1/dashboard/federation/groups":
					h.handleFedGroupList(rr, req)
				case "/v1/dashboard/federation/groups/g1/domains":
					h.handleFedGroupDomainAdd(rr, req)
				case "/v1/dashboard/federation/groups/g1/domains/remove":
					h.handleFedGroupDomainRemove(rr, req)
				case "/v1/dashboard/federation/groups/g1/self-role":
					h.handleFedGroupSelfRole(rr, req)
				default:
					h.handleFedGroupRosterControl(rr, req)
				}
				return rr.Code
			}

			// A verified but non-operator Ed25519 agent is never upgraded to the
			// node's signing identity.
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, strings.Repeat("b", 64)))
			if got := call(req); got != http.StatusForbidden {
				t.Fatalf("nonoperator status=%d", got)
			}

			// Neither loopback browser shape nor a no-Origin local process is an
			// operator credential (including encryption-off nodes).
			for _, browser := range []bool{false, true} {
				req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				req.RemoteAddr = "127.0.0.1:43210"
				if browser {
					req.Host = "localhost:8080"
					req.Header.Set("Origin", "http://localhost:8080")
					req.Header.Set("Sec-Fetch-Site", "same-origin")
				}
				if got := call(req); got != http.StatusForbidden {
					t.Fatalf("unsigned loopback browser=%v status=%d", browser, got)
				}
			}

			// Exact verified operator identity crosses this boundary. Federation is
			// intentionally unwired in the fixture, so the next guard returns 501.
			req = httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, h.NodeOperatorAgentID))
			if got := call(req); got != http.StatusNotImplemented {
				t.Fatalf("operator did not cross auth boundary: status=%d", got)
			}
		})
	}
}

func TestFederationSettingMutationRequiresOperator(t *testing.T) {
	h, _ := newTestHandler(t)
	operatorPub, operatorKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	h.NodeOperatorAgentID = hex.EncodeToString(operatorPub)
	h.FederationEnabled = true
	settingCalls := 0
	restartCalls := 0
	h.SetFederationEnabledFn = func(enabled bool) error {
		settingCalls++
		if enabled {
			t.Fatal("test mutation unexpectedly enabled federation")
		}
		return nil
	}
	h.RequestRestart = func() error {
		restartCalls++
		return nil
	}
	router := testRouter(h)

	_, otherKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/federation", strings.NewReader(`{"enabled":false}`))
	signAgentRequest(t, req, otherKey, []byte(`{"enabled":false}`))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("nonoperator setting mutation status=%d body=%s", rr.Code, rr.Body.String())
	}
	if settingCalls != 0 || restartCalls != 0 || !h.FederationEnabled {
		t.Fatalf("nonoperator reached federation mutation: setting_calls=%d restart_calls=%d enabled=%v", settingCalls, restartCalls, h.FederationEnabled)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/federation", strings.NewReader(`{"enabled":false}`))
	signAgentRequest(t, req, operatorKey, []byte(`{"enabled":false}`))
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	wantStatus := http.StatusAccepted
	if !restartInProcessSupported() {
		wantStatus = http.StatusOK
	}
	if rr.Code != wantStatus {
		t.Fatalf("operator setting mutation status=%d body=%s", rr.Code, rr.Body.String())
	}
	if settingCalls != 1 || h.FederationEnabled {
		t.Fatalf("operator mutation did not persist exactly once: setting_calls=%d enabled=%v", settingCalls, h.FederationEnabled)
	}
	if restartInProcessSupported() && restartCalls != 1 {
		t.Fatalf("operator mutation did not request exactly one restart: restart_calls=%d", restartCalls)
	}
}
