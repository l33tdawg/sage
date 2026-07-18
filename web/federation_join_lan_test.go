package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/netguard"
)

// TestFedLanEndpointNeverLoopback is the regression for the v11.4.0 LAN-join
// bug: the host baked "localhost:8444" into its join code, so the guest dialed
// its own loopback and got connection refused. The suggested endpoint must
// never be a loopback address, and every candidate must be a well-formed
// https://<ip>:8444 URL when the handler uses the default listener.
func TestFedLanEndpointNeverLoopback(t *testing.T) {
	h := &DashboardHandler{}
	rr := httptest.NewRecorder()
	h.handleFedLanEndpoint(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/lan-endpoint", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		Port              int    `json:"port"`
		SuggestedEndpoint string `json:"suggested_endpoint"`
		Candidates        []struct {
			Endpoint  string `json:"endpoint"`
			IP        string `json:"ip"`
			IsPrivate bool   `json:"is_private"`
		} `json:"candidates"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Port != fedDefaultPort {
		t.Errorf("port = %d, want %d", resp.Port, fedDefaultPort)
	}

	loopback := func(s string) bool {
		s = strings.ToLower(s)
		return strings.Contains(s, "localhost") || strings.Contains(s, "127.0.0.1") || strings.Contains(s, "::1")
	}
	if loopback(resp.SuggestedEndpoint) {
		t.Errorf("suggested endpoint is loopback: %q", resp.SuggestedEndpoint)
	}
	for _, c := range resp.Candidates {
		if loopback(c.Endpoint) {
			t.Errorf("candidate is loopback: %q", c.Endpoint)
		}
		if !strings.HasPrefix(c.Endpoint, "https://") || !strings.HasSuffix(c.Endpoint, ":8444") {
			t.Errorf("malformed candidate endpoint: %q", c.Endpoint)
		}
		if _, err := netguard.LocalLANHTTPBase(c.Endpoint, "https"); err != nil {
			t.Errorf("discovery emitted a JOIN-rejected candidate %q: %v", c.Endpoint, err)
		}
	}
	// On any machine with a LAN/Wi-Fi interface (CI included), we expect at
	// least one candidate; if none, the suggestion is empty (never loopback),
	// which the frontend falls back from — assert only the never-loopback
	// invariant above, and log coverage.
	t.Logf("candidates=%d suggested=%q", len(resp.Candidates), resp.SuggestedEndpoint)
}

func TestFedLanEndpointDoesNotAdvertisePublicOrOverlayBindAsLAN(t *testing.T) {
	for _, addr := range []string{"100.64.1.2:18444", "203.0.113.10:18444"} {
		h := &DashboardHandler{FederationAddr: addr}
		rr := httptest.NewRecorder()
		h.handleFedLanEndpoint(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/lan-endpoint", nil))
		var resp struct {
			SuggestedEndpoint string            `json:"suggested_endpoint"`
			Candidates        []FedLanCandidate `json:"candidates"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode %q: %v", addr, err)
		}
		if resp.SuggestedEndpoint != "" || len(resp.Candidates) != 0 {
			t.Errorf("LAN discovery advertised %q as %+v", addr, resp)
		}
	}
}

func TestFedLanEndpointUsesConfiguredFederationListenerPort(t *testing.T) {
	h := &DashboardHandler{FederationAddr: "0.0.0.0:18444"}
	rr := httptest.NewRecorder()
	h.handleFedLanEndpoint(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/lan-endpoint", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp struct {
		Port              int               `json:"port"`
		SuggestedEndpoint string            `json:"suggested_endpoint"`
		Candidates        []FedLanCandidate `json:"candidates"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Port != 18444 {
		t.Fatalf("port = %d, want configured federation port 18444", resp.Port)
	}
	if resp.SuggestedEndpoint != "" && !strings.HasSuffix(resp.SuggestedEndpoint, ":18444") {
		t.Errorf("suggested endpoint = %q, want configured port", resp.SuggestedEndpoint)
	}
	for _, c := range resp.Candidates {
		if !strings.HasSuffix(c.Endpoint, ":18444") {
			t.Errorf("candidate endpoint = %q, want configured port", c.Endpoint)
		}
	}
}

func TestPrivateFederationDashboardCandidate(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/lan-endpoint", nil)
	request.Host = "192.168.44.20:8080"
	candidate, ok := privateFederationDashboardCandidate(request, 18444)
	if !ok {
		t.Fatal("private dashboard host was not accepted as a wildcard-listener fallback")
	}
	if candidate.Endpoint != "https://192.168.44.20:18444" {
		t.Fatalf("candidate endpoint = %q, want %q", candidate.Endpoint, "https://192.168.44.20:18444")
	}

	request.Host = "203.0.113.20:8080"
	if _, ok := privateFederationDashboardCandidate(request, 18444); ok {
		t.Fatal("public dashboard host must not become an automatic federation candidate")
	}
}

func TestFedLanEndpointDoesNotUseDashboardHostForExplicitPublicListener(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/lan-endpoint", nil)
	request.Host = "192.168.44.20:8080"
	h := &DashboardHandler{FederationAddr: "203.0.113.10:18444"}
	rr := httptest.NewRecorder()
	h.handleFedLanEndpoint(rr, request)
	var resp struct {
		SuggestedEndpoint string `json:"suggested_endpoint"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.SuggestedEndpoint != "" {
		t.Fatalf("suggested endpoint = %q, want empty for explicit public listener", resp.SuggestedEndpoint)
	}
}

func TestFederationListenerPortFallsBackSafely(t *testing.T) {
	for _, addr := range []string{"", "not-an-address", "0.0.0.0:0", "0.0.0.0:99999"} {
		h := &DashboardHandler{FederationAddr: addr}
		_, got := h.federationListenerBind()
		if got != fedDefaultPort {
			t.Errorf("federationListenerBind(%q) port = %d, want %d", addr, got, fedDefaultPort)
		}
	}
}

func TestFedLanEndpointHonorsExplicitListenerHost(t *testing.T) {
	for _, tc := range []struct {
		addr string
		want string
	}{
		{"127.0.0.1:19444", "https://127.0.0.1:19444"},
		{"[::1]:20444", "https://[::1]:20444"},
		{"10.20.30.40:21444", "https://10.20.30.40:21444"},
	} {
		h := &DashboardHandler{FederationAddr: tc.addr}
		rr := httptest.NewRecorder()
		h.handleFedLanEndpoint(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/lan-endpoint", nil))
		var resp struct {
			SuggestedEndpoint string            `json:"suggested_endpoint"`
			Candidates        []FedLanCandidate `json:"candidates"`
		}
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode %q: %v", tc.addr, err)
		}
		if resp.SuggestedEndpoint != tc.want {
			t.Errorf("suggested endpoint for %q = %q, want %q", tc.addr, resp.SuggestedEndpoint, tc.want)
		}
		if len(resp.Candidates) != 1 || resp.Candidates[0].Endpoint != tc.want {
			t.Errorf("candidates for %q = %+v, want exact configured listener", tc.addr, resp.Candidates)
		}
	}
}
