package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestFedLanEndpointNeverLoopback is the regression for the v11.4.0 LAN-join
// bug: the host baked "localhost:8444" into its join code, so the guest dialed
// its own loopback and got connection refused. The suggested endpoint must
// never be a loopback address, and every candidate must be a well-formed
// https://<ip>:8444 URL.
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
	}
	// On any machine with a LAN/Wi-Fi interface (CI included), we expect at
	// least one candidate; if none, the suggestion is empty (never loopback),
	// which the frontend falls back from — assert only the never-loopback
	// invariant above, and log coverage.
	t.Logf("candidates=%d suggested=%q", len(resp.Candidates), resp.SuggestedEndpoint)
}
