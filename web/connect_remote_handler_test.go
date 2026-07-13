package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnectRemoteTokenMintsTransportNeutralBearer(t *testing.T) {
	h, _ := newTestHandler(t)
	h.NodeOperatorAgentID = strings.Repeat("a", 64)
	router := testRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/connect/remote/token", bytes.NewBufferString(`{"token_name":"vpn-laptop"}`))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(req)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", w.Code, w.Body.String())
	}
	var got struct {
		Name  string `json:"name"`
		Token string `json:"token"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "vpn-laptop" || got.Token == "" {
		t.Fatalf("unexpected token response: %#v", got)
	}
}

func TestParseMCPTLSAddr(t *testing.T) {
	cases := []struct {
		in       string
		wantHost string
		wantPort int
		wantOK   bool
	}{
		{"127.0.0.1:8443", "127.0.0.1", 8443, true},
		{"0.0.0.0:8443", "0.0.0.0", 8443, true},
		{"[::]:8443", "::", 8443, true},
		{"192.168.1.5:9000", "192.168.1.5", 9000, true},
		{":8443", "", 8443, true}, // bare :port — wildcard bind, must parse OK
		{"", "", 0, false},        // unset — not OK
		{"garbage", "", 0, false}, // unparseable — not OK
		{"host:notaport", "host", 0, true},
	}
	for _, c := range cases {
		h, p, ok := parseMCPTLSAddr(c.in)
		if h != c.wantHost || p != c.wantPort || ok != c.wantOK {
			t.Errorf("parseMCPTLSAddr(%q) = (%q,%d,%v), want (%q,%d,%v)", c.in, h, p, ok, c.wantHost, c.wantPort, c.wantOK)
		}
	}
}

func TestMCPBindIsRemote(t *testing.T) {
	// mcpBindIsRemote is only called on a successfully-parsed bind (ok=true),
	// where host=="" means a bare ":port" wildcard → remote.
	cases := []struct {
		host string
		want bool
	}{
		{"", true},            // wildcard bind (bare :port)
		{"0.0.0.0", true},     // wildcard
		{"::", true},          // IPv6 wildcard
		{"127.0.0.1", false},  // explicit loopback
		{"::1", false},        // IPv6 loopback
		{"localhost", false},  // loopback name
		{"192.168.1.5", true}, // explicit LAN IP
		{"10.0.0.2", true},    // explicit private IP
	}
	for _, c := range cases {
		if got := mcpBindIsRemote(c.host); got != c.want {
			t.Errorf("mcpBindIsRemote(%q) = %v, want %v", c.host, got, c.want)
		}
	}
}

func TestIsContainerIface(t *testing.T) {
	skip := []string{
		"docker0", "veth1a2b", "br-0123456789ab", "vmnet8", "vmenet0",
		"vboxnet0", "podman0", "cni0", "flannel.1", "kube-bridge",
		"cali123", "cbr0", "tunl0", "weave", "bridge100", "bridge101", "virbr0",
	}
	for _, n := range skip {
		if !isContainerIface(n) {
			t.Errorf("isContainerIface(%q) = false, want true (should be skipped)", n)
		}
	}
	keep := []string{
		"en0", "eth0", "eth1", "wlan0", "br-lan", "br-wan", "bridge0",
		"utun3", "tun0", "tailscale0", "wg0", "zt5u4", "enp3s0",
	}
	for _, n := range keep {
		if isContainerIface(n) {
			t.Errorf("isContainerIface(%q) = true, want false (should be a candidate)", n)
		}
	}
}

func TestIsOverlayIface(t *testing.T) {
	overlay := []string{"utun3", "tun0", "tap0", "wg0", "tailscale0", "ppp0", "zt5u4"}
	for _, n := range overlay {
		if !isOverlayIface(n) {
			t.Errorf("isOverlayIface(%q) = false, want true", n)
		}
	}
	physical := []string{"en0", "eth0", "wlan0", "br-lan", "enp3s0", "bridge0"}
	for _, n := range physical {
		if isOverlayIface(n) {
			t.Errorf("isOverlayIface(%q) = true, want false", n)
		}
	}
}
