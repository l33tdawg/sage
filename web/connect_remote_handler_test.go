package web

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestTunnelHostnameFromConfig(t *testing.T) {
	dir := t.TempDir()
	// Point the in-package cloudflared home at our temp dir (test-only seam).
	prev := cloudflaredHomeOverride
	cloudflaredHomeOverride = dir
	t.Cleanup(func() { cloudflaredHomeOverride = prev })

	// No config file yet → empty.
	if got := tunnelHostnameFromConfig(); got != "" {
		t.Fatalf("no config: got %q, want empty", got)
	}

	// Config with a hostname (plus an inline comment to exercise stripping).
	cfg := `tunnel: 328de5a1-00dd-4326-b38a-b0763781ccb6
credentials-file: /home/x/.cloudflared/tunnel.json

ingress:
  - hostname: sage.example.com  # the MCP surface
    path: ^/v1/mcp/.*$
    service: https://localhost:8443
  - hostname: sage.example.com
    path: ^/oauth/.*$
    service: https://localhost:8443
  - service: http_status:404
`
	if err := os.WriteFile(filepath.Join(dir, "config.yml"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := tunnelHostnameFromConfig(); got != "sage.example.com" {
		t.Fatalf("got %q, want sage.example.com", got)
	}

	// A config with no ingress hostname → empty.
	if err := os.WriteFile(filepath.Join(dir, "config.yml"), []byte("tunnel: x\ningress:\n  - service: http_status:404\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := tunnelHostnameFromConfig(); got != "" {
		t.Fatalf("no-hostname config: got %q, want empty", got)
	}
}
