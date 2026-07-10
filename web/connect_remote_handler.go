package web

// Remote-connect discovery (Phase 5b-2) — the backing endpoint for the
// "connect a tool on ANOTHER computer" flow. A tool on a different machine can
// only reach this node two ways:
//
//   1. A public Cloudflare tunnel (the ChatGPT wizard sets this up). This is
//      the clean path: a real TLS cert, a stable hostname, works for hosted
//      chats (ChatGPT via OAuth) and remote IDE clients (Cursor/Windsurf via
//      bearer) alike.
//   2. A direct LAN hit on the MCP TLS port — but ONLY when the node actually
//      binds that port on a non-loopback interface. A personal-mode node binds
//      127.0.0.1:8443 (localhost only), so there is NO direct remote path; a
//      quorum-mode (or explicitly configured) node binds 0.0.0.0:8443 and is
//      reachable at https://<lan-ip>:8443 — with the caveat that the node cert
//      is self-signed and its SANs are 127.0.0.1/localhost, so the client must
//      accept an untrusted/mismatched cert.
//
// This handler reports both so the frontend can generate the right paste block
// or, when neither path exists, tell the user honestly that the tool can't
// reach this node yet (and offer to set up a tunnel).

import (
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// remoteURLResp is the JSON contract consumed by the Flow-2 RemoteConnectPanel.
type remoteURLResp struct {
	// Tunnel (public) path.
	HasTunnel         bool   `json:"has_tunnel"`
	TunnelHostname    string `json:"tunnel_hostname,omitempty"`
	TunnelMCPURL      string `json:"tunnel_mcp_url,omitempty"`
	OAuthAuthorizeURL string `json:"oauth_authorize_url,omitempty"`
	OAuthTokenURL     string `json:"oauth_token_url,omitempty"`

	// LAN (direct) path. LANExposed is true when the MCP TLS listener binds a
	// non-loopback interface AND at least one usable address was found. Rather
	// than guess a single "the LAN IP" (unwinnable on hosts with VM bridges,
	// VPNs, and multiple NICs), we return every plausible address labelled with
	// its interface and let the operator pick the one a peer can actually route
	// to — the LAN path only exists in quorum/0.0.0.0-bind mode anyway, i.e. a
	// user who deliberately exposed the node and knows their own network.
	LANExposed    bool           `json:"lan_exposed"`
	LANCandidates []lanCandidate `json:"lan_candidates,omitempty"`
	MCPPort       int            `json:"mcp_port"`
	// SelfSigned is true for the LAN path — the node cert is self-signed with
	// SANs 127.0.0.1/localhost, so a client hitting https://<lan-ip>:8443 must
	// be told to accept an untrusted/mismatched certificate.
	SelfSigned bool `json:"self_signed"`
}

// lanCandidate is one address a peer might use to reach the node directly,
// tagged with the interface it lives on (so the operator can tell "en0 = my
// wifi" from "utun3 = my VPN") and whether it's an RFC-1918 LAN address.
type lanCandidate struct {
	IP        string `json:"ip"`
	Iface     string `json:"iface"`
	IsPrivate bool   `json:"is_private"`
}

// handleConnectRemoteURL reports how a tool on another computer can reach this
// node. GET, cookie-authed (registered in the authed route group).
func (h *DashboardHandler) handleConnectRemoteURL(w http.ResponseWriter, r *http.Request) {
	resp := remoteURLResp{MCPPort: mcpDefaultPort}

	// --- Tunnel path: read the hostname the wizard wrote into cloudflared's
	// config.yml. Absent/unreadable config simply means "no tunnel".
	if host := tunnelHostnameFromConfig(); host != "" {
		resp.HasTunnel = true
		resp.TunnelHostname = host
		resp.TunnelMCPURL = "https://" + host + "/v1/mcp/sse"
		resp.OAuthAuthorizeURL = "https://" + host + "/oauth/authorize"
		resp.OAuthTokenURL = "https://" + host + "/oauth/token"
	}

	// --- LAN path: the node is only reachable directly when the MCP TLS
	// listener binds a non-loopback interface. A truly unset/unparseable
	// MCPTLSAddr (ok == false) is treated as unknown → local-only (safe).
	host, port, ok := parseMCPTLSAddr(h.MCPTLSAddr)
	if ok && port > 0 {
		resp.MCPPort = port
	}
	if ok && mcpBindIsRemote(host) {
		if cands := directIPv4Candidates(); len(cands) > 0 {
			resp.LANExposed = true
			resp.LANCandidates = cands
			resp.SelfSigned = true
		}
	}

	writeJSONResp(w, http.StatusOK, resp)
}

// mcpDefaultPort is the canonical MCP bearer TLS port used across the codebase
// (cloudflared ingress → service https://localhost:8443, CursorSetupPanel).
const mcpDefaultPort = 8443

// tunnelHostnameFromConfig extracts the tunnel hostname from cloudflared's
// config.yml (the file the ChatGPT wizard writes). The config lists the same
// hostname on every ingress rule; we return the first valid one. Returns ""
// when the file is missing/unreadable or has no usable hostname.
func tunnelHostnameFromConfig() string {
	configPath := filepath.Join(cloudflaredHome(), "config.yml")
	data, err := os.ReadFile(configPath) //nolint:gosec // path is under the user's home dir, not request input
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		// Ingress rules carry the hostname on a YAML list item ("- hostname: x"),
		// so strip a leading list marker before matching the key.
		trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
		if !strings.HasPrefix(trimmed, "hostname:") {
			continue
		}
		val := strings.TrimSpace(strings.TrimPrefix(trimmed, "hostname:"))
		// Strip an inline comment and any surrounding quotes.
		if idx := strings.IndexByte(val, '#'); idx >= 0 {
			val = strings.TrimSpace(val[:idx])
		}
		val = strings.Trim(val, `"'`)
		val = strings.ToLower(val)
		if validZoneRe.MatchString(val) {
			return val
		}
	}
	return ""
}

// parseMCPTLSAddr splits a "host:port" bind address into host and port. ok is
// true only when addr was non-empty AND split successfully — this lets the
// caller distinguish an UNSET/unparseable addr (ok=false → unknown, treat as
// local-only) from a bare ":port" wildcard bind (ok=true, host="" → remote).
func parseMCPTLSAddr(addr string) (host string, port int, ok bool) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", 0, false
	}
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, false
	}
	n, convErr := strconv.Atoi(p)
	if convErr != nil {
		n = 0
	}
	return h, n, true
}

// mcpBindIsRemote reports whether an MCP TLS bind host makes the node reachable
// from another computer. Call it ONLY on a successfully-parsed bind (ok=true):
// a wildcard bind ("" from a bare ":port", "0.0.0.0", "::") or any explicit
// non-loopback address is remote-reachable; an explicit loopback is local-only.
func mcpBindIsRemote(host string) bool {
	switch host {
	case "", "0.0.0.0", "::":
		return true // wildcard bind (bare :port or explicit wildcard) → all interfaces
	}
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsLoopback()
	}
	// A non-IP host (e.g. "localhost") — treat only literal loopback names as local.
	return host != "localhost"
}

// directIPv4Candidates returns every address a peer could plausibly use to
// reach this node directly, tagged with its interface. It drops loopback,
// link-local, and container/VM host interfaces that a separate physical machine
// can never route to (docker0, calico, k8s bridges, VMware/Apple VM bridges),
// but KEEPS VPN/overlay interfaces (Tailscale/WireGuard/OpenVPN/ZeroTier) since
// those may be the only reachable path. Ordering is a display hint only — a
// real single "winner" can't be inferred, so the frontend lets the operator
// choose: physical-LAN private first, then overlay private, then physical
// global (public/CGNAT), then overlay global.
func directIPv4Candidates() []lanCandidate {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	out := make([]lanCandidate, 0, len(ifaces))
	type ranked struct {
		c    lanCandidate
		rank int
	}
	ranks := make([]ranked, 0, len(ifaces))
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if isContainerIface(iface.Name) {
			continue
		}
		addrs, addrErr := iface.Addrs()
		if addrErr != nil {
			continue
		}
		for _, a := range addrs {
			var ipp net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ipp = v.IP
			case *net.IPAddr:
				ipp = v.IP
			}
			ip4 := ipp.To4()
			// IsGlobalUnicast() excludes loopback, link-local, multicast and the
			// unspecified address, keeping ordinary routable v4 (incl. RFC-1918).
			if ip4 == nil || ip4.IsLoopback() || ip4.IsLinkLocalUnicast() || !ip4.IsGlobalUnicast() {
				continue
			}
			isPriv := ip4.IsPrivate()
			// rank: 0 physical-private, 1 overlay-private, 2 physical-global, 3 overlay-global.
			rank := 0
			if !isPriv {
				rank = 2
			}
			if isOverlayIface(iface.Name) {
				rank++
			}
			ranks = append(ranks, ranked{
				c:    lanCandidate{IP: ip4.String(), Iface: iface.Name, IsPrivate: isPriv},
				rank: rank,
			})
		}
	}
	// Stable sort by rank so the most-likely-reachable address is first while
	// every candidate stays visible for the operator to pick.
	sort.SliceStable(ranks, func(i, j int) bool { return ranks[i].rank < ranks[j].rank })
	for _, r := range ranks {
		out = append(out, r.c)
	}
	return out
}

// isContainerIface reports whether an interface is a container/VM host network
// that a separate physical machine can never route to, so it should not appear
// as a reachability candidate. It deliberately does NOT match generic bridge
// names like OpenWrt's br-lan/br-wan (only Docker's br-<12hex> user bridges) or
// VPN overlays (see isOverlayIface — those are kept but de-ranked).
func isContainerIface(name string) bool {
	n := strings.ToLower(name)
	// Docker user-defined bridges are "br-" + 12 hex chars; skip those but keep
	// human-named bridges (br-lan, br-wan, ...).
	if strings.HasPrefix(n, "br-") && len(n) == 3+12 && isHex(n[3:]) {
		return true
	}
	// macOS VM host bridges are bridge100/bridge101/... (the >=100 range is
	// Internet Sharing / vmnet); the low-numbered bridge0 is a real HW bridge.
	if strings.HasPrefix(n, "bridge") {
		if num, convErr := strconv.Atoi(n[len("bridge"):]); convErr == nil && num >= 100 {
			return true
		}
	}
	for _, p := range []string{
		"docker", "veth", "virbr", "vmnet", "vmenet", "vboxnet", "vnic",
		"podman", "cni", "flannel", "kube", "cali", "cbr", "tunl", "weave",
	} {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	return false
}

// isOverlayIface reports whether an interface is a VPN/overlay tunnel. These are
// kept as candidates (they may be the only reachable path) but de-ranked below
// physical NICs so a real LAN address is the default when both exist. "tunl"
// (Calico IPIP) is handled as a container iface, so the "tun" prefix here only
// matches real tunnels (tun0/utun3).
func isOverlayIface(name string) bool {
	n := strings.ToLower(name)
	for _, p := range []string{"utun", "tun", "tap", "wg", "tailscale", "ppp", "zt"} {
		if strings.HasPrefix(n, p) {
			return true
		}
	}
	return false
}

// isHex reports whether s is non-empty and all lowercase hex digits.
func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
