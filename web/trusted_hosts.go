package web

import (
	"net"
	"os"
	"strings"
)

// allowedCEREBRUMHostsEnv names the operator-configured extra hostnames that
// the CEREBRUM loopback boundary accepts in the Host header (and in
// forwarding headers) in addition to localhost and loopback IPs.
//
// The boundary is a DNS-rebinding and LAN-exposure defense: the connected
// peer must still be loopback and every forwarded-for hop must still be
// loopback — this list only widens WHICH hostname a loopback reverse proxy
// (for example Caddy serving *.stands-macbook-pro.local with a rewritten
// Host) may present. Hosts are exact-match after port stripping; there is
// deliberately no wildcard support, because a wildcard would re-open the
// rebinding vector the boundary exists to close.
const allowedCEREBRUMHostsEnv = "SAGE_ALLOWED_CEREBRUM_HOSTS"

// allowedCEREBRUMHosts returns the operator-configured extra CEREBRUM
// hostnames (empty when none are configured). Parsed per call: traffic through
// this boundary is human-scale, and no caching means tests (and operators
// editing the env between requests) always see current configuration.
func allowedCEREBRUMHosts() []string {
	raw := os.Getenv(allowedCEREBRUMHostsEnv)
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	seen := make(map[string]struct{})
	hosts := make([]string, 0, 8)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if h, _, err := net.SplitHostPort(entry); err == nil {
			entry = h
		}
		entry = strings.TrimPrefix(strings.TrimSuffix(entry, "]"), "[") // unwrap [::1]
		entry = strings.ToLower(strings.TrimSpace(entry))
		if entry == "" {
			continue
		}
		if _, dup := seen[entry]; dup {
			continue
		}
		seen[entry] = struct{}{}
		hosts = append(hosts, entry)
	}
	if len(hosts) == 0 {
		return nil
	}
	return hosts
}

// forwardedProtoScheme parses all X-Forwarded-Proto field-lines and
// comma-joined hop values. A proxy chain is usable only when every token is a
// valid, case-insensitive HTTP scheme and all hops agree. Rejecting empty,
// malformed, and mixed chains prevents client-supplied forwarding metadata
// from overriding the request's actual transport ambiguously.
func forwardedProtoScheme(values []string) (string, bool) {
	var scheme string
	for _, value := range values {
		for _, entry := range strings.Split(value, ",") {
			entry = strings.ToLower(strings.TrimSpace(entry))
			if entry != "http" && entry != "https" {
				return "", false
			}
			if scheme != "" && entry != scheme {
				return "", false
			}
			scheme = entry
		}
	}
	return scheme, scheme != ""
}

// hostIsTrustedCEREBRUMHost reports whether host is loopback or an
// operator-configured extra hostname. It is the Host check used by the
// CEREBRUM boundary; peer and forwarded-for checks stay loopback-only.
func hostIsTrustedCEREBRUMHost(host string) bool {
	if hostIsLoopback(host) {
		return true
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	for _, allowed := range allowedCEREBRUMHosts() {
		if host == allowed {
			return true
		}
	}
	return false
}

// hostIsAllowedBrowserHost extends hostIsLoopbackOrIP (the browser
// anti-rebinding Host gate) with the operator-configured extra hostnames.
func hostIsAllowedBrowserHost(host string) bool {
	if hostIsLoopbackOrIP(host) {
		return true
	}
	return hostIsTrustedCEREBRUMHost(host)
}
