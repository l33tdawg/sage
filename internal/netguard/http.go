package netguard

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
)

// LocalLANHTTPBase returns a canonical HTTP(S) base URL whose host resolves to
// localhost or a LAN-scoped IP. SAGE uses this for operator-provided local
// services (reranker sidecars, pairing listeners, and v11 LAN federation).
func LocalLANHTTPBase(raw string, schemes ...string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("url must be an absolute HTTP(S) base URL without userinfo, query, or fragment")
	}
	if !schemeAllowed(u.Scheme, schemes) {
		return "", fmt.Errorf("unsupported url scheme %q", u.Scheme)
	}
	host := u.Hostname()
	port := u.Port()
	if host == "" {
		return "", fmt.Errorf("url host is required")
	}
	if port != "" {
		p, pErr := strconv.Atoi(port)
		if pErr != nil || p < 1 || p > 65535 {
			return "", fmt.Errorf("invalid url port")
		}
	}
	if !LocalLANHost(host) {
		return "", fmt.Errorf("url host must be localhost or a LAN address")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.RawPath = ""
	return u.String(), nil
}

// LocalLANHostPort validates and canonicalizes a host:port pair for the LAN
// pairing listener encoded in QR tokens.
func LocalLANHostPort(raw string) (string, error) {
	host, port, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("address must be host:port: %w", err)
	}
	if !LocalLANHost(host) {
		return "", fmt.Errorf("address host must be localhost or a LAN address")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return "", fmt.Errorf("invalid address port")
	}
	return net.JoinHostPort(host, strconv.Itoa(p)), nil
}

// JoinPath appends an absolute endpoint path (with optional query string) to a
// pre-validated base URL. The path may not carry a scheme, host, userinfo, or
// fragment, so it can never redirect the request off the validated base.
func JoinPath(base, path string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(path, "/") || strings.HasPrefix(path, "//") || strings.Contains(path, "\x00") {
		return "", fmt.Errorf("endpoint path must be absolute")
	}
	// Parse (rather than assign to url.URL.Path) so a query string survives as
	// RawQuery instead of being percent-escaped into the path — assigning
	// "/a?b=c" to .Path serialized as "/a%3Fb=c" and 404'd the join status
	// poll. Parsing keeps ? and # semantics; the checks below re-establish the
	// no-redirect guarantees.
	ref, err := url.Parse(path)
	if err != nil {
		return "", fmt.Errorf("endpoint path invalid: %w", err)
	}
	if ref.Scheme != "" || ref.Host != "" || ref.User != nil || ref.Fragment != "" {
		return "", fmt.Errorf("endpoint path must not carry scheme, host, userinfo, or fragment")
	}
	return u.ResolveReference(ref).String(), nil
}

// LocalLANHost accepts only localhost names and literal IPs that are loopback
// or private RFC1918/ULA. DNS and link-local names are deliberately rejected so
// a scanned token cannot redirect SAGE through DNS or cloud metadata routes.
func LocalLANHost(host string) bool {
	h := strings.Trim(host, "[]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	addr, err := netip.ParseAddr(h)
	if err != nil {
		return false
	}
	return addr.IsLoopback() || addr.IsPrivate()
}

func schemeAllowed(scheme string, allowed []string) bool {
	for _, allowedScheme := range allowed {
		if strings.EqualFold(scheme, allowedScheme) {
			return true
		}
	}
	return false
}
