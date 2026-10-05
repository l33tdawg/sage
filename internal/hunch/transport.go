package hunch

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrLocalOnly refuses judge connections outside this machine. The error never
// includes the configured URL, which may contain credentials.
var ErrLocalOnly = errors.New("hunch: judge URL must use HTTP(S) on localhost or a loopback IP, without credentials, query or fragment")

// ValidateLocalURL checks syntax without DNS or network access. Arbitrary DNS
// names are refused even if they currently resolve to loopback.
func ValidateLocalURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || (u.Scheme != "http" && u.Scheme != "https") ||
		u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		!loopbackHost(u.Hostname()) {
		return ErrLocalOnly
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func localClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		// Never forward a judgement body, even to another loopback service.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// Deliberately no Proxy: environment proxies must not see memory.
			DialContext:           dialLoopback,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: time.Second,
		},
	}
}

// dialLoopback enforces the boundary again at the actual connection. localhost
// is pinned to numeric loopback addresses, never resolved through DNS or hosts.
func dialLoopback(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || !loopbackHost(host) {
		return nil, ErrLocalOnly
	}
	dialer := net.Dialer{}
	if !strings.EqualFold(host, "localhost") {
		return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
	}
	conn, err := dialer.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port))
	if err == nil {
		return conn, nil
	}
	return dialer.DialContext(ctx, network, net.JoinHostPort("::1", port))
}
