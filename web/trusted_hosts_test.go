package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAllowedCEREBRUMHostsParsing(t *testing.T) {
	t.Setenv(allowedCEREBRUMHostsEnv, " sage.stands-macbook-pro.local , cerebrum.stands-macbook-pro.local:443 ,, sage.stands-macbook-pro.local ")
	hosts := allowedCEREBRUMHosts()
	if len(hosts) != 2 {
		t.Fatalf("expected 2 deduped hosts, got %d: %v", len(hosts), hosts)
	}
	if hosts[0] != "sage.stands-macbook-pro.local" || hosts[1] != "cerebrum.stands-macbook-pro.local" {
		t.Fatalf("unexpected hosts: %v", hosts)
	}

	t.Setenv(allowedCEREBRUMHostsEnv, "")
	if got := allowedCEREBRUMHosts(); got != nil {
		t.Fatalf("expected nil for empty env, got %v", got)
	}
	t.Setenv(allowedCEREBRUMHostsEnv, " , , ")
	if got := allowedCEREBRUMHosts(); got != nil {
		t.Fatalf("expected nil for separator-only env, got %v", got)
	}
}

func TestHostIsTrustedCEREBRUMHost(t *testing.T) {
	t.Setenv(allowedCEREBRUMHostsEnv, "sage.stands-macbook-pro.local")
	cases := []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"localhost:8080", true},
		{"127.0.0.1:8080", true},
		{"[::1]:8080", true},
		{"sage.stands-macbook-pro.local", true},
		{"SAGE.STANDS-MACBOOK-PRO.LOCAL", true},
		{"sage.stands-macbook-pro.local:443", true},
		{"evil.example.com", false},
		{"sage.stands-macbook-pro.local.evil.example.com", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := hostIsTrustedCEREBRUMHost(tc.host); got != tc.want {
			t.Errorf("hostIsTrustedCEREBRUMHost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

func loopbackCEREBRUMRequest(host string, mutate func(*http.Request)) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/ui/", nil)
	r.RemoteAddr = "127.0.0.1:51422"
	r.Host = host
	if mutate != nil {
		mutate(r)
	}
	return r
}

func TestIsLoopbackCEREBRUMRequestAllowedHosts(t *testing.T) {
	const proxiedHost = "sage.stands-macbook-pro.local"

	// Default: an unconfigured hostname stays hidden behind 404.
	if isLoopbackCEREBRUMRequest(loopbackCEREBRUMRequest(proxiedHost, nil)) {
		t.Fatal("foreign Host must be rejected when no extra hosts are configured")
	}

	t.Setenv(allowedCEREBRUMHostsEnv, proxiedHost)

	// Loopback peer + configured Host + loopback forwarded metadata: accepted,
	// including the proxy passing the original Host through in X-Forwarded-Host.
	r := loopbackCEREBRUMRequest(proxiedHost, func(r *http.Request) {
		r.Header.Set("X-Forwarded-For", "127.0.0.1")
		r.Header.Set("X-Forwarded-Host", proxiedHost)
		r.Header.Set("X-Forwarded-Proto", "https")
	})
	if !isLoopbackCEREBRUMRequest(r) {
		t.Fatal("configured Host with loopback peer/forwarding must be accepted")
	}

	// An attacker hostname remains rejected even with the feature enabled.
	if isLoopbackCEREBRUMRequest(loopbackCEREBRUMRequest("evil.example.com", nil)) {
		t.Fatal("unconfigured foreign Host must stay rejected")
	}

	// A configured Host from a non-loopback peer is still rejected.
	lan := loopbackCEREBRUMRequest(proxiedHost, nil)
	lan.RemoteAddr = "192.168.0.50:51422"
	if isLoopbackCEREBRUMRequest(lan) {
		t.Fatal("non-loopback peer must stay rejected regardless of Host")
	}

	// A configured Host forwarded from a non-loopback hop is still rejected.
	r = loopbackCEREBRUMRequest(proxiedHost, func(r *http.Request) {
		r.Header.Set("X-Forwarded-For", "192.168.0.50")
	})
	if isLoopbackCEREBRUMRequest(r) {
		t.Fatal("non-loopback X-Forwarded-For hop must stay rejected")
	}
}

func TestIsLocalRequestAllowedBrowserHost(t *testing.T) {
	const proxiedHost = "cerebrum.stands-macbook-pro.local"
	t.Setenv(allowedCEREBRUMHostsEnv, proxiedHost)

	// Browser request through a TLS-terminating loopback proxy: same-origin
	// Fetch Metadata, https Origin, Host passthrough, X-Forwarded-Proto https.
	r := httptest.NewRequest(http.MethodGet, "/v1/dashboard/health", nil)
	r.RemoteAddr = "127.0.0.1:51422"
	r.Host = proxiedHost
	r.Header.Set("Sec-Fetch-Site", "same-origin")
	r.Header.Set("Origin", "https://"+proxiedHost)
	r.Header.Set("X-Forwarded-Proto", "https")
	if !isLocalRequest(r) {
		t.Fatal("same-origin browser request via configured host must be local")
	}

	// Cross-site browser request stays rejected.
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	if isLocalRequest(r) {
		t.Fatal("cross-site request must stay rejected")
	}
}

func TestOriginMatchesRequestForwardedProto(t *testing.T) {
	httpsOrigin := "https://sage.stands-macbook-pro.local"
	newRequest := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/ui/", nil) // plain HTTP behind proxy
		r.Host = "sage.stands-macbook-pro.local:443"
		return r
	}

	r := newRequest()
	if originMatchesRequest(r, httpsOrigin) {
		t.Fatal("https origin must not match a plain-HTTP request without X-Forwarded-Proto")
	}
	r.Header.Set("X-Forwarded-Proto", "https")
	if !originMatchesRequest(r, httpsOrigin) {
		t.Fatal("https origin must match when X-Forwarded-Proto is https")
	}

	r.Host = "sage.stands-macbook-pro.local"
	if !originMatchesRequest(r, httpsOrigin) {
		t.Fatal("default https port must normalize")
	}
	if originMatchesRequest(r, "https://evil.example.com") {
		t.Fatal("different origin host must never match")
	}

	for _, tc := range []struct {
		name   string
		values []string
		origin string
		match  bool
	}{
		{name: "mixed case", values: []string{"HTTPS"}, origin: httpsOrigin, match: true},
		{name: "unanimous comma chain", values: []string{"https, HTTPS"}, origin: httpsOrigin, match: true},
		{name: "unanimous field lines", values: []string{"https", "HTTPS"}, origin: httpsOrigin, match: true},
		{name: "unanimous HTTP chain", values: []string{"HTTP, http"}, origin: "http://sage.stands-macbook-pro.local:443", match: true},
		{name: "mixed scheme chain matching first", values: []string{"http, https"}, origin: "http://sage.stands-macbook-pro.local:443"},
		{name: "mixed scheme chain matching last", values: []string{"https, http"}, origin: httpsOrigin},
		{name: "mixed scheme field lines", values: []string{"https", "http"}, origin: httpsOrigin},
		{name: "malformed scheme", values: []string{"ftp"}, origin: "http://sage.stands-macbook-pro.local:443"},
		{name: "empty chain token", values: []string{"http,"}, origin: "http://sage.stands-macbook-pro.local:443"},
		{name: "empty field line", values: []string{""}, origin: "http://sage.stands-macbook-pro.local:443"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRequest()
			for _, value := range tc.values {
				r.Header.Add("X-Forwarded-Proto", value)
			}
			if got := originMatchesRequest(r, tc.origin); got != tc.match {
				t.Fatalf("originMatchesRequest() = %v, want %v for values %q", got, tc.match, tc.values)
			}
		})
	}
}
