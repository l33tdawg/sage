package hunch

import (
	"context"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateLocalURL(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1:8791", "http://127.2.3.4:8791/base", "http://localhost:8791", "http://LOCALHOST:8791", "https://[::1]:8791", "http://[::ffff:127.0.0.1]:8791"} {
		t.Run(raw, func(t *testing.T) { require.NoError(t, ValidateLocalURL(raw)) })
	}
	for _, raw := range []string{"", "https://judge.example", "https://8.8.8.8", "http://192.168.1.2", "http://10.0.0.2", "http://0.0.0.0", "http://[::]", "http://[fe80::1]", "http://[::1%25en0]", "http://localhost.example", "http://127.0.0.1.example", "http://127.1", "http://2130706433", "http://localhost.", "ftp://127.0.0.1", "//localhost:8791", "http://user:secret@localhost:8791", "http://localhost:8791?token=secret", "http://localhost:8791?", "http://localhost:8791/#secret", "http://localhost:invalid"} {
		t.Run(raw, func(t *testing.T) { require.ErrorIs(t, ValidateLocalURL(raw), ErrLocalOnly) })
	}
}

func TestYesNo_LocalhostBypassesEnvironmentProxy(t *testing.T) {
	var proxied atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { proxied.Add(1) }))
	defer proxy.Close()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(key, proxy.URL)
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	c := server(t, http.StatusOK, map[string]any{"results": map[string]any{"lasting": map[string]any{"p_yes": 0.95}}}, nil)
	c.BaseURL = strings.Replace(c.BaseURL, "127.0.0.1", "localhost", 1)
	require.Nil(t, c.http.Transport.(*http.Transport).Proxy)
	_, err := c.YesNo(context.Background(), "private memory", map[string]Check{"lasting": Lasting})
	require.NoError(t, err)
	require.Zero(t, proxied.Load())
}

func TestYesNo_DoesNotForwardRedirectedBody(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) }))
	defer target.Close()
	for _, location := range []string{target.URL, "https://judge.example/v1/judge"} {
		t.Run(location, func(t *testing.T) {
			redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, location, http.StatusTemporaryRedirect)
			}))
			defer redirect.Close()
			c := New(redirect.URL, "secret", "", time.Second)
			_, err := c.YesNo(context.Background(), map[string]string{"memory": "private", "evidence": "private source"}, map[string]Check{"lasting": Lasting})
			require.Error(t, err)
			require.Zero(t, forwarded.Load(), "307 must never replay memory or evidence to another service")
		})
	}
}

func TestYesNo_RevalidatesURLBeforeSending(t *testing.T) {
	c := New("http://127.0.0.1:8791", "", "", time.Second)
	c.BaseURL = "https://judge.example"
	_, err := c.YesNo(context.Background(), "private memory", map[string]Check{"lasting": Lasting})
	require.ErrorIs(t, err, ErrLocalOnly)
}

func TestDialLoopbackRejectsRemoteAddresses(t *testing.T) {
	for _, addr := range []string{"judge.example:443", "192.168.1.2:8791", "0.0.0.0:8791", "[::]:8791", "localhost.example:8791"} {
		conn, err := dialLoopback(context.Background(), "tcp", addr)
		require.ErrorIs(t, err, ErrLocalOnly)
		require.Nil(t, conn)
	}
}

func TestYesNo_LocalHTTPSRequiresTrustedCertificate(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"results":{"lasting":{"p_yes":0.95}}}`))
	}))
	defer srv.Close()
	c := New(srv.URL, "", "", time.Second)
	_, err := c.YesNo(context.Background(), "private memory", map[string]Check{"lasting": Lasting})
	require.Error(t, err, "local-only must not weaken TLS verification")
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	transport := c.http.Transport.(*http.Transport)
	transport.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	transport.TLSClientConfig.RootCAs = roots
	_, err = c.YesNo(context.Background(), "private memory", map[string]Check{"lasting": Lasting})
	require.NoError(t, err)
}
