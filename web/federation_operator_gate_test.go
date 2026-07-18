package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFederationOperatorGateAllowsAuthenticatedHeaderlessLoopbackWebView(t *testing.T) {
	h, _ := newTestHandler(t)
	h.Encrypted.Store(true)
	h.sessions.Store("valid-webview-session", time.Now().Add(time.Hour))

	for _, host := range []string{"localhost:8080", "127.0.0.1:8080", "[::1]:8080"} {
		t.Run(host, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/connections", nil)
			req.Host = host
			req.RemoteAddr = "127.0.0.1:54321"
			req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "valid-webview-session"})
			w := httptest.NewRecorder()

			h.federationOperatorGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(w, req)

			require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
		})
	}
}

func TestFederationOperatorGateRejectsUnsafeHeaderlessRequests(t *testing.T) {
	tests := []struct {
		name       string
		encrypted  bool
		session    string
		host       string
		remoteAddr string
	}{
		{
			name:       "missing session",
			encrypted:  true,
			host:       "localhost:8080",
			remoteAddr: "127.0.0.1:54321",
		},
		{
			name:       "invalid session",
			encrypted:  true,
			session:    "invalid-session",
			host:       "localhost:8080",
			remoteAddr: "127.0.0.1:54321",
		},
		{
			name:       "unencrypted node",
			session:    "valid-webview-session",
			host:       "localhost:8080",
			remoteAddr: "127.0.0.1:54321",
		},
		{
			name:       "non-loopback source",
			encrypted:  true,
			session:    "valid-webview-session",
			host:       "192.168.1.10:8080",
			remoteAddr: "192.168.1.20:54321",
		},
		{
			name:       "dns rebinding host",
			encrypted:  true,
			session:    "valid-webview-session",
			host:       "attacker.example:8080",
			remoteAddr: "127.0.0.1:54321",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler(t)
			h.Encrypted.Store(tt.encrypted)
			h.sessions.Store("valid-webview-session", time.Now().Add(time.Hour))

			req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/connections", nil)
			req.Host = tt.host
			req.RemoteAddr = tt.remoteAddr
			if tt.session != "" {
				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: tt.session})
			}
			w := httptest.NewRecorder()

			h.federationOperatorGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(w, req)

			require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
		})
	}
}

func TestFederationOperatorGateRejectsCrossSiteBrowserWithValidSession(t *testing.T) {
	h, _ := newTestHandler(t)
	h.Encrypted.Store(true)
	h.sessions.Store("valid-browser-session", time.Now().Add(time.Hour))
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/federation/connections", nil)
	req.Host = "127.0.0.1:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	req.Header.Set("Origin", "https://attacker.example")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "valid-browser-session"})
	w := httptest.NewRecorder()

	h.federationOperatorGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(w, req)

	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}
