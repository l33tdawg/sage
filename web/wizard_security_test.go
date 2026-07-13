package web

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWizardSecurityGateRejectsCrossOriginBrowser(t *testing.T) {
	h, _ := newTestHandler(t)
	gate := h.wizardSecurityGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for _, tc := range []struct {
		name      string
		setHeader func(*http.Request)
	}{
		{"origin", func(req *http.Request) { req.Header.Set("Origin", "https://example.com") }},
		{"fetch metadata", func(req *http.Request) { req.Header.Set("Sec-Fetch-Site", "cross-site") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/connect/remote/token", nil)
			tc.setHeader(req)
			w := httptest.NewRecorder()
			gate.ServeHTTP(w, req)
			assert.Equal(t, http.StatusForbidden, w.Code)
		})
	}
}

func TestWizardSecurityGateAllowsLocalCLI(t *testing.T) {
	h, _ := newTestHandler(t)
	gate := h.wizardSecurityGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/connect/remote/token", nil)
	w := httptest.NewRecorder()
	gate.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddlewareDeniesCrossOriginWhenEncryptionOff(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	require.False(t, h.Encrypted.Load())

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
	req.Header.Set("Origin", "https://example.com")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestAuthMiddlewareAllowsSameOriginWhenEncryptionOff(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	require.False(t, h.Encrypted.Load())

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
	req.Header.Set("Origin", "http://localhost:8080")
	req.Host = "localhost:8080"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
