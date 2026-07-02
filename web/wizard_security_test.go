package web

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWizard_Rejects_CrossOriginBrowser verifies that a browser tab on a
// non-local origin (chatgpt.com is the canonical example) cannot drive any
// wizard endpoint, even on a fresh-install node where dashboard encryption
// is off. Both the Origin header and Sec-Fetch-Site signal must be
// independently honoured.
func TestWizard_Rejects_CrossOriginBrowser(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("wizard install path is POSIX-only")
	}
	withFakeCloudflared(t, nil)
	h, _ := newTestHandler(t)
	r := testRouter(h)

	cases := []struct {
		name      string
		setHeader func(*http.Request)
	}{
		{
			name: "origin chatgpt.com",
			setHeader: func(req *http.Request) {
				req.Header.Set("Origin", "https://chatgpt.com")
			},
		},
		{
			name: "sec-fetch-site cross-site",
			setHeader: func(req *http.Request) {
				req.Header.Set("Sec-Fetch-Site", "cross-site")
			},
		},
		{
			name: "origin attacker subdomain",
			setHeader: func(req *http.Request) {
				req.Header.Set("Origin", "https://example.com")
			},
		},
	}

	endpoints := []struct {
		method, path string
	}{
		{http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared"},
		{http.MethodPost, "/v1/wizard/chatgpt/login"},
		{http.MethodGet, "/v1/wizard/chatgpt/login-status"},
		{http.MethodPost, "/v1/wizard/chatgpt/create-tunnel"},
		{http.MethodPost, "/v1/wizard/chatgpt/mint-token"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, ep := range endpoints {
				req := httptest.NewRequest(ep.method, ep.path, nil)
				tc.setHeader(req)
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				// authMiddleware (401) and wizardSecurityGate (403) both
				// reject cross-origin requests — accept either.
				if w.Code != http.StatusUnauthorized && w.Code != http.StatusForbidden {
					t.Errorf("%s %s expected 401 or 403, got %d", ep.method, ep.path, w.Code)
				}
			}
		})
	}
}

// TestWizardSecurityGate_DefenseInDepth confirms the wizard's own same-origin
// check fires independently of authMiddleware — even an authenticated session
// cookie does not let a cross-origin browser tab drive the wizard. This is
// the belt-and-braces layer that survives any future regression of the
// outer auth check.
func TestWizardSecurityGate_DefenseInDepth(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("wizard install path is POSIX-only")
	}
	withFakeCloudflared(t, nil)
	h, _ := newTestHandler(t)
	r := testRouter(h)

	gate := h.wizardSecurityGate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil)
	req.Header.Set("Origin", "https://chatgpt.com")
	w := httptest.NewRecorder()
	gate.ServeHTTP(w, req)
	assert.Equal(t, http.StatusForbidden, w.Code)

	_ = r // touched so the broader test setup compiles cleanly
}

// TestWizard_Allows_SameOriginBrowser verifies the dashboard SPA's same-origin
// fetches continue to reach the wizard endpoints (the path most users hit).
func TestWizard_Allows_SameOriginBrowser(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("wizard install path is POSIX-only")
	}
	withFakeCloudflared(t, nil)
	h, _ := newTestHandler(t)
	r := testRouter(h)

	for _, tc := range []struct{ origin, host string }{
		{"http://localhost:8080", "localhost:8080"},
		{"http://127.0.0.1:8080", "127.0.0.1:8080"},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil)
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		req.Host = tc.host // a real same-origin request's Host matches its origin
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code, "same-origin from %s should pass", tc.origin)
	}
}

// TestWizard_Rejects_DNSRebinding verifies the anti-rebinding Host gate: a
// browser request that looks same-origin (Sec-Fetch-Site: same-origin) but
// carries an attacker DOMAIN in Host - the shape of a DNS-rebinding attack that
// re-resolves that domain to 127.0.0.1 - is refused.
func TestWizard_Rejects_DNSRebinding(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("wizard install path is POSIX-only")
	}
	withFakeCloudflared(t, nil)
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil)
	req.Header.Set("Origin", "http://attacker.example")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "attacker.example" // rebound to 127.0.0.1, but Host is a domain
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	// authMiddleware's isLocalRequest fails first in unencrypted mode, so the
	// refusal surfaces as 401 (before the wizard gate's own 403).
	assert.Equal(t, http.StatusUnauthorized, w.Code, "rebinding domain Host must be refused")
}

// TestWizard_Allows_NoOrigin_CLI confirms non-browser callers (curl, native
// CLIs) which don't emit Origin headers continue to work.
func TestWizard_Allows_NoOrigin_CLI(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("wizard install path is POSIX-only")
	}
	withFakeCloudflared(t, nil)
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest(http.MethodPost, "/v1/wizard/chatgpt/check-cloudflared", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

// TestValidateCloudflareLoginURL exercises the login-URL allowlist used to
// guard against a poisoned cloudflared printing an attacker-controlled URL.
func TestValidateCloudflareLoginURL(t *testing.T) {
	good := []string{
		"https://dash.cloudflare.com/argotunnel?abc=1",
		"https://login.cloudflare.com/oauth?x=2",
		"https://cloudflare.com/whatever",
	}
	for _, u := range good {
		_, err := validateCloudflareLoginURL(u)
		assert.NoError(t, err, "should accept %s", u)
	}

	bad := []string{
		"http://dash.cloudflare.com/argotunnel?abc=1",   // not https
		"https://evil.example.com/login",                // not cloudflare
		"https://cloudflare.com.evil.example.com/login", // suffix trick
		"https://user:pass@dash.cloudflare.com/login",   // userinfo
		"javascript:alert(1)",                           // not http(s)
		"file:///etc/passwd",                            // file scheme
		"  ",                                            // empty
	}
	for _, u := range bad {
		_, err := validateCloudflareLoginURL(u)
		assert.Error(t, err, "should reject %s", u)
	}
}

// TestReadCloudflaredCertFile_RejectsSymlink confirms cert.pem reads refuse
// to follow a symlink — even one swapped in between Lstat and Open.
func TestReadCloudflaredCertFile_RejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "real.pem")
	require.NoError(t, os.WriteFile(target, []byte("fake cert content"), 0o600))
	link := filepath.Join(dir, "cert.pem")
	require.NoError(t, os.Symlink(target, link))

	_, err := readCloudflaredCertFile(link)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "symlink")
}

// TestReadCloudflaredCertFile_ReadsRegular confirms the happy path still
// works after the symlink-rejection added on top.
func TestReadCloudflaredCertFile_ReadsRegular(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cert.pem")
	require.NoError(t, os.WriteFile(path, []byte("real content"), 0o600))
	got, err := readCloudflaredCertFile(path)
	require.NoError(t, err)
	assert.Equal(t, "real content", string(got))
}

// TestAuthMiddleware_DeniesCrossOrigin_WhenEncryptionOff confirms the
// dashboard root-router auth no longer fail-opens for cross-origin browser
// requests when the synaptic-ledger vault is unset.
func TestAuthMiddleware_DeniesCrossOrigin_WhenEncryptionOff(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	require.False(t, h.Encrypted.Load(), "test handler must start with encryption off")

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
	req.Header.Set("Origin", "https://chatgpt.com")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestAuthMiddleware_AllowsSameOrigin_WhenEncryptionOff confirms the SPA's
// own fetches continue to work after the auth tightening.
func TestAuthMiddleware_AllowsSameOrigin_WhenEncryptionOff(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	require.False(t, h.Encrypted.Load())

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
	req.Header.Set("Origin", "http://localhost:8080")
	req.Host = "localhost:8080" // a real same-origin request's Host matches its origin
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}
