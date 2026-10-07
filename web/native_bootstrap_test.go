package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/nativebootstrap"
	"github.com/l33tdawg/sage/internal/vault"
)

var nativeBootstrapTestProcess atomic.Uint64

func configureNativeBootstrapTestHandler(t *testing.T, h *DashboardHandler) http.Handler {
	t.Helper()
	h.NativeBinding = nativebootstrap.Binding{
		Generation: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)),
		Origin:     "http://127.0.0.1:18080",
	}
	var err error
	h.NativeBootstrap, err = nativebootstrap.New(h.NativeBinding)
	require.NoError(t, err)
	// Use the production registration without historical testRouter metadata
	// injection: an actual native request must carry no browser assertions.
	router := chi.NewRouter()
	h.RegisterRoutes(router)
	t.Cleanup(h.WaitBackground)
	return router
}

func newNativeBootstrapTestHandler(t *testing.T) (*DashboardHandler, http.Handler) {
	t.Helper()
	h, _ := newTestHandler(t)
	return h, configureNativeBootstrapTestHandler(t, h)
}

func nativeBootstrapTestRequest(method, path string, body []byte, token string) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = "127.0.0.1:18080"
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set(nativeSessionHeader, token)
	}
	return req
}

func issueNativeBootstrapTestProof(t *testing.T, h *DashboardHandler) map[string]string {
	t.Helper()
	public, private, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	publicText := base64.RawURLEncoding.EncodeToString(public)
	peer := nativebootstrap.Peer{
		ProcessBinding: "audit-token-" + strconv.FormatUint(nativeBootstrapTestProcess.Add(1), 10),
		Identifier:     "com.sage.native.test", TeamID: "TEST", CDHash: strings.Repeat("a", 40),
	}
	ticket, err := h.NativeBootstrap.Issue(peer, h.NativeBinding, publicText)
	require.NoError(t, err)
	redemption := nativebootstrap.Redemption{
		Ticket: ticket.Ticket, Challenge: ticket.Challenge, PublicKey: publicText, Binding: h.NativeBinding,
	}
	signature := base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, nativebootstrap.SignatureMessage(redemption)))
	return map[string]string{
		"ticket": ticket.Ticket, "challenge": ticket.Challenge, "public_key": publicText,
		"instance_generation": h.NativeBinding.Generation, "ui_origin": h.NativeBinding.Origin,
		"startup_proof": h.NativeBinding.StartupProof, "signature": signature,
	}
}

func nativeBootstrapJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func redeemNativeBootstrapTestProof(t *testing.T, h *DashboardHandler, router http.Handler, proof map[string]string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/redeem", nativeBootstrapJSON(t, proof), ""))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Empty(t, rec.Header().Values("Set-Cookie"), "native transport redemption must never mint a vault cookie")
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	var response struct {
		Token   string `json:"native_session"`
		Expires int    `json:"expires_in_seconds"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, 900, response.Expires)
	require.True(t, h.NativeBootstrap.Verify(response.Token, h.NativeBinding))
	return response.Token
}

func nativeBootstrapTestAdmission(t *testing.T, h *DashboardHandler, router http.Handler) string {
	t.Helper()
	return redeemNativeBootstrapTestProof(t, h, router, issueNativeBootstrapTestProof(t, h))
}

func TestNativeBootstrapRedemptionIsTransportOnlyAndOneUse(t *testing.T) {
	h, router := newNativeBootstrapTestHandler(t)
	h.Encrypted.Store(true)
	proof := issueNativeBootstrapTestProof(t, h)
	token := redeemNativeBootstrapTestProof(t, h, router, proof)
	replay := httptest.NewRecorder()
	router.ServeHTTP(replay, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/redeem", nativeBootstrapJSON(t, proof), ""))
	require.Equal(t, http.StatusUnauthorized, replay.Code, replay.Body.String())
	require.Empty(t, replay.Header().Values("Set-Cookie"))

	check := httptest.NewRecorder()
	router.ServeHTTP(check, nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/auth/check", nil, token))
	require.Equal(t, http.StatusOK, check.Code, check.Body.String())
	var authState map[string]any
	require.NoError(t, json.Unmarshal(check.Body.Bytes(), &authState))
	require.Equal(t, true, authState["auth_required"])
	require.Equal(t, false, authState["authenticated"], "transport admission must not authenticate the vault")
	require.Empty(t, check.Header().Values("Set-Cookie"))

	for _, path := range []string{"/v1/dashboard/memory/list", "/v1/dashboard/stats"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodGet, path, nil, token))
		require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
	}
}

func TestNativeBootstrapAdmissionCannotBecomeUnencryptedRoot(t *testing.T) {
	fixture := newAppV23DashboardRouteFixture(t)
	h := fixture.handler
	router := configureNativeBootstrapTestHandler(t, h)
	token := nativeBootstrapTestAdmission(t, h, router)
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/v1/dashboard/auth/check"},
		{http.MethodPost, "/v1/dashboard/auth/login"},
		{http.MethodGet, "/v1/dashboard/memory/list"},
		{http.MethodPost, "/v1/dashboard/tasks"},
		{http.MethodPost, "/v1/dashboard/settings/ledger/enable"},
	} {
		t.Run(route.method+route.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, nativeBootstrapTestRequest(route.method, route.path, []byte(`{"passphrase":"test-native-vault"}`), token))
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), "encrypted vault")
			require.Empty(t, rec.Header().Values("Set-Cookie"))
		})
	}
	gate := h.nativeSessionGate(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		require.False(t, h.isCEREBRUMOperatorRequest(req), "verified application must not acquire current Root")
		require.False(t, h.appV23SessionHasRootAuthority(req))
	}))
	gate.ServeHTTP(httptest.NewRecorder(), nativeBootstrapTestRequest(http.MethodGet, "/", nil, token))
}

func TestNativeBootstrapExactRequestBoundary(t *testing.T) {
	cases := map[string]func(*http.Request){
		"remote peer": func(r *http.Request) { r.RemoteAddr = "192.168.1.2:54321" },
		"other host":  func(r *http.Request) { r.Host = "localhost:18080" },
		"other port":  func(r *http.Request) { r.Host = "127.0.0.1:18081" },
		"rebind host": func(r *http.Request) { r.Host = "attacker.example:18080" },
		"https":       func(r *http.Request) { r.TLS = &tls.ConnectionState{} },
	}
	for _, header := range []string{"Origin", "Sec-Fetch-Site", "Sec-Fetch-Mode", "X-Agent-ID", "X-Signature", "X-Timestamp", "X-Nonce", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
		cases[header] = func(r *http.Request) { r.Header.Set(header, "test") }
		cases[header+" empty"] = func(r *http.Request) { r.Header.Set(header, "") }
	}
	h, router := newNativeBootstrapTestHandler(t)
	h.Encrypted.Store(true)
	token := nativeBootstrapTestAdmission(t, h, router)
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			proof := issueNativeBootstrapTestProof(t, h)
			request := nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/redeem", nativeBootstrapJSON(t, proof), "")
			mutate(request)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, request)
			require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
			require.Empty(t, rec.Header().Values("Set-Cookie"))
			// The denied request must not spend a legitimate ticket.
			redeemNativeBootstrapTestProof(t, h, router, proof)

			request = nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/auth/check", nil, token)
			mutate(request)
			rec = httptest.NewRecorder()
			router.ServeHTTP(rec, request)
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
		})
	}
}

func TestNativeBootstrapClosedRedemptionSchema(t *testing.T) {
	cases := map[string]func(map[string]string, []byte) ([]byte, string){
		"case variant field": func(p map[string]string, _ []byte) ([]byte, string) {
			p["Ticket"] = p["ticket"]
			delete(p, "ticket")
			return nativeBootstrapJSON(t, p), "application/json"
		},
		"case alias duplicate": func(p map[string]string, b []byte) ([]byte, string) {
			return append([]byte(`{"Ticket":"`+p["ticket"]+`",`), b[1:]...), "application/json"
		},
		"unknown field": func(p map[string]string, _ []byte) ([]byte, string) {
			p["role"] = "Root"
			return nativeBootstrapJSON(t, p), "application/json"
		},
		"missing startup proof": func(p map[string]string, _ []byte) ([]byte, string) {
			delete(p, "startup_proof")
			return nativeBootstrapJSON(t, p), "application/json"
		},
		"null startup proof": func(_ map[string]string, b []byte) ([]byte, string) {
			return bytes.Replace(b, []byte(`"startup_proof":""`), []byte(`"startup_proof":null`), 1), "application/json"
		},
		"duplicate ticket": func(p map[string]string, b []byte) ([]byte, string) {
			return append([]byte(`{"ticket":"`+p["ticket"]+`",`), b[1:]...), "application/json"
		},
		"duplicate startup proof": func(_ map[string]string, b []byte) ([]byte, string) {
			return append([]byte(`{"startup_proof":"",`), b[1:]...), "application/json"
		},
		"trailing object": func(_ map[string]string, b []byte) ([]byte, string) {
			return append(b, []byte(`{}`)...), "application/json"
		},
		"oversize": func(_ map[string]string, b []byte) ([]byte, string) {
			return append(b, bytes.Repeat([]byte(" "), 4096)...), "application/json"
		},
		"malformed json":       func(_ map[string]string, _ []byte) ([]byte, string) { return []byte(`{"ticket":`), "application/json" },
		"array":                func(_ map[string]string, _ []byte) ([]byte, string) { return []byte(`[]`), "application/json" },
		"null":                 func(_ map[string]string, _ []byte) ([]byte, string) { return []byte(`null`), "application/json" },
		"text plain":           func(_ map[string]string, b []byte) ([]byte, string) { return b, "text/plain" },
		"missing content type": func(_ map[string]string, b []byte) ([]byte, string) { return b, "" },
	}
	for name, malformed := range cases {
		t.Run(name, func(t *testing.T) {
			h, router := newNativeBootstrapTestHandler(t)
			proof := issueNativeBootstrapTestProof(t, h)
			original := make(map[string]string, len(proof))
			for key, value := range proof {
				original[key] = value
			}
			body, contentType := malformed(proof, nativeBootstrapJSON(t, proof))
			req := nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/redeem", body, "")
			req.Header.Set("Content-Type", contentType)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if contentType == "application/json" {
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			} else {
				require.Equal(t, http.StatusUnsupportedMediaType, rec.Code, rec.Body.String())
			}
			require.Empty(t, rec.Header().Values("Set-Cookie"))
			redeemNativeBootstrapTestProof(t, h, router, original)
		})
	}
}

func TestNativeBootstrapRejectsForgedOrForeignProofWithoutSpendingTicket(t *testing.T) {
	for _, field := range []string{"ticket", "challenge", "instance_generation", "ui_origin", "startup_proof", "public_key", "signature"} {
		t.Run(field, func(t *testing.T) {
			h, router := newNativeBootstrapTestHandler(t)
			proof := issueNativeBootstrapTestProof(t, h)
			original := make(map[string]string, len(proof))
			for key, value := range proof {
				original[key] = value
			}
			if field == "ui_origin" {
				proof[field] = "http://localhost:18080"
			} else {
				proof[field] = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/redeem", nativeBootstrapJSON(t, proof), ""))
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			require.Empty(t, rec.Header().Values("Set-Cookie"))
			redeemNativeBootstrapTestProof(t, h, router, original)
		})
	}
}

func TestNativeBootstrapInvalidHeaderNeverFallsBackToBrowserOrCookie(t *testing.T) {
	h, router := newNativeBootstrapTestHandler(t)
	h.Encrypted.Store(true)
	const browserCookie = "valid-browser-session"
	h.sessions.Store(browserCookie, time.Now().Add(time.Hour))
	valid := nativeBootstrapTestAdmission(t, h, router)
	cases := map[string][]string{
		"empty": {""}, "missing value": {}, "malformed": {"invalid"},
		"unissued":        {base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))},
		"duplicate valid": {valid, valid}, "mixed valid invalid": {valid, "invalid"},
		"comma joined": {valid + "," + valid},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/v1/dashboard/auth/check", "/v1/dashboard/memory/list", "/v1/dashboard/health"} {
				req := nativeBootstrapTestRequest(http.MethodGet, path, nil, "")
				req.Header[http.CanonicalHeaderKey(nativeSessionHeader)] = values
				req.Header.Set("Origin", h.NativeBinding.Origin)
				req.Header.Set("Sec-Fetch-Site", "same-origin")
				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: browserCookie})
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			}
		})
	}
	// A browser request with no native credential remains unchanged.
	req := nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/memory/list", nil, "")
	req.Header.Set("Origin", h.NativeBinding.Origin)
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: browserCookie})
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func loginNativeBootstrapTestVault(t *testing.T, h *DashboardHandler, router http.Handler, token string) *http.Cookie {
	t.Helper()
	const passphrase = "native-test-vault-passphrase"
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(h.VaultKeyPath, passphrase))
	h.Encrypted.Store(true)
	wrong := httptest.NewRecorder()
	router.ServeHTTP(wrong, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/auth/login", []byte(`{"passphrase":"wrong"}`), token))
	require.Equal(t, http.StatusUnauthorized, wrong.Code, wrong.Body.String())
	require.Empty(t, wrong.Header().Values("Set-Cookie"))
	require.True(t, h.NativeBootstrap.Verify(token, h.NativeBinding), "wrong passphrase must leave transport available for retry")
	login := httptest.NewRecorder()
	router.ServeHTTP(login, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/auth/login", nativeBootstrapJSON(t, map[string]string{"passphrase": passphrase}), token))
	require.Equal(t, http.StatusOK, login.Code, login.Body.String())
	for _, cookie := range login.Result().Cookies() {
		if cookie.Name == sessionCookieName && cookie.Value != "" {
			return cookie
		}
	}
	t.Fatal("successful vault login did not set a session")
	return nil
}

func TestNativeBootstrapVaultCookieRequiresMatchingAdmission(t *testing.T) {
	h, router := newNativeBootstrapTestHandler(t)
	token := nativeBootstrapTestAdmission(t, h, router)
	cookie := loginNativeBootstrapTestVault(t, h, router, token)
	other := nativeBootstrapTestAdmission(t, h, router)
	for _, shape := range []struct {
		name, token string
		browser     bool
		status      int
	}{
		{"matching", token, false, http.StatusOK},
		{"missing", "", false, http.StatusUnauthorized},
		{"different admission", other, false, http.StatusUnauthorized},
		{"browser downgrade", "", true, http.StatusUnauthorized},
	} {
		t.Run(shape.name, func(t *testing.T) {
			req := nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/memory/list", nil, shape.token)
			req.AddCookie(cookie)
			if shape.browser {
				req.Header.Set("Origin", h.NativeBinding.Origin)
				req.Header.Set("Sec-Fetch-Site", "same-origin")
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, shape.status, rec.Code, rec.Body.String())
		})
	}
	// A headerless auth/check must not report a stripped native cookie as authenticated.
	req := nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/auth/check", nil, "")
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var state map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &state))
	require.Equal(t, false, state["authenticated"])
}

func TestNativeBootstrapLockAndExplicitRevokeInvalidateAdmission(t *testing.T) {
	for _, path := range []string{"/v1/dashboard/auth/lock", "/v1/dashboard/native/revoke"} {
		t.Run(path, func(t *testing.T) {
			h, router := newNativeBootstrapTestHandler(t)
			token := nativeBootstrapTestAdmission(t, h, router)
			cookie := loginNativeBootstrapTestVault(t, h, router, token)
			const independent = "independent-browser-session"
			h.sessions.Store(independent, time.Now().Add(time.Hour))
			req := nativeBootstrapTestRequest(http.MethodPost, path, nil, token)
			req.AddCookie(cookie)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.False(t, h.NativeBootstrap.Verify(token, h.NativeBinding))
			require.True(t, h.validSession(independent), "locking native must not revoke independent browser session")
			req = nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/memory/list", nil, token)
			req.AddCookie(cookie)
			rec = httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code)
			req = nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/memory/list", nil, "")
			req.Header.Set("Origin", h.NativeBinding.Origin)
			req.Header.Set("Sec-Fetch-Site", "same-origin")
			req.AddCookie(cookie)
			rec = httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code, "revoked native cookie may not become a browser session")
		})
	}
}

func TestNativeBootstrapRevokeRequiresExplicitValidAdmission(t *testing.T) {
	h, router := newNativeBootstrapTestHandler(t)
	for _, token := range []string{"", "invalid"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/revoke", nil, token))
		require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
	}
	// Transport revocation is permitted before vault login.
	token := nativeBootstrapTestAdmission(t, h, router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/revoke", nil, token))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.False(t, h.NativeBootstrap.Verify(token, h.NativeBinding))
}

func TestNativeBootstrapDisabledDaemonFailsClosed(t *testing.T) {
	h, _ := newTestHandler(t)
	router := chi.NewRouter()
	h.RegisterRoutes(router)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/native/redeem", []byte(`{}`), ""))
	require.Equal(t, http.StatusNotFound, rec.Code)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/auth/check", nil, "invalid"))
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// An established stream has already passed the router's authentication checks.
// Revocation therefore must cancel that request, not only deny its next poll.
func TestNativeBootstrapRevocationAndDrainCancelEstablishedSSE(t *testing.T) {
	for _, mode := range []string{"revoke", "drain"} {
		t.Run(mode, func(t *testing.T) {
			h, router := newNativeBootstrapTestHandler(t)
			h.Encrypted.Store(true)
			token := nativeBootstrapTestAdmission(t, h, router)
			const cookie = "stream-vault-session"
			h.sessions.Store(cookie, time.Now().Add(time.Hour))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/events", nil, token).WithContext(ctx)
			req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: cookie})
			rec := httptest.NewRecorder()
			completed := make(chan struct{})
			go func() { defer close(completed); router.ServeHTTP(rec, req) }()
			require.Eventually(t, func() bool { return h.SSE.ClientCount() == 1 }, time.Second, 5*time.Millisecond, "SSE did not pass production route gates")
			if mode == "revoke" {
				h.NativeBootstrap.Revoke(token)
			} else {
				h.NativeBootstrap.Invalidate()
			}
			select {
			case <-completed:
			case <-time.After(2500 * time.Millisecond):
				cancel()
				<-completed
				t.Fatal("native admission retirement did not cancel established SSE")
			}
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Zero(t, h.SSE.ClientCount(), "native stream subscription leaked after cancellation")
			require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
		})
	}
}

func TestNativeBootstrapConcurrentRevocationNeverDowngradesVaultCookie(t *testing.T) {
	h, router := newNativeBootstrapTestHandler(t)
	h.Encrypted.Store(true)
	headerless := nativeBootstrapTestRequest(http.MethodGet, "/v1/dashboard/auth/check", nil, "")
	// Stress the cross-map boundary without repeating expensive passphrase work:
	// issueDashboardSession is the exact successful-login cookie publication path.
	for range 100 {
		token := nativeBootstrapTestAdmission(t, h, router)
		issue := h.nativeSessionGate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.issueDashboardSession(w, r) }))
		response := httptest.NewRecorder()
		issue.ServeHTTP(response, nativeBootstrapTestRequest(http.MethodPost, "/v1/dashboard/auth/login", nil, token))
		cookies := response.Result().Cookies()
		require.Len(t, cookies, 1)
		cookie := cookies[0].Value
		var readers sync.WaitGroup
		started := make(chan struct{}, 16)
		begin := make(chan struct{})
		var downgraded atomic.Bool
		for range 16 {
			readers.Add(1)
			go func() {
				defer readers.Done()
				<-begin
				started <- struct{}{}
				for range 1000 {
					if h.validSessionForRequest(cookie, headerless) {
						downgraded.Store(true)
						return
					}
				}
			}()
		}
		close(begin)
		for range 16 {
			<-started
		}
		h.revokeNativeSession(token)
		readers.Wait()
		require.False(t, downgraded.Load(), "revocation briefly promoted a bound native cookie to an ordinary browser session")
	}
}
