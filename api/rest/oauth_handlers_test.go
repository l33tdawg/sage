package rest

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// newOAuthRouter spins up a real SQLite-backed OAuth handler on a chi router.
// `authed` controls whether IsRequestAuthenticated returns true. Use
// `redirectURL` to assert the unauthenticated-redirect path.
func newOAuthRouter(t *testing.T, authed bool, redirectURL string) (*OAuthHandler, http.Handler, *store.SQLiteStore) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "oauth.db")
	memStore, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = memStore.Close() })

	checker := func(_ *http.Request) (bool, string) { return authed, redirectURL }
	h := NewOAuthHandler(memStore, checker, func(_ *http.Request) string { return "https://sage.test" })
	r := chi.NewRouter()
	MountOAuthRoutes(r, h)
	return h, r, memStore
}

// pkceChallenge returns the S256 base64url-no-pad challenge for the verifier.
func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func TestOAuth_DiscoveryDocShape(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var doc map[string]any
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&doc))

	assert.Equal(t, "https://sage.test", doc["issuer"])
	assert.Equal(t, "https://sage.test/oauth/authorize", doc["authorization_endpoint"])
	assert.Equal(t, "https://sage.test/oauth/token", doc["token_endpoint"])
	assert.ElementsMatch(t, []any{"code"}, doc["response_types_supported"])
	assert.ElementsMatch(t, []any{"S256"}, doc["code_challenge_methods_supported"])
	assert.ElementsMatch(t, []any{"authorization_code"}, doc["grant_types_supported"])
	// DCR not implemented: registration_endpoint must NOT be present.
	_, hasReg := doc["registration_endpoint"]
	assert.False(t, hasReg, "v6.7.2 doesn't advertise registration_endpoint (DCR not implemented)")
}

func TestOAuth_Authorize_MissingParams_400(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	cases := []string{
		"",                                                                    // no params at all
		"?client_id=chatgpt",                                                  // missing redirect_uri
		"?client_id=chatgpt&redirect_uri=https://x/cb",                        // missing challenge
		"?client_id=chatgpt&redirect_uri=https://x/cb&code_challenge=abc&code_challenge_method=plain", // bad method
	}
	for _, q := range cases {
		req := httptest.NewRequest(http.MethodGet, "/oauth/authorize"+q, nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code, "query=%s", q)
	}
}

func TestOAuth_Authorize_Unauthed_RedirectsToLogin(t *testing.T) {
	_, r, _ := newOAuthRouter(t, false, "/ui/?next=/oauth/authorize?stub")

	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id=chatgpt&redirect_uri=https://x/cb&code_challenge=abc&code_challenge_method=S256&response_type=code",
		nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusFound, rr.Code)
	assert.Contains(t, rr.Header().Get("Location"), "/ui/?next=")
}

func TestOAuth_Authorize_GET_RendersConsent(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id=chatgpt&redirect_uri=https://x/cb&code_challenge=abc&code_challenge_method=S256&response_type=code&state=xyz",
		nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "Authorize MCP Client")
	assert.Contains(t, body, "chatgpt")
	assert.Contains(t, body, "https://x/cb")
	assert.Contains(t, rr.Header().Get("Content-Type"), "text/html")
}

func TestOAuth_Authorize_POST_RedirectsWithCode(t *testing.T) {
	_, r, memStore := newOAuthRouter(t, true, "")

	verifier := "verifier-with-enough-length-aaaaaa-bbbbbb"
	challenge := pkceChallenge(verifier)

	form := url.Values{}
	form.Set("agent_id", strings.Repeat("a", 64))
	form.Set("token_name", "chatgpt-test")

	authURL := "/oauth/authorize?client_id=chatgpt&redirect_uri=https%3A%2F%2Fchat.openai.com%2Fcb&code_challenge=" +
		challenge + "&code_challenge_method=S256&response_type=code&state=opaque-state"
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusFound, rr.Code, "body=%s", rr.Body.String())
	loc := rr.Header().Get("Location")
	require.Contains(t, loc, "https://chat.openai.com/cb?")
	assert.Contains(t, loc, "code=")
	assert.Contains(t, loc, "state=opaque-state")

	// Parse the code out for the token-exchange test.
	parsed, err := url.Parse(loc)
	require.NoError(t, err)
	code := parsed.Query().Get("code")
	require.NotEmpty(t, code)

	// Confirm a mcp_tokens row was minted.
	tokens, err := memStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, tokens, 1)
	assert.Equal(t, "chatgpt-test", tokens[0].Name)
	assert.Equal(t, strings.Repeat("a", 64), tokens[0].AgentID)
}

func TestOAuth_Token_Happy(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	verifier := "verifier-token-happy-path-aaaaaa-bbbbbb"
	challenge := pkceChallenge(verifier)
	redirect := "https://chat.openai.com/cb"

	// Drive the /authorize POST to mint the code.
	form := url.Values{}
	form.Set("agent_id", strings.Repeat("b", 64))
	authURL := "/oauth/authorize?client_id=chatgpt&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + challenge + "&code_challenge_method=S256&response_type=code&state=happy"
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusFound, rr.Code)
	parsed, _ := url.Parse(rr.Header().Get("Location"))
	code := parsed.Query().Get("code")

	// Now exchange.
	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	tokenForm.Set("code", code)
	tokenForm.Set("code_verifier", verifier)
	tokenForm.Set("redirect_uri", redirect)
	tokenForm.Set("client_id", "chatgpt")

	tokReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokRR := httptest.NewRecorder()
	r.ServeHTTP(tokRR, tokReq)

	require.Equal(t, http.StatusOK, tokRR.Code, "body=%s", tokRR.Body.String())
	var resp map[string]any
	require.NoError(t, json.NewDecoder(tokRR.Body).Decode(&resp))
	assert.NotEmpty(t, resp["access_token"])
	assert.Equal(t, "Bearer", resp["token_type"])
}

func TestOAuth_Token_ReusedCode_400(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	verifier := "verifier-for-reuse-test-aaaaaa-bbbbbb"
	challenge := pkceChallenge(verifier)
	redirect := "https://chat.openai.com/cb"

	// Mint a code.
	form := url.Values{}
	form.Set("agent_id", strings.Repeat("c", 64))
	authURL := "/oauth/authorize?client_id=chatgpt&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + challenge + "&code_challenge_method=S256&response_type=code"
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	parsed, _ := url.Parse(rr.Header().Get("Location"))
	code := parsed.Query().Get("code")

	// First redeem succeeds.
	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	tokenForm.Set("code", code)
	tokenForm.Set("code_verifier", verifier)
	tokenForm.Set("redirect_uri", redirect)
	tokenForm.Set("client_id", "chatgpt")

	tokReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.ServeHTTP(httptest.NewRecorder(), tokReq)

	// Second redeem fails 400.
	tokReq2 := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq2.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokRR2 := httptest.NewRecorder()
	r.ServeHTTP(tokRR2, tokReq2)

	assert.Equal(t, http.StatusBadRequest, tokRR2.Code)
	assert.Contains(t, tokRR2.Body.String(), "invalid_grant")
}

func TestOAuth_Token_BadVerifier_400(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	correctVerifier := "correct-verifier-correct-verifier-aaaaaa"
	challenge := pkceChallenge(correctVerifier)
	redirect := "https://chat.openai.com/cb"

	form := url.Values{}
	form.Set("agent_id", strings.Repeat("d", 64))
	authURL := "/oauth/authorize?client_id=chatgpt&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + challenge + "&code_challenge_method=S256&response_type=code"
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	parsed, _ := url.Parse(rr.Header().Get("Location"))
	code := parsed.Query().Get("code")

	// Submit with WRONG verifier.
	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	tokenForm.Set("code", code)
	tokenForm.Set("code_verifier", "totally-different-verifier-aaaaaa-bbbb")
	tokenForm.Set("redirect_uri", redirect)
	tokenForm.Set("client_id", "chatgpt")

	tokReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokRR := httptest.NewRecorder()
	r.ServeHTTP(tokRR, tokReq)

	assert.Equal(t, http.StatusBadRequest, tokRR.Code)
	assert.Contains(t, tokRR.Body.String(), "invalid_grant")
	assert.Contains(t, tokRR.Body.String(), "PKCE")
}

func TestOAuth_Token_RedirectMismatch_400(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	verifier := "verifier-redirect-mismatch-aaaaaa-bbbbbb"
	challenge := pkceChallenge(verifier)
	originalRedirect := "https://chat.openai.com/cb"

	form := url.Values{}
	form.Set("agent_id", strings.Repeat("e", 64))
	authURL := "/oauth/authorize?client_id=chatgpt&redirect_uri=" + url.QueryEscape(originalRedirect) +
		"&code_challenge=" + challenge + "&code_challenge_method=S256&response_type=code"
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	parsed, _ := url.Parse(rr.Header().Get("Location"))
	code := parsed.Query().Get("code")

	// Tamper redirect_uri at /token.
	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	tokenForm.Set("code", code)
	tokenForm.Set("code_verifier", verifier)
	tokenForm.Set("redirect_uri", "https://attacker.example.com/cb")
	tokenForm.Set("client_id", "chatgpt")

	tokReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokRR := httptest.NewRecorder()
	r.ServeHTTP(tokRR, tokReq)

	assert.Equal(t, http.StatusBadRequest, tokRR.Code)
	assert.Contains(t, tokRR.Body.String(), "invalid_grant")
}

func TestOAuth_Token_BadGrantType_400(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "client_credentials")
	tokenForm.Set("code", "anything")

	tokReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokRR := httptest.NewRecorder()
	r.ServeHTTP(tokRR, tokReq)

	assert.Equal(t, http.StatusBadRequest, tokRR.Code)
	assert.Contains(t, tokRR.Body.String(), "unsupported_grant_type")
}

func TestOAuth_Token_MissingFields_400(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	tokenForm := url.Values{}
	tokenForm.Set("grant_type", "authorization_code")
	// No code, no verifier, no redirect_uri.

	tokReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokRR := httptest.NewRecorder()
	r.ServeHTTP(tokRR, tokReq)

	assert.Equal(t, http.StatusBadRequest, tokRR.Code)
	assert.Contains(t, tokRR.Body.String(), "invalid_request")
}

func TestOAuth_Token_GET_405(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	// chi will route POST-only registrations to method-not-allowed for other verbs.
	req := httptest.NewRequest(http.MethodGet, "/oauth/token", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestOAuth_Authorize_BadAgentID_RerendersConsent(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")

	verifier := "verifier-bad-agent-id-test-aaaaaa-bbbb"
	challenge := pkceChallenge(verifier)

	form := url.Values{}
	form.Set("agent_id", "too-short")
	authURL := "/oauth/authorize?client_id=chatgpt&redirect_uri=https%3A%2F%2Fx%2Fcb&code_challenge=" +
		challenge + "&code_challenge_method=S256&response_type=code"
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "should re-render the consent form, not redirect")
	assert.Contains(t, rr.Body.String(), "agent_id must be a 64-char hex")
}
