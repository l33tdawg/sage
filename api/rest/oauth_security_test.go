package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/web"
)

type cancelingAuthCodeStore struct {
	*store.SQLiteStore
	cancel               context.CancelFunc
	revokeSawCanceledCtx bool
}

func (s *cancelingAuthCodeStore) IssueAuthCode(context.Context, string, string, string, string, string, string, string, string, time.Duration) error {
	s.cancel()
	return errors.New("injected auth-code persistence failure")
}

func (s *cancelingAuthCodeStore) RevokeMCPToken(ctx context.Context, id string) error {
	s.revokeSawCanceledCtx = ctx.Err() != nil
	return s.SQLiteStore.RevokeMCPToken(ctx, id)
}

type redirectDisappearsStore struct {
	*store.SQLiteStore
	getCalls       int
	deleteCodeCall int
	revokeCall     int
	issuedCode     string
}

func (s *redirectDisappearsStore) GetOAuthClient(ctx context.Context, clientID string) (*store.OAuthClient, error) {
	s.getCalls++
	// GET /authorize resolves once, then the POST resolves before issuance and
	// again at the redirect sink. Fail only at that final sink check.
	if s.getCalls >= 4 {
		return nil, store.ErrOAuthClientNotFound
	}
	return s.SQLiteStore.GetOAuthClient(ctx, clientID)
}

func (s *redirectDisappearsStore) DeleteAuthCode(ctx context.Context, code string) error {
	s.deleteCodeCall++
	return s.SQLiteStore.DeleteAuthCode(ctx, code)
}

func (s *redirectDisappearsStore) IssueAuthCode(ctx context.Context, code, tokenID, codeChallenge, codeChallengeMethod, redirectURI, clientID, state, bearer string, ttl time.Duration) error {
	s.issuedCode = code
	return s.SQLiteStore.IssueAuthCode(ctx, code, tokenID, codeChallenge, codeChallengeMethod, redirectURI, clientID, state, bearer, ttl)
}

func (s *redirectDisappearsStore) RevokeMCPToken(ctx context.Context, id string) error {
	s.revokeCall++
	return s.SQLiteStore.RevokeMCPToken(ctx, id)
}

func signOAuthRequestsAs(t *testing.T, next http.Handler, priv ed25519.PrivateKey) http.Handler {
	t.Helper()
	pub := priv.Public().(ed25519.PublicKey)
	agentID := hex.EncodeToString(pub)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body []byte
		var err error
		if r.Body != nil {
			body, err = io.ReadAll(r.Body)
			require.NoError(t, err)
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		ts := time.Now().Unix()
		nonce := make([]byte, 16)
		_, err = rand.Read(nonce)
		require.NoError(t, err)
		r.Header.Set("X-Agent-ID", agentID)
		r.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
		r.Header.Set("X-Nonce", hex.EncodeToString(nonce))
		r.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(priv, r.Method, r.URL.RequestURI(), body, ts, nonce)))
		next.ServeHTTP(w, r)
	})
}

func TestOAuth_EncryptionOffRemoteCannotReachConsentOrMintCode(t *testing.T) {
	h, r, tokenStore := newOAuthRouter(t, true, "")
	dashboard := web.NewDashboardHandler(tokenStore, "test")
	dashboard.NodeOperatorAgentID = strings.Repeat("a", 64)
	// Encryption is off by default. The actual production checker must still
	// distinguish a local operator browser from a tunnel/remote visitor.
	h.IsAuthed = dashboard.IsRequestAuthenticated
	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)
	challenge := pkceChallenge("remote-attacker-verifier-aaaaaaaa-bbbbbbbb")
	authURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + challenge +
		"&code_challenge_method=S256&response_type=code&state=remote"

	remote := func(req *http.Request) {
		req.RemoteAddr = "203.0.113.42:44321"
		req.Host = "sage.example.com"
	}
	for _, browser := range []bool{false, true} {
		getReq := httptest.NewRequest(http.MethodGet, authURL, nil)
		remote(getReq)
		if browser {
			getReq.Header.Set("Sec-Fetch-Site", "cross-site")
		}
		getRR := httptest.NewRecorder()
		r.ServeHTTP(getRR, getReq)
		require.Equal(t, http.StatusFound, getRR.Code)
		require.NotContains(t, getRR.Body.String(), `name="csrf_nonce"`)
	}

	form := url.Values{"csrf_nonce": {"forged"}, "token_name": {"attacker"}}
	postReq := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	postReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	remote(postReq)
	postRR := httptest.NewRecorder()
	r.ServeHTTP(postRR, postReq)
	require.Equal(t, http.StatusFound, postRR.Code)
	require.NotEqual(t, redirect, postRR.Header().Get("Location"), "remote request must not receive a client authorization redirect")

	rows, err := tokenStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Empty(t, rows, "remote encryption-off authorize flow must mint no bearer")

	tokenForm := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {"guessed-code"},
		"code_verifier": {"remote-attacker-verifier-aaaaaaaa-bbbbbbbb"},
		"redirect_uri":  {redirect},
		"client_id":     {clientID},
	}
	tokenReq := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(tokenForm.Encode()))
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	remote(tokenReq)
	tokenRR := httptest.NewRecorder()
	r.ServeHTTP(tokenRR, tokenReq)
	require.Equal(t, http.StatusBadRequest, tokenRR.Code)
}

func TestOAuth_EncryptionOffUnsignedLoopbackHeadersCannotConsent(t *testing.T) {
	h, r, tokenStore := newOAuthRouter(t, true, "")
	dashboard := web.NewDashboardHandler(tokenStore, "test")
	dashboard.NodeOperatorAgentID = strings.Repeat("a", 64)
	h.IsAuthed = dashboard.IsRequestAuthenticated
	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)
	local := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.RemoteAddr = "127.0.0.1:43210"
		req.Host = "localhost:8080"
		req.Header.Set("Origin", "http://localhost:8080")
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		r.ServeHTTP(w, req)
	})
	authURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + pkceChallenge("local-operator-verifier-aaaaaaaa-bbbbbbbb") +
		"&code_challenge_method=S256&response_type=code&state=local"
	for _, forgeOperatorHeader := range []bool{false, true} {
		req := httptest.NewRequest(http.MethodGet, authURL, nil)
		if forgeOperatorHeader {
			req.Header.Set("X-Agent-ID", dashboard.NodeOperatorAgentID)
		}
		rr := httptest.NewRecorder()
		local.ServeHTTP(rr, req)
		require.Equal(t, http.StatusFound, rr.Code)
		require.NotContains(t, rr.Body.String(), `name="csrf_nonce"`)
	}
	// Even possession of a structurally valid consent form/CSRF nonce cannot
	// turn spoofable loopback and browser headers into operator authority.
	p := authorizeFormParams{
		ClientID: clientID, RedirectURI: redirect, State: "local",
		CodeChallenge:       pkceChallenge("local-operator-verifier-aaaaaaaa-bbbbbbbb"),
		CodeChallengeMethod: "S256", ResponseType: "code", Scope: "mcp",
	}
	form := url.Values{
		"client_id": {p.ClientID}, "redirect_uri": {p.RedirectURI}, "state": {p.State},
		"code_challenge": {p.CodeChallenge}, "code_challenge_method": {p.CodeChallengeMethod},
		"response_type": {p.ResponseType}, "scope": {p.Scope}, "csrf_nonce": {h.signCSRFNonce(p)},
		"token_name": {"unsigned-loopback"},
	}
	post := httptest.NewRequest(http.MethodPost, "/oauth/authorize", strings.NewReader(form.Encode()))
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	postRR := httptest.NewRecorder()
	local.ServeHTTP(postRR, post)
	require.Equal(t, http.StatusFound, postRR.Code)
	require.NotEqual(t, redirect, postRR.Header().Get("Location"))

	rows, err := tokenStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestOAuth_EncryptionOffArbitrarySignedAgentCannotConsent(t *testing.T) {
	h, r, tokenStore := newOAuthRouter(t, true, "")
	operatorPub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dashboard := web.NewDashboardHandler(tokenStore, "test")
	dashboard.NodeOperatorAgentID = hex.EncodeToString(operatorPub)
	h.NodeOperatorAgentID = dashboard.NodeOperatorAgentID
	h.IsAuthed = dashboard.IsRequestAuthenticated
	_, attackerPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signedAttacker := signOAuthRequestsAs(t, r, attackerPriv)

	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)
	authURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + pkceChallenge("attacker-verifier-aaaaaaaa-bbbbbbbb") +
		"&code_challenge_method=S256&response_type=code&state=attacker"
	rr := httptest.NewRecorder()
	signedAttacker.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, authURL, nil))
	require.Equal(t, http.StatusFound, rr.Code)
	rows, err := tokenStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestOAuth_EncryptionOffExactOperatorSignatureFullPKCE(t *testing.T) {
	h, r, tokenStore := newOAuthRouter(t, true, "")
	operatorPub, operatorPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	operatorID := hex.EncodeToString(operatorPub)
	dashboard := web.NewDashboardHandler(tokenStore, "test")
	dashboard.NodeOperatorAgentID = operatorID
	h.NodeOperatorAgentID = operatorID
	h.IsAuthed = dashboard.IsRequestAuthenticated
	signedOperator := signOAuthRequestsAs(t, r, operatorPriv)

	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)
	verifier := "operator-verifier-aaaaaaaa-bbbbbbbb-cccccccc"
	code := mintAuthCode(t, signedOperator, clientID, redirect, pkceChallenge(verifier), "operator", "")

	form := url.Values{
		"grant_type": {"authorization_code"}, "code": {code},
		"code_verifier": {verifier}, "redirect_uri": {redirect}, "client_id": {clientID},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	require.NotEmpty(t, response.AccessToken)

	rows, err := tokenStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, operatorID, rows[0].AgentID)
}

func TestOAuth_IssueAuthCodeFailureCleansUpWithCanceledRequestContext(t *testing.T) {
	h, r, tokenStore := newOAuthRouter(t, true, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wrapped := &cancelingAuthCodeStore{SQLiteStore: tokenStore, cancel: cancel}
	h.Store = wrapped

	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)
	challenge := pkceChallenge("cancel-cleanup-verifier-aaaaaaaa-bbbbbbbb")
	authURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + challenge + "&code_challenge_method=S256&response_type=code&state=cleanup"
	nonce := fetchCSRFNonce(t, r, authURL)
	form := url.Values{"token_name": {"cleanup"}, "csrf_nonce": {nonce}}
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode())).WithContext(ctx)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusInternalServerError, rr.Code)
	require.False(t, wrapped.revokeSawCanceledCtx, "cleanup must use independent bounded context")
	rows, err := tokenStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.False(t, rows[0].RevokedAt.IsZero(), "failed auth-code issuance must not leave a live bearer")
}

func TestOAuth_LateRedirectValidationRevokesBearerAndDeletesCode(t *testing.T) {
	h, r, tokenStore := newOAuthRouter(t, true, "")
	wrapped := &redirectDisappearsStore{SQLiteStore: tokenStore}
	h.Store = wrapped

	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)
	challenge := pkceChallenge("redirect-race-verifier-aaaaaaaa-bbbbbbbb")
	authURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=" + challenge + "&code_challenge_method=S256&response_type=code&state=redirect-race"
	nonce := fetchCSRFNonce(t, r, authURL)
	form := url.Values{"token_name": {"redirect-race"}, "csrf_nonce": {nonce}}
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Equal(t, 1, wrapped.deleteCodeCall, "a code containing the bearer plaintext must be erased")
	require.Equal(t, 1, wrapped.revokeCall, "late redirect validation failure must revoke the bearer")
	_, err := tokenStore.RedeemAuthCode(context.Background(), wrapped.issuedCode, "redirect-race-verifier-aaaaaaaa-bbbbbbbb", redirect, clientID)
	require.ErrorIs(t, err, store.ErrAuthCodeNotFound, "the issued code must be deleted, not merely left unredeemable")
	rows, err := tokenStore.ListMCPTokens(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.False(t, rows[0].RevokedAt.IsZero())
}

func TestOAuth_TokenRejectsSameRedirectDifferentClient(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	redirect := "https://chat.openai.com/cb"
	clientA := registerOAuthClient(t, r, redirect)
	clientB := registerOAuthClient(t, r, redirect)
	verifier := "same-redirect-client-binding-verifier-aaaaaaaa"
	code := mintAuthCode(t, r, clientA, redirect, pkceChallenge(verifier), "client-binding", "")

	form := url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "code_verifier": {verifier},
		"redirect_uri": {redirect}, "client_id": {clientB},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "invalid_grant")

	form.Set("client_id", clientA)
	req = httptest.NewRequest(http.MethodPost, "/oauth/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, "client mismatch must not consume the code")
}

// TestOAuth_Register_RejectsBadRedirects confirms /oauth/register refuses
// non-https redirects, userinfo, fragments, and empty input.
func TestOAuth_Register_RejectsBadRedirects(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	cases := []map[string]any{
		{"redirect_uris": []string{}},                                   // empty
		{"redirect_uris": []string{"http://chat.openai.com/cb"}},        // http
		{"redirect_uris": []string{"https://user:pass@chat.openai/cb"}}, // userinfo
		{"redirect_uris": []string{"https://chat.openai.com/cb#x"}},     // fragment
		{"redirect_uris": []string{"not-a-url"}},                        // not absolute
	}
	for _, body := range cases {
		buf, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(string(buf)))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusBadRequest, rr.Code, "body=%+v -> %s", body, rr.Body.String())
		assert.Contains(t, rr.Body.String(), "invalid_redirect_uri")
	}
}

// TestOAuth_Authorize_RejectsUnregisteredClient is the C1 fix: an attacker
// cannot drive /oauth/authorize with a fabricated client_id.
func TestOAuth_Authorize_RejectsUnregisteredClient(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id=never-registered&redirect_uri=https://x/cb&code_challenge=abc&code_challenge_method=S256&response_type=code&state=s",
		nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "client_id is not registered")
}

// TestOAuth_Authorize_RejectsUnregisteredRedirect is the open-redirect close.
// A registered client trying to redirect to an attacker URL must be rejected.
func TestOAuth_Authorize_RejectsUnregisteredRedirect(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	clientID := registerOAuthClient(t, r, "https://chat.openai.com/cb")

	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id="+url.QueryEscape(clientID)+
			"&redirect_uri=https://attacker.example/grab&code_challenge=abc&code_challenge_method=S256&response_type=code&state=s",
		nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "redirect_uri does not match")
}

// TestOAuth_Authorize_StateMandatory confirms missing state -> 400.
func TestOAuth_Authorize_StateMandatory(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	clientID := registerOAuthClient(t, r, "https://x/cb")

	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id="+url.QueryEscape(clientID)+
			"&redirect_uri=https://x/cb&code_challenge=abc&code_challenge_method=S256&response_type=code",
		nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "state is required")
}

// TestOAuth_Authorize_POST_RejectsMissingCSRFNonce confirms a POST without a
// signed nonce re-renders the consent screen with an error rather than
// minting a code.
func TestOAuth_Authorize_POST_RejectsMissingCSRFNonce(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)

	authURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=abc&code_challenge_method=S256&response_type=code&state=csrf-test"

	form := url.Values{}
	form.Set("agent_id", strings.Repeat("a", 64))
	// no csrf_nonce
	req := httptest.NewRequest(http.MethodPost, authURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "should re-render consent, not 302")
	assert.Contains(t, rr.Body.String(), "Consent request could not be verified")
}

// TestOAuth_Authorize_POST_RejectsForgedCSRFNonce confirms a nonce signed
// for a different /authorize request fails verification.
func TestOAuth_Authorize_POST_RejectsForgedCSRFNonce(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	redirect := "https://chat.openai.com/cb"
	clientID := registerOAuthClient(t, r, redirect)

	// Pull a nonce for one set of parameters, attempt to use it for a
	// different state value — verifyCSRFNonce must reject.
	nonceURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=abc&code_challenge_method=S256&response_type=code&state=original"
	nonce := fetchCSRFNonce(t, r, nonceURL)

	postURL := "/oauth/authorize?client_id=" + url.QueryEscape(clientID) +
		"&redirect_uri=" + url.QueryEscape(redirect) +
		"&code_challenge=abc&code_challenge_method=S256&response_type=code&state=DIFFERENT"
	form := url.Values{}
	form.Set("agent_id", strings.Repeat("a", 64))
	form.Set("csrf_nonce", nonce)
	req := httptest.NewRequest(http.MethodPost, postURL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "should re-render consent")
	assert.Contains(t, rr.Body.String(), "Consent request could not be verified")
}

// TestOAuth_Authorize_NoAgentRosterPreAuth confirms a tunnel-exposed visitor
// without a dashboard cookie sees only an 8-char hex prefix of the node
// operator's pubkey, never the full identity or roster.
func TestOAuth_Authorize_NoAgentRosterPreAuth(t *testing.T) {
	// HasDashboardCookie is left nil — every request appears unauthenticated
	// to the consent renderer regardless of IsAuthed's verdict.
	_, r, _ := newOAuthRouter(t, true, "")
	clientID := registerOAuthClient(t, r, "https://x/cb")

	req := httptest.NewRequest(http.MethodGet,
		"/oauth/authorize?client_id="+url.QueryEscape(clientID)+
			"&redirect_uri=https://x/cb&code_challenge=abc&code_challenge_method=S256&response_type=code&state=s",
		nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	body := rr.Body.String()
	require.Equal(t, http.StatusOK, rr.Code)
	// No picker / dropdown / free-text input for agent_id.
	assert.NotContains(t, body, `<select name="agent_id"`)
	assert.NotContains(t, body, `name="agent_id"`)
	// The operator label is the 8-char prefix of the test handler's
	// NodeOperatorAgentID (64×'a' → "aaaaaaaa…").
	assert.Contains(t, body, "aaaaaaaa")
	// The full pubkey must NOT be rendered to an unauthenticated visitor.
	assert.NotContains(t, body, strings.Repeat("a", 64))
}

// TestOAuth_Register_RateLimited confirms /oauth/register limits abuse from
// a single remote address.
func TestOAuth_Register_RateLimited(t *testing.T) {
	_, r, _ := newOAuthRouter(t, true, "")
	body, _ := json.Marshal(map[string]any{
		"redirect_uris": []string{"https://chat.openai.com/cb"},
	})
	limited := false
	for i := 0; i < dcrRegisterPerIPLimit+5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/oauth/register", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.RemoteAddr = "203.0.113.42:1234"
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		if rr.Code == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	assert.True(t, limited, "expected rate-limit response after %d registrations", dcrRegisterPerIPLimit)
}
