package rest

// OAuth 2.0 + PKCE wrapper around SAGE's bearer-token MCP transport.
//
// Why this exists (v6.7.2):
//   ChatGPT's MCP connector form requires Authorization URL + Token URL —
//   the form's red banner explicitly rejects "static bearer in Client ID"
//   as an auth method. SAGE's HTTP MCP transport (v6.7.0/v6.7.1) only
//   accepts a long-lived bearer in the Authorization header. To close the
//   gap without disturbing v6.7.1, this layer presents a standards-track
//   OAuth flow whose `access_token` IS the existing bearer.
//
// Endpoints (mounted at the host root, not under /v1/mcp):
//   GET  /.well-known/oauth-authorization-server  RFC 8414 discovery doc
//   GET  /oauth/authorize                          consent screen + code mint
//   POST /oauth/authorize                          consent submission
//   POST /oauth/token                              code → bearer redemption
//
// Discovery URL placement: ChatGPT auto-discovers from the MCP server URL's
// host. The discovery doc MUST live at the root (`/.well-known/...`), not
// under `/v1/mcp/...` — the host is the OAuth issuer.
//
// Dynamic Client Registration (RFC 7591): NOT implemented in v6.7.2. The
// ChatGPT form's "User-Defined OAuth Client" path doesn't need DCR; the
// metadata simply omits `registration_endpoint`. Future work.
//
// Bearer reuse: at /authorize approval time we mint a fresh bearer via the
// existing IssueMCPToken path (so the operator gets a normal mcp_tokens row
// they can revoke from the dashboard), and stash the plaintext into the
// auth-code row. /token redeems it back out and returns it as access_token.
// Sessions to /v1/mcp/sse continue to use the same Authorization: Bearer
// scheme as before. Zero changes to the bearer-auth middleware.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html/template"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/l33tdawg/sage/internal/store"
)

// AuthCodeTTL is the lifetime of an issued OAuth authorization code. RFC
// 6749 §4.1.2 recommends ≤10 minutes; SAGE uses 5.
const AuthCodeTTL = 5 * time.Minute

// OAuthStore is the storage surface OAuthHandler needs. The full
// SQLiteStore satisfies it.
type OAuthStore interface {
	// Token table — to mint a bearer at /authorize approval and to list the
	// operator's existing tokens for the consent screen dropdown.
	InsertMCPToken(ctx context.Context, id, name, agentID, tokenSHA256 string) error
	ListMCPTokens(ctx context.Context) ([]*store.MCPToken, error)
	// Auth-code table.
	IssueAuthCode(ctx context.Context,
		code, tokenID, codeChallenge, codeChallengeMethod, redirectURI, clientID, state, bearerPlaintext string,
		ttl time.Duration) error
	RedeemAuthCode(ctx context.Context, code, codeVerifier, redirectURI string) (string, error)
}

// OAuthSessionChecker reports whether the inbound request is authenticated
// for the SAGE dashboard. Used by /oauth/authorize to gate the consent
// screen. If `false`, the second return is the location header to redirect
// the user to (typically `/ui/?next=<encoded /oauth/authorize URL>`).
//
// On nodes without encryption enabled, the dashboard considers all requests
// authenticated, so this returns (true, "") and /authorize serves directly.
type OAuthSessionChecker func(r *http.Request) (authenticated bool, loginRedirect string)

// OAuthHandler bundles the OAuth 2.0 endpoints. Built once at server start,
// mounted on the chi router at the host root.
type OAuthHandler struct {
	Store         OAuthStore
	IsAuthed      OAuthSessionChecker
	IssuerBaseURL func(r *http.Request) string // e.g. "https://host:8443" — derived per request
}

// NewOAuthHandler constructs a handler with sensible defaults.
//
// `issuer` may be nil; if so the handler infers the issuer from the inbound
// request (`scheme://host`).
func NewOAuthHandler(s OAuthStore, isAuthed OAuthSessionChecker, issuer func(r *http.Request) string) *OAuthHandler {
	if isAuthed == nil {
		// Default: open. (Test convenience — production mounts the dashboard checker.)
		isAuthed = func(_ *http.Request) (bool, string) { return true, "" }
	}
	if issuer == nil {
		issuer = inferIssuer
	}
	return &OAuthHandler{Store: s, IsAuthed: isAuthed, IssuerBaseURL: issuer}
}

// inferIssuer reconstructs `scheme://host` from the request. Honors
// X-Forwarded-Proto / X-Forwarded-Host for reverse-proxy setups.
func inferIssuer(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if xfp := r.Header.Get("X-Forwarded-Proto"); xfp != "" {
		scheme = xfp
	}
	host := r.Host
	if xfh := r.Header.Get("X-Forwarded-Host"); xfh != "" {
		host = xfh
	}
	return scheme + "://" + host
}

// HandleDiscovery serves the RFC 8414 OAuth Authorization Server Metadata
// document. ChatGPT's MCP connector reads this to auto-populate
// authorization_endpoint and token_endpoint.
func (h *OAuthHandler) HandleDiscovery(w http.ResponseWriter, r *http.Request) {
	issuer := h.IssuerBaseURL(r)
	doc := map[string]any{
		"issuer":                                issuer,
		"authorization_endpoint":                issuer + "/oauth/authorize",
		"token_endpoint":                        issuer + "/oauth/token",
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code"},
		"code_challenge_methods_supported":      []string{"S256"},
		"token_endpoint_auth_methods_supported": []string{"none"},
		"scopes_supported":                      []string{"mcp"},
		// We do NOT advertise registration_endpoint — DCR is not implemented.
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_ = json.NewEncoder(w).Encode(doc)
}

// authorizeFormParams captures the validated /authorize query parameters.
type authorizeFormParams struct {
	ClientID            string
	RedirectURI         string
	State               string
	CodeChallenge       string
	CodeChallengeMethod string
	ResponseType        string
	Scope               string
}

// parseAuthorizeParams reads + validates the /authorize query. Returns a
// problem-detail string for the caller to surface as a 400 if invalid.
func parseAuthorizeParams(r *http.Request) (authorizeFormParams, string) {
	q := r.URL.Query()
	p := authorizeFormParams{
		ClientID:            strings.TrimSpace(q.Get("client_id")),
		RedirectURI:         strings.TrimSpace(q.Get("redirect_uri")),
		State:               q.Get("state"),
		CodeChallenge:       strings.TrimSpace(q.Get("code_challenge")),
		CodeChallengeMethod: strings.ToUpper(strings.TrimSpace(q.Get("code_challenge_method"))),
		ResponseType:        strings.ToLower(strings.TrimSpace(q.Get("response_type"))),
		Scope:               strings.TrimSpace(q.Get("scope")),
	}
	if p.ClientID == "" {
		return p, "client_id is required"
	}
	if p.RedirectURI == "" {
		return p, "redirect_uri is required"
	}
	if _, err := url.Parse(p.RedirectURI); err != nil {
		return p, "redirect_uri must be a valid URL"
	}
	if p.ResponseType == "" {
		p.ResponseType = "code"
	}
	if p.ResponseType != "code" {
		return p, "response_type must be 'code'"
	}
	if p.CodeChallenge == "" {
		return p, "code_challenge is required (PKCE)"
	}
	if p.CodeChallengeMethod == "" {
		p.CodeChallengeMethod = "S256"
	}
	if p.CodeChallengeMethod != "S256" {
		return p, "code_challenge_method must be S256"
	}
	return p, ""
}

// HandleAuthorize serves the consent screen (GET) and processes consent
// submission (POST). The user must be authenticated to the dashboard
// (when encryption is on) — IsAuthed gates that.
func (h *OAuthHandler) HandleAuthorize(w http.ResponseWriter, r *http.Request) {
	params, problem := parseAuthorizeParams(r)
	if problem != "" {
		http.Error(w, problem, http.StatusBadRequest)
		return
	}

	// Dashboard auth gate. If unauthenticated, redirect to the SPA with a
	// `next` param so the SPA login flow round-trips back here on success.
	if h.IsAuthed != nil {
		ok, loginURL := h.IsAuthed(r)
		if !ok {
			if loginURL == "" {
				// Build "/ui/?next=<full /oauth/authorize URL with query>"
				next := r.URL.RequestURI()
				loginURL = "/ui/?next=" + url.QueryEscape(next)
			}
			http.Redirect(w, r, loginURL, http.StatusFound)
			return
		}
	}

	switch r.Method {
	case http.MethodGet:
		h.renderConsent(w, r, params, "")
	case http.MethodPost:
		h.processConsent(w, r, params)
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// consentTemplate is the minimal HTML form. Lists existing active tokens as
// radio choices; offers a "mint a new token" path that takes a name + agent.
//
// We keep the markup deliberately spartan — this page is operator-facing,
// rarely seen, and ChatGPT's redirect destination so it doesn't need to be
// branded.
var consentTemplate = template.Must(template.New("consent").Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Authorize MCP client — SAGE</title>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; max-width: 640px; margin: 4em auto; padding: 0 1em; color: #222; }
  h1 { font-size: 1.4em; margin-bottom: 0.2em; }
  .sub { color: #666; font-size: 0.95em; margin-top: 0; }
  .card { border: 1px solid #ddd; border-radius: 8px; padding: 1em 1.2em; margin: 1em 0; background: #fafafa; }
  label { display: block; padding: 0.4em 0; cursor: pointer; }
  label:hover { background: #f0f0f0; }
  input[type=text], input[type=submit], button { padding: 0.5em 0.8em; font-size: 1em; }
  input[type=submit], button { background: #2b7cd1; color: white; border: 0; border-radius: 4px; cursor: pointer; }
  input[type=submit]:hover, button:hover { background: #1f5fa3; }
  .err { color: #b00020; padding: 0.5em; background: #fde7ea; border-radius: 4px; }
  code { background: #eee; padding: 0.1em 0.3em; border-radius: 3px; font-size: 0.9em; }
  .meta { color: #888; font-size: 0.85em; }
</style>
</head>
<body>
  <h1>Authorize MCP Client</h1>
  <p class="sub">A client wants to connect to your SAGE node as an MCP agent.</p>

  <div class="card">
    <p><strong>Client:</strong> <code>{{.ClientID}}</code></p>
    <p><strong>Redirect URI:</strong> <code>{{.RedirectURI}}</code></p>
    {{if .Scope}}<p><strong>Scope:</strong> <code>{{.Scope}}</code></p>{{end}}
  </div>

  {{if .Error}}<p class="err">{{.Error}}</p>{{end}}

  <form method="POST" action="/oauth/authorize?{{.QueryString}}">
    <h2 style="font-size: 1.1em;">Mint a new bearer for this client</h2>
    <p class="meta">A new <code>mcp_tokens</code> row is created for this connection — you can revoke it at any time from <code>sage-gui mcp-token revoke &lt;id&gt;</code> or the dashboard.</p>
    <p>
      <label>Token name (optional, e.g. "chatgpt-laptop")<br>
        <input type="text" name="token_name" value="" style="width: 100%; box-sizing: border-box;">
      </label>
    </p>
    <p>
      <label>Agent ID (64-char hex ed25519 pubkey)<br>
        <input type="text" name="agent_id" required placeholder="64 hex chars" style="width: 100%; box-sizing: border-box;">
      </label>
    </p>
    <p><button type="submit">Authorize</button></p>
  </form>

  <p class="meta">After authorization, you'll be redirected to {{.RedirectURI}}.<br>
  This consent screen is a thin OAuth wrapper around SAGE's existing bearer-token MCP transport.</p>
</body>
</html>`))

// renderConsent writes the GET-side HTML form.
func (h *OAuthHandler) renderConsent(w http.ResponseWriter, r *http.Request, p authorizeFormParams, errMsg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// Preserve the inbound query so the POST round-trip can read the same
	// PKCE / redirect parameters back out.
	data := struct {
		ClientID    string
		RedirectURI string
		Scope       string
		QueryString string
		Error       string
	}{
		ClientID:    p.ClientID,
		RedirectURI: p.RedirectURI,
		Scope:       p.Scope,
		QueryString: r.URL.RawQuery,
		Error:       errMsg,
	}
	if err := consentTemplate.Execute(w, data); err != nil {
		http.Error(w, "failed to render consent page", http.StatusInternalServerError)
	}
}

// processConsent handles the POST: mint a bearer for the chosen agent, bind
// it to a fresh authorization code, and 302-redirect the user back to the
// client's redirect_uri carrying the code (and original state).
func (h *OAuthHandler) processConsent(w http.ResponseWriter, r *http.Request, p authorizeFormParams) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16) // 64KB cap on consent form bodies
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form body", http.StatusBadRequest)
		return
	}
	agentID := strings.TrimSpace(r.FormValue("agent_id"))
	tokenName := strings.TrimSpace(r.FormValue("token_name"))
	if tokenName == "" {
		tokenName = "oauth-" + p.ClientID
	}

	if len(agentID) != 64 {
		h.renderConsent(w, r, p, "agent_id must be a 64-char hex-encoded ed25519 public key")
		return
	}
	if _, decErr := hex.DecodeString(agentID); decErr != nil {
		h.renderConsent(w, r, p, "agent_id must be hex-encoded")
		return
	}

	// 1. Mint a brand-new bearer via the same code path /v1/mcp/tokens uses.
	bearerRaw := make([]byte, 32)
	if _, err := rand.Read(bearerRaw); err != nil {
		http.Error(w, "failed to generate token", http.StatusInternalServerError)
		return
	}
	bearerPlaintext := base64.RawURLEncoding.EncodeToString(bearerRaw)
	digest := sha256.Sum256([]byte(bearerPlaintext))
	digestHex := hex.EncodeToString(digest[:])
	tokenID := uuid.NewString()
	if err := h.Store.InsertMCPToken(r.Context(), tokenID, tokenName, agentID, digestHex); err != nil {
		http.Error(w, "failed to persist token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 2. Mint the auth code and bind it to the bearer plaintext.
	codeRaw := make([]byte, 32)
	if _, err := rand.Read(codeRaw); err != nil {
		http.Error(w, "failed to generate auth code", http.StatusInternalServerError)
		return
	}
	code := base64.RawURLEncoding.EncodeToString(codeRaw)
	if err := h.Store.IssueAuthCode(r.Context(),
		code, tokenID, p.CodeChallenge, p.CodeChallengeMethod,
		p.RedirectURI, p.ClientID, p.State, bearerPlaintext, AuthCodeTTL); err != nil {
		http.Error(w, "failed to issue auth code: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Redirect back to the client's redirect_uri with the code + state.
	redirectURL, err := url.Parse(p.RedirectURI)
	if err != nil {
		http.Error(w, "invalid redirect_uri", http.StatusBadRequest)
		return
	}
	q := redirectURL.Query()
	q.Set("code", code)
	if p.State != "" {
		q.Set("state", p.State)
	}
	redirectURL.RawQuery = q.Encode()
	http.Redirect(w, r, redirectURL.String(), http.StatusFound)
}

// HandleToken implements POST /oauth/token. Form-encoded body per OAuth 2.0
// §4.1.3:
//
//	grant_type=authorization_code&code=...&redirect_uri=...&code_verifier=...&client_id=...
//
// On success returns:
//
//	{"access_token": "<bearer>", "token_type": "Bearer", "expires_in": 0}
//
// expires_in=0 = does not expire — the underlying mcp_tokens revocation
// status is the real lifetime gate, and the OAuth spec does NOT require a
// non-zero value.
func (h *OAuthHandler) HandleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		writeOAuthError(w, http.StatusMethodNotAllowed, "invalid_request", "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16) // 64KB cap on /token form bodies
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "invalid form body")
		return
	}

	grantType := r.FormValue("grant_type")
	if grantType != "authorization_code" {
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type",
			"only authorization_code is supported")
		return
	}
	code := strings.TrimSpace(r.FormValue("code"))
	codeVerifier := strings.TrimSpace(r.FormValue("code_verifier"))
	redirectURI := strings.TrimSpace(r.FormValue("redirect_uri"))
	if code == "" || codeVerifier == "" || redirectURI == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request",
			"code, code_verifier, redirect_uri are required")
		return
	}

	bearer, err := h.Store.RedeemAuthCode(r.Context(), code, codeVerifier, redirectURI)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAuthCodeNotFound),
			errors.Is(err, store.ErrAuthCodeUsed),
			errors.Is(err, store.ErrAuthCodeExpired):
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant", err.Error())
		case errors.Is(err, store.ErrAuthCodeRedirectMismatch):
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant",
				"redirect_uri does not match the authorization request")
		case errors.Is(err, store.ErrAuthCodePKCEMismatch):
			writeOAuthError(w, http.StatusBadRequest, "invalid_grant",
				"PKCE code_verifier does not match code_challenge")
		default:
			writeOAuthError(w, http.StatusInternalServerError, "server_error",
				"failed to redeem authorization code")
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": bearer,
		"token_type":   "Bearer",
		"expires_in":   0,
		"scope":        "mcp",
	})
}

// writeOAuthError emits an RFC 6749 §5.2 error response. The body is JSON
// with `error` + `error_description` fields.
func writeOAuthError(w http.ResponseWriter, status int, code, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":             code,
		"error_description": description,
	})
}

// MountOAuthRoutes wires the OAuth endpoints onto a chi-compatible router.
// Caller is responsible for mounting at the host root (NOT under /v1/mcp/).
func MountOAuthRoutes(r interface {
	Get(pattern string, h http.HandlerFunc)
	Post(pattern string, h http.HandlerFunc)
}, h *OAuthHandler) {
	r.Get("/.well-known/oauth-authorization-server", h.HandleDiscovery)
	r.Get("/oauth/authorize", h.HandleAuthorize)
	r.Post("/oauth/authorize", h.HandleAuthorize)
	r.Post("/oauth/token", h.HandleToken)
}

