package middleware

// Bearer-token authentication middleware for the HTTP MCP transport.
//
// External MCP clients (ChatGPT, Cursor, Cline) send a bearer token in the
// Authorization header. We hash it with SHA-256 and look up the digest in
// the mcp_tokens table. On match, we put the associated agent identity in
// the request context so downstream MCP tool handlers run as that agent.
//
// This middleware sits ONLY in front of /v1/mcp/sse and /v1/mcp/streamable.
// /v1/mcp/tokens (the issuer endpoints) keeps its ed25519 admin auth.

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// MCPTokenLookup is the storage-side hook the middleware needs. The full
// SQLiteStore implementation satisfies it.
type MCPTokenLookup interface {
	LookupMCPToken(ctx context.Context, tokenSHA256 string) (*MCPTokenInfo, error)
}

// MCPTokenInfo is the shape we need from the store. Defined here (instead
// of importing internal/store) to avoid an import cycle: middleware is
// already a leaf package in the dependency graph.
type MCPTokenInfo struct {
	ID      string
	AgentID string
}

// MCPTokenLookupFn is a function adapter so callers can wire the SQLite
// store's method without forcing the interface above on it. It returns the
// resolved agent ID and, for a KEYED token, the per-token ed25519 signing key so
// downstream REST calls sign as the token's OWN on-chain identity. A nil signer
// means a legacy keyless token — the caller falls back to the node operator key.
type MCPTokenLookupFn func(ctx context.Context, tokenSHA256 string) (agentID string, signer ed25519.PrivateKey, err error)

type mcpTokenFingerprintKeyType struct{}

var mcpTokenFingerprintKey mcpTokenFingerprintKeyType

type mcpSignerKeyType struct{}

var mcpSignerKey mcpSignerKeyType

// WithMCPSigner installs the per-token ed25519 signing key resolved by the
// bearer middleware into ctx, so the MCP transport signs downstream REST calls
// as the token's own on-chain identity instead of collapsing to the node
// operator. A nil key (legacy keyless token / non-bearer path) is never
// installed; readers then fall back to the operator key.
func WithMCPSigner(ctx context.Context, signer ed25519.PrivateKey) context.Context {
	return context.WithValue(ctx, mcpSignerKey, signer)
}

// ContextMCPSigner returns the per-token signing key installed by the bearer
// middleware, or nil when the request is not keyed. Callers that see nil sign as
// the node operator (the pre-v11.8 behavior).
func ContextMCPSigner(ctx context.Context) ed25519.PrivateKey {
	v, _ := ctx.Value(mcpSignerKey).(ed25519.PrivateKey)
	return v
}

// MCPBearerAuthMiddleware returns a middleware that validates the
// Authorization header against a bearer-token store. On success, the
// resolved agent ID is placed in the request context under the same key
// used by Ed25519AuthMiddleware so downstream handlers can read it via
// ContextAgentID.
//
// The lookup callback is responsible for consulting the storage layer and
// returning the agent ID associated with the token (or an error). Storage
// implementations should hash the token before lookup; the middleware
// passes the SHA-256 hex digest already, so the callback works directly
// against the persisted form.
//
// If the lookup returns ErrTokenRevoked (sentinel forwarded from the store),
// we 401. Any other store error 500s — which prevents accidentally
// auth'ing a request just because the DB is down.
func MCPBearerAuthMiddleware(lookup MCPTokenLookupFn) func(http.Handler) http.Handler {
	// writeMCPUnauthorized writes a 401 with a WWW-Authenticate header per the
	// MCP authorization spec (June 2025 revision). The resource_metadata URL
	// points at the Protected Resource Metadata document (RFC 9728) which
	// MCP clients use to discover the OAuth authorization server. Without
	// this header, ChatGPT's MCP connector hangs at "Continue to SAGE"
	// because it can't bootstrap the OAuth flow from a bare 401.
	writeMCPUnauthorized := func(w http.ResponseWriter, r *http.Request, title, detail string) {
		scheme := "https"
		if r.TLS == nil && r.Header.Get("X-Forwarded-Proto") != "https" {
			scheme = "http"
		}
		host := r.Host
		if h := r.Header.Get("X-Forwarded-Host"); h != "" {
			host = h
		}
		resourceMeta := fmt.Sprintf("%s://%s/.well-known/oauth-protected-resource", scheme, host)
		w.Header().Set("WWW-Authenticate",
			fmt.Sprintf(`Bearer realm="sage", resource_metadata="%s"`, resourceMeta))
		writeProblem(w, http.StatusUnauthorized, title, detail)
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authz := strings.TrimSpace(r.Header.Get("Authorization"))
			if authz == "" {
				writeMCPUnauthorized(w, r, "Missing bearer token",
					"Authorization: Bearer <token> is required for /v1/mcp endpoints.")
				return
			}
			parts := strings.SplitN(authz, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				writeMCPUnauthorized(w, r, "Invalid authorization scheme",
					"Authorization header must use the Bearer scheme.")
				return
			}
			token := strings.TrimSpace(parts[1])
			if token == "" {
				writeMCPUnauthorized(w, r, "Empty bearer token",
					"Authorization: Bearer <token> requires a non-empty token.")
				return
			}

			digest := sha256.Sum256([]byte(token))
			digestHex := hex.EncodeToString(digest[:])

			agentID, signer, err := lookup(r.Context(), digestHex)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					writeMCPUnauthorized(w, r, "Invalid bearer token",
						"This token is not recognized.")
					return
				}
				if errors.Is(err, ErrMCPTokenRevoked) {
					writeMCPUnauthorized(w, r, "Revoked bearer token",
						"This token has been revoked.")
					return
				}
				// Anything else (DB errors etc.) — fail closed.
				writeProblem(w, http.StatusInternalServerError, "Token lookup failed",
					"The server could not verify this token.")
				return
			}
			if agentID == "" {
				writeProblem(w, http.StatusUnauthorized, "Invalid bearer token",
					"This token is not bound to an agent.")
				return
			}

			ctx := context.WithValue(r.Context(), agentIDKey, agentID)
			// Bind transport sessions to the credential, not only the effective
			// operator principal. Multiple operator tokens intentionally resolve to
			// the same agent ID, but must not be able to post into one another's SSE
			// stream if a session UUID leaks.
			ctx = context.WithValue(ctx, mcpTokenFingerprintKey, digestHex)
			// KEYED token: thread the per-token signer so the transport signs
			// downstream REST calls as the token's own identity. Legacy keyless
			// tokens carry a nil signer and keep signing as the node operator.
			if signer != nil {
				ctx = WithMCPSigner(ctx, signer)
			}
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ErrMCPTokenRevoked is the sentinel the bearer middleware uses to detect
// "exists but revoked" so it can return a clearer message. The store
// package returns its own ErrTokenRevoked; callers wiring the middleware
// should translate or shadow that to this sentinel.
var ErrMCPTokenRevoked = errors.New("mcp token revoked")

// ContextMCPTokenFingerprint returns the SHA-256 digest of the bearer that
// authenticated an MCP request. It is safe for binding/isolation and never
// exposes the plaintext credential.
func ContextMCPTokenFingerprint(ctx context.Context) string {
	v, _ := ctx.Value(mcpTokenFingerprintKey).(string)
	return v
}
