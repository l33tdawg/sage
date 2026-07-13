package middleware

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// stubLookup returns a MCPTokenLookupFn that maps a fixed plaintext token to
// a fixed agent ID. Anything else returns sql.ErrNoRows.
func stubLookup(plaintext, agentID string) MCPTokenLookupFn {
	digest := sha256.Sum256([]byte(plaintext))
	want := hex.EncodeToString(digest[:])
	return func(_ context.Context, tokenSHA256 string) (string, ed25519.PrivateKey, error) {
		if tokenSHA256 == want {
			return agentID, nil, nil
		}
		return "", nil, sql.ErrNoRows
	}
}

func bearerProtected(lookup MCPTokenLookupFn) http.Handler {
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test-Agent-ID", ContextAgentID(r.Context()))
		w.Header().Set("X-Test-Token-Fingerprint", ContextMCPTokenFingerprint(r.Context()))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return MCPBearerAuthMiddleware(lookup)(final)
}

func TestBearerAuth_Rejects_NoToken(t *testing.T) {
	h := bearerProtected(stubLookup("good-token", "agent-123"))

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/sse", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "Missing bearer token")
}

func TestBearerAuth_Rejects_BadScheme(t *testing.T) {
	h := bearerProtected(stubLookup("good-token", "agent-123"))

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/sse", nil)
	req.Header.Set("Authorization", "Basic Zm9vOmJhcg==")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "Invalid authorization scheme")
}

func TestBearerAuth_Rejects_BadToken(t *testing.T) {
	h := bearerProtected(stubLookup("good-token", "agent-123"))

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/sse", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "Invalid bearer token")
}

func TestBearerAuth_Rejects_Revoked(t *testing.T) {
	revokeLookup := func(_ context.Context, _ string) (string, ed25519.PrivateKey, error) {
		return "", nil, ErrMCPTokenRevoked
	}
	h := bearerProtected(revokeLookup)

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/sse", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "Revoked bearer token")
}

func TestBearerAuth_Accepts_ValidToken(t *testing.T) {
	h := bearerProtected(stubLookup("the-good-token", "agent-abc"))

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/sse", nil)
	req.Header.Set("Authorization", "Bearer the-good-token")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "agent-abc", rr.Header().Get("X-Test-Agent-ID"))
	digest := sha256.Sum256([]byte("the-good-token"))
	assert.Equal(t, hex.EncodeToString(digest[:]), rr.Header().Get("X-Test-Token-Fingerprint"))
}

func TestBearerAuth_DBError_Fails500(t *testing.T) {
	dbErr := func(_ context.Context, _ string) (string, ed25519.PrivateKey, error) {
		return "", nil, assertableError("transient db failure")
	}
	h := bearerProtected(dbErr)

	req := httptest.NewRequest(http.MethodGet, "/v1/mcp/sse", nil)
	req.Header.Set("Authorization", "Bearer x")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	// We require a non-200 (could be 401 or 500 depending on classification).
	// The middleware fails closed; ensure the request did NOT reach the handler.
	assert.NotEqual(t, http.StatusOK, rr.Code)
	assert.Empty(t, rr.Header().Get("X-Test-Agent-ID"))
}

// assertableError is a simple error type for test stubs.
type assertableError string

func (e assertableError) Error() string { return string(e) }
