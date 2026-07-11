package middleware

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/httprate"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// okHandler is a simple handler that returns 200 OK.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	agentID := ContextAgentID(r.Context())
	w.Header().Set("X-Test-Agent-ID", agentID)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
})

func signedRequest(t *testing.T, method, path string, body []byte) (*http.Request, string) {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	ts := time.Now().Unix()
	sig := auth.SignRequest(priv, method, path, body, ts)

	agentID := auth.PublicKeyToAgentID(pub)

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))

	return req, agentID
}

func TestAuthMiddleware_ValidSignature(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)
	body := []byte(`{"content":"test memory"}`)
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, agentID, rr.Header().Get("X-Test-Agent-ID"))
}

func TestAuthMiddleware_MissingHeaders(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)

	tests := []struct {
		name    string
		headers map[string]string
	}{
		{"no headers", map[string]string{}},
		{"missing signature", map[string]string{"X-Agent-ID": "abc", "X-Timestamp": "123"}},
		{"missing agent ID", map[string]string{"X-Signature": "abc", "X-Timestamp": "123"}},
		{"missing timestamp", map[string]string{"X-Agent-ID": "abc", "X-Signature": "def"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/agent/me", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusUnauthorized, rr.Code)
			assert.Contains(t, rr.Header().Get("Content-Type"), "application/problem+json")
		})
	}
}

func TestAuthMiddleware_ExpiredTimestamp(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	// Timestamp 10 minutes ago — outside the 5-minute window.
	ts := time.Now().Add(-10 * time.Minute).Unix()
	body := []byte(`{}`)
	sig := auth.SignRequest(priv, http.MethodPost, "/v1/memory/submit", body, ts)

	req := httptest.NewRequest(http.MethodPost, "/v1/memory/submit", bytes.NewReader(body))
	req.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(pub))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "Timestamp expired")
}

func TestAuthMiddleware_InvalidSignature(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)

	pub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)

	ts := time.Now().Unix()
	body := []byte(`{"content":"test"}`)

	// Use a fake signature (wrong bytes).
	fakeSig := make([]byte, 64)
	for i := range fakeSig {
		fakeSig[i] = 0xFF
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/memory/submit", bytes.NewReader(body))
	req.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(pub))
	req.Header.Set("X-Signature", hex.EncodeToString(fakeSig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code)
	assert.Contains(t, rr.Body.String(), "Invalid signature")
}

func TestAuthMiddleware_SkipPaths(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)

	for _, path := range []string{"/health", "/ready"} {
		t.Run(path, func(t *testing.T) {
			// No auth headers at all — should still pass.
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			assert.Equal(t, http.StatusOK, rr.Code)
			assert.Equal(t, "ok", rr.Body.String())
		})
	}
}

func TestAuthMiddleware_WithNonce(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	body := []byte(`{"domain_tag":"crypto","status":"committed","limit":50}`)
	ts := time.Now().Unix()
	nonce := []byte("random01") // 8 bytes
	sig := auth.SignRequestWithNonce(priv, http.MethodGet, "/v1/memory/list", body, ts, nonce)

	req := httptest.NewRequest(http.MethodGet, "/v1/memory/list", bytes.NewReader(body))
	req.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(pub))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestAuthMiddleware_NoncePreventsReplay(t *testing.T) {
	// Reset the replay cache for this test.
	sigCache = &replayCache{
		seen:    make(map[string]time.Time),
		maxSize: 10000,
	}

	handler := Ed25519AuthMiddleware(okHandler)

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	body := []byte(`{"domain_tag":"crypto","status":"committed","limit":50}`)
	ts := time.Now().Unix()

	// Two requests with same body+timestamp but different nonces.
	nonce1 := []byte("nonce001")
	nonce2 := []byte("nonce002")
	sig1 := auth.SignRequestWithNonce(priv, http.MethodGet, "/v1/memory/list", body, ts, nonce1)
	sig2 := auth.SignRequestWithNonce(priv, http.MethodGet, "/v1/memory/list", body, ts, nonce2)

	agentID := auth.PublicKeyToAgentID(pub)

	// First request — should pass.
	req1 := httptest.NewRequest(http.MethodGet, "/v1/memory/list", bytes.NewReader(body))
	req1.Header.Set("X-Agent-ID", agentID)
	req1.Header.Set("X-Signature", hex.EncodeToString(sig1))
	req1.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req1.Header.Set("X-Nonce", hex.EncodeToString(nonce1))
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	assert.Equal(t, http.StatusOK, rr1.Code, "first request should pass")

	// Second request (different nonce) — should also pass (not a replay).
	req2 := httptest.NewRequest(http.MethodGet, "/v1/memory/list", bytes.NewReader(body))
	req2.Header.Set("X-Agent-ID", agentID)
	req2.Header.Set("X-Signature", hex.EncodeToString(sig2))
	req2.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req2.Header.Set("X-Nonce", hex.EncodeToString(nonce2))
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	assert.Equal(t, http.StatusOK, rr2.Code, "second request with different nonce should pass")
}

func TestAuthMiddleware_EmptyBody(t *testing.T) {
	handler := Ed25519AuthMiddleware(okHandler)
	req, _ := signedRequest(t, http.MethodGet, "/v1/agent/me", nil)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestRateLimitMiddleware(t *testing.T) {
	limiter := httprate.LimitBy(
		5,
		time.Minute,
		func(r *http.Request) (string, error) {
			return r.Header.Get("X-Agent-ID"), nil
		},
		httprate.WithLimitHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "60")
			writeProblem(w, http.StatusTooManyRequests, "Rate limit exceeded", "Too many requests.")
		})),
	)
	handler := limiter(okHandler)

	// Send 5 requests — all should pass.
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/agent/me", nil)
		req.Header.Set("X-Agent-ID", "test-agent")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusOK, rr.Code, "request %d should pass", i+1)
	}

	// 6th request should be rate-limited.
	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me", nil)
	req.Header.Set("X-Agent-ID", "test-agent")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusTooManyRequests, rr.Code)
	assert.Equal(t, "60", rr.Header().Get("Retry-After"))
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/problem+json")
}

func TestRequestLogger(t *testing.T) {
	handler := RequestLogger(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/v1/agent/me", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.NotEmpty(t, rr.Header().Get("X-Request-ID"), "should set X-Request-ID header")
	assert.Len(t, rr.Header().Get("X-Request-ID"), 32, "request ID should be 16 bytes hex = 32 chars")
}

func TestReplayCacheFailsClosedAtBoundAndRecoversExpiredSpace(t *testing.T) {
	rc := &replayCache{seen: make(map[string]time.Time), maxSize: 2}
	replayed, saturated := rc.check("sig-a")
	assert.False(t, replayed)
	assert.False(t, saturated)
	replayed, saturated = rc.check("sig-b")
	assert.False(t, replayed)
	assert.False(t, saturated)

	replayed, saturated = rc.check("sig-c")
	assert.False(t, replayed, "a fresh signature in a saturated window is not a replay")
	assert.True(t, saturated, "a fresh saturated window must fail closed as saturation")
	assert.Len(t, rc.seen, 2, "cache must never grow past maxSize")

	replayed, saturated = rc.check("sig-a")
	assert.True(t, replayed, "a recorded signature must still report replay under saturation")
	assert.False(t, saturated)

	rc.seen["sig-a"] = time.Now().Add(-maxTimestampSkew - time.Second)
	replayed, saturated = rc.check("sig-c")
	assert.False(t, replayed, "expired space should admit a new signature")
	assert.False(t, saturated)
	assert.Len(t, rc.seen, 2)
	assert.NotContains(t, rc.seen, "sig-a")
}
