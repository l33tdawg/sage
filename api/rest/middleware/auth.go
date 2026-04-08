package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/rs/zerolog/log"
)

// replayCache prevents signature replay within the timestamp validity window.
// Keyed on (agentID + signature hex), entries expire after maxTimestampSkew.
type replayCache struct {
	mu      sync.Mutex
	seen    map[string]time.Time
	maxSize int
}

var sigCache = &replayCache{
	seen:    make(map[string]time.Time),
	maxSize: 10000,
}

// check returns true if the signature has been seen before (replay).
// If not seen, it records it and returns false.
func (rc *replayCache) check(key string) bool {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Evict expired entries if cache is getting large.
	if len(rc.seen) >= rc.maxSize {
		now := time.Now()
		for k, t := range rc.seen {
			if now.Sub(t) > maxTimestampSkew {
				delete(rc.seen, k)
			}
		}
	}

	if _, exists := rc.seen[key]; exists {
		return true // replay detected
	}
	rc.seen[key] = time.Now()
	return false
}

type contextKey string

const (
	agentIDKey  contextKey = "agent_id"
	agentAuthKey contextKey = "agent_auth" // Raw agent auth proof for on-chain embedding
)

// AgentAuthProof holds the raw cryptographic material from the agent's request
// authentication, to be embedded in transactions for on-chain verification.
type AgentAuthProof struct {
	PubKey    []byte // Ed25519 public key (32 bytes)
	Signature []byte // Ed25519 signature (64 bytes)
	Timestamp int64  // Unix seconds used in signing
	BodyHash  []byte // SHA-256 of request body (32 bytes)
	Nonce     []byte // Optional nonce used in signing (nil if legacy)
}

// skipAuthPaths lists paths that bypass authentication.
var skipAuthPaths = map[string]bool{
	"/health": true,
	"/ready":  true,
}

// maxTimestampSkew is the maximum allowed clock drift for request timestamps.
const maxTimestampSkew = 5 * time.Minute

// Ed25519AuthMiddleware validates Ed25519 signature authentication via headers:
//   - X-Agent-ID:  hex-encoded Ed25519 public key
//   - X-Signature: hex-encoded Ed25519 signature
//   - X-Timestamp: unix epoch seconds
//
// The signed message is SHA-256(method + " " + path + "\n" + body) + timestamp (big-endian int64).
// This binds signatures to specific endpoints, preventing cross-endpoint replay.
func Ed25519AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health/readiness probes.
		if skipAuthPaths[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}

		agentID := strings.TrimSpace(r.Header.Get("X-Agent-ID"))
		sigHex := strings.TrimSpace(r.Header.Get("X-Signature"))
		tsStr := strings.TrimSpace(r.Header.Get("X-Timestamp"))

		if agentID == "" || sigHex == "" || tsStr == "" {
			writeProblem(w, http.StatusUnauthorized, "Missing authentication headers",
				"X-Agent-ID, X-Signature, and X-Timestamp headers are required.")
			return
		}

		// Parse and validate timestamp.
		tsUnix, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			writeProblem(w, http.StatusUnauthorized, "Invalid timestamp",
				"X-Timestamp must be a valid unix epoch in seconds.")
			return
		}

		now := time.Now().Unix()
		diff := now - tsUnix
		if diff < 0 {
			diff = -diff
		}
		if time.Duration(diff)*time.Second > maxTimestampSkew {
			writeProblem(w, http.StatusUnauthorized, "Timestamp expired",
				"X-Timestamp is outside the acceptable 5-minute window.")
			return
		}

		// Decode public key from agent ID.
		pubKey, err := auth.AgentIDToPublicKey(agentID)
		if err != nil {
			writeProblem(w, http.StatusUnauthorized, "Invalid agent ID",
				"X-Agent-ID must be a valid hex-encoded Ed25519 public key.")
			return
		}

		// Decode signature.
		sig, err := hex.DecodeString(sigHex)
		if err != nil {
			writeProblem(w, http.StatusUnauthorized, "Invalid signature encoding",
				"X-Signature must be hex-encoded.")
			return
		}

		// Read and buffer the request body for signature verification (capped at 1 MB).
		var body []byte
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
			body, err = io.ReadAll(r.Body)
			if err != nil {
				log.Error().Err(err).Msg("failed to read request body for auth")
				writeProblem(w, http.StatusInternalServerError, "Internal error",
					"Failed to read request body.")
				return
			}
			// Replace the body so downstream handlers can read it.
			r.Body = io.NopCloser(bytes.NewReader(body))
		}

		// Verify Ed25519 signature (covers method + path + query + body + timestamp + optional nonce).
		// Use RequestURI to include query params — the MCP client signs the full path.
		reqPath := r.URL.Path
		if r.URL.RawQuery != "" {
			reqPath = r.URL.Path + "?" + r.URL.RawQuery
		}

		// Support optional X-Nonce header for sub-second replay protection.
		// If present, include nonce in signature verification; otherwise fall back
		// to legacy (nonce-less) verification for backward compatibility.
		var nonce []byte
		if nonceHex := strings.TrimSpace(r.Header.Get("X-Nonce")); nonceHex != "" {
			nonce, err = hex.DecodeString(nonceHex)
			if err != nil {
				writeProblem(w, http.StatusUnauthorized, "Invalid nonce encoding",
					"X-Nonce must be hex-encoded.")
				return
			}
		}

		var sigValid bool
		if len(nonce) > 0 {
			sigValid = auth.VerifyRequestWithNonce(pubKey, r.Method, reqPath, body, tsUnix, nonce, sig)
		} else {
			sigValid = auth.VerifyRequest(pubKey, r.Method, reqPath, body, tsUnix, sig)
		}
		if !sigValid {
			writeProblem(w, http.StatusUnauthorized, "Invalid signature",
				"Ed25519 signature verification failed.")
			return
		}

		// Replay protection: reject duplicate (agentID, signature) pairs within the skew window.
		cacheKey := agentID + ":" + sigHex
		if sigCache.check(cacheKey) {
			writeProblem(w, http.StatusUnauthorized, "Replay detected",
				"This exact request signature has already been used.")
			return
		}

		// Compute canonical body hash for on-chain embedding.
		// Must match the signing format: SHA-256(method + " " + path[+query] + "\n" + body)
		canonical := []byte(r.Method + " " + reqPath + "\n")
		canonical = append(canonical, body...)
		bodyHash := sha256.Sum256(canonical)

		// Store agent ID and raw auth proof in context for downstream handlers.
		proof := &AgentAuthProof{
			PubKey:    []byte(pubKey),
			Signature: sig,
			Timestamp: tsUnix,
			BodyHash:  bodyHash[:],
			Nonce:     nonce,
		}
		ctx := context.WithValue(r.Context(), agentIDKey, agentID)
		ctx = context.WithValue(ctx, agentAuthKey, proof)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ContextAgentID extracts the authenticated agent ID from the request context.
// Returns an empty string if no agent ID is present (e.g. unauthenticated paths).
func ContextAgentID(ctx context.Context) string {
	v, _ := ctx.Value(agentIDKey).(string)
	return v
}

// ContextAgentAuth extracts the raw agent auth proof from the request context.
// Returns nil if not present.
func ContextAgentAuth(ctx context.Context) *AgentAuthProof {
	v, _ := ctx.Value(agentAuthKey).(*AgentAuthProof)
	return v
}
