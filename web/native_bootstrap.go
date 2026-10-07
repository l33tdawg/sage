package web

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"time"

	"github.com/l33tdawg/sage/internal/nativebootstrap"
)

const nativeSessionHeader = "X-SAGE-Native-Session"

type nativeSessionContextKey struct{}

func nativeSessionToken(r *http.Request) string {
	token, _ := r.Context().Value(nativeSessionContextKey{}).(string)
	return token
}

// Native transport admission never authenticates a vault or supplies a Root or
// agent identity. Existing encrypted-dashboard cookies and authority gates are
// still required. A malformed native credential may not fall back to browser
// metadata, a copied cookie, or an agent signature.
func (h *DashboardHandler) nativeSessionGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values, present := r.Header[http.CanonicalHeaderKey(nativeSessionHeader)]
		if !present {
			next.ServeHTTP(w, r)
			return
		}
		if len(values) != 1 || !h.nativeRequestBoundary(r) || !h.NativeBootstrap.Verify(values[0], h.NativeBinding) {
			writeUnauthorized(w)
			return
		}
		ctx, cancel := context.WithCancel(context.WithValue(r.Context(), nativeSessionContextKey{}, values[0]))
		defer cancel()
		// Long-lived SSE and other in-flight work must lose their admission
		// when a token is revoked, expires, or the daemon starts draining.
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if !h.NativeBootstrap.Verify(values[0], h.NativeBinding) {
						cancel()
						return
					}
				}
			}
		}()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (h *DashboardHandler) nativeRequestBoundary(r *http.Request) bool {
	if h.NativeBootstrap == nil || r.TLS != nil || !isLoopbackCEREBRUMRequest(r) || "http://"+r.Host != h.NativeBinding.Origin {
		return false
	}
	// A native peer does not need browser, proxy or agent identity assertions.
	for _, name := range []string{"Origin", "Sec-Fetch-Site", "Sec-Fetch-Mode", "X-Agent-ID", "X-Signature", "X-Timestamp", "X-Nonce", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto", "X-Real-IP"} {
		if _, present := r.Header[http.CanonicalHeaderKey(name)]; present {
			return false
		}
	}
	return true
}

func (h *DashboardHandler) handleNativeRedeem(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !h.nativeRequestBoundary(r) || len(r.Header.Values(nativeSessionHeader)) != 0 {
		http.NotFound(w, r)
		return
	}
	if media := r.Header.Get("Content-Type"); media != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}
	var req struct {
		Ticket       string  `json:"ticket"`
		Challenge    string  `json:"challenge"`
		Generation   string  `json:"instance_generation"`
		Origin       string  `json:"ui_origin"`
		StartupProof *string `json:"startup_proof"`
		PublicKey    string  `json:"public_key"`
		Signature    string  `json:"signature"`
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil || nativebootstrap.DecodeObject(data, &req) != nil || req.StartupProof == nil {
		writeError(w, http.StatusBadRequest, "invalid native redemption")
		return
	}
	admission, err := h.NativeBootstrap.Redeem(nativebootstrap.Redemption{Ticket: req.Ticket, Challenge: req.Challenge, PublicKey: req.PublicKey, Signature: req.Signature, Binding: nativebootstrap.Binding{Generation: req.Generation, Origin: req.Origin, StartupProof: *req.StartupProof}})
	if err != nil {
		writeUnauthorized(w)
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"native_session": admission.Token, "expires_in_seconds": admission.ExpiresInSeconds})
}

func rejectUnencryptedNative(w http.ResponseWriter, r *http.Request, encrypted bool) bool {
	if nativeSessionToken(r) == "" || encrypted {
		return false
	}
	writeError(w, http.StatusForbidden, "Native CEREBRUM requires an encrypted vault. Enable the Synaptic Ledger in browser CEREBRUM on this machine, then reconnect.")
	return true
}

func (h *DashboardHandler) handleNativeRevoke(w http.ResponseWriter, r *http.Request) {
	if !h.nativeRequestBoundary(r) {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	token := nativeSessionToken(r)
	if token == "" {
		writeUnauthorized(w)
		return
	}
	h.revokeNativeSession(token)
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}

// Native authority metadata and expiry are one immutable map value. Separate
// maps would let a revoke race reinterpret a native-only cookie as a browser one.
type nativeDashboardSession struct {
	Expires time.Time
	Binding [sha256.Size]byte
}

func (h *DashboardHandler) validSessionForRequest(token string, r *http.Request) bool {
	value, ok := h.sessions.Load(token)
	if !ok {
		return false
	}
	var expiry time.Time
	switch session := value.(type) {
	case time.Time:
		expiry = session
	case nativeDashboardSession:
		native := nativeSessionToken(r)
		if native == "" || session.Binding != sha256.Sum256([]byte(native)) || h.NativeBootstrap == nil || !h.NativeBootstrap.Verify(native, h.NativeBinding) {
			return false
		}
		expiry = session.Expires
	default:
		return false
	}
	if !time.Now().Before(expiry) {
		h.sessions.Delete(token)
		return false
	}
	return true
}

func (h *DashboardHandler) revokeNativeSession(token string) {
	h.NativeBootstrap.Revoke(token)
	binding := sha256.Sum256([]byte(token))
	h.sessions.Range(func(key, value any) bool {
		if session, ok := value.(nativeDashboardSession); ok && session.Binding == binding {
			h.sessions.Delete(key)
		}
		return true
	})
}
