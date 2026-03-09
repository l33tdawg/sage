package web

import (
	"net/http"
	"strings"
)

// RedeployChecker is implemented by types that can report whether a
// chain redeployment is currently in progress.
type RedeployChecker interface {
	IsRedeploying() bool
}

// redeployGuard returns 503 Service Unavailable for write endpoints while a
// chain redeployment is active.  Read endpoints, health, auth, SSE, and the
// redeploy status polling endpoint are always allowed through.
func redeployGuard(checker RedeployChecker) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// No-op when checker is nil (no redeployer configured).
			if checker == nil {
				next.ServeHTTP(w, r)
				return
			}

			// Only gate write methods — reads are safe because SQLite is
			// never wiped during redeployment.
			if !isWriteMethod(r.Method) {
				next.ServeHTTP(w, r)
				return
			}

			// Exempt paths that must remain available during redeployment.
			if isExemptPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			if checker.IsRedeploying() {
				w.Header().Set("Retry-After", "30")
				writeJSONResp(w, http.StatusServiceUnavailable, map[string]any{
					"error":       "Network reconfiguration in progress",
					"retry_after": 30,
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// isWriteMethod returns true for HTTP methods that mutate state.
func isWriteMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// isExemptPath returns true for paths that must remain accessible during
// an active redeployment.
func isExemptPath(path string) bool {
	switch {
	case path == "/v1/dashboard/network/redeploy/status":
		return true
	case path == "/v1/dashboard/health":
		return true
	case strings.HasPrefix(path, "/v1/dashboard/auth/"):
		return true
	case path == "/v1/dashboard/events": // SSE endpoint
		return true
	}
	return false
}
