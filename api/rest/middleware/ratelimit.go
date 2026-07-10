package middleware

import (
	"net/http"
	"time"

	"github.com/go-chi/httprate"
)

// RateLimitMiddleware applies per-agent rate limiting.
// Each agent (identified by X-Agent-ID header) is allowed up to 10000 requests
// per minute. Over-limit requests receive a 429 response with RFC 7807 body
// and a Retry-After header.
func RateLimitMiddleware() func(http.Handler) http.Handler {
	return httprate.LimitBy(
		10000,
		time.Minute,
		func(r *http.Request) (string, error) {
			agentID := r.Header.Get("X-Agent-ID")
			if agentID == "" {
				// Fall back to remote address for unauthenticated endpoints.
				return r.RemoteAddr, nil
			}
			return agentID, nil
		},
		httprate.WithLimitHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "60")
			writeProblem(w, http.StatusTooManyRequests, "Rate limit exceeded",
				"You have exceeded the rate limit of 10000 requests per minute. Please retry after the Retry-After period.")
		})),
	)
}
