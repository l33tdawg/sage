package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A recall whose caller has gone (cancelled or timed out) returns right after
// the store lookup: no per-result batches, no last_seen write, no response.
// The test store ignores the context, so before the guard each of these
// served a full 200 page to a caller that was no longer there.
func TestRecallHandlers_StopWhenCallerCancelled(t *testing.T) {
	for _, tc := range []struct{ path, body string }{
		{"/v1/memory/query", `{"embedding":[0.1,0.2,0.3],"top_k":3}`},
		{"/v1/memory/search", `{"query":"lesson","top_k":3}`},
		{"/v1/memory/hybrid", `{"query":"lesson","embedding":[0.1,0.2,0.3],"top_k":3}`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			srv, memStore, _ := newTestServer(t, "")
			memStore.memories["m1"] = &memory.MemoryRecord{
				MemoryID:        "m1",
				SubmittingAgent: "agent-x",
				Content:         "a lesson",
				ContentHash:     []byte{1},
				MemoryType:      memory.TypeFact,
				DomainTag:       "general",
				ConfidenceScore: 0.9,
				Status:          memory.StatusCommitted,
				CreatedAt:       time.Now().Add(-time.Hour),
			}

			// Control: the same request with a live context serves a result.
			req, _ := signedRequest(t, http.MethodPost, tc.path, []byte(tc.body))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			require.Contains(t, rr.Body.String(), `"m1"`)

			req, _ = signedRequest(t, http.MethodPost, tc.path, []byte(tc.body))
			ctx, cancel := context.WithCancel(req.Context())
			cancel()
			rr = httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req.WithContext(ctx))
			assert.Empty(t, rr.Body.String(), "cancelled recall must not serialize a page")
		})
	}
}
