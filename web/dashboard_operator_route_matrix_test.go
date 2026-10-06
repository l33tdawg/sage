package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

var dashboardRouteParamPattern = regexp.MustCompile(`\{[^}]+\}`)

// TestEveryProtectedDashboardMutationRejectsArbitrarySignedMember walks the
// production router instead of maintaining a hand-copied route list. A new
// mutation is therefore operator-protected by default or this test fails until
// its narrower, independently-authorized exception is reviewed here.
func TestEveryProtectedDashboardMutationRejectsArbitrarySignedMember(t *testing.T) {
	h, _ := newTestHandler(t)
	operatorPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	h.NodeOperatorAgentID = auth.PublicKeyToAgentID(operatorPub)
	router := testRouter(h)

	// These routes intentionally sit outside the protected dashboard group or
	// carry their own exact, non-operator authority.
	reviewedExceptions := map[string]struct{}{
		"POST /v1/dashboard/auth/lock":               {},
		"POST /v1/dashboard/auth/login":              {},
		"POST /v1/dashboard/native/redeem":           {}, // One-use native proof, no operator authority.
		"POST /v1/dashboard/native/revoke":           {}, // Exact native admission revocation only.
		"POST /v1/dashboard/network/claim":           {},
		"POST /v1/dashboard/settings/ledger/recover": {},
		"POST /v1/memory/pre-validate":               {},
		"PUT /v1/dashboard/tasks/{id}/status":        {},
	}

	var routes []string
	require.NoError(t, chi.Walk(router, func(
		method, route string,
		_ http.Handler,
		_ ...func(http.Handler) http.Handler,
	) error {
		switch method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
			return nil
		}
		routes = append(routes, method+" "+route)
		return nil
	}))
	sort.Strings(routes)

	_, memberKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	protectedCount := 0
	for _, route := range routes {
		if _, reviewed := reviewedExceptions[route]; reviewed {
			continue
		}
		protectedCount++
		t.Run(route, func(t *testing.T) {
			parts := regexp.MustCompile(`\s+`).Split(route, 2)
			require.Len(t, parts, 2)
			path := dashboardRouteParamPattern.ReplaceAllString(parts[1], "matrix")
			body := []byte(`{}`)
			req := httptest.NewRequest(parts[0], path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			signAgentRequest(t, req, memberKey, body)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "operator authority")
		})
	}
	require.Greater(t, protectedCount, 90, "route walk unexpectedly missed protected mutations")
}

// TestEveryHumanCEREBRUMRouteRejectsRemoteBrowserInBothVaultModes walks the
// complete production dashboard router. The reviewed exceptions are data
// planes whose authority is not a human CEREBRUM session: public status,
// token-bound pairing/claim redemption, and the narrow signed-agent
// compatibility surface. Every remaining method/path must fail at the
// peer+Host locality boundary before its handler runs, whether the optional
// vault is disabled or a valid encrypted session cookie is present.
func TestEveryHumanCEREBRUMRouteRejectsRemoteBrowserInBothVaultModes(t *testing.T) {
	isReviewedNonCEREBRUM := func(method, route string) bool {
		switch method + " " + route {
		case "GET /v1/dashboard/health",
			"GET /v1/dashboard/chain/validators",
			"GET /v1/mcp-config",
			"GET /v1/dashboard/network/pair/{code}",
			"POST /v1/dashboard/network/claim":
			return true
		}
		path := dashboardRouteParamPattern.ReplaceAllString(route, "matrix")
		req := httptest.NewRequest(method, path, nil)
		return isRemoteSignedAgentDashboardRoute(req)
	}

	for _, encrypted := range []bool{false, true} {
		mode := "unencrypted"
		if encrypted {
			mode = "encrypted"
		}
		t.Run(mode, func(t *testing.T) {
			h, _ := newTestHandler(t)
			h.Encrypted.Store(encrypted)
			const token = "all-routes-session"
			if encrypted {
				h.sessions.Store(token, time.Now().Add(time.Hour))
			}
			router := testRouter(h)

			checked := 0
			require.NoError(t, chi.Walk(router, func(
				method, route string,
				_ http.Handler,
				_ ...func(http.Handler) http.Handler,
			) error {
				if method == http.MethodOptions ||
					isReviewedNonCEREBRUM(method, route) {
					return nil
				}
				checked++
				t.Run(method+" "+route, func(t *testing.T) {
					path := dashboardRouteParamPattern.ReplaceAllString(route, "matrix")
					body := []byte(nil)
					if method != http.MethodGet && method != http.MethodHead {
						body = []byte(`{}`)
					}
					ctx, cancel := context.WithCancel(context.Background())
					cancel() // keeps an accidental SSE fall-through from blocking.
					req := httptest.NewRequest(
						method, path, bytes.NewReader(body),
					).WithContext(ctx)
					req.RemoteAddr = "192.168.50.20:54321"
					req.Host = "192.168.50.10:8080"
					req.Header.Set("Origin", "http://192.168.50.10:8080")
					req.Header.Set("Sec-Fetch-Site", "same-origin")
					if len(body) != 0 {
						req.Header.Set("Content-Type", "application/json")
					}
					if encrypted {
						req.AddCookie(&http.Cookie{
							Name: sessionCookieName, Value: token,
						})
					}
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					require.Equal(t, http.StatusNotFound, rec.Code,
						"remote browser reached %s %s: %s",
						method, route, rec.Body.String())
				})
				return nil
			}))
			require.Greater(t, checked, 120,
				"route walk unexpectedly missed the human CEREBRUM surface")
		})
	}
}

func TestDashboardOperatorAuthorityMatrix(t *testing.T) {
	_, operatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, memberKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	type requestShape struct {
		encrypted bool
		session   bool
		remote    string
		host      string
		origin    string
		secFetch  string
		signer    ed25519.PrivateKey
	}
	tests := []struct {
		name       string
		shape      requestShape
		wantStatus int
	}{
		{
			name: "unencrypted loopback same-origin SPA",
			shape: requestShape{
				remote: "127.0.0.1:54321", host: "localhost:8080",
				origin: "http://localhost:8080", secFetch: "same-origin",
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "unencrypted unsigned no-Origin localhost process",
			shape: requestShape{
				remote: "127.0.0.1:54321", host: "localhost:8080",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "unencrypted LAN same-origin browser",
			shape: requestShape{
				remote: "192.168.1.20:54321", host: "192.168.1.10:8080",
				origin: "http://192.168.1.10:8080", secFetch: "same-origin",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "unencrypted cross-origin loopback browser",
			shape: requestShape{
				remote: "127.0.0.1:54321", host: "localhost:8080",
				origin: "https://attacker.example", secFetch: "cross-site",
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "unencrypted DNS rebinding browser",
			shape: requestShape{
				remote: "127.0.0.1:54321", host: "attacker.example:8080",
				origin: "http://attacker.example:8080", secFetch: "same-origin",
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "arbitrary signed Member",
			shape: requestShape{
				signer: memberKey,
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "exact signed node operator on loopback",
			shape: requestShape{
				remote: "127.0.0.1:54321", host: "localhost:8080",
				signer: operatorKey,
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "exact signed node operator over LAN",
			shape: requestShape{
				remote: "192.168.1.20:54321", host: "192.168.1.10:8080",
				signer: operatorKey,
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "encrypted loopback session",
			shape: requestShape{
				encrypted: true, session: true,
				remote: "127.0.0.1:54321", host: "localhost:8080",
				origin: "http://localhost:8080", secFetch: "same-origin",
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "encrypted LAN session",
			shape: requestShape{
				encrypted: true, session: true,
				remote: "192.168.1.20:54321", host: "192.168.1.10:8080",
				origin: "http://192.168.1.10:8080", secFetch: "same-origin",
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name: "encrypted DNS rebinding session",
			shape: requestShape{
				encrypted: true, session: true,
				remote: "127.0.0.1:54321", host: "attacker.example:8080",
				origin: "http://attacker.example:8080", secFetch: "same-origin",
			},
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, _ := newTestHandler(t)
			h.NodeOperatorAgentID = auth.PublicKeyToAgentID(
				operatorKey.Public().(ed25519.PublicKey),
			)
			h.Encrypted.Store(tt.shape.encrypted)
			const token = "matrix-session"
			if tt.shape.session {
				h.sessions.Store(token, time.Now().Add(time.Hour))
			}
			protected := h.authMiddleware(h.dashboardOperatorMutationGate(
				http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusNoContent)
				}),
			))
			body := []byte(`{}`)
			req := httptest.NewRequest(
				http.MethodPost, "/v1/dashboard/settings/onboarding", bytes.NewReader(body),
			)
			req.Header.Set("Content-Type", "application/json")
			if tt.shape.remote != "" {
				req.RemoteAddr = tt.shape.remote
			}
			if tt.shape.host != "" {
				req.Host = tt.shape.host
			}
			if tt.shape.origin != "" {
				req.Header.Set("Origin", tt.shape.origin)
			}
			if tt.shape.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tt.shape.secFetch)
			}
			if tt.shape.session {
				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
			}
			if len(tt.shape.signer) == ed25519.PrivateKeySize {
				signAgentRequest(t, req, tt.shape.signer, body)
			}
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// TestSignedRemoteAgentDashboardSurfaceAllowlist fixes the network shape of the
// few historical dashboard routes that are genuinely agent-facing. A verified
// agent may use those exact routes from another machine; signing a request must
// not turn the rest of the human CEREBRUM read surface into a remote API.
func TestSignedRemoteAgentDashboardSurfaceAllowlist(t *testing.T) {
	_, memberKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	tests := []struct {
		name       string
		method     string
		path       string
		body       []byte
		wantStatus int
	}{
		{
			name: "task list remains agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/tasks", wantStatus: http.StatusNoContent,
		},
		{
			name: "task notifications remain agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/task-notifications", wantStatus: http.StatusNoContent,
		},
		{
			name: "boot instructions remain agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/settings/boot-instructions", wantStatus: http.StatusNoContent,
		},
		{
			name: "governance proposal list remains agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/governance/proposals", wantStatus: http.StatusNoContent,
		},
		{
			name: "governance proposal detail remains agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/governance/proposals/proposal-matrix", wantStatus: http.StatusNoContent,
		},
		{
			name: "exact task status remains agent-facing", method: http.MethodPut,
			path: "/v1/dashboard/tasks/task-matrix/status", body: []byte(`{}`),
			wantStatus: http.StatusNoContent,
		},
		{
			name: "pre-validation remains agent-facing", method: http.MethodPost,
			path: "/v1/memory/pre-validate", body: []byte(`{}`),
			wantStatus: http.StatusNoContent,
		},
		{
			name: "node stats remain agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/stats", wantStatus: http.StatusNoContent,
		},
		{
			name: "recall settings remain agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/settings/recall", wantStatus: http.StatusNoContent,
		},
		{
			name: "memory mode remains agent-facing", method: http.MethodGet,
			path: "/v1/dashboard/settings/memory-mode", wantStatus: http.StatusNoContent,
		},
		{
			name: "embedding setup is human CEREBRUM", method: http.MethodGet,
			path: "/v1/dashboard/embeddings/status", wantStatus: http.StatusNotFound,
		},
		{
			name: "federation wizard is human CEREBRUM", method: http.MethodGet,
			path: "/v1/dashboard/federation/readiness", wantStatus: http.StatusNotFound,
		},
		{
			name: "ChatGPT setup is human CEREBRUM", method: http.MethodGet,
			path: "/v1/dashboard/chatgpt-tunnel/status", wantStatus: http.StatusNotFound,
		},
		{
			name: "network templates are human CEREBRUM", method: http.MethodGet,
			path: "/v1/dashboard/network/templates", wantStatus: http.StatusNotFound,
		},
		{
			name: "ledger state is human CEREBRUM", method: http.MethodGet,
			path: "/v1/dashboard/settings/ledger", wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, agentStore := newTestHandler(t)
			require.NoError(t, agentStore.CreateAgent(context.Background(), &store.AgentEntry{
				AgentID: auth.PublicKeyToAgentID(
					memberKey.Public().(ed25519.PublicKey),
				),
				Name:      "Remote matrix member",
				Role:      "member",
				Status:    "active",
				CreatedAt: time.Now().UTC(),
			}))
			protected := h.authMiddleware(h.cerebrumBrowserLocalityGate(
				h.dashboardOperatorMutationGate(http.HandlerFunc(
					func(w http.ResponseWriter, _ *http.Request) {
						w.WriteHeader(http.StatusNoContent)
					},
				)),
			))
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(tt.body))
			req.Host = "192.168.1.10:8080"
			req.RemoteAddr = "192.168.1.20:54321"
			if len(tt.body) > 0 {
				req.Header.Set("Content-Type", "application/json")
			}
			signAgentRequest(t, req, memberKey, tt.body)
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}
