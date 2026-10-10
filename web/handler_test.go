package web

import (
	"bytes"
	"context"
	ed25519pkg "crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func newTestHandler(t *testing.T) (*DashboardHandler, *store.SQLiteStore) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	h := NewDashboardHandler(s, "test")
	return h, s
}

func testRouter(h *DashboardHandler) *chi.Mux {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// httptest.NewRequest uses example.com and 192.0.2.1 as placeholders.
			// Most historical dashboard tests model the local CEREBRUM caller and
			// predate the explicit peer+Host boundary, so translate only that exact
			// untouched pair. Security tests that set either value keep their
			// deliberate local, LAN, or rebinding shape.
			if req.Host == "example.com" && req.RemoteAddr == "192.0.2.1:1234" {
				req.Host = "localhost:8080"
				req.RemoteAddr = "127.0.0.1:54321"
				// Historical handler tests model browser use of the local
				// CEREBRUM dashboard. Supply the browser metadata that those
				// tests predate; security tests which deliberately model a
				// headerless native process set their network shape explicitly.
				if strings.TrimSpace(req.Header.Get("X-Agent-ID")) == "" &&
					strings.TrimSpace(req.Header.Get("Origin")) == "" &&
					strings.TrimSpace(req.Header.Get("Sec-Fetch-Site")) == "" {
					req.Header.Set("Origin", "http://localhost:8080")
					req.Header.Set("Sec-Fetch-Site", "same-origin")
				}
			}
			next.ServeHTTP(w, req)
		})
	})
	h.RegisterRoutes(r)
	return r
}

func insertTestMemory(t *testing.T, s *store.SQLiteStore, id, domain string) {
	t.Helper()
	h := sha256.Sum256([]byte("content-" + id))
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "agent1",
		Content:         "content-" + id,
		ContentHash:     h[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       domain,
		ConfidenceScore: 0.85,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(context.Background(), rec))
}

func insertTestTask(t *testing.T, s *store.SQLiteStore, id, domain, provider string) {
	t.Helper()
	h := sha256.Sum256([]byte("[TASK] " + id))
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "author-agent",
		Content:         "[TASK] " + id,
		ContentHash:     h[:],
		MemoryType:      memory.TypeTask,
		DomainTag:       domain,
		Provider:        provider,
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		TaskStatus:      memory.TaskStatusPlanned,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(context.Background(), rec))
	require.NoError(t, s.UpdateMemoryClassification(context.Background(), id, store.ClearancePublic))
}

func TestSignedAgentCannotMutateDashboardMemoryMetadata(t *testing.T) {
	h, s := newTestHandler(t)
	router := testRouter(h)
	insertTestMemory(t, s, "metadata-target", "original.domain")

	_, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)

	tests := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{
			name:   "single memory update",
			method: http.MethodPatch,
			path:   "/v1/dashboard/memory/metadata-target",
			body:   []byte(`{"domain":"attacker.domain","tags":["injected"]}`),
		},
		{
			name:   "bulk memory update",
			method: http.MethodPost,
			path:   "/v1/dashboard/memory/bulk",
			body:   []byte(`{"ids":["metadata-target"],"domain":"attacker.domain","add_tags":["injected"]}`),
		},
		{
			name:   "replace memory tags",
			method: http.MethodPut,
			path:   "/v1/dashboard/memory/metadata-target/tags",
			body:   []byte(`{"tags":["injected"]}`),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewReader(tc.body))
			signAgentRequest(t, req, priv, tc.body)
			rr := httptest.NewRecorder()
			router.ServeHTTP(rr, req)
			require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
		})
	}

	rec, err := s.GetMemory(context.Background(), "metadata-target")
	require.NoError(t, err)
	assert.Equal(t, "original.domain", rec.DomainTag)
	tags, err := s.GetTags(context.Background(), "metadata-target")
	require.NoError(t, err)
	assert.Empty(t, tags)
}

func TestUnencryptedLoopbackCEREBRUMCanReadMemories(t *testing.T) {
	h, s := newTestHandler(t)
	router := testRouter(h)
	insertTestMemory(t, s, "loopback-target", "original.domain")

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/dashboard/memory/list",
		nil,
	)
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "loopback-target")
}

func TestCEREBRUMUIIsLoopbackOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	clients := []struct {
		name       string
		remoteAddr string
		host       string
		loopback   bool
	}{
		{
			name:       "loopback CEREBRUM",
			remoteAddr: "127.0.0.1:54321",
			host:       "localhost:8080",
			loopback:   true,
		},
		{
			name:       "IPv6 loopback CEREBRUM",
			remoteAddr: "[::1]:54321",
			host:       "[::1]:8080",
			loopback:   true,
		},
		{
			name:       "LAN CEREBRUM",
			remoteAddr: "192.168.1.20:54321",
			host:       "192.168.1.10:8080",
		},
		{
			name:       "DNS rebinding host",
			remoteAddr: "127.0.0.1:54321",
			host:       "attacker.example:8080",
		},
	}
	paths := []struct {
		path           string
		loopbackStatus int
	}{
		{path: "/", loopbackStatus: http.StatusFound},
		{path: "/ui/", loopbackStatus: http.StatusOK},
		{path: "/ui/launch", loopbackStatus: http.StatusFound},
		{path: "/ui/presence", loopbackStatus: http.StatusOK},
	}
	for _, client := range clients {
		for _, path := range paths {
			t.Run(client.name+" "+path.path, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, path.path, nil)
				req.RemoteAddr = client.remoteAddr
				req.Host = client.host
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				wantStatus := http.StatusNotFound
				if client.loopback {
					wantStatus = path.loopbackStatus
				}
				require.Equal(t, wantStatus, rec.Code, rec.Body.String())
			})
		}
	}
}

func TestCEREBRUMMissingStaticAssetsReturnTrue404(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)

	for _, requestPath := range []string{
		"/ui/js/missing.js",
		"/ui/js/missing.mjs",
		"/ui/js/vendor/missing.js",
		"/ui/css/missing.css",
		"/ui/js/missing.js.map",
		"/ui/assets/missing.wasm",
	} {
		t.Run(requestPath, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, requestPath, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
			assert.NotContains(t, strings.ToLower(rec.Header().Get("Content-Type")), "text/html")
			assert.NotContains(t, strings.ToLower(rec.Body.String()), "<!doctype html")
			assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
		})
	}
}

func TestCEREBRUMExtensionlessRouteUsesBootstrapShell(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	req := httptest.NewRequest(http.MethodGet, "/ui/future-client-route", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, rec.Body.String(), `data-sage-bootstrap-state="pending"`)
	assert.NotContains(t, rec.Body.String(), "?v=774",
		"SPA fallback must receive the same content-hash rewrite as /ui/")
}

func TestCEREBRUMHumanControlEntryPointsAreLoopbackOnly(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	entryPoints := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/v1/dashboard/auth/login"},
		{method: http.MethodPost, path: "/v1/dashboard/auth/lock"},
		{method: http.MethodGet, path: "/v1/dashboard/auth/check"},
		{method: http.MethodPost, path: "/v1/dashboard/settings/ledger/recover"},
	}
	for _, entry := range entryPoints {
		t.Run(entry.method+" "+entry.path, func(t *testing.T) {
			for _, client := range []struct {
				name       string
				remoteAddr string
				host       string
				hidden     bool
			}{
				{
					name:       "local",
					remoteAddr: "127.0.0.1:54321",
					host:       "localhost:8080",
				},
				{
					name:       "LAN",
					remoteAddr: "192.168.1.20:54321",
					host:       "192.168.1.10:8080",
					hidden:     true,
				},
				{
					name:       "rebinding",
					remoteAddr: "127.0.0.1:54321",
					host:       "attacker.example:8080",
					hidden:     true,
				},
			} {
				req := httptest.NewRequest(entry.method, entry.path, bytes.NewReader([]byte(`{}`)))
				req.Header.Set("Content-Type", "application/json")
				req.RemoteAddr = client.remoteAddr
				req.Host = client.host
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if client.hidden {
					require.Equal(t, http.StatusNotFound, rec.Code, client.name+": "+rec.Body.String())
				} else {
					require.NotEqual(t, http.StatusNotFound, rec.Code, client.name+": "+rec.Body.String())
				}
			}
		})
	}
}

func TestCEREBRUMRejectsRemoteRequestsForwardedThroughLoopbackProxy(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)

	for _, proxyEvidence := range []struct {
		name  string
		key   string
		value string
	}{
		{name: "X-Forwarded-For", key: "X-Forwarded-For", value: "198.51.100.8"},
		{name: "X-Real-IP", key: "X-Real-IP", value: "198.51.100.8"},
		{name: "X-Forwarded-Host", key: "X-Forwarded-Host", value: "sage.example.com"},
		{
			name: "Forwarded", key: "Forwarded",
			value: `for=198.51.100.8;host="localhost:8080"`,
		},
	} {
		for _, routeClass := range []struct {
			name   string
			method string
			path   string
			body   []byte
		}{
			{name: "UI", method: http.MethodGet, path: "/ui/"},
			{name: "authentication", method: http.MethodGet, path: "/v1/dashboard/auth/check"},
			{
				name: "recovery", method: http.MethodPost,
				path: "/v1/dashboard/settings/ledger/recover", body: []byte(`{}`),
			},
			{
				name: "protected dashboard", method: http.MethodGet,
				path: "/v1/dashboard/settings/onboarding",
			},
		} {
			t.Run(proxyEvidence.name+"/"+routeClass.name, func(t *testing.T) {
				req := httptest.NewRequest(
					routeClass.method, routeClass.path, bytes.NewReader(routeClass.body),
				)
				req.RemoteAddr = "127.0.0.1:54321"
				req.Host = "localhost:8080"
				req.Header.Set("Origin", "http://localhost:8080")
				req.Header.Set("Sec-Fetch-Site", "same-origin")
				req.Header.Set(proxyEvidence.key, proxyEvidence.value)
				if len(routeClass.body) > 0 {
					req.Header.Set("Content-Type", "application/json")
				}

				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
			})
		}
	}
}

func TestUnencryptedCEREBRUMOperatorCompatibilityIsLoopbackBrowserOnly(t *testing.T) {
	tests := []struct {
		name      string
		remote    string
		host      string
		origin    string
		secFetch  string
		wantAllow bool
	}{
		{
			name:   "loopback browser fetch metadata",
			remote: "127.0.0.1:54321", host: "localhost:8080",
			secFetch: "same-origin", wantAllow: true,
		},
		{
			name:   "loopback older browser origin fallback",
			remote: "127.0.0.1:54321", host: "127.0.0.1:8080",
			origin: "http://127.0.0.1:8080", wantAllow: true,
		},
		{
			name:   "unsigned loopback process",
			remote: "127.0.0.1:54321", host: "localhost:8080",
		},
		{
			name:   "cross site browser",
			remote: "127.0.0.1:54321", host: "localhost:8080",
			origin: "https://attacker.example", secFetch: "cross-site",
		},
		{
			name:   "LAN browser",
			remote: "192.168.1.42:54321", host: "192.168.1.10:8080",
			origin: "http://192.168.1.10:8080", secFetch: "same-origin",
		},
		{
			name:   "remote peer spoofing loopback headers",
			remote: "192.168.1.42:54321", host: "localhost:8080",
			origin: "http://localhost:8080", secFetch: "same-origin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
			req.RemoteAddr = tt.remote
			req.Host = tt.host
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			if tt.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tt.secFetch)
			}
			assert.Equal(t, tt.wantAllow, isLoopbackCEREBRUMBrowserRequest(req))
		})
	}

	t.Run("signed non-operator browser", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
		req.RemoteAddr = "127.0.0.1:54321"
		req.Host = "localhost:8080"
		req.Header.Set("Origin", "http://localhost:8080")
		req.Header.Set("Sec-Fetch-Site", "same-origin")
		req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, "signed-non-operator"))
		assert.False(t, isLoopbackCEREBRUMBrowserRequest(req))
	})
}

func TestExactSignedNodeOperatorCanMutateDashboardMemoryMetadata(t *testing.T) {
	h, s := newTestHandler(t)
	router := testRouter(h)
	insertTestMemory(t, s, "operator-target", "original.domain")

	pub, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.NodeOperatorAgentID = auth.PublicKeyToAgentID(pub)
	body := []byte(`{"domain":"operator.domain"}`)
	req := httptest.NewRequest(
		http.MethodPatch,
		"/v1/dashboard/memory/operator-target",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	signAgentRequest(t, req, priv, body)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	record, err := s.GetMemory(context.Background(), "operator-target")
	require.NoError(t, err)
	assert.Equal(t, "operator.domain", record.DomainTag)
}

func TestDashboardOperatorDefaultDeniesUnsignedAgentRemovalAndBundleDownload(t *testing.T) {
	h, s := newTestHandler(t)
	router := testRouter(h)
	expires := time.Now().Add(time.Hour)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID:        "operator-gate-target",
		Name:           "Operator Gate Target",
		Role:           "member",
		Status:         "active",
		BundlePath:     filepath.Join(t.TempDir(), "agent-bundle.zip"),
		ClaimToken:     "SECRET-CLAIM",
		ClaimExpiresAt: &expires,
		CreatedAt:      time.Now().UTC(),
	}))

	unsignedProcessRequest := func(method, path string) *http.Request {
		req := httptest.NewRequest(method, path, nil)
		req.Host = "localhost:8080"
		req.RemoteAddr = "127.0.0.1:54321"
		return req
	}

	removeResp := httptest.NewRecorder()
	router.ServeHTTP(removeResp, unsignedProcessRequest(http.MethodDelete, "/v1/dashboard/network/agents/operator-gate-target"))
	require.Equal(t, http.StatusForbidden, removeResp.Code, removeResp.Body.String())
	_, err := s.GetAgent(context.Background(), "operator-gate-target")
	require.NoError(t, err, "denied removal must leave the agent intact")

	bundleResp := httptest.NewRecorder()
	router.ServeHTTP(bundleResp, unsignedProcessRequest(http.MethodGet, "/v1/dashboard/network/agents/operator-gate-target/bundle"))
	require.Equal(t, http.StatusForbidden, bundleResp.Code, bundleResp.Body.String())

	exportResp := httptest.NewRecorder()
	router.ServeHTTP(exportResp, unsignedProcessRequest(http.MethodGet, "/v1/dashboard/export"))
	require.Equal(t, http.StatusForbidden, exportResp.Code, exportResp.Body.String())

	memoryResp := httptest.NewRecorder()
	router.ServeHTTP(memoryResp, unsignedProcessRequest(http.MethodGet, "/v1/dashboard/memory/list"))
	require.Equal(t, http.StatusForbidden, memoryResp.Code, memoryResp.Body.String())

	pipelineResp := httptest.NewRecorder()
	router.ServeHTTP(pipelineResp, unsignedProcessRequest(http.MethodGet, "/v1/dashboard/pipeline"))
	require.Equal(t, http.StatusForbidden, pipelineResp.Code, pipelineResp.Body.String())

	listResp := httptest.NewRecorder()
	router.ServeHTTP(listResp, unsignedProcessRequest(http.MethodGet, "/v1/dashboard/network/agents"))
	require.Equal(t, http.StatusForbidden, listResp.Code, listResp.Body.String())

	enableBody := []byte(`{"passphrase":"local-process-chosen"}`)
	enableReq := unsignedProcessRequest(http.MethodPost, "/v1/dashboard/settings/ledger/enable")
	enableReq.Body = io.NopCloser(bytes.NewReader(enableBody))
	enableReq.Header.Set("Content-Type", "application/json")
	enableResp := httptest.NewRecorder()
	router.ServeHTTP(enableResp, enableReq)
	require.Equal(t, http.StatusForbidden, enableResp.Code, enableResp.Body.String())
	assert.False(t, h.Encrypted.Load(), "unsigned local software must not choose the vault passphrase")
}

func TestDashboardOperatorDefaultRejectsOrdinarySignerAndAcceptsExactOperator(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	body := []byte(`{"done":true}`)

	_, ordinaryKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	ordinaryReq := httptest.NewRequest(
		http.MethodPost,
		"/v1/dashboard/settings/onboarding",
		bytes.NewReader(body),
	)
	ordinaryReq.Header.Set("Content-Type", "application/json")
	signAgentRequest(t, ordinaryReq, ordinaryKey, body)
	ordinaryResp := httptest.NewRecorder()
	router.ServeHTTP(ordinaryResp, ordinaryReq)
	require.Equal(t, http.StatusForbidden, ordinaryResp.Code, ordinaryResp.Body.String())

	ordinaryReadReq := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list", nil)
	signAgentGET(t, ordinaryReadReq, ordinaryKey)
	ordinaryReadResp := httptest.NewRecorder()
	router.ServeHTTP(ordinaryReadResp, ordinaryReadReq)
	require.Equal(t, http.StatusForbidden, ordinaryReadResp.Code, ordinaryReadResp.Body.String())

	operatorPub, operatorKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.NodeOperatorAgentID = auth.PublicKeyToAgentID(operatorPub)
	operatorReq := httptest.NewRequest(
		http.MethodPost,
		"/v1/dashboard/settings/onboarding",
		bytes.NewReader(body),
	)
	operatorReq.Header.Set("Content-Type", "application/json")
	signAgentRequest(t, operatorReq, operatorKey, body)
	operatorResp := httptest.NewRecorder()
	router.ServeHTTP(operatorResp, operatorReq)
	require.Equal(t, http.StatusOK, operatorResp.Code, operatorResp.Body.String())
}

func TestBootInstructionsReadRequiresOperatorOrActiveRegisteredAgent(t *testing.T) {
	h, s := newTestHandler(t)
	require.NoError(t, h.prefStore.SetPreference(context.Background(), "boot_instructions", "operator-selected context"))
	router := testRouter(h)

	activePub, activeKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	activeID := auth.PublicKeyToAgentID(activePub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: activeID, Name: "active", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	inactivePub, inactiveKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	inactiveID := auth.PublicKeyToAgentID(inactivePub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: inactiveID, Name: "inactive", Role: "member", Status: "inactive", CreatedAt: time.Now().UTC(),
	}))

	removedPub, removedKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	removedID := auth.PublicKeyToAgentID(removedPub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: removedID, Name: "removed", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))
	require.NoError(t, s.RemoveAgent(context.Background(), removedID))

	get := func(key ed25519pkg.PrivateKey) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/settings/boot-instructions", nil)
		signAgentGET(t, req, key)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		return rr
	}

	unsignedReq := httptest.NewRequest(http.MethodGet, "/v1/dashboard/settings/boot-instructions", nil)
	unsignedReq.Header.Set("Origin", "http://localhost:8080")
	unsignedReq.Header.Set("Sec-Fetch-Site", "same-origin")
	unsignedReq.Host = "localhost:8080"
	unsignedReq.RemoteAddr = "127.0.0.1:54321"
	unsigned := httptest.NewRecorder()
	router.ServeHTTP(unsigned, unsignedReq)
	require.Equal(t, http.StatusOK, unsigned.Code, unsigned.Body.String())
	assert.Contains(t, unsigned.Body.String(), "operator-selected context")

	_, unregisteredKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusForbidden, get(unregisteredKey).Code)
	require.Equal(t, http.StatusForbidden, get(inactiveKey).Code)
	require.Equal(t, http.StatusForbidden, get(removedKey).Code)

	active := get(activeKey)
	require.Equal(t, http.StatusOK, active.Code, active.Body.String())
	assert.Contains(t, active.Body.String(), "operator-selected context")

	operatorPub, operatorKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.NodeOperatorAgentID = auth.PublicKeyToAgentID(operatorPub)
	operator := get(operatorKey)
	require.Equal(t, http.StatusOK, operator.Code, operator.Body.String())

	sessionReq := httptest.NewRequest(http.MethodGet, "/v1/dashboard/settings/boot-instructions", nil)
	markLocalCEREBRUM(h, sessionReq)
	session := httptest.NewRecorder()
	router.ServeHTTP(session, sessionReq)
	require.Equal(t, http.StatusOK, session.Code, session.Body.String())
}

func signAgentGET(t *testing.T, req *http.Request, priv ed25519pkg.PrivateKey) string {
	return signAgentRequest(t, req, priv, nil)
}

func markLocalCEREBRUM(h *DashboardHandler, req *http.Request) {
	const token = "test-cerebrum-session"
	h.Encrypted.Store(true)
	h.sessions.Store(token, time.Now().Add(time.Hour))
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
}

func TestDashboardPresenceTracksLiveSSEClients(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)

	check := func(wantActive bool, wantClients int) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/ui/presence", nil)
		req.RemoteAddr = "127.0.0.1:54321"
		req.Host = "localhost:8080"
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		var payload struct {
			Active  bool `json:"active"`
			Clients int  `json:"clients"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
		assert.Equal(t, wantActive, payload.Active)
		assert.Equal(t, wantClients, payload.Clients)
	}

	check(false, 0)
	ch := h.SSE.Subscribe()
	require.NotNil(t, ch)
	check(true, 1)
	h.SSE.Unsubscribe(ch)
	check(false, 0)

	remoteReq := httptest.NewRequest(http.MethodGet, "/ui/presence", nil)
	remoteReq.RemoteAddr = "192.0.2.10:54321"
	remoteW := httptest.NewRecorder()
	router.ServeHTTP(remoteW, remoteReq)
	assert.Equal(t, http.StatusNotFound, remoteW.Code)
}

func signAgentRequest(t *testing.T, req *http.Request, priv ed25519pkg.PrivateKey, body []byte) string {
	t.Helper()
	pub := priv.Public().(ed25519pkg.PublicKey)
	agentID := hex.EncodeToString(pub)
	ts := time.Now().Unix()
	nonce := make([]byte, 8)
	_, randErr := rand.Read(nonce)
	require.NoError(t, randErr)
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(priv, req.Method, req.URL.RequestURI(), body, ts, nonce)))
	return agentID
}

func signExactNodeOperatorRequest(t *testing.T, h *DashboardHandler, req *http.Request, body []byte) {
	t.Helper()
	pub, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.NodeOperatorAgentID = auth.PublicKeyToAgentID(pub)
	signAgentRequest(t, req, priv, body)
}

func TestDashboardAgentSignatureRejectsReplay(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	_, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)

	first := httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	signAgentGET(t, first, priv)
	second := httptest.NewRequest(http.MethodGet, first.URL.RequestURI(), nil)
	second.Header = first.Header.Clone()

	w := httptest.NewRecorder()
	router.ServeHTTP(w, first)
	require.NotEqual(t, http.StatusUnauthorized, w.Code, "first valid signature must pass authentication: %s", w.Body.String())
	w = httptest.NewRecorder()
	router.ServeHTTP(w, second)
	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
}

type transientClassificationStore struct {
	*store.SQLiteStore
	failNext atomic.Bool
}

func (s *transientClassificationStore) GetMemoryClassificationLocal(ctx context.Context, memoryID string) (int, error) {
	if s.failNext.CompareAndSwap(true, false) {
		return 0, fmt.Errorf("temporary classification lookup failure")
	}
	return s.SQLiteStore.GetMemoryClassificationLocal(ctx, memoryID)
}

func TestHandleListMemories(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	for i := 0; i < 5; i++ {
		insertTestMemory(t, s, fmt.Sprintf("m%d", i), "general")
	}

	req := httptest.NewRequest("GET", "/v1/dashboard/memory/list?limit=3", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, float64(5), resp["total"])
	memories := resp["memories"].([]any)
	assert.Len(t, memories, 3)
}

func TestHandleStats(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "security")
	insertTestMemory(t, s, "m2", "general")

	req := httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, float64(2), resp["total_memories"])
}

func TestCerebrumHidesFederationAuditMemories(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "user-memory", "general")
	insertTestMemory(t, s, "sync-anchor", "SAGE-SYNCAUDIT-GRP-ABC")

	t.Run("list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list?limit=50", nil)
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp struct {
			Memories []*memory.MemoryRecord `json:"memories"`
			Total    int                    `json:"total"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		require.Equal(t, 1, resp.Total)
		require.Len(t, resp.Memories, 1)
		assert.Equal(t, "user-memory", resp.Memories[0].MemoryID)
	})

	t.Run("search", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/list?q=content&limit=50", nil)
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp struct {
			Memories []*memory.MemoryRecord `json:"memories"`
			Total    int                    `json:"total"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		require.Equal(t, 1, resp.Total)
		require.Len(t, resp.Memories, 1)
		assert.Equal(t, "user-memory", resp.Memories[0].MemoryID)
	})

	t.Run("stats", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/stats", nil)
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp store.StoreStats
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		assert.Equal(t, 1, resp.TotalMemories)
		assert.Equal(t, map[string]int{"general": 1}, resp.ByDomain)
		assert.Equal(t, 1, resp.ByStatus[string(memory.StatusProposed)])
		assert.Equal(t, 1, resp.ByAgent["agent1"])
	})

	t.Run("graph", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/memory/graph?status=proposed&limit=50", nil)
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp struct {
			Nodes        []graphNode    `json:"nodes"`
			Total        int            `json:"total"`
			DomainCounts map[string]int `json:"domain_counts"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		require.Len(t, resp.Nodes, 1)
		assert.Equal(t, "user-memory", resp.Nodes[0].ID)
		assert.Equal(t, 1, resp.Total)
		assert.Equal(t, map[string]int{"general": 1}, resp.DomainCounts)
	})

	t.Run("timeline", func(t *testing.T) {
		from := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
		to := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		path := "/v1/dashboard/memory/timeline?bucket=hour&from=" + from + "&to=" + to
		req := httptest.NewRequest(http.MethodGet, path, nil)
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())

		var resp struct {
			Buckets []store.TimelineBucket `json:"buckets"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
		total := 0
		for _, bucket := range resp.Buckets {
			total += bucket.Count
		}
		assert.Equal(t, 1, total)
	})

	t.Run("portable export", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/export", nil)
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Contains(t, w.Body.String(), `"memory_id":"user-memory"`)
		assert.NotContains(t, strings.ToLower(w.Body.String()), "sage-syncaudit-")
		assert.NotContains(t, w.Body.String(), `"memory_id":"sync-anchor"`)
	})
}

func TestCerebrumCannotMutateFederationAuditMemory(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestMemory(t, s, "sync-anchor", "sage-syncaudit-grp-abc")
	insertTestMemory(t, s, "user-memory", "general")

	deleteReq := httptest.NewRequest(http.MethodDelete, "/v1/dashboard/memory/sync-anchor", nil)
	markLocalCEREBRUM(h, deleteReq)
	deleteW := httptest.NewRecorder()
	r.ServeHTTP(deleteW, deleteReq)
	require.Equal(t, http.StatusNotFound, deleteW.Code, deleteW.Body.String())

	body := bytes.NewBufferString(`{"domain":"general"}`)
	updateReq := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/memory/sync-anchor", body)
	updateReq.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, updateReq)
	updateW := httptest.NewRecorder()
	r.ServeHTTP(updateW, updateReq)
	require.Equal(t, http.StatusNotFound, updateW.Code, updateW.Body.String())

	reservedBody := bytes.NewBufferString(`{"domain":"SAGE-SYNCAUDIT-GRP-FAKE"}`)
	reservedReq := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/memory/user-memory", reservedBody)
	reservedReq.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, reservedReq)
	reservedW := httptest.NewRecorder()
	r.ServeHTTP(reservedW, reservedReq)
	require.Equal(t, http.StatusBadRequest, reservedW.Code, reservedW.Body.String())

	record, err := s.GetMemory(context.Background(), "sync-anchor")
	require.NoError(t, err)
	assert.Equal(t, "sage-syncaudit-grp-abc", record.DomainTag)
	assert.Equal(t, memory.StatusProposed, record.Status)
	userRecord, err := s.GetMemory(context.Background(), "user-memory")
	require.NoError(t, err)
	assert.Equal(t, "general", userRecord.DomainTag)
}

func TestTaskAssignmentCrossProviderAppearsInBacklogAndInbox(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	ctx := context.Background()
	insertTestTask(t, s, "assigned-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, s.CreateAgent(ctx, &store.AgentEntry{
		AgentID: agentID, Name: "claude-code/tii-sage", RegisteredName: "claude-code/tii-sage",
		Provider: "claude-code", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/assigned-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var assigned struct {
		NotificationCreated bool `json:"notification_created"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &assigned))
	require.True(t, assigned.NotificationCreated)

	body = bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req = httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/assigned-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &assigned))
	require.False(t, assigned.NotificationCreated)

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?provider=claude-code", nil)
	require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var backlog struct {
		Tasks []struct {
			MemoryID string `json:"memory_id"`
			Assignee string `json:"assignee"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &backlog))
	require.Len(t, backlog.Tasks, 1)
	require.Equal(t, "assigned-task", backlog.Tasks[0].MemoryID)
	require.Equal(t, agentID, backlog.Tasks[0].Assignee)

	// A dashboard/session/local caller cannot forge X-Agent-ID and consume the
	// agent's notice. The subsequent signed read must still receive it.
	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	req.Header.Set("X-Agent-ID", agentID)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())

	for read := 0; read < 2; read++ {
		wantCount := 1
		req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
		require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
		w = httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var inbox struct {
			Items []*store.AgentNotification `json:"items"`
			Count int                        `json:"count"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &inbox))
		require.Equal(t, wantCount, inbox.Count)
		if wantCount == 1 {
			require.Equal(t, "assigned-task", inbox.Items[0].TaskID)
		}
	}
}

func TestTaskBoardAllTrueRejectsSignedAgentCaller(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "public-board-task", "public-work", "codex")
	pub, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: hex.EncodeToString(pub),
		Name:    "task-board-agent",
		Status:  "active",
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true&limit=10000", nil)
	signAgentGET(t, req, priv)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), "task backlog")
}

func TestTaskBoardAllTrueRequiresLocalHumanCEREBRUM(t *testing.T) {
	h, s := newTestHandler(t)
	boardHandler := http.HandlerFunc(h.handleGetTasks)
	insertTestTask(t, s, "board-task", "work", "codex")

	tests := []struct {
		name       string
		origin     string
		secFetch   string
		host       string
		wantStatus int
	}{
		{name: "same-origin browser without session", origin: "http://localhost:8080", secFetch: "same-origin", host: "localhost:8080", wantStatus: http.StatusOK},
		{name: "origin-less caller", host: "localhost:8080", wantStatus: http.StatusForbidden},
		{name: "cross-origin browser", origin: "https://evil.example", secFetch: "cross-site", host: "localhost:8080", wantStatus: http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
			req.Host = tc.host
			if tc.name == "same-origin browser without session" {
				req.RemoteAddr = "127.0.0.1:54321"
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tc.secFetch)
			}
			w := httptest.NewRecorder()
			boardHandler.ServeHTTP(w, req)
			assert.Equal(t, tc.wantStatus, w.Code, w.Body.String())
		})
	}
}

func TestTaskBoardReturnsStatusTransitionClock(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestTask(t, s, "completed-board-task", "work", "codex")
	require.NoError(t, s.UpdateTaskStatus(context.Background(), "completed-board-task", memory.TaskStatusDone))

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	h.handleGetTasks(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var payload struct {
		Tasks []struct {
			MemoryID            string `json:"memory_id"`
			TaskStatusUpdatedAt string `json:"task_status_updated_at"`
		} `json:"tasks"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &payload))
	require.Len(t, payload.Tasks, 1)
	require.Equal(t, "completed-board-task", payload.Tasks[0].MemoryID)
	require.NotEmpty(t, payload.Tasks[0].TaskStatusUpdatedAt)
}

func TestTaskBoardReorderPersistsForLocalOperator(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestTask(t, s, "order-a", "work", "codex")
	insertTestTask(t, s, "order-b", "work", "codex")

	body := bytes.NewBufferString(`{"task_status":"planned","task_ids":["order-a","order-b"]}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/order", body)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	h.handleReorderTasksDashboard(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	tasks, err := s.GetAllTasks(context.Background(), "", 10)
	require.NoError(t, err)
	require.Equal(t, []string{"order-a", "order-b"}, []string{tasks[0].MemoryID, tasks[1].MemoryID})

	body = bytes.NewBufferString(`{"task_status":"planned","task_ids":["order-b","order-a"]}`)
	req = httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/order", body)
	req.RemoteAddr = "192.168.1.20:54321"
	w = httptest.NewRecorder()
	h.handleReorderTasksDashboard(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskOperatorSurfacesRejectAgentsAndUnauthenticatedLANCallers(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "guarded-task", "work", "codex")
	_, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)

	createBody := []byte(`{"content":"agent bypass","domain":"work"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/tasks", bytes.NewReader(createBody))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, priv, createBody)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())

	req = httptest.NewRequest(http.MethodDelete, "/v1/dashboard/memory/guarded-task", nil)
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, priv, nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	got, err := s.GetMemory(context.Background(), "guarded-task")
	require.NoError(t, err)
	require.NotEqual(t, memory.StatusDeprecated, got.Status)

	statusBody := []byte(`{"task_status":"done"}`)
	for _, req = range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks", nil),
		httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/guarded-task/status", bytes.NewReader(statusBody)),
	} {
		req.Host = "localhost:8080"
		req.RemoteAddr = "127.0.0.1:54321"
		if req.Method == http.MethodPut {
			req.Header.Set("Content-Type", "application/json")
		}
		w = httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	}

	// Browser headers are not LAN authentication when encryption is disabled.
	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
	req.Header.Set("Origin", "http://192.168.1.10:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "192.168.1.10:8080"
	req.RemoteAddr = "192.168.1.20:54321"
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
}

func TestTaskBoardRejectsAuthenticatedEncryptedLANSession(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestTask(t, s, "lan-task", "work", "codex")
	h.Encrypted.Store(true)
	h.sessions.Store("valid-lan-session", time.Now().Add(time.Hour))
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
	req.Header.Set("Origin", "http://192.168.1.10:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "192.168.1.10:8080"
	req.RemoteAddr = "192.168.1.20:54321"
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "valid-lan-session"})
	w := httptest.NewRecorder()
	h.handleGetTasks(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestCreateTaskConsensusFailureDoesNotInsertPhantom(t *testing.T) {
	h, s := newTestHandler(t)
	_, key, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.SigningKey = key
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/broadcast_tx_commit", r.URL.Path)
		writeCommitTxRejected(w, r, 7, "rejected", 1)
	}))
	t.Cleanup(rpc.Close)
	h.CometBFTRPC = rpc.URL
	r := testRouter(h)
	body := []byte(`{"content":"must commit","domain":"work"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	stats, err := s.GetStats(context.Background())
	require.NoError(t, err)
	require.Zero(t, stats.TotalMemories, "rejected consensus task must not appear in the local board")
}

func TestCreateTaskStandaloneStoresProposedAuthoredTask(t *testing.T) {
	h, s := newTestHandler(t)
	h.SetEmbedder(&fakeEmbedder{name: "ollama", dimension: 3, ready: true, semantic: true})
	r := testRouter(h)
	body := []byte(`{"content":"standalone work","domain":"work"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	var response map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	got, err := s.GetMemory(context.Background(), response["memory_id"])
	require.NoError(t, err)
	require.Equal(t, memory.StatusProposed, got.Status)
	require.Equal(t, "cerebrum", got.SubmittingAgent)
	require.Equal(t, memory.TaskStatusPlanned, got.TaskStatus)
	counts, err := s.CountMemoriesByProvider(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, counts["ollama:3"])
	require.Zero(t, counts[""], "a newly embedded dashboard task must not re-enter the repair queue")
}

func TestTaskAssignmentRejectsInactiveOrUnknownAgent(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "guarded-task", "work", "codex")
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "inactive-agent", Name: "inactive", RegisteredName: "inactive", Status: "inactive",
	}))

	for _, assignee := range []string{"missing-agent", "inactive-agent"} {
		body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, assignee))
		req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/guarded-task/assign", body)
		req.Header.Set("Content-Type", "application/json")
		markLocalCEREBRUM(h, req)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	}
}

func TestTaskAssignmentRejectsSignedAgentCaller(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "operator-only-task", "work", "codex")
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "target-agent", Name: "target", RegisteredName: "target", Status: "active",
	}))

	pub, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	body := []byte(`{"assignee":"target-agent"}`)
	path := "/v1/dashboard/tasks/operator-only-task/assign"
	ts := time.Now().Unix()
	req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", hex.EncodeToString(pub))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", ts))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequest(priv, http.MethodPut, path, body, ts)))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskAssignmentDoesNotBypassAgentDomainAllowlist(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "forbidden-domain-task", "work", "codex")
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "restricted-agent", Name: "restricted", RegisteredName: "restricted", Status: "active",
		DomainAccess: `[{"domain":"other","read":true,"write":true}]`,
	}))

	body := bytes.NewBufferString(`{"assignee":"restricted-agent"}`)
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/forbidden-domain-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskNotificationRechecksRBACAfterAssignment(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "reclassified-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: agentID, Name: "agent-a", RegisteredName: "agent-a", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/reclassified-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, s.UpdateMemoryClassification(context.Background(), "reclassified-task", store.ClearanceConfidential))

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var inbox struct {
		Count int `json:"count"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &inbox))
	require.Zero(t, inbox.Count, "a notice must not reveal a task after its read access is revoked")
}

func TestTaskNotificationRejectsAgentDeactivatedAfterAssignment(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "deactivated-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: agentID, Name: "agent-a", RegisteredName: "agent-a", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/deactivated-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NoError(t, s.UpdateAgentStatus(context.Background(), agentID, "inactive"))

	req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
	require.Equal(t, agentID, signAgentGET(t, req, agentPriv))
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
}

func TestTaskNotificationTransientAuthorizationFailureDoesNotConsumeNotice(t *testing.T) {
	h, sqliteStore := newTestHandler(t)
	wrapped := &transientClassificationStore{SQLiteStore: sqliteStore}
	h.store = wrapped
	r := testRouter(h)
	insertTestTask(t, sqliteStore, "retry-auth-task", "work", "codex")
	agentPub, agentPriv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(agentPub)
	require.NoError(t, sqliteStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: agentID, Name: "agent-a", RegisteredName: "agent-a", Status: "active",
	}))

	body := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/retry-auth-task/assign", body)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	wrapped.failNext.Store(true)
	for _, wantCount := range []int{0, 1} {
		req = httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications", nil)
		signAgentGET(t, req, agentPriv)
		w = httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		var inbox struct {
			Count int `json:"count"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &inbox))
		require.Equal(t, wantCount, inbox.Count)
	}
}

func TestAgentTaskStatusRequiresActiveCurrentOwnerAndCannotReopen(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	insertTestTask(t, s, "owned-task", "work", "codex")
	pubA, privA, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	pubB, privB, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentA := hex.EncodeToString(pubA)
	agentB := hex.EncodeToString(pubB)
	for id, name := range map[string]string{agentA: "agent-a", agentB: "agent-b"} {
		require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
			AgentID: id, Name: name, RegisteredName: name, Status: "active",
		}))
	}

	assignBody := bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentA))
	req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/owned-task/assign", assignBody)
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	putStatus := func(priv ed25519pkg.PrivateKey, status string) *httptest.ResponseRecorder {
		t.Helper()
		body := []byte(fmt.Sprintf(`{"task_status":%q}`, status))
		path := "/v1/dashboard/tasks/owned-task/status"
		req := httptest.NewRequest(http.MethodPut, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		signAgentRequest(t, req, priv, body)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		return rr
	}

	for _, status := range []string{"done", "dropped"} {
		rr := putStatus(privB, status)
		require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	}
	require.Equal(t, http.StatusForbidden, putStatus(privB, "planned").Code)

	require.NoError(t, s.UpdateAgentStatus(context.Background(), agentA, "inactive"))
	require.Equal(t, http.StatusForbidden, putStatus(privA, "done").Code)
	require.NoError(t, s.UpdateAgentStatus(context.Background(), agentA, "active"))
	require.Equal(t, http.StatusOK, putStatus(privA, "done").Code)
	require.Equal(t, http.StatusForbidden, putStatus(privA, "planned").Code)

	tasks, err := s.GetAllTasks(context.Background(), "work", 10)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, memory.TaskStatusDone, tasks[0].TaskStatus)
	require.Equal(t, agentA, tasks[0].Assignee, "completed card must retain the responsible agent")
}

func TestHandleDeleteMemory(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	req := httptest.NewRequest("DELETE", "/v1/dashboard/memory/m1", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "deleted", resp["status"])

	// Verify in store
	got, err := s.GetMemory(context.Background(), "m1")
	require.NoError(t, err)
	assert.Equal(t, memory.StatusDeprecated, got.Status)
}

func TestHandleUpdateMemory(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	body, _ := json.Marshal(map[string]string{"domain": "security"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/memory/m1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	got, err := s.GetMemory(context.Background(), "m1")
	require.NoError(t, err)
	assert.Equal(t, "security", got.DomainTag)
}

func TestHandleUpdateMemory_MissingDomain(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/memory/m1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGraph(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")
	insertTestMemory(t, s, "m2", "general")
	insertTestMemory(t, s, "m3", "security")

	req := httptest.NewRequest("GET", "/v1/dashboard/memory/graph?status=proposed", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	nodes := resp["nodes"].([]any)
	assert.Len(t, nodes, 3)
	// Should have domain edges (2 general memories = 1 domain edge)
	edges := resp["edges"].([]any)
	assert.GreaterOrEqual(t, len(edges), 1)
}

func TestGraphMaxNodesDefaultsToDenseMRISample(t *testing.T) {
	t.Setenv("SAGE_GRAPH_MAX_NODES", "")
	assert.Equal(t, 1500, graphMaxNodes())

	t.Setenv("SAGE_GRAPH_MAX_NODES", "8000")
	assert.Equal(t, appV23CerebrumInteractiveScanBudget, graphMaxNodes())
}

func TestHandleAuthCheck_NoAuth(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/auth/check", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["auth_required"])
	assert.Equal(t, true, resp["authenticated"])
}

func TestHandleAuthMiddleware_BlocksWithoutSession(t *testing.T) {
	h, _ := newTestHandler(t)
	// Simulate encryption enabled
	h.VaultKeyPath = "/tmp/fake-vault.key"
	h.Encrypted.Store(true)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "unauthorized", resp["error"])
	assert.Equal(t, true, resp["login_required"])
}

func TestHandleExport_AllMemories(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Insert more than the list endpoint's 200 cap
	for i := 0; i < 5; i++ {
		insertTestMemory(t, s, fmt.Sprintf("export-%d", i), "test-domain")
	}

	req := httptest.NewRequest("GET", "/v1/dashboard/export", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-ndjson")
	assert.Contains(t, w.Header().Get("Content-Disposition"), "sage-backup-")

	// Parse JSONL lines
	lines := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte("\n"))
	assert.Len(t, lines, 5)

	// Verify each line is valid JSON with expected fields
	for _, line := range lines {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		assert.NotEmpty(t, rec["memory_id"])
		assert.NotEmpty(t, rec["content"])
		assert.Equal(t, "test-domain", rec["domain_tag"])
		// Embeddings should be excluded
		assert.Nil(t, rec["embedding"])
	}
}

func TestHandleExport_ExcludesDeprecatedMemories(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "kept", "test-domain")
	insertTestMemory(t, s, "forgotten", "test-domain")
	require.NoError(t, s.UpdateStatus(context.Background(), "forgotten", memory.StatusDeprecated, time.Now().UTC()))

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/export", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	lines := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte("\n"))
	require.Len(t, lines, 1)
	var rec memory.MemoryRecord
	require.NoError(t, json.Unmarshal(lines[0], &rec))
	assert.Equal(t, "kept", rec.MemoryID)
	assert.NotContains(t, w.Body.String(), "content-forgotten")
}

func TestHandleExport_PaginatesAcrossPages(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Insert more than the internal page size (500) to force multiple pages.
	totalRecords := 520
	for i := 0; i < totalRecords; i++ {
		insertTestMemory(t, s, fmt.Sprintf("page-%04d", i), "bulk-domain")
	}

	req := httptest.NewRequest("GET", "/v1/dashboard/export", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Parse JSONL lines — should have ALL records, not capped at 500
	lines := bytes.Split(bytes.TrimSpace(w.Body.Bytes()), []byte("\n"))
	assert.Len(t, lines, totalRecords)

	// Verify first and last records are valid JSON
	var first, last map[string]any
	require.NoError(t, json.Unmarshal(lines[0], &first))
	require.NoError(t, json.Unmarshal(lines[totalRecords-1], &last))
	assert.NotEmpty(t, first["memory_id"])
	assert.NotEmpty(t, last["memory_id"])
	assert.Equal(t, "bulk-domain", first["domain_tag"])
	assert.Equal(t, "bulk-domain", last["domain_tag"])
}

func TestHandleExport_EmptyDB(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/export", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "application/x-ndjson")
	// The body carries a format marker so Import can truthfully identify an
	// empty SAGE backup instead of misclassifying it as another JSONL format.
	var manifest map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(w.Body.Bytes()), &manifest))
	assert.Equal(t, "sage_backup_manifest", manifest["record_type"])
	assert.Equal(t, float64(1), manifest["sage_backup_version"])
}

func TestMemoryBackup_FreshHandlerRoundTrip(t *testing.T) {
	sourceHandler, sourceStore := newTestHandler(t)
	insertTestMemory(t, sourceStore, "portable-1", "project-alpha")
	insertTestMemory(t, sourceStore, "portable-2", "project-beta")
	insertTestMemory(t, sourceStore, "forgotten", "private-history")
	require.NoError(t, sourceStore.UpdateStatus(context.Background(), "forgotten", memory.StatusDeprecated, time.Now().UTC()))

	exportReq := httptest.NewRequest(http.MethodGet, "/v1/dashboard/export", nil)
	markLocalCEREBRUM(sourceHandler, exportReq)
	exportW := httptest.NewRecorder()
	testRouter(sourceHandler).ServeHTTP(exportW, exportReq)
	require.Equal(t, http.StatusOK, exportW.Code)

	// A second handler/store represents a newly installed SAGE. Exercise the
	// same preview + explicit confirmation routes used by CEREBRUM rather than
	// calling the parser or store directly.
	freshHandler, freshStore := newTestHandler(t)
	freshRouter := testRouter(freshHandler)
	var upload bytes.Buffer
	writer := multipart.NewWriter(&upload)
	part, err := writer.CreateFormFile("file", "sage-memory-backup.jsonl")
	require.NoError(t, err)
	_, err = part.Write(exportW.Body.Bytes())
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	previewReq := httptest.NewRequest(http.MethodPost, "/v1/dashboard/import/preview", &upload)
	previewReq.Header.Set("Content-Type", writer.FormDataContentType())
	markLocalCEREBRUM(freshHandler, previewReq)
	previewW := httptest.NewRecorder()
	freshRouter.ServeHTTP(previewW, previewReq)
	require.Equal(t, http.StatusOK, previewW.Code, previewW.Body.String())
	var preview map[string]any
	require.NoError(t, json.Unmarshal(previewW.Body.Bytes(), &preview))
	require.Equal(t, "sage-backup", preview["source"])
	require.Equal(t, float64(2), preview["total"])
	importID, ok := preview["import_id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, importID)

	confirmBody, err := json.Marshal(map[string]string{"import_id": importID})
	require.NoError(t, err)
	confirmReq := httptest.NewRequest(http.MethodPost, "/v1/dashboard/import/confirm", bytes.NewReader(confirmBody))
	confirmReq.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(freshHandler, confirmReq)
	confirmW := httptest.NewRecorder()
	freshRouter.ServeHTTP(confirmW, confirmReq)
	require.Equal(t, http.StatusOK, confirmW.Code, confirmW.Body.String())

	restored, total, err := freshStore.ListMemories(context.Background(), store.ListOptions{Limit: 10, Sort: "oldest"})
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, restored, 2)
	assert.Equal(t, "project-alpha", restored[0].DomainTag)
	assert.Equal(t, "content-portable-1", restored[0].Content)
	assert.NotEqual(t, "portable-1", restored[0].MemoryID)
	expectedHash := sha256.Sum256([]byte(restored[0].Content))
	assert.Equal(t, expectedHash[:], restored[0].ContentHash)
}

func TestMemoryBackup_DeprecatedOnlyPreviewExplainsNothingWillRestore(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	var upload bytes.Buffer
	writer := multipart.NewWriter(&upload)
	part, err := writer.CreateFormFile("file", "old-sage-backup.jsonl")
	require.NoError(t, err)
	_, err = part.Write([]byte(`{"memory_id":"forgotten","content":"Do not restore this","memory_type":"observation","domain_tag":"general","status":"deprecated"}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/import/preview", &upload)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	var response map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	assert.Equal(t, "no_restorable_memories", response["error"])
	assert.Contains(t, response["message"], "Memories you chose to forget will stay forgotten")
}

func TestHandleAuthMiddleware_DynamicEncryption(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	// Encryption OFF — should allow access
	req := httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Enable encryption dynamically (simulates enabling via dashboard)
	h.Encrypted.Store(true)

	// Same request without cookie — should be blocked
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)

	// Disable encryption — should allow again
	h.Encrypted.Store(false)
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleLock_InvalidatesSession(t *testing.T) {
	h, _ := newTestHandler(t)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")
	r := testRouter(h)

	// Enable encryption and login
	body, _ := json.Marshal(map[string]string{"passphrase": "test-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	signExactNodeOperatorRequest(t, h, req, body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Login to get session
	body, _ = json.Marshal(map[string]string{"passphrase": "test-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	cookies := w.Result().Cookies()
	require.NotEmpty(t, cookies)
	sessionCookie := cookies[0]

	// Verify session works
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Lock — invalidates the session
	req = httptest.NewRequest("POST", "/v1/dashboard/auth/lock", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)
	var lockResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &lockResp))
	assert.Equal(t, true, lockResp["locked"])

	// Same session cookie should now be rejected
	req = httptest.NewRequest("GET", "/v1/dashboard/stats", nil)
	req.AddCookie(sessionCookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleTimeline(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	insertTestMemory(t, s, "m1", "general")

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour).Format(time.RFC3339)
	to := now.Add(time.Hour).Format(time.RFC3339)
	url := fmt.Sprintf("/v1/dashboard/memory/timeline?from=%s&to=%s&bucket=hour", from, to)

	req := httptest.NewRequest("GET", url, nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Contains(t, resp, "buckets")
}

// ---------------------------------------------------------------------------
// Network / Template tests
// ---------------------------------------------------------------------------

func TestHandleTemplates(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/network/templates", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Templates []struct {
			Name      string `json:"name"`
			Role      string `json:"role"`
			Bio       string `json:"bio"`
			Clearance int    `json:"clearance"`
			Avatar    string `json:"avatar"`
		} `json:"templates"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.GreaterOrEqual(t, len(resp.Templates), 2, "should have multiple templates")

	// Verify Coding Assistant template is present
	found := false
	for _, tmpl := range resp.Templates {
		if tmpl.Name == "Coding Assistant" {
			found = true
			assert.Equal(t, "member", tmpl.Role)
			assert.NotEmpty(t, tmpl.Bio)
			assert.Equal(t, 1, tmpl.Clearance)
			break
		}
	}
	assert.True(t, found, "Coding Assistant template should be present")
}

func TestHandleTemplatesContainsExpected(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/network/templates", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp struct {
		Templates []struct {
			Name string `json:"name"`
		} `json:"templates"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))

	names := make([]string, len(resp.Templates))
	for i, t := range resp.Templates {
		names[i] = t.Name
	}
	assert.Contains(t, names, "Coding Assistant")
	assert.Contains(t, names, "Voice Assistant")
	assert.Contains(t, names, "Research Agent")
	assert.Contains(t, names, "Custom")
}

// ---------------------------------------------------------------------------
// Unregistered agents test
// ---------------------------------------------------------------------------

func insertTestMemoryWithAgent(t *testing.T, s *store.SQLiteStore, id, domain, agentID string) {
	t.Helper()
	h := sha256.Sum256([]byte("content-" + id))
	rec := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: agentID,
		Content:         "content-" + id,
		ContentHash:     h[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       domain,
		ConfidenceScore: 0.85,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(context.Background(), rec))
}

func TestHandleUnregisteredAgents(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Create an agent in the dashboard
	agent := &store.AgentEntry{
		AgentID:   "registered-agent-id",
		Name:      "Registered Agent",
		Role:      "admin",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Insert memories from the registered agent
	insertTestMemoryWithAgent(t, s, "m1", "general", "registered-agent-id")

	// Insert memories from an unregistered agent
	insertTestMemoryWithAgent(t, s, "m2", "general", "orphan-agent-id")
	insertTestMemoryWithAgent(t, s, "m3", "security", "orphan-agent-id")

	req := httptest.NewRequest("GET", "/v1/dashboard/network/unregistered", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Unregistered []struct {
			AgentID     string `json:"agent_id"`
			MemoryCount int    `json:"memory_count"`
			ShortID     string `json:"short_id"`
		} `json:"unregistered"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Len(t, resp.Unregistered, 1, "should find exactly one unregistered agent")
	assert.Equal(t, "orphan-agent-id", resp.Unregistered[0].AgentID)
	assert.Equal(t, 2, resp.Unregistered[0].MemoryCount)
	assert.NotEmpty(t, resp.Unregistered[0].ShortID)
}

func TestHandleUnregisteredAgents_NoneOrphan(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)

	// Create an agent and memories from only that agent
	agent := &store.AgentEntry{
		AgentID:   "only-agent",
		Name:      "Only Agent",
		Role:      "admin",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))
	insertTestMemoryWithAgent(t, s, "m1", "general", "only-agent")

	req := httptest.NewRequest("GET", "/v1/dashboard/network/unregistered", nil)
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp struct {
		Unregistered []any `json:"unregistered"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Len(t, resp.Unregistered, 0, "no orphaned agents should be found")
}

// --- Agent Update Synchronous Broadcast Tests --------------------------------

func TestHandleUpdateAgent_SyncBroadcast_Success(t *testing.T) {
	// When CometBFT is available, name updates should broadcast synchronously
	// and the response should NOT contain on_chain_warning.
	var broadcastSeen bool
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcastSeen = true
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"result":{"code":0,"hash":"%s"}}`, requestBoundHash(r))
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, priv, _ := ed25519GenerateKey()
	h.SigningKey = priv
	r := testRouter(h)

	// Create an agent in SQLite
	agent := &store.AgentEntry{
		AgentID:   "agent-rename-1",
		Name:      "Old Name",
		Role:      "member",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	// Rename via PATCH
	body, _ := json.Marshal(map[string]string{"name": "New Display Name"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/network/agents/agent-rename-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "New Display Name", resp["name"])
	assert.Nil(t, resp["on_chain_warning"], "should not have warning on success")
	assert.True(t, broadcastSeen, "CometBFT broadcast should have been called")

	// Verify SQLite was also updated
	updated, err := s.GetAgent(context.Background(), "agent-rename-1")
	require.NoError(t, err)
	assert.Equal(t, "New Display Name", updated.Name)
}

func TestHandleUpdateAgent_SyncBroadcast_Failure_ReturnsWarning(t *testing.T) {
	// When CometBFT broadcast fails, the response should include on_chain_warning
	// but the SQLite update should still succeed (best-effort).
	// Simulate CometBFT being down. That outcome is indeterminate and fences
	// the signing key, so the stub resolves the fence at teardown.
	cometMock := fenceResolvingRPC(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, priv, _ := ed25519GenerateKey()
	h.SigningKey = priv
	r := testRouter(h)

	agent := &store.AgentEntry{
		AgentID:   "agent-rename-2",
		Name:      "Old Name",
		Role:      "member",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	body, _ := json.Marshal(map[string]string{"name": "Renamed Agent"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/network/agents/agent-rename-2", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "Renamed Agent", resp["name"])
	assert.NotNil(t, resp["on_chain_warning"], "should have warning when broadcast fails")

	// SQLite should still have the new name
	updated, err := s.GetAgent(context.Background(), "agent-rename-2")
	require.NoError(t, err)
	assert.Equal(t, "Renamed Agent", updated.Name)
}

func TestHandleUpdateAgent_NoCometBFT_NoWarning(t *testing.T) {
	// When CometBFT is not configured, no broadcast happens and no warning.
	h, s := newTestHandler(t)
	// CometBFTRPC and SigningKey left empty
	r := testRouter(h)

	agent := &store.AgentEntry{
		AgentID:   "agent-rename-3",
		Name:      "Old Name",
		Role:      "member",
		Status:    "active",
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, s.CreateAgent(context.Background(), agent))

	body, _ := json.Marshal(map[string]string{"name": "Renamed Without Consensus"})
	req := httptest.NewRequest("PATCH", "/v1/dashboard/network/agents/agent-rename-3", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalCEREBRUM(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "Renamed Without Consensus", resp["name"])
	assert.Nil(t, resp["on_chain_warning"])
}

func TestHandleUpdateAgent_PermissionsSignedByGenesisAdmin(t *testing.T) {
	var captured *tx.ParsedTx
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/broadcast_tx_sync" {
			_, _ = fmt.Fprintf(w, `{"result":{"code":0,"hash":"%s"}}`, requestBoundHash(r))
			return
		}
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, decErr := hex.DecodeString(raw)
		require.NoError(t, decErr)
		captured, decErr = tx.DecodeTx(encoded)
		require.NoError(t, decErr)
		w.Header().Set("Content-Type", "application/json")
		writeCommitOK(w, r)
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, validatorKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	adminPub, adminKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.SigningKey = validatorKey
	h.AdminSigningKey = adminKey
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-permission-1", Name: "Local Agent", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body, err := json.Marshal(map[string]any{
		"domain_access": `[{"domain":"research","read":true,"write":false}]`,
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-permission-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AgentSetPermission)
	assert.Equal(t, []byte(adminPub), captured.AgentPubKey, "permission tx must carry genesis admin proof")
	assert.NotEqual(t, validatorKey.Public(), captured.AgentPubKey, "validator key is not the RBAC admin")
}

func TestHandleUpdateAgent_AppV22CapabilitiesCommitBeforeSuccess(t *testing.T) {
	var captured *tx.ParsedTx
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/broadcast_tx_sync" {
			_, _ = fmt.Fprintf(w, `{"result":{"code":0,"hash":"%s"}}`, requestBoundHash(r))
			return
		}
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, decErr := hex.DecodeString(raw)
		require.NoError(t, decErr)
		captured, decErr = tx.DecodeTx(encoded)
		require.NoError(t, decErr)
		w.Header().Set("Content-Type", "application/json")
		writeCommitOKAt(w, r, 88, "")
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, h.SigningKey, _ = ed25519pkg.GenerateKey(nil)
	_, h.AdminSigningKey, _ = ed25519pkg.GenerateKey(nil)
	h.AppV22ActiveFn = func() bool { return true }
	h.BadgerStore = newGrantTestBadger(t)
	require.NoError(t, h.BadgerStore.RegisterAgent("agent-capability-1", "Mynah", "member", "", "voice-bridge", "", 1))
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-capability-1", Name: "Mynah", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body := []byte(`{"name":"Mynah Updated","capabilities":15}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-capability-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	w := httptest.NewRecorder()
	testRouter(h).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AgentSetPermission)
	assert.Equal(t, uint32(15), captured.AgentSetPermission.Capabilities)
	assert.True(t, captured.AgentSetPermission.CapabilitiesPresent)
	updated, err := s.GetAgent(context.Background(), "agent-capability-1")
	require.NoError(t, err)
	assert.Equal(t, "Mynah Updated", updated.Name, "ordinary metadata remains independently persisted")
	assert.Zero(t, updated.Capabilities, "capabilities remain chain-authoritative instead of creating a second SQLite source of truth")
}

func TestHandleUpdateAgent_AppV22CapabilityFailureRestoresProjection(t *testing.T) {
	// A bare 500 is indeterminate (the node may have accepted the permission
	// tx), so it fences the admin key; resolve that fence at teardown.
	cometMock := fenceResolvingRPC(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, h.SigningKey, _ = ed25519pkg.GenerateKey(nil)
	_, h.AdminSigningKey, _ = ed25519pkg.GenerateKey(nil)
	h.AppV22ActiveFn = func() bool { return true }
	h.BadgerStore = newGrantTestBadger(t)
	require.NoError(t, h.BadgerStore.RegisterAgent("agent-capability-2", "Mynah", "member", "", "voice-bridge", "", 1))
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-capability-2", Name: "Mynah", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body := []byte(`{"name":"Mynah Metadata Survives","capabilities":15}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-capability-2", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	w := httptest.NewRecorder()
	testRouter(h).ServeHTTP(w, req)

	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	updated, err := s.GetAgent(context.Background(), "agent-capability-2")
	require.NoError(t, err)
	assert.Equal(t, "Mynah Metadata Survives", updated.Name, "permission rollback must preserve unrelated metadata edits")
	assert.Zero(t, updated.Capabilities, "CEREBRUM must not display an uncommitted restriction mask")
}

func TestHandleUpdateAgent_ZeroCapabilityIsLegacyCompatibleBeforeAppV22(t *testing.T) {
	var captured *tx.ParsedTx
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, decErr := hex.DecodeString(raw)
		require.NoError(t, decErr)
		captured, decErr = tx.DecodeTx(encoded)
		require.NoError(t, decErr)
		w.Header().Set("Content-Type", "application/json")
		writeCommitOKAt(w, r, 87, "")
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, h.SigningKey, _ = ed25519pkg.GenerateKey(nil)
	_, h.AdminSigningKey, _ = ed25519pkg.GenerateKey(nil)
	h.AppV22ActiveFn = func() bool { return false }
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-capability-legacy", Name: "Legacy", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body := []byte(`{"visible_agents":"*","capabilities":0}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-capability-legacy", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	w := httptest.NewRecorder()
	testRouter(h).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	updated, err := s.GetAgent(context.Background(), "agent-capability-legacy")
	require.NoError(t, err)
	assert.Equal(t, "*", updated.VisibleAgents)
	assert.Zero(t, updated.Capabilities)
	require.NotNil(t, captured)
	require.NotNil(t, captured.AgentSetPermission)
	assert.False(t, captured.AgentSetPermission.CapabilitiesPresent,
		"an unchanged explicit zero must stay on the legacy wire before app-v22")
}

func TestHandleUpdateAgent_PermissionFailureRestoresAllPolicyFields(t *testing.T) {
	// Same shape as the capability-failure test above: an indeterminate 500
	// fences the admin key, and the stub resolves it at teardown.
	cometMock := fenceResolvingRPC(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, h.SigningKey, _ = ed25519pkg.GenerateKey(nil)
	_, h.AdminSigningKey, _ = ed25519pkg.GenerateKey(nil)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-policy-rollback", Name: "Rollback", Role: "member",
		Clearance: 1, DomainAccess: `[{"domain":"old","read":true}]`,
		VisibleAgents: "old-agent", OrgID: "old-org", DeptID: "old-dept",
		Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body := []byte(`{"clearance":4,"domain_access":"[{\"domain\":\"new\",\"read\":true,\"write\":true}]","visible_agents":"*","org_id":"new-org","dept_id":"new-dept"}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-policy-rollback", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	w := httptest.NewRecorder()
	testRouter(h).ServeHTTP(w, req)

	require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
	updated, err := s.GetAgent(context.Background(), "agent-policy-rollback")
	require.NoError(t, err)
	assert.Equal(t, 1, updated.Clearance)
	assert.Equal(t, `[{"domain":"old","read":true}]`, updated.DomainAccess)
	assert.Equal(t, "old-agent", updated.VisibleAgents)
	assert.Equal(t, "old-org", updated.OrgID)
	assert.Equal(t, "old-dept", updated.DeptID)
}

func TestOverlayOnChainAgentPolicyReplacesStaleSQLiteRBAC(t *testing.T) {
	projected := &store.AgentEntry{
		Role: "member", Clearance: 1, OrgID: "stale-org", DeptID: "stale-dept",
		DomainAccess:  `[{"domain":"stale","read":true,"write":true}]`,
		VisibleAgents: "*", Capabilities: 0,
	}
	authoritative := &store.OnChainAgent{
		Role: "member", Clearance: 2, OrgID: "live-org", DeptID: "live-dept",
		DomainAccess:  `[{"domain":"live","read":true}]`,
		VisibleAgents: "agent-live", Capabilities: store.AgentCapabilityReadAllDomains,
	}

	overlayOnChainAgentPolicy(projected, authoritative)

	assert.Equal(t, 2, projected.Clearance)
	assert.Equal(t, "live-org", projected.OrgID)
	assert.Equal(t, "live-dept", projected.DeptID)
	assert.Equal(t, `[{"domain":"live","read":true}]`, projected.DomainAccess)
	assert.Equal(t, "agent-live", projected.VisibleAgents)
	assert.Equal(t, store.AgentCapabilityReadAllDomains, projected.Capabilities)
}

func TestHandleUpdateAgent_PublishesCommittedPermissionInvalidation(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeCommitOKAt(w, r, 77, "")
	}))
	defer cometMock.Close()

	h, s := newTestHandler(t)
	h.CometBFTRPC = cometMock.URL
	_, validatorKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.SigningKey = validatorKey
	_, adminKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	h.AdminSigningKey = adminKey
	h.SSE = NewSSEBroadcaster()
	events := h.SSE.Subscribe()
	defer h.SSE.Unsubscribe(events)
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-activity-1", Name: "Activity Agent", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))

	body := []byte(`{"clearance":2}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-activity-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	requirePayloadFreeAccessInvalidation(t, events)
}

func TestHandleUpdateAgent_PermissionsRejectSignedAgentWithoutOverride(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-policy-target", Name: "Target", Role: "member", Status: "active", Clearance: 1, CreatedAt: time.Now().UTC(),
	}))
	_, callerKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	body, err := json.Marshal(map[string]any{
		"clearance":     4,
		"domain_access": `[{"domain":"everything","read":true,"write":true}]`,
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-policy-target", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, callerKey, body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	after, err := s.GetAgent(context.Background(), "agent-policy-target")
	require.NoError(t, err)
	assert.Equal(t, 1, after.Clearance)
	assert.Empty(t, after.DomainAccess, "rejected signed-agent update must not mutate local policy")
}

func TestHandleCreateAgentRejectsSignedAgentWithoutSideEffects(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	_, callerKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	body, err := json.Marshal(map[string]any{
		"name":          "self-elevated-agent",
		"role":          "member",
		"clearance":     4,
		"domain_access": `[{"domain":"*","read":true,"write":true,"modify":true}]`,
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/network/agents", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, callerKey, body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code, w.Body.String())
	agents, err := s.ListAgents(context.Background())
	require.NoError(t, err)
	assert.Empty(t, agents, "rejected create must not persist or mint an agent")
}

func TestDomainAccessRejectsModifyForDynamicallySharedDomain(t *testing.T) {
	h, s := newTestHandler(t)
	shared := newGrantTestBadger(t)
	require.NoError(t, shared.SetState("shared_domain:community-updates", []byte{1}))
	h.BadgerStore = shared
	r := testRouter(h)

	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "dynamic-shared-target", Name: "Target", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))
	body := []byte(`{"domain_access":"[{\"domain\":\"community-updates\",\"read\":true,\"write\":true,\"modify\":true}]"}`)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/dynamic-shared-target", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	agent, err := s.GetAgent(context.Background(), "dynamic-shared-target")
	require.NoError(t, err)
	assert.Empty(t, agent.DomainAccess, "rejected level-3 shared request must not be persisted")

	createReq := httptest.NewRequest(http.MethodPost, "/v1/dashboard/network/agents", bytes.NewReader(body))
	createReq.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, createReq)
	createW := httptest.NewRecorder()
	r.ServeHTTP(createW, createReq)
	assert.Equal(t, http.StatusBadRequest, createW.Code, createW.Body.String())
	agents, err := s.ListAgents(context.Background())
	require.NoError(t, err)
	assert.Len(t, agents, 1, "rejected create must not persist an agent")
}

func TestHandleUpdateAgent_AdminOverrideRejectsSignedAgent(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-override-target", Name: "Target", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))
	_, callerKey, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	body, err := json.Marshal(map[string]any{
		"domain_access": `[{"domain":"research","read":true,"write":false}]`,
		"admin_override": []map[string]any{{
			"domain": "research", "owner_id": strings.Repeat("a", 64), "owned_domain": "research", "level": 1,
		}},
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-override-target", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	signAgentRequest(t, req, callerKey, body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
	after, err := s.GetAgent(context.Background(), "agent-override-target")
	require.NoError(t, err)
	assert.Empty(t, after.DomainAccess, "rejected agent override must not mutate local policy")
}

func TestHandleUpdateAgent_AdminOverrideRejectsNoOriginCaller(t *testing.T) {
	h, s := newTestHandler(t)
	r := testRouter(h)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "agent-override-target", Name: "Target", Role: "member", Status: "active", CreatedAt: time.Now().UTC(),
	}))
	body, err := json.Marshal(map[string]any{
		"admin_override": []map[string]any{{
			"domain": "research", "owner_id": strings.Repeat("a", 64), "owned_domain": "research", "level": 1,
		}},
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPatch, "/v1/dashboard/network/agents/agent-override-target", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)
}

// ed25519GenerateKey is a test helper wrapping crypto/ed25519.GenerateKey.
func ed25519GenerateKey() (ed25519PublicKey, ed25519PrivateKey, error) {
	return ed25519pkg.GenerateKey(nil)
}

// Type aliases to avoid import naming conflicts with sha256.
type ed25519PublicKey = ed25519pkg.PublicKey
type ed25519PrivateKey = ed25519pkg.PrivateKey

func TestTaskNoticeInspectionStaysUnreadUntilExplicitAcknowledgement(t *testing.T) {
	h, s := newTestHandler(t)
	router := testRouter(h)
	pub, priv, err := ed25519pkg.GenerateKey(nil)
	require.NoError(t, err)
	agentID := hex.EncodeToString(pub)
	require.NoError(t, s.CreateAgent(context.Background(), &store.AgentEntry{AgentID: agentID, Name: "notice-agent", RegisteredName: "notice-agent", Status: "active"}))
	for _, id := range []string{"notice-task-a", "notice-task-b"} {
		insertTestTask(t, s, id, "work", "codex")
		req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/tasks/"+id+"/assign", bytes.NewBufferString(fmt.Sprintf(`{"assignee":%q}`, agentID)))
		req.Header.Set("Content-Type", "application/json")
		markLocalCEREBRUM(h, req)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		require.Equal(t, 200, recorder.Code, recorder.Body.String())
	}
	var id string
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/task-notifications/inbox?limit=1", nil)
		signAgentGET(t, req, priv)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		require.Equal(t, 200, recorder.Code, recorder.Body.String())
		var body struct {
			Items   []store.AgentNotification
			Passive bool
			HasMore bool   `json:"has_more"`
			Next    string `json:"next_cursor"`
		}
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
		require.True(t, body.Passive)
		require.True(t, body.HasMore)
		require.NotEmpty(t, body.Next)
		require.Len(t, body.Items, 1)
		require.Equal(t, "unread", body.Items[0].State)
		id = body.Items[0].NotificationID
	}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPut, "/v1/dashboard/task-notifications/"+id+"/ack", nil)
		signAgentRequest(t, req, priv, nil)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		require.Equal(t, 200, recorder.Code, recorder.Body.String())
	}
	remaining, err := s.PeekAgentNotifications(context.Background(), agentID, 20)
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	require.NotEqual(t, id, remaining[0].NotificationID)
}
