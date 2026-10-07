package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

type appV23DashboardRouteFixture struct {
	handler *DashboardHandler
	sql     *store.SQLiteStore
	badger  *store.BadgerStore
	keys    map[string]ed25519.PrivateKey
	ids     map[string]string
}

func publishAppV23DashboardRecord(
	t *testing.T,
	sqlStore *store.SQLiteStore,
	badgerStore *store.BadgerStore,
	memoryID string,
	classification uint8,
	appV23Created bool,
) {
	t.Helper()
	record, err := sqlStore.GetMemory(context.Background(), memoryID)
	require.NoError(t, err)
	require.NoError(t, badgerStore.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))
	require.NoError(t, badgerStore.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, badgerStore.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, badgerStore.SetMemoryClassification(record.MemoryID, classification))
	if appV23Created {
		require.NoError(t, badgerStore.SetMemoryAuthorPrincipal(
			record.MemoryID, record.SubmittingAgent,
		))
	}
}

func newAppV23DashboardRouteFixture(t *testing.T) appV23DashboardRouteFixture {
	t.Helper()
	sqlStore, err := store.NewSQLiteStore(
		context.Background(), filepath.Join(t.TempDir(), "sage.db"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })

	keys := make(map[string]ed25519.PrivateKey)
	ids := make(map[string]string)
	for _, name := range []string{
		"historical-root", "current-root", "member", "manager",
		"stale-admin", "current-admin", "pending", "inactive", "unregistered",
	} {
		_, key, keyErr := ed25519.GenerateKey(nil)
		require.NoError(t, keyErr)
		keys[name] = key
		ids[name] = agentIDForKey(key)
	}

	require.NoError(t, badgerStore.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: ids["historical-root"], Scope: "dashboard-route-matrix",
		AgentID: ids["member"], Profile: store.AppV23ProfileStandard,
		HomeDomain: "member.home", Clearance: 2, Capabilities: 0,
		Height: 1, BootstrapDigest: "dashboard-route-fixture",
	}))
	approve := func(
		name, role string,
		generation uint64,
		active bool,
		clearance uint8,
		capabilities store.AgentCapabilities,
	) {
		t.Helper()
		require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
			ids[name], name, store.AppV23RoleMember, "", "test", "", 2, 0,
		))
		require.NoError(t, badgerStore.ApproveAppV23LocalAgent(
			store.AppV23LocalEnrollment{
				AgentID: ids[name], ApprovedBy: ids["historical-root"],
				RootGeneration: generation, Profile: store.AppV23ProfileStandard,
				HomeDomain: name + ".home", Clearance: clearance,
				Capabilities: capabilities, Active: active, UpdatedHeight: 3,
			},
			role, 0, 0,
		))
	}
	approve("manager", store.AppV23RoleManager, 1, true, 2, 0)
	approve(
		"stale-admin", store.AppV23RoleAdmin, 1, true, 4,
		store.AgentCapabilityReadAllDomains,
	)
	require.NoError(t, badgerStore.RegisterAgentWithCapabilities(
		ids["pending"], "pending", store.AppV23RoleMember, "", "test", "", 2, 0,
	))
	approve("inactive", store.AppV23RoleMember, 1, false, 2, 0)
	require.NoError(t, badgerStore.RotateAppV23RootCredential(
		1, ids["current-root"], 4,
	))
	approve(
		"current-admin", store.AppV23RoleAdmin, 2, true, 4,
		store.AgentCapabilityReadAllDomains,
	)

	for _, name := range []string{
		"historical-root", "current-root", "member", "manager",
		"stale-admin", "current-admin", "pending", "inactive", "unregistered",
	} {
		require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
			AgentID: ids[name], Name: name, Role: store.AppV23RoleMember,
			Status: "active", CreatedAt: time.Now().UTC(),
		}))
	}

	h := NewDashboardHandler(sqlStore, "test")
	// Mirror the native node's supervised shutdown: audits must finish before
	// the fixture closes Badger/SQLite, including cancelled request audits.
	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	var workerMu sync.Mutex
	stopping := false
	h.RunBackground = func(fn func(context.Context)) {
		workerMu.Lock()
		defer workerMu.Unlock()
		if stopping {
			return
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			fn(workerCtx)
		}()
	}
	t.Cleanup(func() {
		workerMu.Lock()
		stopping = true
		cancelWorkers()
		workerMu.Unlock()
		workers.Wait()
	})
	h.BadgerStore = badgerStore
	h.AdminSigningKey = keys["current-root"]
	h.AppV23ActiveFn = func() bool { return true }
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		for name, candidateID := range ids {
			if candidateID == id {
				return keys[name], true
			}
		}
		return nil, false
	}
	return appV23DashboardRouteFixture{
		handler: h, sql: sqlStore, badger: badgerStore, keys: keys, ids: ids,
	}
}

func TestAppV23SignedDashboardCompatibilityRequiresOrdinaryConsensusIdentity(t *testing.T) {
	fixture := newAppV23DashboardRouteFixture(t)
	protected := fixture.handler.authMiddleware(
		fixture.handler.cerebrumBrowserLocalityGate(http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			},
		)),
	)
	routes := []struct {
		name   string
		method string
		path   string
		body   []byte
	}{
		{name: "tasks", method: http.MethodGet, path: "/v1/dashboard/tasks"},
		{name: "task notifications", method: http.MethodGet, path: "/v1/dashboard/task-notifications"},
		{name: "boot instructions", method: http.MethodGet, path: "/v1/dashboard/settings/boot-instructions"},
		{name: "stats", method: http.MethodGet, path: "/v1/dashboard/stats"},
		{name: "recall settings", method: http.MethodGet, path: "/v1/dashboard/settings/recall"},
		{name: "memory mode", method: http.MethodGet, path: "/v1/dashboard/settings/memory-mode"},
		{name: "proposals", method: http.MethodGet, path: "/v1/dashboard/governance/proposals"},
		{name: "proposal detail", method: http.MethodGet, path: "/v1/dashboard/governance/proposals/proposal-1"},
		{
			name: "pre validate", method: http.MethodPost, path: "/v1/memory/pre-validate",
			body: []byte(`{"content":"test","domain":"test"}`),
		},
		{
			name: "task status", method: http.MethodPut, path: "/v1/dashboard/tasks/task-1/status",
			body: []byte(`{"task_status":"in_progress"}`),
		},
	}
	for _, tc := range []struct {
		name  string
		allow bool
	}{
		{name: "member", allow: true},
		{name: "manager", allow: true},
		{name: "historical-root"},
		{name: "current-root"},
		{name: "stale-admin"},
		{name: "current-admin"},
		{name: "pending"},
		{name: "inactive"},
		{name: "unregistered"},
	} {
		for _, route := range routes {
			t.Run("remote "+tc.name+" "+route.name, func(t *testing.T) {
				req := httptest.NewRequest(
					route.method, route.path, bytes.NewReader(route.body),
				)
				req.RemoteAddr = "192.0.2.20:54321"
				req.Host = "192.0.2.10:8080"
				signAgentRequest(t, req, fixture.keys[tc.name], route.body)
				rec := httptest.NewRecorder()
				protected.ServeHTTP(rec, req)
				want := http.StatusForbidden
				if tc.allow {
					want = http.StatusNoContent
				}
				require.Equal(t, want, rec.Code, rec.Body.String())
			})
		}
	}
	for _, route := range routes {
		t.Run("local current-admin "+route.name, func(t *testing.T) {
			req := httptest.NewRequest(
				route.method, route.path, bytes.NewReader(route.body),
			)
			req.RemoteAddr = "127.0.0.1:54321"
			req.Host = "localhost:8080"
			signAgentRequest(t, req, fixture.keys["current-admin"], route.body)
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, req)
			require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
		})
	}
}

func TestAppV23LegacyMCPStatsReturnsOnlyCallerPresenceLowerBound(t *testing.T) {
	fixture := newAppV23DashboardRouteFixture(t)
	router := testRouter(fixture.handler)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/stats", nil)
	req.RemoteAddr = "192.0.2.20:54321"
	req.Host = "192.0.2.10:8080"
	signAgentGET(t, req, fixture.keys["member"])
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, float64(1), response["total_memories"])
	require.Equal(t, false, response["total_exact"])
	require.Equal(t, true, response["has_more"])
	require.Equal(t, "caller", response["scope"])
	for _, forbidden := range []string{
		"projection", "db_size_bytes", "by_agent", "by_domain", "by_status",
	} {
		require.NotContains(t, response, forbidden)
	}
}

func TestPreV23SignedDashboardCompatibilityPreservesActiveRegistryBehavior(t *testing.T) {
	h, sqlStore := newTestHandler(t)
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	id := agentIDForKey(key)
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: id, Name: "legacy admin", Role: store.AppV23RoleAdmin,
		Status: "active", CreatedAt: time.Now().UTC(),
	}))
	protected := h.authMiddleware(h.cerebrumBrowserLocalityGate(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	))
	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks", nil)
	req.RemoteAddr = "192.0.2.20:54321"
	req.Host = "192.0.2.10:8080"
	signAgentGET(t, req, key)
	rec := httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
}

func TestAppV23TaskAgentRoutesRejectRootHistoryAndStaleAdmin(t *testing.T) {
	fixture := newAppV23DashboardRouteFixture(t)
	protected := fixture.handler.authMiddleware(
		fixture.handler.cerebrumBrowserLocalityGate(http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			},
		)),
	)
	for _, actor := range []string{
		"historical-root", "current-root", "stale-admin",
	} {
		for _, route := range []struct {
			method string
			path   string
			body   []byte
		}{
			{
				method: http.MethodGet,
				path:   "/v1/dashboard/task-notifications",
			},
			{
				method: http.MethodPut,
				path:   "/v1/dashboard/tasks/task-1/status",
				body:   []byte(`{"task_status":"in_progress"}`),
			},
		} {
			t.Run(actor+" "+route.method+" "+route.path, func(t *testing.T) {
				req := httptest.NewRequest(
					route.method, route.path, bytes.NewReader(route.body),
				)
				req.RemoteAddr = "127.0.0.1:54321"
				req.Host = "localhost:8080"
				signAgentRequest(t, req, fixture.keys[actor], route.body)
				rec := httptest.NewRecorder()
				protected.ServeHTTP(rec, req)
				want := http.StatusForbidden
				if actor == "current-root" && route.method == http.MethodPut {
					want = http.StatusNoContent
				}
				require.Equal(t, want, rec.Code, rec.Body.String())
			})
		}
	}

	router := testRouter(fixture.handler)
	insertTestTask(t, fixture.sql, "admin-task", "member.home", fixture.ids["member"])
	publishAppV23DashboardRecord(
		t, fixture.sql, fixture.badger, "admin-task",
		uint8(store.ClearancePublic), true,
	)
	_, err := fixture.sql.AssignTaskAndNotify(
		context.Background(), "admin-task", fixture.ids["current-admin"],
	)
	require.NoError(t, err)

	notificationReq := httptest.NewRequest(
		http.MethodGet, "/v1/dashboard/task-notifications", nil,
	)
	notificationReq.RemoteAddr = "192.0.2.20:54321"
	notificationReq.Host = "192.0.2.10:8080"
	signAgentGET(t, notificationReq, fixture.keys["current-admin"])
	notificationRec := httptest.NewRecorder()
	router.ServeHTTP(notificationRec, notificationReq)
	require.Equal(t, http.StatusForbidden, notificationRec.Code,
		"a promoted Admin must not use agent compatibility routes remotely: %s",
		notificationRec.Body.String())

	statusBody := []byte(`{"task_status":"in_progress"}`)
	statusReq := httptest.NewRequest(
		http.MethodPut, "/v1/dashboard/tasks/admin-task/status",
		bytes.NewReader(statusBody),
	)
	statusReq.RemoteAddr = "192.0.2.20:54321"
	statusReq.Host = "192.0.2.10:8080"
	statusReq.Header.Set("Content-Type", "application/json")
	signAgentRequest(t, statusReq, fixture.keys["current-admin"], statusBody)
	statusRec := httptest.NewRecorder()
	router.ServeHTTP(statusRec, statusReq)
	require.Equal(t, http.StatusForbidden, statusRec.Code,
		"a promoted Admin must not update tasks remotely: %s",
		statusRec.Body.String())

	localNotificationReq := httptest.NewRequest(
		http.MethodGet, "/v1/dashboard/task-notifications", nil,
	)
	localNotificationReq.RemoteAddr = "127.0.0.1:54321"
	localNotificationReq.Host = "localhost:8080"
	signAgentGET(t, localNotificationReq, fixture.keys["current-admin"])
	localNotificationRec := httptest.NewRecorder()
	router.ServeHTTP(localNotificationRec, localNotificationReq)
	require.Equal(t, http.StatusOK, localNotificationRec.Code,
		"a promoted current-generation Admin remains usable on its local node: %s",
		localNotificationRec.Body.String())

	localStatusReq := httptest.NewRequest(
		http.MethodPut, "/v1/dashboard/tasks/admin-task/status",
		bytes.NewReader(statusBody),
	)
	localStatusReq.RemoteAddr = "127.0.0.1:54321"
	localStatusReq.Host = "localhost:8080"
	localStatusReq.Header.Set("Content-Type", "application/json")
	signAgentRequest(t, localStatusReq, fixture.keys["current-admin"], statusBody)
	localStatusRec := httptest.NewRecorder()
	router.ServeHTTP(localStatusRec, localStatusReq)
	require.Equal(t, http.StatusOK, localStatusRec.Code,
		"a promoted current-generation Admin remains an ordinary task agent locally: %s",
		localStatusRec.Body.String())

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/tasks?all=true", nil)
	req.RemoteAddr = "127.0.0.1:54321"
	req.Host = "localhost:8080"
	signAgentGET(t, req, fixture.keys["current-root"])
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code,
		"current Root retains the mixed human CEREBRUM task-list view: %s",
		rec.Body.String())
}

func TestAppV23TaskStatusDispatchSeparatesLocalOperatorsFromExactAssignees(t *testing.T) {
	fixture := newAppV23DashboardRouteFixture(t)
	router := testRouter(fixture.handler)

	tests := []struct {
		name       string
		actor      string
		local      bool
		assignee   string
		status     memory.TaskStatus
		wantStatus int
		wantTask   memory.TaskStatus
	}{
		{
			name:  "local current Admin replans another agent task",
			actor: "current-admin", local: true, assignee: "member",
			status:     memory.TaskStatusPlanned,
			wantStatus: http.StatusOK, wantTask: memory.TaskStatusPlanned,
		},
		{
			name:  "remote current Admin is not an ordinary task agent",
			actor: "current-admin", assignee: "current-admin",
			status:     memory.TaskStatusDone,
			wantStatus: http.StatusForbidden, wantTask: memory.TaskStatusInProgress,
		},
		{
			name:  "local stale Admin is denied",
			actor: "stale-admin", local: true, assignee: "stale-admin",
			status:     memory.TaskStatusDone,
			wantStatus: http.StatusForbidden, wantTask: memory.TaskStatusInProgress,
		},
		{
			name:  "Member completes exact assignment",
			actor: "member", local: true, assignee: "member",
			status:     memory.TaskStatusDone,
			wantStatus: http.StatusOK, wantTask: memory.TaskStatusDone,
		},
		{
			name:  "Manager cannot complete another agents assignment",
			actor: "manager", local: true, assignee: "member",
			status:     memory.TaskStatusDone,
			wantStatus: http.StatusForbidden, wantTask: memory.TaskStatusInProgress,
		},
		{
			name:  "local current Root uses operator path",
			actor: "current-root", local: true, assignee: "member",
			status:     memory.TaskStatusPlanned,
			wantStatus: http.StatusOK, wantTask: memory.TaskStatusPlanned,
		},
		{
			name:  "historical Root is never ordinary",
			actor: "historical-root", local: true, assignee: "historical-root",
			status:     memory.TaskStatusDone,
			wantStatus: http.StatusForbidden, wantTask: memory.TaskStatusInProgress,
		},
	}

	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			taskID := fmt.Sprintf("dispatch-task-%d", index)
			insertTestTask(t, fixture.sql, taskID, "member.home", fixture.ids["member"])
			publishAppV23DashboardRecord(
				t, fixture.sql, fixture.badger, taskID,
				uint8(store.ClearancePublic), true,
			)
			_, err := fixture.sql.AssignTaskAndNotify(
				context.Background(), taskID, fixture.ids[tc.assignee],
			)
			require.NoError(t, err)

			body := []byte(fmt.Sprintf(`{"task_status":%q}`, tc.status))
			req := httptest.NewRequest(
				http.MethodPut, "/v1/dashboard/tasks/"+taskID+"/status",
				bytes.NewReader(body),
			)
			if tc.local {
				req.RemoteAddr = "127.0.0.1:54321"
				req.Host = "localhost:8080"
			} else {
				req.RemoteAddr = "192.0.2.20:54321"
				req.Host = "192.0.2.10:8080"
			}
			req.Header.Set("Content-Type", "application/json")
			signAgentRequest(t, req, fixture.keys[tc.actor], body)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())

			task, err := fixture.sql.GetMemory(context.Background(), taskID)
			require.NoError(t, err)
			require.Equal(t, tc.wantTask, task.TaskStatus)
		})
	}
}

func TestUnsignedUnencryptedProtectedDashboardRequiresSameOriginCEREBRUM(t *testing.T) {
	tests := []struct {
		name       string
		encrypted  bool
		session    bool
		origin     string
		secFetch   string
		wantStatus int
	}{
		{
			name:       "unencrypted headerless local process",
			wantStatus: http.StatusForbidden,
		},
		{
			name:   "unencrypted same-origin browser",
			origin: "http://localhost:8080", secFetch: "same-origin",
			wantStatus: http.StatusOK,
		},
		{
			name:   "unencrypted same-site browser",
			origin: "http://localhost:8080", secFetch: "same-site",
			wantStatus: http.StatusForbidden,
		},
		{
			name:      "encrypted valid headerless local session remains compatible",
			encrypted: true, session: true, wantStatus: http.StatusOK,
		},
		{
			name:      "encrypted valid same-origin session",
			encrypted: true, session: true,
			origin: "http://localhost:8080", secFetch: "same-origin",
			wantStatus: http.StatusOK,
		},
		{
			name:      "encrypted valid cross-site session is still denied",
			encrypted: true, session: true,
			origin: "https://attacker.example", secFetch: "cross-site",
			wantStatus: http.StatusForbidden,
		},
		{
			name:      "encrypted request without session",
			encrypted: true, wantStatus: http.StatusUnauthorized,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHandler(t)
			h.Encrypted.Store(tc.encrypted)
			const token = "protected-route-session"
			if tc.session {
				h.sessions.Store(token, time.Now().Add(time.Hour))
			}
			router := testRouter(h)
			req := httptest.NewRequest(
				http.MethodGet, "/v1/dashboard/settings/onboarding", nil,
			)
			req.RemoteAddr = "127.0.0.1:54321"
			req.Host = "localhost:8080"
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tc.secFetch)
			}
			if tc.session {
				req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: token})
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

func TestCEREBRUMEntryRouteBrowserMetadataMatrix(t *testing.T) {
	handler := cerebrumLoopbackOnly(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		},
	))
	for _, tc := range []struct {
		name      string
		origin    string
		host      string
		secFetch  string
		forwarded string
		allow     bool
	}{
		{name: "headerless native local call", allow: true},
		{name: "same-origin without Origin", secFetch: "same-origin", allow: true},
		{name: "navigation none without Origin", secFetch: "none", allow: true},
		{
			name:     "same-origin exact loopback Origin",
			secFetch: "same-origin", origin: "http://localhost:8080", allow: true,
		},
		{
			name:   "older browser exact loopback Origin",
			origin: "http://localhost:8080", allow: true,
		},
		{
			name:     "same-origin metadata cannot override different port",
			secFetch: "same-origin", origin: "http://localhost:9090",
		},
		{
			name:   "older browser different port denied",
			origin: "http://localhost:9090",
		},
		{
			name:     "same-origin metadata cannot override different scheme",
			secFetch: "same-origin", origin: "https://localhost:8080",
		},
		{
			name:     "localhost and ipv4 loopback are distinct origins",
			secFetch: "same-origin", origin: "http://127.0.0.1:8080",
		},
		{
			name: "ipv4 loopback and localhost are distinct origins",
			host: "127.0.0.1:8080", secFetch: "same-origin",
			origin: "http://localhost:8080",
		},
		{
			name: "exact ipv4 loopback origin",
			host: "127.0.0.1:8080", secFetch: "same-origin",
			origin: "http://127.0.0.1:8080", allow: true,
		},
		{
			name: "default http port is normalized",
			host: "localhost", secFetch: "same-origin",
			origin: "http://localhost", allow: true,
		},
		{
			name:     "same-site denied",
			secFetch: "same-site", origin: "http://localhost:8080",
		},
		{
			name:     "cross-site denied",
			secFetch: "cross-site", origin: "http://localhost:8080",
		},
		{
			name:     "same-origin foreign Origin denied",
			secFetch: "same-origin", origin: "https://attacker.example",
		},
		{name: "null Origin denied", secFetch: "same-origin", origin: "null"},
		{
			name:      "forwarded remote denied",
			forwarded: "198.51.100.8",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
			req.RemoteAddr = "127.0.0.1:54321"
			req.Host = tc.host
			if req.Host == "" {
				req.Host = "localhost:8080"
			}
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.secFetch != "" {
				req.Header.Set("Sec-Fetch-Site", tc.secFetch)
			}
			if tc.forwarded != "" {
				req.Header.Set("X-Forwarded-For", tc.forwarded)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			want := http.StatusNotFound
			if tc.allow {
				want = http.StatusNoContent
			}
			require.Equal(t, want, rec.Code, rec.Body.String())
		})
	}
}

func TestOriginMatchesRequestUsesExactNormalizedTuple(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target string
		host   string
		origin string
		match  bool
	}{
		{
			name: "http exact tuple", target: "http://localhost:8080/ui/",
			host: "localhost:8080", origin: "http://localhost:8080", match: true,
		},
		{
			name: "https exact tuple", target: "https://localhost:8443/ui/",
			host: "localhost:8443", origin: "https://localhost:8443", match: true,
		},
		{
			name: "http default port normalization", target: "http://localhost/ui/",
			host: "localhost", origin: "http://localhost:80", match: true,
		},
		{
			name: "https default port normalization", target: "https://localhost/ui/",
			host: "localhost:443", origin: "https://localhost", match: true,
		},
		{
			name: "different port", target: "http://localhost:8080/ui/",
			host: "localhost:8080", origin: "http://localhost:9090",
		},
		{
			name: "https origin on http target", target: "http://localhost:8080/ui/",
			host: "localhost:8080", origin: "https://localhost:8080",
		},
		{
			name: "http origin on https target", target: "https://localhost:8443/ui/",
			host: "localhost:8443", origin: "http://localhost:8443",
		},
		{
			name: "localhost is not ipv4 loopback", target: "http://localhost:8080/ui/",
			host: "localhost:8080", origin: "http://127.0.0.1:8080",
		},
		{
			name: "ipv4 loopback is not localhost", target: "http://127.0.0.1:8080/ui/",
			host: "127.0.0.1:8080", origin: "http://localhost:8080",
		},
		{
			name: "origin path is invalid", target: "http://localhost:8080/ui/",
			host: "localhost:8080", origin: "http://localhost:8080/",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.target, nil)
			req.Host = tc.host
			require.Equal(t, tc.match, originMatchesRequest(req, tc.origin))
		})
	}
}
