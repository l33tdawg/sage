package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

func appV23RESTAgentID(pair string) string {
	return strings.Repeat(pair, 32)
}

func setupAppV23RESTAccess(t *testing.T) (*Server, *store.BadgerStore, string, string, string) {
	t.Helper()
	srv, _, badger, _ := newRBACTestServer(t)
	rootID := appV23RESTAgentID("11")
	memberID := appV23RESTAgentID("22")
	ownerID := appV23RESTAgentID("33")
	outsiderID := appV23RESTAgentID("44")

	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "chain-a", AgentID: memberID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "member.home",
		Clearance: 2, Capabilities: 0, Height: 1, BootstrapDigest: "bootstrap",
	}))
	for _, input := range []struct {
		id, name, home string
	}{
		{ownerID, "owner", "owner.home"},
		{outsiderID, "outsider", "outsider.home"},
	} {
		require.NoError(t, badger.RegisterAgent(input.id, input.name, store.AppV23RoleMember, "", "test", "", 2))
		require.NoError(t, badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
			AgentID: input.id, ApprovedBy: rootID, RootGeneration: 1,
			Profile: store.AppV23ProfileStandard, HomeDomain: input.home,
			Clearance: 2, Capabilities: 0, Active: true, UpdatedHeight: 2,
		}, store.AppV23RoleMember, 0, 0))
	}
	require.NoError(t, badger.MutateAppV23AccessGroup(
		rootID, "local-team", "Local team", []string{memberID, ownerID}, 0, false, 3,
	))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })
	return srv, badger, memberID, ownerID, outsiderID
}

func TestAppV23RESTAccessGroupsDriveReadAndManagerVerbs(t *testing.T) {
	srv, badger, memberID, ownerID, outsiderID := setupAppV23RESTAccess(t)

	require.NoError(t, srv.checkDomainAccess(context.Background(), memberID, "member.home", "read"))
	require.NoError(t, srv.checkDomainAccess(context.Background(), memberID, "owner.home", "read"))
	require.Error(t, srv.checkDomainAccess(context.Background(), memberID, "owner.home", "write"))
	require.Error(t, srv.checkDomainAccess(context.Background(), outsiderID, "owner.home", "read"))

	memberEnrollment, err := badger.GetAppV23Enrollment(memberID)
	require.NoError(t, err)
	memberRole, err := badger.GetAppV23Role(memberID)
	require.NoError(t, err)
	require.NoError(t, badger.SetAppV23Policy(
		appV23RESTAgentID("11"), memberID, store.AppV23RoleManager,
		memberEnrollment.Profile, store.AppV23ProfileStandard, 2, 0,
		memberRole.Revision, memberEnrollment.Revision, 4,
	))
	require.NoError(t, srv.checkDomainAccess(context.Background(), memberID, "owner.home", "write"))
	require.NoError(t, srv.checkDomainAccess(context.Background(), memberID, "owner.home", "modify"))

	require.NoError(t, badger.SetAccessGrant("owner.home", outsiderID, 1, 0, ownerID))
	require.NoError(t, srv.checkDomainAccess(context.Background(), outsiderID, "owner.home", "read"))
	require.Error(t, srv.checkDomainAccess(context.Background(), outsiderID, "owner.home", "write"))
}

func TestAppV23RESTPendingAndNoDomainRecallFailClosed(t *testing.T) {
	srv, badger, memberID, _, outsiderID := setupAppV23RESTAccess(t)

	_, seeAll := srv.resolveVisibleAgents(memberID)
	require.True(t, seeAll, "app-v23 must defer no-domain filtering to per-record domain authorization")

	enrollment, err := badger.GetAppV23Enrollment(outsiderID)
	require.NoError(t, err)
	role, err := badger.GetAppV23Role(outsiderID)
	require.NoError(t, err)
	require.NoError(t, badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
		AgentID: outsiderID, ApprovedBy: appV23RESTAgentID("11"), RootGeneration: 1,
		Profile: enrollment.Profile, HomeDomain: enrollment.HomeDomain,
		Clearance: enrollment.Clearance, Capabilities: enrollment.Capabilities,
		Active: false, UpdatedHeight: 5,
	}, role.Role, enrollment.Revision, role.Revision))

	require.Error(t, srv.checkDomainAccess(context.Background(), outsiderID, "outsider.home", "read"))
	_, seeAll = srv.resolveVisibleAgents(outsiderID)
	require.False(t, seeAll)
}

func TestAppV23FirstUseDomainReadDenialIsMachineTypedAcrossRecallRoutes(t *testing.T) {
	srv, _, memberID, _, _ := setupAppV23RESTAccess(t)
	const domain = "clean-first-use-probe"
	require.Error(t, srv.checkDomainAccess(
		context.Background(), memberID, domain, "read",
	), "the domain must not become readable before its first committed write claims it")

	for _, tc := range []struct {
		name    string
		path    string
		body    string
		handler http.HandlerFunc
	}{
		{
			name: "semantic", path: "/v1/memory/query",
			body:    `{"embedding":[0.1,0.2,0.3],"domain_tag":"clean-first-use-probe","top_k":5}`,
			handler: srv.handleQueryMemory,
		},
		{
			name: "text", path: "/v1/memory/search",
			body:    `{"query":"first use","domain_tag":"clean-first-use-probe","top_k":5}`,
			handler: srv.handleSearchMemory,
		},
		{
			name: "hybrid", path: "/v1/memory/hybrid",
			body:    `{"query":"first use","embedding":[0.1,0.2,0.3],"domain_tag":"clean-first-use-probe","top_k":5}`,
			handler: srv.handleHybridSearchMemory,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewBufferString(tc.body))
			req = req.WithContext(middleware.WithAgentID(req.Context(), memberID))
			recorder := httptest.NewRecorder()
			tc.handler.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
			require.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
			var problem map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
			require.Equal(t, domainReadDeniedProblemType, problem["type"])
			require.Equal(t, float64(http.StatusForbidden), problem["status"])
		})
	}

	t.Run("policy backend failure stays retryable", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		writeDomainReadAccessError(recorder,
			errors.New("app-v23 access-control state is unavailable"))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		var problem map[string]any
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
		require.NotEqual(t, domainReadDeniedProblemType, problem["type"],
			"a policy backend outage must never be suppressed as harmless first use")
	})
}

func TestAppV23RESTManagerCanLinkWithinGroupWithoutLegacyGrant(t *testing.T) {
	srv, badger, managerID, ownerID, _ := setupAppV23RESTAccess(t)
	enrollment, err := badger.GetAppV23Enrollment(managerID)
	require.NoError(t, err)
	role, err := badger.GetAppV23Role(managerID)
	require.NoError(t, err)
	require.NoError(t, badger.SetAppV23Policy(
		appV23RESTAgentID("11"), managerID, store.AppV23RoleManager,
		enrollment.Profile, store.AppV23ProfileStandard, enrollment.Clearance, 0,
		role.Revision, enrollment.Revision, 4,
	))
	agents := srv.agentStore.(*mockAgentStore)
	agents.agents[managerID] = &store.AgentEntry{
		AgentID: managerID, Name: "manager", Status: "active",
	}
	memStore := srv.store.(*rbacMockMemoryStore)
	seedMemory(t, memStore, "source", ownerID, "owner.home", "source")
	seedMemory(t, memStore, "target", managerID, "member.home", "target")
	require.NoError(t, badger.SetMemoryClassification("source", 0))
	require.NoError(t, badger.SetMemoryClassification("target", 0))

	body, err := json.Marshal(map[string]any{
		"source_id": "source", "target_id": "target", "link_type": "related",
	})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/memory/link", bytes.NewReader(body))
	req = req.WithContext(middleware.WithAgentID(req.Context(), managerID))
	rec := httptest.NewRecorder()
	srv.handleLinkMemories(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestAppV23RESTMemberGroupReadCannotMutateTaskStatus(t *testing.T) {
	srv, _, memberID, ownerID, _ := setupAppV23RESTAccess(t)
	agents := srv.agentStore.(*mockAgentStore)
	agents.agents[memberID] = &store.AgentEntry{
		AgentID: memberID, Name: "member", Status: "active",
	}
	memStore := srv.store.(*rbacMockMemoryStore)
	seedMemory(t, memStore, "group-task", ownerID, "owner.home", "group task")
	memStore.memories["group-task"].MemoryType = memory.TypeTask
	memStore.memories["group-task"].TaskStatus = memory.TaskStatusPlanned

	req := httptest.NewRequest(
		http.MethodPut, "/v1/memory/group-task/task-status",
		bytes.NewBufferString(`{"task_status":"in_progress"}`),
	)
	route := chi.NewRouteContext()
	route.URLParams.Add("memory_id", "group-task")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
	req = req.WithContext(middleware.WithAgentID(req.Context(), memberID))
	rec := httptest.NewRecorder()
	srv.handleUpdateTaskStatus(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
}

func TestAppV23RESTRejectsStaleAdminAndOldRootAfterRotation(t *testing.T) {
	srv, badger, adminID, _, _ := setupAppV23RESTAccess(t)
	enrollment, err := badger.GetAppV23Enrollment(adminID)
	require.NoError(t, err)
	role, err := badger.GetAppV23Role(adminID)
	require.NoError(t, err)
	rootID := appV23RESTAgentID("11")
	require.NoError(t, badger.SetAppV23Policy(
		rootID, adminID, store.AppV23RoleAdmin,
		enrollment.Profile, store.AppV23ProfileStandard, 4,
		store.AgentCapabilityReadAllDomains,
		role.Revision, enrollment.Revision, 4,
	))
	newRootID := appV23RESTAgentID("55")
	require.NoError(t, badger.RotateAppV23RootCredential(1, newRootID, 5))

	require.Error(t, srv.checkDomainAccess(
		context.Background(), adminID, "owner.home", "read",
	))
	require.Error(t, srv.checkDomainAccess(
		context.Background(), rootID, "owner.home", "read",
	))
	require.NoError(t, srv.checkDomainAccess(
		context.Background(), newRootID, "owner.home", "read",
	))
	_, seeAll := srv.resolveVisibleAgents(newRootID)
	require.True(t, seeAll, "current Root credential must resolve through the immutable policy principal")
	_, staleAdminSeeAll := srv.resolveVisibleAgents(adminID)
	require.False(t, staleAdminSeeAll)
	_, oldRootSeeAll := srv.resolveVisibleAgents(rootID)
	require.False(t, oldRootSeeAll)
	canRead, err := srv.hasMemoryReadAccess(
		"owner.home", newRootID, 4, time.Now(),
	)
	require.NoError(t, err)
	require.True(t, canRead)
	canFederate, clearance := srv.federationCallerCanRead(
		context.Background(), newRootID, "owner.home",
	)
	require.False(t, canFederate,
		"current Root controls federation locally but is not an ordinary federated recall agent")
	require.Zero(t, clearance)
	require.False(t, srv.callerIsOperatorOrAdmin(context.Background(), adminID))
	require.False(t, srv.callerIsOperatorOrAdmin(context.Background(), rootID))
	require.True(t, srv.callerIsOperatorOrAdmin(context.Background(), newRootID))
	for actorID, expected := range map[string]bool{
		adminID: false, rootID: false, newRootID: true,
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(middleware.WithAgentID(req.Context(), actorID))
		require.Equal(t, expected, srv.callerIsGlobalAdmin(req), actorID)
	}

	for _, actorID := range []string{adminID, rootID} {
		req := httptest.NewRequest(http.MethodGet, "/v1/memory/list", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		req = req.WithContext(middleware.WithAgentID(req.Context(), actorID))
		rec := httptest.NewRecorder()
		srv.appV23LocalAdminBoundary(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code, "actor %s: %s", actorID, rec.Body.String())
	}
}

func TestAppV23RootHandoverPreservesDomainAuthorityAndRecallsOldAndNewAuthorship(t *testing.T) {
	srv, memStore, badger, _ := newRBACTestServer(t)
	oldRootID := appV23RESTAgentID("11")
	memberID := appV23RESTAgentID("22")
	newRootID := appV23RESTAgentID("55")
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: oldRootID, Scope: "root-recall", AgentID: memberID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "member.home",
		Clearance: 2, Capabilities: 0, Height: 1, BootstrapDigest: "bootstrap",
	}))
	require.NoError(t, badger.RegisterDomain("root.archive", oldRootID, "", 2))
	require.NoError(t, badger.RotateAppV23RootCredential(1, newRootID, 3))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })

	seedMemory(t, memStore, "before-handover", oldRootID, "root.archive", "old root memory")
	seedMemory(t, memStore, "after-handover", newRootID, "root.archive", "new root memory")
	require.NoError(t, badger.SetMemoryClassification("before-handover", 0))
	require.NoError(t, badger.SetMemoryClassification("after-handover", 0))

	owner, err := badger.GetDomainOwner("root.archive")
	require.NoError(t, err)
	require.Equal(t, oldRootID, owner, "domain authority remains on the immutable Root principal")
	require.NoError(t, srv.checkDomainAccess(
		context.Background(), newRootID, "root.archive", "write",
	))
	require.Error(t, srv.checkDomainAccess(
		context.Background(), oldRootID, "root.archive", "read",
	))

	req := httptest.NewRequest(
		http.MethodPost, "/v1/memory/query",
		bytes.NewBufferString(`{"embedding":[0.1,0.2,0.3],"domain_tag":"root.archive","top_k":10}`),
	)
	req = req.WithContext(middleware.WithAgentID(req.Context(), newRootID))
	recorder := httptest.NewRecorder()
	srv.handleQueryMemory(recorder, req)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var response QueryMemoryResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	authors := make(map[string]string, len(response.Results))
	for _, result := range response.Results {
		authors[result.MemoryID] = result.SubmittingAgent
	}
	require.Equal(t, oldRootID, authors["before-handover"])
	require.Equal(t, newRootID, authors["after-handover"])
}

func TestAppV23RecallOmitsUnreadableClassificationAcrossAllBroadModes(t *testing.T) {
	srv, badger, memberID, ownerID, _ := setupAppV23RESTAccess(t)
	memStore := srv.store.(*rbacMockMemoryStore)
	seedMemory(t, memStore, "corrupt-classification", ownerID, "owner.home", "needle classified memory")
	require.NoError(t, badger.SetMemoryClassification("corrupt-classification", 0xff))

	for _, tc := range []struct {
		name    string
		path    string
		body    string
		handler http.HandlerFunc
	}{
		{
			name: "semantic", path: "/v1/memory/query",
			body:    `{"embedding":[0.1,0.2,0.3],"domain_tag":"owner.home","top_k":10}`,
			handler: srv.handleQueryMemory,
		},
		{
			name: "text", path: "/v1/memory/search",
			body:    `{"query":"needle","domain_tag":"owner.home","top_k":10}`,
			handler: srv.handleSearchMemory,
		},
		{
			name: "hybrid", path: "/v1/memory/hybrid",
			body:    `{"query":"needle","embedding":[0.1,0.2,0.3],"domain_tag":"owner.home","top_k":10}`,
			handler: srv.handleHybridSearchMemory,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewBufferString(tc.body))
			req = req.WithContext(middleware.WithAgentID(req.Context(), memberID))
			recorder := httptest.NewRecorder()
			tc.handler.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), `"results":[]`)
			require.NotContains(t, recorder.Body.String(), "needle classified memory")
		})
	}
}
