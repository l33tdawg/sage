package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestLocalAgentLookupReturnsBoundedShortNameCandidates(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	agents, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "agents.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = agents.Close() })
	srv.agentStore = agents

	require.NoError(t, agents.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "voice-bridge", Name: "MYNAH (SAGE Voice Bridge Agent)",
		RegisteredName: "SAGE Voice Bridge", Provider: "mynah-appliance", Role: "member", Status: "active",
	}))
	require.NoError(t, agents.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: "mac-app", Name: "MYNAH (Mac App)",
		RegisteredName: "MYNAH Mac", Provider: "mynah-app", Role: "member", Status: "active",
	}))

	req, _ := signedRequest(t, http.MethodGet, "/v1/agents/lookup?name=mynah&limit=20", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response struct {
		Agents []*store.AgentEntry `json:"agents"`
		Total  int                 `json:"total"`
	}
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&response))
	assert.Equal(t, 2, response.Total)
	require.Len(t, response.Agents, 2)
	assert.Equal(t, "mac-app", response.Agents[0].AgentID)
	assert.Equal(t, "voice-bridge", response.Agents[1].AgentID)
}

func TestPostgresAgentBackendImplementsRESTNameLookup(t *testing.T) {
	var backend store.AgentStore = (*store.PostgresStore)(nil)
	_, ok := backend.(agentNameFinder)
	assert.True(t, ok, "amid's Postgres backend must implement bounded /v1/agents/lookup")
	_, paged := backend.(agentNamePageFinder)
	assert.True(t, paged, "amid's Postgres backend must page lookup candidates before canonical filtering")
}

func TestAppV22AgentReadEndpointsOverlayConsensusCapabilities(t *testing.T) {
	srv, _, badger, agents := newRBACTestServer(t)
	pub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	targetID := auth.PublicKeyToAgentID(pub)
	mask := store.AgentCapabilityReadAllDomains |
		store.AgentCapabilityDenySharedDomainWrite |
		store.AgentCapabilityDenyForeignDomainWrite
	require.NoError(t, badger.RegisterAgentWithCapabilities(
		targetID, "Mynah", "member", "", "voice-bridge", "", 1, mask,
	))
	agents.agents[targetID] = &store.AgentEntry{
		AgentID: targetID,
		Name:    "Mynah",
		Role:    "member",
		Status:  "active",
		// Deliberately stale pre-v22 SQL projection.
		Capabilities: 0,
	}

	detailReq, _ := signedRequest(t, http.MethodGet, "/v1/agent/"+targetID, nil)
	detailRR := httptest.NewRecorder()
	srv.Router().ServeHTTP(detailRR, detailReq)
	require.Equal(t, http.StatusOK, detailRR.Code, detailRR.Body.String())
	var detail store.AgentEntry
	require.NoError(t, json.NewDecoder(detailRR.Body).Decode(&detail))
	assert.Equal(t, mask, detail.Capabilities)

	listReq, _ := signedRequest(t, http.MethodGet, "/v1/agents", nil)
	listRR := httptest.NewRecorder()
	srv.Router().ServeHTTP(listRR, listReq)
	require.Equal(t, http.StatusOK, listRR.Code, listRR.Body.String())
	var listed struct {
		Agents []*store.AgentEntry `json:"agents"`
		Total  int                 `json:"total"`
	}
	require.NoError(t, json.NewDecoder(listRR.Body).Decode(&listed))
	require.Equal(t, 1, listed.Total)
	require.Len(t, listed.Agents, 1)
	assert.Equal(t, mask, listed.Agents[0].Capabilities)
}

func TestAppV22ReadAllDomainsIsReadOnlyAndClearanceBounded(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })

	const callerID = "appv22-companion"
	require.NoError(t, badger.RegisterAgent(callerID, "Mynah", "member", "", "voice-bridge", "", 1))
	require.NoError(t, badger.SetAgentPermissionWithCapabilities(
		callerID,
		1,
		`[{"domain":"private.only","read":false,"write":false}]`,
		"",
		"",
		"",
		store.AgentCapabilityReadAllDomains,
	))

	require.NoError(t, srv.checkDomainAccess(context.Background(), callerID, "foreign.research", "read"))
	require.Error(t, srv.checkDomainAccess(context.Background(), callerID, "foreign.research", "write"))

	allowed, err := srv.hasMemoryReadAccess("foreign.research", callerID, 1, time.Now())
	require.NoError(t, err)
	assert.True(t, allowed)
	allowed, err = srv.hasMemoryReadAccess("foreign.research", callerID, 2, time.Now())
	require.NoError(t, err)
	assert.False(t, allowed, "read-all must not raise the agent's on-chain clearance")

	_, seeAll := srv.resolveVisibleAgents(callerID)
	assert.True(t, seeAll)

	allowed, clearance := srv.federationCallerCanRead(context.Background(), callerID, "foreign.research")
	assert.True(t, allowed)
	assert.Equal(t, 1, clearance)
	assert.Equal(t, []string{"foreign.research"}, srv.federationVisibleRemoteScopes(
		context.Background(), callerID, "foreign.research",
	))
}

func TestAppV22MalformedCapabilityMaskFailsClosedOffConsensus(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })

	const callerID = "malformed-capability-agent"
	require.NoError(t, badger.RegisterAgent(callerID, "Malformed", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(
		callerID,
		1,
		`[{"domain":"private.only","read":false,"write":false}]`,
		"*",
		"",
		"",
	))

	agent, err := badger.GetRegisteredAgent(callerID)
	require.NoError(t, err)
	agent.Capabilities = store.AgentCapabilityReadAllDomains | store.AgentCapabilities(1<<31)
	rawAgent, err := json.Marshal(agent)
	require.NoError(t, err)
	require.NoError(t, badger.SetRawForTest([]byte("agent:"+callerID), rawAgent))

	require.Error(t, srv.checkDomainAccess(context.Background(), callerID, "foreign.research", "read"))
	allowed, err := srv.hasMemoryReadAccess("foreign.research", callerID, 1, time.Now())
	require.Error(t, err)
	assert.False(t, allowed)

	visibleAgents, seeAll := srv.resolveVisibleAgents(callerID)
	assert.False(t, seeAll)
	assert.Equal(t, []string{callerID}, visibleAgents)

	allowed, clearance := srv.federationCallerCanRead(context.Background(), callerID, "foreign.research")
	assert.False(t, allowed)
	assert.Zero(t, clearance)
	assert.Nil(t, srv.federationVisibleRemoteScopes(context.Background(), callerID, "foreign.research"))
	assert.False(t, srv.callerMayUseFederatedPipe(callerID))
}

func TestAppV22FederatedInboxRemainsEnabledUnlessExplicitlyDenied(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })

	const callerID = "appv22-companion"
	require.NoError(t, badger.RegisterAgent(callerID, "Mynah", "member", "", "voice-bridge", "", 1))

	companionMask := store.AgentCapabilityReadAllDomains |
		store.AgentCapabilityDenySharedDomainWrite |
		store.AgentCapabilityDenyDomainClaim |
		store.AgentCapabilityDenyForeignDomainWrite
	require.NoError(t, badger.SetAgentPermissionWithCapabilities(
		callerID, 1, "[]", "", "", "", companionMask,
	))

	target := &federation.RemotePipeTarget{
		ChainID: "remote-chain",
		AgentID: "remote-agent",
		Domains: []federation.PipeContactDomain{{Domain: "shared.research"}},
	}
	assert.True(t, srv.callerCanReachFederatedPipeTarget(context.Background(), callerID, target),
		"the companion preset must support cross-network inbox messaging")
	assert.True(t, srv.callerMayUseFederatedPipe(callerID))
	nodeTarget := *target
	nodeTarget.AuthorizationMode = federation.NodeMessageAuthorizationMode
	nodeTarget.Domains = nil
	assert.True(t, srv.callerCanReachFederatedPipeTarget(context.Background(), callerID, &nodeTarget),
		"trusted-node messaging needs no memory domain intersection")

	require.NoError(t, badger.SetAgentPermissionWithCapabilities(
		callerID, 1, "[]", "", "", "",
		companionMask|store.AgentCapabilityDenyFederatedPipe,
	))
	assert.False(t, srv.callerCanReachFederatedPipeTarget(context.Background(), callerID, target),
		"operators retain a separate kill switch for federated inbox messaging")
	assert.False(t, srv.callerMayUseFederatedPipe(callerID))
	assert.False(t, srv.callerCanReachFederatedPipeTarget(context.Background(), callerID, &nodeTarget),
		"domain-free messaging must preserve the same hard messaging block")
	assert.False(t, srv.callerMayUseFederatedPipe("unknown-agent"),
		"post-v22 recipient discovery must fail closed without consensus agent state")
}

func TestAppV22CapabilitiesStayDormantBeforeActivation(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	srv.SetPostV22ForNextTxAccessor(func() bool { return false })

	const callerID = "future-companion"
	require.NoError(t, badger.RegisterAgent(callerID, "Future Mynah", "member", "", "voice-bridge", "", 1))
	require.NoError(t, badger.SetAgentPermissionWithCapabilities(
		callerID,
		1,
		`[{"domain":"private.only","read":true}]`,
		"",
		"",
		"",
		store.AgentCapabilityReadAllDomains|store.AgentCapabilityDenyFederatedPipe,
	))

	require.Error(t, srv.checkDomainAccess(context.Background(), callerID, "foreign.research", "read"))
	_, seeAll := srv.resolveVisibleAgents(callerID)
	assert.False(t, seeAll)
	allowed, _ := srv.federationCallerCanRead(context.Background(), callerID, "foreign.research")
	assert.False(t, allowed)
	assert.True(t, srv.callerMayUseFederatedPipe(callerID),
		"the future deny bit must remain dormant until app-v22 activates")
}

func TestAppV22OrgAndDepartmentEscalationRESTRequiresGlobalAdmin(t *testing.T) {
	cometMock := permTestCometMock(t, 0, "accepted")
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	badger, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })

	orgAdminPub, orgAdminPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	orgAdminID := auth.PublicKeyToAgentID(orgAdminPub)
	globalAdminPub, globalAdminPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	globalAdminID := auth.PublicKeyToAgentID(globalAdminPub)
	targetPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	targetID := auth.PublicKeyToAgentID(targetPub)

	require.NoError(t, badger.RegisterAgent(orgAdminID, "org-admin", "member", "", "", "", 1))
	require.NoError(t, badger.RegisterAgent(globalAdminID, "global-admin", "admin", "", "", "", 1))
	require.NoError(t, badger.RegisterAgent(targetID, "target", "member", "", "", "", 1))
	require.NoError(t, badger.RegisterOrg("local-org", "Local Org", "", orgAdminID, 1))
	require.NoError(t, badger.AddOrgMember("local-org", orgAdminID, 4, "admin", 1))
	require.NoError(t, badger.AddOrgMember("local-org", globalAdminID, 4, "member", 1))
	require.NoError(t, badger.SetFederation(
		"federation-id", "local-org", "local-org",
		[]string{"*"}, 4, 0, false, "proposed",
	))

	tests := []struct {
		name        string
		method      string
		path        string
		body        []byte
		successCode int
	}{
		{
			name:        "register organization",
			method:      http.MethodPost,
			path:        "/v1/org/register",
			body:        []byte(`{"name":"global-admin-created"}`),
			successCode: http.StatusCreated,
		},
		{
			name:        "add organization member",
			method:      http.MethodPost,
			path:        "/v1/org/local-org/member",
			body:        []byte(`{"agent_id":"` + targetID + `","clearance":4}`),
			successCode: http.StatusCreated,
		},
		{
			name:        "set organization clearance",
			method:      http.MethodPost,
			path:        "/v1/org/local-org/clearance",
			body:        []byte(`{"agent_id":"` + targetID + `","clearance":4}`),
			successCode: http.StatusOK,
		},
		{
			name:        "remove organization member",
			method:      http.MethodDelete,
			path:        "/v1/org/local-org/member/" + targetID,
			successCode: http.StatusOK,
		},
		{
			name:        "register department",
			method:      http.MethodPost,
			path:        "/v1/org/local-org/dept",
			body:        []byte(`{"name":"voice-ops"}`),
			successCode: http.StatusCreated,
		},
		{
			name:        "add department member",
			method:      http.MethodPost,
			path:        "/v1/org/local-org/dept/voice-ops/member",
			body:        []byte(`{"agent_id":"` + targetID + `","clearance":4}`),
			successCode: http.StatusCreated,
		},
		{
			name:        "remove department member",
			method:      http.MethodDelete,
			path:        "/v1/org/local-org/dept/voice-ops/member/" + targetID,
			successCode: http.StatusOK,
		},
		{
			name:        "propose federation",
			method:      http.MethodPost,
			path:        "/v1/federation/propose",
			body:        []byte(`{"target_org_id":"remote-org","allowed_domains":["voice"],"max_clearance":2}`),
			successCode: http.StatusCreated,
		},
		{
			name:        "approve federation",
			method:      http.MethodPost,
			path:        "/v1/federation/federation-id/approve",
			successCode: http.StatusOK,
		},
		{
			name:        "revoke federation",
			method:      http.MethodPost,
			path:        "/v1/federation/federation-id/revoke",
			body:        []byte(`{"reason":"rotation"}`),
			successCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			denied := signedRequestAs(t, orgAdminPriv, orgAdminID, tc.method, tc.path, tc.body)
			deniedRecorder := httptest.NewRecorder()
			srv.Router().ServeHTTP(deniedRecorder, denied)
			require.Equal(t, http.StatusForbidden, deniedRecorder.Code, deniedRecorder.Body.String())
			require.Contains(t, deniedRecorder.Body.String(), "global admin")

			allowed := signedRequestAs(t, globalAdminPriv, globalAdminID, tc.method, tc.path, tc.body)
			allowedRecorder := httptest.NewRecorder()
			srv.Router().ServeHTTP(allowedRecorder, allowed)
			require.Equal(t, tc.successCode, allowedRecorder.Code, allowedRecorder.Body.String())
		})
	}
}

func TestFederationProposeRESTPreservesExplicitPublicAndSelectsExactOrg(t *testing.T) {
	var captured []string
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = append(captured, r.URL.Query().Get("tx"))
		writeCometCommitFixture(t, w, r, 0, "", 0, "", 1)
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	badger, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	adminID := auth.PublicKeyToAgentID(pub)
	require.NoError(t, badger.RegisterAgent(adminID, "global-admin", "admin", "", "", "", 4))
	require.NoError(t, badger.AddOrgMember("org-a", adminID, 4, "member", 1))
	require.NoError(t, badger.AddOrgMember("org-b", adminID, 4, "member", 2))

	explicit := signedRequestAs(
		t, priv, adminID, http.MethodPost, "/v1/federation/propose",
		[]byte(`{"proposer_org_id":"org-a","target_org_id":"remote-org","allowed_domains":["voice"],"max_clearance":0}`),
	)
	explicitRecorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(explicitRecorder, explicit)
	require.Equal(t, http.StatusCreated, explicitRecorder.Code, explicitRecorder.Body.String())
	require.Len(t, captured, 1)
	parsed, err := tx.DecodeTx(decodeHexTxParam(t, captured[0]))
	require.NoError(t, err)
	require.NotNil(t, parsed.FederationPropose)
	assert.Equal(t, "org-a", parsed.FederationPropose.ProposerOrgID)
	assert.Equal(t, tx.ClearancePublic, parsed.FederationPropose.MaxClearance)

	unauthorized := signedRequestAs(
		t, priv, adminID, http.MethodPost, "/v1/federation/propose",
		[]byte(`{"proposer_org_id":"org-c","target_org_id":"remote-org","allowed_domains":["voice"]}`),
	)
	unauthorizedRecorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(unauthorizedRecorder, unauthorized)
	require.Equal(t, http.StatusForbidden, unauthorizedRecorder.Code, unauthorizedRecorder.Body.String())
	require.Len(t, captured, 1, "unauthorized proposer org must not broadcast")

	omitted := signedRequestAs(
		t, priv, adminID, http.MethodPost, "/v1/federation/propose",
		[]byte(`{"target_org_id":"remote-org","allowed_domains":["voice"]}`),
	)
	omittedRecorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(omittedRecorder, omitted)
	require.Equal(t, http.StatusCreated, omittedRecorder.Code, omittedRecorder.Body.String())
	require.Len(t, captured, 2)
	parsed, err = tx.DecodeTx(decodeHexTxParam(t, captured[1]))
	require.NoError(t, err)
	require.NotNil(t, parsed.FederationPropose)
	assert.Equal(t, "org-b", parsed.FederationPropose.ProposerOrgID)
	assert.Equal(t, tx.ClearanceConfidential, parsed.FederationPropose.MaxClearance)

	for _, invalid := range []int{-1, 5} {
		req := signedRequestAs(
			t, priv, adminID, http.MethodPost, "/v1/federation/propose",
			[]byte(`{"target_org_id":"remote-org","max_clearance":`+fmt.Sprint(invalid)+`}`),
		)
		rr := httptest.NewRecorder()
		srv.Router().ServeHTTP(rr, req)
		require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	}
	require.Len(t, captured, 2, "invalid clearance must not broadcast")
}

func TestFederationApprovalRESTUsesStoredExactMembership(t *testing.T) {
	var captured []string
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = append(captured, r.URL.Query().Get("tx"))
		writeCometCommitFixture(t, w, r, 0, "", 0, "", 1)
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	badger, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })

	adminPub, adminPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	adminID := auth.PublicKeyToAgentID(adminPub)
	outsidePub, outsidePriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	outsideID := auth.PublicKeyToAgentID(outsidePub)
	require.NoError(t, badger.RegisterAgent(adminID, "global-admin", "admin", "", "", "", 4))
	require.NoError(t, badger.RegisterAgent(outsideID, "outside-global-admin", "admin", "", "", "", 4))
	require.NoError(t, badger.AddOrgMember("org-a", adminID, 4, "member", 1))
	require.NoError(t, badger.AddOrgMember("org-b", adminID, 4, "member", 2))
	require.NoError(t, badger.AddOrgMember("org-c", outsideID, 4, "member", 3))
	require.NoError(t, badger.SetFederation(
		"fed-exact", "org-a", "org-a",
		[]string{"*"}, 4, 0, false, "proposed",
	))

	approve := signedRequestAs(
		t, adminPriv, adminID, http.MethodPost,
		"/v1/federation/fed-exact/approve", nil,
	)
	approveRecorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(approveRecorder, approve)
	require.Equal(t, http.StatusOK, approveRecorder.Code, approveRecorder.Body.String())
	parsed, err := tx.DecodeTx(decodeHexTxParam(t, captured[0]))
	require.NoError(t, err)
	require.NotNil(t, parsed.FederationApprove)
	assert.Equal(t, "org-a", parsed.FederationApprove.ApproverOrgID,
		"stored target membership must override the legacy primary org-b slot")

	revoke := signedRequestAs(
		t, adminPriv, adminID, http.MethodPost,
		"/v1/federation/fed-exact/revoke", []byte(`{"reason":"rotation"}`),
	)
	revokeRecorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(revokeRecorder, revoke)
	require.Equal(t, http.StatusOK, revokeRecorder.Code, revokeRecorder.Body.String())
	parsed, err = tx.DecodeTx(decodeHexTxParam(t, captured[1]))
	require.NoError(t, err)
	require.NotNil(t, parsed.FederationRevoke)
	assert.Equal(t, "org-a", parsed.FederationRevoke.RevokerOrgID)

	for _, path := range []string{
		"/v1/federation/fed-exact/approve",
		"/v1/federation/fed-exact/revoke",
	} {
		req := signedRequestAs(t, outsidePriv, outsideID, http.MethodPost, path, nil)
		rr := httptest.NewRecorder()
		srv.Router().ServeHTTP(rr, req)
		require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	}
	require.Len(t, captured, 2, "unrelated global admin must not broadcast")
}
