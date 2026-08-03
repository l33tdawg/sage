package rest

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

func addAppV23LookupAgent(
	t *testing.T,
	fixture appV23RESTRouteFixture,
	name, displayName, registeredName, provider string,
	clearance uint8,
	enrolled bool,
) string {
	t.Helper()
	pub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	id := auth.PublicKeyToAgentID(pub)
	require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
		id, displayName, store.AppV23RoleMember, "", provider, "", int64(clearance), 0,
	))
	if enrolled {
		require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(
			store.AppV23LocalEnrollment{
				AgentID: id, ApprovedBy: fixture.ids["current-root"],
				RootGeneration: 2, Profile: store.AppV23ProfileStandard,
				HomeDomain: name + ".home", Clearance: clearance,
				Capabilities: 0, Active: true, UpdatedHeight: 5,
			},
			store.AppV23RoleMember, 0, 0,
		))
	}
	fixture.agents.agents[id] = &store.AgentEntry{
		AgentID: id, Name: displayName, RegisteredName: registeredName,
		Provider: provider, Role: store.AppV23RoleMember,
		Status: "active", Clearance: int(clearance),
	}
	return id
}

func decodeAgentLookup(t *testing.T, rec *httptest.ResponseRecorder) struct {
	Agents []struct {
		AgentID        string `json:"agent_id"`
		Name           string `json:"name"`
		RegisteredName string `json:"registered_name"`
		Provider       string `json:"provider"`
		Status         string `json:"status"`
		MatchKind      string `json:"match_kind"`
	} `json:"agents"`
	Total int `json:"total"`
} {
	t.Helper()
	var response struct {
		Agents []struct {
			AgentID        string `json:"agent_id"`
			Name           string `json:"name"`
			RegisteredName string `json:"registered_name"`
			Provider       string `json:"provider"`
			Status         string `json:"status"`
			MatchKind      string `json:"match_kind"`
		} `json:"agents"`
		Total int `json:"total"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	return response
}

func TestAppV23AgentLookupIsCallerScopedAndReturnsCanonicalMatches(t *testing.T) {
	fixture := newAppV23RESTRouteFixture(t)
	voiceID := addAppV23LookupAgent(
		t, fixture, "voice", "MYNAH (SAGE Voice Bridge Agent)",
		"SAGE Voice Bridge", "mynah-appliance", 4, true,
	)
	_ = addAppV23LookupAgent(
		t, fixture, "pending-mynah", "MYNAH pending", "pending-mynah",
		"mynah-pending", 1, false,
	)

	// Local pipeline messaging is not a memory disclosure. A lower-clearance
	// active Member may discover a higher-clearance active local recipient;
	// the receiver still acts only under its own authority.
	req := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet,
		"/v1/agents/lookup?name=MYNAH&limit=20", nil, false,
	)
	rec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	response := decodeAgentLookup(t, rec)
	require.Equal(t, 1, response.Total)
	require.Len(t, response.Agents, 1)
	require.Equal(t, voiceID, response.Agents[0].AgentID)
	require.Equal(t, "substring", response.Agents[0].MatchKind)
	require.Equal(t, "active", response.Agents[0].Status)
	require.Equal(t, 1, fixture.agents.lookupCandidateCalls,
		"one logical lookup must use one bounded store query")
	// Fill the single candidate batch with consensus-pending substring rows.
	// The exact active recipient ranks first and must still be returned rather
	// than turning a bounded-but-complete exact lookup into a 503.
	for i := 0; i < agentLookupCandidateBatchSize-1; i++ {
		id := fmt.Sprintf("%064x", i+1000)
		fixture.agents.agents[id] = &store.AgentEntry{
			AgentID: id, Name: fmt.Sprintf("pending appliance %03d", i),
			RegisteredName: fmt.Sprintf("pending/appliance/%03d", i),
			Provider:       "mynah-appliance-pending", Status: "active",
		}
	}

	exactReq := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet,
		"/v1/agents/lookup?name=MYNAH-APPLIANCE&limit=20", nil, false,
	)
	exactRec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(exactRec, exactReq)
	require.Equal(t, http.StatusOK, exactRec.Code, exactRec.Body.String())
	exactResponse := decodeAgentLookup(t, exactRec)
	require.Len(t, exactResponse.Agents, 1)
	require.Equal(t, "exact", exactResponse.Agents[0].MatchKind)
	require.Equal(t, 2, fixture.agents.lookupCandidateCalls)

	idReq := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet,
		"/v1/agents/lookup?name="+voiceID+"&limit=20", nil, false,
	)
	idRec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(idRec, idReq)
	require.Equal(t, http.StatusOK, idRec.Code, idRec.Body.String())
	idResponse := decodeAgentLookup(t, idRec)
	require.Len(t, idResponse.Agents, 1)
	require.Equal(t, voiceID, idResponse.Agents[0].AgentID)
	require.Equal(t, "exact", idResponse.Agents[0].MatchKind)
	require.Equal(t, 3, fixture.agents.lookupCandidateCalls)

	// SQL discoverability cannot resurrect a consensus-inactive agent.
	inactiveReq := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet,
		"/v1/agents/lookup?name=inactive&limit=20", nil, false,
	)
	inactiveRec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(inactiveRec, inactiveReq)
	require.Equal(t, http.StatusOK, inactiveRec.Code, inactiveRec.Body.String())
	require.Zero(t, decodeAgentLookup(t, inactiveRec).Total)
	require.Equal(t, 4, fixture.agents.lookupCandidateCalls,
		"an ordinary negative lookup must finish after one bounded store query")

	for i := 0; i < agentLookupCandidateBatchSize; i++ {
		id := fmt.Sprintf("%064x", i+10000)
		fixture.agents.agents[id] = &store.AgentEntry{
			AgentID: id, Name: fmt.Sprintf("wide pending %03d", i),
			RegisteredName: fmt.Sprintf("wide/pending/%03d", i),
			Provider:       "wide-pending", Status: "active",
		}
	}
	wideReq := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet,
		"/v1/agents/lookup?name=wide&limit=20", nil, false,
	)
	wideRec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(wideRec, wideReq)
	require.Equal(t, http.StatusServiceUnavailable, wideRec.Code, wideRec.Body.String())
	require.Equal(t, 5, fixture.agents.lookupCandidateCalls,
		"even an exhausted broad query must issue only one candidate query")
}

func TestAppV23AgentRosterRequiresSignatureAndFiltersCanonicalEnrollment(t *testing.T) {
	fixture := newAppV23RESTRouteFixture(t)
	activeID := addAppV23LookupAgent(
		t, fixture, "active-roster", "Active roster agent", "active-roster",
		"test", 1, true,
	)
	_ = addAppV23LookupAgent(
		t, fixture, "pending-roster", "Pending roster agent", "pending-roster",
		"test", 1, false,
	)

	unsigned := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(
		unsigned,
		httptest.NewRequest(http.MethodGet, "/v1/agents", nil),
	)
	require.Equal(t, http.StatusUnauthorized, unsigned.Code, unsigned.Body.String())

	req := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet, "/v1/agents", nil, false,
	)
	rec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response struct {
		Agents []*store.AgentEntry `json:"agents"`
		Total  int                 `json:"total"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	require.NotZero(t, response.Total)
	for _, agent := range response.Agents {
		require.NotNil(t, agent)
		require.NotEqual(t, "pending-roster", agent.RegisteredName)
	}
	require.Contains(t, func() []string {
		ids := make([]string, 0, len(response.Agents))
		for _, agent := range response.Agents {
			ids = append(ids, agent.AgentID)
		}
		return ids
	}(), activeID)
}

func TestAppV23AgentDirectoryCapsDatabaseAndCanonicalWork(t *testing.T) {
	fixture := newAppV23RESTRouteFixture(t)
	for i := 0; i < 110; i++ {
		addAppV23LookupAgent(
			t, fixture, fmt.Sprintf("directory-%03d", i),
			fmt.Sprintf("Directory agent %03d", i),
			fmt.Sprintf("directory/%03d", i), "test", 1, true,
		)
	}
	req := appV23SignedRESTRouteRequest(
		t, fixture, "member", http.MethodGet, "/v1/agents/directory", nil, false,
	)
	rec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response struct {
		Agents    []agentDirectoryEntry `json:"agents"`
		Total     int                   `json:"total"`
		Truncated bool                  `json:"truncated"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&response))
	require.LessOrEqual(t, len(response.Agents), 100)
	require.Equal(t, len(response.Agents), response.Total)
	require.NotEmpty(t, response.Agents)
	require.True(t, response.Truncated)
	require.Equal(t, 1, fixture.agents.directoryCalls)
	require.Equal(t, 101, fixture.agents.directoryLimit,
		"one sentinel row must bound both SQL work and canonical enrollment checks")
}

func TestAppV23AgentLookupRejectsUnknownInactiveAndRootCallers(t *testing.T) {
	fixture := newAppV23RESTRouteFixture(t)
	for _, actor := range []struct {
		name  string
		local bool
	}{
		{name: "inactive"},
		{name: "historical-root", local: true},
		{name: "current-root", local: true},
	} {
		t.Run(actor.name, func(t *testing.T) {
			req := appV23SignedRESTRouteRequest(
				t, fixture, actor.name, http.MethodGet,
				"/v1/agents/lookup?name=member&limit=20", nil, actor.local,
			)
			rec := httptest.NewRecorder()
			fixture.server.Router().ServeHTTP(rec, req)
			require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
		})
	}

	pub, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	unknownID := auth.PublicKeyToAgentID(pub)
	req := signedRequestAs(
		t, key, unknownID, http.MethodGet,
		"/v1/agents/lookup?name=member&limit=20", nil,
	)
	rec := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
}
