package web

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

// appV23SynapseFixture is the connectome endpoint standing on an app-v23-ACTIVE
// node. Every other synapse test in web/synapse_handler_test.go leaves
// AppV23ActiveFn nil, and appV23IsActive reports false for a nil func
// (web/appv23_access_handler.go:379-381), so those tests only ever exercise the
// LEGACY half of resolveAgentRBAC. This fixture exercises the app-v23 half that
// new nodes actually run.
type appV23SynapseFixture struct {
	handler   *DashboardHandler
	sql       *store.SQLiteStore
	badger    *store.BadgerStore
	rootID    string
	memberID  string
	strangerA string
	strangerB string
}

// newAppV23SynapseFixture bootstraps the app-v23 genesis the way
// web/appv23_projection_visibility_test.go:197 does, then enrolls two further
// agents that the member has no relationship with whatsoever: separate owned
// home domains, no grants, no shared domain between them.
func newAppV23SynapseFixture(t *testing.T) appV23SynapseFixture {
	t.Helper()

	sqlStore, err := store.NewSQLiteStore(
		context.Background(), filepath.Join(t.TempDir(), "sage.db"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })

	badgerStore, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })

	newID := func() string {
		_, key, keyErr := ed25519.GenerateKey(nil)
		require.NoError(t, keyErr)
		return agentIDForKey(key)
	}
	fixture := appV23SynapseFixture{
		sql: sqlStore, badger: badgerStore,
		rootID: newID(), memberID: newID(),
		strangerA: newID(), strangerB: newID(),
	}

	// The member is a STANDARD/MEMBER enrollment with capability mask 0: it does
	// NOT hold AgentCapabilityReadAllDomains, so its read reach is exactly its
	// own home domain plus explicit grants. This is the most restricted active
	// app-v23 principal that still passes every resolveAgentRBAC precondition.
	require.NoError(t, badgerStore.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: fixture.rootID, Scope: "synapse-appv23-rbac-fixture",
		AgentID: fixture.memberID, Profile: store.AppV23ProfileStandard,
		HomeDomain: "member-home", Clearance: uint8(store.ClearanceInternal),
		Capabilities: 0, Height: 1, BootstrapDigest: "synapse-appv23-rbac-fixture",
	}))

	// The neuron half of the response reads the on-chain agent registry, which
	// is a separate record from the app-v23 enrollment. Approval requires the
	// registry row to exist first.
	for i, id := range []string{fixture.memberID, fixture.strangerA, fixture.strangerB} {
		require.NoError(t, badgerStore.RegisterAgent(
			id, id, store.AppV23RoleMember, "", "test", "", int64(i+1),
		))
	}

	for _, stranger := range []struct{ id, home string }{
		{fixture.strangerA, "stranger-a-home"},
		{fixture.strangerB, "stranger-b-home"},
	} {
		require.NoError(t, badgerStore.ApproveAppV23LocalAgent(
			store.AppV23LocalEnrollment{
				AgentID: stranger.id, ApprovedBy: fixture.rootID, RootGeneration: 1,
				Profile: store.AppV23ProfileStandard, HomeDomain: stranger.home,
				Clearance: uint8(store.ClearanceInternal), Capabilities: 0,
				Active: true, UpdatedHeight: 2,
			},
			store.AppV23RoleMember, 0, 0,
		))
	}

	handler := NewDashboardHandler(sqlStore, "test")
	handler.BadgerStore = badgerStore
	handler.AppV23ActiveFn = func() bool { return true }
	fixture.handler = handler
	return fixture
}

// TestHandleSynapsesAppV23MemberCurrentlySeesAllNeuronsAndEdges is a
// CHARACTERISATION test. It records what the CEREBRUM connectome returns TODAY
// for a restricted app-v23 member; it does NOT assert what the endpoint ought to
// return. Read every assertion below as "this is what happens", never as "this
// is correct".
//
// The behaviour it pins:
//
//	web/handler.go:396-431 resolveAgentRBAC, once the app-v23 fork is active,
//	returns (nil, true) — seeAll — for ANY caller holding a valid enrollment and
//	role, regardless of role, profile, capability mask, or clearance. The comment
//	at web/handler.go:428-430 justifies dropping the submitter pre-filter with
//	"every record is still checked by agentDomainReadDecision".
//
//	web/synapse_handler.go never calls agentDomainReadDecision. It consumes
//	seeAll at line 63 and short-circuits BOTH the neuron filter (line 76) and the
//	edge guard (line 98) on it, so the compensating check the comment names does
//	not run on this route.
//
// The net effect asserted here: a restricted member with no grant, no shared
// domain, and no traffic of its own receives the complete neuron list and the
// complete edge set — including a directed edge between two agents whose home
// domains it is definitively DENIED read on (asserted as a precondition below,
// so the mismatch between the justification and the route is executable, not
// prose).
//
// This is BELIEVED TO BE A GAP. Whether app-v23 connectome seeAll is intentional
// (domain-derived visibility deliberately not applying to bus topology) or a
// missing guard is the MAINTAINER'S CALL, not this test's. If the disposition is
// "fix it", this test is expected to fail and should be rewritten to assert the
// filtered result. If the disposition is "intended", keep it as the explicit
// record of that decision.
func TestHandleSynapsesAppV23MemberCurrentlySeesAllNeuronsAndEdges(t *testing.T) {
	fixture := newAppV23SynapseFixture(t)
	h := fixture.handler

	// Precondition 1: the app-v23 branch is the one under test. Without this the
	// test would silently re-run the legacy branch, which is the exact blind spot
	// this file exists to close.
	require.True(t, h.appV23IsActive(), "precondition: app-v23 fork is active")

	// Precondition 2: the member really is domain-restricted. agentDomainReadDecision
	// — the check web/handler.go:428-430 names as the compensating control — returns
	// a DEFINITIVE DENY for this member on each stranger's home domain.
	for _, domain := range []string{"stranger-a-home", "stranger-b-home"} {
		allowedDomain, definitive := h.agentDomainReadDecision(
			context.Background(), fixture.memberID, domain,
		)
		require.True(t, definitive,
			"precondition: the domain decision is definitive for %s", domain)
		require.False(t, allowedDomain,
			"precondition: member is denied read on %s", domain)
	}
	// The member's own home domain is readable, proving the deny above is a real
	// restriction and not a broken fixture that denies everything.
	ownAllowed, ownDefinitive := h.agentDomainReadDecision(
		context.Background(), fixture.memberID, "member-home",
	)
	require.True(t, ownDefinitive)
	require.True(t, ownAllowed, "precondition: member reads its own home domain")

	// Bus traffic exclusively between the two strangers. The member is not an
	// endpoint of any edge, so under two-endpoint visibility it would see none.
	base := time.Now().UTC().Truncate(time.Second)
	insertSynapseMessage(t, fixture.sql, "v23-1", fixture.strangerA, fixture.strangerB, base)
	insertSynapseMessage(t, fixture.sql, "v23-2", fixture.strangerA, fixture.strangerB,
		base.Add(time.Minute))
	insertSynapseMessage(t, fixture.sql, "v23-3", fixture.strangerB, fixture.strangerA, base)

	req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/network/synapses", nil)
	req = req.WithContext(context.WithValue(
		req.Context(), verifiedDashboardAgentKey{}, fixture.memberID,
	))

	// Precondition 3: resolveAgentRBAC hands this member the unfiltered posture.
	// Asserted exactly: allowed is nil (not merely empty) and seeAll is true.
	allowed, seeAll := h.resolveAgentRBAC(req)
	require.Nil(t, allowed,
		"CURRENT: app-v23 resolveAgentRBAC returns no allowlist for a restricted member")
	require.True(t, seeAll,
		"CURRENT: app-v23 resolveAgentRBAC returns seeAll for a restricted member")

	rec := httptest.NewRecorder()
	h.handleSynapses(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	body := decodeSynapseBody(t, rec)

	// CURRENT BEHAVIOUR — neurons: the member is handed the whole registry,
	// including two agents it can neither read from nor address.
	neurons := make(map[string]bool, len(body.Neurons))
	for _, n := range body.Neurons {
		neurons[n.AgentID] = true
	}
	require.Len(t, body.Neurons, 4,
		"CURRENT: every registered agent is a neuron for a restricted app-v23 member")
	require.True(t, neurons[fixture.memberID])
	require.True(t, neurons[fixture.strangerA],
		"CURRENT: a domain-denied agent is still named as a neuron")
	require.True(t, neurons[fixture.strangerB],
		"CURRENT: a domain-denied agent is still named as a neuron")
	require.True(t, neurons[fixture.rootID],
		"CURRENT: the CEREBRUM root identity is disclosed to the member too")

	// CURRENT BEHAVIOUR — edges: both directed stranger-to-stranger synapses are
	// returned in full, weights included, to a caller that is not an endpoint of
	// either and is denied read on both endpoints' home domains.
	byEdge := make(map[string]store.PipeSynapse, len(body.Synapses))
	for _, e := range body.Synapses {
		byEdge[e.FromAgent+"|"+e.ToAgent] = e
	}
	require.Len(t, body.Synapses, 2,
		"CURRENT: the edge guard does not run for an app-v23 member; got %v", byEdge)

	forward, ok := byEdge[fixture.strangerA+"|"+fixture.strangerB]
	require.True(t, ok,
		"CURRENT: an edge between two agents invisible to the caller is returned")
	require.Equal(t, fixture.strangerA, forward.FromAgent)
	require.Equal(t, fixture.strangerB, forward.ToAgent)
	require.Equal(t, int64(2), forward.Count,
		"CURRENT: the full synaptic weight is disclosed, not a redacted one")

	reverse, ok := byEdge[fixture.strangerB+"|"+fixture.strangerA]
	require.True(t, ok,
		"CURRENT: the reverse edge between two invisible agents is returned too")
	require.Equal(t, fixture.strangerB, reverse.FromAgent)
	require.Equal(t, fixture.strangerA, reverse.ToAgent)
	require.Equal(t, int64(1), reverse.Count)

	// Nothing in the response involves the caller. Every byte it received
	// describes traffic between two agents it has no authorization to read.
	for edge := range byEdge {
		require.NotContains(t, edge, fixture.memberID,
			"the caller is an endpoint of no returned edge")
	}
}
