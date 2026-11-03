package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// index_status distinguishes a genuinely-empty domain from one whose rows the
// active vector space cannot return. It is emitted ONLY when it can be proven to
// leak nothing: the completeness verdict is computed with no per-record
// authorization, so it is disclosed only to a caller PROVEN to see every record
// in the domain (callerHasProvenFullDomainVisibility). Every other caller, a
// non-default status universe, and a missing active space all collapse to
// "unavailable". These tests exercise the REAL app-v23 authorization predicate
// (not a mock) and drive only the store completeness probe through the mock.

// fullVisibilityCaller approves a TopSecret (clearance 4) agent that owns `home`,
// so hasMemoryReadAccess at the top of the lattice proves it can read every
// classification in that domain.
func fullVisibilityCaller(t *testing.T, srv *Server, badger *store.BadgerStore, home string) string {
	t.Helper()
	id := appV23RESTAgentID("77")
	rootID := appV23RESTAgentID("11")
	require.NoError(t, badger.RegisterAgent(id, "topsecret", store.AppV23RoleMember, "", "test", "", 4))
	require.NoError(t, badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
		AgentID: id, ApprovedBy: rootID, RootGeneration: 1,
		Profile: store.AppV23ProfileStandard, HomeDomain: home,
		Clearance: 4, Capabilities: 0, Active: true, UpdatedHeight: 2,
	}, store.AppV23RoleMember, 0, 0))
	revision, err := mockStoreOf(srv).MemoryProjectionRevision(context.Background())
	require.NoError(t, err)
	badger.PublishCanonicalMemoryProjectionAuditAt(
		true, false, false, revision, badger.CanonicalMemoryProjectionRevision(),
		mockStoreOf(srv).VaultGeneration(),
	)
	return id
}

func queryEmptyRecall(t *testing.T, srv *Server, agentID, domain, statusFilter string) (*httptest.ResponseRecorder, QueryMemoryResponse) {
	t.Helper()
	body := fmt.Sprintf(`{"embedding":[0.1,0.2,0.3],"domain_tag":%q,"top_k":10,"status_filter":%q}`, domain, statusFilter)
	req := httptest.NewRequest(http.MethodPost, "/v1/memory/query", bytes.NewBufferString(body))
	req = req.WithContext(middleware.WithAgentID(req.Context(), agentID))
	rec := httptest.NewRecorder()
	srv.handleQueryMemory(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return rec, resp
}

func mockStoreOf(srv *Server) *mockMemoryStore {
	return srv.store.(*rbacMockMemoryStore).mockMemoryStore
}

// P1 (Dhillon): seeAll is NOT proof of full record visibility. The verdict is
// emitted only to a caller proven to read every classification in the domain; a
// caller that can read the domain at low classification but is not fully cleared
// (here the clearance-2 owner) must get "unavailable", never a verdict derived
// from rows it may not see.
func TestRecallIndexStatusFullVisibilityPredicateGate(t *testing.T) {
	srv, badger, _, ownerID, _ := setupAppV23RESTAccess(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
	now := time.Now()

	// Predicate itself: the fully-cleared owner is proven, the clearance-2 owner is not.
	require.True(t, srv.callerHasProvenFullDomainVisibility(tsID, "fv.home", now),
		"a TopSecret agent that owns the domain is proven to see every record")
	require.False(t, srv.callerHasProvenFullDomainVisibility(ownerID, "owner.home", now),
		"a clearance-2 caller cannot be proven to dominate every classification")

	// End to end: the clearance-2 caller gets unavailable even though the mock probe
	// would say incomplete — the probe result must never reach a non-fully-visible caller.
	mockStoreOf(srv).outside = outsideSpaceProbe{hasOutside: true, established: true}
	_, resp := queryEmptyRecall(t, srv, ownerID, "owner.home", "committed")
	require.Empty(t, resp.Results)
	assert.Equal(t, "unavailable", resp.IndexStatus)
}

// For a proven fully-visible caller the store completeness verdict maps to the
// index_status enum; a bounded/errored probe degrades to unavailable.
func TestRecallIndexStatusEnumForFullVisibilityCaller(t *testing.T) {
	cases := []struct {
		name   string
		probe  outsideSpaceProbe
		expect string
	}{
		{"out-of-space row -> incomplete", outsideSpaceProbe{hasOutside: true, established: true}, "incomplete"},
		{"fully reachable -> complete", outsideSpaceProbe{hasOutside: false, established: true}, "complete"},
		{"domain too large -> unavailable", outsideSpaceProbe{hasOutside: false, established: false}, "unavailable"},
		{"store error -> unavailable", outsideSpaceProbe{err: assert.AnError}, "unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, badger, _, _, _ := setupAppV23RESTAccess(t)
			srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
			tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
			ms := mockStoreOf(srv)
			ms.outside = tc.probe
			var sawSpace string
			ms.outside.sawSpace = &sawSpace
			_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
			require.Empty(t, resp.Results)
			assert.Equal(t, tc.expect, resp.IndexStatus)
			if tc.probe.err == nil {
				assert.NotEmpty(t, sawSpace, "the probe must run with an exact active vector space")
			}
		})
	}
}

// P1#3 (Dhillon): the probe hard-codes committed/challenged, so the verdict may
// only describe the exact default recall contract. A non-"committed" status
// universe would let a committed/challenged out-of-space row falsely settle the
// verdict for a query over different rows -> unavailable.
func TestRecallIndexStatusNonDefaultStatusUniverseIsUnavailable(t *testing.T) {
	for _, statusFilter := range []string{"", "deprecated", "proposed", "validated"} {
		t.Run("status="+statusFilter, func(t *testing.T) {
			srv, badger, _, _, _ := setupAppV23RESTAccess(t)
			srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
			tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
			mockStoreOf(srv).outside = outsideSpaceProbe{hasOutside: true, established: true}
			_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", statusFilter)
			assert.Equal(t, "unavailable", resp.IndexStatus,
				"only the committed contract the probe scans may be described")
		})
	}
}

// P2#5 (Dhillon): without an exact active vector space, recall does not partition
// by space at all, so the probe's space test is meaningless -> unavailable, even
// for a fully-visible caller with out-of-space rows.
func TestRecallIndexStatusEmptyActiveSpaceIsUnavailable(t *testing.T) {
	srv, badger, _, _, _ := setupAppV23RESTAccess(t)
	srv.embedder = nil // activeEmbeddingProvider() == ""
	tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
	mockStoreOf(srv).outside = outsideSpaceProbe{hasOutside: true, established: true}
	_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
	assert.Equal(t, "unavailable", resp.IndexStatus)
}

func TestRecallIndexStatusRequiresExactCanonicalProjectionHealth(t *testing.T) {
	srv, badger, _, _, _ := setupAppV23RESTAccess(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
	require.True(t, srv.callerHasProvenFullDomainVisibility(tsID, "fv.home", time.Now()))

	// A complete inventory that found even one unsafe projection means raw SQL
	// rows are not the caller-visible universe. The probe must not run, regardless
	// of whether its answer would otherwise be complete or incomplete.
	badger.PublishCanonicalMemoryProjectionAudit(true, false, true)
	_, exact := srv.exactCanonicalMemoryProjectionSource(context.Background())
	require.False(t, exact)

	render := func(probe outsideSpaceProbe) []byte {
		mockStoreOf(srv).outside = probe
		rec, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
		require.Equal(t, "unavailable", resp.IndexStatus)
		return append([]byte(nil), rec.Body.Bytes()...)
	}
	base := render(outsideSpaceProbe{hasOutside: false, established: true})
	assert.Equal(t, string(base), string(render(outsideSpaceProbe{hasOutside: true, established: true})),
		"a quarantined projection must not become a complete/incomplete oracle")
}

func TestRecallIndexStatusRejectsStaleOrChangingProjectionAudit(t *testing.T) {
	t.Run("SQL revision changed after audit", func(t *testing.T) {
		srv, badger, _, _, _ := setupAppV23RESTAccess(t)
		srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
		tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
		ms := mockStoreOf(srv)
		ms.projectionRevision++ // models SQL-before-Badger publication after the exact audit
		var sawSpace string
		ms.outside = outsideSpaceProbe{hasOutside: true, established: true, sawSpace: &sawSpace}
		_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
		assert.Equal(t, "unavailable", resp.IndexStatus)
		assert.Empty(t, sawSpace, "a stale source token must stop the raw SQL probe")
	})

	t.Run("embedding space changed after semantic query", func(t *testing.T) {
		srv, badger, _, _, _ := setupAppV23RESTAccess(t)
		srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
		tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
		ms := mockStoreOf(srv)
		ms.querySimilarAfter = func() { ms.spaceRevision++ }
		ms.outside = outsideSpaceProbe{hasOutside: false, established: true}
		_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
		assert.Equal(t, "unavailable", resp.IndexStatus,
			"empty lookup and completeness must share one vector-space generation")
	})

	t.Run("vault generation changed after semantic query", func(t *testing.T) {
		srv, badger, _, _, _ := setupAppV23RESTAccess(t)
		srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
		tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
		ms := mockStoreOf(srv)
		ms.querySimilarAfter = func() { ms.vaultGeneration++ }
		ms.outside = outsideSpaceProbe{hasOutside: false, established: true}
		_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
		assert.Equal(t, "unavailable", resp.IndexStatus,
			"empty lookup and completeness must share one vault generation")
	})

	t.Run("source changed during probe", func(t *testing.T) {
		srv, badger, _, _, _ := setupAppV23RESTAccess(t)
		srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
		tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
		ms := mockStoreOf(srv)
		ms.outside = outsideSpaceProbe{
			hasOutside: true, established: true,
			after: func() { ms.spaceRevision++ },
		}
		_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
		assert.Equal(t, "unavailable", resp.IndexStatus,
			"a verdict over a source that changed during the scan must be discarded")
	})
}

func TestRecallIndexStatusRemoteOnlyFederatedEmptyIsUnavailable(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2}, name: "ollama"}
	srv.SetFederation(&fakeFederation{})

	body, err := json.Marshal(QueryMemoryRequest{
		Embedding:         []float32{0.1, 0.2},
		EmbeddingProvider: srv.activeEmbeddingProvider(),
		DomainTag:         "remote.only",
		StatusFilter:      "committed",
		Federated:         true,
	})
	require.NoError(t, err)
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)
	require.NoError(t, badger.RegisterAgent(callerID, "operator", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(
		callerID, 1, `[{"domain":"remote.only","read":false}]`, "*", "", "",
	))
	srv.SetNodeOperatorID(callerID) // federation authority; local policy remains denied

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Results)
	assert.Equal(t, "unavailable", resp.IndexStatus)
}

func TestRecallIndexStatusExactQueryUniverseGate(t *testing.T) {
	base := QueryMemoryRequest{StatusFilter: "committed"}
	require.True(t, recallIndexStatusHasExactQueryUniverse(base))

	cases := map[string]QueryMemoryRequest{
		"non-default status":  {StatusFilter: "proposed"},
		"text arm":            {StatusFilter: "committed", Query: "needle"},
		"provider":            {StatusFilter: "committed", Provider: "other"},
		"confidence":          {StatusFilter: "committed", MinConfidence: 0.5},
		"tags":                {StatusFilter: "committed", Tags: []string{"one"}},
		"cursor":              {StatusFilter: "committed", Cursor: "next"},
		"all federation":      {StatusFilter: "committed", Federated: true},
		"selected federation": {StatusFilter: "committed", FederateChains: []string{"peer"}},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			assert.False(t, recallIndexStatusHasExactQueryUniverse(req))
		})
	}
}

// P1#2 (Dhillon): a hidden row must not change the response. The store probe is
// never reached for a caller who could encounter per-record denial, so the
// completeness verdict cannot perturb such a caller's response: it is byte-for-
// byte identical no matter what the probe would have returned. (The probe itself
// applies no per-record authorization — proven in the store tests — so it cannot
// count a hidden row even on the path it does run.)
func TestRecallIndexStatusByteIdenticalForNonVisibleCaller(t *testing.T) {
	render := func(probe outsideSpaceProbe) []byte {
		srv, _, _, ownerID, _ := setupAppV23RESTAccess(t)
		srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
		mockStoreOf(srv).outside = probe // would flip incomplete/complete/unavailable if it leaked
		rec, _ := queryEmptyRecall(t, srv, ownerID, "owner.home", "committed")
		return rec.Body.Bytes()
	}
	base := render(outsideSpaceProbe{hasOutside: false, established: true})
	for _, probe := range []outsideSpaceProbe{
		{hasOutside: true, established: true},
		{hasOutside: false, established: false},
		{err: assert.AnError},
	} {
		assert.Equal(t, string(base), string(render(probe)),
			"the completeness verdict must never perturb a non-fully-visible caller's response")
	}
}

// index_status describes an EMPTY recall; it must never appear next to results.
func TestRecallIndexStatusOnlyOnEmpty(t *testing.T) {
	srv, badger, _, _, _ := setupAppV23RESTAccess(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	tsID := fullVisibilityCaller(t, srv, badger, "fv.home")
	mockStoreOf(srv).outside = outsideSpaceProbe{hasOutside: true, established: true}
	seedMemory(t, srv.store.(*rbacMockMemoryStore), "seeded", tsID, "fv.home", "a real memory")
	_, resp := queryEmptyRecall(t, srv, tsID, "fv.home", "committed")
	require.NotEmpty(t, resp.Results, "a populated recall")
	assert.Empty(t, resp.IndexStatus, "index_status is emitted only on an empty result, never alongside hits")
}
