package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

type linksReadResponse struct {
	Links []struct {
		SourceID string `json:"source_id"`
		TargetID string `json:"target_id"`
		LinkType string `json:"link_type"`
	} `json:"links"`
}

// TestGetLinksAmongGatesOnReadAccess is the load-bearing security test: a link
// must never disclose the existence of a memory the caller cannot read. The
// handler filters the requested IDs down to the caller-readable subset BEFORE
// GetLinksAmong (which requires both endpoints in its input), so a link to an
// unreadable memory can never surface.
func TestGetLinksAmongGatesOnReadAccess(t *testing.T) {
	srv, memStore, badger, agents := newRBACTestServer(t)

	// The store holds an A→B link. It may only surface when BOTH endpoints are
	// requested AND readable. The hook captures the IDs the handler actually
	// passed, so we can assert the unreadable one was filtered before the call.
	seeded := memory.MemoryLink{SourceID: "mem-a", TargetID: "mem-b", LinkType: "supersedes"}
	var passed []string
	memStore.getLinksAmongHook = func(ids []string) ([]memory.MemoryLink, error) {
		passed = append([]string(nil), ids...)
		set := map[string]bool{}
		for _, id := range ids {
			set[id] = true
		}
		if set[seeded.SourceID] && set[seeded.TargetID] {
			return []memory.MemoryLink{seeded}, nil
		}
		return nil, nil
	}

	body := []byte(`{"memory_ids":["mem-a","mem-b"]}`)
	callerPub, callerPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	callerID := auth.PublicKeyToAgentID(callerPub)
	req := signedRequestAs(t, callerPriv, callerID, http.MethodPost, "/v1/memory/links", body)
	agents.agents[callerID] = &store.AgentEntry{AgentID: callerID, Name: "reader", Role: "member", Status: "active"}
	require.NoError(t, badger.RegisterAgent(callerID, "reader", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, "", "*", "", ""))

	const ownerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, badger.RegisterAgent(ownerID, "owner", "member", "", "test", "", 1))
	require.NoError(t, badger.RegisterDomain("source.domain", ownerID, "", 1))
	require.NoError(t, badger.RegisterDomain("secret.domain", ownerID, "", 1))
	require.NoError(t, badger.SetMemoryClassification("mem-a", 1))
	require.NoError(t, badger.SetMemoryClassification("mem-b", 1))
	seedMemory(t, memStore, "mem-a", ownerID, "source.domain", "readable")
	seedMemory(t, memStore, "mem-b", ownerID, "secret.domain", "unreadable")

	// Phase 1: caller can read source.domain but has NO grant on secret.domain.
	require.NoError(t, badger.SetAccessGrant("source.domain", callerID, 1, 0, ownerID))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Equal(t, []string{"mem-a"}, passed,
		"unreadable mem-b must be filtered out BEFORE GetLinksAmong")
	var denied linksReadResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &denied))
	assert.Empty(t, denied.Links, "a link to an unreadable memory must not be disclosed")

	// Phase 2: grant read on secret.domain — now both readable, the link surfaces.
	require.NoError(t, badger.SetAccessGrant("secret.domain", callerID, 1, 0, ownerID))
	passed = nil
	// Reorder the ids so the signed payload differs (avoids replay detection); the
	// handler and GetLinksAmong are order-independent.
	body2 := []byte(`{"memory_ids":["mem-b","mem-a"]}`)
	req2 := signedRequestAs(t, callerPriv, callerID, http.MethodPost, "/v1/memory/links", body2)

	rr2 := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr2, req2)
	require.Equal(t, http.StatusOK, rr2.Code, rr2.Body.String())
	require.ElementsMatch(t, []string{"mem-a", "mem-b"}, passed,
		"both readable memories must reach GetLinksAmong")
	var allowed linksReadResponse
	require.NoError(t, json.Unmarshal(rr2.Body.Bytes(), &allowed))
	require.Len(t, allowed.Links, 1)
	assert.Equal(t, "mem-a", allowed.Links[0].SourceID)
	assert.Equal(t, "mem-b", allowed.Links[0].TargetID)
	assert.Equal(t, "supersedes", allowed.Links[0].LinkType)
}

func TestGetLinksAmongRequiresActiveAgent(t *testing.T) {
	srv, _, badger, agents := newRBACTestServer(t)
	body := []byte(`{"memory_ids":["mem-a"]}`)
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/links", body)
	agents.agents[callerID] = &store.AgentEntry{AgentID: callerID, Name: "inactive", Role: "member", Status: "inactive"}
	require.NoError(t, badger.RegisterAgent(callerID, "inactive", "member", "", "test", "", 1))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "Active agent")
}

func TestGetLinksAmongEmptyInputReturnsEmpty(t *testing.T) {
	srv, memStore, badger, agents := newRBACTestServer(t)
	called := false
	memStore.getLinksAmongHook = func(ids []string) ([]memory.MemoryLink, error) {
		called = true
		return nil, nil
	}
	body := []byte(`{"memory_ids":[]}`)
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/links", body)
	agents.agents[callerID] = &store.AgentEntry{AgentID: callerID, Name: "reader", Role: "member", Status: "active"}
	require.NoError(t, badger.RegisterAgent(callerID, "reader", "member", "", "test", "", 1))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.False(t, called, "empty input must short-circuit before the store call")
	var resp linksReadResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Links)
}

// TestGetLinksAmongEmptyInputStillRequiresIdentity proves the empty-input early
// return happens AFTER identity is established: an unregistered/unauthenticated
// caller must get 403, not a 200 that silently confirms the endpoint exists.
func TestGetLinksAmongEmptyInputStillRequiresIdentity(t *testing.T) {
	srv, _, _, _ := newRBACTestServer(t)
	body := []byte(`{"memory_ids":[]}`)
	// A signed request whose agent is never registered on-chain / activated.
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/links", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "Active agent")
}

// TestGetLinksAmongRejectsOversizeBatch proves an over-limit batch is rejected
// with a 4xx rather than silently truncated into an order-dependent partial graph.
func TestGetLinksAmongRejectsOversizeBatch(t *testing.T) {
	srv, memStore, badger, agents := newRBACTestServer(t)
	called := false
	memStore.getLinksAmongHook = func(ids []string) ([]memory.MemoryLink, error) {
		called = true
		return nil, nil
	}
	ids := make([]string, maxLinkReadBatch+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("mem-%d", i)
	}
	body, err := json.Marshal(map[string]any{"memory_ids": ids})
	require.NoError(t, err)
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/links", body)
	agents.agents[callerID] = &store.AgentEntry{AgentID: callerID, Name: "reader", Role: "member", Status: "active"}
	require.NoError(t, badger.RegisterAgent(callerID, "reader", "member", "", "test", "", 1))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "Too many memory IDs")
	assert.False(t, called, "an oversize batch must be rejected before any store read")
}

// TestGetLinksAmongSurfacesOperationalLookupError proves a backend outage during
// record lookup is surfaced as a retryable 5xx, never collapsed into a false
// 200 {"links":[]} that looks like a successful empty answer.
func TestGetLinksAmongSurfacesOperationalLookupError(t *testing.T) {
	srv, memStore, badger, agents := newRBACTestServer(t)
	memStore.getMemoryErr = errors.New("badger: backend temporarily unavailable")
	linksCalled := false
	memStore.getLinksAmongHook = func(ids []string) ([]memory.MemoryLink, error) {
		linksCalled = true
		return nil, nil
	}
	body := []byte(`{"memory_ids":["mem-a","mem-b"]}`)
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/links", body)
	agents.agents[callerID] = &store.AgentEntry{AgentID: callerID, Name: "reader", Role: "member", Status: "active"}
	require.NoError(t, badger.RegisterAgent(callerID, "reader", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, "", "*", "", ""))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	assert.False(t, linksCalled, "an operational lookup failure must abort before GetLinksAmong")
}

// TestLinkReadRecordDisclosableSurfacesPostV23OperationalFailure proves that
// post-v23, an operational disclosure failure (authorization state unavailable) is
// SURFACED as an error rather than silently skipped — the record must not be
// dropped as "not visible". Combined with the handler dropping resolveVisibleAgents
// on post-v23, this closes the masked-operational-failure path.
func TestLinkReadRecordDisclosableSurfacesPostV23OperationalFailure(t *testing.T) {
	srv, _, _, _ := newRBACTestServer(t)
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })
	// Missing policy infrastructure is a genuine request-wide authorization failure.
	srv.badgerStore = nil
	rec := &memory.MemoryRecord{MemoryID: "mem-x", SubmittingAgent: "someone-else", DomainTag: "d"}

	ok, err := srv.linkReadRecordDisclosable(context.Background(), "reader", false, rec, time.Now())
	require.Error(t, err, "an operational disclosure failure must be surfaced, not swallowed")
	require.False(t, ok)
	require.False(t, isUnsafeAppV23Projection(err),
		"an operational failure must not be misclassified as a skippable unsafe projection")
}

// TestGetLinksAmongSurfacesPreV23PolicyFailure is the pre-v23 fail-soft regression:
// an operational access-control policy failure (here a malformed on-chain domain
// access policy) must return a retryable 5xx, not a successful empty graph. It
// exercises the typed classifier (errAccessControlOperational) end to end.
func TestGetLinksAmongSurfacesPreV23PolicyFailure(t *testing.T) {
	srv, memStore, badger, agents := newRBACTestServer(t)
	body := []byte(`{"memory_ids":["mem-a"]}`)
	callerPub, callerPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	callerID := auth.PublicKeyToAgentID(callerPub)
	req := signedRequestAs(t, callerPriv, callerID, http.MethodPost, "/v1/memory/links", body)
	agents.agents[callerID] = &store.AgentEntry{AgentID: callerID, Name: "reader", Role: "member", Status: "active"}
	require.NoError(t, badger.RegisterAgent(callerID, "reader", "member", "", "test", "", 1))
	// A malformed domain-access policy is an operational failure, not a denial.
	require.NoError(t, badger.SetAgentPermission(callerID, 1, "not-valid-json", "*", "", ""))

	const ownerID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, badger.RegisterAgent(ownerID, "owner", "member", "", "test", "", 1))
	require.NoError(t, badger.RegisterDomain("src.domain", ownerID, "", 1))
	require.NoError(t, badger.SetMemoryClassification("mem-a", 1))
	seedMemory(t, memStore, "mem-a", ownerID, "src.domain", "content")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, rr.Body.String())
}

// TestResolveVisibleSubmittersSurfacesRegisteredAgentFailure injects an on-chain
// store failure and proves the pre-v23 submitter resolver returns an operational
// error rather than collapsing into self-only visibility (which would silently drop
// other-authored records for the link endpoint).
func TestResolveVisibleSubmittersSurfacesRegisteredAgentFailure(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	// Close the on-chain store so GetRegisteredAgent fails operationally.
	require.NoError(t, badger.CloseBadger())

	allowed, seeAll, err := srv.resolveVisibleSubmittersOrError("some-agent-id")
	require.Error(t, err, "an on-chain read failure must be surfaced")
	require.True(t, isAccessControlOperationalError(err), "the failure must be typed operational")
	require.False(t, seeAll)
	require.Nil(t, allowed)
}

// TestResolveVisibleSubmittersSurfacesDirectoryFailure injects an agent-directory
// (SQLite fallback) failure and proves the same surfacing.
func TestResolveVisibleSubmittersSurfacesDirectoryFailure(t *testing.T) {
	srv, _, badger, agents := newRBACTestServer(t)
	const agentID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	// Registered on-chain with EMPTY visible_agents, so the resolver consults the
	// agent directory (SQLite fallback) — which we make fail operationally.
	require.NoError(t, badger.RegisterAgent(agentID, "a", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(agentID, 1, "", "", "", ""))
	agents.getAgentErr = errors.New("agent directory temporarily unavailable")

	allowed, seeAll, err := srv.resolveVisibleSubmittersOrError(agentID)
	require.Error(t, err, "an agent-directory read failure must be surfaced")
	require.True(t, isAccessControlOperationalError(err), "the failure must be typed operational")
	require.False(t, seeAll)
	require.Nil(t, allowed)
}

func TestResolveVisibleSubmittersSurfacesMalformedVisibilityPolicy(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	const agentID = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	require.NoError(t, badger.RegisterAgent(agentID, "a", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(agentID, 1, "", "not-json", "", ""))

	allowed, seeAll, err := srv.resolveVisibleSubmittersOrError(agentID)
	require.Error(t, err, "a malformed visibility policy must be surfaced")
	require.True(t, isAccessControlOperationalError(err), "the failure must be typed operational")
	require.False(t, seeAll)
	require.Nil(t, allowed)
}

func TestTopSecretVisibilitySurfacesOrgStoreFailure(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	require.NoError(t, badger.CloseBadger())

	allowed, err := srv.agentHasTopSecretClearanceOrError("some-agent-id")
	require.Error(t, err, "an org-membership store failure must be surfaced")
	require.False(t, allowed)
}

// TestGetLinksAmongDoesNotLeakProtectedRecordExistence is the blocker-1 regression:
// a protected (unsafe/unpublished projection) record must be indistinguishable from
// an absent id. Before the fix, a disclosure error on the protected record surfaced
// as a 503 while an absent id was silently skipped — letting a caller tell protected
// records apart from nonexistent ones. Both must now be withheld identically (200,
// no link, no 503).
func TestGetLinksAmongDoesNotLeakProtectedRecordExistence(t *testing.T) {
	srv, memStore, badger, _, readerID, ownerID, _ := appV23DisclosureFixture(t)
	// Ensure the reader passes the handler's on-chain active-agent gate.
	require.NoError(t, badger.RegisterAgent(readerID, "reader", store.AppV23RoleMember, "", "test", "", 2))

	// A record present in the projection store but NEVER published to the canonical
	// badger projection → ValidateMemoryProjection fails as unpublished (an unsafe,
	// nondisclosable projection). Authored by a group-visible owner so it clears the
	// author-visibility filter and actually reaches the disclosure gate.
	const protectedID = "protected-unsafe-projection"
	memStore.memories[protectedID] = &memory.MemoryRecord{
		MemoryID:        protectedID,
		SubmittingAgent: ownerID,
		Content:         "protected",
		ContentHash:     memory.ComputeContentHash("protected"),
		MemoryType:      memory.TypeObservation,
		DomainTag:       "owner.home",
		Status:          memory.StatusCommitted,
	}
	const absentID = "absent-nowhere"

	body, err := json.Marshal(map[string]any{"memory_ids": []string{protectedID, absentID}})
	require.NoError(t, err)
	req := appV23DisclosureRequest(readerID, http.MethodPost, "/v1/memory/links", body)

	rr := httptest.NewRecorder()
	srv.handleGetLinksAmong(rr, req)
	require.Equal(t, http.StatusOK, rr.Code,
		"a protected record must not surface as a 503 that distinguishes it from an absent id: %s", rr.Body.String())
	var resp linksReadResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Empty(t, resp.Links, "neither a protected nor an absent id may produce a link")
}
