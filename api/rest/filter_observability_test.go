package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// v6.6.2 item 1: silent-filter observability
//
// When the RBAC submitting_agents filter or the classification+multi-org
// per-record filter hides data from the caller, the response must surface
// the fact - via `X-SAGE-Filter-Applied` response header and a `filtered`
// field in the JSON body - so clients can distinguish "empty domain" from
// "access-limited result".
// ---------------------------------------------------------------------------

// TestFilterObservability_List_FilterApplied exercises the /v1/memory/list
// endpoint with a caller whose resolveVisibleAgents returns seeAll=false and
// no grant on the queried domain. The RBAC submitting_agents filter fires;
// response must carry the header and envelope with total_before_filter/visible.
func TestFilterObservability_List_FilterApplied(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	req, callerID := signedRequest(t, http.MethodGet, "/v1/memory/list?domain=shared.secret&limit=50", nil)

	// Caller is NOT registered on-chain - resolveVisibleAgents falls through to
	// SQLite fallback. The SQLite mock has no record either, so allowedAgents =
	// [callerID], seeAll=false. Keep caller unregistered so the list path's
	// grant-aware override cannot flip seeAll to true.

	// Seed owner + domain + grants so the grant-aware overrides don't trigger.
	ownerID := "0000000000000000000000000000000000000000000000000000000000000001"
	require.NoError(t, bs.RegisterAgent(ownerID, "owner", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterDomain("shared.secret", ownerID, "", 1))
	require.NoError(t, bs.SetAccessGrant("shared.secret", ownerID, 2, 0, ownerID))

	// Three memories in the domain, only one submitted by the caller.
	seedMemory(t, memStore, "m-own", callerID, "shared.secret", "mine")
	seedMemory(t, memStore, "m-hidden-1", ownerID, "shared.secret", "not mine")
	seedMemory(t, memStore, "m-hidden-2", ownerID, "shared.secret", "also not mine")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Equal(t, filterBySubmittingAgts, rr.Header().Get(filterHeader),
		"header must announce that the submitting_agents filter was applied")

	var resp struct {
		Memories []any        `json:"memories"`
		Total    int          `json:"total"`
		Filtered *FilterInfo  `json:"filtered"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	require.NotNil(t, resp.Filtered, "envelope must include filtered block when filter applies")
	assert.Equal(t, []string{filterBySubmittingAgts}, resp.Filtered.By)
	require.NotNil(t, resp.Filtered.TotalBeforeFilter)
	require.NotNil(t, resp.Filtered.Visible)
	assert.Equal(t, 3, *resp.Filtered.TotalBeforeFilter, "total_before_filter must count all memories in domain")
	assert.Equal(t, 1, *resp.Filtered.Visible, "visible must count only caller-submitted memories")
	assert.Equal(t, 1, resp.Total, "legacy total field must match filtered visible count")
}

// TestFilterObservability_List_NoFilter verifies that when the caller has
// see-all visibility (wildcard), no header and no filtered envelope are emitted.
func TestFilterObservability_List_NoFilter(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	req, callerID := signedRequest(t, http.MethodGet, "/v1/memory/list?domain=shared.secret&limit=50", nil)

	// Caller has visible_agents="*" - resolveVisibleAgents returns seeAll=true.
	require.NoError(t, bs.RegisterAgent(callerID, "caller", "member", "", "test", "", 1))
	require.NoError(t, bs.SetAgentPermission(callerID, 1, "", "*", "", ""))

	seedMemory(t, memStore, "m1", callerID, "shared.secret", "content")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Empty(t, rr.Header().Get(filterHeader), "no filter means no header")

	var resp struct {
		Filtered *FilterInfo `json:"filtered"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Nil(t, resp.Filtered, "no filter means no envelope")
}

// TestFilterObservability_Query_SubmittingAgentsFilter verifies /v1/memory/query
// surfaces the submitting_agents filter when it applies.
// The filter only fires when req.DomainTag is empty (a domain-scoped query
// passes checkDomainAccess which flips seeAll=true).
func TestFilterObservability_Query_SubmittingAgentsFilter(t *testing.T) {
	srv, memStore, _, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	// No DomainTag → checkDomainAccess is skipped → seeAll stays false.
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Caller intentionally unregistered - resolveVisibleAgents returns seeAll=false.

	// One of caller's memories and one of someone else's.
	otherID := "0000000000000000000000000000000000000000000000000000000000000002"
	seedMemory(t, memStore, "m-own", callerID, "anydomain", "mine")
	seedMemory(t, memStore, "m-other", otherID, "anydomain", "not mine")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Equal(t, filterBySubmittingAgts, rr.Header().Get(filterHeader))

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.Filtered)
	assert.Equal(t, []string{filterBySubmittingAgts}, resp.Filtered.By)
	// No total-before-filter on /query (topk-bounded, no unbounded total).
	assert.Nil(t, resp.Filtered.TotalBeforeFilter)
}

// TestFilterObservability_Query_ClassificationFilter verifies the per-record
// classification+multi-org filter surfaces in the envelope when it hides
// records - including the case where the submitting_agents filter does NOT
// apply (domain-scoped query that the caller is authorized to read at the
// domain level but not cleared for a specific memory's classification).
func TestFilterObservability_Query_ClassificationFilter(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "restricted.domain", TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Caller registered on-chain - passes checkDomainAccess (no DomainAccess
	// restrictions configured) so seeAll flips to true. No submitting_agents
	// filter. Classification filter still runs per-record.
	require.NoError(t, bs.RegisterAgent(callerID, "caller", "member", "", "test", "", 1))

	// Domain has a registered owner (required for classification filter to engage).
	ownerID := "0000000000000000000000000000000000000000000000000000000000000003"
	require.NoError(t, bs.RegisterAgent(ownerID, "owner", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterDomain("restricted.domain", ownerID, "", 1))

	// Set classification=2 (CONFIDENTIAL) on the owner's memory. Caller has no
	// multi-org access at that level, so the in-loop filter drops it.
	seedMemory(t, memStore, "m-classified", ownerID, "restricted.domain", "owner's classified fact")
	require.NoError(t, bs.SetMemoryClassification("m-classified", 2))

	// Caller's own memory passes through (rec.SubmittingAgent == queryAgentID).
	seedMemory(t, memStore, "m-own", callerID, "restricted.domain", "mine")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Equal(t, filterByClassification, rr.Header().Get(filterHeader))

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.Filtered)
	assert.Equal(t, []string{filterByClassification}, resp.Filtered.By)
	require.NotNil(t, resp.Filtered.HiddenCount)
	assert.Equal(t, 1, *resp.Filtered.HiddenCount, "one memory hidden by classification")
}

// ---------------------------------------------------------------------------
// v6.6.2 item 2: org-clearance-as-seeAll
//
// A TopSecret member of an org should bypass the submitting_agents filter
// without needing visible_agents="*" explicitly. Closes homogeneous-trust
// boilerplate for single-org deployments. Per-domain access control still
// applies - this only lifts the submitting_agents filter.
// ---------------------------------------------------------------------------

func TestOrgClearance_TopSecretBypassesSubmittingAgentsFilter(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	// Omit DomainTag so checkDomainAccess doesn't flip seeAll=true unrelated
	// to clearance - this isolates the new clearance path.
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Caller is a TopSecret member of org "acme".
	require.NoError(t, bs.RegisterAgent(callerID, "caller", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterOrg("acme", "Acme Inc", "", callerID, 1))
	require.NoError(t, bs.AddOrgMember("acme", callerID, uint8(tx.ClearanceTopSecret), "member", 1))

	otherID := "0000000000000000000000000000000000000000000000000000000000000002"
	seedMemory(t, memStore, "m-other", otherID, "anydomain", "not mine")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, resp.TotalCount, "TopSecret clearance must lift agent isolation - caller sees other agents' memories")
	assert.Nil(t, resp.Filtered, "no filter applied means no envelope")
	assert.Empty(t, rr.Header().Get(filterHeader), "no filter applied means no header")
}

func TestOrgClearance_InternalClearanceStillFiltered(t *testing.T) {
	// Negative control: sub-TopSecret clearance must NOT bypass the filter.
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	require.NoError(t, bs.RegisterAgent(callerID, "caller", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterOrg("acme", "Acme Inc", "", callerID, 1))
	// Internal clearance (1) - well below TopSecret (4).
	require.NoError(t, bs.AddOrgMember("acme", callerID, uint8(tx.ClearanceInternal), "member", 1))

	otherID := "0000000000000000000000000000000000000000000000000000000000000002"
	seedMemory(t, memStore, "m-other", otherID, "anydomain", "not mine")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.TotalCount, "internal clearance must NOT bypass filter")
	require.NotNil(t, resp.Filtered)
	assert.Contains(t, resp.Filtered.By, filterBySubmittingAgts)
}

// TestFilterObservability_Query_NoFilter verifies clean response when no
// filter runs - caller sees everything, no envelope, no header.
func TestFilterObservability_Query_NoFilter(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "open.domain", TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Caller registered with wildcard visibility → resolveVisibleAgents seeAll=true.
	require.NoError(t, bs.RegisterAgent(callerID, "caller", "member", "", "test", "", 1))
	require.NoError(t, bs.SetAgentPermission(callerID, 1, "", "*", "", ""))

	seedMemory(t, memStore, "m1", callerID, "open.domain", "content")

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Empty(t, rr.Header().Get(filterHeader))

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Nil(t, resp.Filtered)
}

// TestClassificationGate_PublicMemoryCrossesAgentBoundary is the v6.8.6
// regression for the silent classification default-bump that broke cross-
// agent visibility for any memory submitted without explicit classification.
//
// Pre-fix: processMemorySubmit and the REST submit handler both bumped the
// caller's Classification=0 to INTERNAL(1). Combined with the per-record
// gate at memory_handler.go:627, this filtered out every cross-agent read
// where the reader had no shared-org path to the writer — even with
// visible_agents="*" granted. Symptom: HTTP 200 + total_count=0 +
// `filtered:{by:["classification"]}`. (Surfaced by the LevelUp pipeline on
// 2026-05-05: 10 agents granted visible_agents="*", reader still got 0 mems
// on cross-agent calibration.* recall.)
//
// Post-fix: a memory written with classification=0 stays PUBLIC, the gate
// is skipped (`if memClass > 0` short-circuit), and the cross-agent reader
// gets the record without needing any org bootstrapping.
func TestClassificationGate_PublicMemoryCrossesAgentBoundary(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "calibration.ir_triage", TopK: 10})
	req, readerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	// Reader has visible_agents="*" but is NOT in any org — exactly the
	// bridge's "_register_and_grant_visibility" terminal state when org
	// add_member silently failed but the bridge logged success anyway.
	require.NoError(t, bs.RegisterAgent(readerID, "designer", "member", "", "test", "", 1))
	require.NoError(t, bs.SetAgentPermission(readerID, 4, "", "*", "", ""))

	// Writer is a different agent; their write auto-registers the domain
	// to themselves on first submission (mirrors processMemorySubmit's
	// auto-register path). Writer has no shared org with the reader.
	writerID := "0000000000000000000000000000000000000000000000000000000000000099"
	require.NoError(t, bs.RegisterAgent(writerID, "calibrator", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterDomain("calibration.ir_triage", writerID, "", 1))

	// Seed the writer's memory and pin classification=0 (PUBLIC) — this
	// is what v6.8.6 actually writes; pre-v6.8.6 ABCI would have written 1.
	seedMemory(t, memStore, "m-public", writerID, "calibration.ir_triage", "ir triage signals")
	require.NoError(t, bs.SetMemoryClassification("m-public", uint8(tx.ClearancePublic)))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Empty(t, rr.Header().Get(filterHeader),
		"PUBLIC memory must not engage the classification filter")

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Nil(t, resp.Filtered, "no filter envelope expected for cross-agent PUBLIC read")
	require.Len(t, resp.Results, 1, "writer's PUBLIC memory must be visible to a cross-agent reader with visible_agents='*'")
	assert.Equal(t, "m-public", resp.Results[0].MemoryID)
}

// TestClassificationGate_InternalMemoryStillGated locks in the other half
// of v6.8.6: when a writer explicitly classifies a memory as INTERNAL or
// higher, the per-record gate STILL fires for cross-agent reads without a
// shared-org path. The fix is "Public-by-default", not "classification
// disabled".
func TestClassificationGate_InternalMemoryStillGated(t *testing.T) {
	srv, memStore, bs, _ := newRBACTestServer(t)

	embedding := make([]float32, 8)
	for i := range embedding {
		embedding[i] = 0.1
	}
	body, _ := json.Marshal(QueryMemoryRequest{Embedding: embedding, DomainTag: "calibration.ir_triage", TopK: 10})
	req, readerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)

	require.NoError(t, bs.RegisterAgent(readerID, "designer", "member", "", "test", "", 1))
	require.NoError(t, bs.SetAgentPermission(readerID, 4, "", "*", "", ""))

	writerID := "0000000000000000000000000000000000000000000000000000000000000099"
	require.NoError(t, bs.RegisterAgent(writerID, "calibrator", "member", "", "test", "", 1))
	require.NoError(t, bs.RegisterDomain("calibration.ir_triage", writerID, "", 1))

	seedMemory(t, memStore, "m-internal", writerID, "calibration.ir_triage", "internal-only signals")
	require.NoError(t, bs.SetMemoryClassification("m-internal", uint8(tx.ClearanceInternal)))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	assert.Equal(t, filterByClassification, rr.Header().Get(filterHeader),
		"INTERNAL memory must still engage the classification filter for cross-agent readers without a shared org")

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotNil(t, resp.Filtered)
	assert.Equal(t, []string{filterByClassification}, resp.Filtered.By)
	require.NotNil(t, resp.Filtered.HiddenCount)
	assert.Equal(t, 1, *resp.Filtered.HiddenCount)
}
