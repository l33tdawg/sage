package rest

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestTaintProjectionErrorsAreSkippableRecordLocalDenials(t *testing.T) {
	for _, sentinel := range []error{
		store.ErrMemoryProjectionTainted,
		store.ErrMemoryProjectionTaintPending,
	} {
		err := fmt.Errorf("%w: %w", store.ErrMemoryProjectionUnpublished, sentinel)
		require.True(t, isUnsafeAppV23Projection(err))
	}
}

func TestTaintClassificationFailureAbortsBroadMemoryList(t *testing.T) {
	for _, healthyPrefix := range []bool{false, true} {
		t.Run(fmt.Sprintf("healthy_prefix_%t", healthyPrefix), func(t *testing.T) {
			srv, canonical, readerID, ownerID, _ := setupAppV23RESTAccess(t)
			memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
			srv.store = memStore
			now := time.Now()
			broken := appV23PagingRecord("taint-read-error", ownerID, "owner.home",
				"non-disclosable test content", memory.TypeObservation, memory.StatusCommitted, now)
			if healthyPrefix {
				healthy := appV23PagingRecord("healthy-prefix", ownerID, "owner.home",
					"healthy test content", memory.TypeObservation, memory.StatusCommitted, now)
				memStore.listed = append(memStore.listed, healthy)
			}
			memStore.listed = append(memStore.listed, broken)
			for _, record := range memStore.listed {
				memStore.memories[record.MemoryID] = record
				require.NoError(t, canonical.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			}
			decisionHash := sha256.Sum256([]byte("malformed serving decision"))
			decisionID := fmt.Sprintf("%x", decisionHash)
			ref := tx.ParentRef{SourceChainID: "sage", MemoryID: broken.MemoryID}
			copy(ref.ContentHash[:], broken.ContentHash)
			_, err := canonical.ApplyTaintDecision("sage", decisionID, ref, sha256.Sum256([]byte("serving payload")))
			require.NoError(t, err)
			require.NoError(t, canonical.SetRawForTest(taint.DecisionKey(decisionID), []byte("invalid header")))
			_, disclosureErr := srv.evaluateAppV23RecordDisclosure(readerID, broken, now)
			require.ErrorIs(t, disclosureErr, store.ErrTaintStateMalformed)

			req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=50", nil)
			req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
			out := httptest.NewRecorder()
			srv.handleListMemoriesAuth(out, req)
			require.Equal(t, http.StatusServiceUnavailable, out.Code,
				"taint-read failure must not be a successful empty or partial list: %s", out.Body.String())
			require.False(t, isUnsafeAppV23Projection(disclosureErr))
			require.NotContains(t, out.Body.String(), broken.Content)
			require.NotContains(t, out.Body.String(), "healthy test content")
		})
	}
}

// TestTaintServingZeroDecisionDepthBoundaryBroadList uses direct store lineage
// plus a mock SQL list. It does not prove memory admission or signed execution.
func TestTaintServingZeroDecisionDepthBoundaryBroadList(t *testing.T) {
	for _, tc := range []struct {
		name   string
		nodes  int
		domain string
		denied bool
	}{
		{name: "256_nodes_reader_domain", nodes: 256, domain: "member.home"},
		{name: "257_nodes_reader_domain", nodes: 257, domain: "member.home"},
		{name: "257_nodes_denied_domain", nodes: 257, domain: "stranger.private", denied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, canonical, readerID, ownerID, outsiderID := setupAppV23RESTAccess(t)
			memStore := &appV23PagingStore{rbacMockMemoryStore: newRBACMockMemoryStore()}
			srv.store = memStore
			now := time.Now()
			normal := appV23PagingRecord("healthy-boundary-prefix", ownerID, "owner.home",
				"ordinary readable boundary content", memory.TypeObservation, memory.StatusCommitted, now)
			submitter := readerID
			if tc.denied {
				submitter = outsiderID
			}
			deep := appV23PagingRecord("deep-boundary-leaf", submitter, tc.domain,
				"deep lineage boundary content", memory.TypeObservation, memory.StatusCommitted, now)
			for _, record := range []*memory.MemoryRecord{normal, deep} {
				memStore.memories[record.MemoryID] = record
				require.NoError(t, canonical.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			}
			childID := deep.MemoryID
			for n := 1; n < tc.nodes; n++ {
				parentID := fmt.Sprintf("depth-parent-%03d", n)
				parentHash := sha256.Sum256([]byte(parentID))
				require.NoError(t, canonical.SetMemoryHash(parentID, parentHash[:], string(memory.StatusCommitted)))
				require.NoError(t, canonical.SetMemoryLineage(childID, []tx.ParentRef{{
					SourceChainID: "sage", MemoryID: parentID, ContentHash: parentHash,
				}}))
				childID = parentID
			}
			active, err := canonical.ActiveTaintDecisionCount()
			require.NoError(t, err)
			require.Zero(t, active)
			revision, err := canonical.TaintRevision()
			require.NoError(t, err)
			require.Zero(t, revision, "fresh store received no Apply or Clear")
			req := httptest.NewRequest(http.MethodGet, "/v1/memory/list?limit=50", nil)
			req = req.WithContext(middleware.WithAgentID(req.Context(), readerID))
			accessErr := srv.checkDomainAccess(req.Context(), readerID, tc.domain, "read")
			if tc.denied {
				require.Error(t, accessErr)
			} else {
				require.NoError(t, accessErr)
			}
			normalDisclosure, err := srv.evaluateAppV23RecordDisclosure(readerID, normal, now)
			require.NoError(t, err)
			require.True(t, normalDisclosure.Allowed)
			memStore.listed = []*memory.MemoryRecord{normal}
			control := httptest.NewRecorder()
			srv.handleListMemoriesAuth(control, req)
			require.Equal(t, http.StatusOK, control.Code)
			require.Contains(t, control.Body.String(), normal.MemoryID)

			verdict, classificationErr := canonical.ClassifyMemoryTaint(deep.MemoryID)
			memStore.listed = []*memory.MemoryRecord{normal, deep}
			out := httptest.NewRecorder()
			srv.handleListMemoriesAuth(out, req)
			t.Logf("BOUNDARY_OBSERVATION nodes=%d domain=%s denied=%t active=%d revision=%d visited=%d classify_error=%v HTTP=%d body=%s",
				tc.nodes, tc.domain, tc.denied, active, revision, verdict.Nodes, classificationErr, out.Code, out.Body.String())
			require.Equal(t, http.StatusOK, out.Code,
				"zero-decision lineage must not erase a separately readable normal row")
			require.Contains(t, out.Body.String(), normal.MemoryID)
			if tc.denied {
				require.NotContains(t, out.Body.String(), deep.MemoryID)
				require.NotContains(t, out.Body.String(), deep.Content)
			} else {
				require.Contains(t, out.Body.String(), deep.MemoryID)
			}
			require.NoError(t, classificationErr)
			require.Equal(t, store.TaintServingClear, verdict.Outcome)
		})
	}
}

func TestTaintProjectionProblemIsMachineDistinguishable(t *testing.T) {
	for _, tc := range []struct {
		sentinel    error
		problemType string
	}{
		{store.ErrMemoryProjectionTainted, memoryTaintedProblemType},
		{store.ErrMemoryProjectionTaintPending, memoryTaintPendingProblemType},
	} {
		err := fmt.Errorf("%w: %w", store.ErrMemoryProjectionUnpublished, tc.sentinel)
		recorder := httptest.NewRecorder()
		require.True(t, writeAppV30TaintProjectionProblem(recorder, err))
		require.Equal(t, http.StatusUnavailableForLegalReasons, recorder.Code)
		var problem map[string]any
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
		require.Equal(t, tc.problemType, problem["type"])
		require.EqualValues(t, http.StatusUnavailableForLegalReasons, problem["status"])
	}
}

// This is an actual detail-handler read over canonical Badger state and a mock
// SQL projection. It is not a signed request, transaction or network fixture.
func TestGetMemoryTaintProblemUsesCanonicalAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pending     bool
		denied      bool
		malformed   bool
		budget      bool
		status      int
		problemType string
		title       string
		detail      string
	}{
		{name: "confirmed", status: http.StatusUnavailableForLegalReasons,
			problemType: memoryTaintedProblemType, title: memoryTaintedProblemTitle, detail: memoryTaintedProblemDetail},
		{name: "pending", pending: true, status: http.StatusUnavailableForLegalReasons,
			problemType: memoryTaintPendingProblemType, title: memoryTaintPendingProblemTitle, detail: memoryTaintPendingProblemDetail},
		{name: "denied_confirmed", denied: true, status: http.StatusForbidden},
		{name: "denied_pending", pending: true, denied: true, status: http.StatusForbidden},
		{name: "denied_malformed", denied: true, malformed: true, status: http.StatusForbidden},
		{name: "malformed", malformed: true, status: http.StatusServiceUnavailable},
		{name: "ancestor_budget", budget: true, status: http.StatusServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var srv *Server
			var reader string
			var record *memory.MemoryRecord
			if tc.budget {
				var canonical *store.BadgerStore
				srv, canonical, _, reader, record = appV23TaintCoverageFixture(t, false, false)
				_, err := canonical.ClassifyMemoryTaint(record.MemoryID)
				require.ErrorIs(t, err, store.ErrTaintServingWalkBudget,
					"the fixture must reach actual canonical ancestor-budget uncertainty")
			} else {
				var rows *rbacMockMemoryStore
				var canonical *store.BadgerStore
				var owner, outsider string
				srv, rows, canonical, _, reader, owner, outsider = appV23DisclosureFixture(t)
				record = appV23PagingRecord("typed-taint-detail", owner, "owner.home",
					"protected detail fixture content", memory.TypeObservation, memory.StatusCommitted, time.Now())
				rows.memories[record.MemoryID] = record
				publishAppV23RESTRecord(t, canonical, record, 0)
				ref := tx.ParentRef{SourceChainID: "sage", MemoryID: record.MemoryID}
				copy(ref.ContentHash[:], record.ContentHash)
				if tc.pending {
					ref.MemoryID = "typed-taint-parent"
					ref.ContentHash = sha256.Sum256([]byte("typed parent content"))
					require.NoError(t, canonical.SetMemoryHash(ref.MemoryID, ref.ContentHash[:], string(memory.StatusCommitted)))
					require.NoError(t, canonical.SetMemoryLineage(record.MemoryID, []tx.ParentRef{ref}))
				}
				decisionID := fmt.Sprintf("%x", sha256.Sum256([]byte("typed detail decision")))
				created, err := canonical.ApplyTaintDecision("sage", decisionID, ref, sha256.Sum256([]byte("typed detail payload")))
				require.NoError(t, err)
				require.True(t, created)
				if tc.malformed {
					require.NoError(t, canonical.SetRawForTest(taint.DecisionKey(decisionID), []byte("invalid header")))
				}
				if tc.denied {
					reader = outsider
				}
			}

			router := chi.NewRouter()
			router.Get("/v1/memory/{memory_id}", srv.handleGetMemory)
			request := appV23DisclosureRequest(reader, http.MethodGet, "/v1/memory/"+record.MemoryID, nil)
			out := httptest.NewRecorder()
			router.ServeHTTP(out, request)
			require.Equal(t, tc.status, out.Code, out.Body.String())
			require.Equal(t, "application/problem+json", out.Header().Get("Content-Type"))
			var problem map[string]any
			require.NoError(t, json.Unmarshal(out.Body.Bytes(), &problem))
			if tc.status == http.StatusUnavailableForLegalReasons {
				require.Equal(t, map[string]any{
					"type": tc.problemType, "title": tc.title,
					"status": float64(tc.status), "detail": tc.detail,
				}, problem, "typed denial must contain no content or record metadata")
			} else {
				require.Equal(t, fmt.Sprintf("https://sage.dev/errors/%d", tc.status), problem["type"])
				require.NotContains(t, out.Body.String(), "memory-tainted")
				require.NotContains(t, out.Body.String(), "memory-taint-pending")
			}
			for _, hidden := range []string{record.MemoryID, record.Content, record.DomainTag, record.SubmittingAgent} {
				require.NotContains(t, out.Body.String(), hidden)
			}
			require.NotContains(t, out.Body.String(), `"content_hash"`)
			require.NotContains(t, out.Body.String(), `"taint_coverage"`)
		})
	}
}

func TestTaintProjectionProblemRejectsUncertaintyAndBackendErrors(t *testing.T) {
	for name, err := range map[string]error{
		"backend":         fmt.Errorf("classify taint: %w", errors.New("backend unavailable")),
		"malformed":       fmt.Errorf("classify taint: %w", store.ErrTaintStateMalformed),
		"ancestor_budget": fmt.Errorf("%w: %w", store.ErrMemoryProjectionTaintIndeterminate, store.ErrTaintServingWalkBudget),
	} {
		t.Run(name, func(t *testing.T) {
			out := httptest.NewRecorder()
			require.False(t, writeAppV30TaintProjectionProblem(out, err))
			require.Empty(t, out.Body.String(), "unrelated failures must remain with the existing caller error path")
		})
	}
}
