package rest

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
