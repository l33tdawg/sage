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
