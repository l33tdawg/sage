package rest

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
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

func TestTaintedExactDenialRequiresRecordAuthorizationFirst(t *testing.T) {
	srv, memStore, badger, _, readerID, ownerID, outsiderID := appV23DisclosureFixture(t)
	content := "protected tainted memory"
	seedMemory(t, memStore, "protected-tainted", ownerID, "owner.home", content)
	record := memStore.memories["protected-tainted"]
	publishAppV23RESTRecord(t, badger, record, 1)

	before, err := srv.evaluateAppV23RecordDisclosure(readerID, record, time.Now())
	require.NoError(t, err)
	require.True(t, before.Allowed)

	target := tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      record.MemoryID,
		ContentHash:   sha256.Sum256([]byte(content)),
	}
	created, err := badger.ApplyTaintDecision(
		"sage",
		strings.Repeat("e", 64),
		target,
		sha256.Sum256([]byte("protected taint payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	_, err = srv.evaluateAppV23RecordDisclosure(readerID, record, time.Now())
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)

	denied, err := srv.evaluateAppV23RecordDisclosure(outsiderID, record, time.Now())
	require.NoError(t, err, "unauthorized callers must receive ordinary denial, not a taint existence signal")
	require.False(t, denied.Allowed)
}
