package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
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
