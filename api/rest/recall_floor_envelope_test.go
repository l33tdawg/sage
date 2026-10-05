package rest

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func storeQueryOptionsWithFloor(floor float64) store.QueryOptions {
	return store.QueryOptions{MinConfidence: floor}
}

func timeNowUTC() time.Time { return time.Now().UTC() }

// The confidence floor is a silent-hide filter whose setting lives in operator
// preferences, not in the call. These tests pin the disclosure that makes it
// non-silent: the caller must be able to tell an empty store from a store whose
// matching records were filtered away.
func TestSetFilterInfoDisclosesTheConfidenceFloor(t *testing.T) {
	t.Run("floor with hidden candidates", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		resp := &QueryMemoryResponse{}

		setFilterInfo(recorder, resp, false, 0, floorInfo{floor: 0.85, hidden: 2, applied: true})

		require.NotNil(t, resp.Filtered)
		require.Contains(t, resp.Filtered.By, filterByConfidence)
		require.NotNil(t, resp.Filtered.ConfidenceFloor)
		require.Equal(t, 0.85, *resp.Filtered.ConfidenceFloor)
		require.NotNil(t, resp.Filtered.HiddenByConfidenceFloor)
		require.Equal(t, 2, *resp.Filtered.HiddenByConfidenceFloor)
		require.Contains(t, recorder.Header().Get(filterHeader), filterByConfidence)
	})

	t.Run("floor that hid nothing is still disclosed", func(t *testing.T) {
		// "The floor ran and removed nothing" is the fact that lets a caller trust
		// an empty result. Staying silent here would rebuild the original bug with
		// the count merely absent.
		recorder := httptest.NewRecorder()
		resp := &QueryMemoryResponse{}

		setFilterInfo(recorder, resp, false, 0, floorInfo{floor: 0.70, applied: true})

		require.NotNil(t, resp.Filtered)
		require.NotNil(t, resp.Filtered.ConfidenceFloor)
		require.Equal(t, 0.70, *resp.Filtered.ConfidenceFloor)
		require.NotNil(t, resp.Filtered.HiddenByConfidenceFloor)
		require.Zero(t, *resp.Filtered.HiddenByConfidenceFloor)
	})

	t.Run("no floor was requested", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		resp := &QueryMemoryResponse{}

		setFilterInfo(recorder, resp, false, 0, floorInfo{})

		require.Nil(t, resp.Filtered, "a filter that did not run must not appear in the envelope")
		require.Empty(t, recorder.Header().Get(filterHeader))
	})

	t.Run("floor composes with the other silent-hide filters", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		resp := &QueryMemoryResponse{}

		setFilterInfo(recorder, resp, true, 4, floorInfo{floor: 0.85, hidden: 1, applied: true})

		require.NotNil(t, resp.Filtered)
		require.ElementsMatch(t,
			[]string{filterBySubmittingAgts, filterByClassification, filterByConfidence},
			resp.Filtered.By)
		require.Equal(t, 4, *resp.Filtered.HiddenCount)
		require.Equal(t, 1, *resp.Filtered.HiddenByConfidenceFloor)
	})
}

// The count is read from the store's sink AFTER the query, so the plumbing has
// to survive a nil sink (a floor requested on a path where the store never ran)
// without inventing a number.
func TestFloorInfoResolvedReadsTheStoreSink(t *testing.T) {
	dropped := 3
	got := floorInfo{floor: 0.85, applied: true}.resolved(&dropped)
	require.Equal(t, 3, got.hidden)
	require.Equal(t, 0.85, got.floor)

	require.Zero(t, floorInfo{floor: 0.85, applied: true}.resolved(nil).hidden,
		"an unfilled sink reports zero rather than a stale or invented count")
}

// setDecayFloor must hand the store a sink and leave the legacy stored-column
// filter off, which is what makes the decayed count the only one that matters.
func TestSetDecayFloorAllocatesTheDroppedSink(t *testing.T) {
	opts := storeQueryOptionsWithFloor(0.85)
	info := setDecayFloor(&opts, timeNowUTC())

	require.True(t, info.applied)
	require.Equal(t, 0.85, info.floor)
	require.Zero(t, opts.MinConfidence, "the legacy decay-blind filter must be disabled")
	require.Equal(t, 0.85, opts.DecayFloor)
	require.NotNil(t, opts.DecayFloorDropped, "the store needs somewhere to report what it dropped")

	// No floor requested: nothing allocated, nothing reported.
	opts = storeQueryOptionsWithFloor(0)
	info = setDecayFloor(&opts, timeNowUTC())
	require.False(t, info.applied)
	require.Nil(t, opts.DecayFloorDropped)
}
