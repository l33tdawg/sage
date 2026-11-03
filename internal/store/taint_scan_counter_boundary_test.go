package store

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
)

// These are defensive damaged-counter fixtures, not proof that valid admission
// can produce an undercount. Decision/header/membership state is created by the
// real store lifecycle before the counter or later header is explicitly damaged.
func seedCounterBoundaryDecision(t *testing.T, s *BadgerStore, id, label string, complete bool) string {
	t.Helper()
	target := testTaintParent(label)
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], string(memory.StatusCommitted)))
	created, err := s.ApplyTaintDecision("sage", id, target, testTaintPayloadHash(label))
	require.NoError(t, err)
	require.True(t, created)
	if complete {
		expanded, err := s.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		require.Equal(t, taint.StatusComplete, expanded.Status)
	}
	return target.MemoryID
}

func TestTaintSnapshotsIgnoreCounterBeforeLaterIncompleteHeader(t *testing.T) {
	for _, tc := range []struct {
		name    string
		counter uint64
	}{
		{"false_zero", 0},
		{"undercount_one", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			canonical, mirror := boundedMirrorFixture(t)
			earlyID := strings.Repeat("1", taint.DecisionIDBytes)
			lateID := strings.Repeat("e", taint.DecisionIDBytes)
			early := seedCounterBoundaryDecision(t, canonical, earlyID, "counter-early", true)
			late := seedCounterBoundaryDecision(t, canonical, lateID, "counter-late", false)
			header, found, err := canonical.GetTaintDecision(lateID)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, header.Status.Incomplete())
			active, err := canonical.ActiveTaintDecisionCount()
			require.NoError(t, err)
			require.Equal(t, uint64(2), active)
			counter := make([]byte, 8)
			binary.BigEndian.PutUint64(counter, tc.counter)
			require.NoError(t, canonical.SetRawForTest(taint.ActiveCountKey(), counter))

			revision, ids, err := canonical.SnapshotEffectiveTaintMemberships()
			require.NoError(t, err)
			require.Equal(t, []string{early, late}, ids,
				"both direct memberships remain effective beyond the counter boundary")
			coverageRevision, complete, err := canonical.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
			require.NoError(t, err)
			require.Equal(t, revision, coverageRevision)
			require.False(t, complete, "the later incomplete header cannot be hidden by the singleton")

			publishedRevision, count, published, err := RefreshMemoryTaintMirrorBounded(
				context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
			require.NoError(t, err)
			require.True(t, published)
			require.Equal(t, revision, publishedRevision)
			require.Equal(t, 2, count)
			assertBoundedMirrorRows(t, mirror, revision, []string{early, late})
		})
	}
}

func TestTaintSnapshotsRejectMalformedHeaderAfterCounterBoundary(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	earlyID := strings.Repeat("1", taint.DecisionIDBytes)
	lateID := strings.Repeat("e", taint.DecisionIDBytes)
	seedCounterBoundaryDecision(t, canonical, earlyID, "malformed-early", true)
	seedCounterBoundaryDecision(t, canonical, lateID, "malformed-late", true)
	counter := make([]byte, 8)
	binary.BigEndian.PutUint64(counter, 1)
	require.NoError(t, canonical.SetRawForTest(taint.ActiveCountKey(), counter))
	require.NoError(t, canonical.SetRawForTest(taint.DecisionKey(lateID), []byte("invalid")))
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 0, []string{"old"}))

	_, _, err := canonical.SnapshotEffectiveTaintMemberships()
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	_, complete, err := canonical.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	require.False(t, complete)
	_, _, published, err := RefreshMemoryTaintMirrorBounded(
		context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 0, []string{"old"})
}

func TestBoundedTaintSnapshotChargesRetiredHistoryAfterActiveBoundary(t *testing.T) {
	canonical, mirror := boundedMirrorFixture(t)
	activeID := strings.Repeat("1", taint.DecisionIDBytes)
	activeTarget := seedCounterBoundaryDecision(t, canonical, activeID, "history-active", true)
	// Five charged visits exactly fit revision + header key/value + the
	// single target index/value before any retained history is added.
	_, ids, err := canonical.snapshotEffectiveTaintMemberships(context.Background(), 5, 10)
	require.NoError(t, err)
	require.Equal(t, []string{activeTarget}, ids)
	for _, tc := range []struct {
		id    string
		label string
	}{
		{strings.Repeat("e", taint.DecisionIDBytes), "history-retired-one"},
		{strings.Repeat("f", taint.DecisionIDBytes), "history-retired-two"},
	} {
		seedCounterBoundaryDecision(t, canonical, tc.id, tc.label, false)
		changed, err := canonical.ClearTaintDecision(tc.id)
		require.NoError(t, err)
		require.True(t, changed)
		finishServingDecisionCleanup(t, canonical, tc.id)
		header, found, err := canonical.GetTaintDecision(tc.id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, taint.StatusClean, header.Status)
	}
	active, err := canonical.ActiveTaintDecisionCount()
	require.NoError(t, err)
	require.Equal(t, uint64(1), active)
	require.NoError(t, mirror.ReplaceMemoryTaintMirror(context.Background(), 0, []string{"old"}))
	_, _, published, err := RefreshMemoryTaintMirrorBounded(context.Background(), canonical, mirror,
		MemoryTaintMirrorLimits{MaxIDs: 10, MaxRows: 10, MaxVisits: 5})
	require.ErrorIs(t, err, ErrMemoryTaintMirrorWorkLimit,
		"retired headers after the known active boundary still consume scan work")
	require.False(t, published)
	assertBoundedMirrorRows(t, mirror, 0, []string{"old"})

	revision, count, published, err := RefreshMemoryTaintMirrorBounded(
		context.Background(), canonical, mirror, DefaultMemoryTaintMirrorLimits())
	require.NoError(t, err)
	require.True(t, published)
	require.Equal(t, 1, count)
	assertBoundedMirrorRows(t, mirror, revision, []string{activeTarget})
	scope := NewTaintServingReadScope()
	_, complete, err := canonical.TaintServingCoverageSnapshot(context.Background(), scope)
	require.NoError(t, err)
	require.True(t, complete)
	require.Equal(t, taintServingHeaderReadLimit-3, scope.remaining,
		"coverage reads the active header and both later clean headers")
}
