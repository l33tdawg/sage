package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/stretchr/testify/require"
)

func TestTaintServingCoverageSnapshotUsesRealExpansion(t *testing.T) {
	s := openTaintStore(t)
	ctx := context.Background()
	revision, complete, err := s.TaintServingCoverageSnapshot(ctx, nil)
	require.NoError(t, err)
	require.Zero(t, revision)
	require.True(t, complete)
	id := seedServingIncompleteDecision(t, s, "coverage-real-expansion")
	revision, complete, err = s.TaintServingCoverageSnapshot(ctx, NewTaintServingReadScope())
	require.NoError(t, err)
	require.False(t, complete)
	wantRevision, err := s.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, wantRevision, revision)
	expanded, err := s.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	active, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	require.Equal(t, uint64(1), active, "completed decisions can remain active")
	revision, complete, err = s.TaintServingCoverageSnapshot(ctx, NewTaintServingReadScope())
	require.NoError(t, err)
	require.True(t, complete)
	wantRevision, err = s.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, wantRevision, revision)
}

func TestTaintServingCoverageSnapshotIgnoresFalseZeroCounter(t *testing.T) {
	s := openTaintStore(t)
	seedServingIncompleteDecision(t, s, "coverage-false-zero")
	require.NoError(t, s.SetRawForTest(taint.ActiveCountKey(), make([]byte, 8)))
	_, complete, err := s.TaintServingCoverageSnapshot(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, complete, "zero advisory counter is not an absence proof")
}

func TestTaintServingCoverageSnapshotCapsAndSharedAllowance(t *testing.T) {
	for _, count := range []int{taintServingHeaderReadLimit, taintServingHeaderReadLimit + 1} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s := openTaintStore(t)
			// The existing fixture completes Apply/Expand/Clear/Cleanup before
			// copying clean headers solely to challenge the bounded scan.
			seedServingHeaderScanFixture(t, s, count)
			scope := NewTaintServingReadScope()
			_, complete, err := s.TaintServingCoverageSnapshot(context.Background(), scope)
			require.NoError(t, err)
			require.Equal(t, count == taintServingHeaderReadLimit, complete)
			require.Zero(t, scope.remaining)
		})
	}
	s := openTaintStore(t)
	seedServingHeaderScanFixture(t, s, 2)
	_, complete, err := s.TaintServingCoverageSnapshot(context.Background(), &TaintServingReadScope{})
	require.NoError(t, err)
	require.False(t, complete, "a request with exhausted allowance cannot start a new cold scan")
	_, complete, err = s.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
	require.NoError(t, err)
	require.True(t, complete, "a partial request must not publish a false snapshot cache entry")
}

func TestTaintServingCoverageSnapshotRevisionSharesHeaderView(t *testing.T) {
	s := openTaintStore(t)
	require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
		oldRevision, err := reader.TaintRevision()
		require.NoError(t, err)
		seedServingIncompleteDecision(t, s, "coverage-newer-snapshot")
		revision, complete, err := reader.TaintServingCoverageSnapshot(context.Background(), nil)
		require.NoError(t, err)
		require.Equal(t, oldRevision, revision)
		require.True(t, complete, "revision and headers must remain in the old reader snapshot")
		return nil
	}))
	_, complete, err := s.TaintServingCoverageSnapshot(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, complete, "a new snapshot sees the new incomplete header")
}

func TestTaintServingCoverageSnapshotMalformedOrCancelled(t *testing.T) {
	s := openTaintStore(t)
	id := seedServingIncompleteDecision(t, s, "coverage-malformed")
	require.NoError(t, s.SetRawForTest(taint.DecisionKey(id), []byte("invalid")))
	_, complete, err := s.TaintServingCoverageSnapshot(context.Background(), nil)
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	require.False(t, complete)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, complete, err = s.TaintServingCoverageSnapshot(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, complete)
}
