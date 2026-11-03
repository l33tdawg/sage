package store

import (
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
)

func TestTaintWorkCapOverflowOnlyCallPersistsTerminalState(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("work-cap-transition-root")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	// The target consumes one slot. Populate the entire real expansion cap
	// plus one overflowing child; never rewrite a header/counter or frontier.
	const children = taint.MaxExpansionMemberships
	childID := func(i int) string { return fmt.Sprintf("work-cap-transition-child-%05d", i) }
	for start := 0; start < children; start += 256 {
		func() {
			batch := s.BeginConsensusTransaction(nil)
			defer batch.DiscardConsensusTransaction()
			end := start + 256
			if end > children {
				end = children
			}
			for i := start; i < end; i++ {
				seedTaintChild(t, batch, target, childID(i))
			}
			require.NoError(t, batch.CommitConsensusTransaction())
		}()
	}
	require.NoError(t, s.ValidateLineageIndexes())
	decisionID := testTaintDecisionID("real cap separate overflow transition")
	created, err := s.ApplyTaintDecision("sage", decisionID, target, testTaintPayloadHash(decisionID))
	require.NoError(t, err)
	require.True(t, created)

	// Fill through ordinary execution. Once the already-selected root has at
	// most170 slots left, spend exactly the accepted3 fixed +6 per fresh child
	// units. That call ends before it can examine the overflowing edge.
	alignedFill := false
	for call := 0; call < 200; call++ {
		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		remaining := taint.MaxExpansionMemberships - int(header.Materialized)
		require.Positive(t, remaining)
		budget := taint.PerExecutionUnitBudget
		if header.ParentMemoryID == target.MemoryID && remaining <= (taint.PerExecutionUnitBudget-3)/6 {
			budget = 3 + 6*remaining
			alignedFill = true
		}
		result, err := s.ExpandTaintDecision("sage", decisionID, budget)
		require.NoError(t, err)
		require.True(t, result.Changed)
		require.Equal(t, taint.StatusExpanding, result.Status,
			"the separate overflow edge must not yet have been examined")
		require.Equal(t, result.Materialized, result.Scanned)
		require.Equal(t, result.Materialized, result.Enqueued)
		require.LessOrEqual(t, result.Units, budget)
		require.LessOrEqual(t, result.Materialized, taint.MaxNewMembershipsPerExec)
		if alignedFill {
			require.Equal(t, remaining, result.Materialized)
			require.Equal(t, budget, result.Units)
			break
		}
	}
	require.True(t, alignedFill, "the cap must be reached by real bounded work, not a synthetic near-cap header")
	before, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, taint.MaxExpansionMemberships, before.Materialized)
	require.EqualValues(t, children-1, before.Scanned)
	require.EqualValues(t, children, before.FrontierTail)
	require.EqualValues(t, 1, before.FrontierHead)
	require.Equal(t, target.MemoryID, before.ParentMemoryID)
	require.Equal(t, childID(children-2), before.LastEdge)
	require.Equal(t, taint.StatusExpanding, before.Status)
	require.Equal(t, children, countTaintPrefix(t, s, taint.DecisionIndexPrefix(decisionID)))
	revisionBefore, err := s.TaintRevision()
	require.NoError(t, err)

	// Overflow is the first edge in this execution: no pop, successful scan,
	// materialization or cursor movement can incidentally mark it changed.
	result, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	after, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	revisionAfter, err := s.TaintRevision()
	require.NoError(t, err)
	t.Logf("overflow-only: result_status=%d changed=%t units=%d scanned=%d materialized=%d enqueued=%d persisted_status=%d revision=%d->%d parent=%q cursor=%q",
		result.Status, result.Changed, result.Units, result.Scanned, result.Materialized, result.Enqueued,
		after.Status, revisionBefore, revisionAfter, after.ParentMemoryID, after.LastEdge)
	require.True(t, result.Changed, "selecting a new terminal status is itself a durable mutation")
	require.Equal(t, taint.StatusWorkExhausted, result.Status)
	require.Equal(t, 5, result.Units, "header/revision mutation plus the two overflow-edge reads")
	require.Zero(t, result.Scanned)
	require.Zero(t, result.Materialized)
	require.Zero(t, result.Enqueued)
	want := before
	want.Status = taint.StatusWorkExhausted
	require.Equal(t, want, after, "only the terminal status changes; the unconsumed edge/cursor/frontier stay intact")
	require.Equal(t, revisionBefore+1, revisionAfter)
	for _, key := range [][]byte{
		taint.MembershipKey(childID(children-1), decisionID),
		taint.DecisionIndexKey(decisionID, childID(children-1)),
	} {
		require.NoError(t, s.view(func(reader *badger.Txn) error {
			_, readErr := reader.Get(key)
			require.ErrorIs(t, readErr, badger.ErrKeyNotFound)
			return nil
		}))
	}
	beyond, err := s.ClassifyMemoryTaint(childID(children - 1))
	require.NoError(t, err)
	require.Equal(t, TaintServingPending, beyond.Outcome)
	require.Equal(t, []string{decisionID}, beyond.DecisionIDs)
	require.True(t, beyond.WorkExhausted)
	require.NoError(t, s.ValidateTaintState())

	terminalBytes := readTaintValue(t, s, taint.DecisionKey(decisionID))
	replayed, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, TaintExpandResult{Units: 1, Status: taint.StatusWorkExhausted}, replayed)
	require.Equal(t, terminalBytes, readTaintValue(t, s, taint.DecisionKey(decisionID)))
	revisionReplayed, err := s.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, revisionAfter, revisionReplayed)
}
