package store

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// This is a store-level graph/work test, not signed memory admission or a
// network/quorum proof. All edges and memberships are real; no decision header,
// scan counter, materialization counter or frontier is fabricated.
func TestTaintAdversarialTenThousandFanoutDiamondAndBeforeCursor(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("adversarial-root")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	const fanout = 10000
	children := make([]tx.ParentRef, 0, fanout)
	// Small real transactions avoid making fixture construction itself one huge
	// pending-write iterator. They do not bypass any canonical lineage encoder.
	for start := 0; start < fanout; start += 128 {
		batch := s.BeginConsensusTransaction(nil)
		t.Cleanup(batch.DiscardConsensusTransaction)
		end := start + 128
		if end > fanout {
			end = fanout
		}
		for i := start; i < end; i++ {
			children = append(children, seedTaintChild(t, batch, target,
				fmt.Sprintf("adversarial-fanout-%05d", i)))
		}
		require.NoError(t, batch.CommitConsensusTransaction())
	}

	// The same grandchild is reachable through two different fanout children.
	// It and its descendant must each be materialized and queued only once.
	diamond := testTaintParent("adversarial-diamond")
	require.NoError(t, s.SetMemoryHash(diamond.MemoryID, diamond.ContentHash[:], "committed"))
	diamondParents := []tx.ParentRef{children[0], children[1]}
	tx.SortParentRefs(diamondParents)
	require.NoError(t, s.SetMemoryLineage(diamond.MemoryID, diamondParents))
	diamondChild := seedTaintChild(t, s, diamond, "adversarial-diamond-child")
	require.NoError(t, s.ValidateLineageIndexes())

	decisionID := testTaintDecisionID("adversarial real fanout diamond cursor")
	created, err := s.ApplyTaintDecision("sage", decisionID, target, testTaintPayloadHash(decisionID))
	require.NoError(t, err)
	require.True(t, created)
	first, err := s.ExpandTaintDecision("sage", decisionID, 50)
	require.NoError(t, err)
	require.True(t, first.Changed)
	require.Equal(t, 47, first.Units)
	require.Equal(t, 7, first.Scanned)
	require.Equal(t, 7, first.Materialized)
	require.Equal(t, first.Materialized, first.Enqueued)
	require.Equal(t, taint.StatusExpanding, first.Status)
	beforeInsertion, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, target.MemoryID, beforeInsertion.ParentMemoryID)
	require.Equal(t, children[6].MemoryID, beforeInsertion.LastEdge)

	// A new edge inserted before the saved cursor is not rediscovered by a
	// forward-only scan. The real store admission seam must inherit the hold in
	// the same transaction that installs its lineage, including its descendant.
	lateID := "adversarial-fanout-00000-late"
	require.Less(t, lateID, beforeInsertion.LastEdge)
	admission := s.BeginConsensusTransaction(nil)
	t.Cleanup(admission.DiscardConsensusTransaction)
	admit := func(id string, parent tx.ParentRef) tx.ParentRef {
		child := testTaintParent(id)
		require.NoError(t, admission.SetMemoryHash(id, child.ContentHash[:], "committed"))
		require.NoError(t, admission.ValidateMemoryLineageParents(id, "sage", []tx.ParentRef{parent}))
		require.NoError(t, admission.SetMemoryLineage(id, []tx.ParentRef{parent}))
		inherited, err := admission.ApplyTaintAdmission("sage", id, []tx.ParentRef{parent}, nil)
		require.NoError(t, err)
		require.Equal(t, 1, inherited.Decisions)
		return child
	}
	late := admit(lateID, target)
	lateChild := admit("adversarial-late-descendant", late)
	require.NoError(t, admission.CommitConsensusTransaction())
	afterInsertion, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, beforeInsertion, afterInsertion,
		"admission must not rewind or fabricate the expansion cursor/counters")
	rootRefID, err := tx.ParentRefIDHex(target)
	require.NoError(t, err)
	nextChildren, err := s.ListLineageChildren(rootRefID, beforeInsertion.LastEdge, 2)
	require.NoError(t, err)
	require.Equal(t, []string{children[7].MemoryID, children[8].MemoryID}, nextChildren,
		"the inserted earlier key is genuinely behind the exclusive continuation cursor")
	for _, ref := range []tx.ParentRef{late, lateChild} {
		membership, err := taint.DecodeMembership(readTaintValue(t, s,
			taint.MembershipKey(ref.MemoryID, decisionID)))
		require.NoError(t, err)
		require.Equal(t, taint.OriginAdmit, membership.Origin)
		require.EqualValues(t, 1, membership.Generation)
		require.Zero(t, membership.Depth)
		decisions, err := s.GetEffectiveTaintDecisions(ref.MemoryID)
		require.NoError(t, err)
		require.Equal(t, []string{decisionID}, decisions)
	}

	totalScanned, totalMaterialized := first.Scanned, first.Materialized
	previous := afterInsertion
	complete := false
	for call := 0; call < 200; call++ {
		result, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		require.True(t, result.Changed, "unfinished real work must make bounded progress (call %d)", call)
		require.LessOrEqual(t, result.Units, taint.PerExecutionUnitBudget)
		require.LessOrEqual(t, result.Materialized, taint.MaxNewMembershipsPerExec)
		require.LessOrEqual(t, result.Scanned, (taint.PerExecutionUnitBudget-3)/2)
		require.GreaterOrEqual(t, result.Units, 3+2*result.Scanned+4*result.Materialized,
			"each scanned edge costs two reads; each new membership adds four units")
		require.Equal(t, result.Materialized, result.Enqueued)
		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.EqualValues(t, uint64(previous.Scanned)+uint64(result.Scanned), header.Scanned)
		require.EqualValues(t, uint64(previous.Materialized)+uint64(result.Materialized), header.Materialized)
		require.Equal(t, previous.FrontierTail+uint64(result.Enqueued), header.FrontierTail)
		require.GreaterOrEqual(t, header.FrontierHead, previous.FrontierHead)
		if header.ParentRefID != "" && header.ParentRefID == previous.ParentRefID {
			require.Greater(t, header.LastEdge, previous.LastEdge)
		}
		totalScanned += result.Scanned
		totalMaterialized += result.Materialized
		previous = header
		if result.Status == taint.StatusComplete {
			complete = true
			break
		}
		require.Equal(t, taint.StatusExpanding, result.Status)
	}
	require.True(t, complete, "real 10k fanout must finish without exhausting the 16,384-member cap")
	// Original edges: 10k fanout + two diamond parents + one diamond child.
	// The two later inherited nodes remain covered without rescanning old keys.
	require.Equal(t, fanout+3, totalScanned)
	require.Equal(t, fanout+2, totalMaterialized)
	require.EqualValues(t, fanout+3, previous.Materialized)
	require.EqualValues(t, fanout+3, previous.Scanned)
	require.EqualValues(t, fanout+3, previous.FrontierTail)
	require.Equal(t, previous.FrontierTail, previous.FrontierHead)
	require.EqualValues(t, 3, previous.MaxDepth)
	require.Empty(t, previous.ParentRefID)
	require.Empty(t, previous.LastEdge)
	require.Zero(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))
	require.Equal(t, fanout+5, countTaintPrefix(t, s, taint.DecisionIndexPrefix(decisionID)))
	for _, entry := range []struct {
		ref   tx.ParentRef
		depth uint16
	}{{diamond, 2}, {diamondChild, 3}} {
		membership, err := taint.DecodeMembership(readTaintValue(t, s,
			taint.MembershipKey(entry.ref.MemoryID, decisionID)))
		require.NoError(t, err)
		require.Equal(t, taint.Membership{Generation: 1, Depth: entry.depth, Origin: taint.OriginExpand}, membership)
	}
	want := make([]string, 0, 5+len(children))
	want = append(want, target.MemoryID, diamond.MemoryID, diamondChild.MemoryID, late.MemoryID, lateChild.MemoryID)
	for _, child := range children {
		want = append(want, child.MemoryID)
	}
	sort.Strings(want)
	revision, effective, err := s.SnapshotEffectiveTaintMemberships()
	require.NoError(t, err)
	require.Equal(t, want, effective, "no reachable original or newly inherited node may silently disappear")
	require.NoError(t, s.ValidateLineageIndexes())
	require.NoError(t, s.ValidateTaintState())
	replayed, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.False(t, replayed.Changed)
	require.Equal(t, taint.StatusComplete, replayed.Status)
	require.Equal(t, 1, replayed.Units)
	require.Zero(t, replayed.Scanned)
	require.Zero(t, replayed.Materialized)
	revisionAfter, err := s.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, revision, revisionAfter)
}

// 8192 is the ranked-recall policy-candidate cap, not the expansion scan cap.
// Exercise real SQLite FTS candidates; no synthetic scanned-counter fixture.
func TestTaintAdversarialSQLiteSparseCandidateTail8192And8193(t *testing.T) {
	require.Equal(t, 8192, CandidateFilterScanBudget)
	s := newTestStore(t)
	ctx := context.Background()
	const needle = "adversarialtailneedle"
	ids := make([]string, CandidateFilterScanBudget+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("adversarial-candidate-%05d", i)
		record := testMemory(ids[i], "candidate-agent", needle, "candidate-domain")
		record.Status = memory.StatusCommitted
		require.NoError(t, s.InsertMemory(ctx, record))
	}
	var indexed int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM memories_fts WHERE memories_fts MATCH ?`, needle).Scan(&indexed))
	require.Equal(t, 8193, indexed, "all candidates, including the sparse tail, must really exist in FTS")

	for _, tc := range []struct {
		name         string
		topK         int
		allowAtCap   bool
		allowPastCap bool
		wantBudget   bool
	}{
		{name: "eligible_at_8192", topK: 1, allowAtCap: true},
		{name: "eligible_only_at_8193", topK: 1, allowPastCap: true, wantBudget: true},
		{name: "partial_at_8192_must_not_hide_8193", topK: 2, allowAtCap: true, allowPastCap: true, wantBudget: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			visited := make([]string, 0, CandidateFilterScanBudget)
			results, err := s.SearchByText(ctx, needle, QueryOptions{
				TopK: tc.topK, StatusFilter: "committed",
				CandidateFilter: func(record *memory.MemoryRecord) (bool, error) {
					visited = append(visited, record.MemoryID)
					return (tc.allowAtCap && record.MemoryID == ids[8191]) ||
						(tc.allowPastCap && record.MemoryID == ids[8192]), nil
				},
			})
			require.Equal(t, ids[:8192], visited, "stable equal-rank ordering must scan each ranked candidate exactly once")
			if tc.wantBudget {
				require.ErrorIs(t, err, ErrCandidateFilterScanBudgetExceeded)
				require.Nil(t, results, "budget exhaustion must not become misleading empty or partial success")
			} else {
				require.NoError(t, err)
				require.Len(t, results, 1)
				require.Equal(t, ids[8191], results[0].MemoryID)
			}
		})
	}
}
