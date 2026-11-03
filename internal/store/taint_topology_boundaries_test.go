package store

import (
	"context"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// These are structural store fixtures, not signed transaction or process-crash
// tests. Every hash and immutable lineage edge is installed before Apply, so
// only normal expansion can create the decision's descendant memberships.
func TestTaintTopologyLinkedDepthBoundary(t *testing.T) {
	for _, edges := range []int{64, 65} {
		t.Run(fmt.Sprintf("edges_%d", edges), func(t *testing.T) {
			s := openTaintStore(t)
			refs := seedTaintTopology(t, s, edges+1, true)
			decisionID := applyTaintTopology(t, s, refs[0])
			assertTaintTopologyCoverage(t, s, decisionID, refs[0], false)

			// A small execution budget forces the actual linked walk to resume.
			header, trace := expandTaintTopology(t, s, decisionID, 31)
			require.Greater(t, trace.calls, 1)
			require.Equal(t, uint32(64), header.MaxDepth)
			require.Equal(t, uint32(65), header.Materialized)
			require.Equal(t, uint32(edges), header.Scanned)
			require.Equal(t, uint64(65), header.FrontierHead)
			require.Equal(t, header.FrontierHead, header.FrontierTail)
			require.Empty(t, header.ParentRefID)
			require.Empty(t, header.ParentMemoryID)
			require.Empty(t, header.LastEdge)
			require.Zero(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))
			assertTaintTopologyMemberships(t, s, decisionID, refs, 65, true)

			wantStatus := taint.StatusComplete
			if edges == 65 {
				wantStatus = taint.StatusDepthExhausted
			}
			require.Equal(t, wantStatus, header.Status)
			assertTaintTopologyCoverage(t, s, decisionID, refs[0], edges == 64)
			lastMember, memberErr := s.ClassifyMemoryTaint(refs[64].MemoryID)
			require.NoError(t, memberErr)
			require.Equal(t, TaintServingTainted, lastMember.Outcome)
			require.Equal(t, []string{decisionID}, lastMember.DecisionIDs)
			require.Equal(t, edges == 65, lastMember.DepthExhausted)
			require.False(t, lastMember.WorkExhausted)
			if edges == 65 {
				// The real edge beyond depth 64 was scanned, but its child never
				// gained a membership. Its incomplete authenticated parent still
				// requires a hold rather than a false clear result.
				beyond, beyondErr := s.ClassifyMemoryTaint(refs[65].MemoryID)
				require.NoError(t, beyondErr)
				require.Equal(t, TaintServingPending, beyond.Outcome)
				require.Equal(t, []string{decisionID}, beyond.DecisionIDs)
				require.True(t, beyond.DepthExhausted)
				require.False(t, beyond.WorkExhausted)
				require.False(t, beyond.CoverageComplete)
			}
			assertTaintTopologyTerminalReplay(t, s, decisionID, header.Status)
			require.NoError(t, s.ValidateLineageIndexes())
			require.NoError(t, s.ValidateTaintState())
			t.Logf("linked edges=%d calls=%d max_units=%d materialized=%d scanned=%d status=%d",
				edges, trace.calls, trace.maxUnits, header.Materialized, header.Scanned, header.Status)
		})
	}
}

func TestTaintTopologyExpansionMembershipBoundary(t *testing.T) {
	// The target is itself one membership. The second fixture therefore has
	// 16,384 real children, not a fabricated near-cap Materialized counter.
	for _, nodes := range []int{16384, 16385} {
		t.Run(fmt.Sprintf("nodes_%d", nodes), func(t *testing.T) {
			s := openTaintStore(t)
			refs := seedTaintTopology(t, s, nodes, false)
			decisionID := applyTaintTopology(t, s, refs[0])
			assertTaintTopologyCoverage(t, s, decisionID, refs[0], false)

			header, trace := expandTaintTopology(t, s, decisionID, taint.PerExecutionUnitBudget)
			require.Greater(t, trace.calls, 1)
			require.Greater(t, trace.cursorResumes, 0, "the same real parent resumes strictly after LastEdge")
			require.Equal(t, taint.StatusExpanding, trace.first.Status)
			require.Equal(t, refs[0].MemoryID, trace.first.ParentMemoryID)
			require.NotEmpty(t, trace.first.LastEdge)
			require.Greater(t, trace.first.Materialized, uint32(1))
			require.Less(t, trace.first.Materialized, uint32(16384))
			require.Equal(t, uint32(16384), header.Materialized)
			require.Equal(t, uint32(16383), header.Scanned)
			require.Equal(t, uint32(1), header.MaxDepth)
			require.Equal(t, uint64(16384), header.FrontierTail)
			assertTaintTopologyMemberships(t, s, decisionID, refs, 16384, false)

			complete := nodes == 16384
			if complete {
				require.Equal(t, taint.StatusComplete, header.Status)
				require.Equal(t, header.FrontierTail, header.FrontierHead)
				require.Empty(t, header.ParentRefID)
				require.Empty(t, header.ParentMemoryID)
				require.Empty(t, header.LastEdge)
				require.Zero(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))
			} else {
				require.Equal(t, taint.StatusWorkExhausted, header.Status)
				require.Equal(t, uint64(1), header.FrontierHead)
				parentID, parentErr := tx.ParentRefIDHex(refs[0])
				require.NoError(t, parentErr)
				require.Equal(t, parentID, header.ParentRefID)
				require.Equal(t, refs[0].MemoryID, header.ParentMemoryID)
				require.Zero(t, header.ParentDepth)
				require.Equal(t, refs[16383].MemoryID, header.LastEdge,
					"the overflow edge must not advance the persisted cursor")
				require.Equal(t, 16383, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))
				beyond, beyondErr := s.ClassifyMemoryTaint(refs[16384].MemoryID)
				require.NoError(t, beyondErr)
				require.Equal(t, TaintServingPending, beyond.Outcome)
				require.Equal(t, []string{decisionID}, beyond.DecisionIDs)
				require.True(t, beyond.WorkExhausted)
				require.False(t, beyond.DepthExhausted)
				require.False(t, beyond.CoverageComplete)
			}
			assertTaintTopologyCoverage(t, s, decisionID, refs[0], complete)
			lastMember, memberErr := s.ClassifyMemoryTaint(refs[16383].MemoryID)
			require.NoError(t, memberErr)
			require.Equal(t, TaintServingTainted, lastMember.Outcome)
			require.Equal(t, []string{decisionID}, lastMember.DecisionIDs)
			require.Equal(t, !complete, lastMember.WorkExhausted)
			require.False(t, lastMember.DepthExhausted)
			require.Equal(t, complete, lastMember.CoverageComplete)
			assertTaintTopologyTerminalReplay(t, s, decisionID, header.Status)
			require.NoError(t, s.ValidateLineageIndexes())
			require.NoError(t, s.ValidateTaintState())
			t.Logf("fanout nodes=%d calls=%d cursor_resumes=%d max_units=%d materialized=%d scanned=%d status=%d",
				nodes, trace.calls, trace.cursorResumes, trace.maxUnits, header.Materialized, header.Scanned, header.Status)
		})
	}
}

func seedTaintTopology(t *testing.T, s *BadgerStore, nodes int, linked bool) []tx.ParentRef {
	t.Helper()
	refs := make([]tx.ParentRef, nodes)
	for i := range refs {
		refs[i] = testTaintParent(fmt.Sprintf("topology-node-%05d", i))
	}
	// Batch only ordinary hash/lineage setup. No taint header, queue,
	// membership, index, cursor or revision is written by the fixture.
	const batchSize = 256
	for start := 0; start < len(refs); start += batchSize {
		end := min(start+batchSize, len(refs))
		func() {
			staged := s.BeginConsensusTransaction(nil)
			defer staged.DiscardConsensusTransaction()
			for i := start; i < end; i++ {
				require.NoError(t, staged.SetMemoryHash(refs[i].MemoryID, refs[i].ContentHash[:], "committed"))
				if i == 0 {
					continue
				}
				parent := refs[0]
				if linked {
					parent = refs[i-1]
				}
				require.NoError(t, staged.ValidateMemoryLineageParents(refs[i].MemoryID, "sage", []tx.ParentRef{parent}))
				require.NoError(t, staged.SetMemoryLineage(refs[i].MemoryID, []tx.ParentRef{parent}))
			}
			require.NoError(t, staged.CommitConsensusTransaction())
		}()
	}
	require.NoError(t, s.SetAppV30Provenance(refs[0].MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
	require.NoError(t, s.ValidateLineageIndexes())
	return refs
}

func applyTaintTopology(t *testing.T, s *BadgerStore, target tx.ParentRef) string {
	t.Helper()
	decisionID := testTaintDecisionID(t.Name())
	created, applyErr := s.ApplyTaintDecision("sage", decisionID, target, testTaintPayloadHash(decisionID))
	require.NoError(t, applyErr)
	require.True(t, created)
	header, found, headerErr := s.GetTaintDecision(decisionID)
	require.NoError(t, headerErr)
	require.True(t, found)
	require.Equal(t, uint32(1), header.Materialized)
	require.Zero(t, header.Scanned)
	require.Zero(t, header.MaxDepth)
	require.Zero(t, header.FrontierHead)
	require.Equal(t, uint64(1), header.FrontierTail)
	return decisionID
}

type taintTopologyExpansionTrace struct {
	calls         int
	maxUnits      int
	cursorResumes int
	first         taint.Header
}

func expandTaintTopology(t *testing.T, s *BadgerStore, decisionID string, budget int) (taint.Header, taintTopologyExpansionTrace) {
	t.Helper()
	var trace taintTopologyExpansionTrace
	for attempt := 0; attempt < 1024; attempt++ {
		before, beforeFound, beforeErr := s.GetTaintDecision(decisionID)
		require.NoError(t, beforeErr)
		require.True(t, beforeFound)
		revisionBefore, revisionBeforeErr := s.TaintRevision()
		require.NoError(t, revisionBeforeErr)
		result, expandErr := s.ExpandTaintDecision("sage", decisionID, budget)
		require.NoError(t, expandErr)
		header, found, headerErr := s.GetTaintDecision(decisionID)
		require.NoError(t, headerErr)
		require.True(t, found)
		revisionAfter, revisionAfterErr := s.TaintRevision()
		require.NoError(t, revisionAfterErr)
		trace.calls++
		trace.maxUnits = max(trace.maxUnits, result.Units)
		if trace.calls == 1 {
			trace.first = header
		}
		require.Positive(t, result.Units)
		require.LessOrEqual(t, result.Units, budget, "every actual expansion fits its supplied execution budget")
		require.LessOrEqual(t, result.Materialized, taint.MaxNewMembershipsPerExec)
		require.Equal(t, result.Materialized, result.Enqueued)
		require.GreaterOrEqual(t, header.Scanned, before.Scanned)
		require.GreaterOrEqual(t, header.Materialized, before.Materialized)
		require.LessOrEqual(t, header.Materialized, uint32(taint.MaxExpansionMemberships))
		require.Equal(t, uint32(result.Scanned), header.Scanned-before.Scanned)
		require.Equal(t, uint32(result.Materialized), header.Materialized-before.Materialized)
		require.GreaterOrEqual(t, header.FrontierHead, before.FrontierHead)
		require.GreaterOrEqual(t, header.FrontierTail, before.FrontierTail)
		require.Equal(t, uint64(result.Enqueued), header.FrontierTail-before.FrontierTail)
		require.LessOrEqual(t, header.FrontierHead, header.FrontierTail)
		require.Equal(t, header.Status, result.Status)
		if result.Changed {
			require.NotEqual(t, before, header)
			require.Equal(t, revisionBefore+1, revisionAfter)
		} else {
			require.Equal(t, before, header)
			require.Equal(t, revisionBefore, revisionAfter)
		}
		if before.ParentRefID != "" && before.ParentRefID == header.ParentRefID &&
			before.LastEdge != "" && result.Scanned > 0 {
			require.True(t, header.LastEdge > before.LastEdge, "resumption must strictly advance the raw edge cursor")
			trace.cursorResumes++
		}
		if header.Status == taint.StatusComplete || header.Status == taint.StatusWorkExhausted ||
			(header.Status == taint.StatusDepthExhausted && header.FrontierHead == header.FrontierTail && header.ParentRefID == "") {
			return header, trace
		}
		require.True(t, result.Changed, "nonterminal expansion must make durable progress")
	}
	t.Fatal("bounded expansion did not reach its topology boundary in 1024 executions")
	return taint.Header{}, trace
}

func assertTaintTopologyMemberships(t *testing.T, s *BadgerStore, decisionID string, refs []tx.ParentRef, present int, linked bool) {
	t.Helper()
	require.Equal(t, present, countTaintPrefix(t, s, taint.DecisionIndexPrefix(decisionID)))
	require.NoError(t, s.view(func(reader *badger.Txn) error {
		for i, ref := range refs {
			item, memberErr := reader.Get(taint.MembershipKey(ref.MemoryID, decisionID))
			index, indexErr := reader.Get(taint.DecisionIndexKey(decisionID, ref.MemoryID))
			if i >= present {
				require.ErrorIs(t, memberErr, badger.ErrKeyNotFound)
				require.ErrorIs(t, indexErr, badger.ErrKeyNotFound)
				continue
			}
			require.NoError(t, memberErr, "membership %d", i)
			require.NoError(t, indexErr, "decision index %d", i)
			raw, valueErr := item.ValueCopy(nil)
			require.NoError(t, valueErr)
			membership, decodeErr := taint.DecodeMembership(raw)
			require.NoError(t, decodeErr)
			want := taint.Membership{Generation: 1, Origin: taint.OriginExpand, Depth: 1}
			if i == 0 {
				want.Origin = taint.OriginTarget
				want.Depth = 0
			} else if linked {
				want.Depth = uint16(i) // Only the 0..64 materialized linked nodes reach here.
			}
			require.Equal(t, want, membership, "membership %d", i)
			require.Zero(t, index.ValueSize())
		}
		return nil
	}))
}

func assertTaintTopologyCoverage(t *testing.T, s *BadgerStore, decisionID string, root tx.ParentRef, complete bool) {
	t.Helper()
	revision, gotComplete, coverageErr := s.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
	require.NoError(t, coverageErr)
	wantRevision, revisionErr := s.TaintRevision()
	require.NoError(t, revisionErr)
	require.Equal(t, wantRevision, revision)
	require.Equal(t, complete, gotComplete)
	verdict, classifyErr := s.ClassifyMemoryTaint(root.MemoryID)
	require.NoError(t, classifyErr)
	require.Equal(t, TaintServingTainted, verdict.Outcome)
	require.Equal(t, []string{decisionID}, verdict.DecisionIDs)
	require.Equal(t, complete, verdict.CoverageComplete,
		"the target has exact leaf provenance, so incompleteness comes from the real decision status")
}

func assertTaintTopologyTerminalReplay(t *testing.T, s *BadgerStore, decisionID string, status taint.Status) {
	t.Helper()
	before := readTaintValue(t, s, taint.DecisionKey(decisionID))
	revisionBefore, beforeErr := s.TaintRevision()
	require.NoError(t, beforeErr)
	result, expandErr := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, expandErr)
	require.Equal(t, TaintExpandResult{Units: 1, Status: status}, result)
	require.Equal(t, before, readTaintValue(t, s, taint.DecisionKey(decisionID)))
	revisionAfter, afterErr := s.TaintRevision()
	require.NoError(t, afterErr)
	require.Equal(t, revisionBefore, revisionAfter)
}
