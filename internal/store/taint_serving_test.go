package store

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func servingMemoryRecord(memoryID, content string) *memory.MemoryRecord {
	contentHash := sha256.Sum256([]byte(content))
	return &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: "agent-a",
		Content:         content,
		ContentHash:     contentHash[:],
		MemoryType:      memory.TypeFact,
		DomainTag:       "general",
		Status:          memory.StatusCommitted,
	}
}

func TestClassifyMemoryTaintHoldsIncompleteAuthenticatedAncestor(t *testing.T) {
	s := openTaintStore(t)
	target := servingMemoryRecord("taint-target", "target content")
	child := servingMemoryRecord("taint-child", "child content")
	grandchild := servingMemoryRecord("taint-grandchild", "grandchild content")
	for _, record := range []*memory.MemoryRecord{target, child, grandchild} {
		require.NoError(t, s.SetMemoryHash(
			record.MemoryID,
			record.ContentHash,
			string(record.Status),
		))
	}
	targetRef := testTaintParent(target.MemoryID)
	copy(targetRef.ContentHash[:], target.ContentHash)
	childRef := testTaintParent(child.MemoryID)
	copy(childRef.ContentHash[:], child.ContentHash)
	require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{targetRef}))
	require.NoError(t, s.SetMemoryLineage(grandchild.MemoryID, []tx.ParentRef{childRef}))

	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		targetRef,
		sha256.Sum256([]byte("taint serving payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	verdict, err := s.ClassifyMemoryTaint(grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingPending, verdict.Outcome)
	require.Equal(t, []string{decisionID}, verdict.DecisionIDs)
	_, disposition, err := s.ClassifyMemoryProjection(grandchild)
	require.ErrorIs(t, err, ErrMemoryProjectionTaintPending)
	require.Equal(t, MemoryProjectionTaintPending, disposition)

	// Complete means expansion actually materialized every reachable child;
	// a raw status rewrite would not establish that serving invariant.
	expanded, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	verdict, err = s.ClassifyMemoryTaint(grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingTainted, verdict.Outcome)

	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	verdict, err = s.ClassifyMemoryTaint(grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	_, disposition, err = s.ClassifyMemoryProjection(grandchild)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
}

func TestMemoryProjectionPreservesTaintClassificationErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		want error
		seed func(*testing.T, *BadgerStore, *memory.MemoryRecord)
	}{
		{
			name: "malformed decision header",
			want: ErrTaintStateMalformed,
			seed: func(t *testing.T, s *BadgerStore, record *memory.MemoryRecord) {
				decisionID := testTaintDecisionID("malformed-serving-header")
				ref := testTaintParent(record.MemoryID)
				copy(ref.ContentHash[:], record.ContentHash)
				_, err := s.ApplyTaintDecision("sage", decisionID, ref, testTaintPayloadHash("serving-header"))
				require.NoError(t, err)
				require.NoError(t, s.SetRawForTest(taint.DecisionKey(decisionID), []byte("invalid header")))
			},
		},
		{
			name: "ancestor walk budget",
			want: ErrTaintServingWalkBudget,
			seed: func(t *testing.T, s *BadgerStore, record *memory.MemoryRecord) {
				childID := record.MemoryID
				for i := 0; i < taint.MaxServingAncestorWalk; i++ {
					parent := testTaintParent(fmt.Sprintf("serving-ancestor-%03d", i))
					require.NoError(t, s.SetMemoryLineage(childID, []tx.ParentRef{parent}))
					childID = parent.MemoryID
				}
				unrelated := testTaintParent("unrelated-incomplete-target")
				require.NoError(t, s.SetMemoryHash(unrelated.MemoryID, unrelated.ContentHash[:], string(memory.StatusCommitted)))
				created, err := s.ApplyTaintDecision("sage", testTaintDecisionID("unrelated-serving-budget"),
					unrelated, testTaintPayloadHash("unrelated-serving-budget"))
				require.NoError(t, err)
				require.True(t, created)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTaintStore(t)
			record := servingMemoryRecord("classification-error", "preserved content")
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			tc.seed(t, s, record)
			_, classifyErr := s.ClassifyMemoryTaint(record.MemoryID)
			require.ErrorIs(t, classifyErr, tc.want)

			_, projectionErr := s.ValidateMemoryProjection(record)
			require.ErrorIs(t, projectionErr, tc.want)
			require.NotErrorIs(t, projectionErr, ErrMemoryProjectionUnpublished,
				"an incomplete classification must not become a skippable record denial")

			batch, err := s.ClassifyMemoryProjections([]*memory.MemoryRecord{record})
			require.NoError(t, err)
			require.Len(t, batch, 1)
			require.ErrorIs(t, batch[0].Err, tc.want)
			require.NotErrorIs(t, batch[0].Err, ErrMemoryProjectionUnpublished)
		})
	}
}

func TestClassifyMemoryTaintZeroHeadersDeepLineage(t *testing.T) {
	for _, withHeader := range []bool{false, true} {
		t.Run(fmt.Sprintf("unrelated_incomplete_header_%t", withHeader), func(t *testing.T) {
			s := openTaintStore(t)
			record := servingMemoryRecord("zero-header-leaf", "zero-header content")
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			childID := record.MemoryID
			for i := 0; i < taint.MaxServingAncestorWalk; i++ {
				parent := testTaintParent(fmt.Sprintf("zero-header-parent-%03d", i))
				require.NoError(t, s.SetMemoryHash(parent.MemoryID, parent.ContentHash[:], string(memory.StatusCommitted)))
				require.NoError(t, s.SetMemoryLineage(childID, []tx.ParentRef{parent}))
				childID = parent.MemoryID
			}
			if withHeader {
				id := testTaintDecisionID("unrelated-incomplete")
				target := testTaintParent("unrelated-incomplete")
				require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], string(memory.StatusCommitted)))
				created, err := s.ApplyTaintDecision("sage", id, target, testTaintPayloadHash("unrelated-incomplete"))
				require.NoError(t, err)
				require.True(t, created)
				header, found, err := s.GetTaintDecision(id)
				require.NoError(t, err)
				require.True(t, found)
				require.True(t, header.Status.Incomplete())
				// A stale zero counter is not evidence that canonical headers are absent.
				require.NoError(t, s.SetRawForTest(taint.ActiveCountKey(), make([]byte, 8)))
			}
			active, err := s.ActiveTaintDecisionCount()
			require.NoError(t, err)
			require.Zero(t, active)
			verdict, err := s.ClassifyMemoryTaint(record.MemoryID)
			if withHeader {
				require.ErrorIs(t, err, ErrTaintServingWalkBudget)
				require.Equal(t, taint.MaxServingAncestorWalk, verdict.Nodes)
			} else {
				require.NoError(t, err)
				require.Equal(t, TaintServingClear, verdict.Outcome)
				require.Equal(t, 1, verdict.Nodes, "only the direct record is checked when no header exists")
			}
		})
	}
}

func TestClassifyMemoryTaintDirectDamageBeforeZeroHeaderFastPath(t *testing.T) {
	for _, damage := range []string{"malformed_membership", "missing_header", "malformed_header"} {
		t.Run(damage, func(t *testing.T) {
			s := openTaintStore(t)
			record := servingMemoryRecord("damaged-direct", "direct damage content")
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			id := testTaintDecisionID("damaged-direct")
			switch damage {
			case "malformed_membership":
				require.NoError(t, s.SetRawForTest(taint.MembershipKey(record.MemoryID, id), []byte{1}))
			case "missing_header":
				member, err := taint.EncodeMembership(taint.Membership{Generation: 1, Origin: taint.OriginTarget})
				require.NoError(t, err)
				require.NoError(t, s.SetRawForTest(taint.MembershipKey(record.MemoryID, id), member))
			case "malformed_header":
				target := testTaintParent(record.MemoryID)
				copy(target.ContentHash[:], record.ContentHash)
				created, err := s.ApplyTaintDecision("sage", id, target, testTaintPayloadHash("damaged-direct"))
				require.NoError(t, err)
				require.True(t, created)
				require.NoError(t, s.SetRawForTest(taint.DecisionKey(id), []byte("invalid header")))
			}
			_, err := s.ClassifyMemoryTaint(record.MemoryID)
			require.ErrorIs(t, err, ErrTaintStateMalformed)
		})
	}
}
