package store

import (
	"crypto/sha256"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
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

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	header.Status = taint.StatusComplete
	encoded, err := taint.EncodeHeader(header)
	require.NoError(t, err)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, taint.DecisionKey(decisionID), encoded)
	}))
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

func TestTaintServingCoverageCompleteRequiresFinishedExpansion(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("coverage-target")
	decisionID := strings.Repeat("c", taint.DecisionIDBytes)
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		sha256.Sum256([]byte("coverage payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	complete, err := s.TaintServingCoverageComplete()
	require.NoError(t, err)
	require.False(t, complete)

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	header.Status = taint.StatusComplete
	encoded, err := taint.EncodeHeader(header)
	require.NoError(t, err)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, taint.DecisionKey(decisionID), encoded)
	}))
	complete, err = s.TaintServingCoverageComplete()
	require.NoError(t, err)
	require.True(t, complete)
}
