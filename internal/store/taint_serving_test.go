package store

import (
	"crypto/sha256"
	"fmt"
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
	require.False(t, verdict.CoverageComplete)
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
	require.True(t, verdict.CoverageComplete)

	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	verdict, err = s.ClassifyMemoryTaint(grandchild.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.False(t, verdict.CoverageComplete)
	require.True(t, verdict.LineageMissing)
	_, disposition, err = s.ClassifyMemoryProjection(grandchild)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
}

func TestClassifyMemoryTaintDistinguishesKnownRootFromMissingLineage(t *testing.T) {
	s := openTaintStore(t)
	root := testTaintParent("provenance-root")
	child := testTaintParent("provenance-child")
	require.NoError(t, s.SetMemoryHash(root.MemoryID, root.ContentHash[:], "committed"))
	require.NoError(t, s.SetMemoryHash(child.MemoryID, child.ContentHash[:], "committed"))
	require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{root}))
	signer := testAppV30Signer("a")

	verdict, err := s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.False(t, verdict.CoverageComplete)
	require.True(t, verdict.LineageMissing)

	for _, ref := range []tx.ParentRef{root, child} {
		require.NoError(t, s.SetAppV30Provenance(
			ref.MemoryID,
			[]AppV30ProvenanceSigner{signer},
		))
	}
	verdict, err = s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.True(t, verdict.CoverageComplete)
	require.False(t, verdict.LineageMissing)
}

func TestClassifyMemoryTaintExposesDepthAndWorkExhaustion(t *testing.T) {
	for _, status := range []taint.Status{
		taint.StatusDepthExhausted,
		taint.StatusWorkExhausted,
	} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			s := openTaintStore(t)
			target := testTaintParent("terminal-target")
			decisionID := strings.Repeat("f", taint.DecisionIDBytes)
			created, err := s.ApplyTaintDecision(
				"sage",
				decisionID,
				target,
				sha256.Sum256([]byte("terminal payload")),
			)
			require.NoError(t, err)
			require.True(t, created)
			header, found, err := s.GetTaintDecision(decisionID)
			require.NoError(t, err)
			require.True(t, found)
			header.Status = status
			encoded, err := taint.EncodeHeader(header)
			require.NoError(t, err)
			require.NoError(t, s.update(func(txn *badger.Txn) error {
				return s.txnSet(txn, taint.DecisionKey(decisionID), encoded)
			}))

			verdict, err := s.ClassifyMemoryTaint(target.MemoryID)
			require.NoError(t, err)
			require.Equal(t, TaintServingTainted, verdict.Outcome)
			require.False(t, verdict.CoverageComplete)
			require.Equal(t, status == taint.StatusDepthExhausted, verdict.DepthExhausted)
			require.Equal(t, status == taint.StatusWorkExhausted, verdict.WorkExhausted)
		})
	}
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

func TestTaintServingCoverageSkipsRetiredDecisionHistory(t *testing.T) {
	s := openTaintStore(t)
	for i := 0; i < 3; i++ {
		target := testTaintParent("retired-target-" + string(rune('a'+i)))
		decisionID := strings.Repeat(string(rune('1'+i)), taint.DecisionIDBytes)
		created, err := s.ApplyTaintDecision(
			"sage",
			decisionID,
			target,
			sha256.Sum256([]byte("retired payload")),
		)
		require.NoError(t, err)
		require.True(t, created)
		changed, err := s.ClearTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, changed)
		for attempts := 0; attempts < 3; attempts++ {
			cleaned, err := s.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
			require.NoError(t, err)
			if cleaned.Status == taint.StatusClean {
				break
			}
		}
		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, taint.StatusClean, header.Status)
	}

	activeTarget := testTaintParent("active-target")
	activeDecision := strings.Repeat("a", taint.DecisionIDBytes)
	created, err := s.ApplyTaintDecision(
		"sage",
		activeDecision,
		activeTarget,
		sha256.Sum256([]byte("active payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	_, err = s.ExpandTaintDecision("sage", activeDecision, taint.PerExecutionUnitBudget)
	require.NoError(t, err)

	complete, err := s.TaintServingCoverageComplete()
	require.NoError(t, err)
	require.True(t, complete)
	_, memoryIDs, err := s.SnapshotEffectiveTaintMemberships()
	require.NoError(t, err)
	require.Equal(t, []string{activeTarget.MemoryID}, memoryIDs)
}
