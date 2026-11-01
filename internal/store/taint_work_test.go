package store

import (
	"bytes"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func seedTaintChild(
	t *testing.T,
	s *BadgerStore,
	parent tx.ParentRef,
	childID string,
) tx.ParentRef {
	t.Helper()
	child := testTaintParent(childID)
	require.NoError(t, s.SetMemoryHash(
		childID,
		child.ContentHash[:],
		"committed",
	))
	require.NoError(t, s.SetMemoryLineage(childID, []tx.ParentRef{parent}))
	return child
}

func countTaintPrefix(t *testing.T, s *BadgerStore, prefix []byte) int {
	t.Helper()
	count := 0
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.Prefix = prefix
		options.PrefetchValues = false
		it := txn.NewIterator(options)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	}))
	return count
}

func TestExpandTaintDecisionMaterializesBreadthFirstLineage(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("expand-tree")
	target := testTaintParent("target")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
	)
	require.NoError(t, err)
	require.True(t, created)

	childA := seedTaintChild(t, s, target, "child-a")
	childB := seedTaintChild(t, s, target, "child-b")
	grandchild := seedTaintChild(t, s, childA, "grandchild")

	result, err := s.ExpandTaintDecision("sage", decisionID, 128)
	require.NoError(t, err)
	assert.True(t, result.Changed)
	assert.Equal(t, taint.StatusComplete, result.Status)
	assert.Equal(t, 3, result.Scanned)
	assert.Equal(t, 3, result.Materialized)
	assert.Equal(t, 3, result.Enqueued)
	assert.Equal(t, 31, result.Units)

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, taint.StatusComplete, header.Status)
	assert.Equal(t, uint64(4), header.FrontierHead)
	assert.Equal(t, uint64(4), header.FrontierTail)
	assert.Equal(t, uint32(3), header.Scanned)
	assert.Equal(t, uint32(4), header.Materialized)
	assert.Equal(t, uint32(2), header.MaxDepth)
	assert.Empty(t, header.ParentRefID)
	assert.Empty(t, header.ParentMemoryID)
	assert.Empty(t, header.LastEdge)

	for _, memoryID := range []string{
		target.MemoryID,
		childA.MemoryID,
		childB.MemoryID,
		grandchild.MemoryID,
	} {
		decisions, err := s.GetEffectiveTaintDecisions(memoryID)
		require.NoError(t, err)
		assert.Equal(t, []string{decisionID}, decisions)
	}

	membership, err := taint.DecodeMembership(
		readTaintValue(t, s, taint.MembershipKey(grandchild.MemoryID, decisionID)),
	)
	require.NoError(t, err)
	assert.Equal(t, taint.Membership{
		Generation: 1,
		Depth:      2,
		Origin:     taint.OriginExpand,
	}, membership)
	assert.Zero(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))
}

func TestExpandTaintDecisionResumesPastLargeFanout(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("expand-fanout")
	target := testTaintParent("fanout-target")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
	)
	require.NoError(t, err)
	require.True(t, created)

	children := make([]tx.ParentRef, 0, 30)
	for i := 0; i < 30; i++ {
		children = append(children, seedTaintChild(
			t,
			s,
			target,
			fmt.Sprintf("fanout-child-%02d", i),
		))
	}

	first, err := s.ExpandTaintDecision("sage", decisionID, 50)
	require.NoError(t, err)
	assert.True(t, first.Changed)
	assert.Equal(t, taint.StatusExpanding, first.Status)
	assert.Positive(t, first.Materialized)
	assert.Less(t, first.Materialized, len(children))
	assert.Equal(t, 47, first.Units)

	for attempts := 0; attempts < 10; attempts++ {
		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		if header.Status == taint.StatusComplete {
			break
		}
		result, err := s.ExpandTaintDecision(
			"sage",
			decisionID,
			taint.PerExecutionUnitBudget,
		)
		require.NoError(t, err)
		assert.LessOrEqual(t, result.Units, taint.PerExecutionUnitBudget)
	}

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, taint.StatusComplete, header.Status)
	assert.Equal(t, uint64(31), header.FrontierHead)
	assert.Equal(t, uint64(31), header.FrontierTail)
	assert.Equal(t, uint32(30), header.Scanned)
	assert.Equal(t, uint32(31), header.Materialized)
	for _, child := range children {
		decisions, err := s.GetEffectiveTaintDecisions(child.MemoryID)
		require.NoError(t, err)
		assert.Equal(t, []string{decisionID}, decisions)
	}
	assert.Zero(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))
}

func TestExpandTaintDecisionUnitBudgetBindsBeforeMembershipCap(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("expand-unit-bound")
	target := testTaintParent("unit-bound-target")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash(decisionID),
	)
	require.NoError(t, err)
	require.True(t, created)

	for i := 0; i < 220; i++ {
		seedTaintChild(
			t,
			s,
			target,
			fmt.Sprintf("unit-bound-child-%03d", i),
		)
	}

	result, err := s.ExpandTaintDecision(
		"sage",
		decisionID,
		taint.PerExecutionUnitBudget,
	)
	require.NoError(t, err)
	assert.Equal(t, 169, result.Materialized)
	assert.Less(t, result.Materialized, taint.MaxNewMembershipsPerExec)
	assert.Equal(t, 1019, result.Units)
	assert.Equal(t, taint.StatusExpanding, result.Status)

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, uint32(170), header.Materialized)
	assert.Equal(t, uint64(170), header.FrontierTail)
	assert.Equal(t, uint64(1), header.FrontierHead)
	assert.NotEmpty(t, header.ParentRefID)
}

func TestExpandTaintDecisionStopsAtDepthAndWorkCaps(t *testing.T) {
	t.Run("depth", func(t *testing.T) {
		s := openTaintStore(t)
		decisionID := testTaintDecisionID("expand-depth")
		target := testTaintParent("depth-target")
		require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
		created, err := s.ApplyTaintDecision(
			"sage", decisionID, target, testTaintPayloadHash(decisionID),
		)
		require.NoError(t, err)
		require.True(t, created)
		child := seedTaintChild(t, s, target, "depth-child")

		targetRefID, err := tx.ParentRefIDHex(target)
		require.NoError(t, err)
		queueValue, err := taint.EncodeQueueEntry(taint.QueueEntry{
			ParentRefID: targetRefID,
			MemoryID:    target.MemoryID,
			Depth:       taint.MaxDepth,
		})
		require.NoError(t, err)
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return s.txnSet(txn, taint.FrontierKey(decisionID, 1, 0), queueValue)
		}))

		result, err := s.ExpandTaintDecision("sage", decisionID, 64)
		require.NoError(t, err)
		assert.Equal(t, taint.StatusDepthExhausted, result.Status)
		assert.Equal(t, 1, result.Scanned)
		assert.Zero(t, result.Materialized)
		revision, err := s.TaintRevision()
		require.NoError(t, err)
		replayed, err := s.ExpandTaintDecision("sage", decisionID, 64)
		require.NoError(t, err)
		assert.False(t, replayed.Changed)
		assert.Equal(t, 1, replayed.Units)
		revisionAfter, err := s.TaintRevision()
		require.NoError(t, err)
		assert.Equal(t, revision, revisionAfter)

		decisions, err := s.GetEffectiveTaintDecisions(child.MemoryID)
		require.NoError(t, err)
		assert.Empty(t, decisions)
	})

	t.Run("work", func(t *testing.T) {
		s := openTaintStore(t)
		decisionID := testTaintDecisionID("expand-work")
		target := testTaintParent("work-target")
		require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
		created, err := s.ApplyTaintDecision(
			"sage", decisionID, target, testTaintPayloadHash(decisionID),
		)
		require.NoError(t, err)
		require.True(t, created)
		seedTaintChild(t, s, target, "work-child")

		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		header.Materialized = taint.MaxExpansionMemberships
		encoded, err := taint.EncodeHeader(header)
		require.NoError(t, err)
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return s.txnSet(txn, taint.DecisionKey(decisionID), encoded)
		}))

		result, err := s.ExpandTaintDecision("sage", decisionID, 64)
		require.NoError(t, err)
		assert.Equal(t, taint.StatusWorkExhausted, result.Status)
		assert.Zero(t, result.Materialized)
		replayed, err := s.ExpandTaintDecision("sage", decisionID, 64)
		require.NoError(t, err)
		assert.False(t, replayed.Changed)
		assert.Equal(t, taint.StatusWorkExhausted, replayed.Status)

		decisions, err := s.GetEffectiveTaintDecisions("work-child")
		require.NoError(t, err)
		assert.Empty(t, decisions)
	})
}

func TestExpandTaintDecisionRollsBackMissingChildIdentity(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("expand-missing-child")
	target := testTaintParent("missing-target")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	created, err := s.ApplyTaintDecision(
		"sage", decisionID, target, testTaintPayloadHash(decisionID),
	)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, s.SetMemoryLineage("missing-child", []tx.ParentRef{target}))
	revisionBefore, err := s.TaintRevision()
	require.NoError(t, err)

	_, err = s.ExpandTaintDecision("sage", decisionID, 64)
	require.ErrorIs(t, err, ErrTaintStateMalformed)

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, taint.StatusExpanding, header.Status)
	assert.Equal(t, uint64(0), header.FrontierHead)
	assert.Equal(t, uint64(1), header.FrontierTail)
	assert.Zero(t, header.Scanned)
	revisionAfter, err := s.TaintRevision()
	require.NoError(t, err)
	assert.Equal(t, revisionBefore, revisionAfter)
}

func TestCleanupTaintDecisionResumesAfterRestartAndReleasesSlot(t *testing.T) {
	path := t.TempDir()
	s, err := NewBadgerStore(path)
	require.NoError(t, err)

	decisionID := testTaintDecisionID("cleanup")
	otherDecisionID := testTaintDecisionID("cleanup-other")
	target := testTaintParent("cleanup-target")
	otherTarget := testTaintParent("cleanup-other-target")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], "committed"))
	require.NoError(t, s.SetMemoryHash(
		otherTarget.MemoryID,
		otherTarget.ContentHash[:],
		"committed",
	))
	for _, item := range []struct {
		decisionID string
		target     tx.ParentRef
	}{
		{decisionID, target},
		{otherDecisionID, otherTarget},
	} {
		created, err := s.ApplyTaintDecision(
			"sage",
			item.decisionID,
			item.target,
			testTaintPayloadHash(item.decisionID),
		)
		require.NoError(t, err)
		require.True(t, created)
	}
	for i := 0; i < 30; i++ {
		seedTaintChild(t, s, target, fmt.Sprintf("cleanup-child-%02d", i))
	}
	expanded, err := s.ExpandTaintDecision("sage", decisionID, 50)
	require.NoError(t, err)
	require.Positive(t, expanded.Materialized)
	require.Equal(t, taint.StatusExpanding, expanded.Status)
	otherMembershipBefore := readTaintValue(
		t,
		s,
		taint.MembershipKey(otherTarget.MemoryID, otherDecisionID),
	)

	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)

	first, err := s.CleanupTaintDecision(decisionID, 20)
	require.NoError(t, err)
	assert.True(t, first.Changed)
	assert.Equal(t, taint.CleanupMembers, first.Phase)
	assert.Positive(t, first.MembersDeleted)
	assert.Equal(t, 18, first.Units)

	for attempts := 0; attempts < 30; attempts++ {
		header, found, err := s.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		if header.CleanupPhase == taint.CleanupQueue {
			break
		}
		result, err := s.CleanupTaintDecision(
			decisionID,
			taint.PerExecutionUnitBudget,
		)
		require.NoError(t, err)
		assert.LessOrEqual(t, result.Units, taint.PerExecutionUnitBudget)
	}
	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.CleanupQueue, header.CleanupPhase)
	require.Greater(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)), 0)
	activeBeforeQueue, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), activeBeforeQueue)

	require.NoError(t, s.CloseBadger())
	s, err = NewBadgerStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })

	cleaned, err := s.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	assert.True(t, cleaned.Changed)
	assert.True(t, cleaned.ActiveReleased)
	assert.Equal(t, taint.StatusClean, cleaned.Status)
	assert.Equal(t, taint.CleanupNone, cleaned.Phase)
	assert.Equal(t, expanded.Materialized, cleaned.QueueDeleted)
	assert.Equal(
		t,
		taintFixedMutationUnits+
			expanded.Materialized*taintQueueCleanupUnits+
			taintActiveReleaseUnits,
		cleaned.Units,
	)

	assert.Zero(t, countTaintPrefix(t, s, taint.MembershipPrefix(target.MemoryID)))
	assert.Zero(t, countTaintPrefix(t, s, taint.DecisionIndexPrefix(decisionID)))
	assert.Zero(t, countTaintPrefix(t, s, taint.FrontierPrefix(decisionID)))

	active, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), active)
	otherDecisions, err := s.GetEffectiveTaintDecisions(otherTarget.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, []string{otherDecisionID}, otherDecisions)
	assert.Equal(
		t,
		otherMembershipBefore,
		readTaintValue(t, s, taint.MembershipKey(otherTarget.MemoryID, otherDecisionID)),
	)

	replayed, err := s.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	assert.False(t, replayed.Changed)
	assert.Equal(t, taint.StatusClean, replayed.Status)
}

func TestCleanupTaintDecisionRequiresClearedStateAndValidBudget(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("cleanup-validation")
	target := testTaintParent("cleanup-validation-target")
	created, err := s.ApplyTaintDecision(
		"sage", decisionID, target, testTaintPayloadHash(decisionID),
	)
	require.NoError(t, err)
	require.True(t, created)

	_, err = s.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
	require.ErrorIs(t, err, ErrTaintNotActive)
	_, err = s.CleanupTaintDecision(decisionID, 1)
	require.ErrorIs(t, err, ErrTaintBudget)
	_, err = s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget+1)
	require.ErrorIs(t, err, ErrTaintBudget)
	_, err = s.ExpandTaintDecision("", decisionID, taint.PerExecutionUnitBudget)
	require.ErrorIs(t, err, ErrTaintStateMalformed)
}

func TestTaintWorkKeysRemainCanonical(t *testing.T) {
	for _, key := range [][]byte{
		taint.FrontierKey(testTaintDecisionID("work-key"), 2, 3),
		taint.DecisionIndexKey(testTaintDecisionID("work-key"), "memory"),
	} {
		assert.True(t, bytes.HasPrefix(
			key,
			[]byte("taint"),
		))
		assert.True(t, isCanonicalMemoryProjectionKey(key))
	}
	assert.False(t, isCanonicalMemoryProjectionKey([]byte("lineage:unrelated")))
}
