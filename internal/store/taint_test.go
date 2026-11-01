package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func testTaintDecisionID(label string) string {
	digest := sha256.Sum256([]byte("decision:" + label))
	return hex.EncodeToString(digest[:])
}

func testTaintPayloadHash(label string) [sha256.Size]byte {
	return sha256.Sum256([]byte("payload:" + label))
}

func testTaintParent(memoryID string) tx.ParentRef {
	return tx.ParentRef{
		SourceChainID: "sage",
		MemoryID:      memoryID,
		ContentHash:   sha256.Sum256([]byte("content:" + memoryID)),
	}
}

func openTaintStore(t *testing.T) *BadgerStore {
	t.Helper()
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	return s
}

func readTaintValue(t *testing.T, s *BadgerStore, key []byte) []byte {
	t.Helper()
	var value []byte
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	}))
	return value
}

func TestApplyTaintDecisionWritesCanonicalInitialState(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("initial")
	payloadHash := testTaintPayloadHash("initial")
	target := testTaintParent("target")

	created, err := s.ApplyTaintDecision("sage", decisionID, target, payloadHash)
	require.NoError(t, err)
	assert.True(t, created)

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, taint.Header{
		Action:       taint.ActionApply,
		Status:       taint.StatusExpanding,
		Generation:   1,
		FrontierTail: 1,
		Materialized: 1,
		PayloadHash:  payloadHash,
	}, header)

	membership, err := taint.DecodeMembership(
		readTaintValue(t, s, taint.MembershipKey(target.MemoryID, decisionID)),
	)
	require.NoError(t, err)
	assert.Equal(t, taint.Membership{
		Generation: 1,
		Depth:      0,
		Origin:     taint.OriginTarget,
	}, membership)
	assert.Empty(t, readTaintValue(t, s, taint.DecisionIndexKey(decisionID, target.MemoryID)))
	assert.Empty(t, readTaintValue(t, s, taint.ActiveDecisionKey(decisionID)))

	targetRefID, err := tx.ParentRefIDHex(target)
	require.NoError(t, err)
	queueEntry, err := taint.DecodeQueueEntry(
		readTaintValue(t, s, taint.FrontierKey(decisionID, 1, 0)),
	)
	require.NoError(t, err)
	assert.Equal(t, taint.QueueEntry{
		ParentRefID: targetRefID,
		MemoryID:    target.MemoryID,
		Depth:       0,
	}, queueEntry)

	decisions, err := s.GetEffectiveTaintDecisions(target.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, []string{decisionID}, decisions)

	count, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
	revision, err := s.TaintRevision()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), revision)
}

func TestApplyTaintDecisionReplayAndConflict(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("replay")
	payloadHash := testTaintPayloadHash("replay")
	target := testTaintParent("target")

	created, err := s.ApplyTaintDecision("sage", decisionID, target, payloadHash)
	require.NoError(t, err)
	require.True(t, created)

	created, err = s.ApplyTaintDecision("sage", decisionID, target, payloadHash)
	require.NoError(t, err)
	assert.False(t, created)

	created, err = s.ApplyTaintDecision(
		"sage", decisionID, target, testTaintPayloadHash("different"),
	)
	assert.False(t, created)
	require.ErrorIs(t, err, ErrTaintConflict)

	count, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)
	revision, err := s.TaintRevision()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), revision)
}

func TestTaintDecisionCapacityIsNotReleasedBeforeCleanup(t *testing.T) {
	s := openTaintStore(t)
	decisionIDs := make([]string, 0, taint.MaxActiveDecisions)
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		decisionID := testTaintDecisionID(fmt.Sprintf("capacity-%02d", i))
		decisionIDs = append(decisionIDs, decisionID)
		created, err := s.ApplyTaintDecision(
			"sage",
			decisionID,
			testTaintParent(fmt.Sprintf("target-%02d", i)),
			testTaintPayloadHash(fmt.Sprintf("capacity-%02d", i)),
		)
		require.NoError(t, err)
		require.True(t, created)
	}

	_, err := s.ApplyTaintDecision(
		"sage",
		testTaintDecisionID("capacity-overflow"),
		testTaintParent("target-overflow"),
		testTaintPayloadHash("capacity-overflow"),
	)
	require.ErrorIs(t, err, ErrTaintCapacity)

	changed, err := s.ClearTaintDecision(decisionIDs[0])
	require.NoError(t, err)
	require.True(t, changed)

	_, err = s.ApplyTaintDecision(
		"sage",
		testTaintDecisionID("capacity-still-full"),
		testTaintParent("target-still-full"),
		testTaintPayloadHash("capacity-still-full"),
	)
	require.ErrorIs(t, err, ErrTaintCapacity)

	count, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(taint.MaxActiveDecisions), count)

	for attempts := 0; attempts < 3; attempts++ {
		header, found, err := s.GetTaintDecision(decisionIDs[0])
		require.NoError(t, err)
		require.True(t, found)
		if header.Status == taint.StatusClean {
			break
		}
		_, err = s.CleanupTaintDecision(
			decisionIDs[0],
			taint.PerExecutionUnitBudget,
		)
		require.NoError(t, err)
	}
	count, err = s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(taint.MaxActiveDecisions-1), count)

	created, err := s.ApplyTaintDecision(
		"sage",
		testTaintDecisionID("capacity-released"),
		testTaintParent("target-released"),
		testTaintPayloadHash("capacity-released"),
	)
	require.NoError(t, err)
	assert.True(t, created)
}

func TestClearTaintDecisionInvalidatesGenerationAndReplays(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("clear")
	payloadHash := testTaintPayloadHash("clear")
	target := testTaintParent("parent")
	created, err := s.ApplyTaintDecision("sage", decisionID, target, payloadHash)
	require.NoError(t, err)
	require.True(t, created)

	inherited, err := s.InheritTaintMemberships(
		"sage", "child", []tx.ParentRef{target},
	)
	require.NoError(t, err)
	assert.Equal(t, InheritedTaintResult{Decisions: 1, Units: 6}, inherited)

	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)

	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, taint.StatusCleaning, header.Status)
	assert.Equal(t, uint64(2), header.Generation)
	assert.Equal(t, taint.CleanupMembers, header.CleanupPhase)
	assert.Empty(t, header.ParentRefID)
	assert.Empty(t, header.ParentMemoryID)
	assert.Zero(t, header.ParentDepth)
	assert.Empty(t, header.LastEdge)
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		found, err := keyExistsTxn(txn, taint.ActiveDecisionKey(decisionID))
		if err != nil {
			return err
		}
		assert.False(t, found, "clear must immediately withdraw the active-decision index")
		return nil
	}))

	for _, memoryID := range []string{target.MemoryID, "child"} {
		decisions, err := s.GetEffectiveTaintDecisions(memoryID)
		require.NoError(t, err)
		assert.Empty(t, decisions)
	}

	revisionBeforeReplay, err := s.TaintRevision()
	require.NoError(t, err)
	changed, err = s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	assert.False(t, changed)
	revisionAfterReplay, err := s.TaintRevision()
	require.NoError(t, err)
	assert.Equal(t, revisionBeforeReplay, revisionAfterReplay)

	created, err = s.ApplyTaintDecision("sage", decisionID, target, payloadHash)
	require.NoError(t, err)
	assert.False(t, created)
	decisions, err := s.GetEffectiveTaintDecisions(target.MemoryID)
	require.NoError(t, err)
	assert.Empty(t, decisions)

	count, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), count)

	_, err = s.ClearTaintDecision(testTaintDecisionID("unknown"))
	require.ErrorIs(t, err, ErrTaintNotActive)
}

func TestInheritTaintMembershipsUsesEffectiveParentDecisions(t *testing.T) {
	s := openTaintStore(t)
	parentA := testTaintParent("parent-a")
	parentB := testTaintParent("parent-b")
	decisionA := testTaintDecisionID("inherit-a")
	decisionB := testTaintDecisionID("inherit-b")
	decisionC := testTaintDecisionID("inherit-c")

	for _, item := range []struct {
		ref        tx.ParentRef
		decisionID string
	}{
		{parentA, decisionA},
		{parentA, decisionB},
		{parentB, decisionC},
	} {
		created, err := s.ApplyTaintDecision(
			"sage",
			item.decisionID,
			item.ref,
			testTaintPayloadHash(item.decisionID),
		)
		require.NoError(t, err)
		require.True(t, created)
	}
	membership, err := taint.EncodeMembership(taint.Membership{
		Generation: 1,
		Depth:      0,
		Origin:     taint.OriginTarget,
	})
	require.NoError(t, err)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(
			txn,
			taint.MembershipKey(parentB.MemoryID, decisionB),
			membership,
		)
	}))

	changed, err := s.ClearTaintDecision(decisionA)
	require.NoError(t, err)
	require.True(t, changed)
	parents := []tx.ParentRef{parentA, parentB}
	tx.SortParentRefs(parents)
	revisionBefore, err := s.TaintRevision()
	require.NoError(t, err)
	preflight, err := s.ValidateTaintInheritance("sage", "child", parents)
	require.NoError(t, err)
	assert.Equal(t, InheritedTaintResult{Decisions: 2, Units: 14}, preflight)
	invisible, err := s.GetEffectiveTaintDecisions("child")
	require.NoError(t, err)
	assert.Empty(t, invisible)
	revisionAfterPreflight, err := s.TaintRevision()
	require.NoError(t, err)
	assert.Equal(t, revisionBefore, revisionAfterPreflight)

	result, err := s.InheritTaintMemberships("sage", "child", parents)
	require.NoError(t, err)
	assert.Equal(t, InheritedTaintResult{Decisions: 2, Units: 14}, result)

	decisions, err := s.GetEffectiveTaintDecisions("child")
	require.NoError(t, err)
	wantDecisions := []string{decisionB, decisionC}
	sort.Strings(wantDecisions)
	assert.Equal(t, wantDecisions, decisions)
	for _, decisionID := range decisions {
		membership, err := taint.DecodeMembership(
			readTaintValue(t, s, taint.MembershipKey("child", decisionID)),
		)
		require.NoError(t, err)
		assert.Equal(t, taint.Membership{
			Generation: 1,
			Depth:      0,
			Origin:     taint.OriginAdmit,
		}, membership)
		assert.Empty(t, readTaintValue(t, s, taint.DecisionIndexKey(decisionID, "child")))
	}
	revisionAfter, err := s.TaintRevision()
	require.NoError(t, err)
	assert.Equal(t, revisionBefore+1, revisionAfter)
}

func TestSameConsensusTransactionAdmitsNewTaintedParentAndChild(t *testing.T) {
	base := openTaintStore(t)
	parent := testTaintParent("same-block-parent")
	child := testTaintParent("same-block-child")
	decisionID := testTaintDecisionID("same-block")
	scoped := base.BeginConsensusTransaction(nil)

	require.NoError(t, scoped.SetMemoryHash(
		parent.MemoryID,
		parent.ContentHash[:],
		"committed",
	))
	created, err := scoped.ApplyTaintDecision(
		"sage",
		decisionID,
		parent,
		testTaintPayloadHash("same-block"),
	)
	require.NoError(t, err)
	require.True(t, created)

	require.NoError(t, scoped.SetMemoryHash(
		child.MemoryID,
		child.ContentHash[:],
		"committed",
	))
	require.NoError(t, scoped.SetMemoryLineage(
		child.MemoryID,
		[]tx.ParentRef{parent},
	))
	inherited, err := scoped.InheritTaintMemberships(
		"sage",
		child.MemoryID,
		[]tx.ParentRef{parent},
	)
	require.NoError(t, err)
	require.Equal(t, 1, inherited.Decisions)

	stagedDecisions, err := scoped.GetEffectiveTaintDecisions(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, stagedDecisions)
	require.NoError(t, scoped.CommitConsensusTransaction())

	decisions, err := base.GetEffectiveTaintDecisions(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)
	lineage, err := base.GetMemoryLineage(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{parent}, lineage)
}

func TestInheritTaintMembershipsHonorsAdmissionBudgetBoundary(t *testing.T) {
	s := openTaintStore(t)
	parents := make([]tx.ParentRef, taint.MaxParents)
	for i := range parents {
		parents[i] = testTaintParent(fmt.Sprintf("parent-%02d", i))
	}
	tx.SortParentRefs(parents)

	decisionIDs := make([]string, 0, taint.MaxActiveDecisions)
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		decisionID := testTaintDecisionID(fmt.Sprintf("budget-%02d", i))
		decisionIDs = append(decisionIDs, decisionID)
		created, err := s.ApplyTaintDecision(
			"sage",
			decisionID,
			parents[0],
			testTaintPayloadHash(decisionID),
		)
		require.NoError(t, err)
		require.True(t, created)
	}

	membership, err := taint.EncodeMembership(taint.Membership{
		Generation: 1,
		Depth:      0,
		Origin:     taint.OriginTarget,
	})
	require.NoError(t, err)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		for _, parent := range parents[1:] {
			for _, decisionID := range decisionIDs {
				if err := s.txnSet(
					txn,
					taint.MembershipKey(parent.MemoryID, decisionID),
					membership,
				); err != nil {
					return err
				}
			}
		}
		return nil
	}))

	result, err := s.InheritTaintMemberships("sage", "child", parents)
	require.NoError(t, err)
	assert.Equal(t, InheritedTaintResult{
		Decisions: taint.MaxActiveDecisions,
		Units:     taint.MaxAdmissionTaintUnits,
	}, result)

	decisions, err := s.GetEffectiveTaintDecisions("child")
	require.NoError(t, err)
	sort.Strings(decisionIDs)
	assert.Equal(t, decisionIDs, decisions)
}

func TestTaintMutationsRespectConsensusTransactionIsolation(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("transaction")
	payloadHash := testTaintPayloadHash("transaction")
	target := testTaintParent("target")

	discarded := s.BeginConsensusTransaction(nil)
	created, err := discarded.ApplyTaintDecision(
		"sage", decisionID, target, payloadHash,
	)
	require.NoError(t, err)
	require.True(t, created)
	_, found, err := discarded.GetTaintDecision(decisionID)
	require.NoError(t, err)
	assert.True(t, found)
	_, found, err = s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	assert.False(t, found)
	discarded.DiscardConsensusTransaction()

	_, found, err = s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	assert.False(t, found)

	committed := s.BeginConsensusTransaction(nil)
	created, err = committed.ApplyTaintDecision(
		"sage", decisionID, target, payloadHash,
	)
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, committed.CommitConsensusTransaction())
	header, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, payloadHash, header.PayloadHash)
}

func TestTaintMutationPoisonsTransactionAfterPartialWrite(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("poisoned")
	payloadHash := testTaintPayloadHash("poisoned")
	target := testTaintParent("target")

	scoped := s.BeginConsensusTransaction(nil)
	scoped.writeFaultHook = func(attempt int) error {
		if attempt == 2 {
			return fmt.Errorf("injected write failure")
		}
		return nil
	}
	created, err := scoped.ApplyTaintDecision(
		"sage", decisionID, target, payloadHash,
	)
	assert.False(t, created)
	require.Error(t, err)
	require.Error(t, scoped.ConsensusTransactionError())
	require.Error(t, scoped.CommitConsensusTransaction())

	_, found, err := s.GetTaintDecision(decisionID)
	require.NoError(t, err)
	assert.False(t, found)
	count, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	assert.Zero(t, count)
}

func TestTaintStoreRejectsMalformedAndInvalidInputs(t *testing.T) {
	s := openTaintStore(t)
	target := testTaintParent("target")
	payloadHash := testTaintPayloadHash("malformed")

	_, err := s.ApplyTaintDecision("", testTaintDecisionID("bad-chain"), target, payloadHash)
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	_, err = s.ApplyTaintDecision("sage", "not-a-decision", target, payloadHash)
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	crossChainTarget := testTaintParent("other")
	crossChainTarget.SourceChainID = "other"
	_, err = s.ApplyTaintDecision(
		"sage", testTaintDecisionID("cross-chain"), crossChainTarget, payloadHash,
	)
	require.ErrorIs(t, err, ErrTaintStateMalformed)

	decisionID := testTaintDecisionID("malformed-membership")
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(
			txn,
			taint.MembershipKey(target.MemoryID, decisionID),
			make([]byte, 11),
		)
	}))
	_, err = s.GetEffectiveTaintDecisions(target.MemoryID)
	require.ErrorIs(t, err, ErrTaintStateMalformed)

	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, taint.RevisionKey(), []byte{1, 2, 3})
	}))
	_, err = s.TaintRevision()
	require.ErrorIs(t, err, ErrTaintStateMalformed)

	_, err = s.InheritTaintMemberships("sage", "child", []tx.ParentRef{
		{SourceChainID: "other", MemoryID: "parent"},
	})
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	_, err = s.InheritTaintMemberships("sage", strings.Repeat("c", taint.MaxMemoryIDBytes+1), []tx.ParentRef{
		testTaintParent("parent"),
	})
	require.ErrorIs(t, err, ErrTaintStateMalformed)
	_, err = s.InheritTaintMemberships("sage", "child", []tx.ParentRef{
		testTaintParent("child"),
	})
	require.ErrorIs(t, err, ErrTaintStateMalformed)
}

func TestTaintKeysParticipateInCanonicalProjectionHealth(t *testing.T) {
	for _, key := range [][]byte{
		taint.DecisionKey(testTaintDecisionID("projection")),
		TaintDecisionScopeKey(testTaintDecisionID("projection")),
		taint.ActiveDecisionKey(testTaintDecisionID("projection")),
		taint.MembershipKey("memory", testTaintDecisionID("projection")),
		taint.DecisionIndexKey(testTaintDecisionID("projection"), "memory"),
		taint.FrontierKey(testTaintDecisionID("projection"), 1, 0),
		taint.RevisionKey(),
		taint.ActiveCountKey(),
	} {
		assert.True(t, isCanonicalMemoryProjectionKey(key), string(key))
	}
	assert.False(t, isCanonicalMemoryProjectionKey([]byte("taintother")))
}

func TestTaintMutationsAdvanceCanonicalProjectionRevision(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("projection-revision")
	target := testTaintParent("projection-target")
	before := s.CanonicalMemoryProjectionRevision()
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash("projection-revision"),
	)
	require.NoError(t, err)
	require.True(t, created)
	afterApply := s.CanonicalMemoryProjectionRevision()
	assert.Greater(t, afterApply, before)

	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	assert.Greater(t, s.CanonicalMemoryProjectionRevision(), afterApply)
}
