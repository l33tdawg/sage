package store

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func seedServingDeepRecord(t *testing.T, s *BadgerStore, label string) *memory.MemoryRecord {
	t.Helper()
	record := servingMemoryRecord(label+"-leaf", label+" content")
	require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	child := record.MemoryID
	for i := 0; i < taint.MaxServingAncestorWalk; i++ {
		parent := testTaintParent(fmt.Sprintf("%s-parent-%03d", label, i))
		require.NoError(t, s.SetMemoryHash(parent.MemoryID, parent.ContentHash[:], string(memory.StatusCommitted)))
		require.NoError(t, s.SetMemoryLineage(child, []tx.ParentRef{parent}))
		child = parent.MemoryID
	}
	return record
}

func seedServingIncompleteDecision(t *testing.T, s *BadgerStore, label string) string {
	t.Helper()
	target := testTaintParent(label + "-target")
	require.NoError(t, s.SetMemoryHash(target.MemoryID, target.ContentHash[:], string(memory.StatusCommitted)))
	id := testTaintDecisionID(label)
	created, err := s.ApplyTaintDecision("sage", id, target, testTaintPayloadHash(label))
	require.NoError(t, err)
	require.True(t, created)
	return id
}

func finishServingDecisionCleanup(t *testing.T, s *BadgerStore, id string) {
	t.Helper()
	for attempt := 0; attempt < 4; attempt++ {
		cleaned, err := s.CleanupTaintDecision(id, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		if cleaned.Status == taint.StatusClean {
			return
		}
	}
	t.Fatal("small decision did not finish cleanup within four calls")
}

func TestTaintServingCompleteCleaningAndCleanHistory(t *testing.T) {
	s := openTaintStore(t)
	record := seedServingDeepRecord(t, s, "history")
	id := seedServingIncompleteDecision(t, s, "history-unrelated")
	_, err := s.ClassifyMemoryTaint(record.MemoryID)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget)

	expanded, err := s.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	verdict, err := s.ClassifyMemoryTaint(record.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, verdict.Outcome)
	require.Equal(t, 1, verdict.Nodes)
	verdict, err = s.ClassifyMemoryTaint("history-unrelated-target")
	require.NoError(t, err)
	require.Equal(t, TaintServingTainted, verdict.Outcome,
		"direct effective membership is checked before the complete-history shortcut")

	changed, err := s.ClearTaintDecision(id)
	require.NoError(t, err)
	require.True(t, changed)
	header, found, err := s.GetTaintDecision(id)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusCleaning, header.Status)
	verdict, err = s.ClassifyMemoryTaint(record.MemoryID)
	require.NoError(t, err)
	require.Equal(t, 1, verdict.Nodes)
	finishServingDecisionCleanup(t, s, id)
	verdict, err = s.ClassifyMemoryTaint(record.MemoryID)
	require.NoError(t, err)
	require.Equal(t, 1, verdict.Nodes)
}

// These copies exercise only the bounded header decoder/cache, not recovery or
// materialization completeness. The template reaches Clean through real work.
func seedServingHeaderScanFixture(t *testing.T, s *BadgerStore, count int) {
	t.Helper()
	id := seedServingIncompleteDecision(t, s, "scan-template")
	expanded, err := s.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	_, err = s.ClearTaintDecision(id)
	require.NoError(t, err)
	finishServingDecisionCleanup(t, s, id)
	header, found, err := s.GetTaintDecision(id)
	require.NoError(t, err)
	require.True(t, found)
	raw, err := taint.EncodeHeader(header)
	require.NoError(t, err)
	for i := 1; i < count; i++ {
		require.NoError(t, s.SetRawForTest(taint.DecisionKey(testTaintDecisionID(fmt.Sprintf("scan-%04d", i))), raw))
	}
}

func TestTaintServingColdHeaderCapAndRequestAllowance(t *testing.T) {
	for _, count := range []int{taintServingHeaderReadLimit, taintServingHeaderReadLimit + 1} {
		t.Run(fmt.Sprintf("headers_%d", count), func(t *testing.T) {
			s := openTaintStore(t)
			seedServingHeaderScanFixture(t, s, count)
			scope := NewTaintServingReadScope()
			require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
				status, err := reader.incompleteTaintStatus(scope)
				require.NoError(t, err)
				want := taintIncompleteNone
				if count > taintServingHeaderReadLimit {
					want = taintIncompleteUnknown
				}
				require.Equal(t, want, status)
				require.Zero(t, scope.remaining)
				status, err = reader.incompleteTaintStatus(scope)
				require.NoError(t, err)
				require.Equal(t, want, status, "an exact-snapshot cache hit needs no cold allowance")
				return nil
			}))
		})
	}
	t.Run("partial request does not poison snapshot", func(t *testing.T) {
		s := openTaintStore(t)
		seedServingHeaderScanFixture(t, s, 2)
		require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
			status, err := reader.incompleteTaintStatus(&TaintServingReadScope{remaining: 1})
			require.NoError(t, err)
			require.Equal(t, taintIncompleteUnknown, status)
			_, cached := s.taintServingCache.lookup(s.db, reader.txn.ReadTs())
			require.False(t, cached)
			status, err = reader.incompleteTaintStatus(NewTaintServingReadScope())
			require.NoError(t, err)
			require.Equal(t, taintIncompleteNone, status)
			return nil
		}))
	})
	t.Run("same request across new snapshots", func(t *testing.T) {
		s := openTaintStore(t)
		seedServingHeaderScanFixture(t, s, 2)
		scope := &TaintServingReadScope{remaining: 3}
		for pass := 0; pass < 2; pass++ {
			if pass == 1 {
				require.NoError(t, s.SetRawForTest([]byte("serving-test-snapshot"), []byte{1}))
			}
			require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
				status, err := reader.incompleteTaintStatus(scope)
				require.NoError(t, err)
				if pass == 0 {
					require.Equal(t, taintIncompleteNone, status)
				} else {
					require.Equal(t, taintIncompleteUnknown, status)
				}
				return nil
			}))
		}
		require.Zero(t, scope.remaining)
	})
}

func TestTaintServingCacheSnapshotAndSpeculativeIsolation(t *testing.T) {
	s := openTaintStore(t)
	record := seedServingDeepRecord(t, s, "snapshot")
	require.NoError(t, s.withTaintServingReadSnapshot(func(old *BadgerStore) error {
		status, err := old.incompleteTaintStatus(NewTaintServingReadScope())
		require.NoError(t, err)
		require.Equal(t, taintIncompleteNone, status)
		working := s.BeginConsensusTransaction(nil)
		require.Equal(t, old.txn.ReadTs(), working.txn.ReadTs())
		seedServingIncompleteDecision(t, working, "speculative-unrelated")
		_, err = working.ClassifyMemoryTaint(record.MemoryID)
		require.ErrorIs(t, err, ErrTaintServingWalkBudget,
			"speculative writes must not reuse the base snapshot's proven-none cache")
		working.DiscardConsensusTransaction()
		verdict, err := s.ClassifyMemoryTaint(record.MemoryID)
		require.NoError(t, err)
		require.Equal(t, TaintServingClear, verdict.Outcome)
		seedServingIncompleteDecision(t, s, "committed-unrelated")
		status, err = old.incompleteTaintStatus(NewTaintServingReadScope())
		require.NoError(t, err)
		require.Equal(t, taintIncompleteNone, status, "the older pinned view remains coherent")
		return nil
	}))
	_, err := s.ClassifyMemoryTaint(record.MemoryID)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget, "a later committed snapshot must be rescanned")
}

func TestTaintServingCacheBoundUpgradeAndConcurrentScope(t *testing.T) {
	s := openTaintStore(t)
	c := s.taintServingCache
	for ts := uint64(1); ts <= 6; ts++ {
		c.publish(s.db, ts, taintIncompleteUnknown)
	}
	require.Len(t, c.entries, taintServingCacheEntries)
	_, found := c.lookup(s.db, 1)
	require.False(t, found)
	c.publish(s.db, 6, taintIncompleteNone)
	c.publish(s.db, 6, taintIncompleteUnknown)
	status, found := c.lookup(s.db, 6)
	require.True(t, found)
	require.Equal(t, taintIncompleteNone, status, "a capped scan cannot overwrite a full proof")
	other := openTaintStore(t)
	_, found = c.lookup(other.db, 6)
	require.False(t, found, "ReadTs alone cannot identify a DB instance")

	scope := NewTaintServingReadScope()
	var accepted atomic.Int32
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for scope.takeHeaderRead() {
				accepted.Add(1)
			}
		}()
	}
	workers.Wait()
	require.Equal(t, int32(taintServingHeaderReadLimit), accepted.Load())
	var zero TaintServingReadScope
	require.False(t, zero.takeHeaderRead())
}

func TestTaintServingCacheDoesNotSurviveReopen(t *testing.T) {
	path := t.TempDir()
	s, err := NewBadgerStore(path)
	require.NoError(t, err)
	require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
		_, err := reader.incompleteTaintStatus(NewTaintServingReadScope())
		return err
	}))
	oldCache, oldDB := s.taintServingCache, s.db
	require.NoError(t, s.CloseBadger())
	reopened, err := NewBadgerStore(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.CloseBadger()) })
	require.NotSame(t, oldDB, reopened.db)
	require.NotSame(t, oldCache, reopened.taintServingCache)
	require.Empty(t, reopened.taintServingCache.entries)
}

func TestTaintServingConcurrentColdReaders(t *testing.T) {
	s := openTaintStore(t)
	seedServingHeaderScanFixture(t, s, 2)
	scope := NewTaintServingReadScope()
	results := make(chan error, 8)
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			results <- s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
				status, err := reader.incompleteTaintStatus(scope)
				if err != nil {
					return err
				}
				if status != taintIncompleteNone {
					return fmt.Errorf("complete two-header scan returned %d", status)
				}
				return nil
			})
		}()
	}
	workers.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	require.GreaterOrEqual(t, scope.remaining, taintServingHeaderReadLimit-16)
	require.Len(t, s.taintServingCache.entries, 1)
}

func TestTaintServingIncompleteStatusVariants(t *testing.T) {
	for _, status := range []taint.Status{
		taint.StatusExpanding, taint.StatusDepthExhausted, taint.StatusWorkExhausted,
	} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			s := openTaintStore(t)
			id := seedServingIncompleteDecision(t, s, "incomplete-status")
			header, found, err := s.GetTaintDecision(id)
			require.NoError(t, err)
			require.True(t, found)
			// Decoder/status control only; this is not a claim that work reached
			// an exhaustion threshold or that a fabricated Complete is valid.
			header.Status = status
			raw, err := taint.EncodeHeader(header)
			require.NoError(t, err)
			require.NoError(t, s.SetRawForTest(taint.DecisionKey(id), raw))
			require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
				got, err := reader.incompleteTaintStatus(NewTaintServingReadScope())
				require.NoError(t, err)
				require.Equal(t, taintIncompletePresent, got)
				return nil
			}))
		})
	}
}

func TestMemoryProjectionReadAuthorizesValidatedEnvelopeBeforeTaint(t *testing.T) {
	s := openTaintStore(t)
	record := seedServingDeepRecord(t, s, "policy")
	require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, s.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, s.SetMemoryClassification(record.MemoryID, 3))
	seedServingIncompleteDecision(t, s, "policy-unrelated")
	scope := NewTaintServingReadScope()
	called := false
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, scope,
		func(reader *BadgerStore, canonical *MemoryDisclosureState) (bool, error) {
			called = true
			require.True(t, canonical.DomainRecorded)
			require.True(t, canonical.AuthorRecorded)
			require.True(t, canonical.ClassificationRecorded)
			require.Equal(t, uint8(3), canonical.Classification)
			require.ErrorIs(t, reader.SetMemoryDomain(record.MemoryID, "changed"), badger.ErrReadOnlyTxn)
			return false, nil
		})
	require.NoError(t, err)
	require.True(t, called)
	require.False(t, allowed)
	require.Nil(t, state)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.Equal(t, taintServingHeaderReadLimit, scope.remaining,
		"a denied row must not consume cold reads or become taint uncertainty")

	wrong := *record
	wrong.DomainTag = "untrusted-domain"
	called = false
	_, disposition, allowed, err = s.ClassifyMemoryProjectionForRead(&wrong, scope,
		func(*BadgerStore, *MemoryDisclosureState) (bool, error) { called = true; return true, nil })
	require.ErrorIs(t, err, ErrMemoryProjectionQuarantined)
	require.False(t, called, "unvalidated SQL fields must never reach policy")
	require.False(t, allowed)
	require.Equal(t, MemoryProjectionQuarantined, disposition)
	_, _, _, err = s.ClassifyMemoryProjectionForRead(record, scope, nil)
	require.Error(t, err)
}

func TestMemoryProjectionReadSnapshotPolicyAndStrictErrors(t *testing.T) {
	s := openTaintStore(t)
	record := servingMemoryRecord("read-snapshot", "read snapshot content")
	require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
	state, _, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(),
		func(reader *BadgerStore, canonical *MemoryDisclosureState) (bool, error) {
			require.NoError(t, s.SetMemoryDomain(record.MemoryID, "later-domain"))
			target := testTaintParent(record.MemoryID)
			copy(target.ContentHash[:], record.ContentHash)
			_, err := s.ApplyTaintDecision("sage", testTaintDecisionID("later-direct"),
				target, testTaintPayloadHash("later-direct"))
			require.NoError(t, err)
			domain, err := reader.GetMemoryDomain(record.MemoryID)
			require.NoError(t, err)
			require.Equal(t, canonical.Domain, domain)
			return true, nil
		})
	require.NoError(t, err, "policy and taint must both remain on the earlier pinned view")
	require.True(t, allowed)
	require.NotNil(t, state)
	_, _, err = s.ClassifyMemoryProjection(record)
	require.ErrorIs(t, err, ErrMemoryProjectionQuarantined, "a later call observes the changed envelope")

	policyFailure := errors.New("policy backend unavailable")
	record.DomainTag = "later-domain"
	_, _, allowed, err = s.ClassifyMemoryProjectionForRead(record, nil,
		func(*BadgerStore, *MemoryDisclosureState) (bool, error) { return false, policyFailure })
	require.ErrorIs(t, err, policyFailure)
	require.NotErrorIs(t, err, ErrMemoryProjectionTaintIndeterminate)
	require.False(t, allowed)
}

func TestMemoryProjectionReadIndeterminateIsNotQuarantine(t *testing.T) {
	s := openTaintStore(t)
	record := seedServingDeepRecord(t, s, "indeterminate")
	seedServingIncompleteDecision(t, s, "indeterminate-unrelated")
	s.PublishCanonicalMemoryProjectionAudit(true, false, false)
	before := s.CanonicalMemoryProjectionHealth()
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), allowMemoryProjectionRead)
	require.Nil(t, state)
	require.True(t, allowed)
	require.Equal(t, MemoryProjectionTaintIndeterminate, disposition)
	require.ErrorIs(t, err, ErrMemoryProjectionTaintIndeterminate)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget)
	require.NotErrorIs(t, err, ErrMemoryProjectionUnpublished)
	require.Equal(t, before, s.CanonicalMemoryProjectionHealth())
	state, _, err = s.ClassifyMemoryProjection(record)
	require.Nil(t, state)
	require.ErrorIs(t, err, ErrMemoryProjectionTaintIndeterminate)
	state, err = s.ValidateMemoryProjection(record)
	require.Nil(t, state)
	require.ErrorIs(t, err, ErrMemoryProjectionTaintIndeterminate)
}

func TestTaintServingHeaderCorruptionIsNotIndeterminate(t *testing.T) {
	for _, invalidKey := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid_key_%t", invalidKey), func(t *testing.T) {
			s := openTaintStore(t)
			record := servingMemoryRecord("corrupt-unrelated-header", "canonical content")
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			key := taint.DecisionKey(testTaintDecisionID("corrupt-unrelated-header"))
			if invalidKey {
				key = append(taint.DecisionPrefix(), []byte("invalid")...)
			}
			require.NoError(t, s.SetRawForTest(key, []byte("invalid header")))
			state, _, allowed, err := s.ClassifyMemoryProjectionForRead(record, nil, allowMemoryProjectionRead)
			require.Nil(t, state)
			require.True(t, allowed)
			require.ErrorIs(t, err, ErrTaintStateMalformed)
			require.NotErrorIs(t, err, ErrMemoryProjectionTaintIndeterminate)
			require.NotErrorIs(t, err, ErrMemoryProjectionUnpublished)
		})
	}
}

func TestMemoryProjectionReadMalformedLineageIsStrict(t *testing.T) {
	s := openTaintStore(t)
	record := servingMemoryRecord("corrupt-lineage", "canonical content")
	require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	seedServingIncompleteDecision(t, s, "lineage-unrelated")
	parent := testTaintParent("corrupt-lineage-parent")
	parentID, err := tx.ParentRefIDHex(parent)
	require.NoError(t, err)
	require.NoError(t, s.SetRawForTest(lineage.ForwardKey(record.MemoryID, parentID), []byte("invalid parent")))
	state, _, allowed, err := s.ClassifyMemoryProjectionForRead(record, nil, allowMemoryProjectionRead)
	require.Nil(t, state)
	require.True(t, allowed)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)
	require.NotErrorIs(t, err, ErrMemoryProjectionTaintIndeterminate)
	require.NotErrorIs(t, err, ErrMemoryProjectionUnpublished)
}
