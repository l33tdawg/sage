package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func seedModernCompleteScope(t *testing.T, s *BadgerStore, label string, scope TaintDecisionScope) string {
	t.Helper()
	target := seedCoverageMemory(t, s, label+"-target")
	id := testTaintDecisionID(label)
	created, err := s.ApplyTaintDecisionWithScope("sage", id, target, testTaintPayloadHash(label), scope)
	require.NoError(t, err)
	require.True(t, created)
	expanded, err := s.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	return id
}

func TestTaintScopeModernCompleteAncestorAndClearSnapshot(t *testing.T) {
	s := openTaintStore(t)
	credential := testCredential("a")
	id := seedModernCompleteScope(t, s, "complete-ancestor", testCredentialScope(credential))
	// These records are created after expansion finished. No materialized
	// membership exists, but the active immutable author scope still applies.
	parent := seedCoverageMemory(t, s, "post-complete-parent")
	child := seedCoverageMemory(t, s, "post-complete-child")
	require.NoError(t, s.SetAppV30Provenance(parent.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
	require.NoError(t, s.SetAppV30Provenance(child.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("b")}))
	require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{parent}))
	memberships, err := s.GetEffectiveTaintDecisions(child.MemoryID)
	require.NoError(t, err)
	require.Empty(t, memberships)
	got, err := s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingTainted, got.Outcome)
	require.Equal(t, []string{id}, got.DecisionIDs)
	require.Equal(t, 2, got.Nodes)
	_, complete, err := s.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
	require.NoError(t, err)
	require.False(t, complete, "Complete expansion is not SQL scope-match completeness")

	require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
		changed, err := s.ClearTaintDecision(id)
		require.NoError(t, err)
		require.True(t, changed)
		old, err := reader.classifyMemoryTaintForRead(child.MemoryID, NewTaintServingReadScope())
		require.NoError(t, err)
		require.Equal(t, TaintServingTainted, old.Outcome, "clear cannot mix into an older pinned read")
		return nil
	}))
	got, err = s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, got.Outcome)
	require.Equal(t, 1, got.Nodes)
	_, complete, err = s.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
	require.NoError(t, err)
	require.True(t, complete)
}

func TestTaintScopeModernDeepFastPathAndStrictTransition(t *testing.T) {
	s := openTaintStore(t)
	record := seedServingDeepRecord(t, s, "scope-deep")
	assertClear := func() {
		t.Helper()
		got, err := s.ClassifyMemoryTaint(record.MemoryID)
		require.NoError(t, err)
		require.Equal(t, TaintServingClear, got.Outcome)
		require.Equal(t, 1, got.Nodes)
	}
	assertClear()
	seedModernCompleteScope(t, s, "scope-deep-direct", TaintDecisionScope{Kind: tx.TaintScopeDirect})
	assertClear()
	id := seedModernCompleteScope(t, s, "scope-deep-credential", testCredentialScope(testCredential("a")))
	got, err := s.ClassifyMemoryTaint(record.MemoryID)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget)
	require.False(t, got.CoverageComplete)
	require.Equal(t, taint.MaxServingAncestorWalk, got.Nodes)
	_, err = s.ClearTaintDecision(id)
	require.NoError(t, err)
	assertClear()
}

func TestTaintScopeModernRequestAllowanceAndPolicyFirst(t *testing.T) {
	s := openTaintStore(t)
	record := servingMemoryRecord("shared-scope-budget", "shared request content")
	require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, s.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	scope := NewTaintServingReadScope()
	before := scope.mandatoryRemaining
	projection, _, allowed, err := s.ClassifyMemoryProjectionForRead(record, scope,
		func(reader *BadgerStore, state *MemoryDisclosureState) (bool, error) {
			require.NotSame(t, s, reader)
			require.Equal(t, record.ContentHash, state.ContentHash)
			return false, nil
		})
	require.NoError(t, err)
	require.False(t, allowed)
	require.Nil(t, projection)
	require.Equal(t, before, scope.mandatoryRemaining, "denied rows must not inspect scopes")
	first, err := s.classifyMemoryTaintForRead(record.MemoryID, scope)
	require.NoError(t, err)
	require.Greater(t, first.Units, 0)
	require.Equal(t, taint.MaxServingRequestUnits-first.Units, scope.mandatoryRemaining)
	// Actual repeated canonical reads, not calls to the counter helper, show
	// the same allowance is shared by subsequent filter/final/page passes.
	for i := 1; i < taint.MaxServingRequestUnits/first.Units; i++ {
		got, err := s.classifyMemoryTaintForRead(record.MemoryID, scope)
		require.NoError(t, err)
		require.Equal(t, first.Units, got.Units)
	}
	_, err = s.classifyMemoryTaintForRead(record.MemoryID, scope)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget)
	_, err = s.classifyMemoryTaintForRead(record.MemoryID, scope)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget, "a later pass must not reset the allowance")
	_, err = s.ClassifyMemoryTaint(record.MemoryID)
	require.NoError(t, err, "a distinct request receives its own allowance")
}

func TestTaintScopeModernPolicyAndTaintShareOneSnapshot(t *testing.T) {
	s := openTaintStore(t)
	record := servingMemoryRecord("policy-scope-pinned", "same envelope")
	record.SubmittingAgent = testCredential("a")
	require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, s.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, s.SetAppV30Provenance(record.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
	id := seedModernCompleteScope(t, s, "policy-scope", testCredentialScope(testCredential("a")))
	called := false
	_, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(),
		func(reader *BadgerStore, state *MemoryDisclosureState) (bool, error) {
			called = true
			changed, err := s.ClearTaintDecision(id)
			require.NoError(t, err)
			require.True(t, changed)
			header, found, err := reader.GetTaintDecision(id)
			require.NoError(t, err)
			require.True(t, found)
			require.True(t, header.Status.Active())
			return true, nil
		})
	require.True(t, called)
	require.True(t, allowed)
	require.ErrorIs(t, err, ErrMemoryProjectionTainted)
	require.Equal(t, MemoryProjectionTainted, disposition)
	_, disposition, err = s.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
}

func TestTaintScopeModernCorruptionAndAncestorHashRemainStrict(t *testing.T) {
	for _, fault := range []string{"nonempty index", "oversized scope", "missing scope", "missing index", "false zero counter", "ancestor hash"} {
		t.Run(fault, func(t *testing.T) {
			s := openTaintStore(t)
			id := seedModernCompleteScope(t, s, "corrupt-scope", testCredentialScope(testCredential("a")))
			parent := seedCoverageMemory(t, s, "scope-corrupt-parent")
			child := seedCoverageMemory(t, s, "scope-corrupt-child")
			require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{parent}))
			switch fault {
			case "nonempty index":
				require.NoError(t, s.SetRawForTest(taint.ActiveDecisionKey(id), []byte{1}))
			case "oversized scope":
				require.NoError(t, s.SetRawForTest(TaintDecisionScopeKey(id), make([]byte, taintScopeValueMaxBytes+1)))
			case "missing scope":
				require.NoError(t, s.DB().Update(func(txn *badger.Txn) error { return txn.Delete(TaintDecisionScopeKey(id)) }))
			case "missing index":
				require.NoError(t, s.DB().Update(func(txn *badger.Txn) error { return txn.Delete(taint.ActiveDecisionKey(id)) }))
			case "false zero counter":
				require.NoError(t, s.SetRawForTest(taint.ActiveCountKey(), make([]byte, 8)))
			case "ancestor hash":
				hash := sha256.Sum256([]byte("changed canonical parent"))
				require.NoError(t, s.SetMemoryHash(parent.MemoryID, hash[:], string(memory.StatusCommitted)))
			}
			got, err := s.ClassifyMemoryTaint(child.MemoryID)
			require.Error(t, err)
			if fault == "ancestor hash" {
				require.ErrorIs(t, err, ErrMemoryLineageMalformed)
			} else {
				require.ErrorIs(t, err, ErrTaintStateMalformed)
			}
			require.False(t, got.CoverageComplete)
			if fault != "ancestor hash" {
				_, complete, coverageErr := s.TaintServingCoverageSnapshot(context.Background(), NewTaintServingReadScope())
				require.ErrorIs(t, coverageErr, ErrTaintStateMalformed)
				require.False(t, complete)
			}
		})
	}
}

func TestTaintScopeModernAdmissionWorstCaseUnits(t *testing.T) {
	s := openTaintStore(t)
	parents := make([]tx.ParentRef, taint.MaxParents)
	for i := range parents {
		parents[i] = seedCoverageMemory(t, s, fmt.Sprintf("scope-worst-parent-%02d", i))
	}
	tx.SortParentRefs(parents)
	signers := make([]AppV30ProvenanceSigner, taint.MaxSigners)
	for i := range signers {
		credential := fmt.Sprintf("%064x", i+1)
		signers[i] = AppV30ProvenanceSigner{CredentialID: credential, PrincipalID: credential, Exact: true}
	}
	credentials := make([]string, tx.MaxTaintCredentialSetEntries)
	for i := range credentials {
		credentials[i] = signers[i].CredentialID
	}
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		id := testTaintDecisionID(fmt.Sprintf("scope-worst-decision-%02d", i))
		created, err := s.ApplyTaintDecisionWithScope("sage", id, parents[0], testTaintPayloadHash(id), testCredentialScope(credentials...))
		require.NoError(t, err)
		require.True(t, created)
	}
	// Real admission to each other parent creates all 16*32 effective parent
	// memberships without bypassing the typed state API.
	for _, parent := range parents[1:] {
		_, err := s.ApplyTaintAdmission("sage", parent.MemoryID, nil, signers)
		require.NoError(t, err)
	}
	preflight, err := s.ValidateTaintAdmission("sage", "scope-worst-child", parents, signers)
	require.NoError(t, err)
	require.Equal(t, taint.MaxActiveDecisions, preflight.Decisions)
	require.Equal(t, 723, preflight.Units, "provisional cap must be justified by actual modern fixture reads")
	require.Equal(t, taint.MaxProvenanceAdmissionTaintUnits, preflight.Units)
	written, err := s.ApplyTaintAdmission("sage", "scope-worst-child", parents, signers)
	require.NoError(t, err)
	require.Equal(t, preflight, written)
	require.NoError(t, s.ValidateTaintState())
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		meter := newTaintUnitMeter(97, ErrTaintBudget)
		_, err := readActiveTaintDecisionScopesTxn(txn, meter)
		require.ErrorIs(t, err, ErrTaintBudget, "the 98th real inventory read is refused")
		return nil
	}))
}

func TestTaintScopeModernJournalRestoresScopeIndexAndTarget(t *testing.T) {
	s := openTaintStore(t)
	control := s.BeginConsensusTransaction(nil)
	t.Cleanup(control.DiscardConsensusTransaction)
	before, err := control.ComputeAppHash()
	require.NoError(t, err)
	require.NoError(t, control.BeginAppV30WriteStage(1<<20))
	id := testTaintDecisionID("scope-journal")
	target := testTaintParent("scope-journal-target")
	_, err = control.ApplyTaintDecisionWithScope("sage", id, target, testTaintPayloadHash(id), testCredentialScope(testCredential("a")))
	require.NoError(t, err)
	keys := [][]byte{
		taint.DecisionKey(id), taint.TargetKey(id), TaintDecisionScopeKey(id),
		taint.ActiveDecisionKey(id), taint.MembershipKey(target.MemoryID, id),
		taint.DecisionIndexKey(id, target.MemoryID), taint.FrontierKey(id, 1, 0),
		taint.ActiveCountKey(), taint.RevisionKey(),
	}
	var generated int64
	for _, key := range keys {
		generated += int64(len(key) + len(readTaintValue(t, control, key)))
	}
	require.Equal(t, generated, control.AppV30WriteStageStats().Generated,
		"scope and active index must be charged in addition to the immutable target and existing writes")
	require.NoError(t, control.AbortAppV30WriteStage())
	after, err := control.ComputeAppHash()
	require.NoError(t, err)
	require.Equal(t, before, after)
	for _, key := range [][]byte{taint.TargetKey(id), TaintDecisionScopeKey(id), taint.ActiveDecisionKey(id)} {
		require.NoError(t, control.view(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			require.ErrorIs(t, err, badger.ErrKeyNotFound)
			return nil
		}))
	}
	require.NoError(t, control.ValidateTaintState())
}

func TestTaintScopeModernOptionalCoverageIncludesScopeOnlyMatches(t *testing.T) {
	for _, earlierDirect := range []bool{false, true} {
		t.Run(fmt.Sprintf("earlier_direct_%t", earlierDirect), func(t *testing.T) {
			s := openTaintStore(t)
			parent := seedCoverageMemory(t, s, "optional-scope-parent")
			child := seedCoverageMemory(t, s, "optional-scope-child")
			require.NoError(t, s.SetAppV30Provenance(parent.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
			require.NoError(t, s.SetAppV30Provenance(child.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("b")}))
			require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{parent}))
			if earlierDirect {
				directID := testTaintDecisionID("optional-earlier-direct")
				_, err := s.ApplyTaintDecision("sage", directID, child, testTaintPayloadHash(directID))
				require.NoError(t, err)
				expanded, err := s.ExpandTaintDecision("sage", directID, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
				require.Equal(t, taint.StatusComplete, expanded.Status)
			}
			target := seedCoverageMemory(t, s, "optional-unrelated-target")
			scopeID := testTaintDecisionID("optional-incomplete-scope")
			_, err := s.ApplyTaintDecisionWithScope("sage", scopeID, target, testTaintPayloadHash(scopeID), testCredentialScope(testCredential("a")))
			require.NoError(t, err)
			memberships, err := s.GetEffectiveTaintDecisions(parent.MemoryID)
			require.NoError(t, err)
			require.Empty(t, memberships, "the ancestor match is scope-only")
			got, err := s.ClassifyMemoryTaint(child.MemoryID)
			require.NoError(t, err)
			require.Equal(t, TaintServingTainted, got.Outcome)
			require.False(t, got.CoverageComplete, "a later incomplete scope cannot be hidden by an earlier direct decision")
			require.False(t, got.LineageMissing)
			if earlierDirect {
				require.Equal(t, 1, got.Nodes, "optional graph proof is independent of mandatory early return")
			} else {
				require.Equal(t, []string{scopeID}, got.DecisionIDs)
				require.Equal(t, 2, got.Nodes)
			}
		})
	}
}

func TestTaintScopeModernRetainedSlotClearReplayAndRelease(t *testing.T) {
	s := openTaintStore(t)
	id := seedModernCompleteScope(t, s, "retained-cleaning", testCredentialScope(testCredential("a")))
	changed, err := s.ClearTaintDecision(id)
	require.NoError(t, err)
	require.True(t, changed)
	changed, err = s.ClearTaintDecision(id)
	require.NoError(t, err)
	require.False(t, changed)
	count, err := s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(taint.ActiveDecisionKey(id))
		require.NoError(t, err, "Cleaning still owns its capacity slot")
		meter := newTaintUnitMeter(taint.MaxServingRequestUnits, ErrTaintServingWalkBudget)
		scopes, err := readActiveTaintDecisionScopesTxn(txn, meter)
		require.NoError(t, err)
		require.Empty(t, scopes, "retained Cleaning entries must never match")
		require.Equal(t, 5, meter.units, "setup+index+header+scope+counter")
		return nil
	}))
	require.NoError(t, s.ValidateTaintState())
	finishServingDecisionCleanup(t, s, id)
	count, err = s.ActiveTaintDecisionCount()
	require.NoError(t, err)
	require.Zero(t, count)
	require.NoError(t, s.view(func(txn *badger.Txn) error {
		_, err := txn.Get(taint.ActiveDecisionKey(id))
		require.ErrorIs(t, err, badger.ErrKeyNotFound)
		return nil
	}))
	require.NoError(t, s.ValidateTaintState())
}

func TestTaintScopeModernJournalRestoresClearAndCleanupSlots(t *testing.T) {
	for _, cleanup := range []bool{false, true} {
		t.Run(fmt.Sprintf("cleanup_%t", cleanup), func(t *testing.T) {
			s := openTaintStore(t)
			id := seedModernCompleteScope(t, s, "slot-rollback", testCredentialScope(testCredential("a")))
			wantStatus := taint.StatusComplete
			if cleanup {
				_, err := s.ClearTaintDecision(id)
				require.NoError(t, err)
				wantStatus = taint.StatusCleaning
			}
			control := s.BeginConsensusTransaction(nil)
			t.Cleanup(control.DiscardConsensusTransaction)
			before, err := control.ComputeAppHash()
			require.NoError(t, err)
			require.NoError(t, control.BeginAppV30WriteStage(1<<20))
			if cleanup {
				// Cleanup advances one phase per call. Keep both phases inside
				// this stage so abort must restore the released slot as well.
				members, err := control.CleanupTaintDecision(id, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
				require.Equal(t, taint.StatusCleaning, members.Status)
				require.Equal(t, taint.CleanupQueue, members.Phase)
				require.False(t, members.ActiveReleased)
				queue, err := control.CleanupTaintDecision(id, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
				require.Equal(t, taint.StatusClean, queue.Status)
				require.True(t, queue.ActiveReleased)
				count, err := control.ActiveTaintDecisionCount()
				require.NoError(t, err)
				require.Zero(t, count)
				require.NoError(t, control.view(func(txn *badger.Txn) error {
					_, err := txn.Get(taint.ActiveDecisionKey(id))
					require.ErrorIs(t, err, badger.ErrKeyNotFound, "cleanup releases the slot before abort")
					return nil
				}))
			} else {
				changed, err := control.ClearTaintDecision(id)
				require.NoError(t, err)
				require.True(t, changed)
			}
			require.Positive(t, control.AppV30WriteStageStats().Generated)
			require.NoError(t, control.AbortAppV30WriteStage())
			after, err := control.ComputeAppHash()
			require.NoError(t, err)
			require.Equal(t, before, after)
			header, found, err := control.GetTaintDecision(id)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, wantStatus, header.Status)
			count, err := control.ActiveTaintDecisionCount()
			require.NoError(t, err)
			require.Equal(t, uint64(1), count)
			require.NoError(t, control.view(func(txn *badger.Txn) error {
				_, err := txn.Get(taint.ActiveDecisionKey(id))
				require.NoError(t, err, "abort restores the retained-slot index")
				return nil
			}))
			require.NoError(t, control.ValidateTaintState())
		})
	}
}
