package store

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

// Direct Badger/read-policy fixtures only: no new-write admission, SQL,
// signed transaction, process recovery or complete startup-index acceptance.
func TestHistoricalWideMemoryServingPreservesReadAndStatus(t *testing.T) {
	s := identityTestStore(t)
	id := strings.Repeat("legacy:", 80)
	record := identityProjectionFixture(t, s, id)
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), allowMemoryProjectionRead)
	require.NoError(t, err)
	require.True(t, allowed)
	require.NotNil(t, state)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.NoError(t, s.SetMemoryStatusPreservingHash(id, "deprecated"))
	record.Status = memory.StatusDeprecated
	state, disposition, err = s.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.Equal(t, record.ContentHash, state.ContentHash)
	// Every default/read-only public primitive and writer still owns its512 cap.
	_, err = s.ClassifyMemoryTaint(id)
	require.Error(t, err)
	_, err = s.GetEffectiveTaintDecisions(id)
	require.Error(t, err)
	_, err = s.AuthorProvenance(id)
	require.Error(t, err)
	_, err = s.GetMemoryDisclosureStateAppV30(id)
	require.Error(t, err)
	require.Error(t, s.SetMemoryStatusPreservingHashAppV30(id, "committed"))
	require.Error(t, lineage.ValidateChildID(id))
	require.Error(t, validatePublicMemoryIdentity(id, true))
	_, err = s.ApplyTaintDecision("sage", testTaintDecisionID("wide-write-refusal"), testTaintParent(id), testTaintPayloadHash("wide-write-refusal"))
	require.Error(t, err)
}

func TestHistoricalWideMemoryServingEvaluatesCredentialScope(t *testing.T) {
	s := identityTestStore(t)
	record := identityProjectionFixture(t, s, strings.Repeat("wide", 180))
	target := identityProjectionFixture(t, s, "scope-target")
	ref := testTaintParent(target.MemoryID)
	copy(ref.ContentHash[:], target.ContentHash)
	id := testTaintDecisionID("wide-author-scope")
	created, err := s.ApplyTaintDecisionWithScope("sage", id, ref, testTaintPayloadHash(id), testCredentialScope(record.SubmittingAgent))
	require.NoError(t, err)
	require.True(t, created)
	_, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), allowMemoryProjectionRead)
	require.True(t, allowed)
	require.ErrorIs(t, err, ErrMemoryProjectionTainted)
	require.Equal(t, MemoryProjectionTainted, disposition)
	// The same valid canonical row is policy-denied before any scope disclosure.
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), func(*BadgerStore, *MemoryDisclosureState) (bool, error) { return false, nil })
	require.NoError(t, err)
	require.False(t, allowed)
	require.Nil(t, state)
	require.Equal(t, MemoryProjectionExact, disposition)
	changed, err := s.ClearTaintDecision(id)
	require.NoError(t, err)
	require.True(t, changed)
	_, disposition, err = s.ClassifyMemoryProjection(record)
	require.NoError(t, err)
	require.Equal(t, MemoryProjectionExact, disposition)
}

func TestHistoricalWideMemoryServingRejectsMalformedIndexesAfterPolicy(t *testing.T) {
	for _, kind := range []string{"membership", "forward-edge"} {
		t.Run(kind, func(t *testing.T) {
			s := identityTestStore(t)
			record := identityProjectionFixture(t, s, strings.Repeat("w", 513))
			identity := taintReadIdentityPolicy{legacyRootID: record.MemoryID}
			var key []byte
			if kind == "membership" {
				prefix, err := identity.membershipPrefix(record.MemoryID)
				require.NoError(t, err)
				key = append(prefix, testTaintDecisionID("malformed-wide")...)
			} else {
				// A real unrelated incomplete decision requires mandatory ancestry, so
				// this control proves malformed present wide edges are not treated absent.
				target := testTaintParent("unrelated-wide-target")
				created, err := s.ApplyTaintDecision("sage", testTaintDecisionID("unrelated-wide"), target, testTaintPayloadHash("unrelated-wide"))
				require.NoError(t, err)
				require.True(t, created)
				prefix, err := identity.forwardPrefix(record.MemoryID)
				require.NoError(t, err)
				key = append(prefix, strings.Repeat("a", 2*lineage.ParentIDBytes)...)
			}
			require.NoError(t, s.db.Update(func(txn *badger.Txn) error { return txn.Set(key, []byte{1}) }))
			_, _, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), func(*BadgerStore, *MemoryDisclosureState) (bool, error) { return false, nil })
			require.NoError(t, err)
			require.False(t, allowed)
			_, _, _, err = s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), allowMemoryProjectionRead)
			if kind == "membership" {
				require.ErrorIs(t, err, ErrTaintStateMalformed)
			} else {
				require.ErrorIs(t, err, ErrMemoryLineageMalformed)
			}
		})
	}
}

func TestHistoricalWideMemoryServingChecksPresentAuthenticatedEdge(t *testing.T) {
	s := identityTestStore(t)
	child := identityProjectionFixture(t, s, strings.Repeat("edge:", 110))
	parent := identityProjectionFixture(t, s, "wide-parent")
	ref := testTaintParent(parent.MemoryID)
	copy(ref.ContentHash[:], parent.ContentHash)
	refID, err := tx.ParentRefIDHex(ref)
	require.NoError(t, err)
	encoded, err := tx.EncodeParentRefValue(ref)
	require.NoError(t, err)
	identity := taintReadIdentityPolicy{legacyRootID: child.MemoryID}
	prefix, err := identity.forwardPrefix(child.MemoryID)
	require.NoError(t, err)
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(append(prefix, refID...), encoded); err != nil {
			return err
		}
		return txn.Set(lineage.ReverseKey(refID, child.MemoryID), nil)
	}))
	decision := testTaintDecisionID("wide-ancestor")
	created, err := s.ApplyTaintDecision("sage", decision, ref, testTaintPayloadHash(decision))
	require.NoError(t, err)
	require.True(t, created)
	_, disposition, err := s.ClassifyMemoryProjection(child)
	require.ErrorIs(t, err, ErrMemoryProjectionTaintPending)
	require.Equal(t, MemoryProjectionTaintPending, disposition)
	// Parent codec remains strict even under the historical child's read policy.
	invalidRef := ref
	invalidRef.MemoryID = child.MemoryID
	_, err = tx.EncodeParentRefValue(invalidRef)
	require.Error(t, err)
	// A present reverse edge never substitutes for the canonical parent's hash.
	different := sha256.Sum256([]byte("changed parent"))
	require.NoError(t, s.SetMemoryHash(parent.MemoryID, different[:], "committed"))
	_, _, err = s.ClassifyMemoryProjection(child)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)
}

func TestHistoricalWideMemoryServingAbsentAndBlankStayUnavailable(t *testing.T) {
	s := identityTestStore(t)
	record := identityProjectionFixture(t, s, "existing-control")
	calls := 0
	for _, id := range []string{strings.Repeat("absent", 100), strings.Repeat(" ", 513)} {
		record.MemoryID = id
		if strings.TrimSpace(id) == "" {
			require.NoError(t, s.SetMemoryHash(id, record.ContentHash, "committed"))
		}
		_, _, _, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), func(*BadgerStore, *MemoryDisclosureState) (bool, error) { calls++; return true, nil })
		require.Error(t, err)
	}
	require.Zero(t, calls)
}

func TestHistoricalWideIdentityFramingIsExactAndDoesNotWrap(t *testing.T) {
	for _, id := range []string{"x", " ", "\x00", strings.Repeat("x", 512)} {
		member, err := (taintReadIdentityPolicy{}).membershipPrefix(id)
		require.NoError(t, err)
		require.True(t, bytes.Equal(taint.MembershipPrefix(id), member))
		require.Equal(t, historicalIdentityPrefix("taintm:", id), member)
		forward, err := (taintReadIdentityPolicy{}).forwardPrefix(id)
		require.NoError(t, err)
		require.Equal(t, lineage.ForwardPrefix(id), forward)
		require.Equal(t, historicalIdentityPrefix("lineage:", id), forward)
	}
	id := strings.Repeat("x", 513)
	identity := taintReadIdentityPolicy{legacyRootID: id}
	a, err := identity.membershipPrefix(id)
	require.NoError(t, err)
	require.NotEqual(t, taint.MembershipPrefix(id), a)
	_, err = identity.membershipPrefix(id + ":x")
	require.Error(t, err)
	require.Error(t, identity.validate(strings.Repeat("x", maxHistoricalTaintReadIDBytes+1)))
}
