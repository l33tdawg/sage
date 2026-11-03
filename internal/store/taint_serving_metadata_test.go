package store

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func seedCoverageMemory(t *testing.T, s *BadgerStore, id string) tx.ParentRef {
	t.Helper()
	ref := testTaintParent(id)
	require.NoError(t, s.SetMemoryHash(id, ref.ContentHash[:], string(memory.StatusCommitted)))
	return ref
}

func TestTaintServingMetadataKnownAndMissingRoots(t *testing.T) {
	for _, incomplete := range []bool{false, true} {
		name := "no headers"
		if incomplete {
			name = "unrelated incomplete header"
		}
		t.Run(name, func(t *testing.T) {
			s := openTaintStore(t)
			root := seedCoverageMemory(t, s, "coverage-root")
			child := seedCoverageMemory(t, s, "coverage-child")
			require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{root}))
			if incomplete {
				seedServingIncompleteDecision(t, s, "coverage-unrelated")
			}
			got, err := s.ClassifyMemoryTaint(child.MemoryID)
			require.NoError(t, err)
			require.Equal(t, TaintServingClear, got.Outcome)
			require.False(t, got.CoverageComplete)
			require.True(t, got.LineageMissing, "a checked leaf has no exact provenance record")
			require.False(t, got.DepthExhausted)
			require.False(t, got.WorkExhausted)

			require.NoError(t, s.SetAppV30Provenance(root.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
			got, err = s.ClassifyMemoryTaint(child.MemoryID)
			require.NoError(t, err)
			require.Equal(t, TaintServingClear, got.Outcome)
			require.True(t, got.CoverageComplete)
			require.False(t, got.LineageMissing)
			if !incomplete {
				require.Equal(t, 1, got.Nodes, "optional discovery does not replace the no-incomplete-header shortcut")
			}
		})
	}
}

func TestTaintServingMetadataChecksEveryBranchAndHash(t *testing.T) {
	s := openTaintStore(t)
	child := seedCoverageMemory(t, s, "coverage-fork")
	parents := []tx.ParentRef{
		seedCoverageMemory(t, s, "coverage-parent-a"),
		seedCoverageMemory(t, s, "coverage-parent-b"),
	}
	tx.SortParentRefs(parents)
	require.NoError(t, s.SetMemoryLineage(child.MemoryID, parents))
	firstID, err := tx.ParentRefIDHex(parents[0])
	require.NoError(t, err)
	secondID, err := tx.ParentRefIDHex(parents[1])
	require.NoError(t, err)
	known, unknown := parents[0], parents[1]
	if secondID < firstID {
		known, unknown = unknown, known
	}
	// The first stored branch closes at a known root; that cannot hide the
	// later branch's actually missing provenance.
	require.NoError(t, s.SetAppV30Provenance(known.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
	got, err := s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, got.Outcome)
	require.False(t, got.CoverageComplete)
	require.True(t, got.LineageMissing)
	require.NoError(t, s.SetAppV30Provenance(unknown.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("b")}))
	got, err = s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.True(t, got.CoverageComplete)

	// A valid edge encoding alone is insufficient: its authenticated content
	// hash must still match the canonical parent in the same read view.
	changed := testTaintPayloadHash("changed-parent")
	require.NoError(t, s.SetMemoryHash(unknown.MemoryID, changed[:], string(memory.StatusCommitted)))
	got, err = s.ClassifyMemoryTaint(child.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, got.Outcome)
	require.False(t, got.CoverageComplete)
	require.False(t, got.LineageMissing, "a hash mismatch is unproved, not an observed missing leaf")
}

func TestTaintServingMetadataBudgetIsIndependentAndRequestShared(t *testing.T) {
	s := openTaintStore(t)
	deep := seedServingDeepRecord(t, s, "metadata-budget")
	root := seedCoverageMemory(t, s, "metadata-small-root")
	require.NoError(t, s.SetAppV30Provenance(root.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("c")}))
	scope := NewTaintServingReadScope()
	got, err := s.classifyMemoryTaintForRead(deep.MemoryID, scope)
	require.NoError(t, err, "optional discovery must not regress a deep clear row to a serving error")
	require.Equal(t, TaintServingClear, got.Outcome)
	require.Equal(t, 1, got.Nodes)
	require.False(t, got.CoverageComplete)
	require.False(t, got.LineageMissing, "the metadata budget expires before reaching the leaf")
	require.False(t, got.DepthExhausted)
	require.False(t, got.WorkExhausted)
	require.Zero(t, scope.coverageRemaining)
	require.Equal(t, taintServingHeaderReadLimit, scope.remaining)

	got, err = s.classifyMemoryTaintForRead(root.MemoryID, scope)
	require.NoError(t, err)
	require.Equal(t, TaintServingClear, got.Outcome)
	require.False(t, got.CoverageComplete, "another row cannot reset the request's metadata allowance")
	require.False(t, got.LineageMissing)
	got, err = s.classifyMemoryTaintForRead(root.MemoryID, NewTaintServingReadScope())
	require.NoError(t, err)
	require.True(t, got.CoverageComplete, "the small valid graph proves the exhausted-scope control is non-vacuous")

	seedServingIncompleteDecision(t, s, "mandatory-budget-unrelated")
	got, err = s.ClassifyMemoryTaint(deep.MemoryID)
	require.ErrorIs(t, err, ErrTaintServingWalkBudget, "the original mandatory walk remains strict")
	require.False(t, got.CoverageComplete)
}

func TestTaintServingMetadataOptionalErrorsDoNotReplaceStrictErrors(t *testing.T) {
	t.Run("absent canonical record", func(t *testing.T) {
		s := openTaintStore(t)
		got, err := s.ClassifyMemoryTaint("metadata-absent-record")
		require.NoError(t, err)
		require.Equal(t, TaintServingClear, got.Outcome)
		require.False(t, got.CoverageComplete)
		require.False(t, got.LineageMissing, "no canonical node was observed, so no leaf absence was proved")
	})
	t.Run("malformed optional provenance", func(t *testing.T) {
		s := openTaintStore(t)
		root := seedCoverageMemory(t, s, "metadata-bad-provenance")
		require.NoError(t, s.SetRawForTest(AppV30ProvenanceKey(root.MemoryID), []byte("invalid")))
		got, err := s.ClassifyMemoryTaint(root.MemoryID)
		require.NoError(t, err)
		require.Equal(t, TaintServingClear, got.Outcome)
		require.False(t, got.CoverageComplete)
		require.False(t, got.LineageMissing)
	})
	t.Run("nonexact optional provenance", func(t *testing.T) {
		s := openTaintStore(t)
		root := seedCoverageMemory(t, s, "metadata-nonexact-provenance")
		raw, err := encodeAppV30Provenance([]AppV30ProvenanceSigner{testAppV30Signer("a")})
		require.NoError(t, err)
		raw[len(raw)-1] = 0
		require.NoError(t, s.SetRawForTest(AppV30ProvenanceKey(root.MemoryID), raw))
		got, err := s.ClassifyMemoryTaint(root.MemoryID)
		require.NoError(t, err)
		require.Equal(t, TaintServingClear, got.Outcome)
		require.False(t, got.CoverageComplete, "the canonical decoder must reject a nonexact signer")
		require.False(t, got.LineageMissing, "malformed is unproved, not an absent record")
	})
	t.Run("same malformed edge optional then mandatory", func(t *testing.T) {
		s := openTaintStore(t)
		child := seedCoverageMemory(t, s, "metadata-bad-lineage")
		parent := testTaintParent("metadata-corrupt-parent")
		id, err := tx.ParentRefIDHex(parent)
		require.NoError(t, err)
		require.NoError(t, s.SetRawForTest(lineage.ForwardKey(child.MemoryID, id), []byte("invalid")))
		got, err := s.ClassifyMemoryTaint(child.MemoryID)
		require.NoError(t, err, "the no-incomplete-header disposition proof does not need optional lineage")
		require.Equal(t, TaintServingClear, got.Outcome)
		require.False(t, got.CoverageComplete)
		require.False(t, got.LineageMissing)
		seedServingIncompleteDecision(t, s, "metadata-now-mandatory")
		got, err = s.ClassifyMemoryTaint(child.MemoryID)
		require.ErrorIs(t, err, ErrMemoryLineageMalformed)
		require.False(t, got.CoverageComplete)
	})
	t.Run("malformed mandatory header", func(t *testing.T) {
		s := openTaintStore(t)
		root := seedCoverageMemory(t, s, "metadata-bad-header")
		require.NoError(t, s.SetAppV30Provenance(root.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
		id := seedServingIncompleteDecision(t, s, "metadata-header")
		require.NoError(t, s.SetRawForTest(taint.DecisionKey(id), []byte("invalid")))
		got, err := s.ClassifyMemoryTaint(root.MemoryID)
		require.ErrorIs(t, err, ErrTaintStateMalformed)
		require.False(t, got.CoverageComplete, "a known exact leaf cannot hide a mandatory decoder error")
	})
}

func TestTaintServingMetadataRejectsCorruptGraphWithoutChangingClear(t *testing.T) {
	for _, kind := range []string{"oversized membership key", "oversized parent key", "cycle"} {
		t.Run(kind, func(t *testing.T) {
			s := openTaintStore(t)
			child := seedCoverageMemory(t, s, "metadata-corrupt-child")
			root := seedCoverageMemory(t, s, "metadata-corrupt-root")
			require.NoError(t, s.SetMemoryLineage(child.MemoryID, []tx.ParentRef{root}))
			require.NoError(t, s.SetAppV30Provenance(root.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
			switch kind {
			case "oversized membership key":
				// Put corruption on the ancestor: the mandatory direct check
				// does not read it when no incomplete headers exist.
				key := append(taint.MembershipPrefix(root.MemoryID), strings.Repeat("a", taint.DecisionIDBytes+1)...)
				require.NoError(t, s.SetRawForTest(key, make([]byte, 11)))
			case "oversized parent key":
				key := append(lineage.ForwardPrefix(root.MemoryID), strings.Repeat("b", 2*lineage.ParentIDBytes+1)...)
				require.NoError(t, s.SetRawForTest(key, []byte("invalid")))
			case "cycle":
				// Deliberately malformed stored graph, not a claim that the
				// admission path permits this state. Another exact leaf must
				// not hide the cyclic branch.
				a := seedCoverageMemory(t, s, "metadata-cycle-a")
				b := seedCoverageMemory(t, s, "metadata-cycle-b")
				known := seedCoverageMemory(t, s, "metadata-cycle-known")
				require.NoError(t, s.SetAppV30Provenance(known.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("b")}))
				require.NoError(t, s.SetMemoryLineage(a.MemoryID, []tx.ParentRef{b}))
				refs := []tx.ParentRef{a, known}
				tx.SortParentRefs(refs)
				require.NoError(t, s.SetMemoryLineage(b.MemoryID, refs))
				require.NoError(t, s.SetMemoryLineage(root.MemoryID, []tx.ParentRef{a}))
			}
			got, err := s.ClassifyMemoryTaint(child.MemoryID)
			require.NoError(t, err)
			require.Equal(t, TaintServingClear, got.Outcome)
			require.Equal(t, 1, got.Nodes)
			require.False(t, got.CoverageComplete)
			require.False(t, got.LineageMissing)
		})
	}
}

func TestTaintServingMetadataEffectiveDecisionFlags(t *testing.T) {
	s := openTaintStore(t)
	root := seedCoverageMemory(t, s, "metadata-status-root")
	require.NoError(t, s.SetAppV30Provenance(root.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
	ids := make([]string, 0, 2)
	for _, status := range []taint.Status{taint.StatusDepthExhausted, taint.StatusWorkExhausted} {
		id := testTaintDecisionID(string(rune(status)))
		_, err := s.ApplyTaintDecision("sage", id, root, testTaintPayloadHash(id))
		require.NoError(t, err)
		header, found, err := s.GetTaintDecision(id)
		require.NoError(t, err)
		require.True(t, found)
		// Decoder/status fixture only: this does not claim an actual depth or
		// decision-work threshold was reached by executing a large expansion.
		header.Status = status
		raw, err := taint.EncodeHeader(header)
		require.NoError(t, err)
		require.NoError(t, s.SetRawForTest(taint.DecisionKey(id), raw))
		ids = append(ids, id)
	}
	got, err := s.ClassifyMemoryTaint(root.MemoryID)
	require.NoError(t, err)
	require.Equal(t, TaintServingTainted, got.Outcome)
	require.True(t, got.DepthExhausted)
	require.True(t, got.WorkExhausted)
	require.False(t, got.CoverageComplete)
	_, err = s.ClearTaintDecision(ids[0])
	require.NoError(t, err)
	got, err = s.ClassifyMemoryTaint(root.MemoryID)
	require.NoError(t, err)
	require.False(t, got.DepthExhausted, "an ineffective cleaning membership does not report its former status")
	require.True(t, got.WorkExhausted)
}

func TestTaintServingMetadataUsesPolicySnapshot(t *testing.T) {
	s := openTaintStore(t)
	record := servingMemoryRecord("metadata-policy-root", "metadata policy content")
	require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
	require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, s.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
	require.NoError(t, s.SetMemoryClassification(record.MemoryID, 3))
	scope := NewTaintServingReadScope()
	state, _, allowed, err := s.ClassifyMemoryProjectionForRead(record, scope,
		func(*BadgerStore, *MemoryDisclosureState) (bool, error) { return false, nil })
	require.NoError(t, err)
	require.False(t, allowed)
	require.Nil(t, state)
	require.Equal(t, taint.MaxServingAncestorWalk, scope.coverageRemaining, "denial precedes optional metadata reads")

	require.NoError(t, s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
		require.NoError(t, s.SetAppV30Provenance(record.MemoryID, []AppV30ProvenanceSigner{testAppV30Signer("a")}))
		got, err := reader.classifyMemoryTaintForRead(record.MemoryID, NewTaintServingReadScope())
		require.NoError(t, err)
		require.False(t, got.CoverageComplete)
		require.True(t, got.LineageMissing, "metadata must remain on the pre-publication policy/taint view")
		return nil
	}))
	got, err := s.ClassifyMemoryTaint(record.MemoryID)
	require.NoError(t, err)
	require.True(t, got.CoverageComplete, "a new snapshot observes the exact provenance")
}
