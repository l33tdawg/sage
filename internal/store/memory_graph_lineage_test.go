package store

import (
	"crypto/sha256"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/tx"
)

func testLineageParent(chain, memory, content string) tx.ParentRef {
	return tx.ParentRef{
		SourceChainID: chain,
		MemoryID:      memory,
		ContentHash:   sha256.Sum256([]byte(content)),
	}
}

func openLineageStore(t *testing.T) *BadgerStore {
	t.Helper()
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.db.Close() })
	return s
}

func TestMemoryLineageWritesBothDirectionsIdempotently(t *testing.T) {
	s := openLineageStore(t)
	refs := []tx.ParentRef{
		testLineageParent("sage", "parent-b", "content-b"),
		testLineageParent("sage", "parent-a", "content-a"),
	}
	tx.SortParentRefs(refs)
	require.NoError(t, s.SetMemoryLineage("child", refs))
	require.NoError(t, s.SetMemoryLineage("child", refs), "exact replay is idempotent")

	got, err := s.GetMemoryLineage("child")
	require.NoError(t, err)
	assert.Equal(t, refs, got)

	for _, ref := range refs {
		parentID, err := tx.ParentRefIDHex(ref)
		require.NoError(t, err)
		require.NoError(t, s.view(func(txn *badger.Txn) error {
			if _, err := txn.Get(lineage.ForwardKey("child", parentID)); err != nil {
				return err
			}
			_, err := txn.Get(lineage.ReverseKey(parentID, "child"))
			return err
		}))
	}
}

func TestMemoryLineageRejectsDifferentExistingSet(t *testing.T) {
	s := openLineageStore(t)
	refs := []tx.ParentRef{testLineageParent("sage", "parent-a", "content-a")}
	require.NoError(t, s.SetMemoryLineage("child", refs))

	changed := []tx.ParentRef{testLineageParent("sage", "parent-a", "different-content")}
	err := s.SetMemoryLineage("child", changed)
	require.ErrorIs(t, err, ErrMemoryLineageConflict)

	other := []tx.ParentRef{testLineageParent("sage", "parent-b", "content-b")}
	err = s.SetMemoryLineage("child", other)
	require.ErrorIs(t, err, ErrMemoryLineageConflict)
}

func TestMemoryLineageDetectsHalfEdges(t *testing.T) {
	s := openLineageStore(t)
	ref := testLineageParent("sage", "parent", "content")
	parentID, err := tx.ParentRefIDHex(ref)
	require.NoError(t, err)

	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, lineage.ReverseKey(parentID, "child"), []byte{})
	}))
	err = s.SetMemoryLineage("child", []tx.ParentRef{ref})
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)

	other := openLineageStore(t)
	require.NoError(t, other.update(func(txn *badger.Txn) error {
		value, valueErr := tx.EncodeParentRefValue(ref)
		if valueErr != nil {
			return valueErr
		}
		return other.txnSet(txn, lineage.ForwardKey("child", parentID), value)
	}))
	_, err = other.GetMemoryLineage("child")
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)
}

func TestListLineageChildrenIsDeterministicAndPaged(t *testing.T) {
	s := openLineageStore(t)
	parent := testLineageParent("sage", "parent", "content")
	parentID, err := tx.ParentRefIDHex(parent)
	require.NoError(t, err)
	for _, child := range []string{"child-c", "child-a", "child-d", "child-b"} {
		require.NoError(t, s.SetMemoryLineage(child, []tx.ParentRef{parent}))
	}

	first, err := s.ListLineageChildren(parentID, "", 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"child-a", "child-b"}, first)

	second, err := s.ListLineageChildren(parentID, first[len(first)-1], 2)
	require.NoError(t, err)
	assert.Equal(t, []string{"child-c", "child-d"}, second)

	_, err = s.ListLineageChildren(parentID, "", 0)
	require.Error(t, err)
	_, err = s.ListLineageChildren("not-hex", "", 1)
	require.Error(t, err)
}

func TestMemoryLineageParticipatesInConsensusTransaction(t *testing.T) {
	base := openLineageStore(t)
	scoped := base.BeginConsensusTransaction(nil)
	defer scoped.DiscardConsensusTransaction()
	refs := []tx.ParentRef{testLineageParent("sage", "parent", "content")}
	require.NoError(t, scoped.SetMemoryLineage("child", refs))

	invisible, err := base.GetMemoryLineage("child")
	require.NoError(t, err)
	assert.Empty(t, invisible)

	require.NoError(t, scoped.CommitConsensusTransaction())
	visible, err := base.GetMemoryLineage("child")
	require.NoError(t, err)
	assert.Equal(t, refs, visible)
}

func TestValidateMemoryLineageParents(t *testing.T) {
	s := openLineageStore(t)
	committed := testLineageParent("sage", "parent-committed", "committed-content")
	challenged := testLineageParent("sage", "parent-challenged", "challenged-content")
	require.NoError(t, s.SetMemoryHash(
		committed.MemoryID, committed.ContentHash[:], "committed",
	))
	require.NoError(t, s.SetMemoryHash(
		challenged.MemoryID, challenged.ContentHash[:], "challenged",
	))
	refs := []tx.ParentRef{committed, challenged}
	tx.SortParentRefs(refs)
	require.NoError(t, s.ValidateMemoryLineageParents("child", "sage", refs))

	crossChain := []tx.ParentRef{testLineageParent("other-sage", "parent-committed", "committed-content")}
	err := s.ValidateMemoryLineageParents("child", "sage", crossChain)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)

	self := []tx.ParentRef{testLineageParent("sage", "child", "content")}
	err = s.ValidateMemoryLineageParents("child", "sage", self)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)

	wrongHash := []tx.ParentRef{testLineageParent("sage", "parent-committed", "wrong-content")}
	err = s.ValidateMemoryLineageParents("child", "sage", wrongHash)
	require.ErrorIs(t, err, ErrMemoryLineageUnavailable)

	missing := []tx.ParentRef{testLineageParent("sage", "missing", "content")}
	err = s.ValidateMemoryLineageParents("child", "sage", missing)
	require.ErrorIs(t, err, ErrMemoryLineageUnavailable)

	deprecated := testLineageParent("sage", "parent-deprecated", "deprecated-content")
	require.NoError(t, s.SetMemoryHash(
		deprecated.MemoryID, deprecated.ContentHash[:], "deprecated",
	))
	err = s.ValidateMemoryLineageParents(
		"child", "sage", []tx.ParentRef{deprecated},
	)
	require.ErrorIs(t, err, ErrMemoryLineageUnavailable)
}

func TestMemoryLineageRejectsMalformedInputs(t *testing.T) {
	s := openLineageStore(t)
	err := s.SetMemoryLineage("", []tx.ParentRef{testLineageParent("sage", "parent", "content")})
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)
	err = s.SetMemoryLineage("child", nil)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)

	unsorted := []tx.ParentRef{
		testLineageParent("sage", "parent-b", "content-b"),
		testLineageParent("sage", "parent-a", "content-a"),
	}
	err = s.SetMemoryLineage("child", unsorted)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)

	tooLong := []tx.ParentRef{
		testLineageParent("sage", strings.Repeat("p", tx.MaxDerivedFromMemoryIDBytes+1), "content"),
	}
	err = s.SetMemoryLineage("child", tooLong)
	require.ErrorIs(t, err, ErrMemoryLineageMalformed)

	_, err = s.GetMemoryLineage("")
	require.Error(t, err)
}
