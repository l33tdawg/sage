package store

import (
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestRebuildLineageReverseIndexUsesForwardEdges(t *testing.T) {
	s := openLineageStore(t)
	refs := []tx.ParentRef{
		testLineageParent("sage", "parent-a", "content-a"),
		testLineageParent("sage", "parent-b", "content-b"),
	}
	tx.SortParentRefs(refs)
	require.NoError(t, s.SetMemoryLineage("child", refs))
	require.NoError(t, s.ValidateLineageIndexes())

	parentID, err := tx.ParentRefIDHex(refs[0])
	require.NoError(t, err)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnDelete(txn, lineage.ReverseKey(parentID, "child"))
	}))
	require.ErrorIs(t, s.ValidateLineageIndexes(), ErrAppV30LineageIndex)

	inserted, err := s.RebuildLineageReverseIndex()
	require.NoError(t, err)
	require.Equal(t, 1, inserted)
	require.NoError(t, s.ValidateLineageIndexes())
	inserted, err = s.RebuildLineageReverseIndex()
	require.NoError(t, err)
	require.Zero(t, inserted)
}

func TestValidateLineageIndexesRejectsReverseOnlyEdge(t *testing.T) {
	s := openLineageStore(t)
	parentID := strings.Repeat("a", lineage.ParentIDBytes*2)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, lineage.ReverseKey(parentID, "orphan-child"), []byte{})
	}))
	require.ErrorIs(t, s.ValidateLineageIndexes(), ErrAppV30LineageIndex)
}

func TestRebuildLineageReverseIndexRejectsMalformedForwardValue(t *testing.T) {
	s := openLineageStore(t)
	parentID := strings.Repeat("a", lineage.ParentIDBytes*2)
	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, lineage.ForwardKey("child", parentID), []byte("malformed"))
	}))
	_, err := s.RebuildLineageReverseIndex()
	require.ErrorIs(t, err, ErrAppV30LineageIndex)
}
