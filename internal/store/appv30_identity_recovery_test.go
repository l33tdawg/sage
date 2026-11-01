package store

import (
	"encoding/binary"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV30IdentityRecoveryParsers(t *testing.T) {
	hash := strings.Repeat("a", taint.DecisionIDBytes)
	for _, id := range []string{"victim", "victim:x", "\x00:\xff", "é:\x00", strings.Repeat("m", taint.MaxMemoryIDBytes)} {
		memoryID, decisionID, err := parseTaintMembershipKey(string(taint.MembershipKey(id, hash)))
		require.NoError(t, err)
		require.Equal(t, id, memoryID)
		require.Equal(t, hash, decisionID)
		childID, parentID, err := parseLineageForwardKey(string(lineage.ForwardKey(id, hash)))
		require.NoError(t, err)
		require.Equal(t, id, childID)
		require.Equal(t, hash, parentID)
	}
	for _, prefix := range []string{"taintm:", "lineage:"} {
		parse := parseTaintMembershipKey
		if prefix == "lineage:" {
			parse = parseLineageForwardKey
		}
		for _, key := range []string{
			prefix,
			prefix + "\x00",
			prefix + "victim:" + hash, // No fallback to the ambiguous dormant layout.
			prefix + "\x00\x00:" + hash,
			prefix + "\x02\x01" + strings.Repeat("m", 513) + ":" + hash,
			prefix + "\x00\x06short:" + hash,
			prefix + "\x00\x06victim;" + hash,
			prefix + "\x00\x06victim:" + hash[:63],
			prefix + "\x00\x06victim:" + hash + "x",
			prefix + "\x00\x06victim:" + strings.Repeat("A", 64),
			"wrong:" + "\x00\x06victim:" + hash,
		} {
			_, _, err := parse(key)
			require.Error(t, err, "malformed key %q", key)
		}
	}
}

func TestLineageRecoveryRejectsLengthTamperWithoutAcceptingOldLayout(t *testing.T) {
	for _, oldLayout := range []bool{false, true} {
		dir := t.TempDir()
		s, err := NewBadgerStore(dir)
		require.NoError(t, err)
		parent := testTaintParent("parent")
		require.NoError(t, s.SetMemoryLineage("victim:x", []tx.ParentRef{parent}))
		parentID, err := tx.ParentRefIDHex(parent)
		require.NoError(t, err)
		key := lineage.ForwardKey("victim:x", parentID)
		value := readTaintValue(t, s, key)
		changedKey := append([]byte(nil), key...)
		if oldLayout {
			changedKey = append(append([]byte(nil), key[:len("lineage:")]...), key[len("lineage:")+2:]...)
		} else {
			binary.BigEndian.PutUint16(changedKey[len("lineage:"):], 7)
		}
		require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
			if deleteErr := txn.Delete(key); deleteErr != nil {
				return deleteErr
			}
			return txn.Set(changedKey, value)
		}))
		require.NoError(t, s.CloseBadger())
		s, err = NewBadgerStore(dir)
		require.NoError(t, err)
		require.ErrorIs(t, s.ValidateLineageIndexes(), ErrAppV30LineageIndex)
		_, err = s.RebuildLineageReverseIndex()
		require.ErrorIs(t, err, ErrAppV30LineageIndex)
		require.NoError(t, s.CloseBadger())
	}
}

func TestTaintRecoveryRejectsLengthTamperWithoutAcceptingOldLayout(t *testing.T) {
	for _, replacement := range []func([]byte) []byte{
		func(key []byte) []byte {
			binary.BigEndian.PutUint16(key[len("taintm:"):], 7)
			return key
		},
		func(key []byte) []byte {
			return append(append([]byte(nil), key[:len("taintm:")]...), key[len("taintm:")+2:]...)
		},
	} {
		s := openTaintStore(t)
		decisionID := testTaintDecisionID("tamper")
		created, err := s.ApplyTaintDecision("sage", decisionID, testTaintParent("victim"), testTaintPayloadHash("tamper"))
		require.NoError(t, err)
		require.True(t, created)
		key := taint.MembershipKey("victim", decisionID)
		value := readTaintValue(t, s, key)
		changedKey := replacement(append([]byte(nil), key...))
		require.NoError(t, s.DB().Update(func(txn *badger.Txn) error {
			if err := txn.Delete(key); err != nil {
				return err
			}
			return txn.Set(changedKey, value)
		}))
		require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
	}
}
