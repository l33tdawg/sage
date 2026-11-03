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

func TestRecoveryValidatorsUseCanonicalMemoryIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   string
	}{
		{"ordinary", "child"},
		{"colon", "child:part:"},
		{"nul", "child\x00part"},
		{"binary", string([]byte{0xff, ':', 0, 0x80, 'x'})},
		{"maximum", strings.Repeat("x", taint.MaxMemoryIDBytes)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("lineage rebuild", func(t *testing.T) {
				s := openTaintStore(t)
				ref := testTaintParent("parent")
				require.NoError(t, s.SetMemoryLineage(tc.id, []tx.ParentRef{ref}))
				require.NoError(t, s.ValidateLineageIndexes())
				parentID, err := tx.ParentRefIDHex(ref)
				require.NoError(t, err)
				require.NoError(t, s.update(func(txn *badger.Txn) error {
					return s.txnDelete(txn, lineage.ReverseKey(parentID, tc.id))
				}))
				require.ErrorIs(t, s.ValidateLineageIndexes(), ErrAppV30LineageIndex)
				n, err := s.RebuildLineageReverseIndex()
				require.NoError(t, err)
				require.Equal(t, 1, n)
				require.Empty(t, readTaintValue(t, s, lineage.ReverseKey(parentID, tc.id)))
				require.NoError(t, s.ValidateLineageIndexes())
				n, err = s.RebuildLineageReverseIndex()
				require.NoError(t, err)
				require.Zero(t, n)
			})
			t.Run("taint apply and expand", func(t *testing.T) {
				s := openTaintStore(t)
				decisionID := testTaintDecisionID(tc.name)
				created, err := s.ApplyTaintDecision("sage", decisionID,
					testTaintParent(tc.id), testTaintPayloadHash(tc.name))
				require.NoError(t, err)
				require.True(t, created)
				decisions, err := s.GetEffectiveTaintDecisions(tc.id)
				require.NoError(t, err)
				require.Equal(t, []string{decisionID}, decisions)
				require.NoError(t, s.ValidateTaintState())
				_, err = s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
				require.NoError(t, s.ValidateTaintState())
			})
		})
	}
}

func TestRecoveryKeyParsersRejectMalformedFraming(t *testing.T) {
	for _, parser := range []struct {
		name   string
		prefix string
		key    func(string, string) []byte
		parse  func(string) (string, string, error)
		err    error
	}{
		{"lineage", "lineage:", lineage.ForwardKey, parseLineageForwardKey, ErrAppV30LineageIndex},
		{"membership", "taintm:", taint.MembershipKey, parseTaintMembershipKey, ErrTaintStateMalformed},
	} {
		t.Run(parser.name, func(t *testing.T) {
			fixedID := strings.Repeat("a", 64)
			valid := parser.key("child", fixedID)
			withLength := func(n uint16) []byte {
				key := append([]byte(nil), valid...)
				binary.BigEndian.PutUint16(key[len(parser.prefix):], n)
				return key
			}
			wrongDelimiter := append([]byte(nil), valid...)
			wrongDelimiter[len(parser.prefix)+2+len("child")] = 'x'
			oversized := append([]byte(parser.prefix), 2, 1)
			oversized = append(oversized, strings.Repeat("x", 513)...)
			oversized = append(oversized, ':')
			oversized = append(oversized, fixedID...)
			for _, tc := range []struct {
				name string
				key  []byte
			}{
				{"missing prefix", valid[len(parser.prefix):]},
				{"wrong prefix", append([]byte("other:"), valid[len(parser.prefix):]...)},
				{"short length", append([]byte(parser.prefix), 0)},
				{"zero length", withLength(0)},
				{"short declared length", withLength(4)},
				{"long declared length", withLength(6)},
				{"maximum uint16 length", withLength(65535)},
				{"oversized ID", oversized},
				{"wrong delimiter", wrongDelimiter},
				{"truncated fixed ID", valid[:len(valid)-1]},
				{"trailing byte", append(append([]byte(nil), valid...), 'a')},
				{"uppercase fixed ID", parser.key("child", strings.Repeat("A", 64))},
				{"nonhex fixed ID", parser.key("child", strings.Repeat("z", 64))},
				{"legacy unframed", []byte(parser.prefix + "child:" + fixedID)},
			} {
				t.Run(tc.name, func(t *testing.T) {
					_, _, err := parser.parse(string(tc.key))
					require.ErrorIs(t, err, parser.err)
				})
			}
		})
	}
}
