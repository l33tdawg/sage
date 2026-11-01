package store

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func seedValidTaintState(t *testing.T, s *BadgerStore) (string, tx.ParentRef) {
	t.Helper()
	decisionID := testTaintDecisionID("recovery")
	target := testTaintParent("recovery-target")
	created, err := s.ApplyTaintDecision(
		"sage",
		decisionID,
		target,
		testTaintPayloadHash("recovery"),
	)
	require.NoError(t, err)
	require.True(t, created)
	return decisionID, target
}

func TestValidateTaintStateAcceptsCompleteAndCleanupStates(t *testing.T) {
	s := openTaintStore(t)
	decisionID, _ := seedValidTaintState(t, s)
	require.NoError(t, s.ValidateTaintState())

	_, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.NoError(t, s.ValidateTaintState())

	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	require.NoError(t, s.ValidateTaintState())
}

func TestValidateTaintStateRejectsDirectionAndCounterDamage(t *testing.T) {
	t.Run("missing index", func(t *testing.T) {
		s := openTaintStore(t)
		decisionID, target := seedValidTaintState(t, s)
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return s.txnDelete(txn, taint.DecisionIndexKey(decisionID, target.MemoryID))
		}))
		require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
	})

	t.Run("active count", func(t *testing.T) {
		s := openTaintStore(t)
		seedValidTaintState(t, s)
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return writeTaintUint64Txn(s, txn, taint.ActiveCountKey(), 2)
		}))
		require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
	})

	t.Run("frontier gap", func(t *testing.T) {
		s := openTaintStore(t)
		decisionID, _ := seedValidTaintState(t, s)
		require.NoError(t, s.update(func(txn *badger.Txn) error {
			return s.txnDelete(txn, taint.FrontierKey(decisionID, 1, 0))
		}))
		require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed)
	})
}
