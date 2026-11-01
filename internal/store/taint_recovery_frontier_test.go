package store

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
)

func TestValidateTaintStateRejectsMisboundFrontierKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  func(string) []byte
	}{
		{name: "wrong generation", key: func(id string) []byte { return taint.FrontierKey(id, 2, 0) }},
		{name: "sequence outside frontier", key: func(id string) []byte { return taint.FrontierKey(id, 1, 1) }},
		{name: "trailing bytes", key: func(id string) []byte { return append(taint.FrontierKey(id, 1, 0), 0) }},
		{name: "missing sequence separator", key: func(id string) []byte {
			key := taint.FrontierKey(id, 1, 0)
			key[len("taintq:")+taint.DecisionIDBytes+1+8] = 'x'
			return key
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTaintStore(t)
			decisionID, _ := seedValidTaintState(t, s)
			require.NoError(t, s.ValidateTaintState())
			require.NoError(t, s.update(func(txn *badger.Txn) error {
				key := taint.FrontierKey(decisionID, 1, 0)
				raw, found, err := readTaintValueTxn(txn, key)
				if err != nil {
					return err
				}
				require.True(t, found)
				if deleteErr := s.txnDelete(txn, key); deleteErr != nil {
					return deleteErr
				}
				return s.txnSet(txn, tc.key(decisionID), raw)
			}))
			_, err := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
			require.ErrorIs(t, err, ErrTaintStateMalformed, "the worker cannot resume this frontier")
			require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed,
				"recovery must reject an unresumable frontier even when its cardinality matches")
		})
	}
}

func TestValidateTaintStateAcceptsRetainedCleaningFrontier(t *testing.T) {
	s := openTaintStore(t)
	decisionID, _ := seedValidTaintState(t, s)
	changed, err := s.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	// Clear increments the header generation while leaving the old frontier
	// for the bounded cleanup worker; it is no longer an active work cursor.
	require.NoError(t, s.ValidateTaintState())
	_, err = s.CleanupTaintDecision(decisionID, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.NoError(t, s.ValidateTaintState())
}
