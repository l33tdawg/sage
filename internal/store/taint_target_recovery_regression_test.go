package store

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
)

func TestRecoveryRejectsLostActiveTargetPair(t *testing.T) {
	for _, completed := range []bool{false, true} {
		name := "queued"
		if completed {
			name = "completed without frontier"
		}
		t.Run(name, func(t *testing.T) {
			s := openTaintStore(t)
			record := servingMemoryRecord("target", "retained committed content")
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
			require.NoError(t, s.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
			require.NoError(t, s.SetMemoryClassification(record.MemoryID, 0))
			target := testTaintParent(record.MemoryID)
			copy(target.ContentHash[:], record.ContentHash)
			decisionID := testTaintDecisionID("target-loss")
			created, err := s.ApplyTaintDecision("sage", decisionID, target, testTaintPayloadHash("target-loss"))
			require.NoError(t, err)
			require.True(t, created)
			if completed {
				result, expandErr := s.ExpandTaintDecision("sage", decisionID, taint.PerExecutionUnitBudget)
				require.NoError(t, expandErr)
				require.Equal(t, taint.StatusComplete, result.Status)
				require.NoError(t, s.view(func(txn *badger.Txn) error {
					found, readErr := keyExistsTxn(txn, taint.FrontierKey(decisionID, 1, 0))
					require.NoError(t, readErr)
					require.False(t, found)
					return nil
				}))
			}
			require.NoError(t, s.ValidateTaintState(), "intact canonical state must pass recovery")
			_, disposition, err := s.ClassifyMemoryProjection(record)
			require.ErrorIs(t, err, ErrMemoryProjectionTainted)
			require.Equal(t, MemoryProjectionTainted, disposition)
			headerBefore := readTaintValue(t, s, taint.DecisionKey(decisionID))
			require.NoError(t, s.update(func(txn *badger.Txn) error {
				if deleteErr := s.txnDelete(txn, taint.MembershipKey(record.MemoryID, decisionID)); deleteErr != nil {
					return deleteErr
				}
				return s.txnDelete(txn, taint.DecisionIndexKey(decisionID, record.MemoryID))
			}))
			require.Equal(t, headerBefore, readTaintValue(t, s, taint.DecisionKey(decisionID)))
			count, err := s.ActiveTaintDecisionCount()
			require.NoError(t, err)
			require.Equal(t, uint64(1), count)
			_, afterDisposition, afterErr := s.ClassifyMemoryProjection(record)
			t.Logf("after paired loss: projection disposition=%v error=%v", afterDisposition, afterErr)
			require.ErrorIs(t, s.ValidateTaintState(), ErrTaintStateMalformed,
				"recovery cannot admit an active decision whose exact target pair disappeared")
		})
	}
}
