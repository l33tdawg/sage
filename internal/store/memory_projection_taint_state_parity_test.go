package store

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestClassifyMemoryProjectionsTaintStateParity(t *testing.T) {
	for _, tc := range []struct {
		name        string
		disposition MemoryProjectionDisposition
		cause       error
		unpublished bool
	}{
		{"confirmed", MemoryProjectionTainted, ErrMemoryProjectionTainted, true},
		{"pending", MemoryProjectionTaintPending, ErrMemoryProjectionTaintPending, true},
		{"malformed", MemoryProjectionUnpublished, ErrTaintStateMalformed, false},
		{"budget", MemoryProjectionTaintIndeterminate, ErrTaintServingWalkBudget, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openTaintStore(t)
			record := servingMemoryRecord("parity-"+tc.name, "canonical parity content")
			if tc.name == "budget" {
				record = seedServingDeepRecord(t, s, "parity-budget")
			}
			require.NoError(t, s.SetMemoryHash(record.MemoryID, record.ContentHash, string(record.Status)))
			require.NoError(t, s.SetMemoryDomain(record.MemoryID, record.DomainTag))
			require.NoError(t, s.SetMemoryAuthor(record.MemoryID, record.SubmittingAgent))
			require.NoError(t, s.SetMemoryClassification(record.MemoryID, 1))

			decisionID := testTaintDecisionID("state-parity-" + tc.name)
			switch tc.name {
			case "budget":
				seedServingIncompleteDecision(t, s, "parity-unrelated-incomplete")
			default:
				ref := tx.ParentRef{SourceChainID: "sage", MemoryID: record.MemoryID}
				copy(ref.ContentHash[:], record.ContentHash)
				if tc.name == "pending" {
					ref = testTaintParent("parity-pending-parent")
					require.NoError(t, s.SetMemoryHash(ref.MemoryID, ref.ContentHash[:], string(memory.StatusCommitted)))
					require.NoError(t, s.ValidateMemoryLineageParents(record.MemoryID, "sage", []tx.ParentRef{ref}))
					require.NoError(t, s.SetMemoryLineage(record.MemoryID, []tx.ParentRef{ref}))
				}
				created, err := s.ApplyTaintDecision("sage", decisionID, ref,
					testTaintPayloadHash("state-parity-"+tc.name))
				require.NoError(t, err)
				require.True(t, created)
				if tc.name == "pending" {
					memberships, err := s.GetEffectiveTaintDecisions(record.MemoryID)
					require.NoError(t, err)
					require.Empty(t, memberships, "pending descendant must not be directly materialized")
				}
				if tc.name == "malformed" {
					require.NoError(t, s.SetRawForTest(taint.DecisionKey(decisionID), []byte("invalid header")))
				}
			}

			// Establish the real taint outcome/error before comparing serving APIs.
			verdict, classifyErr := s.ClassifyMemoryTaint(record.MemoryID)
			if tc.name == "confirmed" || tc.name == "pending" {
				require.NoError(t, classifyErr)
				want := TaintServingTainted
				if tc.name == "pending" {
					want = TaintServingPending
				}
				require.Equal(t, want, verdict.Outcome)
			} else {
				require.ErrorIs(t, classifyErr, tc.cause)
			}

			state, disposition, singleErr := s.ClassifyMemoryProjection(record)
			require.Nil(t, state)
			require.Equal(t, tc.disposition, disposition)
			require.ErrorIs(t, singleErr, tc.cause)
			batch, err := s.ClassifyMemoryProjections([]*memory.MemoryRecord{record})
			require.NoError(t, err, "valid gate reads leave classification failures per-row")
			require.Len(t, batch, 1)
			require.Nil(t, batch[0].State, "an envelope validated before taint failure is not a discloseable State")
			require.Equal(t, disposition, batch[0].Disposition)
			require.EqualError(t, batch[0].Err, singleErr.Error())
			require.ErrorIs(t, batch[0].Err, tc.cause)
			for _, projectionErr := range []error{singleErr, batch[0].Err} {
				if tc.unpublished {
					require.ErrorIs(t, projectionErr, ErrMemoryProjectionUnpublished)
				} else {
					require.NotErrorIs(t, projectionErr, ErrMemoryProjectionUnpublished,
						"malformed state and budget uncertainty must not become skippable rows")
				}
				if tc.name == "budget" {
					require.ErrorIs(t, projectionErr, ErrMemoryProjectionTaintIndeterminate)
				} else {
					require.NotErrorIs(t, projectionErr, ErrMemoryProjectionTaintIndeterminate)
				}
			}
		})
	}
}
