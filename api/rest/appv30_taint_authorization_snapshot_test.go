package rest

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/appv23disclosure"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// This deterministic callback interleaving exercises the unified store read
// contract with real access-group and taint mutations. It is not a concurrent
// HTTP route, signed transaction, or network fixture.
func TestMemoryProjectionReadPolicyRevocationAndTaintClearStayAtomic(t *testing.T) {
	srv, rows, canonical, _, readerID, ownerID, _ := appV23DisclosureFixture(t)
	at := time.Now()
	record := appV23PagingRecord("snapshot-revoke-clear", ownerID, "owner.home",
		"unchanged canonical envelope", memory.TypeFact, memory.StatusCommitted, at)
	rows.memories[record.MemoryID] = record
	publishAppV23RESTRecord(t, canonical, record, 1)

	before, err := srv.evaluateAppV23RecordDisclosure(readerID, record, at)
	require.NoError(t, err)
	require.True(t, before.Allowed, "fixture must initially grant group read access")
	envelopeBefore, err := canonical.GetMemoryDisclosureState(record.MemoryID)
	require.NoError(t, err)
	ref := tx.ParentRef{SourceChainID: "sage", MemoryID: record.MemoryID}
	copy(ref.ContentHash[:], record.ContentHash)
	decisionID := strings.Repeat("e", taint.DecisionIDBytes)
	created, err := canonical.ApplyTaintDecision("sage", decisionID, ref,
		sha256.Sum256([]byte("snapshot revoke-clear decision")))
	require.NoError(t, err)
	require.True(t, created)
	initialVerdict, err := canonical.ClassifyMemoryTaint(record.MemoryID)
	require.NoError(t, err)
	require.Equal(t, store.TaintServingTainted, initialVerdict.Outcome)

	policyRecord := appv23disclosure.Record{
		SubmittingAgent: envelopeBefore.Author,
		Domain:          envelopeBefore.Domain,
		Classification:  envelopeBefore.Classification,
	}
	callbackCalled := false
	state, disposition, policyAllowed, err := canonical.ClassifyMemoryProjectionForRead(
		record, store.NewTaintServingReadScope(),
		func(reader *store.BadgerStore, envelope *store.MemoryDisclosureState) (bool, error) {
			callbackCalled = true
			require.Equal(t, envelopeBefore, envelope)
			pinnedPolicy, policyErr := appv23disclosure.Evaluate(reader, readerID, policyRecord, at)
			require.NoError(t, policyErr)
			require.True(t, pinnedPolicy.Allowed)

			// The canonical memory envelope is unchanged in all three states:
			// allowed+tainted -> denied+tainted -> denied+clear.
			require.NoError(t, canonical.MutateAppV23AccessGroup(
				appV23RESTAgentID("11"), "local-team", "Local team",
				[]string{ownerID}, 1, false, 4,
			))
			livePolicy, liveErr := appv23disclosure.Evaluate(canonical, readerID, policyRecord, at)
			require.NoError(t, liveErr)
			require.False(t, livePolicy.Allowed,
				"no grant, legacy compatibility or other authority may keep the fixture readable")
			changed, clearErr := canonical.ClearTaintDecision(decisionID)
			require.NoError(t, clearErr)
			require.True(t, changed)
			liveVerdict, liveErr := canonical.ClassifyMemoryTaint(record.MemoryID)
			require.NoError(t, liveErr)
			require.Equal(t, store.TaintServingClear, liveVerdict.Outcome)
			liveEnvelope, liveErr := canonical.GetMemoryDisclosureState(record.MemoryID)
			require.NoError(t, liveErr)
			require.Equal(t, envelopeBefore, liveEnvelope,
				"an envelope mismatch must not be what catches this interleaving")

			pinnedAgain, policyErr := appv23disclosure.Evaluate(reader, readerID, policyRecord, at)
			require.NoError(t, policyErr)
			require.True(t, pinnedAgain.Allowed, "supplied reader must still hold the original policy view")
			return pinnedPolicy.Allowed, nil
		},
	)
	require.True(t, callbackCalled)
	require.True(t, policyAllowed, "this is the old snapshot's policy result, not disclosure permission")
	require.Nil(t, state, "old authorization must not combine with later clean taint")
	require.Equal(t, store.MemoryProjectionTainted, disposition)
	require.ErrorIs(t, err, store.ErrMemoryProjectionTainted)
	require.ErrorIs(t, err, store.ErrMemoryProjectionUnpublished)

	after, err := srv.evaluateAppV23RecordDisclosure(readerID, record, at)
	require.NoError(t, err, "the later denied+clear view is an ordinary denial")
	require.False(t, after.Allowed)
}
