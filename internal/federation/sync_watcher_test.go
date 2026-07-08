package federation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestSyncWatcherEnqueuesAndNudges(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	seedCommitted(t, ms, "m-in", "hr.public", "watched fact")   // consented subtree
	seedCommitted(t, ms, "m-out", "eng", "unconsented fact")    // outside consent
	seedCommitted(t, ms, "m-copy", "hr", "copy fact")           // synced copy: never re-forward
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "orig-1", LocalMemoryID: "m-copy",
		DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted,
	}))

	// Simulate the drainer having been started (creates the nudge channel).
	m.syncNudge = make(chan struct{}, 1)

	m.SyncWatcher()([]string{"m-in", "m-out", "m-copy", "m-unknown"})

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStatePending], "only the consented, non-copy memory enqueues")
	pending, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStatePending, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "m-in", pending[0].MemoryID)

	select {
	case <-m.syncNudge:
		// nudged — drainer would wake
	default:
		t.Fatal("expected a drainer nudge after enqueue")
	}

	// Re-fire (block replay): INSERT OR IGNORE absorbs it, nudge is
	// non-blocking even with nobody consuming.
	m.SyncWatcher()([]string{"m-in"})
	counts, _ = ms.CountSyncOutboxByState(ctx, "chain-b")
	assert.Equal(t, 1, counts[store.SyncStatePending])
}

func TestSyncWatcherNilSafety(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-1", "hr", "fact")

	// No nudge channel (drainer never started): the watcher must still
	// enqueue without panicking or blocking.
	m.syncNudge = nil
	m.SyncWatcher()([]string{"m-1"})

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStatePending])

	// Empty batch and no consent chains: cheap no-ops.
	m.SyncWatcher()(nil)
	require.NoError(t, ms.DeleteSyncDomains(ctx, "chain-b"))
	m.SyncWatcher()([]string{"m-1"})
}
