package federation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func TestSyncWatcherEnqueuesAndNudges(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	seedCommitted(t, ms, "m-in", "hr.public", "watched fact") // consented subtree
	seedCommitted(t, ms, "m-out", "eng", "unconsented fact")  // outside consent
	seedCommitted(t, ms, "m-copy", "hr", "copy fact")         // synced copy: never re-forward
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

func TestNudgeJournalReconcileAndWaitWaitsForPromptedPass(t *testing.T) {
	m, _, _ := newDrainTestManager(t)
	m.syncJournalNudge = make(chan chan struct{}, 1)
	passStarted := make(chan struct{})
	finishPass := make(chan struct{})
	go func() {
		done := <-m.syncJournalNudge
		close(passStarted)
		<-finishPass
		close(done)
	}()

	returned := make(chan error, 1)
	go func() {
		returned <- m.NudgeJournalReconcileAndWait(context.Background())
	}()
	select {
	case <-passStarted:
	case <-time.After(time.Second):
		t.Fatal("prompted reconcile was not queued")
	}
	select {
	case err := <-returned:
		t.Fatalf("refresh returned before the prompted pass finished: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(finishPass)
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("refresh did not return after the prompted pass finished")
	}
}

func TestSyncWatcherWaitsForAdmissionProvenance(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-copy-race", "hr", "incoming copy")

	unlock := ms.LockSyncOriginWrite()
	done := make(chan struct{})
	go func() {
		m.onCommitted([]string{"m-copy-race"})
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("watcher crossed the provenance admission lease")
	case <-time.After(25 * time.Millisecond):
	}
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-peer", OriginMemoryID: "origin", LocalMemoryID: "m-copy-race",
		DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted,
	}))
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watcher did not resume after provenance commit")
	}
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, counts, "an admitted copy must never be re-forwarded")
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
