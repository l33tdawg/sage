package federation

// Low-latency half of the v11.5 domain-sync watcher: the ABCI Commit tail
// hands over this block's committed memory IDs (via SageApp.SetSyncNotifier),
// we enqueue the ones in consented subtrees and nudge the drainer so delivery
// starts without waiting for the 30s tick. The drainer's poll scan remains
// the correctness backstop — everything here is best-effort acceleration,
// and every enqueue is INSERT OR IGNORE (commit replays re-fire the hook).
//
// Wiring order (runServe): StartSyncDrainer FIRST (creates the nudge
// channel), then app.SetSyncNotifier(fedMgr.SyncWatcher()) — the notifier
// only fires from Commit, which starts after boot completes.

import (
	"context"

	"github.com/l33tdawg/sage/internal/store"
)

// SyncWatcher returns the callback for SageApp.SetSyncNotifier.
func (m *Manager) SyncWatcher() func([]string) {
	return m.onCommitted
}

// onCommitted enqueues just-committed memories toward every consented peer.
// Runs on the Commit tail's dispatch goroutine. Memory METADATA is read from
// the SQLite mirror (guaranteed flushed by the notifier contract) — never
// from Badger, which runs a flush ahead. It does consult ActiveAgreement
// (a Badger db.View through the store's own MVCC, so concurrency-safe), which
// may reflect a slightly newer agreement height than this commit; that is
// harmless because every gate is RE-EVALUATED at send time, so a stale-vs-
// fresh scope read only affects whether a row is enqueued now or one tick
// later, never whether out-of-scope data is delivered.
func (m *Manager) onCommitted(ids []string) {
	ss := m.syncStore()
	if ss == nil || len(ids) == 0 {
		return
	}
	ctx := context.Background()
	chains, err := ss.ListSyncDomainChains(ctx)
	if err != nil || len(chains) == 0 {
		return
	}

	// Load per-chain scope once per batch.
	type scope struct {
		agreement *store.CrossFedRecord
		consented []string
	}
	scopes := make([]scope, 0, len(chains))
	for _, chain := range chains {
		agreement, aErr := m.ActiveAgreement(chain)
		if aErr != nil {
			continue // revoked/expired: fail closed and quiet
		}
		consented, cErr := ss.GetSyncDomains(ctx, chain)
		if cErr != nil || len(consented) == 0 {
			continue
		}
		scopes = append(scopes, scope{agreement: agreement, consented: consented})
	}
	if len(scopes) == 0 {
		return
	}

	enqueued := false
	for _, id := range ids {
		// Loop prevention: a copy admitted FROM a peer commits locally and
		// re-fires this hook — never re-forward it (to anyone).
		if isCopy, cErr := ss.IsSyncedCopy(ctx, id); cErr != nil || isCopy {
			continue
		}
		domain, cls, ok := m.syncMetaFor(ctx, ss, id)
		if !ok {
			continue
		}
		for _, sc := range scopes {
			if !DomainAllowed(sc.consented, domain) || !DomainAllowed(sc.agreement.AllowedDomains, domain) {
				continue
			}
			if cls > int(sc.agreement.MaxClearance) {
				continue
			}
			if _, eErr := ss.EnqueueSyncOutbox(ctx, sc.agreement.RemoteChainID, id); eErr != nil {
				m.logger.Warn().Err(eErr).Str("memory", id).Msg("sync watcher: enqueue failed")
				continue
			}
			enqueued = true
		}
	}
	if enqueued {
		m.nudgeSync()
	}
}

// syncMetaFor resolves (domain, classification) for one memory from the
// SQLite mirror. Fail closed: unreadable metadata means no enqueue (the poll
// scan retries the whole question next tick anyway).
func (m *Manager) syncMetaFor(ctx context.Context, ss *store.SQLiteStore, id string) (string, int, bool) {
	cand, err := ss.GetSyncCandidateByID(ctx, id)
	if err != nil || cand == nil {
		return "", 0, false
	}
	return cand.DomainTag, cand.Classification, true
}

// NudgeSync wakes the outbox drainer immediately (exported for the dashboard
// resend action). Nil-safe and non-blocking, like nudgeSync.
func (m *Manager) NudgeSync() { m.nudgeSync() }

// nudgeSync wakes the drainer without waiting for the ticker. Non-blocking
// (buffered-1 channel: a pending nudge already covers this one) and nil-safe
// (the drainer may be disabled — Postgres backend — or not started in tests).
func (m *Manager) nudgeSync() {
	ch := m.syncNudge
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}
