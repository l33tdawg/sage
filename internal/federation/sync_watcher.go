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
	"encoding/hex"
	"fmt"

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
	if err != nil {
		return
	}

	// Load per-chain PAIRWISE scope once per batch.
	type scope struct {
		agreement *store.CrossFedRecord
		consented []string
		v3        bool
	}
	scopes := make([]scope, 0, len(chains))
	for _, chain := range chains {
		agreement, aErr := m.ActiveAgreement(chain)
		if aErr != nil {
			continue // revoked/expired: fail closed and quiet
		}
		consented, v3, cErr := m.pairwiseEgressPolicy(ctx, ss, agreement)
		if cErr != nil || len(consented) == 0 {
			continue
		}
		scopes = append(scopes, scope{agreement: agreement, consented: consented, v3: v3})
	}
	// A group-only node has no pairwise scopes but still owns domains to fan out
	// (§9.1). Only bail when there is NEITHER pairwise consent NOR any active
	// group member to receive a star push.
	if len(scopes) == 0 {
		if groupChains, gErr := ss.ListActiveGroupMemberChains(ctx); gErr != nil || len(groupChains) == 0 {
			return
		}
	}

	enqueued := false
	for _, id := range ids {
		originUnlock := ss.LockSyncOriginRead()
		// Loop prevention: a copy admitted FROM a peer commits locally and
		// re-fires this hook — never re-forward it (to anyone).
		if isCopy, cErr := ss.IsSyncedCopy(ctx, id); cErr != nil || isCopy {
			originUnlock()
			continue
		}
		domain, cls, ok := m.syncMetaFor(ctx, ss, id)
		if !ok {
			originUnlock()
			continue
		}
		for _, sc := range scopes {
			if !DomainAllowed(sc.consented, domain) ||
				!syncEgressDomainAllowed(sc.agreement, sc.consented, sc.v3, false, false, domain) {
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
		// §9.1 group STAR fan-out: when THIS node OWNS domain D of the (native, per
		// the IsSyncedCopy guard above) memory, enqueue one outbox row per active
		// member holding D. Star only — the owner pushes directly to each member, so
		// there is no A->B->C relay and no cycle. The IsSyncedCopy(:71) lease still
		// covers this loop (I7): a received copy is never fanned out. Clearance +
		// scope bind to the PAIRWISE edge (ActiveAgreement(member)), never the
		// display-only sync_group_domain.max_clearance (docs §9.2 must-fix #12).
		if targets, tErr := ss.ListGroupFanoutTargets(ctx, m.localChainID, domain); tErr == nil {
			for _, tg := range targets {
				ag, aErr := m.ActiveAgreement(tg.MemberChainID)
				if aErr != nil {
					continue // no active trust edge to this member: fail closed
				}
				if paused, pauseErr := m.connectionSharingPaused(ctx, ag); pauseErr != nil || paused {
					continue
				}
				peerAgentID, bound, bindErr := m.exactGroupPeerAgentID(ctx, ss, ag)
				if bindErr != nil || !bound {
					continue
				}
				// The target projection proves the recipient's role/selection. Re-check
				// both exact roster identities for this group so a removed/replaced
				// operator cannot keep a stale best-effort enqueue capability.
				localShares, lErr := ss.MemberSharesGroupDomainForAgent(ctx, tg.GroupID,
					m.localChainID, hex.EncodeToString(m.agentPub), domain)
				peerShares, pErr := ss.MemberSharesGroupDomainForAgent(ctx, tg.GroupID,
					tg.MemberChainID, peerAgentID, domain)
				if lErr != nil || pErr != nil || !localShares || !peerShares {
					continue
				}
				direct, v3, edgeErr := m.pairwiseEgressPolicy(ctx, ss, ag)
				if edgeErr != nil ||
					!syncEgressDomainAllowed(ag, direct, v3, true, false, domain) ||
					cls > int(ag.MaxClearance) {
					continue
				}
				if _, eErr := ss.EnqueueSyncOutbox(ctx, tg.MemberChainID, id); eErr != nil {
					m.logger.Warn().Err(eErr).Str("memory", id).Str("member", tg.MemberChainID).Msg("sync watcher: group enqueue failed")
					continue
				}
				enqueued = true
			}
		} else {
			m.logger.Warn().Err(tErr).Str("memory", id).Msg("sync watcher: group fanout lookup failed")
		}
		originUnlock()
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

// NudgeJournalReconcile asks the bounded group-journal anti-entropy worker to
// run promptly. It is intentionally distinct from NudgeSync: normal memory
// delivery must not trigger a roster scan, while restored trust and an explicit
// Groups refresh should converge signed membership changes immediately.
func (m *Manager) NudgeJournalReconcile() {
	ch := m.syncJournalNudge
	if ch == nil {
		return
	}
	select {
	case ch <- nil:
	default:
	}
}

// NudgeJournalReconcileAndWait asks the anti-entropy worker to run and waits
// until that prompted pass finishes. Unlike the background nudge above, an
// explicit operator refresh must not race its immediate SQLite reload and show
// the same stale group projection. The caller supplies the bound on waiting;
// cancellation never stops or corrupts a worker pass already in progress.
func (m *Manager) NudgeJournalReconcileAndWait(ctx context.Context) error {
	ch := m.syncJournalNudge
	if ch == nil {
		return fmt.Errorf("group journal refresh is not running")
	}
	done := make(chan struct{})
	select {
	case ch <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

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
