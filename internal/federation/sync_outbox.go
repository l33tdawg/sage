package federation

// Sender side of v11.5 domain sync: the outbox drainer.
//
// One goroutine, one ticker. Each tick, per peer chain with sync consent:
//
//   scan  — anti-join the committed set in the consented subtrees against the
//           outbox (ListSyncCandidates also excludes synced copies: never
//           re-forward). Doubles as restart recovery by construction — no
//           watermark to lose. Enqueue gates: active agreement, treaty
//           domain scope, local classification <= treaty ceiling.
//   drain — claim up to one push batch (CAS pending->delivering), re-run
//           EVERY gate at send time (agreements re-scope, classifications
//           change), load decrypted content, deliver via SyncPush, map the
//           per-item outcomes onto outbox states.
//
// Failure taxonomy (locked by the build map):
//   accepted/duplicate            -> delivered (terminal)
//   rejected_* per-item outcomes  -> rejected (terminal, last_error surfaced)
//   retry outcome / transport err -> pending, attempts+1, exp backoff
//   ErrSyncUnsupported            -> pending, 1h floor ("peer lacks sync")
//   sender vault locked           -> pending, short delay, attempts UNCHANGED
//   memory gone / oversized       -> failed (terminal local)
//
// SQLite discipline: every claim/update is a single-row write; no store
// transaction ever spans the network call (ABCI Commit's flush panics the
// node if the write lock is starved).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	// syncDrainInterval is the steady-state tick. The commit-tail watcher
	// (build step 6) nudges delivery sooner; this ticker is the correctness
	// backstop, so it can stay coarse.
	syncDrainInterval = 30 * time.Second
	// syncScanLimit bounds new enqueues per (agreement, tick) so a huge
	// backfill trickles instead of flooding the outbox in one transaction
	// storm; the anti-join finds the remainder next tick.
	syncScanLimit = 200
	// syncPushTimeout is the per-peer delivery deadline, derived from
	// context.Background() + mergeCancel like receipt fan-out (a hung peer
	// must not stall the whole drain loop past this).
	syncPushTimeout = 20 * time.Second

	syncBackoffBase = 30 * time.Second
	syncBackoffMax  = time.Hour
	// syncVaultRetryDelay reschedules vault-locked deferrals: cheap to poll
	// (no network) but pointless to hammer; attempts are NOT incremented.
	syncVaultRetryDelay = 2 * time.Minute
	// syncMaxAttempts bounds redelivery: a row that keeps failing for
	// attempt-counting reasons (transport errors, a persistently-rejecting
	// receiver) is marked failed rather than retried forever. At the capped
	// 1h backoff this is ~half a day of trying. Deferrals that pass
	// bumpAttempts=false (vault locked, peer not upgraded) never reach it.
	syncMaxAttempts = 12
)

// StartSyncDrainer launches the outbox goroutine. No-op (with one log line)
// on non-SQLite backends — sync is SQLite-only and must disable loudly.
func (m *Manager) StartSyncDrainer(ctx context.Context) {
	ss := m.syncStore()
	if ss == nil {
		m.logger.Info().Msg("domain sync disabled: store backend is not SQLite")
		return
	}
	// The nudge channel must exist before the commit watcher can be wired
	// (runServe calls StartSyncDrainer, THEN SetSyncNotifier).
	m.syncNudge = make(chan struct{}, 1)
	nudge := m.syncNudge

	// Crash recovery: any row left 'delivering' by a previous process death
	// (or a shutdown that cancelled the outcome write) is returned to
	// 'pending' before the first scan, so it can be re-claimed.
	if n, err := ss.ResetDeliveringToPending(context.Background()); err != nil {
		m.logger.Warn().Err(err).Msg("sync: reset delivering rows failed")
	} else if n > 0 {
		m.logger.Info().Int("rows", n).Msg("sync: recovered stranded delivering rows on startup")
	}

	go func() {
		// Immediate first pass: restart recovery should not wait a tick.
		m.syncTick(ctx, ss)
		t := time.NewTicker(syncDrainInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.syncTick(ctx, ss)
			case <-nudge:
				m.syncTick(ctx, ss)
			}
		}
	}()
	// Anti-entropy reconciler: its own goroutine so a slow paging pass never
	// starves the delivery loop.
	go func() {
		m.syncReconcileAll(ctx, ss)
		t := time.NewTicker(syncReconcileInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.syncReconcileAll(ctx, ss)
			}
		}
	}()
}

// syncTick runs one scan+drain pass over every peer with sync consent.
func (m *Manager) syncTick(ctx context.Context, ss *store.SQLiteStore) {
	chains, err := ss.ListSyncDomainChains(ctx)
	if err != nil {
		m.logger.Warn().Err(err).Msg("sync: list consent chains failed")
		return
	}
	for _, chain := range chains {
		if ctx.Err() != nil {
			return
		}
		agreement, err := m.ActiveAgreement(chain)
		if err != nil {
			// Revoked/expired/missing agreement: consent rows linger until the
			// revoke purge, but nothing moves — fail closed and quiet.
			continue
		}
		consented, err := ss.GetSyncDomains(ctx, chain)
		if err != nil || len(consented) == 0 {
			continue
		}
		m.syncScan(ctx, ss, agreement, consented)
		m.syncDrain(ctx, ss, agreement, consented)
	}
}

// syncScan enqueues committed memories from the consented subtrees.
func (m *Manager) syncScan(ctx context.Context, ss *store.SQLiteStore, agreement *store.CrossFedRecord, consented []string) {
	cands, err := ss.ListSyncCandidates(ctx, agreement.RemoteChainID, consented, syncScanLimit)
	if err != nil {
		m.logger.Warn().Err(err).Str("peer", agreement.RemoteChainID).Msg("sync: candidate scan failed")
		return
	}
	for _, c := range cands {
		// Enqueue gates (all re-run at send time; these just keep junk out of
		// the queue): treaty scope + local classification ceiling.
		if !DomainAllowed(agreement.AllowedDomains, c.DomainTag) {
			continue
		}
		if c.Classification > int(agreement.MaxClearance) {
			continue
		}
		if _, err := ss.EnqueueSyncOutbox(ctx, agreement.RemoteChainID, c.MemoryID); err != nil {
			m.logger.Warn().Err(err).Str("memory", c.MemoryID).Msg("sync: enqueue failed")
		}
	}
}

// syncDrain claims one batch and delivers it.
func (m *Manager) syncDrain(ctx context.Context, ss *store.SQLiteStore, agreement *store.CrossFedRecord, consented []string) {
	chain := agreement.RemoteChainID
	claimed, err := ss.ClaimDueSyncOutbox(ctx, chain, SyncPushMaxItems)
	if err != nil {
		m.logger.Warn().Err(err).Str("peer", chain).Msg("sync: claim failed")
		return
	}
	if len(claimed) == 0 {
		return
	}

	// State-recording writes use a NON-CANCELABLE context: once a row is
	// claimed (pending->delivering), its outcome MUST be recorded even if the
	// node is shutting down mid-batch — otherwise the row is stranded in
	// 'delivering' (nothing re-claims it, and startup reset only runs once).
	// SQLite writes are local and fast, so a background context is safe.
	writeCtx := context.Background()

	// retry returns a claimed row to pending. bumpAttempts=false is for
	// not-the-item's-fault deferrals (vault locked, peer not upgraded) so they
	// never hit the attempts cap. When attempts reach syncMaxAttempts the row
	// is failed terminal instead of retried forever (bounds churn from a
	// persistently-rejecting receiver).
	retry := func(row store.SyncOutboxItem, bumpAttempts bool, delay time.Duration, reason string) {
		attempts := row.Attempts
		if bumpAttempts {
			attempts++
			if attempts >= syncMaxAttempts {
				if err := ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID,
					truncateString("gave up after "+reason, 200)); err != nil {
					m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: fail mark failed")
				}
				return
			}
		}
		if err := ss.MarkSyncOutboxRetry(writeCtx, chain, row.MemoryID, attempts, time.Now().Add(delay), reason); err != nil {
			m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: retry mark failed")
		}
	}

	// Build the batch, re-running every gate at send time.
	items := make([]SyncItem, 0, len(claimed))
	itemRows := make([]store.SyncOutboxItem, 0, len(claimed))
	for _, row := range claimed {
		rec, err := ss.GetMemory(ctx, row.MemoryID)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				// The memory is genuinely gone (forgotten/deleted): terminal.
				_ = ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID, "memory no longer available")
			} else {
				// Transient read error (SQLITE_BUSY, IO): retry, don't lose it.
				retry(row, true, syncBackoff(row.Attempts+1), "memory read error")
			}
			continue
		}
		if rec.Status != memory.StatusCommitted {
			// Deprecated (or otherwise no longer committed) since enqueue:
			// lifecycle is sovereign and does NOT propagate — terminal skip.
			_ = ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID, "memory no longer committed")
			continue
		}
		if rec.Content == store.VaultLockedPlaceholder {
			// Sender vault locked: defer WITHOUT burning attempts, never push
			// the placeholder literal.
			retry(row, false, syncVaultRetryDelay, "sender vault locked")
			continue
		}
		if rec.Content == "" {
			// Hash-only co-commit rows whose content backfill never arrived.
			_ = ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID, "no content available")
			continue
		}
		if len(rec.Content) > SyncMaxItemContent {
			_ = ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID, "content too large")
			continue
		}
		if !DomainAllowed(agreement.AllowedDomains, rec.DomainTag) || !DomainAllowed(consented, rec.DomainTag) {
			// Agreement re-scoped narrower, consent changed, or the memory was
			// re-domained (v11.3 reassign) since enqueue — a property of the
			// sender's own state, so terminal.
			_ = ss.MarkSyncOutboxRejected(writeCtx, chain, row.MemoryID, "domain out of scope at send time")
			continue
		}
		cls, err := ss.GetMemoryClassificationLocal(ctx, row.MemoryID)
		if err != nil {
			// Fail CLOSED on the egress boundary: an unreadable classification
			// is never treated as "low".
			retry(row, true, syncBackoff(row.Attempts+1), "classification read failed")
			continue
		}
		if cls > int(agreement.MaxClearance) {
			_ = ss.MarkSyncOutboxRejected(writeCtx, chain, row.MemoryID, "classification above agreement ceiling")
			continue
		}
		items = append(items, SyncItem{
			OriginChainID:   m.localChainID,
			OriginMemoryID:  row.MemoryID,
			OriginCreatedAt: rec.CreatedAt.UTC().Format(time.RFC3339),
			Domain:          rec.DomainTag,
			Classification:  cls,
			MemoryType:      string(rec.MemoryType),
			ConfidenceScore: rec.ConfidenceScore,
			Content:         rec.Content,
			ContentHash:     contentHashHex(rec.Content),
			Tags:            nil, // tag replication is a documented v11.5 non-goal
		})
		itemRows = append(itemRows, row)
	}
	if len(items) == 0 {
		return
	}

	// Deliver. Deadline is our own (context.Background()), merged with the
	// node lifecycle ctx — the receipt fan-out pattern.
	pctx, cancel := context.WithTimeout(context.Background(), syncPushTimeout)
	pctx = mergeCancel(pctx, ctx)
	defer cancel()
	push := m.syncPushFn
	if push == nil {
		push = m.SyncPush
	}
	resp, err := push(pctx, chain, &SyncPushRequest{Items: items})
	if err != nil {
		reason := err.Error()
		for _, row := range itemRows {
			if err == ErrSyncUnsupported {
				// Not the item's fault — don't count toward the cap; park at
				// the 1h floor until the peer upgrades.
				retry(row, false, syncBackoffMax, "peer does not support sync")
			} else {
				retry(row, true, syncBackoff(row.Attempts+1), truncateString(reason, 200))
			}
		}
		return
	}

	// Map per-item outcomes BY ORIGIN ID, not array index: a misbehaving (but
	// authenticated) peer that permutes its results must not cause us to mark
	// the wrong outbox row. SyncPush already verified the count matches.
	byID := make(map[string]string, len(resp.Results))
	for _, res := range resp.Results {
		byID[res.OriginMemoryID] = res.Outcome
	}
	for _, row := range itemRows {
		outcome, ok := byID[row.MemoryID]
		if !ok {
			// Peer omitted this row's id — retry rather than silently drop.
			retry(row, true, syncBackoff(row.Attempts+1), "peer omitted result")
			continue
		}
		switch outcome {
		case SyncOutcomeAccepted, SyncOutcomeDuplicate:
			if err := ss.MarkSyncOutboxDelivered(writeCtx, chain, row.MemoryID); err != nil {
				m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: delivered mark failed")
			}
		case SyncOutcomeRejectedXDomainDup:
			// Content-derived on the receiver — will not change without a
			// deprecation there, so terminal on our side.
			if err := ss.MarkSyncOutboxRejected(writeCtx, chain, row.MemoryID, outcome); err != nil {
				m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: rejected mark failed")
			}
		case SyncOutcomeRejectedConsent, SyncOutcomeRejectedScope, SyncOutcomeRejectedClearance:
			// Receiver-CONFIG-dependent: the operator may widen consent /
			// clearance / treaty scope later, so keep RETRYABLE (long backoff,
			// attempts-capped) instead of poisoning the item permanently. This
			// is what lets the bring-up race (backlog pushed before the peer
			// finishes configuring consent) self-heal.
			retry(row, true, syncBackoffMax, truncateString(outcome, 200))
		default: // SyncOutcomeRetry or anything unknown — redeliver later.
			retry(row, true, syncBackoff(row.Attempts+1), "peer asked to retry")
		}
	}
}

// ---- anti-entropy reconciliation ----

const (
	syncReconcileInterval   = time.Hour
	syncReconcileMaxPages   = 50  // per (peer, domain) per cycle
	syncReconcileMaxEnqueue = 500 // per peer per cycle; the remainder next cycle
	syncDigestTimeout       = 4 * time.Second
)

// SyncReconcileStatus is per-peer anti-entropy bookkeeping for the status
// surface.
type SyncReconcileStatus struct {
	LastReconcile   time.Time `json:"last_reconcile"`
	PeerConsented   []string  `json:"peer_consented_domains"`
	PeerUnsupported bool      `json:"peer_unsupported"`
}

// SyncReconcileInfo returns the last reconcile bookkeeping for one peer.
func (m *Manager) SyncReconcileInfo(remoteChainID string) (SyncReconcileStatus, bool) {
	m.syncStatusMu.Lock()
	defer m.syncStatusMu.Unlock()
	st, ok := m.syncReconcile[remoteChainID]
	return st, ok
}

func (m *Manager) setSyncReconcileInfo(remoteChainID string, st SyncReconcileStatus) {
	m.syncStatusMu.Lock()
	defer m.syncStatusMu.Unlock()
	if m.syncReconcile == nil {
		m.syncReconcile = make(map[string]SyncReconcileStatus)
	}
	m.syncReconcile[remoteChainID] = st
}

// syncReconcileAll runs one anti-entropy pass over every consented peer.
func (m *Manager) syncReconcileAll(ctx context.Context, ss *store.SQLiteStore) {
	chains, err := ss.ListSyncDomainChains(ctx)
	if err != nil {
		return
	}
	for _, chain := range chains {
		if ctx.Err() != nil {
			return
		}
		agreement, aErr := m.ActiveAgreement(chain)
		if aErr != nil {
			continue
		}
		consented, cErr := ss.GetSyncDomains(ctx, chain)
		if cErr != nil || len(consented) == 0 {
			continue
		}
		m.reconcilePeer(ctx, ss, agreement, consented)
	}
}

// reconcilePeer pages the peer's admission digest per consented domain and
// backfills the local outbox: candidates the peer has NOT seen are enqueued
// normally; candidates the peer has ALREADY decided (possible only when the
// local outbox was lost/rebuilt — normally a decided item has a terminal
// outbox row) are settled as delivered without redelivery churn. The steady-
// state scan already covers new work; this pass exists for outbox loss and
// for surfacing the peer's consent posture.
func (m *Manager) reconcilePeer(ctx context.Context, ss *store.SQLiteStore, agreement *store.CrossFedRecord, consented []string) {
	chain := agreement.RemoteChainID
	digest := m.syncDigestFn
	if digest == nil {
		digest = m.SyncDigest
	}
	status := SyncReconcileStatus{LastReconcile: time.Now()}
	enqueued := 0
	for _, domain := range consented {
		decided := make(map[string]bool)
		after := ""
		for page := 0; page < syncReconcileMaxPages; page++ {
			dctx, cancel := context.WithTimeout(context.Background(), syncDigestTimeout)
			dctx = mergeCancel(dctx, ctx)
			resp, dErr := digest(dctx, chain, &SyncDigestRequest{Domain: domain, After: after})
			cancel()
			if dErr == ErrSyncUnsupported {
				status.PeerUnsupported = true
				m.setSyncReconcileInfo(chain, status)
				return
			}
			if dErr != nil {
				// Transient: finish what we have, next cycle retries.
				m.logger.Debug().Err(dErr).Str("peer", chain).Msg("sync reconcile: digest page failed")
				break
			}
			status.PeerConsented = resp.ConsentedDomains
			for _, id := range resp.OriginMemoryIDs {
				decided[id] = true
			}
			if resp.NextCursor == "" {
				break
			}
			after = resp.NextCursor
		}
		cands, lErr := ss.ListSyncCandidates(ctx, chain, []string{domain}, syncReconcileMaxEnqueue)
		if lErr != nil {
			continue
		}
		for _, c := range cands {
			if enqueued >= syncReconcileMaxEnqueue {
				break
			}
			if !DomainAllowed(agreement.AllowedDomains, c.DomainTag) || c.Classification > int(agreement.MaxClearance) {
				continue
			}
			created, eErr := ss.EnqueueSyncOutbox(ctx, chain, c.MemoryID)
			if eErr != nil || !created {
				continue
			}
			enqueued++
			if decided[c.MemoryID] {
				// The digest is ADMITTED-only, so a hit means the peer has
				// genuinely accepted this item — settle the freshly-enqueued
				// row as delivered rather than re-pushing it. (Rejections are
				// never in the digest, so we never falsely settle one.)
				_ = ss.MarkSyncOutboxDelivered(context.Background(), chain, c.MemoryID)
			}
		}
	}
	m.setSyncReconcileInfo(chain, status)
	if enqueued > 0 {
		m.nudgeSync()
	}
}

// syncBackoff is min(30s * 2^attempts, 1h), shift-capped.
func syncBackoff(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 7 { // 30s << 7 = 64m > cap
		return syncBackoffMax
	}
	d := syncBackoffBase << uint(attempts) // #nosec G115 -- bounded 0-7
	if d > syncBackoffMax {
		return syncBackoffMax
	}
	return d
}

// contentHashHex is the wire content-hash form (hex sha256 of the content).
func contentHashHex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func truncateString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
