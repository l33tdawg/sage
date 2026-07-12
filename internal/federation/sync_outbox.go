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
// Failure taxonomy:
//   accepted/duplicate            -> delivered (terminal)
//   B-D1 cross-domain dup         -> rejected (terminal, content-derived)
//   transport err (PEER OFFLINE)  -> pending, backoff, NEVER gives up (delivers
//                                    whenever the peer returns — a closed laptop
//                                    for a week still gets its backlog)
//   config reject (consent/scope/ -> pending, 1h floor, NEVER gives up (self-
//     clearance)                     heals when the operator widens scope)
//   ErrSyncUnsupported            -> pending, 1h floor ("peer lacks sync")
//   sender vault locked           -> pending, short delay, attempts UNCHANGED
//   "retry" outcome (peer broad-  -> pending, backoff, EVENTUALLY fails (attempts
//     cast attempted, non-term)      cap) — the only give-up path, to bound a
//                                    permanently-rejecting receiver's churn
//   memory gone / oversized       -> failed (terminal local)
//
// SQLite discipline: every claim/update is a single-row write; no store
// transaction ever spans the network call (ABCI Commit's flush panics the
// node if the write lock is starved).

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
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
	m.syncStartOnce.Do(func() {
		m.startSyncDrainer(ctx)
	})
}

func (m *Manager) startSyncDrainer(parent context.Context) {
	ss := m.syncStore()
	if ss == nil {
		m.logger.Info().Msg("domain sync disabled: store backend is not SQLite")
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.syncCancel = cancel
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

	m.syncWG.Add(2)
	go func() {
		defer m.syncWG.Done()
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
		defer m.syncWG.Done()
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

// StopSyncDrainer cancels and joins both sync workers before the SQLite store
// is closed. It is idempotent and deliberately leaves syncNudge open because
// commit watchers may still hold a nonblocking notifier reference.
func (m *Manager) StopSyncDrainer() {
	m.syncStopOnce.Do(func() {
		if m.syncCancel != nil {
			m.syncCancel()
		}
		m.syncWG.Wait()
		if ss := m.syncStore(); ss != nil {
			if _, err := ss.ResetDeliveringToPending(context.Background()); err != nil {
				m.logger.Warn().Err(err).Msg("sync: shutdown delivery reset failed")
			}
		}
	})
}

// syncTick runs one scan+drain pass over every peer with sync consent.
func (m *Manager) syncTick(ctx context.Context, ss *store.SQLiteStore) {
	if pending, err := ss.ListPendingSyncControls(ctx); err == nil {
		for _, control := range pending {
			if ctx.Err() != nil {
				return
			}
			pctx, cancel := context.WithTimeout(ctx, syncPushTimeout)
			if deliveryErr := m.deliverSyncPolicy(pctx, ss, control.RemoteChainID); deliveryErr != nil {
				m.logger.Debug().Err(deliveryErr).Str("peer", control.RemoteChainID).Msg("sync policy delivery pending")
			}
			cancel()
		}
	} else {
		m.logger.Warn().Err(err).Msg("sync: list pending policies failed")
	}
	// Enumerate the UNION of pairwise consent chains and active group members: a
	// group-only member (a cross_fed edge but no pairwise sync_domains row) is
	// never returned by ListSyncDomainChains, so without the union its live
	// fan-out outbox rows would strand undrained (docs §9.1 strand fix).
	chains, err := m.syncPeerChains(ctx, ss)
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
			// Revoked/expired/missing agreement (or self): consent rows linger
			// until the revoke purge, but nothing moves — fail closed and quiet.
			continue
		}
		consented, err := m.effectiveConsent(ctx, ss, chain)
		if err != nil || len(consented) == 0 {
			continue
		}
		// SENDER scope (docs §9.1 must-fix #3): the scan enqueues LOCAL candidates, so
		// it may only originate pairwise domains + group domains this node OWNS — never
		// re-export a full-sync domain owned by another member. The drain keeps the
		// broader effectiveConsent for its receiver-consent gate.
		senderScope, sErr := m.senderConsent(ctx, ss, chain)
		if sErr != nil {
			continue
		}
		if len(senderScope) > 0 {
			m.syncScan(ctx, ss, agreement, senderScope)
		}
		m.syncDrain(ctx, ss, agreement, consented)
	}
}

// syncPeerChains is the drainer's iteration set: the union of pairwise
// sync-consent chains (ListSyncDomainChains) and active group-member chains
// (ListActiveGroupMemberChains). Deduped; the local chain is harmlessly present
// and skipped by ActiveAgreement's self-federation guard.
func (m *Manager) syncPeerChains(ctx context.Context, ss *store.SQLiteStore) ([]string, error) {
	pairwise, err := ss.ListSyncDomainChains(ctx)
	if err != nil {
		return nil, err
	}
	group, gErr := ss.ListActiveGroupMemberChains(ctx)
	if gErr != nil {
		return nil, gErr
	}
	if len(group) == 0 {
		return pairwise, nil
	}
	seen := make(map[string]struct{}, len(pairwise)+len(group))
	out := make([]string, 0, len(pairwise)+len(group))
	for _, c := range pairwise {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	for _, c := range group {
		if c == m.localChainID {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out, nil
}

// effectiveConsent is the sender-side domain scope toward one peer: the union of
// pairwise receiver consent (sync_domains) and the active group domains both this
// node and the peer are entitled to (docs §9.1). For a 2-node pair the group half
// is empty, so behaviour is unchanged; for a group-only member the pairwise half
// is empty and the group domains carry the fan-out.
func (m *Manager) effectiveConsent(ctx context.Context, ss *store.SQLiteStore, chain string) ([]string, error) {
	pairwise, err := ss.GetSyncDomains(ctx, chain)
	if err != nil {
		return nil, err
	}
	group, gErr := ss.GroupSharedDomains(ctx, m.localChainID, chain)
	if gErr != nil {
		return nil, gErr
	}
	return unionDomains(pairwise, group), nil
}

// senderConsent is the SENDER-side scope toward one peer (docs §9.1 must-fix #3):
// pairwise receiver consent (sync_domains) unioned ONLY with the group domains THIS
// node OWNS that the peer is entitled to receive. A node originates star fan-out only
// for domains it owns, so the anti-entropy SCAN/ENQUEUE must use this — never the
// symmetric effectiveConsent, which also includes group domains a full-sync member
// merely holds (scanning those would re-export another owner's domain).
func (m *Manager) senderConsent(ctx context.Context, ss *store.SQLiteStore, chain string) ([]string, error) {
	pairwise, err := ss.GetSyncDomains(ctx, chain)
	if err != nil {
		return nil, err
	}
	owned, oErr := ss.GroupOwnedDomainsForPeer(ctx, m.localChainID, chain)
	if oErr != nil {
		return nil, oErr
	}
	return unionDomains(pairwise, owned), nil
}

// unionDomains merges two domain slices, dropping duplicates and empties while
// preserving the first slice's order then appending new entries from the second.
func unionDomains(a, b []string) []string {
	if len(b) == 0 {
		return a
	}
	seen := make(map[string]struct{}, len(a)+len(b))
	out := make([]string, 0, len(a)+len(b))
	for _, d := range a {
		if d == "" {
			continue
		}
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	for _, d := range b {
		if d == "" {
			continue
		}
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
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
	claimed, claimErr := ss.ClaimDueSyncOutbox(ctx, chain, SyncPushMaxItems)
	if claimErr != nil {
		m.logger.Warn().Err(claimErr).Str("peer", chain).Msg("sync: claim failed")
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

	// retry returns a claimed row to pending. Two independent knobs:
	//   bumpAttempts — grow the backoff (attempts drives syncBackoff), and
	//   canFail      — allow the give-up cap to fire.
	// canFail is TRUE ONLY for failures that will NEVER succeed no matter how
	// long we wait: a receiver that keeps actively rejecting the item via a
	// SUCCESSFUL push (the deterministic-dead-end / churn case). It is FALSE for
	// everything transient or environmental — peer OFFLINE (transport error,
	// closed laptop, network gap), peer not upgraded, sender vault locked, a
	// local DB read hiccup — because those resolve when the environment
	// recovers, and giving up would silently lose memories queued while the peer
	// was away (the exact "close the laptop overnight" data-loss gap).
	retry := func(row store.SyncOutboxItem, bumpAttempts, canFail bool, delay time.Duration, reason string) {
		attempts := row.Attempts
		if bumpAttempts {
			attempts++
		}
		if canFail && attempts >= syncMaxAttempts {
			if err := ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID,
				truncateString("gave up after "+reason, 200)); err != nil {
				m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: fail mark failed")
			}
			return
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
				retry(row, true, false, syncBackoff(row.Attempts+1), "memory read error")
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
			retry(row, false, false, syncVaultRetryDelay, "sender vault locked")
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
			retry(row, true, false, syncBackoff(row.Attempts+1), "classification read failed")
			continue
		}
		if cls > int(agreement.MaxClearance) {
			_ = ss.MarkSyncOutboxRejected(writeCtx, chain, row.MemoryID, "classification above agreement ceiling")
			continue
		}
		tags, err := ss.GetTags(ctx, row.MemoryID)
		if err != nil {
			retry(row, true, false, syncBackoff(row.Attempts+1), "tag read failed")
			continue
		}
		item := SyncItem{
			OriginChainID:   m.localChainID,
			OriginMemoryID:  row.MemoryID,
			OriginCreatedAt: rec.CreatedAt.UTC().Format(time.RFC3339),
			Domain:          rec.DomainTag,
			Classification:  cls,
			MemoryType:      string(rec.MemoryType),
			ConfidenceScore: rec.ConfidenceScore,
			Content:         rec.Content,
			ContentHash:     contentHashHex(rec.Content),
			Tags:            tags,
		}
		if row.OriginChainID != "" && row.OriginChainID != m.localChainID {
			// D4 RELAYED row (docs §9.2): this node is a pure cache, NOT the origin.
			// Re-serve the ORIGINAL provenance + the origin agent's stored signature
			// VERBATIM — never re-sign with our own key (that would forge/mis-attribute).
			// Content/Domain/Classification/ContentHash/Tags still come from the LOCAL
			// copy above; the receiver admits via the group relay path (ResolveGroupRelay
			// + roster-key origin_sig verify + Gate 6.5). All egress gates already re-ran
			// against the local copy's domain/classification. An absent stored sig means
			// we cannot serve it authentically — terminal (never forge an unsigned relay).
			ro, roErr := ss.GetRelayOrigin(ctx, row.OriginChainID, row.MemoryID)
			if roErr != nil {
				if errors.Is(roErr, sql.ErrNoRows) {
					_ = ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID, "relay origin provenance missing")
				} else {
					retry(row, true, false, syncBackoff(row.Attempts+1), "relay origin read failed")
				}
				continue
			}
			if len(ro.OriginSig) == 0 {
				_ = ss.MarkSyncOutboxFailed(writeCtx, chain, row.MemoryID, "relay origin signature missing")
				continue
			}
			item.OriginChainID = row.OriginChainID
			item.OriginMemoryID = ro.OriginMemoryID
			item.OriginCreatedAt = ro.OriginCreatedAt
			item.OriginSig = ro.OriginSig
			items = append(items, item)
			itemRows = append(itemRows, row)
			continue
		}
		// Sign the item with our operator agent key: we are the ORIGIN of every
		// NATIVE item we enqueue (OriginChainID == localChainID), so this is the origin
		// signature a downstream mesh relayer carries verbatim and the receiver
		// verifies (docs §4.4). Harmless to pre-v11.8 receivers (unknown JSON
		// field is ignored).
		item.OriginSig = signOriginSig(m.agentKey, &item)
		items = append(items, item)
		itemRows = append(itemRows, row)
	}
	if len(items) == 0 {
		return
	}

	// Linearize the last policy check with consent changes. Set/DeleteSyncDomains
	// take the write side of this lease, so once their API returns no push can
	// begin from the old snapshot. Never hold a SQLite transaction here.
	policyUnlock := ss.LockSyncPolicyRead()
	policyLocked := true
	defer func() {
		if policyLocked {
			policyUnlock()
		}
	}()
	currentAgreement, gateErr := m.ActiveAgreement(chain)
	currentConsent, consentErr := m.effectiveConsent(ctx, ss, chain)
	control, controlErr := ss.GetSyncControl(ctx, chain)
	policyPending := control != nil && control.Role == "host" && control.Revision > control.DeliveredRevision
	if gateErr != nil || consentErr != nil || controlErr != nil || len(currentConsent) == 0 || policyPending {
		policyUnlock()
		policyLocked = false
		for _, row := range itemRows {
			reason := "sync policy changed before delivery"
			if policyPending {
				reason = "sync policy awaiting peer acknowledgement"
			}
			retry(row, false, false, syncBackoffBase, reason)
		}
		return
	}
	filteredItems := items[:0]
	filteredRows := itemRows[:0]
	for i, item := range items {
		if !DomainAllowed(currentAgreement.AllowedDomains, item.Domain) || !DomainAllowed(currentConsent, item.Domain) ||
			item.Classification > int(currentAgreement.MaxClearance) {
			_ = ss.MarkSyncOutboxRejected(writeCtx, chain, itemRows[i].MemoryID, "domain out of scope at final policy gate")
			continue
		}
		// A RELAYED item (this node is a cache, not the origin) is authorized ONLY while
		// the receiver, this relayer, and the origin all remain active members of ONE group
		// sharing the domain. Re-verify at send time under the policy lease (docs §10 R1):
		// pairwise consent above (currentConsent = pairwise ∪ GroupSharedDomains) can pass a
		// relayed item whose domain a pairwise relationship coincidentally covers, but a
		// pairwise relationship NEVER authorizes relaying a third party's copy — so a peer
		// removed from the group must not receive a relayed backfill. This is the
		// AUTHORITATIVE anti-leak (receiver Gate 6.5 only fires AFTER the bytes cross).
		if item.OriginChainID != "" && item.OriginChainID != m.localChainID {
			_, ok, rErr := ss.ResolveGroupRelay(ctx, m.localChainID, chain, item.OriginChainID, item.Domain)
			if rErr != nil {
				retry(itemRows[i], true, false, syncBackoff(itemRows[i].Attempts+1), "relay re-authorization read failed")
				continue
			}
			if !ok {
				_ = ss.MarkSyncOutboxRejected(writeCtx, chain, itemRows[i].MemoryID, "relay no longer authorized by a shared group")
				continue
			}
		}
		filteredItems = append(filteredItems, item)
		filteredRows = append(filteredRows, itemRows[i])
	}
	items, itemRows = filteredItems, filteredRows
	if len(items) == 0 {
		policyUnlock()
		policyLocked = false
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
	policyUnlock()
	policyLocked = false
	if err != nil {
		reason := err.Error()
		for _, row := range itemRows {
			if err == ErrSyncUnsupported {
				// Not the item's fault — don't count toward the cap; park at
				// the 1h floor until the peer upgrades.
				retry(row, false, false, syncBackoffMax, "peer does not support sync")
			} else {
				retry(row, true, false, syncBackoff(row.Attempts+1), truncateString(reason, 200))
			}
		}
		return
	}

	// Map per-item outcomes BY THE ORIGIN ID WE SENT, not array index: a misbehaving
	// (but authenticated) peer that permutes its results must not cause us to mark
	// the wrong outbox row. SyncPush already verified the count matches. The result
	// key is the item's OriginMemoryID as PUSHED — which for a RELAYED row is the
	// ORIGINAL origin memory id (items[i].OriginMemoryID), NOT the local copy id the
	// outbox row is keyed by — so we index itemRows/items in parallel to recover the
	// row from its sent origin id.
	byID := make(map[string]string, len(resp.Results))
	for _, res := range resp.Results {
		byID[res.OriginMemoryID] = res.Outcome
	}
	for i, row := range itemRows {
		outcome, ok := byID[items[i].OriginMemoryID]
		if !ok {
			// Peer omitted this row's id — retry rather than silently drop.
			// Cheap (no re-broadcast on the peer), so never give up on it.
			retry(row, true, false, syncBackoff(row.Attempts+1), "peer omitted result")
			continue
		}
		switch outcome {
		case SyncOutcomeAccepted, SyncOutcomeDuplicate:
			if err := ss.MarkSyncOutboxDelivered(writeCtx, chain, row.MemoryID); err != nil {
				m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: delivered mark failed")
			}
		case SyncOutcomeRejectedXDomainDup, SyncOutcomeRejectedOriginSig, SyncOutcomeSuppressed, SyncOutcomeRejectedNotAdmitted:
			// Content-derived / durable on the receiver — a cross-domain dup won't
			// change without a deprecation there, a bad origin signature will never
			// verify, and a local_suppress tombstone is a sovereign local delete
			// (docs §10 Gate 6.5) — all terminal on our side (never retry). The
			// receiver collapses the suppressed reason to the generic
			// rejected_not_admitted on the wire (I5); SyncOutcomeSuppressed is kept
			// here too in case a peer reports the internal value verbatim.
			if err := ss.MarkSyncOutboxRejected(writeCtx, chain, row.MemoryID, outcome); err != nil {
				m.logger.Warn().Err(err).Str("memory", row.MemoryID).Msg("sync: rejected mark failed")
			}
		case SyncOutcomeRejectedConsent, SyncOutcomeRejectedScope, SyncOutcomeRejectedClearance:
			// Receiver-CONFIG-dependent and CHEAP (rejected at the gate, no
			// broadcast): the operator may widen consent / clearance / treaty
			// scope at any point — minutes or days later — so retry at the 1h
			// floor forever rather than giving up. Never-fail is what lets the
			// bring-up race (backlog pushed before the peer finishes configuring
			// consent) self-heal no matter how late the consent arrives.
			retry(row, true, false, syncBackoffMax, truncateString(outcome, 200))
		case SyncOutcomeRejectedWriteAccess:
			// The receiver ADMITTED the item but its federation agent lacks RBAC
			// write access on the domain, so the submit tx was block-included
			// and rejected at FinalizeBlock — a full consensus round burned per
			// attempt. Like the finding-#14 dead-end, cap it (canFail=true)
			// rather than retry forever; the operator grants access + Resends.
			retry(row, true, true, syncBackoff(row.Attempts+1), outcome)
		default:
			// SyncOutcomeRetry: the peer ATTEMPTED a broadcast that came back
			// non-terminal. This conflates transient (receiver vault locked,
			// timeout) with a deterministic dead-end (content-validator reject),
			// and each attempt costs the receiver a consensus broadcast — so
			// this is the ONE case that eventually gives up (attempts cap),
			// bounding the churn from a permanently-rejecting receiver.
			retry(row, true, true, syncBackoff(row.Attempts+1), "peer asked to retry")
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
	chains, err := m.syncPeerChains(ctx, ss)
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
		consented, cErr := m.effectiveConsent(ctx, ss, chain)
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
	// D5 (SF2, §6/§9.2/§10 must-fix #3) RESYNCING watermark, GROUP-SCOPED: while THIS
	// node is rebuilding its journal + local_suppress set in a group shared with this
	// peer, PAUSE only the GROUP backfill pass (the group digest sub-loop, and any
	// group-owned-domain re-origination) — NOT the whole peer. A group backfill could
	// resurrect a suppressed memory mid-rebuild; unrelated PAIRWISE domains carry no
	// group-suppression risk, so blocking them is pure availability loss. Gate 6.5
	// (receiver-side) is the mandatory anti-resurrection backstop; this request-side
	// pause is defense-in-depth. The journal PULL path stays open — that is how the
	// tombstone set rebuilds — unchanged by this scoping.
	selfResyncing, rErr := ss.SelfResyncingSharedWithPeer(ctx, m.localChainID, chain)
	if rErr != nil {
		m.logger.Warn().Err(rErr).Str("peer", chain).Msg("sync reconcile: resyncing check failed")
		return
	}
	if selfResyncing {
		m.logger.Debug().Str("peer", chain).Msg("sync reconcile: group backfill paused (self resyncing); pairwise reconcile continues")
	}
	// SENDER scope (must-fix #3): the enqueue backfills LOCAL candidates, so it may only
	// originate pairwise + owned group domains — never re-export a domain owned by another
	// member. The digest still REQUESTS over the full consented set.
	senderScope, scErr := m.senderConsent(ctx, ss, chain)
	if scErr != nil {
		m.logger.Warn().Err(scErr).Str("peer", chain).Msg("sync reconcile: sender scope failed")
		return
	}
	// While self-resyncing, restrict what we ORIGINATE to the pairwise consent set only:
	// dropping owned group domains from senderScope stops a group-owned domain's backfill
	// from re-exporting (and thus round-tripping / resurrecting) an item mid-rebuild,
	// while leaving the pairwise candidate reconcile fully live.
	if selfResyncing {
		pairwise, pErr := ss.GetSyncDomains(ctx, chain)
		if pErr != nil {
			m.logger.Warn().Err(pErr).Str("peer", chain).Msg("sync reconcile: pairwise scope failed")
			return
		}
		senderScope = pairwise
	}
	// Group domains shared with this peer, indexed by domain -> the groups carrying it, so
	// a group-scoped digest (GroupID set) actually reaches handleSyncDigestGroup (must-fix
	// #8: without this the multi-node digest handler is never reached in production).
	groupsByDomain := map[string][]string{}
	if refs, gErr := ss.GroupSharedDomainsWithGroup(ctx, m.localChainID, chain); gErr == nil {
		for _, ref := range refs {
			groupsByDomain[ref.DomainTag] = append(groupsByDomain[ref.DomainTag], ref.GroupID)
		}
	} else {
		m.logger.Debug().Err(gErr).Str("peer", chain).Msg("sync reconcile: group domain lookup failed")
	}
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
		// GROUP backfill digest (must-fix #8): discover what the peer holds from ANY
		// member origin in this domain, merged into the SAME decided-set so we never
		// re-push an item the peer already holds. The handler scopes what it serves
		// (leak-safe); acting on ids we still LACK (mesh pull-relay of a third member's
		// copy) is Part B. D5: SKIPPED while self-resyncing so a group backfill can't
		// resurrect a suppressed item mid-rebuild (pairwise reconcile continues above).
		for _, gid := range groupsByDomain[domain] {
			if selfResyncing {
				break
			}
			gafter := ""
			for page := 0; page < syncReconcileMaxPages; page++ {
				dctx, cancel := context.WithTimeout(context.Background(), syncDigestTimeout)
				dctx = mergeCancel(dctx, ctx)
				resp, dErr := digest(dctx, chain, &SyncDigestRequest{Domain: domain, After: gafter, GroupID: gid})
				cancel()
				if dErr != nil {
					if dErr == ErrSyncUnsupported {
						status.PeerUnsupported = true
					}
					break
				}
				for _, id := range resp.OriginMemoryIDs {
					decided[id] = true
				}
				if resp.NextCursor == "" {
					break
				}
				gafter = resp.NextCursor
			}
		}
		cands, lErr := ss.ListSyncCandidates(ctx, chain, []string{domain}, syncReconcileMaxEnqueue)
		if lErr != nil {
			continue
		}
		for _, c := range cands {
			if enqueued >= syncReconcileMaxEnqueue {
				break
			}
			// Only ORIGINATE domains in the sender scope (must-fix #3): never enqueue a
			// group domain this node does not own, even though we requested its digest.
			if !DomainAllowed(senderScope, c.DomainTag) {
				continue
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
		// D4 MESH PULL-RELAY enqueue (docs §9.2): beyond the domains this node
		// OWNS (native origination above), relay admitted COPIES of a THIRD member's
		// memories that this node HOLDS and the peer LACKS. Leak-safety is enforced
		// by ListGroupRelayCandidates (same most-specific-covering-domain rule as the
		// digest serve: origin an active member; covering domain unclassified OR
		// relayer-owned; both entitled; local_suppress excluded). We subtract the
		// decided set (what the peer already holds) so a relay is only enqueued for a
		// genuine gap. SKIPPED while self-resyncing (D5) — a backfill must not
		// re-originate a third member's item into the peer while THIS node's tombstone
		// set is still rebuilding.
		if !selfResyncing {
			for _, gid := range groupsByDomain[domain] {
				rafter := ""
				for page := 0; page < syncReconcileMaxPages; page++ {
					if enqueued >= syncReconcileMaxEnqueue {
						break
					}
					rcands, next, rErr := ss.ListGroupRelayCandidates(ctx, gid, chain, m.localChainID, domain, rafter, syncReconcileMaxEnqueue)
					if rErr != nil {
						break
					}
					for _, rc := range rcands {
						if enqueued >= syncReconcileMaxEnqueue {
							break
						}
						if decided[rc.OriginMemoryID] {
							continue // peer already holds this origin's copy
						}
						created, eErr := ss.EnqueueRelayedSyncOutbox(ctx, chain, rc.LocalMemoryID, rc.OriginChainID)
						if eErr != nil || !created {
							continue
						}
						enqueued++
					}
					if next == "" {
						break
					}
					rafter = next
				}
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
