package federation

// v11.8 group removal ENFORCEMENT (build step 7, docs/v11.8-PLAN.md §10 rows
// 1-3, §5.5/§5.6). The apply layer (step 5) already sets removed/left member
// states + removed_revision on domains + advisory tombstones inside
// appendAndApply's per-entry transaction. This file wires the two things apply()
// CANNOT do from inside that transaction:
//
//   1. A GROUP-SCOPED purge toward each evicted chain (PurgeGroupSyncPeerState,
//      its own tx + LockSyncPolicyWrite) — stops group backfill/outbox toward the
//      ex-member while preserving any independent pairwise relationship (I10).
//      NEVER PurgeSyncPeerState, which would wipe pairwise consent.
//   2. The mandatory on-chain ANCHOR of the current roster_journal_head into
//      sage-syncaudit-<group_id> (a consensus broadcast) — best-effort,
//      rate-limited, and log-and-continue on failure.
//
// The hook runs as a POST-BATCH step (after the append completes and journalMu is
// released), keyed on the chains/domains that TRANSITIONED in the applied batch
// (accumulated by apply() into gs.evictedChains / gs.removedDomains), so it is
// idempotent and cheap. It is invoked identically from BOTH ingest paths — a
// controller locally authoring a member_remove (AppendGroupJournalEntry) and a
// member pulling the same entry (PullGroupJournal) — so enforcement is uniform.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

// enforceRemovalBatch is the POST-BATCH removal-enforcement hook. The caller
// invokes it AFTER the journal append completes and m.journalMu is released,
// passing the chains and domains that transitioned to removed/left in the
// just-applied batch. A no-op when nothing transitioned. Its own store writes use
// a NON-CANCELABLE context: like the recordAdmitted discipline, post-commit
// cleanup (the purge + anchor) must land even if the driving request was
// cancelled — otherwise a straggler group outbox row or a missing anchor would
// leak past the removal. Never called from inside apply().
func (m *Manager) enforceRemovalBatch(ss *store.SQLiteStore, groupID string, evictedChains, removedDomains []string) {
	if ss == nil || groupID == "" || (len(evictedChains) == 0 && len(removedDomains) == 0) {
		return
	}
	ctx := context.Background()
	// Group-scoped purge toward each DISTINCT evicted chain (skip self and empties).
	seen := make(map[string]struct{}, len(evictedChains))
	for _, chain := range evictedChains {
		if chain == "" || chain == m.localChainID {
			continue
		}
		if _, dup := seen[chain]; dup {
			continue
		}
		seen[chain] = struct{}{}
		if err := ss.PurgeGroupSyncPeerState(ctx, chain); err != nil {
			m.logger.Warn().Err(err).Str("peer", chain).Str("group", groupID).
				Msg("sync: group-scoped removal purge failed")
		}
	}
	// Mandatory anchor (§5.5/§5.6) of the sub-chain head that CARRIED each removal:
	// the ROSTER head for member removals/leaves, and each removed domain's OWN
	// sub-chain head for domain removals. A domain_remove lands on domain:<tag>, which
	// NEVER advances the roster head — anchoring only the roster head would silently
	// leave domain removals un-attested and adversarially truncatable (the §5.5
	// anti-truncation guarantee would not cover them). Rate-limited per (group, subchain)
	// head; any member OR domain removal in the batch anchors its own sub-chain.
	if len(evictedChains) > 0 {
		m.anchorSubchain(ctx, ss, groupID, RosterSubchain)
	}
	seenDom := make(map[string]struct{}, len(removedDomains))
	for _, dom := range removedDomains {
		if dom == "" {
			continue
		}
		if _, dup := seenDom[dom]; dup {
			continue
		}
		seenDom[dom] = struct{}{}
		m.anchorSubchain(ctx, ss, groupID, DomainSubchain(dom))
	}
}

// anchorSubchain best-effort records a group SUB-CHAIN head on-chain (docs §5.5/§5.6):
// a locally-signed MemorySubmit into the writable shared prefix sage-syncaudit-<group_id>.
// Anchoring the specific sub-chain that carried a removal (roster for members, domain:<tag>
// for domains) is what makes the §5.5 anti-truncation guarantee actually cover DOMAIN
// removals, which never advance the roster head. Rate-limited to at most once per distinct
// (group, subchain) head, and log-and-continue on any failure — a removal is NEVER blocked
// on the anchor (the durable roster_revision_floor is the hard anti-rollback; this anchor
// is auditable defense-in-depth).
func (m *Manager) anchorSubchain(ctx context.Context, ss *store.SQLiteStore, groupID, subchain string) {
	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain)
	if err != nil {
		m.logger.Debug().Err(err).Str("group", groupID).Str("subchain", subchain).Msg("sync: anchor skipped (subchain head read failed)")
		return
	}
	if head == nil || head.EntryHash == "" {
		return
	}
	key := groupID + "|" + subchain

	// Rate-limit: skip if this exact (group, subchain) head was already anchored.
	m.syncAnchorMu.Lock()
	if m.syncAnchoredHead == nil {
		m.syncAnchoredHead = make(map[string]string)
	}
	if m.syncAnchoredHead[key] == head.EntryHash {
		m.syncAnchorMu.Unlock()
		return
	}
	m.syncAnchoredHead[key] = head.EntryHash
	m.syncAnchorMu.Unlock()

	fn := m.syncAnchorFn
	if fn == nil {
		fn = m.broadcastSubchainAnchor
	}
	if err := fn(ctx, groupID, subchain, head.EntryHash); err != nil {
		// Roll the rate-limit marker back so a later batch retries the anchor.
		m.syncAnchorMu.Lock()
		if m.syncAnchoredHead[key] == head.EntryHash {
			delete(m.syncAnchoredHead, key)
		}
		m.syncAnchorMu.Unlock()
		m.logger.Warn().Err(err).Str("group", groupID).Str("subchain", subchain).Msg("sync: subchain head anchor failed (non-fatal)")
	}
}

// broadcastSubchainAnchor is the production anchor: an operator-signed
// TxTypeMemorySubmit carrying a (group, subchain, head) commitment into
// sage-syncaudit-<group_id>, reusing the same buildSyncSubmitTx +
// broadcastSyncSubmit path as an admitted copy (so nonce/encoding/duplicate
// handling are shared). The memory id is deterministic in (group, subchain, head),
// so a redelivery collapses to a duplicate-success rather than a second anchor.
func (m *Manager) broadcastSubchainAnchor(_ context.Context, groupID, subchain, head string) error {
	content := "journal_head:" + groupID + ":" + subchain + ":" + head
	sum := sha256.Sum256([]byte(content))
	item := &SyncItem{
		OriginChainID:   m.localChainID,
		OriginMemoryID:  "syncanchor|" + groupID + "|" + subchain + "|" + head,
		OriginCreatedAt: time.Now().UTC().Format(time.RFC3339),
		Domain:          store.SyncAuditDomainPrefix + groupID,
		Classification:  0,
		MemoryType:      "fact",
		ConfidenceScore: 1,
		Content:         content,
		ContentHash:     hex.EncodeToString(sum[:]),
	}
	localID := syncMemoryID(m.localChainID, item.OriginMemoryID)
	outcome, _ := m.broadcastSyncSubmit(localID, item)
	switch outcome {
	case SyncOutcomeAccepted, SyncOutcomeDuplicate:
		return nil
	default:
		return fmt.Errorf("subchain anchor broadcast outcome %q", outcome)
	}
}
