package federation

// v11.8 group journal EXCHANGE (build step 4, docs/v11.8-PLAN.md §5, §9.3).
//
// Members converge their journal sub-chains by anti-entropy over POST
// /fed/v1/sync/journal. Two security-critical properties live here:
//
//   METADATA ISOLATION (§5.2): the serving node only returns a sub-chain the
//   REQUESTER is allowed to see — the 'roster' sub-chain to any active member,
//   and a 'domain:<D>' sub-chain only to members who SHARE D. A member therefore
//   never learns of a domain it does not share (no signed/durable oracle).
//
//   AUTHENTICATED INGEST: every fetched entry is verified against a key resolved
//   from the LOCAL roster (controller / self-leaving member / domain owner) — the
//   AuthorKeyResolver contract — never against a key taken from the entry itself.
//   Ingest is append-only with fork detection (a different signature-valid entry
//   at an existing seq = controller equivocation -> halt + surface) and
//   anti-truncation (a new entry must extend the local head, seq+1 / prev-links).
//   Semantic anti-rollback (roster_revision < floor) and APPLYING entries to the
//   roster tables are step 5 (they need manifest/state semantics); step 4 keeps
//   the verified log converged and tracks per-member position.

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

// SyncJournalMaxEntries bounds one exchange page.
const SyncJournalMaxEntries = 256

// syncJournalPullTimeout bounds ONE page fetch (never held across a page), and
// syncJournalMaxPages is a hard backstop on pages per pull so no cursor sequence
// a peer returns can spin unboundedly.
const (
	syncJournalPullTimeout = 20 * time.Second
	syncJournalMaxPages    = 4096
)

// JournalWireVersion is the /fed/v1/sync/journal protocol version. Version 2 is
// deliberately mandatory: v11.8 has not shipped, and v1 omitted the controller
// approval fields required to authenticate owner-authored domain_add entries.
const JournalWireVersion = 2

// SyncJournalRequest asks for entries of one sub-chain after an exclusive cursor.
type SyncJournalRequest struct {
	Version  int    `json:"version,omitempty"`
	GroupID  string `json:"group_id"`
	Subchain string `json:"subchain"`  // 'roster' or 'domain:<tag>'
	AfterSeq int64  `json:"after_seq"` // exclusive; -1 = from genesis
	Limit    int    `json:"limit,omitempty"`
}

// JournalEntryWire is one signed journal entry on the wire (group_id is implied
// by the request, so it is not transmitted).
type JournalEntryWire struct {
	Subchain              string `json:"subchain"`
	Seq                   int64  `json:"seq"`
	PrevHash              string `json:"prev_hash"`
	EntryHash             string `json:"entry_hash"`
	EntryType             string `json:"entry_type"`
	PayloadJSON           string `json:"payload_json"`
	AuthorChainID         string `json:"author_chain_id"`
	AuthorAgentPubkey     string `json:"author_agent_pubkey"`
	AuthorSig             string `json:"author_sig"`
	ControllerEpoch       string `json:"controller_epoch"`
	ControllerChainID     string `json:"controller_chain_id"`
	ControllerAgentPubkey string `json:"controller_agent_pubkey"`
	ControllerSig         string `json:"controller_sig"`
}

// SyncJournalResponse returns a page of entries plus the server's roster head
// (for convergence tracking + operator-visible fork detection).
type SyncJournalResponse struct {
	Version      int                `json:"version"`
	Entries      []JournalEntryWire `json:"entries"`
	NextCursor   int64              `json:"next_cursor"`
	RosterHead   string             `json:"roster_head,omitempty"`
	TerminalOnly bool               `json:"terminal_only,omitempty"`
}

func storeToWire(e store.SyncGroupLogEntry) JournalEntryWire {
	return JournalEntryWire{
		Subchain: e.Subchain, Seq: e.Seq, PrevHash: e.PrevHash, EntryHash: e.EntryHash,
		EntryType: e.EntryType, PayloadJSON: e.PayloadJSON, AuthorChainID: e.AuthorChainID,
		AuthorAgentPubkey: e.AuthorAgentPubkey, AuthorSig: e.AuthorSig,
		ControllerEpoch: e.ControllerEpoch, ControllerChainID: e.ControllerChainID,
		ControllerAgentPubkey: e.ControllerAgentPubkey, ControllerSig: e.ControllerSig,
	}
}

func wireToStore(groupID string, w JournalEntryWire) store.SyncGroupLogEntry {
	return store.SyncGroupLogEntry{
		GroupID: groupID, Subchain: w.Subchain, Seq: w.Seq, PrevHash: w.PrevHash, EntryHash: w.EntryHash,
		EntryType: w.EntryType, PayloadJSON: w.PayloadJSON, AuthorChainID: w.AuthorChainID,
		AuthorAgentPubkey: w.AuthorAgentPubkey, AuthorSig: w.AuthorSig,
		ControllerEpoch: w.ControllerEpoch, ControllerChainID: w.ControllerChainID,
		ControllerAgentPubkey: w.ControllerAgentPubkey, ControllerSig: w.ControllerSig,
	}
}

// rosterControllerTypes are the roster entry types authored by the group
// controller (everything except the self-signed member_leave).
var rosterControllerTypes = map[string]bool{
	"group_create": true, "member_invite": true, "member_activate": true,
	"member_remove": true, "role_change": true, "epoch_rotate": true, "manifest": true,
}

var domainOwnerTypes = map[string]bool{
	"domain_add": true, "domain_remove": true, "tombstone": true, "anchor": true,
}

// journalServeScope makes the removed-domain rule explicit. A prior non-owner
// sharer may receive only the signed removal suffix, never the old journal body.
type journalServeScope uint8

const (
	journalServeDeny journalServeScope = iota
	journalServeFull
	journalServeTerminalOnly
)

func isTerminalDomainEntryType(entryType string) bool {
	return entryType == "domain_remove" || entryType == "tombstone" || entryType == "anchor"
}

// removedDomainTerminalFromSeq maps the current immutable removal generation
// (journal seq + 1) back to the first terminal entry in this lifecycle. It is
// called only after journalSubchainServeScope returned terminal-only.
func removedDomainTerminalFromSeq(ctx context.Context, ss *store.SQLiteStore, groupID, subchain string) (int64, error) {
	tag, ok := strings.CutPrefix(subchain, "domain:")
	if !ok || tag == "" {
		return 0, fmt.Errorf("terminal journal response requires a domain sub-chain")
	}
	domains, err := ss.ListSyncGroupDomains(ctx, groupID, false)
	if err != nil {
		return 0, err
	}
	for _, d := range domains {
		if d.DomainTag == tag && d.RemovedRevision > 0 {
			return d.RemovedRevision - 1, nil
		}
	}
	return 0, fmt.Errorf("removed domain generation is unavailable")
}

// groupAuthorResolver returns a SNAPSHOT AuthorKeyResolver = groupApplyState.resolve
// over the current roster (docs §5.4). Used where a single entry is checked against
// a fixed view (AppendGroupJournalEntry's self-check, a whole-chain fold of a pure
// controller chain). The INGEST path uses a live, growing groupApplyState instead,
// so an in-batch member_invite is visible to a later member_leave in the same batch.
func (m *Manager) groupAuthorResolver(ctx context.Context, ss *store.SQLiteStore, groupID string) (AuthorKeyResolver, error) {
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return nil, err
	}
	return gs.resolve, nil
}

func decodePub(hexKey string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, err
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("wrong pubkey size %d", len(b))
	}
	return ed25519.PublicKey(b), nil
}

// authorizeJournalSubchain is the METADATA-ISOLATION gate (§5.2). The requester
// must be an ACTIVE member of the group; the roster sub-chain is group-common,
// but a 'domain:<D>' sub-chain is served ONLY to members who SHARE D. Safe
// default: full-sync members and the domain owner. Selective-sync members' exact
// subsets are refined in step 5 — UNDER-serving is safe (no leak); over-serving
// would breach isolation, so the default fails closed.
func (m *Manager) authorizeJournalSubchain(ctx context.Context, ss *store.SQLiteStore, groupID, memberChainID, subchain string) (bool, error) {
	scope, err := m.journalSubchainServeScope(ctx, ss, groupID, memberChainID, subchain)
	return scope != journalServeDeny, err
}

// journalSubchainServeScope is the metadata-isolation gate with an explicit
// terminal-only result for a removed domain. The historical entitlement is read
// from an immutable snapshot taken at removal, never from live consent rows.
func (m *Manager) journalSubchainServeScope(ctx context.Context, ss *store.SQLiteStore, groupID, memberChainID, subchain string) (journalServeScope, error) {
	member, err := ss.GetSyncGroupMember(ctx, groupID, memberChainID)
	if err != nil {
		return journalServeDeny, err
	}
	if member == nil || (member.MemberState != store.GroupMemberActive && member.MemberState != store.GroupMemberResyncing) {
		return journalServeDeny, nil
	}
	if subchain == RosterSubchain {
		return journalServeFull, nil
	}
	tag, ok := strings.CutPrefix(subchain, "domain:")
	if !ok || tag == "" {
		return journalServeDeny, nil
	}
	// activeOnly=FALSE on purpose: a domain_remove/tombstone lands on the domain's
	// OWN sub-chain, so a prior sharer must still be able to PULL that sub-chain
	// after removal to learn of it — otherwise an offline member never converges on
	// the removal. Serving is still gated by role/ownership (below); step 5's apply
	// layer, not this serve gate, is what refuses APPENDING new content to a
	// removed domain.
	domains, err := ss.ListSyncGroupDomains(ctx, groupID, false)
	if err != nil {
		return journalServeDeny, err
	}
	for _, d := range domains {
		if d.DomainTag != tag {
			continue
		}
		if d.RemovedRevision == 0 {
			// ACTIVE domain: any full-sync member or the owner may pull it; a
			// selective-sync member may pull it ONLY if it holds an active consent row
			// covering the tag (exact or an ancestor) — fold-in F2. A selective member
			// with no covering consent row fails closed (under-serve, no metadata leak).
			if member.Role == store.GroupRoleFullSync || d.OwnerChainID == memberChainID {
				return journalServeFull, nil
			}
			if member.Role == store.GroupRoleSelectiveSync {
				ok, err := ss.MemberConsentsGroupDomain(ctx, groupID, memberChainID, tag)
				if err != nil || !ok {
					return journalServeDeny, err
				}
				return journalServeFull, nil
			}
			return journalServeDeny, nil
		}
		// REMOVED domain: the owner may read its full audit chain. A non-owner
		// must have been captured in the immutable removal entitlement snapshot;
		// even then it receives only terminal records, so it converges without
		// learning any prior/live domain content.
		if d.OwnerChainID == memberChainID {
			return journalServeFull, nil
		}
		wasEntitled, err := ss.WasMemberEntitledAtDomainRemoval(ctx, groupID, tag, memberChainID, d.RemovedRevision)
		if err != nil || !wasEntitled {
			return journalServeDeny, err
		}
		return journalServeTerminalOnly, nil
	}
	return journalServeDeny, nil // not a group domain (active or removed)
}

// handleSyncJournal implements POST /fed/v1/sync/journal (behind peerAuth).
func (m *Manager) handleSyncJournal(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "domain sync requires the SQLite store backend")
		return
	}
	var req SyncJournalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Version != JournalWireVersion {
		httpError(w, http.StatusBadRequest, "unsupported journal wire version")
		return
	}
	if req.GroupID == "" || req.Subchain == "" {
		httpError(w, http.StatusBadRequest, "group_id and subchain are required")
		return
	}
	// Membership and domain projections are effective serve policy. Lease the
	// snapshot from the authorization check through the final response write so a
	// completed removal guarantees that no response can still escape under the
	// old policy.
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	scope, err := m.journalSubchainServeScope(r.Context(), ss, req.GroupID, peer.ChainID, req.Subchain)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "authorization failed")
		return
	}
	if scope == journalServeDeny {
		// One generic 403 for "not a member" AND "sub-chain not shared", so the
		// response is not an oracle for which groups/domains exist (I5).
		httpError(w, http.StatusForbidden, "not authorized for this journal sub-chain")
		return
	}
	limit := req.Limit
	if limit <= 0 || limit > SyncJournalMaxEntries {
		limit = SyncJournalMaxEntries
	}
	var entries []store.SyncGroupLogEntry
	if scope == journalServeTerminalOnly {
		fromSeq, genErr := removedDomainTerminalFromSeq(r.Context(), ss, req.GroupID, req.Subchain)
		if genErr != nil {
			httpError(w, http.StatusInternalServerError, "removed-domain generation lookup failed")
			return
		}
		entries, err = ss.ListSyncGroupTerminalLog(r.Context(), req.GroupID, req.Subchain, fromSeq, req.AfterSeq, limit)
	} else {
		entries, err = ss.ListSyncGroupLog(r.Context(), req.GroupID, req.Subchain, req.AfterSeq, limit)
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, "journal read failed")
		return
	}
	resp := &SyncJournalResponse{Version: JournalWireVersion, Entries: make([]JournalEntryWire, 0, len(entries)), NextCursor: req.AfterSeq, TerminalOnly: scope == journalServeTerminalOnly}
	for _, e := range entries {
		resp.Entries = append(resp.Entries, storeToWire(e))
	}
	if len(entries) > 0 {
		resp.NextCursor = entries[len(entries)-1].Seq
	}
	if head, _ := ss.GetSyncGroupSubchainHead(r.Context(), req.GroupID, RosterSubchain); head != nil {
		resp.RosterHead = head.EntryHash
	}
	writeJSON(w, http.StatusOK, resp)
}

// ingestJournalEntriesLocked verifies, appends, AND APPLIES a batch of foreign
// entries. The CALLER MUST hold m.journalMu (so the read-head/append is atomic
// against local appends). For each entry:
//   - size cap (cheap) then fork check: an existing seq with a DIFFERENT hash =>
//     FORK (equivocation) -> halt; same hash => idempotent skip;
//   - a NEW entry is authorized against a GROWING groupApplyState (so an in-batch
//     member_invite is visible to a later member_leave), verified, and must extend
//     the local head (seq == head+1, prev-links); then appended and APPLIED to the
//     roster/domain projection tables. Returns the count appended (partial kept)
//     PLUS the chains/domains that transitioned to removed/left in this batch, so
//     the caller can run the POST-BATCH removal-enforcement hook once journalMu is
//     released (never from inside apply()'s transaction).
func (m *Manager) ingestJournalEntriesLocked(ctx context.Context, ss *store.SQLiteStore, groupID, subchain string, entries []store.SyncGroupLogEntry) (int, []string, []string, error) {
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return 0, nil, nil, err
	}
	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain)
	if err != nil {
		return 0, nil, nil, err
	}
	wantSeq, wantPrev := int64(0), ""
	if head != nil {
		wantSeq, wantPrev = head.Seq+1, head.EntryHash
	}
	appended := 0
	// gs.evictedChains/removedDomains accumulate as removal entries apply; they are
	// returned on EVERY path (including a partial-batch error) so the caller enforces
	// exactly what durably transitioned before the break.
	for i := range entries {
		e := entries[i]
		e.GroupID = groupID
		if e.Subchain != subchain {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry subchain %q != requested %q", e.Subchain, subchain)
		}
		// Size cap BEFORE any signature work (cheap; a peer can't force verifies on
		// an over-limit payload, and no over-limit entry can ever be admitted).
		if len(e.PayloadJSON) > SyncJournalMaxPayloadBytes {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry %s/%d payload %d bytes exceeds cap %d", subchain, e.Seq, len(e.PayloadJSON), SyncJournalMaxPayloadBytes)
		}
		existing, err := ss.GetSyncGroupLogEntry(ctx, groupID, subchain, e.Seq)
		if err != nil {
			return appended, gs.evictedChains, gs.removedDomains, err
		}
		if existing != nil {
			if existing.EntryHash != e.EntryHash {
				return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("FORK at %s/%d: local entry diverges from peer (equivocation) — halting ingest", subchain, e.Seq)
			}
			// Already held + already applied; advance our expected position past it.
			if e.Seq >= wantSeq {
				wantSeq, wantPrev = e.Seq+1, existing.EntryHash
			}
			continue
		}
		key := gs.resolve(e)
		if key == nil {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry %s/%d: unauthorized author", subchain, e.Seq)
		}
		if err := verifyJournalEntry(e, key); err != nil {
			return appended, gs.evictedChains, gs.removedDomains, err
		}
		if e.Seq != wantSeq {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry %s/%d out of order (want seq %d)", subchain, e.Seq, wantSeq)
		}
		if e.PrevHash != wantPrev {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry %s/%d prev_hash does not link to local head", subchain, e.Seq)
		}
		if err := m.authorizeReplicaGovernance(ctx, groupID, e); err != nil {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("replica governance rejected %s/%d: %w", subchain, e.Seq, err)
		}
		// Persist the canonical payload spelling (the signature binds the parsed map,
		// so a peer's non-Go JSON spelling is accepted above but normalized here).
		e.PayloadJSON = canonicalPayloadJSON(e.PayloadJSON)
		// ATOMIC append + apply: a store-write failure rolls the append back, so a
		// logged entry is always fully applied (semantic-fault skips are no-ops that
		// commit); the idempotent-skip path can never meet a logged-but-unapplied entry.
		if err := m.appendAndApply(ctx, ss, gs, e); err != nil {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("append+apply %s/%d (%s): %w", subchain, e.Seq, e.EntryType, err)
		}
		appended++
		wantSeq, wantPrev = e.Seq+1, e.EntryHash
	}
	if subchain == RosterSubchain && appended > 0 {
		_ = ss.SetSyncGroupRosterJournalHead(ctx, groupID, wantPrev)
	}
	return appended, gs.evictedChains, gs.removedDomains, nil
}

// ingestTerminalJournalEntriesLocked admits the deliberately sparse terminal
// suffix served after a domain removal. It keeps every normal ingest invariant
// except contiguous prev-link availability: a prior sharer is intentionally not
// sent the old domain body, so it cannot possess the immediate predecessor hash.
// Each entry remains independently signature-verified against the pinned roster
// key, type-restricted, strictly ascending, and fork-checked before append.
// CALLER MUST hold m.journalMu.
func (m *Manager) ingestTerminalJournalEntriesLocked(ctx context.Context, ss *store.SQLiteStore, groupID, subchain string, entries []store.SyncGroupLogEntry) (int, []string, []string, error) {
	if _, ok := strings.CutPrefix(subchain, "domain:"); !ok {
		return 0, nil, nil, fmt.Errorf("terminal journal response is valid only for a domain sub-chain")
	}
	gs, err := loadGroupApplyState(ctx, ss, groupID)
	if err != nil {
		return 0, nil, nil, err
	}
	var lastSeq int64 = -1
	appended := 0
	for i := range entries {
		e := entries[i]
		e.GroupID = groupID
		if e.Subchain != subchain || !isTerminalDomainEntryType(e.EntryType) {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("terminal response contains non-terminal entry %s/%d (%s)", e.Subchain, e.Seq, e.EntryType)
		}
		if len(e.PayloadJSON) > SyncJournalMaxPayloadBytes {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry %s/%d payload %d bytes exceeds cap %d", subchain, e.Seq, len(e.PayloadJSON), SyncJournalMaxPayloadBytes)
		}
		if e.Seq <= lastSeq {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("terminal response is not strictly ascending at seq %d", e.Seq)
		}
		lastSeq = e.Seq
		existing, err := ss.GetSyncGroupLogEntry(ctx, groupID, subchain, e.Seq)
		if err != nil {
			return appended, gs.evictedChains, gs.removedDomains, err
		}
		if existing != nil {
			if existing.EntryHash != e.EntryHash {
				return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("FORK at %s/%d: local entry diverges from peer (equivocation) — halting ingest", subchain, e.Seq)
			}
			continue
		}
		key := gs.resolve(e)
		if key == nil {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("entry %s/%d: unauthorized terminal author", subchain, e.Seq)
		}
		if err := verifyJournalEntry(e, key); err != nil {
			return appended, gs.evictedChains, gs.removedDomains, err
		}
		e.PayloadJSON = canonicalPayloadJSON(e.PayloadJSON)
		if err := m.appendAndApply(ctx, ss, gs, e); err != nil {
			return appended, gs.evictedChains, gs.removedDomains, fmt.Errorf("append+apply terminal %s/%d (%s): %w", subchain, e.Seq, e.EntryType, err)
		}
		appended++
	}
	return appended, gs.evictedChains, gs.removedDomains, nil
}

// PullGroupJournal fetches a peer's sub-chain from the local head onward,
// verifies + ingests it, and (for the roster) records the peer's head for
// per-member convergence (§9.3). Returns the number of entries appended.
func (m *Manager) PullGroupJournal(ctx context.Context, remoteChainID, groupID, subchain string) (int, error) {
	ss := m.syncStore()
	if ss == nil {
		return 0, fmt.Errorf("group journal requires the SQLite store backend")
	}
	pull := m.syncJournalFn
	if pull == nil {
		pull = m.SyncJournalPull
	}
	// The cursor is driven by OUR OWN head, never by the peer's NextCursor: a
	// hostile member could otherwise replay already-held entries with a fabricated
	// cursor and spin us forever re-verifying signatures. Normal pages must begin
	// at after+1 and be contiguous+ascending. A v2 terminal-only page is the sole
	// exception: it is a type-restricted, signed sparse suffix for a domain already
	// removed from the requester's entitlement.
	m.journalMu.Lock()
	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain)
	m.journalMu.Unlock()
	if err != nil {
		return 0, err
	}
	after := int64(-1)
	if head != nil {
		after = head.Seq
	}
	appended := 0
	// Accumulate the removals that transition across ALL pages, and run the
	// enforcement hook ONCE after the pull returns — AFTER journalMu is released
	// (the per-page ingest lock is dropped each iteration). A no-op when nothing
	// transitioned; fires even on an early error return so a partial batch still
	// enforces exactly what durably applied (docs §10, POST-BATCH hook).
	var evictedChains, removedDomains []string
	defer func() { m.enforceRemovalBatch(ss, groupID, evictedChains, removedDomains) }()
	var peerRosterHead string
	for page := 0; page < syncJournalMaxPages; page++ {
		if ctx.Err() != nil {
			return appended, ctx.Err()
		}
		// Fetch LOCK-FREE with a bounded per-page deadline, so a slow/hostile peer
		// can never pin journalMu (and thus stall all local journal appends).
		pctx, cancel := context.WithTimeout(ctx, syncJournalPullTimeout)
		resp, pErr := pull(pctx, remoteChainID, &SyncJournalRequest{Version: JournalWireVersion, GroupID: groupID, Subchain: subchain, AfterSeq: after, Limit: SyncJournalMaxEntries})
		cancel()
		if pErr != nil {
			return appended, pErr
		}
		if resp == nil {
			return appended, fmt.Errorf("peer %s returned a nil journal response", remoteChainID)
		}
		if resp.Version != JournalWireVersion {
			return appended, fmt.Errorf("peer %s returned unsupported journal response version %d (want %d)", remoteChainID, resp.Version, JournalWireVersion)
		}
		if resp.RosterHead != "" {
			peerRosterHead = resp.RosterHead
		}
		if len(resp.Entries) == 0 {
			break
		}
		if len(resp.Entries) > SyncJournalMaxEntries {
			return appended, fmt.Errorf("peer %s returned %d entries (max %d)", remoteChainID, len(resp.Entries), SyncJournalMaxEntries)
		}
		if resp.TerminalOnly && !strings.HasPrefix(subchain, "domain:") {
			return appended, fmt.Errorf("peer %s returned terminal-only roster response", remoteChainID)
		}
		// Contiguity/ascending check BEFORE any verification work — a peer cannot
		// make us re-verify entries at or below our head. Terminal-only pages are
		// verified by their dedicated sparse-ingest path below.
		st := make([]store.SyncGroupLogEntry, 0, len(resp.Entries))
		for i, wentry := range resp.Entries {
			if !resp.TerminalOnly && wentry.Seq != after+1+int64(i) {
				return appended, fmt.Errorf("peer %s: non-contiguous journal page at seq %d (want %d)", remoteChainID, wentry.Seq, after+1+int64(i))
			}
			if resp.TerminalOnly && (wentry.Seq <= after || !isTerminalDomainEntryType(wentry.EntryType)) {
				return appended, fmt.Errorf("peer %s returned invalid terminal journal entry %s/%d (%s)", remoteChainID, wentry.Subchain, wentry.Seq, wentry.EntryType)
			}
			st = append(st, wireToStore(groupID, wentry))
		}
		maxSeq := st[len(st)-1].Seq
		// Ingest this page under the lock (fetch was lock-free); re-reads the head
		// each iteration so append-atomicity holds.
		m.journalMu.Lock()
		var n int
		var evicted, removed []string
		var iErr error
		if resp.TerminalOnly {
			n, evicted, removed, iErr = m.ingestTerminalJournalEntriesLocked(ctx, ss, groupID, subchain, st)
		} else {
			n, evicted, removed, iErr = m.ingestJournalEntriesLocked(ctx, ss, groupID, subchain, st)
		}
		m.journalMu.Unlock()
		appended += n
		evictedChains = append(evictedChains, evicted...)
		removedDomains = append(removedDomains, removed...)
		if iErr != nil {
			return appended, iErr
		}
		if maxSeq <= after || len(resp.Entries) < SyncJournalMaxEntries {
			break // no forward progress (defensive) or last page
		}
		after = maxSeq
	}
	// Convergence tracking (§9.3, the #3 dashboard "N revisions behind / last
	// synced T / converged Y/N"):
	//   - the PEER's observed roster head (a no-op if the peer isn't a member row),
	//     leaving last_acked_roster_revision — which the apply layer owns — untouched;
	//   - THIS node's own catch-up: once the roster page is verified + applied, our
	//     self member row advances to the group's current roster_revision (monotonic
	//     via SetSyncGroupManifest), so "am I converged" is renderable. Only after a
	//     real append, and never regressing (roster_revision only advances).
	if subchain == RosterSubchain {
		now := time.Now().UTC().Format(time.RFC3339)
		if peerRosterHead != "" {
			_ = ss.SetSyncGroupMemberSeen(ctx, groupID, remoteChainID, peerRosterHead, now)
		}
		if appended > 0 {
			if g, gErr := ss.GetSyncGroup(ctx, groupID); gErr == nil && g != nil {
				_ = ss.UpdateSyncGroupMemberProgress(ctx, groupID, m.localChainID, g.RosterRevision, g.RosterJournalHead, now)
			}
		}
	}
	return appended, nil
}

// SyncJournalPull is the production client for POST /fed/v1/sync/journal.
func (m *Manager) SyncJournalPull(ctx context.Context, remoteChainID string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
	if req.Version == 0 {
		req.Version = JournalWireVersion
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/journal", req)
	if err != nil {
		return nil, err
	}
	if ok, unsupported := classifySyncStatus(status); !ok {
		if unsupported {
			return nil, ErrSyncUnsupported
		}
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out SyncJournalResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sync journal response: %w", err)
	}
	return &out, nil
}
