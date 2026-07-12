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

// SyncJournalRequest asks for entries of one sub-chain after an exclusive cursor.
type SyncJournalRequest struct {
	GroupID  string `json:"group_id"`
	Subchain string `json:"subchain"`  // 'roster' or 'domain:<tag>'
	AfterSeq int64  `json:"after_seq"` // exclusive; -1 = from genesis
	Limit    int    `json:"limit,omitempty"`
}

// JournalEntryWire is one signed journal entry on the wire (group_id is implied
// by the request, so it is not transmitted).
type JournalEntryWire struct {
	Subchain          string `json:"subchain"`
	Seq               int64  `json:"seq"`
	PrevHash          string `json:"prev_hash"`
	EntryHash         string `json:"entry_hash"`
	EntryType         string `json:"entry_type"`
	PayloadJSON       string `json:"payload_json"`
	AuthorChainID     string `json:"author_chain_id"`
	AuthorAgentPubkey string `json:"author_agent_pubkey"`
	AuthorSig         string `json:"author_sig"`
}

// SyncJournalResponse returns a page of entries plus the server's roster head
// (for convergence tracking + operator-visible fork detection).
type SyncJournalResponse struct {
	Entries    []JournalEntryWire `json:"entries"`
	NextCursor int64              `json:"next_cursor"`
	RosterHead string             `json:"roster_head,omitempty"`
}

func storeToWire(e store.SyncGroupLogEntry) JournalEntryWire {
	return JournalEntryWire{
		Subchain: e.Subchain, Seq: e.Seq, PrevHash: e.PrevHash, EntryHash: e.EntryHash,
		EntryType: e.EntryType, PayloadJSON: e.PayloadJSON, AuthorChainID: e.AuthorChainID,
		AuthorAgentPubkey: e.AuthorAgentPubkey, AuthorSig: e.AuthorSig,
	}
}

func wireToStore(groupID string, w JournalEntryWire) store.SyncGroupLogEntry {
	return store.SyncGroupLogEntry{
		GroupID: groupID, Subchain: w.Subchain, Seq: w.Seq, PrevHash: w.PrevHash, EntryHash: w.EntryHash,
		EntryType: w.EntryType, PayloadJSON: w.PayloadJSON, AuthorChainID: w.AuthorChainID,
		AuthorAgentPubkey: w.AuthorAgentPubkey, AuthorSig: w.AuthorSig,
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

// groupAuthorResolver builds an AuthorKeyResolver from a SNAPSHOT of the local
// roster (docs §5.4). It returns the AUTHORITATIVE key an entry must be signed by
// and NEVER trusts the entry's own author field:
//   - roster controller-types  -> the group's controller_agent_pubkey (and the
//     entry's author_chain_id must equal the controller's chain);
//   - roster member_leave       -> the leaving member's member_agent_pubkey
//     (self-signed; the payload member==author is enforced at apply/step 5);
//   - domain sub-chain types    -> the domain OWNER's member_agent_pubkey (and
//     author_chain_id must equal owner_chain_id).
// Any other (type, subchain, author) combination returns nil -> the fold/ingest
// rejects the entry. (Epoch rotation changing the controller mid-chain, and
// members added within the same batch, are refined in step 5.)
func (m *Manager) groupAuthorResolver(ctx context.Context, ss *store.SQLiteStore, groupID string) (AuthorKeyResolver, error) {
	g, err := ss.GetSyncGroup(ctx, groupID)
	if err != nil {
		return nil, err
	}
	if g == nil {
		return nil, fmt.Errorf("group %s not found", groupID)
	}
	ctlPub, err := decodePub(g.ControllerAgentPubkey)
	if err != nil {
		return nil, fmt.Errorf("group %s: invalid controller pubkey: %w", groupID, err)
	}
	members, err := ss.ListSyncGroupMembers(ctx, groupID)
	if err != nil {
		return nil, err
	}
	memberKey := make(map[string]ed25519.PublicKey, len(members))
	for _, mem := range members {
		if k, e := decodePub(mem.MemberAgentPubkey); e == nil {
			memberKey[mem.MemberChainID] = k
		}
	}
	domains, err := ss.ListSyncGroupDomains(ctx, groupID, false)
	if err != nil {
		return nil, err
	}
	domainOwner := make(map[string]string, len(domains))
	for _, d := range domains {
		domainOwner[d.DomainTag] = d.OwnerChainID
	}

	return func(e store.SyncGroupLogEntry) ed25519.PublicKey {
		if e.Subchain == RosterSubchain {
			if e.EntryType == "member_leave" {
				return memberKey[e.AuthorChainID] // self-signed by the leaver
			}
			if rosterControllerTypes[e.EntryType] && e.AuthorChainID == g.ControllerChainID {
				return ctlPub
			}
			return nil
		}
		if tag, ok := strings.CutPrefix(e.Subchain, "domain:"); ok {
			owner, known := domainOwner[tag]
			if known && domainOwnerTypes[e.EntryType] && e.AuthorChainID == owner {
				return memberKey[owner]
			}
		}
		return nil
	}, nil
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
	member, err := ss.GetSyncGroupMember(ctx, groupID, memberChainID)
	if err != nil {
		return false, err
	}
	if member == nil || (member.MemberState != store.GroupMemberActive && member.MemberState != store.GroupMemberResyncing) {
		return false, nil
	}
	if subchain == RosterSubchain {
		return true, nil
	}
	tag, ok := strings.CutPrefix(subchain, "domain:")
	if !ok || tag == "" {
		return false, nil
	}
	domains, err := ss.ListSyncGroupDomains(ctx, groupID, true) // active only
	if err != nil {
		return false, err
	}
	for _, d := range domains {
		if d.DomainTag != tag {
			continue
		}
		return member.Role == store.GroupRoleFullSync || d.OwnerChainID == memberChainID, nil
	}
	return false, nil // not an active group domain
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
	if req.GroupID == "" || req.Subchain == "" {
		httpError(w, http.StatusBadRequest, "group_id and subchain are required")
		return
	}
	ok, err := m.authorizeJournalSubchain(r.Context(), ss, req.GroupID, peer.ChainID, req.Subchain)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "authorization failed")
		return
	}
	if !ok {
		// One generic 403 for "not a member" AND "sub-chain not shared", so the
		// response is not an oracle for which groups/domains exist (I5).
		httpError(w, http.StatusForbidden, "not authorized for this journal sub-chain")
		return
	}
	limit := req.Limit
	if limit <= 0 || limit > SyncJournalMaxEntries {
		limit = SyncJournalMaxEntries
	}
	entries, err := ss.ListSyncGroupLog(r.Context(), req.GroupID, req.Subchain, req.AfterSeq, limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "journal read failed")
		return
	}
	resp := &SyncJournalResponse{Entries: make([]JournalEntryWire, 0, len(entries)), NextCursor: req.AfterSeq}
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

// ingestJournalEntriesLocked verifies and appends a batch of foreign entries.
// The CALLER MUST hold m.journalMu (so the read-head/append is atomic against
// local appends). It is append-only: verify author (resolver) + hash/sig, then
//   - an existing seq with a DIFFERENT hash => FORK (equivocation) -> halt;
//   - an existing seq with the same hash    => idempotent skip;
//   - a new entry must extend the local head (seq == head+1, prev-links).
// Returns the count appended and the first error (partial progress is kept).
func (m *Manager) ingestJournalEntriesLocked(ctx context.Context, ss *store.SQLiteStore, groupID, subchain string, resolve AuthorKeyResolver, entries []store.SyncGroupLogEntry) (int, error) {
	appended := 0
	for i := range entries {
		e := entries[i]
		e.GroupID = groupID
		if e.Subchain != subchain {
			return appended, fmt.Errorf("entry subchain %q != requested %q", e.Subchain, subchain)
		}
		key := resolve(e)
		if key == nil {
			return appended, fmt.Errorf("entry %s/%d: unauthorized author", subchain, e.Seq)
		}
		if err := verifyJournalEntry(e, key); err != nil {
			return appended, err
		}
		existing, err := ss.GetSyncGroupLogEntry(ctx, groupID, subchain, e.Seq)
		if err != nil {
			return appended, err
		}
		if existing != nil {
			if existing.EntryHash != e.EntryHash {
				return appended, fmt.Errorf("FORK at %s/%d: local entry diverges from peer (equivocation) — halting ingest", subchain, e.Seq)
			}
			continue // idempotent
		}
		head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain)
		if err != nil {
			return appended, err
		}
		wantSeq, wantPrev := int64(0), ""
		if head != nil {
			wantSeq, wantPrev = head.Seq+1, head.EntryHash
		}
		if e.Seq != wantSeq {
			return appended, fmt.Errorf("entry %s/%d out of order (want seq %d)", subchain, e.Seq, wantSeq)
		}
		if e.PrevHash != wantPrev {
			return appended, fmt.Errorf("entry %s/%d prev_hash does not link to local head", subchain, e.Seq)
		}
		if err := ss.AppendSyncGroupLog(ctx, e); err != nil {
			return appended, err
		}
		appended++
	}
	if subchain == RosterSubchain && appended > 0 {
		if head, _ := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain); head != nil {
			_ = ss.SetSyncGroupRosterJournalHead(ctx, groupID, head.EntryHash)
		}
	}
	return appended, nil
}

// PullGroupJournal fetches a peer's sub-chain from the local head onward,
// verifies + ingests it, and (for the roster) records the peer's head for
// per-member convergence (§9.3). Returns the number of entries appended.
func (m *Manager) PullGroupJournal(ctx context.Context, remoteChainID, groupID, subchain string) (int, error) {
	ss := m.syncStore()
	if ss == nil {
		return 0, fmt.Errorf("group journal requires the SQLite store backend")
	}
	resolve, err := m.groupAuthorResolver(ctx, ss, groupID)
	if err != nil {
		return 0, err
	}
	pull := m.syncJournalFn
	if pull == nil {
		pull = m.SyncJournalPull
	}
	m.journalMu.Lock()
	defer m.journalMu.Unlock()

	head, err := ss.GetSyncGroupSubchainHead(ctx, groupID, subchain)
	if err != nil {
		return 0, err
	}
	after := int64(-1)
	if head != nil {
		after = head.Seq
	}
	appended := 0
	var peerRosterHead string
	for {
		resp, err := pull(ctx, remoteChainID, &SyncJournalRequest{GroupID: groupID, Subchain: subchain, AfterSeq: after, Limit: SyncJournalMaxEntries})
		if err != nil {
			return appended, err
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
		st := make([]store.SyncGroupLogEntry, 0, len(resp.Entries))
		for _, wentry := range resp.Entries {
			st = append(st, wireToStore(groupID, wentry))
		}
		n, iErr := m.ingestJournalEntriesLocked(ctx, ss, groupID, subchain, resolve, st)
		appended += n
		if iErr != nil {
			return appended, iErr
		}
		if resp.NextCursor <= after || len(resp.Entries) < SyncJournalMaxEntries {
			break // no forward progress or last page
		}
		after = resp.NextCursor
	}
	// Convergence tracking: record the peer's roster head without disturbing our
	// own last_acked_roster_revision (that advances at apply/step 5).
	if subchain == RosterSubchain && peerRosterHead != "" {
		if mem, _ := ss.GetSyncGroupMember(ctx, groupID, remoteChainID); mem != nil {
			_ = ss.UpdateSyncGroupMemberProgress(ctx, groupID, remoteChainID, mem.LastAckedRosterRevision, peerRosterHead, time.Now().UTC().Format(time.RFC3339))
		}
	}
	return appended, nil
}

// SyncJournalPull is the production client for POST /fed/v1/sync/journal.
func (m *Manager) SyncJournalPull(ctx context.Context, remoteChainID string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
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
