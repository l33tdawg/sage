package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

// journalAs drives handleSyncJournal with an injected authenticated peer.
func journalAs(t *testing.T, m *Manager, peerChain string, req SyncJournalRequest) (*httptest.ResponseRecorder, *SyncJournalResponse) {
	t.Helper()
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/journal", bytes.NewReader(body))
	httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, &peerIdentity{ChainID: peerChain}))
	rr := httptest.NewRecorder()
	m.handleSyncJournal(rr, httpReq)
	if rr.Code != http.StatusOK {
		return rr, nil
	}
	var resp SyncJournalResponse
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	return rr, &resp
}

// seedGroup creates a group with a controller and returns its keypair.
func seedGroup(t *testing.T, ms *store.SQLiteStore, groupID, controllerChain string) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	ctx := context.Background()
	pub, key, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{
		GroupID: groupID, ControllerChainID: controllerChain, ControllerAgentPubkey: hex.EncodeToString(pub),
	}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	return pub, key
}

// TestJournalSubchainAuthorization is the metadata-isolation gate (§5.2).
func TestJournalSubchainAuthorization(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	add := func(chain, role, state string) {
		if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
			GroupID: "g1", MemberChainID: chain, Role: role, MemberState: state,
		}); err != nil {
			t.Fatalf("add member %s: %v", chain, err)
		}
	}
	add("chain-full", store.GroupRoleFullSync, store.GroupMemberActive)
	add("chain-sel", store.GroupRoleSelectiveSync, store.GroupMemberActive)
	add("chain-owner", store.GroupRoleSelectiveSync, store.GroupMemberActive)
	add("chain-invited", store.GroupRoleFullSync, store.GroupMemberInvited)
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "eurorack", OwnerChainID: "chain-owner"}); err != nil {
		t.Fatalf("seed domain: %v", err)
	}

	check := func(member, subchain string, want bool) {
		got, err := m.authorizeJournalSubchain(ctx, ms, "g1", member, subchain)
		if err != nil {
			t.Fatalf("authz(%s,%s) err: %v", member, subchain, err)
		}
		if got != want {
			t.Fatalf("authz(%s,%s)=%v want %v", member, subchain, got, want)
		}
	}
	check("chain-full", RosterSubchain, true)
	check("chain-nonmember", RosterSubchain, false)   // not a member
	check("chain-invited", RosterSubchain, false)      // not active
	check("chain-full", DomainSubchain("eurorack"), true)      // full-sync sees all shared domains
	check("chain-sel", DomainSubchain("eurorack"), false)      // selective non-owner: safe-default deny (no leak)
	check("chain-owner", DomainSubchain("eurorack"), true)     // domain owner
	check("chain-full", DomainSubchain("does-not-exist"), false) // not a group domain
	check("chain-full", "bogus-subchain", false)                 // malformed subchain
}

// TestPullGroupJournalIngest exercises the end-to-end pull -> verify -> ingest ->
// convergence path with an injected peer serving a controller-signed chain.
func TestPullGroupJournalIngest(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	memPub, _, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-peer", Role: store.GroupRoleFullSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: hex.EncodeToString(memPub),
	}); err != nil {
		t.Fatalf("add peer member: %v", err)
	}

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey, map[string]string{"member": "chain-peer"})
	peerChain := []store.SyncGroupLogEntry{e0, e1}
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		resp := &SyncJournalResponse{NextCursor: req.AfterSeq, RosterHead: e1.EntryHash}
		for _, e := range peerChain {
			if e.Seq > req.AfterSeq {
				resp.Entries = append(resp.Entries, storeToWire(e))
			}
		}
		if len(resp.Entries) > 0 {
			resp.NextCursor = resp.Entries[len(resp.Entries)-1].Seq
		}
		return resp, nil
	}

	n, err := m.PullGroupJournal(ctx, "chain-peer", "g1", RosterSubchain)
	if err != nil || n != 2 {
		t.Fatalf("pull: n=%d err=%v", n, err)
	}
	head, _ := ms.GetSyncGroupSubchainHead(ctx, "g1", RosterSubchain)
	if head == nil || head.EntryHash != e1.EntryHash {
		t.Fatalf("head not advanced: %+v", head)
	}
	// Idempotent: re-pull appends nothing.
	if n2, err := m.PullGroupJournal(ctx, "chain-peer", "g1", RosterSubchain); err != nil || n2 != 0 {
		t.Fatalf("idempotent re-pull: n=%d err=%v", n2, err)
	}
	// Convergence tracking recorded the peer's head without disturbing last_acked.
	mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-peer")
	if mem.LastSeenJournalHead != e1.EntryHash || mem.LastAckedRosterRevision != 0 {
		t.Fatalf("convergence not tracked cleanly: %+v", mem)
	}
}

// TestJournalIngestRejectsForgeryForkAndDisorder covers the three ingest guards.
func TestJournalIngestRejectsForgeryForkAndDisorder(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	forgerPub, forgerKey, _ := ed25519.GenerateKey(nil)
	resolve, err := m.groupAuthorResolver(ctx, ms, "g1")
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	ingest := func(entries ...store.SyncGroupLogEntry) (int, error) {
		m.journalMu.Lock()
		defer m.journalMu.Unlock()
		return m.ingestJournalEntriesLocked(ctx, ms, "g1", RosterSubchain, resolve, entries)
	}

	// FORGERY: a member_remove claiming controller authorship but signed by a forger.
	forged := mustEntry(t, "g1", RosterSubchain, 0, "", "member_remove", "chain-ctl", forgerPub, forgerKey, nil)
	if _, err := ingest(forged); err == nil {
		t.Fatalf("forged controller entry must be rejected")
	}

	// Ingest a legit genesis entry.
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	if n, err := ingest(e0); err != nil || n != 1 {
		t.Fatalf("legit e0 ingest: n=%d err=%v", n, err)
	}

	// FORK: a DIFFERENT controller-signed entry at the already-filled seq 0.
	e0fork := mustEntry(t, "g1", RosterSubchain, 0, "", "member_invite", "chain-ctl", ctlPub, ctlKey, map[string]string{"member": "x"})
	if _, err := ingest(e0fork); err == nil || !strings.Contains(err.Error(), "FORK") {
		t.Fatalf("fork must be detected, got: %v", err)
	}

	// DISORDER: a seq-1 entry whose prev_hash does not link to the local head.
	badPrev := mustEntry(t, "g1", RosterSubchain, 1, "deadbeef", "member_invite", "chain-ctl", ctlPub, ctlKey, map[string]string{"member": "y"})
	if _, err := ingest(badPrev); err == nil {
		t.Fatalf("prev_hash that does not link must be rejected")
	}

	// Idempotent: re-ingesting the exact e0 is a no-op success.
	if n, err := ingest(e0); err != nil || n != 0 {
		t.Fatalf("idempotent re-ingest: n=%d err=%v", n, err)
	}
}

// TestGroupAuthorResolver confirms per-entry author authority.
func TestGroupAuthorResolver(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, _ := seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, _, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-owner", Role: store.GroupRoleSelectiveSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: hex.EncodeToString(ownerPub),
	}); err != nil {
		t.Fatalf("member: %v", err)
	}
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "eurorack", OwnerChainID: "chain-owner"}); err != nil {
		t.Fatalf("domain: %v", err)
	}
	resolve, err := m.groupAuthorResolver(ctx, ms, "g1")
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}
	eq := func(a, b ed25519.PublicKey) bool { return string(a) == string(b) }

	// Controller-typed roster entry authored by the controller -> controller key.
	if k := resolve(store.SyncGroupLogEntry{Subchain: RosterSubchain, EntryType: "member_remove", AuthorChainID: "chain-ctl"}); !eq(k, ctlPub) {
		t.Fatalf("controller entry should resolve to controller key")
	}
	// Controller-typed roster entry authored by someone else -> nil (reject).
	if k := resolve(store.SyncGroupLogEntry{Subchain: RosterSubchain, EntryType: "member_remove", AuthorChainID: "chain-owner"}); k != nil {
		t.Fatalf("non-controller author of a controller type must resolve to nil")
	}
	// member_leave -> the leaving member's own key.
	if k := resolve(store.SyncGroupLogEntry{Subchain: RosterSubchain, EntryType: "member_leave", AuthorChainID: "chain-owner"}); !eq(k, ownerPub) {
		t.Fatalf("member_leave should resolve to the member's key")
	}
	// Domain entry authored by the owner -> owner key.
	if k := resolve(store.SyncGroupLogEntry{Subchain: DomainSubchain("eurorack"), EntryType: "domain_add", AuthorChainID: "chain-owner"}); !eq(k, ownerPub) {
		t.Fatalf("domain_add by owner should resolve to owner key")
	}
	// Domain entry authored by a NON-owner -> nil.
	if k := resolve(store.SyncGroupLogEntry{Subchain: DomainSubchain("eurorack"), EntryType: "domain_add", AuthorChainID: "chain-ctl"}); k != nil {
		t.Fatalf("domain_add by non-owner must resolve to nil")
	}
	// Wrong entry_type on a domain sub-chain -> nil.
	if k := resolve(store.SyncGroupLogEntry{Subchain: DomainSubchain("eurorack"), EntryType: "member_remove", AuthorChainID: "chain-owner"}); k != nil {
		t.Fatalf("mismatched type/subchain must resolve to nil")
	}
}

// TestPullGroupJournalRejectsReplaySpin locks the #8 fix: a hostile peer that
// re-serves already-held entries with a fabricated forward cursor must be
// rejected (contiguity) with a BOUNDED number of calls — never an infinite spin
// re-verifying signatures while holding journalMu.
func TestPullGroupJournalRejectsReplaySpin(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-peer", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive,
	}); err != nil {
		t.Fatalf("member: %v", err)
	}
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey, map[string]string{"member": "chain-peer"})

	calls := 0
	// ALWAYS returns [e0,e1] regardless of after_seq, with a cursor claiming
	// forward progress — the replay/spin attack.
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		calls++
		if calls > 20 {
			t.Fatalf("PullGroupJournal is spinning (%d calls) — loop control trusts the peer cursor", calls)
		}
		return &SyncJournalResponse{
			Entries:    []JournalEntryWire{storeToWire(e0), storeToWire(e1)},
			NextCursor: req.AfterSeq + 1, // fabricated forward progress
			RosterHead: e1.EntryHash,
		}, nil
	}

	// First pull ingests the two genuinely-new entries and STOPS (short page).
	if n, err := m.PullGroupJournal(ctx, "chain-peer", "g1", RosterSubchain); err != nil || n != 2 {
		t.Fatalf("first pull: n=%d err=%v", n, err)
	}
	// Second pull: our head is now seq 1; the peer replays seq 0..1, which is at/
	// below our head -> contiguity rejects it immediately, no spin.
	if _, err := m.PullGroupJournal(ctx, "chain-peer", "g1", RosterSubchain); err == nil {
		t.Fatalf("replayed page must be rejected (non-contiguous)")
	}
	if calls > 3 {
		t.Fatalf("too many peer calls (%d) — replay was not rejected promptly", calls)
	}
}

// TestHandleSyncJournalAuthzNonOracle drives the real handler and asserts the
// metadata-isolation gate returns a BYTE-IDENTICAL 403 for every deny reason
// (non-member, absent group, non-active member) so it is not an existence oracle.
func TestHandleSyncJournalAuthzNonOracle(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-a", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive,
	}); err != nil {
		t.Fatalf("member: %v", err)
	}
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-inv", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberInvited,
	}); err != nil {
		t.Fatalf("invited member: %v", err)
	}
	if _, err := m.AppendGroupJournalEntry(ctx, "g1", RosterSubchain, "group_create", "chain-ctl", ctlPub, ctlKey, nil); err != nil {
		t.Fatalf("seed entry: %v", err)
	}

	// Authorized active member -> 200 with entries.
	rr, resp := journalAs(t, m, "chain-a", SyncJournalRequest{GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
	if rr.Code != http.StatusOK || resp == nil || len(resp.Entries) != 1 {
		t.Fatalf("authorized pull: code=%d resp=%+v", rr.Code, resp)
	}

	// Every deny reason -> identical 403 body (no oracle).
	rrNonMember, _ := journalAs(t, m, "chain-nobody", SyncJournalRequest{GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
	rrAbsentGroup, _ := journalAs(t, m, "chain-a", SyncJournalRequest{GroupID: "g-absent", Subchain: RosterSubchain, AfterSeq: -1})
	rrNonActive, _ := journalAs(t, m, "chain-inv", SyncJournalRequest{GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
	for name, d := range map[string]*httptest.ResponseRecorder{"non-member": rrNonMember, "absent-group": rrAbsentGroup, "non-active": rrNonActive} {
		if d.Code != http.StatusForbidden {
			t.Fatalf("%s: code=%d want 403", name, d.Code)
		}
	}
	if rrNonMember.Body.String() != rrAbsentGroup.Body.String() || rrNonMember.Body.String() != rrNonActive.Body.String() {
		t.Fatalf("403 bodies differ -> existence oracle:\n non-member=%q\n absent=%q\n non-active=%q",
			rrNonMember.Body.String(), rrAbsentGroup.Body.String(), rrNonActive.Body.String())
	}

	// Malformed request -> 400.
	if rrBad, _ := journalAs(t, m, "chain-a", SyncJournalRequest{GroupID: "", Subchain: ""}); rrBad.Code != http.StatusBadRequest {
		t.Fatalf("missing fields: code=%d want 400", rrBad.Code)
	}
}

// TestJournalAuthzServesRemovedDomain locks the activeOnly=false fix: a removed
// domain's sub-chain is still servable to a prior sharer (so they converge on the
// removal), but still denied to a non-sharer.
func TestJournalAuthzServesRemovedDomain(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	add := func(chain, role string) {
		if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{GroupID: "g1", MemberChainID: chain, Role: role, MemberState: store.GroupMemberActive}); err != nil {
			t.Fatalf("member %s: %v", chain, err)
		}
	}
	add("chain-full", store.GroupRoleFullSync)
	add("chain-sel", store.GroupRoleSelectiveSync)
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
		GroupID: "g1", DomainTag: "eurorack", OwnerChainID: "chain-owner", AddedRevision: 2, RemovedRevision: 7, // removed
	}); err != nil {
		t.Fatalf("domain: %v", err)
	}
	if ok, _ := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-full", DomainSubchain("eurorack")); !ok {
		t.Fatalf("full-sync member must still be served a REMOVED domain's sub-chain (to learn of the removal)")
	}
	if ok, _ := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-sel", DomainSubchain("eurorack")); ok {
		t.Fatalf("selective non-owner must still be denied a removed domain")
	}
}
