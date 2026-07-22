package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// blockingResponseWriter pauses the handler at the final body write. It lets
// concurrency tests prove that the sync-policy lease covers response emission,
// not merely the authorization queries that preceded it.
type blockingResponseWriter struct {
	header  http.Header
	status  int
	body    bytes.Buffer
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingResponseWriter() *blockingResponseWriter {
	return &blockingResponseWriter{
		header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{}),
	}
}

func (w *blockingResponseWriter) Header() http.Header { return w.header }

func (w *blockingResponseWriter) WriteHeader(status int) { w.status = status }

func (w *blockingResponseWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return w.body.Write(p)
}

// journalAs drives handleSyncJournal with the exact roster key. Older fixtures
// that predate member-key enforcement omitted the key; fill only that empty test
// field so they model the signed invitation invariant production always has.
func journalAs(t *testing.T, m *Manager, peerChain string, req SyncJournalRequest) (*httptest.ResponseRecorder, *SyncJournalResponse) {
	t.Helper()
	agentID := ""
	if ss := m.syncStore(); ss != nil && req.GroupID != "" {
		member, err := ss.GetSyncGroupMember(context.Background(), req.GroupID, peerChain)
		if err == nil && member != nil {
			if member.MemberAgentPubkey == "" {
				pub, _, keyErr := ed25519.GenerateKey(nil)
				if keyErr != nil {
					t.Fatal(keyErr)
				}
				member.MemberAgentPubkey = hex.EncodeToString(pub)
				if err := ss.UpsertSyncGroupMember(context.Background(), *member); err != nil {
					t.Fatal(err)
				}
			}
			agentID = member.MemberAgentPubkey
		}
	}
	if agentID == "" {
		pub, _, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		agentID = hex.EncodeToString(pub)
	}
	return journalAsAgent(t, m, peerChain, agentID, req)
}

func journalAsAgent(t *testing.T, m *Manager, peerChain, peerAgent string, req SyncJournalRequest) (*httptest.ResponseRecorder, *SyncJournalResponse) {
	t.Helper()
	if req.Version == 0 {
		req.Version = JournalWireVersion
	}
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/journal", bytes.NewReader(body))
	peer := &peerIdentity{ChainID: peerChain, AgentID: peerAgent, Agreement: &store.CrossFedRecord{
		RemoteChainID: peerChain, AllowedDomains: []string{"*"}, Status: "active",
	}}
	if ss := m.syncStore(); ss != nil {
		control, err := ss.GetSyncControl(context.Background(), peerChain)
		if err != nil {
			t.Fatal(err)
		}
		if control == nil {
			bindInboundGroupPeer(t, m, ss, peer, "guest")
		} else {
			peer.Agreement.PeerPubKey, _ = hex.DecodeString(control.RemoteCAPin)
		}
	}
	httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peer))
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

func TestMemberRemovalTerminalEntryReturnsNewestLifecycleRemoval(t *testing.T) {
	ctx := context.Background()
	_, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	for seq, item := range []struct {
		entryType string
		chain     string
	}{
		{entryType: "member_remove", chain: "chain-guest"},
		{entryType: "member_invite", chain: "chain-guest"},
		{entryType: "member_activate", chain: "chain-guest"},
		{entryType: "member_remove", chain: "chain-other"},
		{entryType: "member_remove", chain: "chain-guest"},
	} {
		payload, err := json.Marshal(memberChainPayload(item.chain))
		require.NoError(t, err)
		require.NoError(t, ms.AppendSyncGroupLog(ctx, store.SyncGroupLogEntry{
			GroupID: "g-rejoin", Subchain: RosterSubchain, Seq: int64(seq),
			EntryHash: fmt.Sprintf("hash-%d", seq), EntryType: item.entryType,
			PayloadJSON: string(payload),
		}))
	}

	entry, err := memberRemovalTerminalEntry(ctx, ms, "g-rejoin", "chain-guest")
	require.NoError(t, err)
	require.NotNil(t, entry)
	assert.EqualValues(t, 4, entry.Seq,
		"a re-invited guest must receive its newest removal, not the historical removal behind its cursor")
}

// bindPullJournalPeer gives PullGroupJournal the same exact, currently-active
// trust edge production reconciliation requires. A fetched page is not authority
// by itself: its serving chain must still be the JOIN-frozen operator and an
// active exact-key group member when the local append transaction begins.
func bindPullJournalPeer(t *testing.T, m *Manager, ms *store.SQLiteStore, groupID, remoteChain string, remotePub ed25519.PublicKey) {
	t.Helper()
	if m.badger == nil {
		attachBadger(t, m)
	}
	peer := &peerIdentity{
		ChainID: remoteChain,
		AgentID: hex.EncodeToString(remotePub),
		Agreement: &store.CrossFedRecord{
			RemoteChainID:  remoteChain,
			AllowedDomains: []string{"*"},
			Status:         "active",
		},
	}
	bindInboundGroupPeer(t, m, ms, peer, "guest")

	upsertExactActive := func(chain, agentID string) {
		member, err := ms.GetSyncGroupMember(context.Background(), groupID, chain)
		if err != nil {
			t.Fatalf("read pull member %s: %v", chain, err)
		}
		if member == nil {
			member = &store.SyncGroupMember{GroupID: groupID, MemberChainID: chain, Role: store.GroupRoleFullSync}
		}
		if member.Role == "" {
			member.Role = store.GroupRoleFullSync
		}
		member.MemberState = store.GroupMemberActive
		member.MemberAgentPubkey = agentID
		if err := ms.UpsertSyncGroupMember(context.Background(), *member); err != nil {
			t.Fatalf("seed pull member %s: %v", chain, err)
		}
	}
	upsertExactActive(remoteChain, peer.AgentID)
	upsertExactActive(m.localChainID, hex.EncodeToString(m.agentPub))
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
	check("chain-nonmember", RosterSubchain, false)              // not a member
	check("chain-invited", RosterSubchain, false)                // not active
	check("chain-full", DomainSubchain("eurorack"), true)        // full-sync sees all shared domains
	check("chain-sel", DomainSubchain("eurorack"), false)        // selective non-owner: safe-default deny (no leak)
	check("chain-owner", DomainSubchain("eurorack"), true)       // domain owner
	check("chain-full", DomainSubchain("does-not-exist"), false) // not a group domain
	check("chain-full", "bogus-subchain", false)                 // malformed subchain
}

// TestPullGroupJournalIngest exercises the end-to-end pull -> verify -> ingest ->
// convergence path with an injected peer serving a controller-signed chain.
func TestPullGroupJournalIngest(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	peerPub, _, _ := ed25519.GenerateKey(nil)
	bindPullJournalPeer(t, m, ms, "g1", "chain-peer", peerPub)
	memPub, memKey, _ := ed25519.GenerateKey(nil)
	// The serving peer is already an exact active member. The pulled journal may
	// expand the roster, but it cannot bootstrap its own transport authority.

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-new", memPub, memKey, store.GroupRoleFullSync, "pinP"))
	peerChain := []store.SyncGroupLogEntry{e0, e1}
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		resp := &SyncJournalResponse{Version: JournalWireVersion, NextCursor: req.AfterSeq, RosterHead: e1.EntryHash}
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
	// APPLY created chain-new as an invited member with its signed pubkey.
	mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-new")
	if mem == nil || mem.MemberState != store.GroupMemberInvited || mem.MemberAgentPubkey != hex.EncodeToString(memPub) {
		t.Fatalf("member_invite not applied: %+v", mem)
	}
	// Convergence tracking recorded the peer's head without disturbing last_acked.
	peer, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-peer")
	if peer == nil || peer.LastSeenJournalHead != e1.EntryHash || peer.LastAckedRosterRevision != 0 {
		t.Fatalf("convergence not tracked cleanly: %+v", peer)
	}
}

func TestJournalEntryWirePreservesControllerApproval(t *testing.T) {
	ctlPub, ctlKey, _ := ed25519.GenerateKey(nil)
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	e := mustEntry(t, "g1", DomainSubchain("hr"), 0, "", "domain_add", "chain-owner", ownerPub, ownerKey,
		domainAddPayload("hr", "chain-owner", 0))
	attachControllerSignature(&e, "epoch-7", "chain-ctl", ctlPub, ctlKey)

	got := wireToStore("g1", storeToWire(e))
	if got.ControllerEpoch != e.ControllerEpoch || got.ControllerChainID != e.ControllerChainID ||
		got.ControllerAgentPubkey != e.ControllerAgentPubkey || got.ControllerSig != e.ControllerSig {
		t.Fatalf("controller approval lost in wire round trip: got=%+v want=%+v", got, e)
	}
}

func TestJournalWireV2RejectsLegacyRequestsAndDoesNotOmitApprovalFields(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-peer", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive,
	}); err != nil {
		t.Fatalf("member: %v", err)
	}
	for _, version := range []int{0, 1} {
		body, _ := json.Marshal(SyncJournalRequest{Version: version, GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
		req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/journal", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, &peerIdentity{ChainID: "chain-peer"}))
		rr := httptest.NewRecorder()
		m.handleSyncJournal(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("legacy v%d request status=%d want 400", version, rr.Code)
		}
	}
	if rr, resp := journalAs(t, m, "chain-peer", SyncJournalRequest{Version: JournalWireVersion, GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1}); rr.Code != http.StatusOK || resp == nil || resp.Version != JournalWireVersion {
		t.Fatalf("v%d request/response = code %d resp %+v; want 200/current", JournalWireVersion, rr.Code, resp)
	}
	encoded, err := json.Marshal(storeToWire(store.SyncGroupLogEntry{}))
	if err != nil || !strings.Contains(string(encoded), `"controller_sig":""`) || !strings.Contains(string(encoded), `"controller_epoch":""`) {
		t.Fatalf("v2 wire must carry empty controller-proof fields rather than omitting them: json=%s err=%v", encoded, err)
	}
	_ = ctx
}

func TestPullGroupJournalDomainAddRequiresAndPreservesControllerApproval(t *testing.T) {
	ctx := context.Background()
	ctlPub, ctlKey, _ := ed25519.GenerateKey(nil)
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	entry := mustEntry(t, "g1", DomainSubchain("hr"), 0, "", "domain_add", "chain-owner", ownerPub, ownerKey,
		domainAddPayload("hr", "chain-owner", 0))
	attachControllerSignature(&entry, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)

	newReceiver := func(t *testing.T) (*Manager, *store.SQLiteStore) {
		m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
		if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{
			GroupID: "g1", ControllerChainID: "chain-ctl", ControllerAgentPubkey: hex.EncodeToString(ctlPub),
		}); err != nil {
			t.Fatalf("group: %v", err)
		}
		if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
			GroupID: "g1", MemberChainID: "chain-owner", MemberAgentPubkey: hex.EncodeToString(ownerPub),
			Role: store.GroupRoleSelectiveSync, MemberState: store.GroupMemberActive,
		}); err != nil {
			t.Fatalf("owner: %v", err)
		}
		bindPullJournalPeer(t, m, ms, "g1", "chain-owner", ownerPub)
		return m, ms
	}

	m, ms := newReceiver(t)
	m.syncJournalFn = func(context.Context, string, *SyncJournalRequest) (*SyncJournalResponse, error) {
		return &SyncJournalResponse{Version: JournalWireVersion, Entries: []JournalEntryWire{storeToWire(entry)}, NextCursor: 0}, nil
	}
	if n, err := m.PullGroupJournal(ctx, "chain-owner", "g1", DomainSubchain("hr")); err != nil || n != 1 {
		t.Fatalf("approved remote domain_add: n=%d err=%v", n, err)
	}
	if domains, err := ms.ListSyncGroupDomains(ctx, "g1", true); err != nil || len(domains) != 1 || domains[0].DomainTag != "hr" {
		t.Fatalf("approved domain_add did not apply: domains=%+v err=%v", domains, err)
	}

	ownerOnly := entry
	ownerOnly.ControllerEpoch = ""
	ownerOnly.ControllerChainID = ""
	ownerOnly.ControllerAgentPubkey = ""
	ownerOnly.ControllerSig = ""
	m2, _ := newReceiver(t)
	m2.syncJournalFn = func(context.Context, string, *SyncJournalRequest) (*SyncJournalResponse, error) {
		return &SyncJournalResponse{Version: JournalWireVersion, Entries: []JournalEntryWire{storeToWire(ownerOnly)}, NextCursor: 0}, nil
	}
	if n, err := m2.PullGroupJournal(ctx, "chain-owner", "g1", DomainSubchain("hr")); err == nil || n != 0 {
		t.Fatalf("owner-only remote domain_add must fail closed: n=%d err=%v", n, err)
	}
}

func TestPullGroupJournalRejectsLegacyResponseVersionBeforeIngest(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	e := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	m.syncJournalFn = func(context.Context, string, *SyncJournalRequest) (*SyncJournalResponse, error) {
		return &SyncJournalResponse{Version: 1, Entries: []JournalEntryWire{storeToWire(e)}, NextCursor: 0}, nil
	}
	if n, err := m.PullGroupJournal(ctx, "chain-peer", "g1", RosterSubchain); err == nil || n != 0 || !strings.Contains(err.Error(), "response version") {
		t.Fatalf("legacy response must reject before ingest: n=%d err=%v", n, err)
	}
	if head, _ := ms.GetSyncGroupSubchainHead(ctx, "g1", RosterSubchain); head != nil {
		t.Fatalf("legacy response mutated local journal: %+v", head)
	}
}

// TestJournalIngestRejectsForgeryForkAndDisorder covers the three ingest guards.
func TestJournalIngestRejectsForgeryForkAndDisorder(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	forgerPub, forgerKey, _ := ed25519.GenerateKey(nil)
	ingest := func(entries ...store.SyncGroupLogEntry) (int, error) {
		m.journalMu.Lock()
		defer m.journalMu.Unlock()
		n, _, _, err := m.ingestJournalEntriesLocked(ctx, ms, "g1", RosterSubchain, entries)
		return n, err
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
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
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
	// Domain entry authored by the owner AND exactly controller-approved -> owner key.
	ownerAdd := mustEntry(t, "g1", DomainSubchain("eurorack"), 0, "", "domain_add", "chain-owner", ownerPub, ownerKey,
		domainAddPayload("eurorack", "chain-owner", 0))
	attachControllerSignature(&ownerAdd, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if k := resolve(ownerAdd); !eq(k, ownerPub) {
		t.Fatalf("domain_add by owner should resolve to owner key")
	}
	// Domain entry authored by a NON-owner -> nil.
	nonOwnerAdd := mustEntry(t, "g1", DomainSubchain("eurorack"), 0, "", "domain_add", "chain-ctl", ctlPub, ctlKey,
		domainAddPayload("eurorack", "chain-ctl", 0))
	attachControllerSignature(&nonOwnerAdd, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if k := resolve(nonOwnerAdd); k != nil {
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
	peerPub, _, _ := ed25519.GenerateKey(nil)
	bindPullJournalPeer(t, m, ms, "g1", "chain-peer", peerPub)
	memPub, memKey, _ := ed25519.GenerateKey(nil)
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-new", memPub, memKey, store.GroupRoleFullSync, "pinP"))

	calls := 0
	// ALWAYS returns [e0,e1] regardless of after_seq, with a cursor claiming
	// forward progress — the replay/spin attack.
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		calls++
		if calls > 20 {
			t.Fatalf("PullGroupJournal is spinning (%d calls) — loop control trusts the peer cursor", calls)
		}
		return &SyncJournalResponse{Version: JournalWireVersion,
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
	wrongPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	rrWrongKey, _ := journalAsAgent(t, m, "chain-a", hex.EncodeToString(wrongPub), SyncJournalRequest{
		GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1,
	})
	if rrWrongKey.Code != http.StatusForbidden || rrWrongKey.Body.String() != rrNonMember.Body.String() {
		t.Fatalf("same-chain wrong operator leaked group journal: code=%d body=%q", rrWrongKey.Code, rrWrongKey.Body.String())
	}

	// Malformed request -> 400.
	if rrBad, _ := journalAs(t, m, "chain-a", SyncJournalRequest{GroupID: "", Subchain: ""}); rrBad.Code != http.StatusBadRequest {
		t.Fatalf("missing fields: code=%d want 400", rrBad.Code)
	}
}

// TestJournalAuthzRemovedDomainOwnerOnly locks the removed-domain leak fix: a
// REMOVED domain's sub-chain is served ONLY to its owner (so they converge on the
// removal); a full-sync member who never shared it must NOT learn its name (§5.2).
func TestJournalAuthzRemovedDomainOwnerOnly(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	add := func(chain, role string) {
		if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{GroupID: "g1", MemberChainID: chain, Role: role, MemberState: store.GroupMemberActive}); err != nil {
			t.Fatalf("member %s: %v", chain, err)
		}
	}
	add("chain-full", store.GroupRoleFullSync)
	add("chain-owner", store.GroupRoleSelectiveSync)

	// ACTIVE domain: full-sync member and owner both served.
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "live", OwnerChainID: "chain-owner", AddedRevision: 2}); err != nil {
		t.Fatalf("active domain: %v", err)
	}
	if ok, _ := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-full", DomainSubchain("live")); !ok {
		t.Fatalf("full-sync member must be served an ACTIVE shared domain")
	}

	// REMOVED domain: owner served, full-sync member (never shared it) DENIED.
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "gone", OwnerChainID: "chain-owner", AddedRevision: 2, RemovedRevision: 7}); err != nil {
		t.Fatalf("removed domain: %v", err)
	}
	if ok, _ := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-full", DomainSubchain("gone")); ok {
		t.Fatalf("full-sync member must NOT be served a removed domain it never shared (§5.2 leak)")
	}
	if ok, _ := m.authorizeJournalSubchain(ctx, ms, "g1", "chain-owner", DomainSubchain("gone")); !ok {
		t.Fatalf("the owner must still be served its removed domain (to converge on the removal)")
	}
}

func TestRemovedDomainPriorEntitledMemberGetsOnlyTerminalSuffix(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	_, _ = seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	for _, member := range []store.SyncGroupMember{
		{GroupID: "g1", MemberChainID: "chain-owner", MemberAgentPubkey: hex.EncodeToString(ownerPub), Role: store.GroupRoleSelectiveSync, MemberState: store.GroupMemberActive},
		{GroupID: "g1", MemberChainID: "chain-prior", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive},
		{GroupID: "g1", MemberChainID: "chain-late", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberInvited},
	} {
		if err := ms.UpsertSyncGroupMember(ctx, member); err != nil {
			t.Fatalf("member: %v", err)
		}
	}
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "hr", OwnerChainID: "chain-owner", AddedRevision: 3}); err != nil {
		t.Fatalf("domain: %v", err)
	}
	if err := ms.SnapshotRemovedDomainEntitlements(ctx, "g1", "hr", 2); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// chain-late was invited, not active, at removal, then activates afterwards.
	if err := ms.SetSyncGroupMemberState(ctx, "g1", "chain-late", store.GroupMemberActive, 0); err != nil {
		t.Fatalf("activate late: %v", err)
	}
	if err := ms.SetSyncGroupDomainRemoved(ctx, "g1", "hr", 2); err != nil {
		t.Fatalf("remove domain: %v", err)
	}
	d1 := mustEntry(t, "g1", DomainSubchain("hr"), 1, "old-hash", "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	if err := ms.AppendSyncGroupLog(ctx, d1); err != nil {
		t.Fatalf("append removal: %v", err)
	}

	rr, resp := journalAs(t, m, "chain-prior", SyncJournalRequest{GroupID: "g1", Subchain: DomainSubchain("hr"), AfterSeq: -1})
	if rr.Code != http.StatusOK || resp == nil || !resp.TerminalOnly || len(resp.Entries) != 1 || resp.Entries[0].EntryType != "domain_remove" {
		t.Fatalf("prior member terminal serve: code=%d resp=%+v", rr.Code, resp)
	}
	if rr, _ := journalAs(t, m, "chain-late", SyncJournalRequest{GroupID: "g1", Subchain: DomainSubchain("hr"), AfterSeq: -1}); rr.Code != http.StatusForbidden {
		t.Fatalf("post-removal member status=%d want 403", rr.Code)
	}
}

func TestRemovedMemberGetsOnlyItsTerminalRosterRemoval(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	removedPub, _, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-removed", MemberAgentPubkey: hex.EncodeToString(removedPub),
		Role: store.GroupRoleEnrolledNoSync, MemberState: store.GroupMemberRemoved, JoinedRevision: 2, LeftRevision: 5,
	}); err != nil {
		t.Fatalf("removed member: %v", err)
	}
	// The old roster body exists locally but must not escape with the removal.
	prior := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	removal := mustEntry(t, "g1", RosterSubchain, 1, prior.EntryHash, "member_remove", "chain-ctl", ctlPub, ctlKey,
		map[string]string{pkMemberChain: "chain-removed"})
	for _, entry := range []store.SyncGroupLogEntry{prior, removal} {
		if err := ms.AppendSyncGroupLog(ctx, entry); err != nil {
			t.Fatalf("append %s: %v", entry.EntryType, err)
		}
	}
	if _, err := ms.BeginSyncGroupDissolve(ctx, "g1", "chain-ctl"); err != nil {
		t.Fatalf("begin retired-owner fixture: %v", err)
	}
	if err := ms.CompleteSyncGroupDissolve(ctx, "g1", "chain-ctl"); err != nil {
		t.Fatalf("retire owner fixture: %v", err)
	}
	if group, err := ms.GetSyncGroup(ctx, "g1"); err != nil || group != nil {
		t.Fatalf("owner group must be retired before terminal serve: group=%+v err=%v", group, err)
	}

	rr, resp := journalAs(t, m, "chain-removed", SyncJournalRequest{GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
	if rr.Code != http.StatusOK || resp == nil || !resp.TerminalOnly || len(resp.Entries) != 1 || resp.Entries[0].EntryType != "member_remove" || resp.Entries[0].Seq != 1 {
		t.Fatalf("removed member terminal roster serve: code=%d resp=%+v", rr.Code, resp)
	}
}

func TestPullTerminalRemovalSuffixConvergesWithoutOldDomainJournal(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	_, _ = seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	for _, member := range []store.SyncGroupMember{
		{GroupID: "g1", MemberChainID: "chain-owner", MemberAgentPubkey: hex.EncodeToString(ownerPub), Role: store.GroupRoleSelectiveSync, MemberState: store.GroupMemberActive},
		{GroupID: "g1", MemberChainID: "chain-local", Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive},
	} {
		if err := ms.UpsertSyncGroupMember(ctx, member); err != nil {
			t.Fatalf("member: %v", err)
		}
	}
	bindPullJournalPeer(t, m, ms, "g1", "chain-owner", ownerPub)
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "hr", OwnerChainID: "chain-owner", AddedRevision: 3}); err != nil {
		t.Fatalf("domain: %v", err)
	}
	if err := ms.SnapshotRemovedDomainEntitlements(ctx, "g1", "hr", 2); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := ms.SetSyncGroupDomainRemoved(ctx, "g1", "hr", 2); err != nil {
		t.Fatalf("remove domain: %v", err)
	}
	// The predecessor (seq 0) deliberately is NOT present locally: a terminal
	// v2 response must still let this prior sharer ingest the signed removal.
	d1 := mustEntry(t, "g1", DomainSubchain("hr"), 1, "hidden-old-head", "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		if req.Version != JournalWireVersion {
			t.Fatalf("pull version=%d want %d", req.Version, JournalWireVersion)
		}
		return &SyncJournalResponse{Version: JournalWireVersion, Entries: []JournalEntryWire{storeToWire(d1)}, NextCursor: 1, TerminalOnly: true}, nil
	}
	if n, err := m.PullGroupJournal(ctx, "chain-owner", "g1", DomainSubchain("hr")); err != nil || n != 1 {
		t.Fatalf("pull terminal suffix: n=%d err=%v", n, err)
	}
	entries, err := ms.ListSyncGroupLog(ctx, "g1", DomainSubchain("hr"), -1, 10)
	if err != nil || len(entries) != 1 || entries[0].Seq != 1 || entries[0].EntryType != "domain_remove" {
		t.Fatalf("sparse terminal entry did not converge safely: entries=%+v err=%v", entries, err)
	}
	if got, _ := ms.GetSyncGroupSubchainHead(ctx, "g1", DomainSubchain("hr")); got == nil || got.EntryHash != d1.EntryHash {
		t.Fatalf("terminal suffix did not advance local head: %+v", got)
	}
}

func TestInvitedGuestAutomaticallyPullsTerminalRemovalAndHidesGroup(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ownerPub, ownerKey := seedGroup(t, ms, "g1", "chain-owner")
	bindPullJournalPeer(t, m, ms, "g1", "chain-owner", ownerPub)
	if err := ms.SetSyncGroupMemberState(ctx, "g1", m.localChainID, store.GroupMemberInvited, 0); err != nil {
		t.Fatalf("leave guest in invited crash state: %v", err)
	}

	// The predecessor is deliberately absent locally. A removed guest still has
	// to ingest the signed sparse removal and stop rendering the group.
	removal := mustEntry(t, "g1", RosterSubchain, 1, "hidden-old-head", "member_remove", "chain-owner", ownerPub, ownerKey,
		map[string]string{pkMemberChain: m.localChainID})
	m.syncJournalFn = func(_ context.Context, _ string, req *SyncJournalRequest) (*SyncJournalResponse, error) {
		if req.Subchain != RosterSubchain || req.Version != JournalWireVersion {
			t.Fatalf("terminal roster pull request=%+v", req)
		}
		return &SyncJournalResponse{Version: JournalWireVersion, Entries: []JournalEntryWire{storeToWire(removal)}, NextCursor: 1, TerminalOnly: true}, nil
	}
	m.reconcilePeerJournals(ctx, ms, "chain-owner")
	local, err := ms.GetSyncGroupMember(ctx, "g1", m.localChainID)
	if err != nil || local == nil || local.MemberState != store.GroupMemberRemoved {
		t.Fatalf("local guest removal did not converge: member=%+v err=%v", local, err)
	}
}

func TestPullGroupJournalDelayedResponseCannotCommitAfterRevoke(t *testing.T) {
	for _, tc := range []struct {
		name         string
		terminalOnly bool
	}{
		{name: "normal-page"},
		{name: "terminal-sparse-page", terminalOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
			peerPub, peerKey, err := ed25519.GenerateKey(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{
				GroupID: "g-race", ControllerChainID: "chain-peer", ControllerAgentPubkey: hex.EncodeToString(peerPub),
			}); err != nil {
				t.Fatal(err)
			}
			bindPullJournalPeer(t, m, ms, "g-race", "chain-peer", peerPub)

			subchain := RosterSubchain
			entry := mustEntry(t, "g-race", subchain, 0, "", "group_create", "chain-peer", peerPub, peerKey, nil)
			if tc.terminalOnly {
				subchain = DomainSubchain("hr")
				if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
					GroupID: "g-race", DomainTag: "hr", OwnerChainID: "chain-peer", AddedRevision: 1,
				}); err != nil {
					t.Fatal(err)
				}
				entry = mustEntry(t, "g-race", subchain, 1, "withheld-predecessor", "domain_remove", "chain-peer", peerPub, peerKey, domainRemovePayload("hr"))
			}

			entered := make(chan struct{})
			release := make(chan struct{})
			var enteredOnce sync.Once
			m.syncJournalFn = func(context.Context, string, *SyncJournalRequest) (*SyncJournalResponse, error) {
				enteredOnce.Do(func() { close(entered) })
				<-release
				return &SyncJournalResponse{
					Version: JournalWireVersion, Entries: []JournalEntryWire{storeToWire(entry)},
					NextCursor: entry.Seq, TerminalOnly: tc.terminalOnly,
				}, nil
			}

			type pullResult struct {
				n   int
				err error
			}
			pulled := make(chan pullResult, 1)
			go func() {
				n, pullErr := m.PullGroupJournal(ctx, "chain-peer", "g-race", subchain)
				pulled <- pullResult{n: n, err: pullErr}
			}()
			<-entered

			// Complete revocation while the peer still withholds its response. This
			// also proves the network fetch owns neither journalMu nor policy WRITE.
			revoked := make(chan error, 1)
			go func() {
				if revokeErr := m.badger.UpdateCrossFedStatus("chain-peer", "revoked"); revokeErr != nil {
					revoked <- revokeErr
					return
				}
				revoked <- ms.PurgeSyncPeerState(ctx, "chain-peer")
			}()
			select {
			case revokeErr := <-revoked:
				if revokeErr != nil {
					close(release)
					t.Fatalf("complete revoke: %v", revokeErr)
				}
			case <-time.After(2 * time.Second):
				close(release)
				t.Fatal("revoke blocked behind the lock-free journal fetch")
			}
			close(release)

			result := <-pulled
			if result.n != 0 || result.err == nil || !strings.Contains(result.err.Error(), "no longer an active frozen member") {
				t.Fatalf("post-revoke delayed pull = n=%d err=%v", result.n, result.err)
			}
			entries, listErr := ms.ListSyncGroupLog(ctx, "g-race", subchain, -1, 10)
			if listErr != nil || len(entries) != 0 {
				t.Fatalf("delayed response committed after revoke: entries=%+v err=%v", entries, listErr)
			}
		})
	}
}

func TestRemovedDomainGenerationPreventsStaleLifecycleServe(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	oldPub, _, _ := ed25519.GenerateKey(nil)
	newPub, _, _ := ed25519.GenerateKey(nil)
	for _, member := range []store.SyncGroupMember{
		{GroupID: "g1", MemberChainID: "chain-owner", MemberAgentPubkey: hex.EncodeToString(ownerPub), Role: store.GroupRoleSelectiveSync, MemberState: store.GroupMemberActive},
		{GroupID: "g1", MemberChainID: "chain-old", MemberAgentPubkey: hex.EncodeToString(oldPub), Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive},
	} {
		if err := ms.UpsertSyncGroupMember(ctx, member); err != nil {
			t.Fatalf("member: %v", err)
		}
	}
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "hr", OwnerChainID: "chain-owner"}); err != nil {
		t.Fatalf("initial domain: %v", err)
	}
	sub := DomainSubchain("hr")
	d0 := mustEntry(t, "g1", sub, 0, "", "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	if n, err := ingestRoster(t, m, ms, "g1", sub, d0); err != nil || n != 1 {
		t.Fatalf("first removal: n=%d err=%v", n, err)
	}
	if ok, _ := ms.WasMemberEntitledAtDomainRemoval(ctx, "g1", "hr", "chain-old", 1); !ok {
		t.Fatal("old full-sync member missing first lifecycle entitlement")
	}
	// This member remains in the group but no longer selects hr before the
	// re-add. Its old snapshot must never authorize the later removal.
	if err := ms.SetSyncGroupMemberRole(ctx, "g1", "chain-old", store.GroupRoleSelectiveSync); err != nil {
		t.Fatalf("narrow old member: %v", err)
	}
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{GroupID: "g1", MemberChainID: "chain-new", MemberAgentPubkey: hex.EncodeToString(newPub), Role: store.GroupRoleFullSync, MemberState: store.GroupMemberActive}); err != nil {
		t.Fatalf("new member: %v", err)
	}
	d1 := mustEntry(t, "g1", sub, 1, d0.EntryHash, "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("hr", "chain-owner", 0))
	attachControllerSignature(&d1, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if n, err := ingestRoster(t, m, ms, "g1", sub, d1); err != nil || n != 1 {
		t.Fatalf("re-add: n=%d err=%v", n, err)
	}
	d2 := mustEntry(t, "g1", sub, 2, d1.EntryHash, "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	if n, err := ingestRoster(t, m, ms, "g1", sub, d2); err != nil || n != 1 {
		t.Fatalf("second removal: n=%d err=%v", n, err)
	}
	if ok, _ := ms.WasMemberEntitledAtDomainRemoval(ctx, "g1", "hr", "chain-old", 3); ok {
		t.Fatal("stale first-lifecycle entitlement authorized the second removal")
	}
	if ok, _ := ms.WasMemberEntitledAtDomainRemoval(ctx, "g1", "hr", "chain-new", 3); !ok {
		t.Fatal("new full-sync member missing second lifecycle entitlement")
	}
	// A subsequent owner-signed remove is auditable but semantically a no-op: it
	// must not overwrite generation 3 or produce another entitlement snapshot.
	d3 := mustEntry(t, "g1", sub, 3, d2.EntryHash, "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	if n, err := ingestRoster(t, m, ms, "g1", sub, d3); err != nil || n != 1 {
		t.Fatalf("no-op removal should log but skip projection: n=%d err=%v", n, err)
	}
	domains, err := ms.ListSyncGroupDomains(ctx, "g1", false)
	if err != nil || len(domains) != 1 || domains[0].RemovedRevision != 3 {
		t.Fatalf("no-op removal overwrote current generation: domains=%+v err=%v", domains, err)
	}
	if rr, _ := journalAs(t, m, "chain-old", SyncJournalRequest{GroupID: "g1", Subchain: sub, AfterSeq: -1}); rr.Code != http.StatusForbidden {
		t.Fatalf("stale prior member status=%d want 403", rr.Code)
	}
	rr, resp := journalAs(t, m, "chain-new", SyncJournalRequest{GroupID: "g1", Subchain: sub, AfterSeq: -1})
	if rr.Code != http.StatusOK || resp == nil || !resp.TerminalOnly || len(resp.Entries) != 2 || resp.Entries[0].Seq != 2 || resp.Entries[1].Seq != 3 {
		t.Fatalf("second lifecycle terminal suffix leaked/omitted entries: code=%d resp=%+v", rr.Code, resp)
	}
}

func TestSyncJournalServeLeaseLinearizesMemberRemoval(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	peerPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	peerID := hex.EncodeToString(peerPub)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-peer", Role: store.GroupRoleFullSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: peerID,
	}); err != nil {
		t.Fatalf("member: %v", err)
	}

	body, _ := json.Marshal(SyncJournalRequest{Version: JournalWireVersion, GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/journal", bytes.NewReader(body))
	peer := &peerIdentity{ChainID: "chain-peer", AgentID: peerID, Agreement: &store.CrossFedRecord{RemoteChainID: "chain-peer", Status: "active"}}
	bindInboundGroupPeer(t, m, ms, peer, "guest")
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, peer))
	bw := newBlockingResponseWriter()
	served := make(chan struct{})
	go func() { m.handleSyncJournal(bw, req); close(served) }()
	<-bw.entered

	removalStarted := make(chan struct{})
	removalDone := make(chan struct{})
	go func() {
		close(removalStarted)
		unlock := ms.LockSyncPolicyWrite()
		_ = ms.SetSyncGroupMemberState(ctx, "g1", "chain-peer", store.GroupMemberRemoved, 1)
		unlock()
		close(removalDone)
	}()
	<-removalStarted
	select {
	case <-removalDone:
		t.Fatal("member removal returned while a stale-policy journal response was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(bw.release)
	<-served
	<-removalDone
	if bw.status != http.StatusOK {
		t.Fatalf("in-flight authorized response status=%d", bw.status)
	}

	rr, _ := journalAs(t, m, "chain-peer", SyncJournalRequest{GroupID: "g1", Subchain: RosterSubchain, AfterSeq: -1})
	if rr.Code != http.StatusForbidden {
		t.Fatalf("post-removal journal serve status=%d want 403", rr.Code)
	}
}
