package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
)

func ingestRoster(t *testing.T, m *Manager, ms *store.SQLiteStore, groupID, subchain string, entries ...store.SyncGroupLogEntry) (int, error) {
	t.Helper()
	m.journalMu.Lock()
	defer m.journalMu.Unlock()
	n, _, _, err := m.ingestJournalEntriesLocked(context.Background(), ms, groupID, subchain, entries)
	return n, err
}

// TestJournalApplyGrowingResolver is the headline step-5 property: a single batch
// carrying member_invite(B) + member_activate(B) + a SELF-signed member_leave(B)
// applies end-to-end, because the resolver GROWS — B's key (from the invite just
// applied) is what verifies B's later self-signed leave. The static-snapshot
// resolver of step 4 could not do this.
func TestJournalApplyGrowingResolver(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	bPub, bKey, _ := ed25519.GenerateKey(nil)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-b", bPub, bKey, store.GroupRoleFullSync, "pinB"))
	e2 := mustEntry(t, "g1", RosterSubchain, 2, e1.EntryHash, "member_activate", "chain-ctl", ctlPub, ctlKey,
		memberChainPayload("chain-b"))
	// SELF-signed by B (author == chain-b, signed with bKey) — resolvable ONLY
	// because e1's invite was applied earlier in THIS same batch.
	e3 := mustEntry(t, "g1", RosterSubchain, 3, e2.EntryHash, "member_leave", "chain-b", bPub, bKey,
		memberChainPayload("chain-b"))

	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, e1, e2, e3); err != nil || n != 4 {
		t.Fatalf("in-batch invite+activate+leave: n=%d err=%v", n, err)
	}
	mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b")
	if mem == nil || mem.MemberState != store.GroupMemberLeft {
		t.Fatalf("member B should be 'left': %+v", mem)
	}
	// A member-scope advisory tombstone recorded the leave.
	if tombs, _ := ms.ListSyncTombstones(ctx, "g1", store.TombstoneScopeMember); len(tombs) != 1 {
		t.Fatalf("expected 1 member tombstone, got %d", len(tombs))
	}
}

// TestJournalApplyDomainLifecycle: a domain owner establishes and then removes a
// domain on its own per-domain sub-chain; a full-sync member sees the domain go
// active then removed.
func TestJournalApplyDomainLifecycle(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-owner", Role: store.GroupRoleSelectiveSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: hex.EncodeToString(ownerPub),
	}); err != nil {
		t.Fatalf("owner member: %v", err)
	}
	sub := DomainSubchain("eurorack")
	d0 := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("eurorack", "chain-owner", 0))
	attachControllerSignature(&d0, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if n, err := ingestRoster(t, m, ms, "g1", sub, d0); err != nil || n != 1 {
		t.Fatalf("domain_add: n=%d err=%v", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); len(active) != 1 || active[0].DomainTag != "eurorack" || active[0].OwnerChainID != "chain-owner" {
		t.Fatalf("domain not established: %+v", active)
	}
	d1 := mustEntry(t, "g1", sub, 1, d0.EntryHash, "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("eurorack"))
	if n, err := ingestRoster(t, m, ms, "g1", sub, d1); err != nil || n != 1 {
		t.Fatalf("domain_remove: n=%d err=%v", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); len(active) != 0 {
		t.Fatalf("domain should be removed from the active set: %+v", active)
	}
	if tombs, _ := ms.ListSyncTombstones(ctx, "g1", store.TombstoneScopeDomain); len(tombs) != 1 {
		t.Fatalf("expected 1 domain tombstone, got %d", len(tombs))
	}
	// A non-owner cannot author on the domain sub-chain.
	otherPub, otherKey, _ := ed25519.GenerateKey(nil)
	_ = otherPub
	bad := mustEntry(t, "g1", sub, 2, d1.EntryHash, "domain_remove", "chain-owner", otherPub, otherKey, domainRemovePayload("eurorack"))
	if _, err := ingestRoster(t, m, ms, "g1", sub, bad); err == nil {
		t.Fatalf("a domain entry signed by a non-owner key must be rejected")
	}
}

func TestDomainRemoveSnapshotsPriorFullSyncEntitlement(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g-snapshot", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	fullPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g-snapshot", "chain-owner", store.GroupRoleSelectiveSync, ownerPub)
	seedActiveMember(t, ms, "g-snapshot", "chain-full", store.GroupRoleFullSync, fullPub)
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
		GroupID: "g-snapshot", DomainTag: "hr", OwnerChainID: "chain-owner", AddedRevision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	remove := mustEntry(t, "g-snapshot", DomainSubchain("hr"), 0, "", "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("hr"))
	m.journalMu.Lock()
	_, _, _, err := m.ingestJournalEntriesLocked(ctx, ms, "g-snapshot", DomainSubchain("hr"), []store.SyncGroupLogEntry{remove})
	m.journalMu.Unlock()
	if err != nil {
		t.Fatalf("ingest removal: %v", err)
	}
	wasEntitled, err := ms.WasMemberEntitledAtDomainRemoval(ctx, "g-snapshot", "hr", "chain-full", 1)
	if err != nil || !wasEntitled {
		t.Fatalf("full-sync prior entitlement not snapshotted: entitled=%v err=%v", wasEntitled, err)
	}
}

// TestJournalApplyManifestAntiRollback: a manifest advances roster_revision, and a
// stale (lower) manifest is skipped without lowering it or wedging ingest.
func TestJournalApplyManifestAntiRollback(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	// Seed the group at revision 7 (floor auto-advances to 7).
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: "chain-ctl", ControllerAgentPubkey: hex.EncodeToString(ctlPub), RosterRevision: 7}); err != nil {
		t.Fatalf("seed rev: %v", err)
	}
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	stale := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, manifestPayload(3, "hash3"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, stale); err != nil || n != 2 {
		t.Fatalf("stale manifest ingest: n=%d err=%v", n, err)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g.RosterRevision != 7 {
		t.Fatalf("stale manifest must NOT lower revision, got %d", g.RosterRevision)
	}
	fwd := mustEntry(t, "g1", RosterSubchain, 2, stale.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, manifestPayload(9, "hash9"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, fwd); err != nil || n != 1 {
		t.Fatalf("forward manifest ingest: n=%d err=%v", n, err)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g.RosterRevision != 9 || g.ManifestHash != "hash9" {
		t.Fatalf("forward manifest must advance revision+hash, got rev=%d hash=%q", g.RosterRevision, g.ManifestHash)
	}
}

// TestJournalApplyEpochRotate: the outgoing controller rotates to a new controller
// key; the very next entry authored by the NEW controller verifies (the resolver
// grew across the rotation).
func TestJournalApplyEpochRotate(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctl1Pub, ctl1Key := seedGroup(t, ms, "g1", "chain-ctl1")
	ctl2Pub, ctl2Key, _ := ed25519.GenerateKey(nil)
	bPub, bKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-ctl2", store.GroupRoleFullSync, ctl2Pub)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl1", ctl1Pub, ctl1Key, nil)
	// epoch_rotate is authored by the OUTGOING controller (ctl1).
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e2", "chain-ctl2", hex.EncodeToString(ctl2Pub)))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0); err != nil || n != 1 {
		t.Fatalf("seed roster entry: n=%d err=%v", n, err)
	}
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e1); err == nil || n != 0 {
		t.Fatalf("unsigned incoming-controller rotation admitted: n=%d err=%v", n, err)
	}
	outsiderPub, outsiderKey, _ := ed25519.GenerateKey(nil)
	outsider := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e-outsider", "chain-outsider", hex.EncodeToString(outsiderPub)))
	attachControllerSignature(&outsider, "e-outsider", "chain-outsider", outsiderPub, outsiderKey)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, outsider); err == nil || n != 0 {
		t.Fatalf("non-member incoming controller admitted: n=%d err=%v", n, err)
	}
	attachControllerSignature(&e1, "e2", "chain-ctl2", ctl2Pub, ctl2Key)
	// member_invite authored by the NEW controller (ctl2) — only resolvable if the
	// rotation was applied to the growing state.
	invitePayload := signedMemberInvitePayload(t, "g1", "chain-b", bPub, bKey, store.GroupRoleFullSync, "pinB")
	invitePayload[pkInviteHead] = e1.EntryHash
	attachMemberInviteProof("g1", invitePayload, bKey)
	e2 := mustEntry(t, "g1", RosterSubchain, 2, e1.EntryHash, "member_invite", "chain-ctl2", ctl2Pub, ctl2Key, invitePayload)

	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e1, e2); err != nil || n != 2 {
		t.Fatalf("epoch rotate batch: n=%d err=%v", n, err)
	}
	g, _ := ms.GetSyncGroup(ctx, "g1")
	if g.ControllerChainID != "chain-ctl2" || g.ControllerAgentPubkey != hex.EncodeToString(ctl2Pub) || g.Epoch != "e2" {
		t.Fatalf("controller not rotated: %+v", g)
	}
	if mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b"); mem == nil {
		t.Fatalf("new controller's member_invite did not apply")
	}
}

func TestJournalApplyRepeatedInviteCannotReplacePinnedMemberKey(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	originalPub, _, _ := ed25519.GenerateKey(nil)
	attackerPub, attackerKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-victim", store.GroupRoleFullSync, originalPub)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-victim", attackerPub, attackerKey, store.GroupRoleFullSync, "new-pin"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0); err != nil || n != 1 {
		t.Fatalf("ingest repeated invite: n=%d err=%v", n, err)
	}
	member, err := ms.GetSyncGroupMember(ctx, "g1", "chain-victim")
	if err != nil || member == nil {
		t.Fatalf("load victim: %v", err)
	}
	if member.MemberAgentPubkey != hex.EncodeToString(originalPub) || member.MemberState != store.GroupMemberActive {
		t.Fatalf("repeated invite replaced pinned identity/state: %+v", member)
	}
}

func TestControllerRoleChangeCannotForgeMemberConsent(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	memberPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-member", store.GroupRoleFullSync, memberPub)
	if err := ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{GroupID: "g1", DomainTag: "secret", OwnerChainID: "chain-ctl"}); err != nil {
		t.Fatal(err)
	}
	selected, _ := encodeSelectedDomains([]string{"secret"})
	payload := roleChangePayload("chain-member", store.GroupRoleSelectiveSync)
	payload[pkSelectedDomains] = selected
	e := mustEntry(t, "g1", RosterSubchain, 0, "", "role_change", "chain-ctl", ctlPub, ctlKey, payload)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e); err != nil || n != 1 {
		t.Fatalf("controller role change: n=%d err=%v", n, err)
	}
	if consent, err := ms.ListGroupMemberConsentDomains(ctx, "g1", "chain-member"); err != nil || len(consent) != 0 {
		t.Fatalf("controller forged member receive consent: %v err=%v", consent, err)
	}
}

// TestJournalApplyEvictionRevokesAuthority: a removed member can no longer author
// (resolve consults member_state), and an active member still can.
func TestJournalApplyEvictionRevokesAuthority(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	evilPub, evilKey, _ := ed25519.GenerateKey(nil)
	goodPub, goodKey, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-evil", Role: store.GroupRoleSelectiveSync,
		MemberState: store.GroupMemberRemoved, MemberAgentPubkey: hex.EncodeToString(evilPub),
	}); err != nil {
		t.Fatalf("evil member: %v", err)
	}
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-good", Role: store.GroupRoleSelectiveSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: hex.EncodeToString(goodPub),
	}); err != nil {
		t.Fatalf("good member: %v", err)
	}
	// A REMOVED member cannot establish a domain (authority revoked).
	evil := mustEntry(t, "g1", DomainSubchain("secret"), 0, "", "domain_add", "chain-evil", evilPub, evilKey, domainAddPayload("secret", "chain-evil", 0))
	if _, err := ingestRoster(t, m, ms, "g1", DomainSubchain("secret"), evil); err == nil {
		t.Fatalf("a removed member must NOT be able to author a domain_add")
	}
	// An ACTIVE member still can.
	good := mustEntry(t, "g1", DomainSubchain("ok"), 0, "", "domain_add", "chain-good", goodPub, goodKey, domainAddPayload("ok", "chain-good", 0))
	attachControllerSignature(&good, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if n, err := ingestRoster(t, m, ms, "g1", DomainSubchain("ok"), good); err != nil || n != 1 {
		t.Fatalf("active member domain_add: n=%d err=%v", n, err)
	}
}

// TestJournalApplyMalformedSkipsNoWedge: a controller-signed entry with a bad
// payload is LOGGED and SKIPPED (no effect), and the chain still advances so a
// following valid entry applies — no wedge.
func TestJournalApplyMalformedSkipsNoWedge(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	// member_invite with a BAD member pubkey — authentically controller-signed.
	bad := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey,
		map[string]string{"member_chain": "chain-b", "member_pubkey": "not-a-real-key", "role": "full-sync"})
	fwd := mustEntry(t, "g1", RosterSubchain, 2, bad.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, manifestPayload(1, "h1"))

	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, bad, fwd); err != nil || n != 3 {
		t.Fatalf("malformed entry must be logged+skipped, not wedge: n=%d err=%v", n, err)
	}
	if mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b"); mem != nil {
		t.Fatalf("a malformed invite must not add a member: %+v", mem)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g.RosterRevision != 1 {
		t.Fatalf("chain wedged: the manifest after a skipped entry did not apply (rev=%d)", g.RosterRevision)
	}
	if head, _ := ms.GetSyncGroupSubchainHead(ctx, "g1", RosterSubchain); head == nil || head.Seq != 2 {
		t.Fatalf("all 3 entries must be logged: %+v", head)
	}
}

// TestJournalApplyManifestUpperBound: an absurd manifest revision is skipped
// (never poisons the monotonic floor); a later legit manifest still applies.
func TestJournalApplyManifestUpperBound(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	absurd := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, manifestPayload(int64(1)<<50, "hbad"))
	legit := mustEntry(t, "g1", RosterSubchain, 2, absurd.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, manifestPayload(1, "h1"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, absurd, legit); err != nil || n != 3 {
		t.Fatalf("ingest: n=%d err=%v", n, err)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g.RosterRevision != 1 || g.RosterRevisionFloor != 1 {
		t.Fatalf("absurd manifest poisoned the floor; legit manifest could not apply: rev=%d floor=%d", g.RosterRevision, g.RosterRevisionFloor)
	}
}
