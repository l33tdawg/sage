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
	return m.ingestJournalEntriesLocked(context.Background(), ms, groupID, subchain, entries)
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
		memberInvitePayload("chain-b", hex.EncodeToString(bPub), store.GroupRoleFullSync, "pinB"))
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
	seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	if err := ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-owner", Role: store.GroupRoleSelectiveSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: hex.EncodeToString(ownerPub),
	}); err != nil {
		t.Fatalf("owner member: %v", err)
	}
	sub := DomainSubchain("eurorack")
	d0 := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("eurorack", "chain-owner", 0))
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
	bPub, _, _ := ed25519.GenerateKey(nil)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl1", ctl1Pub, ctl1Key, nil)
	// epoch_rotate is authored by the OUTGOING controller (ctl1).
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e2", "chain-ctl2", hex.EncodeToString(ctl2Pub)))
	// member_invite authored by the NEW controller (ctl2) — only resolvable if the
	// rotation was applied to the growing state.
	e2 := mustEntry(t, "g1", RosterSubchain, 2, e1.EntryHash, "member_invite", "chain-ctl2", ctl2Pub, ctl2Key,
		memberInvitePayload("chain-b", hex.EncodeToString(bPub), store.GroupRoleFullSync, "pinB"))

	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, e1, e2); err != nil || n != 3 {
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
