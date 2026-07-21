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
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, map[string]string{pkDisplayName: "Studio Sync"})
	stale := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, manifestPayload(3, "hash3"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, stale); err != nil || n != 2 {
		t.Fatalf("stale manifest ingest: n=%d err=%v", n, err)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g.RosterRevision != 7 || g.DisplayName != "Studio Sync" {
		t.Fatalf("genesis name must persist while stale manifest is skipped: %+v", g)
	}
	fwdPayload := manifestPayload(9, "hash9")
	// An optional display name rides the established manifest entry so older
	// peers validate the same entry while new peers fold the presentation label.
	fwdPayload[pkDisplayName] = "Studio Sync"
	fwd := mustEntry(t, "g1", RosterSubchain, 2, stale.EntryHash, "manifest", "chain-ctl", ctlPub, ctlKey, fwdPayload)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, fwd); err != nil || n != 1 {
		t.Fatalf("forward manifest ingest: n=%d err=%v", n, err)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g.RosterRevision != 9 || g.ManifestHash != "hash9" || g.DisplayName != "Studio Sync" {
		t.Fatalf("forward manifest must advance revision+hash+name, got rev=%d hash=%q name=%q", g.RosterRevision, g.ManifestHash, g.DisplayName)
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
		epochRotatePayload("e2", "chain-ctl2", hex.EncodeToString(ctl2Pub), "{}"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0); err != nil || n != 1 {
		t.Fatalf("seed roster entry: n=%d err=%v", n, err)
	}
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e1); err == nil || n != 0 {
		t.Fatalf("unsigned incoming-controller rotation admitted: n=%d err=%v", n, err)
	}
	outsiderPub, outsiderKey, _ := ed25519.GenerateKey(nil)
	outsider := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e-outsider", "chain-outsider", hex.EncodeToString(outsiderPub), "{}"))
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

func domainActive(domains []store.SyncGroupDomain, tag string) bool {
	for _, d := range domains {
		if d.DomainTag == tag {
			return true
		}
	}
	return false
}

func domainClearance(domains []store.SyncGroupDomain, tag string) int {
	for _, d := range domains {
		if d.DomainTag == tag {
			return d.MaxClearance
		}
	}
	return -1
}

// genesisCreatePayload builds a group_create payload that seeds the genesis
// controller into controllerByEpoch (epoch-0), so a later stale-epoch forgery test
// exercises the H-1 carried-set gate rather than being rejected merely because the
// old controller key was never retained.
func genesisCreatePayload(controllerChain string, controllerPub ed25519.PublicKey) map[string]string {
	return map[string]string{
		pkControllerChain:  controllerChain,
		pkControllerPubkey: hex.EncodeToString(controllerPub),
		pkEpoch:            "",
	}
}

// TestDomainAddStaleEpochForgeryRevokedByRotation is the H-1 property: after control
// rotates away from ctl1, ctl1's RETAINED epoch-0 key can no longer admit a
// brand-new domain — even though ctl1 remains cryptographically able to produce a
// valid epoch-0 countersignature and the owner (chain-a) is still an active member.
// A fresh domain co-signed by the CURRENT controller is still accepted.
func TestDomainAddStaleEpochForgeryRevokedByRotation(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctl1Pub, ctl1Key := seedGroup(t, ms, "g1", "chain-ctl1") // genesis controller, epoch-0
	ctl2Pub, ctl2Key, _ := ed25519.GenerateKey(nil)
	aPub, aKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-ctl2", store.GroupRoleFullSync, ctl2Pub)
	seedActiveMember(t, ms, "g1", "chain-a", store.GroupRoleFullSync, aPub)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl1", ctl1Pub, ctl1Key,
		genesisCreatePayload("chain-ctl1", ctl1Pub))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0); err != nil || n != 1 {
		t.Fatalf("group_create: n=%d err=%v", n, err)
	}

	// chain-a establishes a/notes at the CURRENT epoch (epoch-0), ctl1 co-signs.
	notesSub := DomainSubchain("a/notes")
	notes := mustEntry(t, "g1", notesSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/notes", "chain-a", 0))
	attachControllerSignature(&notes, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	if n, err := ingestRoster(t, m, ms, "g1", notesSub, notes); err != nil || n != 1 {
		t.Fatalf("establish a/notes at current epoch: n=%d err=%v", n, err)
	}

	// Rotate control ctl1 -> ctl2 (epoch e2), re-attesting the active set {a/notes}
	// at its current head seq 0.
	carried, _ := encodeCarriedDomains(map[string]int64{"a/notes": 0})
	rot := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e2", "chain-ctl2", hex.EncodeToString(ctl2Pub), carried))
	attachControllerSignature(&rot, "e2", "chain-ctl2", ctl2Pub, ctl2Key)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, rot); err != nil || n != 1 {
		t.Fatalf("epoch_rotate: n=%d err=%v", n, err)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g == nil || g.Epoch != "e2" {
		t.Fatalf("controller not rotated to e2: %+v", g)
	}

	// FORGERY 1: rotated-out ctl1 self-co-signs a BRAND-NEW domain a/exfil with its
	// retained epoch-0 key. Must be REJECTED: old epoch + tag not in the carried set.
	exfilSub := DomainSubchain("a/exfil")
	exfil := mustEntry(t, "g1", exfilSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/exfil", "chain-a", 0))
	attachControllerSignature(&exfil, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	if n, err := ingestRoster(t, m, ms, "g1", exfilSub, exfil); err == nil || n != 0 {
		t.Fatalf("stale-epoch forgery of a NEW domain admitted (n=%d err=%v) — rotation failed to revoke ctl1", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); domainActive(active, "a/exfil") {
		t.Fatalf("forged domain a/exfil entered the shared set")
	}

	// FORGERY 2 (the re-widen/re-add gap): rotated-out ctl1 self-co-signs a NEW
	// higher-seq domain_add on the CARRIED a/notes sub-chain (seq 1 > attested head 0)
	// with its retained epoch-0 key, raising max_clearance 0 -> 4. Must be REJECTED:
	// the carried head anchor only covers back-fill at/below seq 0, not a fresh
	// post-rotation re-widen the current controller never approved.
	rewiden := mustEntry(t, "g1", notesSub, 1, notes.EntryHash, "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/notes", "chain-a", 4))
	attachControllerSignature(&rewiden, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	if n, err := ingestRoster(t, m, ms, "g1", notesSub, rewiden); err == nil || n != 0 {
		t.Fatalf("stale-epoch re-widen of a CARRIED domain admitted (n=%d err=%v) — H-1 fix incomplete", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); domainClearance(active, "a/notes") != 0 {
		t.Fatalf("a/notes clearance was re-widened by a stale-epoch entry")
	}

	// The SAME re-widen co-signed by the CURRENT controller (ctl2 @ e2) is accepted.
	rewidenOK := mustEntry(t, "g1", notesSub, 1, notes.EntryHash, "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/notes", "chain-a", 4))
	attachControllerSignature(&rewidenOK, "e2", "chain-ctl2", ctl2Pub, ctl2Key)
	if n, err := ingestRoster(t, m, ms, "g1", notesSub, rewidenOK); err != nil || n != 1 {
		t.Fatalf("re-widen co-signed by the current controller rejected: n=%d err=%v", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); domainClearance(active, "a/notes") != 4 {
		t.Fatalf("current-epoch re-widen did not apply")
	}

	// A fresh domain co-signed by the CURRENT controller (ctl2 @ e2) is still admitted.
	newSub := DomainSubchain("a/new")
	fresh := mustEntry(t, "g1", newSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/new", "chain-a", 0))
	attachControllerSignature(&fresh, "e2", "chain-ctl2", ctl2Pub, ctl2Key)
	if n, err := ingestRoster(t, m, ms, "g1", newSub, fresh); err != nil || n != 1 {
		t.Fatalf("fresh domain co-signed by the current controller rejected: n=%d err=%v", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); !domainActive(active, "a/new") {
		t.Fatalf("current-epoch domain a/new was not established")
	}
}

// TestDomainAddCarriedBackfillsToLaggingMember is the H-1 CORRECTNESS guard: the fix
// must NOT break legitimate cross-epoch propagation. A member that only ever sees
// the roster (group_create + a rotation re-attesting {a/notes}) and then back-fills
// the a/notes sub-chain — whose establishing entry legitimately bears the OLD epoch —
// must still accept it, while a non-carried old-epoch domain is rejected.
func TestDomainAddCarriedBackfillsToLaggingMember(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctl1Pub, ctl1Key := seedGroup(t, ms, "g1", "chain-ctl1")
	ctl2Pub, ctl2Key, _ := ed25519.GenerateKey(nil)
	aPub, aKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-ctl2", store.GroupRoleFullSync, ctl2Pub)
	seedActiveMember(t, ms, "g1", "chain-a", store.GroupRoleFullSync, aPub)

	// Roster only: genesis, then a rotation to ctl2 that re-attests {a/notes} — the
	// lagging member has NOT yet pulled the a/notes domain sub-chain.
	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl1", ctl1Pub, ctl1Key,
		genesisCreatePayload("chain-ctl1", ctl1Pub))
	// Re-attest a/notes at head seq 0 (the establishing entry the lagging member is
	// about to back-fill).
	carried, _ := encodeCarriedDomains(map[string]int64{"a/notes": 0})
	rot := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e2", "chain-ctl2", hex.EncodeToString(ctl2Pub), carried))
	attachControllerSignature(&rot, "e2", "chain-ctl2", ctl2Pub, ctl2Key)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, rot); err != nil || n != 2 {
		t.Fatalf("seed roster (genesis + rotation): n=%d err=%v", n, err)
	}

	// Back-fill a/notes: establishing entry bears the OLD epoch (epoch-0) but the tag
	// was re-attested by the current controller, so it MUST be accepted.
	notesSub := DomainSubchain("a/notes")
	notes := mustEntry(t, "g1", notesSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/notes", "chain-a", 0))
	attachControllerSignature(&notes, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	if n, err := ingestRoster(t, m, ms, "g1", notesSub, notes); err != nil || n != 1 {
		t.Fatalf("carried domain back-fill rejected (H-1 fix broke legitimate propagation): n=%d err=%v", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); !domainActive(active, "a/notes") {
		t.Fatalf("carried domain a/notes did not back-fill")
	}

	// A NON-carried old-epoch domain on the same lagging member is still rejected.
	evilSub := DomainSubchain("a/evil")
	evil := mustEntry(t, "g1", evilSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/evil", "chain-a", 0))
	attachControllerSignature(&evil, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	if n, err := ingestRoster(t, m, ms, "g1", evilSub, evil); err == nil || n != 0 {
		t.Fatalf("non-carried stale-epoch domain admitted on lagging member: n=%d err=%v", n, err)
	}
}

// TestDomainAddCarriedBackfillAllowsHistoricalReadd guards the correctness edge the
// head-seq boundary exists to preserve: a lagging member back-filling a carried
// domain whose pre-rotation history includes a re-add (establish@0, remove@1,
// re-add@2, all OLD epoch) must accept ALL of it, because every entry is at/below
// the re-attested head seq 2 — even though seq 2 is a "known"/re-add entry.
func TestDomainAddCarriedBackfillAllowsHistoricalReadd(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctl1Pub, ctl1Key := seedGroup(t, ms, "g1", "chain-ctl1")
	ctl2Pub, ctl2Key, _ := ed25519.GenerateKey(nil)
	aPub, aKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-ctl2", store.GroupRoleFullSync, ctl2Pub)
	seedActiveMember(t, ms, "g1", "chain-a", store.GroupRoleFullSync, aPub)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl1", ctl1Pub, ctl1Key, genesisCreatePayload("chain-ctl1", ctl1Pub))
	carried, _ := encodeCarriedDomains(map[string]int64{"a/notes": 2}) // head at seq 2 (after a re-add)
	rot := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "epoch_rotate", "chain-ctl1", ctl1Pub, ctl1Key,
		epochRotatePayload("e2", "chain-ctl2", hex.EncodeToString(ctl2Pub), carried))
	attachControllerSignature(&rot, "e2", "chain-ctl2", ctl2Pub, ctl2Key)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, rot); err != nil || n != 2 {
		t.Fatalf("seed roster: n=%d err=%v", n, err)
	}

	notesSub := DomainSubchain("a/notes")
	d0 := mustEntry(t, "g1", notesSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/notes", "chain-a", 0))
	attachControllerSignature(&d0, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	d1 := mustEntry(t, "g1", notesSub, 1, d0.EntryHash, "domain_remove", "chain-a", aPub, aKey, domainRemovePayload("a/notes"))
	d2 := mustEntry(t, "g1", notesSub, 2, d1.EntryHash, "domain_add", "chain-a", aPub, aKey, domainAddPayload("a/notes", "chain-a", 0))
	attachControllerSignature(&d2, effectiveControllerEpoch(""), "chain-ctl1", ctl1Pub, ctl1Key)
	if n, err := ingestRoster(t, m, ms, "g1", notesSub, d0, d1, d2); err != nil || n != 3 {
		t.Fatalf("historical re-add back-fill (at/below attested head) rejected — fix over-blocks legit back-fill: n=%d err=%v", n, err)
	}
	if active, _ := ms.ListSyncGroupDomains(ctx, "g1", true); !domainActive(active, "a/notes") {
		t.Fatalf("a/notes should be active after back-filling establish+remove+re-add")
	}
}

// TestEmitEpochRotateReattestsCarriedHeadsAndDoesNotFalseAbort drives a real
// rotation ceremony end-to-end (EmitEpochRotate -> admitEpochRotate) with an active
// shared domain, exercising the admit-time carried-head re-derivation. With no
// concurrent domain change the re-check must NOT abort, and the resulting attested
// head must box a later stale-epoch re-widen (seq above head) out.
func TestEmitEpochRotateReattestsCarriedHeadsAndDoesNotFalseAbort(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	attachBadger(t, m)
	if err := ms.UpsertSyncGroup(ctx, store.SyncGroup{GroupID: "g1", ControllerChainID: m.localChainID, ControllerAgentPubkey: hex.EncodeToString(m.agentPub)}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	// The local node is the controller AND an active member (it rotates the epoch,
	// co-signing as the incoming controller itself — no network round-trip needed).
	seedActiveMember(t, ms, "g1", m.localChainID, store.GroupRoleFullSync, m.agentPub)
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-owner", store.GroupRoleFullSync, ownerPub)

	xaSub := DomainSubchain("x/a")
	xa := mustEntry(t, "g1", xaSub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("x/a", "chain-owner", 0))
	attachControllerSignature(&xa, effectiveControllerEpoch(""), m.localChainID, m.agentPub, m.agentKey)
	if n, err := ingestRoster(t, m, ms, "g1", xaSub, xa); err != nil || n != 1 {
		t.Fatalf("establish x/a: n=%d err=%v", n, err)
	}

	entry, err := m.EmitEpochRotate(ctx, "g1", "e2", m.localChainID, hex.EncodeToString(m.agentPub))
	if err != nil {
		t.Fatalf("self epoch rotation with a carried domain failed (false stale-abort?): %v", err)
	}
	if entry.EntryType != "epoch_rotate" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if g, _ := ms.GetSyncGroup(ctx, "g1"); g == nil || g.Epoch != "e2" {
		t.Fatalf("epoch not rotated: %+v", g)
	}
	// The carried head (x/a -> 0) was re-attested: a stale epoch-0 re-widen at seq 1
	// (above head 0) is rejected, while nothing below the head was disturbed.
	rewiden := mustEntry(t, "g1", xaSub, 1, xa.EntryHash, "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("x/a", "chain-owner", 4))
	attachControllerSignature(&rewiden, effectiveControllerEpoch(""), m.localChainID, m.agentPub, m.agentKey)
	if n, err := ingestRoster(t, m, ms, "g1", xaSub, rewiden); err == nil || n != 0 {
		t.Fatalf("stale-epoch re-widen above the attested head admitted after rotation: n=%d err=%v", n, err)
	}
}

// TestMemberActivateCannotResurrectRemovedMember: a controller-authored
// member_activate must NOT flip a removed/left member back to Active reusing its
// stale role/consent — re-entry requires a fresh, invitee-co-signed member_invite.
func TestMemberActivateCannotResurrectRemovedMember(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	bPub, bKey, _ := ed25519.GenerateKey(nil)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	e1 := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-b", bPub, bKey, store.GroupRoleFullSync, "pinB"))
	e2 := mustEntry(t, "g1", RosterSubchain, 2, e1.EntryHash, "member_activate", "chain-ctl", ctlPub, ctlKey,
		memberChainPayload("chain-b"))
	e3 := mustEntry(t, "g1", RosterSubchain, 3, e2.EntryHash, "member_leave", "chain-b", bPub, bKey,
		memberChainPayload("chain-b"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0, e1, e2, e3); err != nil || n != 4 {
		t.Fatalf("invite+activate+leave: n=%d err=%v", n, err)
	}
	if mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b"); mem == nil || mem.MemberState != store.GroupMemberLeft {
		t.Fatalf("B should be left before the resurrection attempt: %+v", mem)
	}

	// A bare controller-authored member_activate(B) is a LOGGED no-op — B stays left.
	e4 := mustEntry(t, "g1", RosterSubchain, 4, e3.EntryHash, "member_activate", "chain-ctl", ctlPub, ctlKey,
		memberChainPayload("chain-b"))
	if _, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e4); err != nil {
		t.Fatalf("resurrect attempt should be a logged no-op, not an error: %v", err)
	}
	if mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b"); mem == nil || mem.MemberState != store.GroupMemberLeft {
		t.Fatalf("member_activate resurrected a left member (state=%v) — lifecycle guard missing", mem.MemberState)
	}
}

// TestMemberRemoveRetiresConsentRows: removing a member retires its active
// receive-consent rows so no stale entitlement can keep serving to it (and a later
// re-enrollment cannot silently inherit pre-removal consent).
func TestMemberRemoveRetiresConsentRows(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	aPub, aKey, _ := ed25519.GenerateKey(nil)
	bPub, _, _ := ed25519.GenerateKey(nil)
	seedActiveMember(t, ms, "g1", "chain-a", store.GroupRoleFullSync, aPub)
	seedActiveMember(t, ms, "g1", "chain-b", store.GroupRoleSelectiveSync, bPub)

	e0 := mustEntry(t, "g1", RosterSubchain, 0, "", "group_create", "chain-ctl", ctlPub, ctlKey, nil)
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, e0); err != nil || n != 1 {
		t.Fatalf("group_create: n=%d err=%v", n, err)
	}
	// Establish a shared domain x/a (owned by A, co-signed by the current controller)
	// so B's consent to it becomes an ACTIVE entitlement row.
	xaSub := DomainSubchain("x/a")
	xa := mustEntry(t, "g1", xaSub, 0, "", "domain_add", "chain-a", aPub, aKey, domainAddPayload("x/a", "chain-a", 0))
	attachControllerSignature(&xa, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if n, err := ingestRoster(t, m, ms, "g1", xaSub, xa); err != nil || n != 1 {
		t.Fatalf("establish x/a: n=%d err=%v", n, err)
	}
	if err := ms.ReplaceGroupMemberConsentDomains(ctx, "g1", "chain-b", []string{"x/a"}, 1); err != nil {
		t.Fatalf("seed B consent: %v", err)
	}
	if got, _ := ms.ListGroupMemberConsentDomains(ctx, "g1", "chain-b"); len(got) != 1 {
		t.Fatalf("B should hold an active consent row for x/a before removal: %+v", got)
	}

	rm := mustEntry(t, "g1", RosterSubchain, 1, e0.EntryHash, "member_remove", "chain-ctl", ctlPub, ctlKey,
		memberChainPayload("chain-b"))
	if n, err := ingestRoster(t, m, ms, "g1", RosterSubchain, rm); err != nil || n != 1 {
		t.Fatalf("member_remove: n=%d err=%v", n, err)
	}
	if mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b"); mem == nil || mem.MemberState != store.GroupMemberRemoved {
		t.Fatalf("B should be removed: %+v", mem)
	}
	if got, _ := ms.ListGroupMemberConsentDomains(ctx, "g1", "chain-b"); len(got) != 0 {
		t.Fatalf("member_remove did not retire B's active consent rows: %+v", got)
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
