package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSyncGroupRoundTrip exercises the v11.8 group overlay migration + CRUD:
// group head, membership, shared domains, the partitioned journal, tombstones,
// and the sync_control binding.
func TestSyncGroupRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// Absent before creation.
	if g, err := s.GetSyncGroup(ctx, "g1"); err != nil || g != nil {
		t.Fatalf("GetSyncGroup empty: got %+v err %v", g, err)
	}

	g := SyncGroup{
		GroupID:               "g1",
		ControllerChainID:     "chain-ctl",
		ControllerAgentPubkey: "ctlpub",
		Epoch:                 "e1",
		RosterRevision:        3,
		ManifestHash:          "mh",
		RosterJournalHead:     "jh",
		DisplayName:           "Studio",
	}
	if err := s.UpsertSyncGroup(ctx, g); err != nil {
		t.Fatalf("UpsertSyncGroup: %v", err)
	}
	got, err := s.GetSyncGroup(ctx, "g1")
	if err != nil || got == nil {
		t.Fatalf("GetSyncGroup: %+v err %v", got, err)
	}
	if got.RosterRevision != 3 || got.ControllerChainID != "chain-ctl" || got.QuorumMode != "advisory" {
		t.Fatalf("group fields: %+v", got)
	}
	// The anti-rollback floor auto-advances to the revision.
	if got.RosterRevisionFloor != 3 {
		t.Fatalf("floor not advanced: %d", got.RosterRevisionFloor)
	}

	// Floor must never regress even if a stale roster_revision is written.
	stale := g
	stale.RosterRevision = 1
	if err = s.UpsertSyncGroup(ctx, stale); err != nil {
		t.Fatalf("UpsertSyncGroup stale: %v", err)
	}
	got, _ = s.GetSyncGroup(ctx, "g1")
	if got.RosterRevisionFloor != 3 {
		t.Fatalf("floor regressed to %d", got.RosterRevisionFloor)
	}

	// Members: valid role, and the voting_power zero-pin.
	if err = s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-a", Role: GroupRoleFullSync, MemberState: GroupMemberActive,
		MemberAgentPubkey: "apub", CAPin: "pinA",
	}); err != nil {
		t.Fatalf("UpsertSyncGroupMember A: %v", err)
	}
	if err = s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-b", Role: GroupRoleSelectiveSync, MemberState: GroupMemberInvited,
	}); err != nil {
		t.Fatalf("UpsertSyncGroupMember B: %v", err)
	}
	if err = s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-x", Role: "controller", // invalid role
	}); err == nil {
		t.Fatalf("expected invalid role rejection")
	}
	if err = s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-x", Role: GroupRoleFullSync, VotingPower: 1.0,
	}); err == nil {
		t.Fatalf("expected voting_power zero-pin rejection")
	}
	members, err := s.ListSyncGroupMembers(ctx, "g1")
	if err != nil || len(members) != 2 {
		t.Fatalf("ListSyncGroupMembers: %d %v", len(members), err)
	}
	if members[0].MemberChainID != "chain-a" || members[0].Role != GroupRoleFullSync {
		t.Fatalf("member[0]: %+v", members[0])
	}

	// Shared domains.
	if err = s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{
		GroupID: "g1", DomainTag: "eurorack", OwnerChainID: "chain-a", OwnerSig: "sigA", MaxClearance: 0, AddedRevision: 2,
	}); err != nil {
		t.Fatalf("UpsertSyncGroupDomain: %v", err)
	}
	if err = s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{
		GroupID: "g1", DomainTag: "removed-topic", OwnerChainID: "chain-a", AddedRevision: 1, RemovedRevision: 4,
	}); err != nil {
		t.Fatalf("UpsertSyncGroupDomain removed: %v", err)
	}
	all, _ := s.ListSyncGroupDomains(ctx, "g1", false)
	active, _ := s.ListSyncGroupDomains(ctx, "g1", true)
	if len(all) != 2 || len(active) != 1 || active[0].DomainTag != "eurorack" {
		t.Fatalf("domains all=%d active=%d %+v", len(all), len(active), active)
	}

	// Partitioned journal: roster sub-chain + a per-domain sub-chain, with head tracking.
	if err = s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "roster", Seq: 0, EntryHash: "h0", EntryType: "group_create", AuthorChainID: "chain-ctl",
	}); err != nil {
		t.Fatalf("AppendSyncGroupLog roster0: %v", err)
	}
	if err = s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "roster", Seq: 1, PrevHash: "h0", EntryHash: "h1", EntryType: "member_activate",
	}); err != nil {
		t.Fatalf("AppendSyncGroupLog roster1: %v", err)
	}
	if err = s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "domain:eurorack", Seq: 0, EntryHash: "d0", EntryType: "domain_add", AuthorChainID: "chain-a",
	}); err != nil {
		t.Fatalf("AppendSyncGroupLog domain0: %v", err)
	}
	// Duplicate (group, subchain, seq) must fail the PK.
	if err = s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "roster", Seq: 1, EntryHash: "hX", EntryType: "manifest",
	}); err == nil {
		t.Fatalf("expected duplicate seq rejection")
	}
	head, err := s.GetSyncGroupSubchainHead(ctx, "g1", "roster")
	if err != nil || head == nil || head.Seq != 1 || head.EntryHash != "h1" {
		t.Fatalf("roster head: %+v err %v", head, err)
	}
	if emptyHead, _ := s.GetSyncGroupSubchainHead(ctx, "g1", "domain:none"); emptyHead != nil {
		t.Fatalf("expected nil head for empty subchain")
	}
	entries, err := s.ListSyncGroupLog(ctx, "g1", "roster", -1, 100)
	if err != nil || len(entries) != 2 || entries[0].EntryType != "group_create" {
		t.Fatalf("ListSyncGroupLog: %d %v", len(entries), err)
	}

	// Tombstones + anti-resurrection lookup.
	if err = s.InsertSyncTombstone(ctx, SyncTombstone{
		GroupID: "g1", Scope: TombstoneScopeMemory, Enforcement: TombstoneEnforceLocalSuppress,
		OriginChainID: "chain-a", OriginMemoryID: "mem-1", Reason: "local delete",
	}); err != nil {
		t.Fatalf("InsertSyncTombstone: %v", err)
	}
	// Idempotent re-insert.
	if err = s.InsertSyncTombstone(ctx, SyncTombstone{
		GroupID: "g1", Scope: TombstoneScopeMemory, Enforcement: TombstoneEnforceLocalSuppress,
		OriginChainID: "chain-a", OriginMemoryID: "mem-1",
	}); err != nil {
		t.Fatalf("InsertSyncTombstone idempotent: %v", err)
	}
	if err = s.InsertSyncTombstone(ctx, SyncTombstone{
		GroupID: "g1", Scope: "bogus", Enforcement: TombstoneEnforceAdvisory,
	}); err == nil {
		t.Fatalf("expected invalid scope rejection")
	}
	suppressed, err := s.IsLocallySuppressed(ctx, "g1", "chain-a", "mem-1")
	if err != nil || !suppressed {
		t.Fatalf("IsLocallySuppressed true: %v %v", suppressed, err)
	}
	// group_id="" matches any group.
	if got, _ := s.IsLocallySuppressed(ctx, "", "chain-a", "mem-1"); !got {
		t.Fatalf("IsLocallySuppressed any-group should match")
	}
	if got, _ := s.IsLocallySuppressed(ctx, "g1", "chain-a", "mem-2"); got {
		t.Fatalf("IsLocallySuppressed should not match mem-2")
	}
	tombs, _ := s.ListSyncTombstones(ctx, "g1", TombstoneScopeMemory)
	if len(tombs) != 1 {
		t.Fatalf("ListSyncTombstones: %d", len(tombs))
	}

	// sync_control binding (row must exist first).
	if err := s.PrepareSyncControl(ctx, SyncControl{
		RemoteChainID: "chain-a", Role: "host", ControllerChainID: "chain-ctl",
		ControllerAgentID: "ctl", PolicyEpoch: "e1", RemoteCAPin: "pinA",
	}); err != nil {
		t.Fatalf("PrepareSyncControl: %v", err)
	}
	if err := s.SetSyncControlGroupID(ctx, "chain-a", "g1"); err != nil {
		t.Fatalf("SetSyncControlGroupID: %v", err)
	}
	var boundGroup string
	if err := s.conn.QueryRowContext(ctx, `SELECT group_id FROM sync_control WHERE remote_chain_id=?`, "chain-a").Scan(&boundGroup); err != nil {
		t.Fatalf("read group_id: %v", err)
	}
	if boundGroup != "g1" {
		t.Fatalf("sync_control.group_id = %q, want g1", boundGroup)
	}

	if groups, err := s.ListSyncGroups(ctx); err != nil || len(groups) != 1 {
		t.Fatalf("ListSyncGroups: %d %v", len(groups), err)
	}
}

// TestSyncGroupMemberUpsertContract pins the review's finding: the full-row
// upsert requires an explicit state, and the targeted mutators never reset a
// member's identity/state (so a step-3/4 progress bump cannot demote an active
// member or wipe its trust pin).
func TestSyncGroupMemberUpsertContract(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	require := func(err error, msg string) {
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
	}
	require(s.UpsertSyncGroup(ctx, SyncGroup{GroupID: "g1", ControllerChainID: "c", ControllerAgentPubkey: "p"}), "group")

	// Full-row upsert requires an explicit, valid state.
	if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{GroupID: "g1", MemberChainID: "a", Role: GroupRoleFullSync}); err == nil {
		t.Fatalf("upsert without explicit state should error")
	}
	require(s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "a", Role: GroupRoleFullSync, MemberState: GroupMemberActive,
		MemberAgentPubkey: "apub", CAPin: "pinA", JoinedRevision: 5,
	}), "upsert active member")

	// A progress bump touches ONLY the cursors — state/pin/pubkey/joined survive.
	require(s.UpdateSyncGroupMemberProgress(ctx, "g1", "a", 9, "headZ", "2026-07-12T00:00:00Z"), "progress")
	got, err := s.GetSyncGroupMember(ctx, "g1", "a")
	require(err, "get after progress")
	if got.MemberState != GroupMemberActive || got.CAPin != "pinA" || got.MemberAgentPubkey != "apub" || got.JoinedRevision != 5 {
		t.Fatalf("progress bump mutated identity/state: %+v", got)
	}
	if got.LastAckedRosterRevision != 9 || got.LastSeenJournalHead != "headZ" {
		t.Fatalf("progress not applied: %+v", got)
	}

	// A state transition touches only state + left_revision.
	require(s.SetSyncGroupMemberState(ctx, "g1", "a", GroupMemberRemoved, 12), "set state")
	got, _ = s.GetSyncGroupMember(ctx, "g1", "a")
	if got.MemberState != GroupMemberRemoved || got.LeftRevision != 12 || got.CAPin != "pinA" {
		t.Fatalf("state transition wrong: %+v", got)
	}
	if err := s.SetSyncGroupMemberState(ctx, "g1", "a", "bogus", 0); err == nil {
		t.Fatalf("invalid state transition should error")
	}
}

// TestSyncTombstoneEnforcementCoexist verifies a local_suppress row is never
// swallowed by a pre-existing advisory row for the same target (enforcement is
// part of the PK), and IsLocallySuppressed still discriminates correctly.
func TestSyncTombstoneEnforcementCoexist(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	base := SyncTombstone{GroupID: "g1", Scope: TombstoneScopeMemory, OriginChainID: "cx", OriginMemoryID: "m1"}

	adv := base
	adv.Enforcement = TombstoneEnforceAdvisory
	if err := s.InsertSyncTombstone(ctx, adv); err != nil {
		t.Fatalf("insert advisory: %v", err)
	}
	// Before the local_suppress lands, it is NOT suppressed.
	if got, _ := s.IsLocallySuppressed(ctx, "g1", "cx", "m1"); got {
		t.Fatalf("advisory alone must not suppress")
	}
	sup := base
	sup.Enforcement = TombstoneEnforceLocalSuppress
	if err := s.InsertSyncTombstone(ctx, sup); err != nil {
		t.Fatalf("insert suppress (must not be swallowed): %v", err)
	}
	if got, _ := s.IsLocallySuppressed(ctx, "g1", "cx", "m1"); !got {
		t.Fatalf("local_suppress must suppress even with a prior advisory row")
	}
	if got, _ := s.IsLocallySuppressed(ctx, "g2", "cx", "m1"); got {
		t.Fatalf("suppression must not leak across groups")
	}
}

// TestVotingPowerNotInConsensusPaths is guard layer 3 (docs §6.3): the group's
// inert quorum-shaped state (sync_group_member.voting_power, min_quorum,
// quorum_mode, and the SyncGroupMember type) must never be referenced by any
// consensus / quorum code path, so it can never become a v11.9 acceptance input
// without a deliberate fork. Scans the consensus packages for the group-specific
// identifiers (PoE validator VotingPower is a DIFFERENT, legitimate thing and is
// not matched).
func TestVotingPowerNotInConsensusPaths(t *testing.T) {
	root := "../.." // internal/store -> repo root
	consensusDirs := []string{"internal/abci", "internal/consensus", "internal/validator"}
	forbidden := []string{"sync_group", "SyncGroupMember", "min_quorum", "quorum_mode"}
	for _, dir := range consensusDirs {
		full := filepath.Join(root, dir)
		if _, err := os.Stat(full); err != nil {
			continue // package may not exist under that exact name
		}
		err := filepath.WalkDir(full, func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			b, rErr := os.ReadFile(path)
			if rErr != nil {
				return rErr
			}
			src := string(b)
			for _, tok := range forbidden {
				if strings.Contains(src, tok) {
					t.Errorf("consensus path %s references group inert state %q — voting_power/group tables must never reach a quorum decision (docs §6.3)", path, tok)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", full, err)
		}
	}
}

// TestGroupStep6ReadModels exercises the build-step 6 group-aware fan-out getters:
// star targets, digest membership/intersection, relay authorization, the origin-
// chain-agnostic digest source, and the resyncing watermark.
func TestGroupStep6ReadModels(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// Group g1: chain-a OWNS "studio"; chain-b full-sync, chain-c selective-sync.
	mustMember := func(chain, role, state, pub string) {
		if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{
			GroupID: "g1", MemberChainID: chain, MemberAgentPubkey: pub, Role: role, MemberState: state, CAPin: "pin",
		}); err != nil {
			t.Fatalf("UpsertSyncGroupMember(%s): %v", chain, err)
		}
	}
	mustMember("chain-a", GroupRoleFullSync, GroupMemberActive, "aa")
	mustMember("chain-b", GroupRoleFullSync, GroupMemberActive, "bb")
	mustMember("chain-c", GroupRoleSelectiveSync, GroupMemberActive, "cc")
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{GroupID: "g1", DomainTag: "studio", OwnerChainID: "chain-a"}); err != nil {
		t.Fatalf("UpsertSyncGroupDomain: %v", err)
	}

	// Star fan-out from the owner: only the full-sync non-owner member.
	targets, err := s.ListGroupFanoutTargets(ctx, "chain-a", "studio.public")
	if err != nil {
		t.Fatalf("ListGroupFanoutTargets: %v", err)
	}
	if len(targets) != 1 || targets[0].MemberChainID != "chain-b" {
		t.Fatalf("fanout targets = %+v, want [chain-b]", targets)
	}

	// Digest precondition: full-sync member shares the subtree; selective-sync
	// member (no per-member consent storage) is fail-closed.
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "chain-b", "studio.public"); !ok {
		t.Fatalf("chain-b should share studio.public")
	}
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "chain-c", "studio"); ok {
		t.Fatalf("selective-sync chain-c must NOT share studio (fail closed)")
	}

	// Effective consent between two full-sync peers.
	if got, _ := s.GroupSharedDomains(ctx, "chain-a", "chain-b"); len(got) != 1 || got[0] != "studio" {
		t.Fatalf("GroupSharedDomains = %v, want [studio]", got)
	}

	// Membership alone is not consent: selective chain-c has no consent row and
	// therefore cannot receive a relay merely because it is on the roster.
	if gid, ok, _ := s.ResolveGroupRelay(ctx, "chain-c", "chain-b", "chain-a", "studio"); ok {
		t.Fatalf("ResolveGroupRelay = (%q,%v), want fail-closed no-consent denial", gid, ok)
	}
	// A non-member ORIGIN is denied.
	if _, ok, _ := s.ResolveGroupRelay(ctx, "chain-c", "chain-b", "chain-z", "studio"); ok {
		t.Fatalf("relay of non-member origin must be denied")
	}
	// A non-member RECEIVER is denied — closes cross-group laundering: a bridge relayer
	// cannot deliver an origin's memory to a node the origin shares no group with (MF2).
	if _, ok, _ := s.ResolveGroupRelay(ctx, "chain-outsider", "chain-b", "chain-a", "studio"); ok {
		t.Fatalf("relay to a non-member receiver must be denied (cross-group laundering)")
	}
	if key, kerr := s.GetGroupMemberAgentPubkey(ctx, "g1", "chain-a"); kerr != nil || key != "aa" {
		t.Fatalf("GetGroupMemberAgentPubkey = (%q,%v), want aa", key, kerr)
	}

	// Group-scoped, leak-safe digest source (must-fix #1/#3/#12). Fixture:
	//   chain-a/a1 in "studio"        — member origin, unclassified, owned by chain-a
	//   chain-x/x1 in "studio.public" — NON-member origin (chain-x is not in g1)
	//   chain-b/s1 in "studio.secret" — member origin, CLASSIFIED child owned by chain-b
	if err = s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{GroupID: "g1", DomainTag: "studio.secret", OwnerChainID: "chain-b", MaxClearance: 1}); err != nil {
		t.Fatalf("UpsertSyncGroupDomain(secret): %v", err)
	}
	for _, o := range []SyncOrigin{
		{OriginChainID: "chain-a", OriginAgentPubkey: "aa", OriginMemoryID: "a1", DomainTag: "studio", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l1", OriginSig: make([]byte, 64)},
		{OriginChainID: "chain-x", OriginAgentPubkey: "xx", OriginMemoryID: "x1", DomainTag: "studio.public", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l2", OriginSig: make([]byte, 64)},
		{OriginChainID: "chain-b", OriginAgentPubkey: "bb", OriginMemoryID: "s1", DomainTag: "studio.secret", Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l3", OriginSig: make([]byte, 64)},
	} {
		if err = s.RecordSyncOrigin(ctx, o); err != nil {
			t.Fatalf("RecordSyncOrigin: %v", err)
		}
	}
	// Responder chain-a (owns "studio", NOT "studio.secret") serving requester chain-b:
	// a1 only — x1's origin is not a member (MF3); s1 is a classified child chain-a does
	// not own (MF1, owner-star-only serving).
	ids, next, err := s.ListGroupServableOriginIDs(ctx, "g1", "chain-b", "chain-a", "studio", "", 100)
	if err != nil || next != "" || len(ids) != 1 || ids[0] != "a1" {
		t.Fatalf("ListGroupServableOriginIDs(non-owner) = (%v,%q,%v), want [a1]", ids, next, err)
	}
	// The OWNER of the classified child (chain-b) may serve s1 to an entitled full-sync
	// requester (chain-a) — owner-star serving is allowed.
	sids, _, sErr := s.ListGroupServableOriginIDs(ctx, "g1", "chain-a", "chain-b", "studio.secret", "", 100)
	if sErr != nil || len(sids) != 1 || sids[0] != "s1" {
		t.Fatalf("ListGroupServableOriginIDs(owner of classified) = (%v,%v), want [s1]", sids, sErr)
	}

	// Resyncing watermark: none active yet.
	if r, _ := s.SelfResyncingSharedWithPeer(ctx, "chain-a", "chain-b"); r {
		t.Fatalf("chain-a is active, not resyncing")
	}
	if err := s.SetSyncGroupMemberState(ctx, "g1", "chain-a", GroupMemberResyncing, 0); err != nil {
		t.Fatalf("SetSyncGroupMemberState: %v", err)
	}
	if r, _ := s.SelfResyncingSharedWithPeer(ctx, "chain-a", "chain-b"); !r {
		t.Fatalf("chain-a resyncing in a group shared with chain-b should report true")
	}
}
