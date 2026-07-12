package store

import (
	"context"
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
	if err := s.UpsertSyncGroup(ctx, stale); err != nil {
		t.Fatalf("UpsertSyncGroup stale: %v", err)
	}
	got, _ = s.GetSyncGroup(ctx, "g1")
	if got.RosterRevisionFloor != 3 {
		t.Fatalf("floor regressed to %d", got.RosterRevisionFloor)
	}

	// Members: valid role, and the voting_power zero-pin.
	if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-a", Role: GroupRoleFullSync, MemberState: GroupMemberActive,
		MemberAgentPubkey: "apub", CAPin: "pinA",
	}); err != nil {
		t.Fatalf("UpsertSyncGroupMember A: %v", err)
	}
	if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-b", Role: GroupRoleSelectiveSync, MemberState: GroupMemberInvited,
	}); err != nil {
		t.Fatalf("UpsertSyncGroupMember B: %v", err)
	}
	if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-x", Role: "controller", // invalid role
	}); err == nil {
		t.Fatalf("expected invalid role rejection")
	}
	if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{
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
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{
		GroupID: "g1", DomainTag: "eurorack", OwnerChainID: "chain-a", OwnerSig: "sigA", MaxClearance: 0, AddedRevision: 2,
	}); err != nil {
		t.Fatalf("UpsertSyncGroupDomain: %v", err)
	}
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{
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
	if err := s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "roster", Seq: 0, EntryHash: "h0", EntryType: "group_create", AuthorChainID: "chain-ctl",
	}); err != nil {
		t.Fatalf("AppendSyncGroupLog roster0: %v", err)
	}
	if err := s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "roster", Seq: 1, PrevHash: "h0", EntryHash: "h1", EntryType: "member_activate",
	}); err != nil {
		t.Fatalf("AppendSyncGroupLog roster1: %v", err)
	}
	if err := s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
		GroupID: "g1", Subchain: "domain:eurorack", Seq: 0, EntryHash: "d0", EntryType: "domain_add", AuthorChainID: "chain-a",
	}); err != nil {
		t.Fatalf("AppendSyncGroupLog domain0: %v", err)
	}
	// Duplicate (group, subchain, seq) must fail the PK.
	if err := s.AppendSyncGroupLog(ctx, SyncGroupLogEntry{
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
	if err := s.InsertSyncTombstone(ctx, SyncTombstone{
		GroupID: "g1", Scope: TombstoneScopeMemory, Enforcement: TombstoneEnforceLocalSuppress,
		OriginChainID: "chain-a", OriginMemoryID: "mem-1", Reason: "local delete",
	}); err != nil {
		t.Fatalf("InsertSyncTombstone: %v", err)
	}
	// Idempotent re-insert.
	if err := s.InsertSyncTombstone(ctx, SyncTombstone{
		GroupID: "g1", Scope: TombstoneScopeMemory, Enforcement: TombstoneEnforceLocalSuppress,
		OriginChainID: "chain-a", OriginMemoryID: "mem-1",
	}); err != nil {
		t.Fatalf("InsertSyncTombstone idempotent: %v", err)
	}
	if err := s.InsertSyncTombstone(ctx, SyncTombstone{
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
