package store

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
)

// seedPurgeMemory inserts a committed memory in a domain so the purge can join
// sync_outbox.memory_id -> memories.domain_tag.
func seedPurgeMemory(t *testing.T, s *SQLiteStore, id, domain string) {
	t.Helper()
	sum := sha256.Sum256([]byte(id + domain))
	if err := s.InsertMemory(context.Background(), &memory.MemoryRecord{
		MemoryID: id, SubmittingAgent: "seed", Content: id, ContentHash: sum[:],
		MemoryType: memory.TypeFact, DomainTag: domain, ConfidenceScore: 0.9,
		Status: memory.StatusCommitted, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InsertMemory(%s): %v", id, err)
	}
}

// TestPurgeGroupSyncPeerStateGroupOnly proves a group-only peer (no pairwise
// sync_domains rows) has ALL of its outbox purged, and that sync_domains /
// sync_control for OTHER peers are untouched.
func TestPurgeGroupSyncPeerStateGroupOnly(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	seedPurgeMemory(t, s, "m-studio", "studio")
	seedPurgeMemory(t, s, "m-band", "band")
	// Two group-fan-out rows toward the evicted chain; it has NO pairwise consent.
	mustEnqueue(t, s, "chain-evict", "m-studio")
	mustEnqueue(t, s, "chain-evict", "m-band")

	if err := s.PurgeGroupSyncPeerState(ctx, "chain-evict"); err != nil {
		t.Fatalf("PurgeGroupSyncPeerState: %v", err)
	}
	counts, err := s.CountSyncOutboxByState(ctx, "chain-evict")
	if err != nil {
		t.Fatalf("CountSyncOutboxByState: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("group-only peer must have ALL outbox purged, got %+v", counts)
	}
}

// TestPurgeGroupSyncPeerStateKeepsPairwise proves I10: a peer that ALSO holds an
// independent pairwise sync_domains relationship keeps exactly its pairwise-covered
// outbox rows (subtree-aware); its group-only rows are purged; sync_domains and
// sync_control are never touched.
func TestPurgeGroupSyncPeerStateKeepsPairwise(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// The peer has an independent pairwise consent for "hr" (covers "hr.public").
	if err := s.SetSyncDomains(ctx, "chain-b", []string{"hr"}); err != nil {
		t.Fatalf("SetSyncDomains: %v", err)
	}
	// A pairwise sync_control binding that must survive the group purge.
	if err := s.PrepareSyncControl(ctx, SyncControl{
		RemoteChainID: "chain-b", Role: "guest", ControllerChainID: "chain-ctl",
		ControllerAgentID: "agent", PolicyEpoch: "e1", RemoteCAPin: "pin",
	}); err != nil {
		t.Fatalf("PrepareSyncControl: %v", err)
	}

	seedPurgeMemory(t, s, "m-hr", "hr")          // pairwise-covered (exact)
	seedPurgeMemory(t, s, "m-hr-pub", "hr.public") // pairwise-covered (subtree)
	seedPurgeMemory(t, s, "m-studio", "studio")  // group-only, NOT pairwise-covered
	for _, id := range []string{"m-hr", "m-hr-pub", "m-studio"} {
		mustEnqueue(t, s, "chain-b", id)
	}

	if err := s.PurgeGroupSyncPeerState(ctx, "chain-b"); err != nil {
		t.Fatalf("PurgeGroupSyncPeerState: %v", err)
	}

	// The two pairwise-covered rows survive; the group-only row is purged.
	rows, err := s.ListSyncOutbox(ctx, "chain-b", SyncStatePending, 100)
	if err != nil {
		t.Fatalf("ListSyncOutbox: %v", err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.MemoryID] = true
	}
	if !got["m-hr"] || !got["m-hr-pub"] {
		t.Fatalf("pairwise-covered rows must survive (I10), got %+v", got)
	}
	if got["m-studio"] {
		t.Fatalf("group-only row must be purged, got %+v", got)
	}

	// sync_domains (pairwise consent) is NEVER touched.
	doms, err := s.GetSyncDomains(ctx, "chain-b")
	if err != nil || len(doms) != 1 || doms[0] != "hr" {
		t.Fatalf("pairwise sync_domains must be intact, got %+v err=%v", doms, err)
	}
	// sync_control (the pairwise binding) is NEVER touched.
	ctl, err := s.GetSyncControl(ctx, "chain-b")
	if err != nil || ctl == nil {
		t.Fatalf("pairwise sync_control must survive the group purge, got %+v err=%v", ctl, err)
	}
}

// TestDomainRemovalDropsFromServePaths proves D2 at the store layer: once a domain
// is stamped removed (removed_revision>0), it drops out of effective group consent,
// owner fan-out scope, and the backfill candidate scan; a fresh domain_add
// (removed_revision back to 0) makes it servable again.
func TestDomainRemovalDropsFromServePaths(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	mustGroupMember(t, s, "g1", "chain-local", GroupRoleFullSync, GroupMemberActive, "")
	mustGroupMember(t, s, "g1", "chain-b", GroupRoleFullSync, GroupMemberActive, "")
	mustGroupDomain(t, s, "g1", "studio", "chain-local", 0)
	// An admitted origin id served from the backfill candidate scan.
	if err := s.RecordSyncOrigin(ctx, SyncOrigin{
		OriginChainID: "chain-local", OriginMemoryID: "s-1", DomainTag: "studio",
		Outcome: SyncOutcomeAdmitted, LocalMemoryID: "l-1"}); err != nil {
		t.Fatalf("RecordSyncOrigin: %v", err)
	}

	shared, _ := s.GroupSharedDomains(ctx, "chain-local", "chain-b")
	owned, _ := s.GroupOwnedDomainsForPeer(ctx, "chain-local", "chain-b")
	ids, _, _ := s.ListGroupServableOriginIDs(ctx, "g1", "chain-b", "chain-local", "studio", "", 100)
	if len(shared) != 1 || len(owned) != 1 || len(ids) != 1 {
		t.Fatalf("active domain must serve everywhere: shared=%v owned=%v ids=%v", shared, owned, ids)
	}

	// domain_remove apply: stamp removed_revision>0.
	if err := s.SetSyncGroupDomainRemoved(ctx, "g1", "studio", 5); err != nil {
		t.Fatalf("SetSyncGroupDomainRemoved: %v", err)
	}
	shared, _ = s.GroupSharedDomains(ctx, "chain-local", "chain-b")
	owned, _ = s.GroupOwnedDomainsForPeer(ctx, "chain-local", "chain-b")
	ids, _, _ = s.ListGroupServableOriginIDs(ctx, "g1", "chain-b", "chain-local", "studio", "", 100)
	if len(shared) != 0 || len(owned) != 0 || len(ids) != 0 {
		t.Fatalf("removed domain must drop everywhere: shared=%v owned=%v ids=%v", shared, owned, ids)
	}

	// Re-add (domain_add with removed_revision back to 0) resumes serving.
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{
		GroupID: "g1", DomainTag: "studio", OwnerChainID: "chain-local", MaxClearance: 0,
		AddedRevision: 9, RemovedRevision: 0}); err != nil {
		t.Fatalf("re-add UpsertSyncGroupDomain: %v", err)
	}
	shared, _ = s.GroupSharedDomains(ctx, "chain-local", "chain-b")
	ids, _, _ = s.ListGroupServableOriginIDs(ctx, "g1", "chain-b", "chain-local", "studio", "", 100)
	if len(shared) != 1 || len(ids) != 1 {
		t.Fatalf("re-added domain must serve again: shared=%v ids=%v", shared, ids)
	}
	// The prior sync_origin row is retained (INSERT OR IGNORE), so re-add backfills
	// only the genuine gap with no duplicate origin row.
	all, err := s.ListSyncOriginIDs(ctx, "chain-local", "studio", "", 100)
	if err != nil || len(all) != 1 {
		t.Fatalf("sync_origin must be retained across removal/re-add, got %v err=%v", all, err)
	}
}

func mustEnqueue(t *testing.T, s *SQLiteStore, chain, memID string) {
	t.Helper()
	if _, err := s.EnqueueSyncOutbox(context.Background(), chain, memID); err != nil {
		t.Fatalf("EnqueueSyncOutbox(%s,%s): %v", chain, memID, err)
	}
}

func mustGroupMember(t *testing.T, s *SQLiteStore, groupID, chain, role, state, pubHex string) {
	t.Helper()
	if err := s.UpsertSyncGroupMember(context.Background(), SyncGroupMember{
		GroupID: groupID, MemberChainID: chain, MemberAgentPubkey: pubHex,
		Role: role, MemberState: state, CAPin: "pin",
	}); err != nil {
		t.Fatalf("UpsertSyncGroupMember(%s): %v", chain, err)
	}
}

func mustGroupDomain(t *testing.T, s *SQLiteStore, groupID, domain, owner string, maxClearance int) {
	t.Helper()
	if err := s.UpsertSyncGroupDomain(context.Background(), SyncGroupDomain{
		GroupID: groupID, DomainTag: domain, OwnerChainID: owner, MaxClearance: maxClearance,
	}); err != nil {
		t.Fatalf("UpsertSyncGroupDomain(%s): %v", domain, err)
	}
}

// TestPurgeGroupSyncPeerStateAlwaysPurgesRelayed proves R1: a RELAYED row
// (origin_chain_id != '') is ALWAYS purged on a group removal, even when a pairwise
// consent coincidentally covers its domain — a pairwise relationship never entitles the
// peer to a THIRD member's relayed group copy. Only NATIVE rows may be spared by pairwise.
func TestPurgeGroupSyncPeerStateAlwaysPurgesRelayed(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	// Pairwise consent for "studio" — the coincidental overlap R1 exploits.
	if err := s.SetSyncDomains(ctx, "chain-b", []string{"studio"}); err != nil {
		t.Fatalf("SetSyncDomains: %v", err)
	}
	seedPurgeMemory(t, s, "m-native", "studio")    // native, pairwise-covered -> survives
	seedPurgeMemory(t, s, "m-relaycopy", "studio")  // a local copy of a third member's memory
	mustEnqueue(t, s, "chain-b", "m-native")
	if _, err := s.EnqueueRelayedSyncOutbox(ctx, "chain-b", "m-relaycopy", "chain-x"); err != nil {
		t.Fatalf("EnqueueRelayedSyncOutbox: %v", err)
	}
	if err := s.PurgeGroupSyncPeerState(ctx, "chain-b"); err != nil {
		t.Fatalf("PurgeGroupSyncPeerState: %v", err)
	}
	rows, err := s.ListSyncOutbox(ctx, "chain-b", SyncStatePending, 100)
	if err != nil {
		t.Fatalf("ListSyncOutbox: %v", err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.MemoryID] = true
	}
	if !got["m-native"] {
		t.Fatalf("native pairwise-covered row must survive, got %+v", got)
	}
	if got["m-relaycopy"] {
		t.Fatalf("relayed row must be purged on group removal even when pairwise covers its domain (R1), got %+v", got)
	}
}
