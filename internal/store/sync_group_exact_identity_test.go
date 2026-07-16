package store

import (
	"context"
	"testing"
)

func TestExactGroupIdentityDoesNotCrossJoinSameDomainAcrossGroups(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// G1 says A owns studio, but its A/B roster identities are stale.
	mustGroupDomain(t, s, "g1", "studio", "chain-a", 0)
	mustGroupMember(t, s, "g1", "chain-a", GroupRoleFullSync, GroupMemberActive, "old-a")
	mustGroupMember(t, s, "g1", "chain-b", GroupRoleFullSync, GroupMemberActive, "old-b")

	// G2 has the current A/B identities and the same domain, but C owns it.
	mustGroupDomain(t, s, "g2", "studio", "chain-c", 0)
	mustGroupMember(t, s, "g2", "chain-a", GroupRoleFullSync, GroupMemberActive, "current-a")
	mustGroupMember(t, s, "g2", "chain-b", GroupRoleFullSync, GroupMemberActive, "current-b")
	mustGroupMember(t, s, "g2", "chain-c", GroupRoleFullSync, GroupMemberActive, "current-c")

	shared, err := s.GroupSharedDomainsForAgents(ctx, "chain-a", "current-a", "chain-b", "current-b")
	if err != nil || len(shared) != 1 || shared[0] != "studio" {
		t.Fatalf("current identities should share g2/studio: shared=%v err=%v", shared, err)
	}
	owned, err := s.GroupOwnedDomainsForPeerAgents(ctx, "chain-a", "current-a", "chain-b", "current-b")
	if err != nil || len(owned) != 0 {
		t.Fatalf("ownership from stale g1 must not cross-join current g2 sharing: owned=%v err=%v", owned, err)
	}
	if ok, err := s.MemberSharesGroupDomainForAgent(ctx, "g1", "chain-a", "current-a", "studio"); err != nil || ok {
		t.Fatalf("same-chain wrong agent used stale g1 membership: ok=%v err=%v", ok, err)
	}
}

func TestGroupOriginIdentityFiltersDigestRelayAndSelectsUnshadowedRoute(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	for _, groupID := range []string{"g1", "g2"} {
		mustGroupDomain(t, s, groupID, "studio", "chain-x", 0)
		mustGroupMember(t, s, groupID, "chain-r", GroupRoleFullSync, GroupMemberActive, "key-r")
		mustGroupMember(t, s, groupID, "chain-m", GroupRoleFullSync, GroupMemberActive, "key-m")
	}
	mustGroupMember(t, s, "g1", "chain-x", GroupRoleFullSync, GroupMemberActive, "key-b")
	mustGroupMember(t, s, "g2", "chain-x", GroupRoleFullSync, GroupMemberActive, "key-a")

	// The copy was authenticated as agent A. G1 is lexically first but pins B;
	// exact route selection must continue to G2 rather than shadowing it.
	if err := s.RecordSyncOrigin(ctx, SyncOrigin{
		OriginChainID: "chain-x", OriginAgentPubkey: "key-a", OriginMemoryID: "x-1",
		LocalMemoryID: "local-x1", DomainTag: "studio", Outcome: SyncOutcomeAdmitted,
		OriginSig: make([]byte, 64),
	}); err != nil {
		t.Fatal(err)
	}
	if groupID, ok, err := s.ResolveGroupRelayForOriginAgent(ctx,
		"chain-r", "key-r", "chain-m", "key-m", "chain-x", "key-a", "studio"); err != nil || !ok || groupID != "g2" {
		t.Fatalf("origin A route = (%q,%v,%v), want g2", groupID, ok, err)
	}

	// A group that pins B must disclose neither the A-origin digest id nor an A
	// relay candidate, even though the chain id and domain are identical.
	ids, _, err := s.ListGroupServableOriginIDs(ctx, "g1", "chain-m", "chain-r", "studio", "", 100)
	if err != nil || len(ids) != 0 {
		t.Fatalf("wrong-origin-key digest leaked ids=%v err=%v", ids, err)
	}
	candidates, _, err := s.ListGroupRelayCandidates(ctx, "g1", "chain-m", "chain-r", "studio", "", 100)
	if err != nil || len(candidates) != 0 {
		t.Fatalf("wrong-origin-key relay enumerated candidates=%v err=%v", candidates, err)
	}
	ids, _, err = s.ListGroupServableOriginIDs(ctx, "g2", "chain-m", "chain-r", "studio", "", 100)
	if err != nil || len(ids) != 1 || ids[0] != "x-1" {
		t.Fatalf("matching-origin-key digest ids=%v err=%v", ids, err)
	}
}
