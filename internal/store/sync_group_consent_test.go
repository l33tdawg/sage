package store

import (
	"context"
	"reflect"
	"testing"
)

// seedConsentGroup builds a group with an owner (full-sync, owns the shared set),
// a full-sync member, a selective-sync member whose consent we manage, and a
// selective-sync member with NO consent rows (the fail-closed control).
func seedConsentGroup(t *testing.T, s *SQLiteStore) {
	t.Helper()
	ctx := context.Background()
	if err := s.UpsertSyncGroup(ctx, SyncGroup{
		GroupID: "g1", ControllerChainID: "chain-ctl", ControllerAgentPubkey: "ctl",
	}); err != nil {
		t.Fatalf("seed group: %v", err)
	}
	for _, m := range []struct{ chain, role string }{
		{"chain-own", GroupRoleFullSync},
		{"chain-full", GroupRoleFullSync},
		{"chain-sel", GroupRoleSelectiveSync},
		{"chain-none", GroupRoleSelectiveSync},
	} {
		if err := s.UpsertSyncGroupMember(ctx, SyncGroupMember{
			GroupID: "g1", MemberChainID: m.chain, Role: m.role, MemberState: GroupMemberActive,
			MemberAgentPubkey: m.chain + "-pub",
		}); err != nil {
			t.Fatalf("seed member %s: %v", m.chain, err)
		}
	}
	for _, tag := range []string{"hr", "eng", "eng.backend"} {
		if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{
			GroupID: "g1", DomainTag: tag, OwnerChainID: "chain-own",
		}); err != nil {
			t.Fatalf("seed domain %s: %v", tag, err)
		}
	}
}

func fanoutHas(targets []GroupFanoutTarget, chain string) bool {
	for _, t := range targets {
		if t.MemberChainID == chain {
			return true
		}
	}
	return false
}

// T2(e): the widened getters serve a selective member its consented, subtree-matched
// subset (consent to "eng" covers "eng" AND "eng.backend" but never "hr"); a
// selective member with no consent rows stays fail-closed (under-serves nothing); a
// full-sync member is unaffected and still gets the whole shared set.
func TestGroupMemberConsentSelectiveServe(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	seedConsentGroup(t, s)

	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "chain-sel", []string{"eng"}, 2); err != nil {
		t.Fatalf("replace consent: %v", err)
	}
	if got, _ := s.ListGroupMemberConsentDomains(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"eng"}) {
		t.Fatalf("consent set = %v; want [eng]", got)
	}

	// GroupDomainsForMember — subtree-matched subset for the selective member.
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"eng", "eng.backend"}) {
		t.Fatalf("selective serve = %v; want [eng eng.backend]", got)
	}
	// A selective member with NO consent rows is served nothing (fail-closed).
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-none"); len(got) != 0 {
		t.Fatalf("no-consent member must be served nothing, got %v", got)
	}
	// A full-sync member still gets the whole active shared set via the role branch.
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-full"); !reflect.DeepEqual(got, []string{"eng", "eng.backend", "hr"}) {
		t.Fatalf("full-sync serve = %v; want [eng eng.backend hr]", got)
	}

	// MemberSharesGroupDomain — subtree match true for consented, false otherwise.
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "chain-sel", "eng.backend"); !ok {
		t.Fatalf("selective member must share the consented subtree eng.backend")
	}
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "chain-sel", "hr"); ok {
		t.Fatalf("selective member must NOT share un-consented hr")
	}
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "chain-none", "eng"); ok {
		t.Fatalf("no-consent member must share nothing")
	}

	// GroupSharedDomains(owner, selective) = the consented subset.
	if got, _ := s.GroupSharedDomains(ctx, "chain-own", "chain-sel"); !reflect.DeepEqual(got, []string{"eng", "eng.backend"}) {
		t.Fatalf("shared(own,sel) = %v; want [eng eng.backend]", got)
	}
	// GroupSharedDomainsWithGroup carries the group id but the same set.
	refs, _ := s.GroupSharedDomainsWithGroup(ctx, "chain-own", "chain-sel")
	var tags []string
	for _, r := range refs {
		tags = append(tags, r.DomainTag)
	}
	if !reflect.DeepEqual(tags, []string{"eng", "eng.backend"}) {
		t.Fatalf("shared-with-group(own,sel) = %v; want [eng eng.backend]", tags)
	}
	// GroupOwnedDomainsForPeer — owner's domains the selective peer may receive.
	if got, _ := s.GroupOwnedDomainsForPeer(ctx, "chain-own", "chain-sel"); !reflect.DeepEqual(got, []string{"eng"}) {
		t.Fatalf("owned-for-peer(sel) = %v; want normalized effective scope [eng]", got)
	}

	// ListGroupFanoutTargets for the owner's eng.backend includes the consenting
	// selective member + the full-sync member, excludes the no-consent member and the
	// owner (never fanned back to its author).
	bk, _ := s.ListGroupFanoutTargets(ctx, "chain-own", "eng.backend")
	if !fanoutHas(bk, "chain-sel") || !fanoutHas(bk, "chain-full") {
		t.Fatalf("eng.backend fan-out must include chain-sel + chain-full: %+v", bk)
	}
	if fanoutHas(bk, "chain-none") || fanoutHas(bk, "chain-own") {
		t.Fatalf("eng.backend fan-out must exclude chain-none + owner: %+v", bk)
	}
	// hr fan-out must NOT reach the selective member (no covering consent).
	hr, _ := s.ListGroupFanoutTargets(ctx, "chain-own", "hr")
	if fanoutHas(hr, "chain-sel") {
		t.Fatalf("selective member must NOT receive hr fan-out: %+v", hr)
	}
	if !fanoutHas(hr, "chain-full") {
		t.Fatalf("full-sync member must still receive hr fan-out: %+v", hr)
	}
}

func TestSelectiveConsentCannotWidenToFutureSiblingAndDescendantWorks(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	for _, m := range []SyncGroupMember{
		{GroupID: "g1", MemberChainID: "owner", Role: GroupRoleFullSync, MemberState: GroupMemberActive, MemberAgentPubkey: "owner-key"},
		{GroupID: "g1", MemberChainID: "member", Role: GroupRoleSelectiveSync, MemberState: GroupMemberActive, MemberAgentPubkey: "member-key"},
	} {
		if err := s.UpsertSyncGroupMember(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{GroupID: "g1", DomainTag: "hr.public", OwnerChainID: "owner"}); err != nil {
		t.Fatal(err)
	}

	// "hr" is broader than today's shared root and must be discarded. Adding a
	// sibling later cannot retroactively turn that invalid choice into consent.
	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "member", []string{"hr"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{GroupID: "g1", DomainTag: "hr.secret", OwnerChainID: "owner"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "member"); len(got) != 0 {
		t.Fatalf("ancestor consent silently widened to future sibling: %v", got)
	}

	// With an explicit shared root, selecting only one descendant produces that
	// exact effective scope across the widened read models.
	if err := s.UpsertSyncGroupDomain(ctx, SyncGroupDomain{GroupID: "g1", DomainTag: "docs", OwnerChainID: "owner"}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "member", []string{"docs.public"}, 2); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "member"); !reflect.DeepEqual(got, []string{"docs.public"}) {
		t.Fatalf("descendant-only effective scope=%v", got)
	}
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "member", "docs.public.note"); !ok {
		t.Fatal("descendant consent under-served")
	}
	if ok, _ := s.MemberSharesGroupDomain(ctx, "g1", "member", "docs.secret"); ok {
		t.Fatal("descendant consent over-served sibling")
	}
	if got, _ := s.GroupSharedDomains(ctx, "owner", "member"); !reflect.DeepEqual(got, []string{"docs.public"}) {
		t.Fatalf("shared descendant scope=%v", got)
	}
	if got, _ := s.GroupOwnedDomainsForPeer(ctx, "owner", "member"); !reflect.DeepEqual(got, []string{"docs.public"}) {
		t.Fatalf("owned descendant scope=%v", got)
	}
	targets, _ := s.ListGroupFanoutTargets(ctx, "owner", "docs.public.note")
	if !fanoutHas(targets, "member") {
		t.Fatal("descendant fanout under-served")
	}
	targets, _ = s.ListGroupFanoutTargets(ctx, "owner", "docs.secret")
	if fanoutHas(targets, "member") {
		t.Fatal("descendant fanout over-served sibling")
	}
}

// T2(e): a consent tag OUTSIDE the active shared set is rejected (never over-served);
// a full-sync->selective narrowing revision-STAMPS the stale rows (monotonic, not
// deleted); a domain_remove stamps the member's stale consent row; and every write
// is idempotent under journal replay.
func TestGroupMemberConsentRevisionStampIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	seedConsentGroup(t, s)

	// Over-serve guard: "secret" is not in the shared set, so it is dropped.
	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "chain-sel", []string{"eng", "secret"}, 2); err != nil {
		t.Fatalf("replace consent: %v", err)
	}
	if got, _ := s.ListGroupMemberConsentDomains(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"eng"}) {
		t.Fatalf("out-of-shared-set tag must be rejected, got %v", got)
	}

	// Idempotent replay of the SAME entry (same set + revision) changes nothing.
	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "chain-sel", []string{"eng", "secret"}, 2); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"eng", "eng.backend"}) {
		t.Fatalf("idempotent replay serve = %v", got)
	}

	// Narrowing to {hr} retires the stale "eng" row (stamped, not deleted).
	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "chain-sel", []string{"hr"}, 5); err != nil {
		t.Fatalf("narrow: %v", err)
	}
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"hr"}) {
		t.Fatalf("post-narrow serve = %v; want [hr]", got)
	}
	if got, _ := s.ListGroupMemberConsentDomains(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"hr"}) {
		t.Fatalf("post-narrow consent = %v; want [hr]", got)
	}
	// Replay of the narrowing entry is idempotent (removed_revision monotonic).
	if err := s.ReplaceGroupMemberConsentDomains(ctx, "g1", "chain-sel", []string{"hr"}, 5); err != nil {
		t.Fatalf("narrow replay: %v", err)
	}
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-sel"); !reflect.DeepEqual(got, []string{"hr"}) {
		t.Fatalf("narrow replay serve = %v; want [hr]", got)
	}

	// domain_remove of "hr" retires the member's stale "hr" consent row.
	if err := s.SetSyncGroupDomainRemoved(ctx, "g1", "hr", 6); err != nil {
		t.Fatalf("remove domain hr: %v", err)
	}
	if err := s.StampGroupMemberConsentDomainRemoved(ctx, "g1", "hr", 6); err != nil {
		t.Fatalf("stamp consent removed: %v", err)
	}
	if got, _ := s.ListGroupMemberConsentDomains(ctx, "g1", "chain-sel"); len(got) != 0 {
		t.Fatalf("hr consent must be retired after domain_remove, got %v", got)
	}
	if got, _ := s.GroupDomainsForMember(ctx, "g1", "chain-sel"); len(got) != 0 {
		t.Fatalf("post-remove serve must be empty, got %v", got)
	}
	// Re-stamping the same removal is a no-op (idempotent).
	if err := s.StampGroupMemberConsentDomainRemoved(ctx, "g1", "hr", 6); err != nil {
		t.Fatalf("stamp replay: %v", err)
	}
	if got, _ := s.ListGroupMemberConsentDomains(ctx, "g1", "chain-sel"); len(got) != 0 {
		t.Fatalf("stamp replay must not resurrect consent, got %v", got)
	}
}
