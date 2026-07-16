package store

import (
	"context"
	"testing"
)

// mustRecordAdmittedCopy records an ADMITTED sync_origin row with a local copy id
// and an origin signature — the shape ListGroupRelayCandidates / GetRelayOrigin read.
func mustRecordAdmittedCopy(t *testing.T, s *SQLiteStore, originChain, originAgent, originMem, localID, domain string, sig []byte) {
	t.Helper()
	if err := s.RecordSyncOrigin(context.Background(), SyncOrigin{
		OriginChainID: originChain, OriginMemoryID: originMem, OriginCreatedAt: "2026-01-01T00:00:00Z",
		OriginAgentPubkey: originAgent, LocalMemoryID: localID, DomainTag: domain, Outcome: SyncOutcomeAdmitted, OriginSig: sig,
	}); err != nil {
		t.Fatalf("RecordSyncOrigin(%s/%s): %v", originChain, originMem, err)
	}
}

// TestGetRelayOriginRoundTrip proves origin_sig + origin_created_at are persisted
// and read back verbatim (the additive column round-trips), and that a NULL sig
// reads back as a nil OriginSig (the "unsigned relay" fail-closed signal).
func TestGetRelayOriginRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	sig := []byte{0x01, 0x02, 0x03, 0x04}
	local := "local-copy-1"
	mustRecordAdmittedCopy(t, s, "chain-x", "keyx", "x-1", local, "studio", sig)

	ro, err := s.GetRelayOrigin(ctx, "chain-x", local)
	if err != nil {
		t.Fatalf("GetRelayOrigin: %v", err)
	}
	if ro.OriginMemoryID != "x-1" || ro.OriginCreatedAt != "2026-01-01T00:00:00Z" {
		t.Fatalf("origin provenance not round-tripped: %+v", ro)
	}
	if string(ro.OriginSig) != string(sig) {
		t.Fatalf("origin_sig not round-tripped: got %x want %x", ro.OriginSig, sig)
	}

	// A NULL-sig admitted copy reads back with a nil OriginSig.
	mustRecordAdmittedCopy(t, s, "chain-y", "keyy", "y-1", "local-copy-2", "studio", nil)
	roNil, err := s.GetRelayOrigin(ctx, "chain-y", "local-copy-2")
	if err != nil {
		t.Fatalf("GetRelayOrigin(nil sig): %v", err)
	}
	if len(roNil.OriginSig) != 0 {
		t.Fatalf("expected nil origin_sig, got %x", roNil.OriginSig)
	}
}

// TestListGroupRelayCandidatesLeakSafe proves the sender-side relay enumeration
// mirrors ListGroupServableOriginIDs' leak-safe covering-domain rule and adds the
// three relay-specific exclusions: origin==requester, no local copy, and
// local_suppress tombstoned rows.
func TestListGroupRelayCandidatesLeakSafe(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)

	// Group g1: origin X, relayer R (responder/local), member M (requester) all
	// active full-sync; unclassified domain "studio".
	mustGroupDomain(t, s, "g1", "studio", "chain-x", 0)
	mustGroupMember(t, s, "g1", "chain-x", GroupRoleFullSync, GroupMemberActive, "keyx")
	mustGroupMember(t, s, "g1", "chain-r", GroupRoleFullSync, GroupMemberActive, "keyr")
	mustGroupMember(t, s, "g1", "chain-m", GroupRoleFullSync, GroupMemberActive, "keym")

	// R (responder) HOLDS an admitted copy of X's studio memory -> a candidate.
	mustRecordAdmittedCopy(t, s, "chain-x", "keyx", "x-1", "local-x1", "studio", make([]byte, 64))
	// R's OWN copy of M's memory: origin == requester -> NOT a relay candidate.
	mustRecordAdmittedCopy(t, s, "chain-m", "keym", "m-1", "local-m1", "studio", make([]byte, 64))
	// A same-domain-dup admission (empty local id) -> no distinct copy to serve.
	mustRecordAdmittedCopy(t, s, "chain-x", "keyx", "x-2", "", "studio", make([]byte, 64))
	// A suppressed (Gate 6.5) origin pair -> excluded even though R holds a copy.
	mustRecordAdmittedCopy(t, s, "chain-x", "keyx", "x-3", "local-x3", "studio", make([]byte, 64))
	if err := s.InsertSyncTombstone(ctx, SyncTombstone{
		GroupID: "g1", Scope: TombstoneScopeMemory, Enforcement: TombstoneEnforceLocalSuppress,
		OriginChainID: "chain-x", OriginMemoryID: "x-3",
	}); err != nil {
		t.Fatalf("InsertSyncTombstone: %v", err)
	}

	cands, _, err := s.ListGroupRelayCandidates(ctx, "g1", "chain-m", "chain-r", "studio", "", 100)
	if err != nil {
		t.Fatalf("ListGroupRelayCandidates: %v", err)
	}
	got := map[string]GroupRelayCandidate{}
	for _, c := range cands {
		got[c.OriginMemoryID] = c
	}
	if _, ok := got["x-1"]; !ok || len(cands) != 1 {
		t.Fatalf("expected exactly the relayable copy x-1, got %+v", cands)
	}
	c := got["x-1"]
	if c.OriginChainID != "chain-x" || c.LocalMemoryID != "local-x1" {
		t.Fatalf("relay candidate carries wrong provenance: %+v", c)
	}
	if _, ok := got["m-1"]; ok {
		t.Fatal("origin==requester must never be a relay candidate (pairwise, not relay)")
	}
	if _, ok := got["x-2"]; ok {
		t.Fatal("a same-domain-dup admission (no local copy) is not relayable")
	}
	if _, ok := got["x-3"]; ok {
		t.Fatal("a local_suppress tombstoned pair must be excluded (Gate 6.5)")
	}
}

// TestListGroupRelayCandidatesClassifiedOwnerOnly proves must-fix #12 on the
// relay-enqueue side: a classified domain is served only by its OWNER-relayer;
// a non-owner relayer enumerates no candidates for it (owner-star-only).
func TestListGroupRelayCandidatesClassifiedOwnerOnly(t *testing.T) {
	ctx := context.Background()

	// Case A: classified "secret" owned by the RELAYER (chain-r) -> served.
	s := newSyncTestStore(t)
	mustGroupDomain(t, s, "g1", "secret", "chain-r", 3)
	mustGroupMember(t, s, "g1", "chain-x", GroupRoleFullSync, GroupMemberActive, "keyx")
	mustGroupMember(t, s, "g1", "chain-r", GroupRoleFullSync, GroupMemberActive, "keyr")
	mustGroupMember(t, s, "g1", "chain-m", GroupRoleFullSync, GroupMemberActive, "keym")
	mustRecordAdmittedCopy(t, s, "chain-x", "keyx", "x-9", "local-x9", "secret", make([]byte, 64))
	cands, _, err := s.ListGroupRelayCandidates(ctx, "g1", "chain-m", "chain-r", "secret", "", 100)
	if err != nil {
		t.Fatalf("ListGroupRelayCandidates(owner): %v", err)
	}
	if len(cands) != 1 || cands[0].OriginMemoryID != "x-9" {
		t.Fatalf("owner relayer must serve its classified domain, got %+v", cands)
	}

	// Case B: identical, but "secret" is owned by X (NOT the relayer) -> a
	// non-owner relayer must enumerate nothing for the classified domain.
	s2 := newSyncTestStore(t)
	mustGroupDomain(t, s2, "g1", "secret", "chain-x", 3)
	mustGroupMember(t, s2, "g1", "chain-x", GroupRoleFullSync, GroupMemberActive, "keyx")
	mustGroupMember(t, s2, "g1", "chain-r", GroupRoleFullSync, GroupMemberActive, "keyr")
	mustGroupMember(t, s2, "g1", "chain-m", GroupRoleFullSync, GroupMemberActive, "keym")
	mustRecordAdmittedCopy(t, s2, "chain-x", "keyx", "x-9", "local-x9", "secret", make([]byte, 64))
	cands2, _, err := s2.ListGroupRelayCandidates(ctx, "g1", "chain-m", "chain-r", "secret", "", 100)
	if err != nil {
		t.Fatalf("ListGroupRelayCandidates(non-owner): %v", err)
	}
	if len(cands2) != 0 {
		t.Fatalf("a non-owner relayer must NOT serve a classified domain (owner-star-only), got %+v", cands2)
	}
}
