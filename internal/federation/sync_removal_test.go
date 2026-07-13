package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// authorRoster appends one controller-authored roster entry via the real
// AppendGroupJournalEntry path (which fires the POST-BATCH removal hook).
func authorRoster(t *testing.T, m *Manager, groupID, entryType, ctl string, ctlPub ed25519.PublicKey, ctlKey ed25519.PrivateKey, payload map[string]string) {
	t.Helper()
	_, err := m.AppendGroupJournalEntry(context.Background(), groupID, RosterSubchain, entryType, ctl, ctlPub, ctlKey, payload)
	require.NoError(t, err)
}

// TestMemberRemovalEnforcement proves D1 (§10 rows 2-3, I10): authoring a
// member_remove runs the POST-BATCH hook — the ex-member's GROUP outbox row is
// purged while an independent pairwise-covered row and the pairwise consent
// survive — and the mandatory roster anchor fires exactly once for the batch.
func TestMemberRemovalEnforcement(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet) // localChainID == "chain-local"
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	bPub, bKey, _ := ed25519.GenerateKey(nil)

	anchors := 0
	m.syncAnchorFn = func(_ context.Context, groupID, subchain, head string) error {
		assert.Equal(t, "g1", groupID)
		assert.Equal(t, RosterSubchain, subchain)
		assert.NotEmpty(t, head)
		anchors++
		return nil
	}

	// Bring chain-b to active via the real journal path (no anchor: not a removal).
	authorRoster(t, m, "g1", "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-b", bPub, bKey, store.GroupRoleFullSync, "pinB"))
	authorRoster(t, m, "g1", "member_activate", "chain-ctl", ctlPub, ctlKey, memberChainPayload("chain-b"))
	require.Equal(t, 0, anchors, "invite/activate must not anchor")

	// An independent pairwise relationship (I10): consent for "hr" + a covered outbox row.
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-hr", "hr", "pairwise fact")
	seedCommitted(t, ms, "m-studio", "studio", "group fan-out fact")
	_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "m-hr")
	require.NoError(t, err)
	_, err = ms.EnqueueSyncOutbox(ctx, "chain-b", "m-studio")
	require.NoError(t, err)

	// Remove chain-b — this is the removal batch.
	authorRoster(t, m, "g1", "member_remove", "chain-ctl", ctlPub, ctlKey, memberChainPayload("chain-b"))

	// The member is removed in the roster projection.
	mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b")
	require.NotNil(t, mem)
	assert.Equal(t, store.GroupMemberRemoved, mem.MemberState)

	// Group-only outbox row purged; pairwise-covered row survives (I10).
	rows, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStatePending, 100)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, r := range rows {
		got[r.MemoryID] = true
	}
	assert.True(t, got["m-hr"], "pairwise-covered outbox row must survive removal")
	assert.False(t, got["m-studio"], "group-only outbox row must be purged on removal")

	// Pairwise consent untouched.
	doms, err := ms.GetSyncDomains(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, []string{"hr"}, doms, "pairwise sync_domains must survive a group removal (I10)")

	// The mandatory anchor fired exactly once for the removal batch.
	assert.Equal(t, 1, anchors, "anchor fires exactly once per removal batch")
}

// TestMemberLeaveEnforcement proves the voluntary-leave path is symmetric: a
// self-signed member_leave purges the leaver's group outbox and fires the anchor.
func TestMemberLeaveEnforcement(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	bPub, bKey, _ := ed25519.GenerateKey(nil)

	anchors := 0
	m.syncAnchorFn = func(_ context.Context, _, _, _ string) error { anchors++; return nil }

	authorRoster(t, m, "g1", "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-b", bPub, bKey, store.GroupRoleFullSync, "pinB"))
	authorRoster(t, m, "g1", "member_activate", "chain-ctl", ctlPub, ctlKey, memberChainPayload("chain-b"))

	seedCommitted(t, ms, "m-studio", "studio", "group fan-out fact")
	_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "m-studio")
	require.NoError(t, err)

	// chain-b authors its OWN self-signed leave (author == member, signed by bKey).
	authorRoster(t, m, "g1", "member_leave", "chain-b", bPub, bKey, memberChainPayload("chain-b"))

	mem, _ := ms.GetSyncGroupMember(ctx, "g1", "chain-b")
	require.NotNil(t, mem)
	assert.Equal(t, store.GroupMemberLeft, mem.MemberState)

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, counts, "a group-only leaver has all its outbox purged")
	assert.Equal(t, 1, anchors, "leave fires the anchor exactly once")
}

// TestEjectedMemberPushRejected proves D1: once a member is removed, its GROUP
// push auto-rejects at Gate 4 — the receiver's effective consent no longer
// contains the group domain (GroupSharedDomains requires active membership on
// both sides). No new gate is needed; it falls out of effectiveConsent.
func TestEjectedMemberPushRejected(t *testing.T) {
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet) // localChainID == "chain-local"

	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberRemoved, "")
	seedGroupDomain(t, ms, "g1", "studio", "chain-local", 0)

	peer := testPeer(2, "*") // ChainID chain-b, treaty allows everything
	item := syncItem("m-x", "studio", "ex-member push")
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[0].Outcome,
		"an ejected member's group push is auto-rejected at receiver consent")
	assert.EqualValues(t, 0, comet.calls.Load(), "a rejected push never broadcasts")
}

// TestRejoinRetainsOriginNoReadmit proves D3 (§10 row 6): sync_origin is retained
// across removal (the group-scoped purge never touches it), so a duplicate inbound
// copy of a pre-removal item replays at Gate 6 as a DUPLICATE — never a second
// broadcast / second local memory.
func TestRejoinRetainsOriginNoReadmit(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)

	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	// Admit an item -> one broadcast, one sync_origin row.
	item := syncItem("m-1", "hr", "pre-removal fact")
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	require.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome)
	require.EqualValues(t, 1, comet.calls.Load())

	// Removal hygiene must NOT touch sync_origin (retained across removal/rejoin).
	require.NoError(t, ms.PurgeGroupSyncPeerState(ctx, "chain-b"))
	prior, err := ms.GetSyncOrigin(ctx, "chain-b", "m-1")
	require.NoError(t, err)
	require.NotEmpty(t, prior.LocalMemoryID)

	// A duplicate inbound copy (rejoin backfill) replays as duplicate, no re-admit.
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeDuplicate, resp.Results[0].Outcome)
	assert.EqualValues(t, 1, comet.calls.Load(), "no second broadcast on a retained-origin duplicate")
}

// TestDomainRemovalAnchored proves the REM-1 fix (§5.5): a domain_remove is anchored on
// its OWN domain:<tag> sub-chain head — NOT the (unchanged) roster head — so the on-chain
// anti-truncation guarantee actually covers domain removals. domain_add does not anchor.
func TestDomainRemovalAnchored(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)

	var subchains []string
	m.syncAnchorFn = func(_ context.Context, groupID, subchain, head string) error {
		assert.Equal(t, "g1", groupID)
		assert.NotEmpty(t, head)
		subchains = append(subchains, subchain)
		return nil
	}

	// Bring chain-owner to active (controller-authored roster; no anchor — not removals).
	authorRoster(t, m, "g1", "member_invite", "chain-ctl", ctlPub, ctlKey,
		signedMemberInvitePayload(t, "g1", "chain-owner", ownerPub, ownerKey, store.GroupRoleFullSync, "pinO"))
	authorRoster(t, m, "g1", "member_activate", "chain-ctl", ctlPub, ctlKey, memberChainPayload("chain-owner"))

	// The owner establishes then removes "studio" on its per-domain sub-chain.
	sub := DomainSubchain("studio")
	d0 := mustEntry(t, "g1", sub, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey,
		domainAddPayload("studio", "chain-owner", 0))
	attachControllerSignature(&d0, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if n, ingestErr := ingestRoster(t, m, ms, "g1", sub, d0); ingestErr != nil || n != 1 {
		t.Fatalf("controller-approved domain_add: n=%d err=%v", n, ingestErr)
	}
	require.Empty(t, subchains, "domain_add must not anchor (not a removal)")

	_, err := m.AppendGroupJournalEntry(ctx, "g1", sub, "domain_remove", "chain-owner", ownerPub, ownerKey,
		domainRemovePayload("studio"))
	require.NoError(t, err)

	// The domain was stamped removed, and its OWN sub-chain head was anchored.
	doms, _ := ms.ListSyncGroupDomains(ctx, "g1", true)
	assert.Empty(t, doms, "studio must be removed from the active set")
	assert.Equal(t, []string{sub}, subchains, "domain_remove anchors its domain:<tag> sub-chain head, not the roster head")
}

// TestDomainRemoveSubchainBindingIngest proves the round-2 fix: a domain_remove whose
// payload domain_tag does NOT match the sub-chain it lands on is SKIPPED on ingest — an
// owner of domain A cannot remove an unowned domain B by authoring on domain:A with
// payload B (so the §5.5 anchor, the authz, and the acted-on tag all agree).
func TestDomainRemoveSubchainBindingIngest(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	ctlPub, ctlKey := seedGroup(t, ms, "g1", "chain-ctl")
	ownerPub, ownerKey, _ := ed25519.GenerateKey(nil)
	require.NoError(t, ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
		GroupID: "g1", MemberChainID: "chain-owner", Role: store.GroupRoleFullSync,
		MemberState: store.GroupMemberActive, MemberAgentPubkey: hex.EncodeToString(ownerPub),
	}))
	// chain-owner establishes domain "a" on its own sub-chain.
	subA := DomainSubchain("a")
	d0 := mustEntry(t, "g1", subA, 0, "", "domain_add", "chain-owner", ownerPub, ownerKey, domainAddPayload("a", "chain-owner", 0))
	attachControllerSignature(&d0, effectiveControllerEpoch(""), "chain-ctl", ctlPub, ctlKey)
	if n, err := ingestRoster(t, m, ms, "g1", subA, d0); err != nil || n != 1 {
		t.Fatalf("domain_add: n=%d err=%v", n, err)
	}
	// A domain_remove on domain:a but with payload tag "b" — authentically signed by A's
	// owner (resolve/verify PASS), but apply SKIPS it: the payload tag != the sub-chain.
	bad := mustEntry(t, "g1", subA, 1, d0.EntryHash, "domain_remove", "chain-owner", ownerPub, ownerKey, domainRemovePayload("b"))
	if n, err := ingestRoster(t, m, ms, "g1", subA, bad); err != nil || n != 1 {
		t.Fatalf("mismatched domain_remove must be logged+skipped, not error: n=%d err=%v", n, err)
	}
	// Domain "a" remains ACTIVE — the cross-domain removal did nothing.
	active, _ := ms.ListSyncGroupDomains(ctx, "g1", true)
	if len(active) != 1 || active[0].DomainTag != "a" {
		t.Fatalf("domain a must remain active after a mismatched domain_remove: %+v", active)
	}
}
