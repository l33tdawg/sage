package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// ---- v11.8 build-step 6 test helpers ----

func seedGroupMember(t *testing.T, ms *store.SQLiteStore, groupID, chain, role, state, pubHex string) {
	t.Helper()
	require.NoError(t, ms.UpsertSyncGroupMember(context.Background(), store.SyncGroupMember{
		GroupID: groupID, MemberChainID: chain, MemberAgentPubkey: pubHex,
		Role: role, MemberState: state, CAPin: "pin",
	}))
}

func seedGroupDomain(t *testing.T, ms *store.SQLiteStore, groupID, domain, owner string, maxClearance int) {
	t.Helper()
	require.NoError(t, ms.UpsertSyncGroupDomain(context.Background(), store.SyncGroupDomain{
		GroupID: groupID, DomainTag: domain, OwnerChainID: owner, MaxClearance: maxClearance,
	}))
}

// TestSyncWatcherGroupStarFanout proves §9.1: an owner's NATIVE memory in a
// shared group domain is enqueued once per ACTIVE full-sync member (never the
// owner, never a selective-sync member), and §9.4: the native memory is never
// tagged a synced copy by the per-member fan-out.
func TestSyncWatcherGroupStarFanout(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t) // localChainID == "chain-local"

	// A group where chain-local OWNS domain "studio"; three peers hold it.
	seedGroupDomain(t, ms, "g1", "studio", "chain-local", 0)
	localID := hex.EncodeToString(m.agentPub)
	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberActive, localID)
	peerIDs := map[string]string{}
	for _, chain := range []string{"chain-b", "chain-c", "chain-d"} {
		pub, _, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		peerIDs[chain] = hex.EncodeToString(pub)
	}
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, peerIDs["chain-b"])
	seedGroupMember(t, ms, "g1", "chain-c", store.GroupRoleFullSync, store.GroupMemberActive, peerIDs["chain-c"])
	seedGroupMember(t, ms, "g1", "chain-d", store.GroupRoleSelectiveSync, store.GroupMemberActive, peerIDs["chain-d"])
	// Trust edges (fan-out re-gates clearance/scope against the pairwise edge).
	seedDrainAgreement(t, bs, "chain-b", 2, "studio")
	seedDrainAgreement(t, bs, "chain-c", 2, "studio")
	seedDrainAgreement(t, bs, "chain-d", 2, "studio")
	for _, chain := range []string{"chain-b", "chain-c", "chain-d"} {
		agreement := mustDrainAgreement(t, m, chain)
		require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{
			RemoteChainID: chain, Role: "host", ControllerChainID: m.localChainID,
			ControllerAgentID: localID, PeerAgentID: peerIDs[chain], PolicyEpoch: "legacy-" + chain,
			RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey), PolicyVersion: 1,
		}))
		require.NoError(t, ms.ActivateSyncControl(ctx, chain, "legacy-"+chain))
	}

	seedCommitted(t, ms, "m-native", "studio", "owner authored fact")
	m.syncNudge = make(chan struct{}, 1)
	m.SyncWatcher()([]string{"m-native"})

	for _, member := range []string{"chain-b", "chain-c"} {
		counts, err := ms.CountSyncOutboxByState(ctx, member)
		require.NoError(t, err)
		assert.Equal(t, 1, counts[store.SyncStatePending], "star fan-out to %s", member)
	}
	// Owner never receives its own memory; selective-sync member is fail-closed.
	for _, excluded := range []string{"chain-local", "chain-d"} {
		counts, err := ms.CountSyncOutboxByState(ctx, excluded)
		require.NoError(t, err)
		assert.Empty(t, counts, "no fan-out to %s", excluded)
	}

	// §9.4 native invariant: fan-out never flips the native memory into a copy.
	isCopy, err := ms.IsSyncedCopy(ctx, "m-native")
	require.NoError(t, err)
	assert.False(t, isCopy, "a locally-authored memory must never be tagged a synced copy")
}

// TestSyncWatcherGroupFanoutSkipsReceivedCopy proves I7 on the hot path: a memory
// WITH a sync_origin row (a received copy) is never fanned out to group members.
func TestSyncWatcherGroupFanoutSkipsReceivedCopy(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedGroupDomain(t, ms, "g1", "studio", "chain-local", 0)
	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedDrainAgreement(t, bs, "chain-b", 2, "studio")

	seedCommitted(t, ms, "m-copy", "studio", "a received copy")
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "orig-1", LocalMemoryID: "m-copy",
		DomainTag: "studio", Outcome: store.SyncOutcomeAdmitted,
	}))
	m.syncNudge = make(chan struct{}, 1)
	m.SyncWatcher()([]string{"m-copy"})

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, counts, "a received copy must never be re-fanned to any group member")
}

// TestSyncDigestGroupHardGate proves §9.2: the group digest serves ANY origin's
// admitted ids for a shared domain, rejects non-shared domains generically,
// clamps ConsentedDomains to the intersection, and (must-fix #12) restricts
// classified-domain backfill serving to the domain owner.
func TestSyncDigestGroupHardGate(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet) // localChainID == "chain-local"
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peerID := hex.EncodeToString(peerPub)
	chainCPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	chainCID := hex.EncodeToString(chainCPub)
	chainXPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	chainXID := hex.EncodeToString(chainXPub)

	// chain-local shares "studio" (owned by chain-c) and "band" (owned locally)
	// with the requester chain-b; "secret" is classified and owned by chain-c.
	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(m.agentPub))
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, peerID)
	seedGroupMember(t, ms, "g1", "chain-c", store.GroupRoleFullSync, store.GroupMemberActive, chainCID)
	seedGroupDomain(t, ms, "g1", "studio", "chain-c", 0)
	seedGroupDomain(t, ms, "g1", "band", "chain-local", 0)
	seedGroupDomain(t, ms, "g1", "secret", "chain-c", 3) // classified, NOT owned locally

	// Admitted ledger holds ids from MULTIPLE origins: chain-c (a member) and chain-b in
	// shared domains; chain-x is NOT a group member; sec-1 sits in the classified "secret".
	for _, o := range []store.SyncOrigin{
		{OriginChainID: "chain-c", OriginAgentPubkey: chainCID, OriginMemoryID: "s-01", DomainTag: "studio", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-1", OriginSig: make([]byte, 64)},
		{OriginChainID: "chain-x", OriginAgentPubkey: chainXID, OriginMemoryID: "s-02", DomainTag: "studio.public", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-2", OriginSig: make([]byte, 64)},
		{OriginChainID: "chain-b", OriginAgentPubkey: peerID, OriginMemoryID: "b-99", DomainTag: "band", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-3", OriginSig: make([]byte, 64)},
		{OriginChainID: "chain-c", OriginAgentPubkey: chainCID, OriginMemoryID: "sec-1", DomainTag: "secret", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-4", OriginSig: make([]byte, 64)},
	} {
		require.NoError(t, ms.RecordSyncOrigin(ctx, o))
	}

	peer := &peerIdentity{ChainID: "chain-b", AgentID: peerID,
		Agreement: &store.CrossFedRecord{RemoteChainID: "chain-b", MaxClearance: 2, AllowedDomains: []string{"*"}, Status: "active"}}
	bindInboundGroupPeer(t, m, ms, peer, "guest")

	// Group digest for a SHARED domain: serves any MEMBER origin's ids, subtree-matched.
	// chain-c (a member) is served; chain-x's id is EXCLUDED because chain-x is not a
	// group member (must-fix #3 — no cross-group provenance leak).
	_, resp := digestAs(t, m, peer, SyncDigestRequest{GroupID: "g1", Domain: "studio"})
	require.NotNil(t, resp)
	assert.True(t, resp.Consented)
	assert.ElementsMatch(t, []string{"s-01"}, resp.OriginMemoryIDs, "serves a member origin's ids; a non-member origin (chain-x) is excluded")
	// Both are full-sync, so the shared intersection is every active group domain
	// (each already knows these domains — not an I5 leak). "finance" (below) is
	// NOT a group domain, so it never appears.
	assert.ElementsMatch(t, []string{"band", "secret", "studio"}, resp.ConsentedDomains, "clamped to the group intersection")

	// A domain the requester does NOT share -> generic 403, no disclosure.
	rr, _ := digestAs(t, m, peer, SyncDigestRequest{GroupID: "g1", Domain: "finance"})
	assert.Equal(t, 403, rr.Code)

	// Classified domain not owned locally -> owner-only serve (star, must-fix #12): a
	// non-owner serves NOTHING for it (sec-1 is filtered out), returned as an empty 200
	// rather than a distinguishing 403 (I5: indistinguishable from an owner with no items).
	_, sResp := digestAs(t, m, peer, SyncDigestRequest{GroupID: "g1", Domain: "secret"})
	require.NotNil(t, sResp)
	assert.Empty(t, sResp.OriginMemoryIDs, "a non-owner must not serve backfill for a classified group domain")

	// A non-member requester -> generic 403.
	stranger := &peerIdentity{ChainID: "chain-z", AgentID: "z",
		Agreement: &store.CrossFedRecord{RemoteChainID: "chain-z", MaxClearance: 2, AllowedDomains: []string{"*"}, Status: "active"}}
	rr, _ = digestAs(t, m, stranger, SyncDigestRequest{GroupID: "g1", Domain: "studio"})
	assert.Equal(t, 403, rr.Code)
}

// TestSyncDigestPairwiseStillWorks confirms the legacy (no group_id) digest path
// is unchanged — it still filters to the requester's OWN origin ids.
func TestSyncDigestPairwiseUnchanged(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-b", OriginMemoryID: "m-1", DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-1"}))
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "m-2", DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-2"}))

	_, resp := digestAs(t, m, peer, SyncDigestRequest{Domain: "hr"})
	require.NotNil(t, resp)
	assert.Equal(t, []string{"m-1"}, resp.OriginMemoryIDs, "pairwise digest still filters to the requester's own origin ids")
}

// TestSyncPushGate65Suppressed proves §10 Gate 6.5: a locally-suppressed origin
// memory is terminally SUPPRESSED on the receive path — not recorded, not
// broadcast — from the owner star / re-enqueue path.
func TestSyncPushGate65Suppressed(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	// A prior local delete recorded a memory-scope local_suppress tombstone.
	require.NoError(t, ms.InsertSyncTombstone(ctx, store.SyncTombstone{
		GroupID: "g1", Scope: store.TombstoneScopeMemory, Enforcement: store.TombstoneEnforceLocalSuppress,
		OriginChainID: "chain-b", OriginMemoryID: "m-deleted",
	}))

	item := syncItem("m-deleted", "hr", "should not resurrect")
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	// I5: the sovereign-delete outcome is collapsed to a generic terminal reason on the
	// wire — the pushing peer learns "not admitted", never that a tombstone exists.
	assert.Equal(t, SyncOutcomeRejectedNotAdmitted, resp.Results[0].Outcome)
	assert.EqualValues(t, 0, comet.calls.Load(), "a suppressed item must never broadcast")

	_, err := ms.GetSyncOrigin(ctx, "chain-b", "m-deleted")
	require.Error(t, err, "a suppressed item must never be recorded to sync_origin")
	isCopy, err := ms.IsSyncedCopy(ctx, syncMemoryID("chain-b", "m-deleted"))
	require.NoError(t, err)
	assert.False(t, isCopy)
}

// TestSyncPushRelayOriginAuth proves §9.2 must-fix #1/#3: a peer may relay ANOTHER
// origin's item only within a shared group, the origin_sig is verified against the
// origin's ROSTER key (not the relayer's), an unsigned relay is rejected, and an
// unauthorized third-chain relay is refused at the door.
func TestSyncPushRelayOriginAuth(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet) // localChainID == "chain-local"

	// The origin (chain-c) is a group member with a pinned roster key.
	originPub, originPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	relayerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	relayerID := hex.EncodeToString(relayerPub)
	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(m.agentPub))
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, relayerID) // relayer
	seedGroupMember(t, ms, "g1", "chain-c", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(originPub))
	seedGroupDomain(t, ms, "g1", "studio", "chain-c", 0)

	// The relayer is chain-b; the item's origin is chain-c.
	relayer := &peerIdentity{ChainID: "chain-b", AgentID: relayerID,
		Agreement: &store.CrossFedRecord{RemoteChainID: "chain-b", MaxClearance: 2, AllowedDomains: []string{"*"}, Status: "active"}}
	bindInboundGroupPeer(t, m, ms, relayer, "guest")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"studio"}))

	relayed := SyncItem{OriginChainID: "chain-c", OriginMemoryID: "m-relayed", Domain: "studio",
		Classification: 1, Content: "relayed fact", ContentHash: contentHashHex("relayed fact")}
	relayed.OriginSig = signOriginSig(originPriv, &relayed)
	comet.after = func() {
		localID := syncMemoryID("chain-c", "m-relayed")
		sum := sha256.Sum256([]byte(relayed.Content))
		_ = seedCommittedMemory(ctx, ms, localID, relayed.Domain, relayed.Content, sum[:])
	}
	_, resp := pushAs(t, m, relayer, SyncPushRequest{Items: []SyncItem{relayed}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome, "a group-authorized, origin-signed relay is admitted")

	// A forged relay (origin_sig corrupted) is rejected against the roster key.
	forged := SyncItem{OriginChainID: "chain-c", OriginMemoryID: "m-forged", Domain: "studio",
		Classification: 1, Content: "forged fact", ContentHash: contentHashHex("forged fact")}
	forged.OriginSig = signOriginSig(originPriv, &forged)
	forged.OriginSig[0] ^= 0xFF
	_, resp = pushAs(t, m, relayer, SyncPushRequest{Items: []SyncItem{forged}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedOriginSig, resp.Results[0].Outcome)

	// An UNSIGNED relay is unauthenticatable -> rejected (only pairwise-origin
	// empty sigs are compat-accepted).
	unsigned := SyncItem{OriginChainID: "chain-c", OriginMemoryID: "m-unsigned", Domain: "studio",
		Classification: 1, Content: "unsigned fact", ContentHash: contentHashHex("unsigned fact")}
	_, resp = pushAs(t, m, relayer, SyncPushRequest{Items: []SyncItem{unsigned}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedOriginSig, resp.Results[0].Outcome)

	// A third-chain relay NOT authorized by any shared group -> whole-batch 400.
	stranger := SyncItem{OriginChainID: "chain-z", OriginMemoryID: "m-z", Domain: "studio",
		Classification: 1, Content: "laundered", ContentHash: contentHashHex("laundered")}
	stranger.OriginSig = signOriginSig(originPriv, &stranger)
	rr, _ := pushAs(t, m, relayer, SyncPushRequest{Items: []SyncItem{stranger}})
	assert.Equal(t, 400, rr.Code, "an unauthorized third-chain relay is refused at the door")
}

// TestSyncReconcileSkipsWhileResyncing proves D5 (SF2, §10 row 6): while THIS node
// is resyncing in a group shared with the peer, the GROUP backfill pass is paused
// and a group-owned domain is NOT re-originated — so a suppressed memory cannot
// resurrect mid-rebuild. (The pairwise-continues property is TestSyncReconcile-
// PairwiseContinuesWhileResyncing in the step-7 suite.)
func TestSyncReconcileSkipsWhileResyncing(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "studio")

	// Local is REBUILDING (resyncing); the peer is an active member of the group.
	seedGroupMember(t, ms, "g1", "chain-local", store.GroupRoleFullSync, store.GroupMemberResyncing, "")
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, "")
	seedGroupDomain(t, ms, "g1", "studio", "chain-b", 0) // owned by the PEER, not local
	seedCommitted(t, ms, "m-cand", "studio", "would-be backfill")

	groupCalled := false
	m.syncDigestFn = func(_ context.Context, _ string, req *SyncDigestRequest) (*SyncDigestResponse, error) {
		if req.GroupID != "" {
			groupCalled = true
		}
		return &SyncDigestResponse{}, nil
	}
	m.reconcilePeer(ctx, ms, mustAgreement(t, m, "chain-b"), []string{"studio"})

	assert.False(t, groupCalled, "no GROUP backfill digest while resyncing")
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, counts, "no group-owned backfill enqueued while resyncing")
}
