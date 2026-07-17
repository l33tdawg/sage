package federation

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

const freshTrustV3Epoch = "fresh-trust-v3-epoch"

// activateFreshTrustOnlyV3 reproduces the post-JOIN state: an exact frozen peer
// identity and v3 policy marker, but no tx-33 domains, no direct Publish, and no
// Subscribe. Group journal projections must work independently from that empty
// direct lane.
func activateFreshTrustOnlyV3(t *testing.T, m *Manager, ms *store.SQLiteStore,
	agreement *store.CrossFedRecord, peerAgentID, role string,
) {
	t.Helper()
	control := store.SyncControl{
		RemoteChainID: agreement.RemoteChainID,
		Role:          role,
		PeerAgentID:   peerAgentID,
		PolicyEpoch:   freshTrustV3Epoch,
		RemoteCAPin:   hex.EncodeToString(agreement.PeerPubKey),
	}
	if role == "host" {
		control.ControllerChainID = m.localChainID
		control.ControllerAgentID = hex.EncodeToString(m.agentPub)
	} else {
		control.ControllerChainID = agreement.RemoteChainID
		control.ControllerAgentID = peerAgentID
	}
	require.NoError(t, ms.PrepareSyncControl(context.Background(), control))
	require.NoError(t, ms.ActivateSyncControl(context.Background(), agreement.RemoteChainID, control.PolicyEpoch))
	got, err := ms.GetSyncControl(context.Background(), agreement.RemoteChainID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, SyncPolicyVersionPeerRBAC, got.PolicyVersion)
	assert.Zero(t, got.Revision)
}

func TestFreshTrustOnlyV3GroupOwnerFanoutIgnoresEmptyDirectLane(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2) // trust-only tx-33 scope is empty
	agreement := mustDrainAgreement(t, m, "chain-b")
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	activateFreshTrustOnlyV3(t, m, ms, agreement, hex.EncodeToString(peerPub), "host")

	seedGroupDomain(t, ms, "g1", "studio", m.localChainID, 0)
	seedGroupMember(t, ms, "g1", m.localChainID, store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(m.agentPub))
	seedGroupMember(t, ms, "g1", "chain-b", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(peerPub))
	seedCommitted(t, ms, "m-group", "studio.project", "journal-authorized group copy")
	seedCommitted(t, ms, "m-private", "private", "not a group domain")
	targets, err := ms.ListGroupFanoutTargets(ctx, m.localChainID, "studio.project")
	require.NoError(t, err)
	require.Len(t, targets, 1)
	localShares, err := ms.MemberSharesGroupDomain(ctx, "g1", m.localChainID, "studio.project")
	require.NoError(t, err)
	peerShares, err := ms.MemberSharesGroupDomain(ctx, "g1", "chain-b", "studio.project")
	require.NoError(t, err)
	require.True(t, localShares)
	require.True(t, peerShares)
	groupChains, err := ms.ListActiveGroupMemberChains(ctx)
	require.NoError(t, err)
	require.Contains(t, groupChains, "chain-b")
	domain, classification, ok := m.syncMetaFor(ctx, ms, "m-group")
	require.True(t, ok)
	require.Equal(t, "studio.project", domain)
	require.LessOrEqual(t, classification, int(agreement.MaxClearance))

	direct, v3, err := m.pairwiseEgressPolicy(ctx, ms, agreement)
	require.NoError(t, err)
	assert.True(t, v3)
	assert.Empty(t, direct, "fresh trust must have no direct Copy capability")
	require.True(t, syncEgressDomainAllowed(agreement, direct, v3, true, false, domain))

	m.onCommitted([]string{"m-group", "m-private"})
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStatePending], "only the owner-authorized group domain is enqueued")

	var pushed []SyncItem
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushed = append(pushed, req.Items...)
		results := make([]SyncItemResult, len(req.Items))
		for i := range req.Items {
			results[i] = SyncItemResult{OriginMemoryID: req.Items[i].OriginMemoryID, Outcome: SyncOutcomeAccepted}
		}
		return &SyncPushResponse{Results: results}, nil
	}
	consented, err := m.effectiveConsent(ctx, ms, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, []string{"studio"}, consented)
	m.syncDrain(ctx, ms, agreement, consented)
	require.Len(t, pushed, 1)
	assert.Equal(t, "m-group", pushed[0].OriginMemoryID)

	// A connection Pause is stronger than direct pairwise policy: it must also
	// close group-native fanout to this exact peer without deleting the roster.
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-b", nil)
	require.NoError(t, err)
	_, err = m.SetPeerRBACPaused(ctx, "chain-b", true)
	require.NoError(t, err)
	pausedConsent, err := m.effectiveConsent(ctx, ms, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, pausedConsent)
	seedCommitted(t, ms, "m-paused-group", "studio", "must not cross a paused edge")
	m.onCommitted([]string{"m-paused-group"})
	counts, err = ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Zero(t, counts[store.SyncStatePending], "paused group edge must not enqueue new work")
	_, err = ms.EnqueueSyncOutbox(ctx, "chain-b", "m-paused-group")
	require.NoError(t, err)
	m.syncDrain(ctx, ms, agreement, []string{"studio"}) // deliberately stale caller snapshot
	assert.Len(t, pushed, 1, "paused group edge must fail closed again at drain")
	counts, err = ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStatePending], "pause must return a claimed row to retryable pending")
	assert.Zero(t, counts[store.SyncStateRejected], "temporary pause must never terminally reject queued Copy work")
	_, err = m.SetPeerRBACPaused(ctx, "chain-b", false)
	require.NoError(t, err)
	resumedConsent, err := m.effectiveConsent(ctx, ms, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, []string{"studio"}, resumedConsent, "resume restores the unchanged group roster")

	// Removal closes the journal capability without touching/re-pairing the trust
	// edge. Even a stale/manual outbox row cannot cross the send-time group gate.
	require.NoError(t, ms.SetSyncGroupMemberState(ctx, "g1", "chain-b", store.GroupMemberRemoved, 1))
	seedCommitted(t, ms, "m-after-remove", "studio", "must not reach removed member")
	_, err = ms.EnqueueSyncOutbox(ctx, "chain-b", "m-after-remove")
	require.NoError(t, err)
	m.syncDrain(ctx, ms, agreement, []string{"studio"}) // deliberately stale caller snapshot
	assert.Len(t, pushed, 1, "removed member must receive no further group bytes")
}

func TestFreshTrustOnlyV3GroupOwnerAdmissionIsIndependentAndExact(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peerPub, peerKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peer := testPeer(2) // trust-only tx-33 scope is empty
	peer.AgentID = hex.EncodeToString(peerPub)
	peer.Agreement.PeerPubKey = []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	activateFreshTrustOnlyV3(t, m, ms, peer.Agreement, peer.AgentID, "guest")

	seedGroupDomain(t, ms, "g1", "studio", peer.ChainID, 0)
	seedGroupMember(t, ms, "g1", m.localChainID, store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(m.agentPub))
	seedGroupMember(t, ms, "g1", peer.ChainID, store.GroupRoleFullSync, store.GroupMemberActive, peer.AgentID)

	groupItem := syncItem("m-group-in", "studio.project", "group owner push")
	groupItem.OriginSig = signOriginSig(peerKey, &groupItem)
	comet.after = func() {
		_ = seedCommittedMemory(ctx, ms, syncMemoryID(peer.ChainID, groupItem.OriginMemoryID),
			groupItem.Domain, groupItem.Content, mustDecodeHex(t, groupItem.ContentHash))
	}
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{groupItem}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome,
		"group owner fanout must not require direct Copy/Subscribe or tx-33")

	nonGroup := syncItem("m-not-group", "private", "not shared by any group")
	nonGroup.OriginSig = signOriginSig(peerKey, &nonGroup)
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{nonGroup}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[0].Outcome)

	tooHigh := syncItem("m-too-high", "studio", "above trust ceiling")
	tooHigh.Classification = 3
	tooHigh.OriginSig = signOriginSig(peerKey, &tooHigh)
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{tooHigh}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedClearance, resp.Results[0].Outcome)

	// Membership alone does not turn a holder into an origin for somebody else's
	// domain: only the journal-recorded owner may use the native group lane.
	seedGroupDomain(t, ms, "g1", "other-owned", "chain-c", 0)
	seedGroupMember(t, ms, "g1", "chain-c", store.GroupRoleFullSync, store.GroupMemberActive, "chain-c-key")
	nonOwner := syncItem("m-non-owner", "other-owned", "holder is not owner")
	nonOwner.OriginSig = signOriginSig(peerKey, &nonOwner)
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{nonOwner}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[0].Outcome)

	wrongPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	wrongPeer := *peer
	wrongPeer.AgentID = hex.EncodeToString(wrongPub)
	rr, resp := pushAs(t, m, &wrongPeer, SyncPushRequest{Items: []SyncItem{groupItem}})
	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Nil(t, resp, "a different operator on the same trusted chain is not the frozen peer")

	require.NoError(t, ms.SetSyncGroupMemberState(ctx, "g1", peer.ChainID, store.GroupMemberRemoved, 2))
	afterRemoval := syncItem("m-after-removal", "studio", "removed member push")
	afterRemoval.OriginSig = signOriginSig(peerKey, &afterRemoval)
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{afterRemoval}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[0].Outcome)
	assert.Equal(t, int32(1), comet.calls.Load(), "all denied items stop before consensus")
}

func TestFreshTrustOnlyV3GroupRelayWorksWithoutDirectCopy(t *testing.T) {
	ctx := context.Background()
	xPub, xKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	// Relayer R has a fresh trust-only edge to receiver M.
	r, msR, bsR := newDrainTestManager(t)
	r.localChainID = "chain-r"
	seedDrainAgreement(t, bsR, "chain-m", 2)
	agreement := mustDrainAgreement(t, r, "chain-m")
	mPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	activateFreshTrustOnlyV3(t, r, msR, agreement, hex.EncodeToString(mPub), "host")
	seedRelayGroup(t, msR, "studio", "chain-x", 0, hex.EncodeToString(xPub),
		hex.EncodeToString(r.agentPub), hex.EncodeToString(mPub))

	origin := SyncItem{
		OriginChainID: "chain-x", OriginMemoryID: "x-fresh-v3", OriginCreatedAt: "2026-07-16T00:00:00Z",
		Domain: "studio", Classification: 1, MemoryType: "fact", ConfidenceScore: 0.9,
		Content: "journal-authorized relay", ContentHash: contentHashHex("journal-authorized relay"),
	}
	origin.OriginSig = signOriginSig(xKey, &origin)
	localID := syncMemoryID(origin.OriginChainID, origin.OriginMemoryID)
	require.NoError(t, seedSyncedMirror(ctx, msR, r, localID, origin))
	require.NoError(t, msR.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: origin.OriginChainID, OriginMemoryID: origin.OriginMemoryID,
		OriginAgentPubkey: hex.EncodeToString(xPub),
		OriginCreatedAt:   origin.OriginCreatedAt, LocalMemoryID: localID, DomainTag: origin.Domain,
		Outcome: store.SyncOutcomeAdmitted, OriginSig: origin.OriginSig,
	}))
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", localID, "chain-x")
	require.NoError(t, err)
	direct, v3, err := r.pairwiseEgressPolicy(ctx, msR, agreement)
	require.NoError(t, err)
	require.True(t, v3)
	require.True(t, syncEgressDomainAllowed(agreement, direct, v3, true, true, origin.Domain),
		"an exact group relay is independent from an empty direct lane")

	var pushed []SyncItem
	r.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushed = append(pushed, req.Items...)
		return &SyncPushResponse{Results: []SyncItemResult{{OriginMemoryID: req.Items[0].OriginMemoryID, Outcome: SyncOutcomeAccepted}}}, nil
	}
	consented, err := r.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	assert.Equal(t, []string{"studio"}, consented)
	r.syncDrain(ctx, msR, agreement, consented)
	require.Len(t, pushed, 1)
	assert.Equal(t, "chain-x", pushed[0].OriginChainID)
	relayGroup, relayAllowed, err := r.resolveGroupRelayForAgreement(ctx, msR, agreement,
		"chain-x", hex.EncodeToString(xPub), "studio")
	require.NoError(t, err)
	require.True(t, relayAllowed)
	require.Equal(t, "g1", relayGroup)
	_, err = r.ReplacePeerRBACPolicy(ctx, "chain-m", nil)
	require.NoError(t, err)
	_, err = r.SetPeerRBACPaused(ctx, "chain-m", true)
	require.NoError(t, err)
	_, relayAllowed, err = r.resolveGroupRelayForAgreement(ctx, msR, agreement,
		"chain-x", hex.EncodeToString(xPub), "studio")
	require.NoError(t, err)
	require.False(t, relayAllowed, "pause must close a relayed Copy lane to the exact peer")
	_, err = r.SetPeerRBACPaused(ctx, "chain-m", false)
	require.NoError(t, err)

	// Removal invalidates the exact three-member relay even though the transport
	// trust edge remains active and the caller deliberately supplies stale consent.
	require.NoError(t, msR.SetSyncGroupMemberState(ctx, "g1", "chain-m", store.GroupMemberRemoved, 1))
	second := origin
	second.OriginMemoryID = "x-after-member-remove"
	second.Content = "must not relay to removed member"
	second.ContentHash = contentHashHex(second.Content)
	second.OriginSig = signOriginSig(xKey, &second)
	secondID := syncMemoryID(second.OriginChainID, second.OriginMemoryID)
	require.NoError(t, seedSyncedMirror(ctx, msR, r, secondID, second))
	require.NoError(t, msR.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: second.OriginChainID, OriginMemoryID: second.OriginMemoryID,
		OriginAgentPubkey: hex.EncodeToString(xPub),
		OriginCreatedAt:   second.OriginCreatedAt, LocalMemoryID: secondID, DomainTag: second.Domain,
		Outcome: store.SyncOutcomeAdmitted, OriginSig: second.OriginSig,
	}))
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", secondID, "chain-x")
	require.NoError(t, err)
	r.syncDrain(ctx, msR, agreement, []string{"studio"})
	assert.Len(t, pushed, 1, "removed member cannot receive a group relay")
}

func TestFreshTrustOnlyV3GroupRelayAdmissionRequiresExactGroup(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	m.localChainID = "chain-m"
	relayerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	xPub, xKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peer := &peerIdentity{
		ChainID: "chain-r", AgentID: hex.EncodeToString(relayerPub),
		Agreement: &store.CrossFedRecord{
			RemoteChainID: "chain-r", MaxClearance: 2, Status: "active",
			PeerPubKey: []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa"),
		},
	}
	activateFreshTrustOnlyV3(t, m, ms, peer.Agreement, peer.AgentID, "guest")
	seedRelayGroup(t, ms, "studio", "chain-x", 0, hex.EncodeToString(xPub),
		hex.EncodeToString(relayerPub), hex.EncodeToString(m.agentPub))

	relayed := SyncItem{
		OriginChainID: "chain-x", OriginMemoryID: "x-inbound-v3", OriginCreatedAt: "2026-07-16T00:00:00Z",
		Domain: "studio", Classification: 1, MemoryType: "fact", ConfidenceScore: 0.9,
		Content: "fresh v3 inbound relay", ContentHash: contentHashHex("fresh v3 inbound relay"),
	}
	relayed.OriginSig = signOriginSig(xKey, &relayed)
	comet.after = func() {
		_ = seedCommittedMemory(ctx, ms, syncMemoryID(relayed.OriginChainID, relayed.OriginMemoryID),
			relayed.Domain, relayed.Content, mustDecodeHex(t, relayed.ContentHash))
	}
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{relayed}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome,
		"exact group relay must not require direct Copy/Subscribe or tx-33")

	outside := relayed
	outside.OriginMemoryID = "x-outside-group"
	outside.Domain = "private"
	outside.Content = "outside group"
	outside.ContentHash = contentHashHex(outside.Content)
	outside.OriginSig = signOriginSig(xKey, &outside)
	rr, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{outside}})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Nil(t, resp)

	require.NoError(t, ms.SetSyncGroupMemberState(ctx, "g1", "chain-x", store.GroupMemberRemoved, 1))
	afterRemoval := relayed
	afterRemoval.OriginMemoryID = "x-removed-origin"
	afterRemoval.Content = "removed origin"
	afterRemoval.ContentHash = contentHashHex(afterRemoval.Content)
	afterRemoval.OriginSig = signOriginSig(xKey, &afterRemoval)
	rr, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{afterRemoval}})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Nil(t, resp)
	assert.Equal(t, int32(1), comet.calls.Load())
}

func TestFreshTrustOnlyV3RelaySelectsExactOriginAcrossSameDomainGroups(t *testing.T) {
	ctx := context.Background()
	xAPub, xAKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	xBPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	mPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	r, ms, bs := newDrainTestManager(t)
	r.localChainID = "chain-r"
	seedDrainAgreement(t, bs, "chain-m", 2)
	agreement := mustDrainAgreement(t, r, "chain-m")
	mID := hex.EncodeToString(mPub)
	rID := hex.EncodeToString(r.agentPub)
	activateFreshTrustOnlyV3(t, r, ms, agreement, mID, "host")

	for _, groupID := range []string{"g1", "g2"} {
		seedGroupDomain(t, ms, groupID, "studio", "chain-x", 0)
		seedGroupMember(t, ms, groupID, "chain-r", store.GroupRoleFullSync, store.GroupMemberActive, rID)
		seedGroupMember(t, ms, groupID, "chain-m", store.GroupRoleFullSync, store.GroupMemberActive, mID)
	}
	// Lexical g1 pins B; g2 pins the actual author A.
	seedGroupMember(t, ms, "g1", "chain-x", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(xBPub))
	seedGroupMember(t, ms, "g2", "chain-x", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(xAPub))

	makeItem := func(originID, content string) SyncItem {
		item := SyncItem{
			OriginChainID: "chain-x", OriginMemoryID: originID,
			OriginCreatedAt: "2026-07-16T00:00:00Z", Domain: "studio",
			Classification: 1, MemoryType: "fact", ConfidenceScore: 0.9,
			Content: content, ContentHash: contentHashHex(content),
		}
		item.OriginSig = signOriginSig(xAKey, &item)
		return item
	}

	good := makeItem("x-key-a", "agent A survives lexical group shadowing")
	resolved, err := r.originVerifyKey(ctx, ms, &peerIdentity{ChainID: "chain-m", AgentID: mID}, &good)
	require.NoError(t, err)
	assert.Equal(t, hex.EncodeToString(xAPub), hex.EncodeToString(resolved),
		"receiver must try every eligible same-domain group and select the signature-matching origin")
	goodLocalID := syncMemoryID(good.OriginChainID, good.OriginMemoryID)
	require.NoError(t, seedSyncedMirror(ctx, ms, r, goodLocalID, good))
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: good.OriginChainID, OriginMemoryID: good.OriginMemoryID,
		OriginCreatedAt: good.OriginCreatedAt, OriginAgentPubkey: hex.EncodeToString(xAPub),
		LocalMemoryID: goodLocalID, DomainTag: good.Domain, Outcome: store.SyncOutcomeAdmitted,
		OriginSig: good.OriginSig,
	}))
	_, err = ms.EnqueueRelayedSyncOutbox(ctx, "chain-m", goodLocalID, good.OriginChainID)
	require.NoError(t, err)

	pushes := 0
	r.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushes++
		return &SyncPushResponse{Results: []SyncItemResult{{
			OriginMemoryID: req.Items[0].OriginMemoryID, Outcome: SyncOutcomeAccepted,
		}}}, nil
	}
	consented, err := r.effectiveConsent(ctx, ms, "chain-m")
	require.NoError(t, err)
	r.syncDrain(ctx, ms, agreement, consented)
	require.Equal(t, 1, pushes, "sender must select g2 by persisted origin A and send once")

	// A persisted key/signature mismatch is rejected at the sender's final gate;
	// receiver-side rejection would be too late because content had crossed.
	bad := makeItem("x-bad-binding", "must never cross the wire")
	badLocalID := syncMemoryID(bad.OriginChainID, bad.OriginMemoryID)
	require.NoError(t, seedSyncedMirror(ctx, ms, r, badLocalID, bad))
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: bad.OriginChainID, OriginMemoryID: bad.OriginMemoryID,
		OriginCreatedAt: bad.OriginCreatedAt, OriginAgentPubkey: hex.EncodeToString(xBPub),
		LocalMemoryID: badLocalID, DomainTag: bad.Domain, Outcome: store.SyncOutcomeAdmitted,
		OriginSig: bad.OriginSig,
	}))
	_, err = ms.EnqueueRelayedSyncOutbox(ctx, "chain-m", badLocalID, bad.OriginChainID)
	require.NoError(t, err)
	r.syncDrain(ctx, ms, agreement, consented)
	assert.Equal(t, 1, pushes, "origin-key mismatch must be rejected before network delivery")
}

func mustDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	b, err := hex.DecodeString(value)
	require.NoError(t, err)
	return b
}
