package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// seedRelayGroup wires one store's roster for the 3-node mesh: origin X, relayer
// R, member M all active full-sync in group g1 carrying `domain` (owner + max
// clearance configurable). X's roster key is pinned so a relayed origin_sig
// verifies against it, never the relayer's.
func seedRelayGroup(t *testing.T, ms *store.SQLiteStore, domain, owner string, maxClearance int, xPubHex, rPubHex, mPubHex string) {
	t.Helper()
	seedGroupDomain(t, ms, "g1", domain, owner, maxClearance)
	seedGroupMember(t, ms, "g1", "chain-x", store.GroupRoleFullSync, store.GroupMemberActive, xPubHex)
	seedGroupMember(t, ms, "g1", "chain-r", store.GroupRoleFullSync, store.GroupMemberActive, rPubHex)
	seedGroupMember(t, ms, "g1", "chain-m", store.GroupRoleFullSync, store.GroupMemberActive, mPubHex)
}

// TestMeshPullRelayEndToEnd is the load-bearing D4 proof (docs §9.2): a rejoining
// member M backfills origin X's item via relayer R end-to-end. X never talks to M;
// R holds an admitted copy of X's memory (with X's persisted origin_sig), R's
// reconcile enqueues it as a RELAYED row, R's drain re-serves the stored sig
// verbatim, and M admits it via the group relay path (ResolveGroupRelay +
// roster-key origin_sig verify + Gate 6.5) — proving the relayer is a pure cache
// that cannot forge or re-attribute.
func TestMeshPullRelayEndToEnd(t *testing.T) {
	ctx := context.Background()

	// X's authoring key — the roster-pinned identity a relayed sig must verify against.
	xPub, xPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	xPubHex := hex.EncodeToString(xPub)

	// The ORIGINAL item X authored. Its fields must equal what R reconstructs from
	// its local copy at drain, so the persisted origin_sig re-verifies at M.
	content := "x authored studio fact"
	sum := sha256.Sum256([]byte(content))
	originItem := SyncItem{
		OriginChainID:   "chain-x",
		OriginMemoryID:  "x-mem-1",
		OriginCreatedAt: "2026-02-02T00:00:00Z",
		Domain:          "studio",
		Classification:  1,
		MemoryType:      "fact",
		ConfidenceScore: 0.9,
		Content:         content,
		ContentHash:     hex.EncodeToString(sum[:]),
	}
	originItem.OriginSig = signOriginSig(xPriv, &originItem)
	localID := syncMemoryID("chain-x", "x-mem-1")

	// ---- Relayer R (holds the copy; drains toward M) ----
	R, msR, bsR := newDrainTestManager(t)
	R.localChainID = "chain-r"
	R.syncNudge = make(chan struct{}, 1)
	comet := &scriptedComet{responses: []string{cometOK}}
	M, msM := newSyncTestManager(t, comet)
	M.localChainID = "chain-m"
	relayerID := hex.EncodeToString(R.agentPub)
	memberID := hex.EncodeToString(M.agentPub)
	seedRelayGroup(t, msR, "studio", "chain-x", 0, xPubHex, relayerID, memberID)
	seedRelayGroup(t, msM, "studio", "chain-x", 0, xPubHex, relayerID, memberID)
	seedDrainAgreement(t, bsR, "chain-m", 2, "studio")
	require.NoError(t, msR.SetSyncDomains(ctx, "chain-m", []string{"studio"}))
	agreement, err := R.ActiveAgreement("chain-m")
	require.NoError(t, err)
	require.NoError(t, msR.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-m", Role: "host", ControllerChainID: R.localChainID,
		ControllerAgentID: relayerID, PeerAgentID: memberID, PolicyEpoch: "legacy-group",
		RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}))
	require.NoError(t, msR.ActivateSyncControl(ctx, "chain-m", "legacy-group"))
	// R holds X's admitted copy as an immutable local memory + provenance ledger row.
	require.NoError(t, seedSyncedMirror(ctx, msR, R, localID, originItem))
	require.NoError(t, msR.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "x-mem-1", OriginCreatedAt: originItem.OriginCreatedAt,
		OriginAgentPubkey: xPubHex,
		LocalMemoryID:     localID, DomainTag: "studio", Outcome: store.SyncOutcomeAdmitted,
		OriginSig: originItem.OriginSig,
	}))
	// R's digest seam reports M holds nothing (a fresh rejoining member).
	R.syncDigestFn = func(_ context.Context, _ string, _ *SyncDigestRequest) (*SyncDigestResponse, error) {
		return &SyncDigestResponse{}, nil
	}

	// ---- Member M (admits via the real handleSyncPush + a scripted broadcast) ----
	require.NoError(t, msM.SetSyncDomains(ctx, "chain-r", []string{"studio"}))
	comet.after = func() {
		lid := syncMemoryID("chain-x", "x-mem-1")
		_ = seedCommittedMemory(ctx, msM, lid, "studio", content, sum[:])
	}

	// R's push seam delivers into M's real receive path, authenticated as peer R.
	peerR := &peerIdentity{
		ChainID: "chain-r",
		AgentID: hex.EncodeToString(R.agentPub),
		Agreement: &store.CrossFedRecord{
			RemoteChainID: "chain-r", MaxClearance: 2, AllowedDomains: []string{"studio"}, Status: "active",
		},
	}
	bindInboundGroupPeer(t, M, msM, peerR, "guest")
	R.syncPushFn = func(_ context.Context, chain string, req *SyncPushRequest) (*SyncPushResponse, error) {
		assert.Equal(t, "chain-m", chain)
		body, mErr := json.Marshal(req)
		require.NoError(t, mErr)
		httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/push", bytes.NewReader(body))
		httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peerR))
		rr := httptest.NewRecorder()
		M.handleSyncPush(rr, httpReq)
		require.Equal(t, http.StatusOK, rr.Code)
		var resp SyncPushResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		return &resp, nil
	}

	// ---- 1. Reconcile: R discovers M lacks X's item and enqueues a RELAYED row ----
	consented, err := R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	R.reconcilePeer(ctx, msR, agreement, consented)

	counts, err := msR.CountSyncOutboxByState(ctx, "chain-m")
	require.NoError(t, err)
	require.Equal(t, 1, counts[store.SyncStatePending], "reconcile must enqueue exactly one relayed backfill row")
	// It is a RELAYED row (origin_chain_id carries X, not R's local origin).
	claimedPeek, err := msR.ClaimDueSyncOutbox(ctx, "chain-m", 10)
	require.NoError(t, err)
	require.Len(t, claimedPeek, 1)
	assert.Equal(t, "chain-x", claimedPeek[0].OriginChainID, "row must carry the ORIGINAL origin chain")
	assert.Equal(t, localID, claimedPeek[0].MemoryID, "row memory_id points at the LOCAL copy for content")
	_, err = msR.ResetDeliveringToPending(ctx)
	require.NoError(t, err)

	// ---- 2. Drain: R re-serves the stored sig; M admits end-to-end ----
	R.syncDrain(ctx, msR, agreement, consented)

	// R settled the row delivered.
	counts, err = msR.CountSyncOutboxByState(ctx, "chain-m")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateDelivered], "relayed row delivered after M admitted it")

	// M admitted the copy end-to-end and persisted X's origin_sig VERBATIM (the
	// relay chain stays authentic: M could itself relay onward with the same sig).
	mOrigin, err := msM.GetSyncOrigin(ctx, "chain-x", "x-mem-1")
	require.NoError(t, err)
	assert.Equal(t, store.SyncOutcomeAdmitted, mOrigin.Outcome)
	assert.Equal(t, syncMemoryID("chain-x", "x-mem-1"), mOrigin.LocalMemoryID)
	assert.Equal(t, originItem.OriginSig, mOrigin.OriginSig, "M must persist the ORIGIN's sig, not the relayer's")
}

// TestTrustOnlyV3MeshRelayUsesIndependentExactGroupRBAC is the fresh-JOIN
// compatibility proof. Both transport agreements have the mandatory empty
// tx-33 envelope. A three-node X -> R -> M backfill uses the exact X/R/M group
// projection independently of direct Copy/Subscribe; removing a group member
// is the revocation gate.
func TestTrustOnlyV3MeshRelayUsesIndependentExactGroupRBAC(t *testing.T) {
	ctx := context.Background()
	xPub, xPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	xPubHex := hex.EncodeToString(xPub)

	R, msR, bsR := newDrainTestManager(t)
	R.localChainID = "chain-r"
	R.syncNudge = make(chan struct{}, 1)
	comet := &scriptedComet{responses: []string{cometOK}}
	M, msM := newSyncTestManager(t, comet)
	M.localChainID = "chain-m"
	memberID := hex.EncodeToString(M.agentPub)
	relayerID := hex.EncodeToString(R.agentPub)
	seedRelayGroup(t, msR, "studio", "chain-x", 0, xPubHex, relayerID, memberID)
	seedRelayGroup(t, msM, "studio", "chain-x", 0, xPubHex, relayerID, memberID)
	seedDrainAgreement(t, bsR, "chain-m", 2) // fresh JOIN: tx-33 grants no domains
	agreementRM, err := R.ActiveAgreement("chain-m")
	require.NoError(t, err)
	require.Empty(t, agreementRM.AllowedDomains)
	require.NoError(t, msR.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-m", Role: "host", ControllerChainID: R.localChainID,
		ControllerAgentID: relayerID, PeerAgentID: memberID, PolicyEpoch: "fresh-v3",
		RemoteCAPin: hex.EncodeToString(agreementRM.PeerPubKey), PolicyVersion: SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, msR.ActivateSyncControl(ctx, "chain-m", "fresh-v3"))
	_, err = R.ReplacePeerRBACPolicy(ctx, "chain-m", []store.PeerRBACDomainPermission{
		{Domain: "studio", Read: true, Copy: true},
	})
	require.NoError(t, err)
	_, err = msR.ApplyLocalDirectionalSyncPolicy(ctx, "chain-m", "fresh-v3",
		SyncPolicyVersionPeerRBAC, 1, "r-local-1", []string{"studio"}, nil)
	require.NoError(t, err)
	_, err = msR.ApplyRemoteDirectionalSyncPolicy(ctx, "chain-m", "fresh-v3",
		SyncPolicyVersionPeerRBAC, 1, "m-remote-1", nil, []string{"studio"})
	require.NoError(t, err)
	require.NoError(t, msR.MarkSyncPolicyDelivered(ctx, "chain-m", "fresh-v3", 1))

	relayerPin := []byte("pin-bytes-32-rrrrrrrrrrrrrrrrrrrr")
	agreementMR := &store.CrossFedRecord{
		RemoteChainID: "chain-r", PeerPubKey: relayerPin, MaxClearance: 2,
		AllowedDomains: nil, Status: "active",
	}
	require.NoError(t, msM.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-r", Role: "guest", ControllerChainID: "chain-r",
		ControllerAgentID: relayerID, PeerAgentID: relayerID, PolicyEpoch: "fresh-v3",
		RemoteCAPin: hex.EncodeToString(relayerPin), PolicyVersion: SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, msM.ActivateSyncControl(ctx, "chain-r", "fresh-v3"))
	_, err = msM.ApplyLocalDirectionalSyncPolicy(ctx, "chain-r", "fresh-v3",
		SyncPolicyVersionPeerRBAC, 1, "m-local-1", nil, []string{"studio"})
	require.NoError(t, err)
	_, err = msM.ApplyRemoteDirectionalSyncPolicy(ctx, "chain-r", "fresh-v3",
		SyncPolicyVersionPeerRBAC, 1, "r-remote-1", []string{"studio"}, nil)
	require.NoError(t, err)
	_, err = msM.ReplacePeerRBACPolicy(ctx, store.PeerRBACPolicy{
		RemoteChainID: "chain-r", PeerAgentID: relayerID,
		PolicyEpoch: "fresh-v3", RemoteCAPin: hex.EncodeToString(relayerPin),
		PolicyVersion: store.CurrentPeerRBACPolicyVersion, Domains: []store.PeerRBACDomainPermission{},
	})
	require.NoError(t, err)

	seedRelayCopy := func(originID, content string) (SyncItem, string) {
		t.Helper()
		sum := sha256.Sum256([]byte(content))
		item := SyncItem{
			OriginChainID: "chain-x", OriginMemoryID: originID,
			OriginCreatedAt: "2026-07-16T00:00:00Z", Domain: "studio",
			Classification: 1, MemoryType: "fact", ConfidenceScore: 0.9,
			Content: content, ContentHash: hex.EncodeToString(sum[:]),
		}
		item.OriginSig = signOriginSig(xPriv, &item)
		localID := syncMemoryID("chain-x", originID)
		require.NoError(t, seedSyncedMirror(ctx, msR, R, localID, item))
		require.NoError(t, msR.RecordSyncOrigin(ctx, store.SyncOrigin{
			OriginChainID: "chain-x", OriginMemoryID: originID,
			OriginAgentPubkey: xPubHex,
			OriginCreatedAt:   item.OriginCreatedAt, LocalMemoryID: localID,
			DomainTag: "studio", Outcome: store.SyncOutcomeAdmitted, OriginSig: item.OriginSig,
		}))
		return item, localID
	}

	accepted, _ := seedRelayCopy("fresh-accepted", "fresh trust-only relay")
	comet.after = func() {
		hash, _ := hex.DecodeString(accepted.ContentHash)
		_ = seedCommittedMemory(ctx, msM, syncMemoryID("chain-x", accepted.OriginMemoryID),
			accepted.Domain, accepted.Content, hash)
	}
	R.syncDigestFn = func(_ context.Context, _ string, _ *SyncDigestRequest) (*SyncDigestResponse, error) {
		return &SyncDigestResponse{}, nil
	}
	peerR := &peerIdentity{ChainID: "chain-r", AgentID: relayerID, Agreement: agreementMR}
	bindTestPeerAgreement(t, M, peerR)
	pushes := 0
	R.syncPushFn = func(_ context.Context, chain string, req *SyncPushRequest) (*SyncPushResponse, error) {
		require.Equal(t, "chain-m", chain)
		pushes++
		body, marshalErr := json.Marshal(req)
		require.NoError(t, marshalErr)
		httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/push", bytes.NewReader(body))
		httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peerR))
		rr := httptest.NewRecorder()
		M.handleSyncPush(rr, httpReq)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var resp SyncPushResponse
		require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
		return &resp, nil
	}

	consented, err := R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	R.reconcilePeer(ctx, msR, agreementRM, consented)
	R.syncDrain(ctx, msR, agreementRM, consented)
	require.Equal(t, 1, pushes)
	require.Equal(t, int32(1), comet.calls.Load())
	_, err = msM.GetSyncOrigin(ctx, "chain-x", accepted.OriginMemoryID)
	require.NoError(t, err, "fresh trust-only v3 relay was not admitted")

	// Direct Copy may be revoked without changing the independently authorized
	// exact group relay.
	_, err = R.ReplacePeerRBACPolicy(ctx, "chain-m", []store.PeerRBACDomainPermission{
		{Domain: "studio", Read: true},
	})
	require.NoError(t, err)
	copyIndependent, copyRevokedID := seedRelayCopy("copy-revoked", "copy revoked before group relay")
	comet.after = func() {
		hash, _ := hex.DecodeString(copyIndependent.ContentHash)
		_ = seedCommittedMemory(ctx, msM, syncMemoryID("chain-x", copyIndependent.OriginMemoryID),
			copyIndependent.Domain, copyIndependent.Content, hash)
	}
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", copyRevokedID, "chain-x")
	require.NoError(t, err)
	consented, err = R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	R.syncDrain(ctx, msR, agreementRM, consented)
	require.Equal(t, 2, pushes, "direct Copy revocation must not revoke exact group RBAC")
	require.Equal(t, int32(2), comet.calls.Load())
	_, err = msM.GetSyncOrigin(ctx, "chain-x", copyIndependent.OriginMemoryID)
	require.NoError(t, err)

	// Direct Subscribe is equally independent from exact group relay admission.
	_, err = R.ReplacePeerRBACPolicy(ctx, "chain-m", []store.PeerRBACDomainPermission{
		{Domain: "studio", Read: true, Copy: true},
	})
	require.NoError(t, err)
	_, err = msM.ApplyLocalDirectionalSyncPolicy(ctx, "chain-r", "fresh-v3",
		SyncPolicyVersionPeerRBAC, 2, "m-local-2", nil, nil)
	require.NoError(t, err)
	subscribeIndependent, subscribeRevokedID := seedRelayCopy("subscribe-revoked", "subscribe revoked before group admission")
	comet.after = func() {
		hash, _ := hex.DecodeString(subscribeIndependent.ContentHash)
		_ = seedCommittedMemory(ctx, msM, syncMemoryID("chain-x", subscribeIndependent.OriginMemoryID),
			subscribeIndependent.Domain, subscribeIndependent.Content, hash)
	}
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", subscribeRevokedID, "chain-x")
	require.NoError(t, err)
	consented, err = R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	R.syncDrain(ctx, msR, agreementRM, consented)
	require.Equal(t, 3, pushes)
	require.Equal(t, int32(3), comet.calls.Load(), "group relay remains authorized after direct Subscribe revoke")
	_, err = msM.GetSyncOrigin(ctx, "chain-x", subscribeIndependent.OriginMemoryID)
	require.NoError(t, err)

	// Removing M from the shared group closes the relay without changing the
	// underlying transport trust or either direct policy snapshot.
	require.NoError(t, msR.SetSyncGroupMemberState(ctx, "g1", "chain-m", store.GroupMemberRemoved, 1))
	removed, removedID := seedRelayCopy("member-removed", "group member removed before relay")
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", removedID, "chain-x")
	require.NoError(t, err)
	consented, err = R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	R.syncDrain(ctx, msR, agreementRM, consented)
	require.Equal(t, 3, pushes, "removed group member must receive no further relay bytes")
	_, err = msM.GetSyncOrigin(ctx, "chain-x", removed.OriginMemoryID)
	require.Error(t, err)
}

// TestMeshPullRelayUnsignedCopyNotServed proves a relayer refuses to serve a copy
// whose stored origin_sig is absent (a pre-v11.8 / unsigned admission): it can
// only relay authentically, never forge a signature — the row is terminal-failed
// and nothing is pushed.
func TestMeshPullRelayUnsignedCopyNotServed(t *testing.T) {
	ctx := context.Background()
	R, msR, bsR := newDrainTestManager(t)
	R.localChainID = "chain-r"
	xPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	mPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	xID, memberID, relayerID := hex.EncodeToString(xPub), hex.EncodeToString(mPub), hex.EncodeToString(R.agentPub)
	seedRelayGroup(t, msR, "studio", "chain-x", 0, xID, relayerID, memberID)
	seedDrainAgreement(t, bsR, "chain-m", 2, "studio")
	agreement, err := R.ActiveAgreement("chain-m")
	require.NoError(t, err)
	require.NoError(t, msR.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-m", Role: "host", ControllerChainID: R.localChainID,
		ControllerAgentID: relayerID, PeerAgentID: memberID, PolicyEpoch: "legacy-unsigned",
		RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}))
	require.NoError(t, msR.ActivateSyncControl(ctx, "chain-m", "legacy-unsigned"))

	content := "unsigned relay fact"
	sum := sha256.Sum256([]byte(content))
	localID := syncMemoryID("chain-x", "x-unsigned")
	require.NoError(t, seedCommittedMemory(ctx, msR, localID, "studio", content, sum[:]))
	// Admitted copy WITHOUT an origin_sig (nil).
	require.NoError(t, msR.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "x-unsigned", LocalMemoryID: localID,
		OriginAgentPubkey: xID, DomainTag: "studio", Outcome: store.SyncOutcomeAdmitted,
	}))
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", localID, "chain-x")
	require.NoError(t, err)

	pushed := false
	R.syncPushFn = func(_ context.Context, _ string, _ *SyncPushRequest) (*SyncPushResponse, error) {
		pushed = true
		return &SyncPushResponse{}, nil
	}
	consented, err := R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	R.syncDrain(ctx, msR, agreement, consented)

	assert.False(t, pushed, "an unsigned copy must never be pushed (no forging)")
	counts, err := msR.CountSyncOutboxByState(ctx, "chain-m")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateFailed], "unsigned relay row is terminal-failed")
}

// TestMeshPullRelayForgedSigRejected proves the receiver-side backstop: a relayed
// item whose origin_sig is present but forged is terminally rejected at Gate 5.5
// (verified against X's roster key, which the relayer cannot forge against).
func TestMeshPullRelayForgedSigRejected(t *testing.T) {
	xPub, xPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	comet := &scriptedComet{responses: []string{cometOK}}
	M, msM := newSyncTestManager(t, comet)
	M.localChainID = "chain-m"
	rPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	seedRelayGroup(t, msM, "studio", "chain-x", 0, hex.EncodeToString(xPub),
		hex.EncodeToString(rPub), hex.EncodeToString(M.agentPub))
	require.NoError(t, msM.SetSyncDomains(context.Background(), "chain-r", []string{"studio"}))

	content := "forged relay fact"
	sum := sha256.Sum256([]byte(content))
	item := SyncItem{
		OriginChainID:   "chain-x",
		OriginMemoryID:  "x-forged",
		OriginCreatedAt: "2026-02-02T00:00:00Z",
		Domain:          "studio",
		Classification:  1,
		MemoryType:      "fact",
		ConfidenceScore: 0.9,
		Content:         content,
		ContentHash:     hex.EncodeToString(sum[:]),
	}
	item.OriginSig = signOriginSig(xPriv, &item)
	item.OriginSig[0] ^= 0xFF // forge

	peerR := &peerIdentity{
		ChainID: "chain-r",
		AgentID: hex.EncodeToString(rPub), // relayer's own key — irrelevant to origin verify
		Agreement: &store.CrossFedRecord{
			RemoteChainID: "chain-r", MaxClearance: 2, AllowedDomains: []string{"studio"}, Status: "active",
		},
	}
	bindInboundGroupPeer(t, M, msM, peerR, "guest")
	// R (chain-r) is already an active member via seedRelayGroup, so relay is
	// authorized (ResolveGroupRelay succeeds); the forged sig is what fails.
	body, err := json.Marshal(SyncPushRequest{Items: []SyncItem{item}})
	require.NoError(t, err)
	httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/push", bytes.NewReader(body))
	httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peerR))
	rr := httptest.NewRecorder()
	M.handleSyncPush(rr, httpReq)
	require.Equal(t, http.StatusOK, rr.Code)
	var resp SyncPushResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	require.Len(t, resp.Results, 1)
	assert.Equal(t, SyncOutcomeRejectedOriginSig, resp.Results[0].Outcome, "forged origin sig must be terminally rejected")
	assert.Equal(t, int32(0), comet.calls.Load(), "a forgery must never reach broadcast")
}

// TestMeshRelayReauthorizedAtDrain proves the AUTHORITATIVE R1 fix: a relayed backfill
// row toward a peer that is NO LONGER in a shared group with the origin is terminal-
// rejected at the drain gate — NOT sent over the wire — even though an independent
// pairwise consent coincidentally covers the domain. Receiver Gate 6.5 is only a
// post-admission backstop; this send-time ResolveGroupRelay re-check is what actually
// keeps a third member's content from reaching an ex-member.
func TestMeshRelayReauthorizedAtDrain(t *testing.T) {
	ctx := context.Background()
	xPub, xPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	content := "x studio fact for ex-member"
	sum := sha256.Sum256([]byte(content))
	originItem := SyncItem{
		OriginChainID: "chain-x", OriginMemoryID: "x-mem-9", OriginCreatedAt: "2026-02-02T00:00:00Z",
		Domain: "studio", Classification: 1, MemoryType: "fact", ConfidenceScore: 0.9,
		Content: content, ContentHash: hex.EncodeToString(sum[:]),
	}
	originItem.OriginSig = signOriginSig(xPriv, &originItem)
	localID := syncMemoryID("chain-x", "x-mem-9")

	R, msR, bsR := newDrainTestManager(t)
	R.localChainID = "chain-r"
	// Group g1 carries "studio" with chain-x + chain-r active; chain-m is NOT a member
	// (removed/ejected) — so ResolveGroupRelay(local=r, peer=m, origin=x, studio) is false.
	seedGroupDomain(t, msR, "g1", "studio", "chain-x", 0)
	seedGroupMember(t, msR, "g1", "chain-x", store.GroupRoleFullSync, store.GroupMemberActive, hex.EncodeToString(xPub))
	seedGroupMember(t, msR, "g1", "chain-r", store.GroupRoleFullSync, store.GroupMemberActive, "keyr")
	// chain-m holds an INDEPENDENT pairwise relationship that coincidentally covers "studio".
	seedDrainAgreement(t, bsR, "chain-m", 2, "studio")
	require.NoError(t, msR.SetSyncDomains(ctx, "chain-m", []string{"studio"}))
	// R holds X's admitted copy + provenance (with X's origin_sig).
	require.NoError(t, seedSyncedMirror(ctx, msR, R, localID, originItem))
	require.NoError(t, msR.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "x-mem-9", OriginCreatedAt: originItem.OriginCreatedAt,
		OriginAgentPubkey: hex.EncodeToString(xPub),
		LocalMemoryID:     localID, DomainTag: "studio", Outcome: store.SyncOutcomeAdmitted, OriginSig: originItem.OriginSig,
	}))
	// A relayed backfill row toward chain-m already sits in the outbox (enqueued while
	// chain-m was still a group member, before its removal).
	_, err = msR.EnqueueRelayedSyncOutbox(ctx, "chain-m", localID, "chain-x")
	require.NoError(t, err)

	pushed := false
	R.syncPushFn = func(_ context.Context, _ string, _ *SyncPushRequest) (*SyncPushResponse, error) {
		pushed = true
		return &SyncPushResponse{}, nil
	}
	agreement, err := R.ActiveAgreement("chain-m")
	require.NoError(t, err)
	consented, err := R.effectiveConsent(ctx, msR, "chain-m")
	require.NoError(t, err)
	require.Contains(t, consented, "studio", "pairwise consent must coincidentally cover the group domain for this test")
	R.syncDrain(ctx, msR, agreement, consented)

	assert.False(t, pushed, "a relayed copy must NOT be sent to a peer no longer in a shared group (R1)")
	counts, err := msR.CountSyncOutboxByState(ctx, "chain-m")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateRejected], "the unauthorized relayed row is terminal-rejected at the drain gate")
}
