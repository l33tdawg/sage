package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func digestAs(t *testing.T, m *Manager, peer *peerIdentity, req SyncDigestRequest) (*httptest.ResponseRecorder, *SyncDigestResponse) {
	t.Helper()
	bindTestPeerAgreement(t, m, peer)
	body, err := json.Marshal(req)
	require.NoError(t, err)
	httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/digest", bytes.NewReader(body))
	httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peer))
	rr := httptest.NewRecorder()
	m.handleSyncDigest(rr, httpReq)
	if rr.Code != http.StatusOK {
		return rr, nil
	}
	var resp SyncDigestResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return rr, &resp
}

func TestSyncDigestPagingAndConsent(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	// Admission ledger: three ADMITTED (one in a subdomain, one in eng). In
	// the corrected model only ADMITTED decisions are recorded at all, so a
	// rejected item simply has no row — the digest is admitted-only, and a
	// rejected item stays re-offerable (it re-evaluates on the next push).
	for _, o := range []store.SyncOrigin{
		{OriginChainID: "chain-b", OriginMemoryID: "m-01", DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-1"},
		{OriginChainID: "chain-b", OriginMemoryID: "m-02", DomainTag: "hr.public", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-2"},
		{OriginChainID: "chain-b", OriginMemoryID: "m-03", DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-3"},
		{OriginChainID: "chain-b", OriginMemoryID: "m-04", DomainTag: "eng", Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "l-4"},
	} {
		require.NoError(t, ms.RecordSyncOrigin(ctx, o))
	}

	// Page size 2: first page + cursor, second page completes.
	_, resp := digestAs(t, m, peer, SyncDigestRequest{Domain: "hr", Limit: 2})
	require.NotNil(t, resp)
	assert.True(t, resp.Consented)
	assert.Equal(t, []string{"hr"}, resp.ConsentedDomains)
	assert.Equal(t, []string{"m-01", "m-02"}, resp.OriginMemoryIDs, "subtree: hr covers hr.public; eng excluded")
	require.Equal(t, "m-02", resp.NextCursor)

	_, resp = digestAs(t, m, peer, SyncDigestRequest{Domain: "hr", Limit: 2, After: resp.NextCursor})
	require.NotNil(t, resp)
	assert.Equal(t, []string{"m-03"}, resp.OriginMemoryIDs)
	assert.Empty(t, resp.NextCursor)

	// Unconsented domain: honest consent=false, ledger still answered.
	_, resp = digestAs(t, m, peer, SyncDigestRequest{Domain: "eng"})
	require.NotNil(t, resp)
	assert.False(t, resp.Consented)
	assert.Equal(t, []string{"m-04"}, resp.OriginMemoryIDs)

	// Missing domain -> 400.
	rr, _ := digestAs(t, m, peer, SyncDigestRequest{})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGroupDigestServeLeaseLinearizesMemberRemoval(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	seedGroup(t, ms, "g1", "chain-ctl")
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	memberIDs := map[string]string{
		"chain-local": hex.EncodeToString(m.agentPub),
		"chain-peer":  hex.EncodeToString(peerPub),
	}
	for _, chain := range []string{"chain-local", "chain-peer"} {
		require.NoError(t, ms.UpsertSyncGroupMember(ctx, store.SyncGroupMember{
			GroupID: "g1", MemberChainID: chain, Role: store.GroupRoleFullSync,
			MemberState: store.GroupMemberActive, MemberAgentPubkey: memberIDs[chain],
		}))
	}
	require.NoError(t, ms.UpsertSyncGroupDomain(ctx, store.SyncGroupDomain{
		GroupID: "g1", DomainTag: "hr", OwnerChainID: "chain-local", AddedRevision: 1,
	}))

	body, err := json.Marshal(SyncDigestRequest{GroupID: "g1", Domain: "hr"})
	require.NoError(t, err)
	peer := &peerIdentity{ChainID: "chain-peer", AgentID: memberIDs["chain-peer"], Agreement: &store.CrossFedRecord{
		RemoteChainID: "chain-peer", AllowedDomains: []string{"hr"}, Status: "active",
	}}
	bindInboundGroupPeer(t, m, ms, peer, "guest")
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/digest", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, peer))
	bw := newBlockingResponseWriter()
	served := make(chan struct{})
	go func() { m.handleSyncDigest(bw, req); close(served) }()
	<-bw.entered

	removalStarted := make(chan struct{})
	removalDone := make(chan struct{})
	go func() {
		close(removalStarted)
		unlock := ms.LockSyncPolicyWrite()
		_ = ms.SetSyncGroupMemberState(ctx, "g1", "chain-peer", store.GroupMemberRemoved, 1)
		unlock()
		close(removalDone)
	}()
	<-removalStarted
	select {
	case <-removalDone:
		t.Fatal("member removal returned while a stale-policy group digest response was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(bw.release)
	<-served
	<-removalDone
	assert.Equal(t, http.StatusOK, bw.status)

	rr, resp := digestAs(t, m, peer, SyncDigestRequest{GroupID: "g1", Domain: "hr"})
	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Nil(t, resp, "no post-removal group digest may be served")
}

func TestPairwiseDigestServeLeaseLinearizesConsentRemoval(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	peer := testPeer(2, "hr")
	bindTestPeerAgreement(t, m, peer)
	require.NoError(t, ms.SetSyncDomains(ctx, peer.ChainID, []string{"hr"}))

	body, err := json.Marshal(SyncDigestRequest{Domain: "hr"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/digest", bytes.NewReader(body))
	req = req.WithContext(context.WithValue(req.Context(), peerCtxKey{}, peer))
	bw := newBlockingResponseWriter()
	served := make(chan struct{})
	go func() { m.handleSyncDigest(bw, req); close(served) }()
	<-bw.entered

	removalDone := make(chan error, 1)
	go func() { removalDone <- ms.DeleteSyncDomains(ctx, peer.ChainID) }()
	select {
	case err := <-removalDone:
		t.Fatalf("consent removal returned while a stale pairwise digest response was in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(bw.release)
	<-served
	require.NoError(t, <-removalDone)
	assert.Equal(t, http.StatusOK, bw.status)

	rr, resp := digestAs(t, m, peer, SyncDigestRequest{Domain: "hr"})
	assert.Equal(t, http.StatusOK, rr.Code)
	require.NotNil(t, resp)
	assert.False(t, resp.Consented, "removed pairwise consent remained visible after the removal returned")
}

func TestLegacyDigestReleasedAfterCompletedRevokeDoesNotExposeOriginIDs(t *testing.T) {
	ctx := context.Background()
	m, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	peer := testPeer(2, "hr")
	bindTestPeerAgreement(t, m, peer)
	require.NoError(t, ms.SetSyncDomains(ctx, peer.ChainID, []string{"hr"}))
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: peer.ChainID, OriginMemoryID: "secret-origin-id", DomainTag: "hr",
		Outcome: store.SyncOutcomeAdmitted, LocalMemoryID: "local-copy",
	}))
	body, err := json.Marshal(SyncDigestRequest{Domain: "hr"})
	require.NoError(t, err)
	blocked := &blockingPeerValueContext{
		Context: context.Background(), peer: peer,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/digest", bytes.NewReader(body)).WithContext(blocked)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		m.handleSyncDigest(rec, req)
		close(done)
	}()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("digest handler did not pause at its authenticated peer context")
	}
	require.NoError(t, m.badger.UpdateCrossFedStatus(peer.ChainID, "revoked"))
	require.NoError(t, ms.PurgeSyncPeerState(ctx, peer.ChainID))
	close(blocked.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("released stale digest did not finish")
	}
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.NotContains(t, rec.Body.String(), "secret-origin-id")
}

func TestSyncReconcileBackfillsAndSettles(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	// Local committed set: one the peer already decided (outbox lost), one
	// genuinely new.
	seedCommitted(t, ms, "m-old", "hr", "peer already has this")
	seedCommitted(t, ms, "m-new", "hr", "peer has never seen this")

	m.syncDigestFn = func(_ context.Context, chain string, req *SyncDigestRequest) (*SyncDigestResponse, error) {
		assert.Equal(t, "chain-b", chain)
		assert.Equal(t, "hr", req.Domain)
		return &SyncDigestResponse{
			Consented:        true,
			ConsentedDomains: []string{"hr"},
			OriginMemoryIDs:  []string{"m-old"},
		}, nil
	}
	m.reconcilePeer(ctx, ms, mustAgreement(t, m, "chain-b"), []string{"hr"})

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateDelivered], "already-decided item settles without redelivery")
	assert.Equal(t, 1, counts[store.SyncStatePending], "fresh item backfills as pending")

	st, ok := m.SyncReconcileInfo("chain-b")
	require.True(t, ok)
	assert.Equal(t, []string{"hr"}, st.PeerConsented)
	assert.False(t, st.PeerUnsupported)
	assert.False(t, st.LastReconcile.IsZero())
}

func TestSyncReconcileUnsupportedPeer(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-1", "hr", "fact")

	m.syncDigestFn = func(_ context.Context, _ string, _ *SyncDigestRequest) (*SyncDigestResponse, error) {
		return nil, ErrSyncUnsupported
	}
	m.reconcilePeer(ctx, ms, mustAgreement(t, m, "chain-b"), []string{"hr"})

	st, ok := m.SyncReconcileInfo("chain-b")
	require.True(t, ok)
	assert.True(t, st.PeerUnsupported)
	// Nothing enqueued by the aborted reconcile (the scan still handles the
	// eventual delivery once the peer upgrades).
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, counts)
}

func mustAgreement(t *testing.T, m *Manager, chain string) *store.CrossFedRecord {
	t.Helper()
	a, err := m.ActiveAgreement(chain)
	require.NoError(t, err)
	return a
}
