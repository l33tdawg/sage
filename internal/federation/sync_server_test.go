package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// scriptedComet serves the CometBFT broadcast_tx_commit JSON shape. Each call pops
// the next scripted response; the last one repeats.
type scriptedComet struct {
	calls     atomic.Int32
	responses []string
	after     func()
}

func (f *scriptedComet) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		n := int(f.calls.Add(1)) - 1
		if n >= len(f.responses) {
			n = len(f.responses) - 1
		}
		if f.after != nil {
			f.after()
		}
		_, _ = w.Write([]byte(f.responses[n]))
	}
}

const cometOK = `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"AB12","height":"7"}}`

func cometReject(code int, log string) string {
	b, _ := json.Marshal(log)
	return fmt.Sprintf(`{"result":{"check_tx":{"code":0},"tx_result":{"code":%d,"log":%s}}}`, code, string(b))
}

// newSyncTestManager builds a Manager wired to an in-memory SQLite store and
// a scripted CometBFT RPC. Same-package test, so unexported fields are fair game.
func newSyncTestManager(t *testing.T, comet *scriptedComet) (*Manager, *store.SQLiteStore) {
	t.Helper()
	ms, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ms.Close() })

	rpc := httptest.NewServer(comet.handler())
	t.Cleanup(rpc.Close)

	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	return &Manager{
		localChainID: "chain-local",
		cometRPC:     rpc.URL,
		agentKey:     priv,
		agentPub:     pub,
		memStore:     ms,
		logger:       zerolog.Nop(),
		broadcastSem: make(chan struct{}, 4),
	}, ms
}

// pushAs performs a sync push with an injected authenticated peer identity
// (peerAuth is exercised by the transport tests; these tests target the gates).
func pushAs(t *testing.T, m *Manager, peer *peerIdentity, req SyncPushRequest) (*httptest.ResponseRecorder, *SyncPushResponse) {
	t.Helper()
	bindTestPeerAgreement(t, m, peer)
	body, err := json.Marshal(req)
	require.NoError(t, err)
	httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/push", bytes.NewReader(body))
	httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peer))
	rr := httptest.NewRecorder()
	m.handleSyncPush(rr, httpReq)
	if rr.Code != http.StatusOK {
		return rr, nil
	}
	var resp SyncPushResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return rr, &resp
}

func testPeer(maxClearance int, allowed ...string) *peerIdentity {
	return &peerIdentity{
		ChainID: "chain-b",
		AgentID: "peer-agent",
		Agreement: &store.CrossFedRecord{
			RemoteChainID:  "chain-b",
			MaxClearance:   uint8(maxClearance), // #nosec G115 -- test values 0-4
			AllowedDomains: allowed,
			Status:         "active",
		},
	}
}

func TestLegacySyncPushReleasedAfterCompletedRevokeCannotAdmit(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	bindTestPeerAgreement(t, m, peer)
	require.NoError(t, ms.SetSyncDomains(ctx, peer.ChainID, []string{"hr"}))
	item := syncItem("revoked-push", "hr", "must not be admitted after revoke")
	body, err := json.Marshal(SyncPushRequest{Items: []SyncItem{item}})
	require.NoError(t, err)
	blocked := &blockingPeerValueContext{
		Context: context.Background(), peer: peer,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/push", bytes.NewReader(body)).WithContext(blocked)
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		m.handleSyncPush(rec, req)
		close(done)
	}()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("sync push did not pause at its authenticated peer context")
	}
	require.NoError(t, m.badger.UpdateCrossFedStatus(peer.ChainID, "revoked"))
	require.NoError(t, ms.PurgeSyncPeerState(ctx, peer.ChainID))
	close(blocked.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("released stale sync push did not finish")
	}
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Zero(t, comet.calls.Load(), "revoked request reached consensus broadcast")
	if origin, getErr := ms.GetSyncOrigin(ctx, peer.ChainID, item.OriginMemoryID); !errors.Is(getErr, sql.ErrNoRows) || origin != nil {
		t.Fatalf("revoked request persisted origin: origin=%+v err=%v", origin, getErr)
	}
}

func syncItem(originID, domain, content string) SyncItem {
	sum := sha256.Sum256([]byte(content))
	return SyncItem{
		OriginChainID:  "chain-b",
		OriginMemoryID: originID,
		Domain:         domain,
		Classification: 1,
		Content:        content,
		ContentHash:    hex.EncodeToString(sum[:]),
	}
}

func TestBuildSyncSubmitTxCarriesTagsOnlyAfterAppV20(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	m := &Manager{agentKey: priv, agentPub: pub}
	item := syncItem("tagged", "research", "federated scoped content")
	item.Tags = []string{"alpha", "zeta"}

	preFork, err := m.buildSyncSubmitTx("00000000-0000-4000-8000-000000000000", &item)
	require.NoError(t, err)
	parsed, err := tx.DecodeTx(preFork)
	require.NoError(t, err)
	require.NoError(t, tx.ActivateMemorySubmitTags(parsed))
	assert.Empty(t, parsed.MemorySubmit.Tags)

	m.postV20ForNextTx = func() bool { return true }
	postFork, err := m.buildSyncSubmitTx("00000000-0000-4000-8000-000000000000", &item)
	require.NoError(t, err)
	parsed, err = tx.DecodeTx(postFork)
	require.NoError(t, err)
	require.NoError(t, tx.ActivateMemorySubmitTags(parsed))
	assert.Equal(t, item.Tags, parsed.MemorySubmit.Tags)
}

func TestSyncPushGateOrder(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr", "eng")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	outsideTreaty := syncItem("m-scope", "finance", "finance fact")
	notConsented := syncItem("m-consent", "eng", "eng fact") // in treaty, not in sync_domains
	tooHigh := syncItem("m-clear", "hr", "secret fact")
	tooHigh.Classification = 4                               // above MaxClearance 2
	happy := syncItem("m-ok", "hr.public", "shared hr fact") // consented via subtree
	happy.Tags = []string{"eurorack", "oscillator"}
	comet.after = func() {
		localID := syncMemoryID("chain-b", "m-ok")
		sum := sha256.Sum256([]byte(happy.Content))
		_ = seedCommittedMemory(ctx, ms, localID, happy.Domain, happy.Content, sum[:])
	}

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{outsideTreaty, notConsented, tooHigh, happy}})
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 4)
	assert.Equal(t, SyncOutcomeRejectedScope, resp.Results[0].Outcome)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[1].Outcome)
	assert.Equal(t, SyncOutcomeRejectedClearance, resp.Results[2].Outcome)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[3].Outcome)
	assert.Equal(t, syncMemoryID("chain-b", "m-ok"), resp.Results[3].LocalMemoryID)
	tags, err := ms.GetTags(ctx, syncMemoryID("chain-b", "m-ok"))
	require.NoError(t, err)
	assert.Equal(t, []string{"eurorack", "oscillator"}, tags)

	// Every terminal decision is in the ledger; redelivery replays it without
	// re-running gates or re-broadcasting.
	broadcastsBefore := comet.calls.Load()
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{outsideTreaty, happy}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedScope, resp.Results[0].Outcome)
	assert.Equal(t, SyncOutcomeDuplicate, resp.Results[1].Outcome)
	assert.Equal(t, syncMemoryID("chain-b", "m-ok"), resp.Results[1].LocalMemoryID)
	assert.Equal(t, broadcastsBefore, comet.calls.Load(), "replay must not broadcast")
}

func TestSyncPushV3RequiresRemoteCopyAndLocalSubscription(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "legacy-only")
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peer.AgentID = hex.EncodeToString(peerPub)
	peer.Agreement.PeerPubKey = []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: peer.ChainID, Role: "guest", ControllerChainID: peer.ChainID,
		ControllerAgentID: peer.AgentID, PeerAgentID: peer.AgentID,
		PolicyEpoch: "epoch-v3", RemoteCAPin: hex.EncodeToString(peer.Agreement.PeerPubKey),
	}))
	require.NoError(t, ms.ActivateSyncControl(ctx, peer.ChainID, "epoch-v3"))
	_, err = ms.ApplyLocalDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-v3",
		SyncPolicyVersionPeerRBAC, 1, "local-1", nil, []string{"tii"})
	require.NoError(t, err)
	_, err = ms.ApplyRemoteDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-v3",
		SyncPolicyVersionPeerRBAC, 1, "remote-1", []string{"tii"}, nil)
	require.NoError(t, err)

	allowed := syncItem("m-v3-allowed", "tii.project", "v3 copy outside legacy treaty")
	seedCommitted(t, ms, "local-legacy-duplicate", "legacy-only", allowed.Content)
	comet.after = func() {
		localID := syncMemoryID(peer.ChainID, allowed.OriginMemoryID)
		sum := sha256.Sum256([]byte(allowed.Content))
		_ = seedCommittedMemory(ctx, ms, localID, allowed.Domain, allowed.Content, sum[:])
	}
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{allowed}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome,
		"active direct v3 must not be gated by legacy tx-33 domains")
	assert.NotEqual(t, SyncOutcomeRejectedXDomainDup, resp.Results[0].Outcome,
		"without an explicit local PeerRBAC Read policy, legacy treaty domains must not be a duplicate-presence oracle")

	_, err = ms.ApplyLocalDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-v3",
		SyncPolicyVersionPeerRBAC, 2, "local-2", nil, nil)
	require.NoError(t, err)
	noSubscription := syncItem("m-v3-no-sub", "tii.project", "receiver did not subscribe")
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{noSubscription}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[0].Outcome)

	_, err = ms.ApplyLocalDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-v3",
		SyncPolicyVersionPeerRBAC, 3, "local-3", nil, []string{"tii"})
	require.NoError(t, err)
	_, err = ms.ApplyRemoteDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-v3",
		SyncPolicyVersionPeerRBAC, 2, "remote-2", nil, nil)
	require.NoError(t, err)
	noCopy := syncItem("m-v3-no-copy", "tii.project", "publisher did not grant copy")
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{noCopy}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[0].Outcome)
	assert.Equal(t, int32(1), comet.calls.Load(), "denied v3 items must never reach consensus")
}

func TestPausedV3PushDoesNotRevealSavedReadDomainDuplicates(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peer := testPeer(2)
	peer.AgentID = hex.EncodeToString(peerPub)
	peer.Agreement.PeerPubKey = []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	bindTestPeerAgreement(t, m, peer)
	require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: peer.ChainID, Role: "guest", ControllerChainID: peer.ChainID,
		ControllerAgentID: peer.AgentID, PeerAgentID: peer.AgentID,
		PolicyEpoch: "epoch-paused-oracle", RemoteCAPin: hex.EncodeToString(peer.Agreement.PeerPubKey),
	}))
	require.NoError(t, ms.ActivateSyncControl(ctx, peer.ChainID, "epoch-paused-oracle"))
	_, err = ms.ApplyLocalDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-paused-oracle",
		SyncPolicyVersionPeerRBAC, 1, "local", nil, []string{"shared"})
	require.NoError(t, err)
	_, err = ms.ApplyRemoteDirectionalSyncPolicy(ctx, peer.ChainID, "epoch-paused-oracle",
		SyncPolicyVersionPeerRBAC, 1, "remote", []string{"shared"}, nil)
	require.NoError(t, err)
	_, err = m.ReplacePeerRBACPolicy(ctx, peer.ChainID, []store.PeerRBACDomainPermission{
		{Domain: "shared", Read: true}, {Domain: "private", Read: true},
	})
	require.NoError(t, err)
	_, err = m.SetPeerRBACPaused(ctx, peer.ChainID, true)
	require.NoError(t, err)

	sameDomain := syncItem("paused-same", "shared", "known same-domain bytes")
	seedCommitted(t, ms, "native-same", "shared", sameDomain.Content)
	comet.after = func() {
		_ = seedCommittedMemory(ctx, ms, syncMemoryID(peer.ChainID, sameDomain.OriginMemoryID),
			sameDomain.Domain, sameDomain.Content, mustDecodeHex(t, sameDomain.ContentHash))
	}
	_, sameResp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{sameDomain}})
	require.NotNil(t, sameResp)
	require.Len(t, sameResp.Results, 1)
	assert.Equal(t, SyncOutcomeAccepted, sameResp.Results[0].Outcome)

	crossDomain := syncItem("paused-cross", "shared", "known cross-domain bytes")
	seedCommitted(t, ms, "native-private", "private", crossDomain.Content)
	comet.after = func() {
		_ = seedCommittedMemory(ctx, ms, syncMemoryID(peer.ChainID, crossDomain.OriginMemoryID),
			crossDomain.Domain, crossDomain.Content, mustDecodeHex(t, crossDomain.ContentHash))
	}
	_, crossResp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{crossDomain}})
	require.NotNil(t, crossResp)
	require.Len(t, crossResp.Results, 1)
	assert.Equal(t, SyncOutcomeAccepted, crossResp.Results[0].Outcome)
	assert.NotEqual(t, SyncOutcomeDuplicate, crossResp.Results[0].Outcome)
	assert.NotEqual(t, SyncOutcomeRejectedXDomainDup, crossResp.Results[0].Outcome)
}

// TestSyncPushOriginSig exercises Gate 5.5: an origin-signed item is admitted;
// a forged (corrupted-sig) or mis-attributed (sig over different content) item
// is rejected terminally; and a pre-v11.8 item with no sig still admits.
func TestSyncPushOriginSig(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)

	// The origin IS the authenticated peer; its AgentID is the hex of the key it
	// signs items with, so the receiver resolves the verifier from peer.AgentID.
	originPub, originPriv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	peer := &peerIdentity{
		ChainID: "chain-b",
		AgentID: hex.EncodeToString(originPub),
		Agreement: &store.CrossFedRecord{
			RemoteChainID: "chain-b", MaxClearance: 2, AllowedDomains: []string{"hr"}, Status: "active",
		},
	}
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	good := syncItem("m-signed", "hr", "signed hr fact")
	good.OriginSig = signOriginSig(originPriv, &good)
	comet.after = func() {
		localID := syncMemoryID("chain-b", "m-signed")
		sum := sha256.Sum256([]byte(good.Content))
		_ = seedCommittedMemory(ctx, ms, localID, good.Domain, good.Content, sum[:])
	}

	// Forged: signature present but corrupted.
	forged := syncItem("m-forged", "hr", "forged hr fact")
	forged.OriginSig = signOriginSig(originPriv, &forged)
	forged.OriginSig[0] ^= 0xFF

	// Mis-attributed: a valid signature over the ORIGINAL content, but the
	// content (and its now-recomputed hash) were swapped after signing — the
	// relayer-forgery vector. content_hash stays self-consistent so it passes
	// structural validation; Gate 5.5 must still catch the stale signature.
	misattr := syncItem("m-misattr", "hr", "original content")
	misattr.OriginSig = signOriginSig(originPriv, &misattr)
	misattr.Content = "swapped content"
	swapped := sha256.Sum256([]byte(misattr.Content))
	misattr.ContentHash = hex.EncodeToString(swapped[:])

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{good, forged, misattr}})
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 3)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome)
	assert.Equal(t, SyncOutcomeRejectedOriginSig, resp.Results[1].Outcome)
	assert.Equal(t, SyncOutcomeRejectedOriginSig, resp.Results[2].Outcome)

	// Backward compatibility: an unsigned item (pre-v11.8 sender) still admits.
	legacy := syncItem("m-legacy", "hr", "legacy hr fact")
	comet.after = func() {
		localID := syncMemoryID("chain-b", "m-legacy")
		s := sha256.Sum256([]byte(legacy.Content))
		_ = seedCommittedMemory(ctx, ms, localID, legacy.Domain, legacy.Content, s[:])
	}
	_, resp2 := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{legacy}})
	require.NotNil(t, resp2)
	assert.Equal(t, SyncOutcomeAccepted, resp2.Results[0].Outcome)
}

func TestSyncItemTagValidationAndPersistence(t *testing.T) {
	item := syncItem("m-tags", "hr", "tagged fact")
	item.Tags = []string{"zeta", "alpha", "alpha"}
	require.NoError(t, validateSyncItem("chain-b", &item))
	assert.Equal(t, []string{"alpha", "zeta"}, item.Tags)

	bad := item
	bad.Tags = []string{" padded "}
	assert.Error(t, validateSyncItem("chain-b", &bad))
	bad = item
	bad.Tags = make([]string, SyncMaxTags+1)
	for i := range bad.Tags {
		bad.Tags[i] = fmt.Sprintf("tag-%d", i)
	}
	assert.Error(t, validateSyncItem("chain-b", &bad))

	_, ms := newSyncTestManager(t, &scriptedComet{responses: []string{cometOK}})
	localID := syncMemoryID("chain-b", "m-tags")
	seedCommitted(t, ms, localID, "hr", "tagged fact")
	require.NoError(t, ms.SetTags(context.Background(), localID, item.Tags))
	got, err := ms.GetTags(context.Background(), localID)
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "zeta"}, got)
}

func TestSyncTagFailureKeepsProvenanceAndNeverRebroadcasts(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	item := syncItem("m-tag-retry", "hr", "tag projection retry")
	item.Tags = []string{"important"}

	// The scripted Comet deliberately does not project the committed memory
	// into SQLite, so memory_tags' FK makes SetTags fail after admission.
	_, first := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, first)
	assert.Equal(t, SyncOutcomeRetry, first.Results[0].Outcome)
	origin, err := ms.GetSyncOrigin(ctx, "chain-b", "m-tag-retry")
	require.NoError(t, err, "provenance must precede fallible tag projection")
	assert.Equal(t, syncMemoryID("chain-b", "m-tag-retry"), origin.LocalMemoryID)

	_, second := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, second)
	assert.Equal(t, SyncOutcomeRetry, second.Results[0].Outcome)
	assert.Equal(t, int32(1), comet.calls.Load(), "tag repair replay must never rebroadcast the admitted copy")
}

func TestPendingSyncOriginRecoversCommittedCopyWithoutRebroadcast(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	item := syncItem("m-crash", "hr", "committed before provenance crash")
	item.Tags = []string{"recovered"}
	localID := syncMemoryID("chain-b", "m-crash")
	require.NoError(t, ms.StageSyncOrigin(ctx, store.SyncOriginPending{
		OriginChainID: "chain-b", OriginMemoryID: "m-crash", LocalMemoryID: localID, DomainTag: "hr",
		ContentHash: item.ContentHash, Classification: item.Classification, MemoryType: "fact",
		SubmittingAgent: hex.EncodeToString(m.agentPub),
	}))
	require.NoError(t, seedSyncedMirror(ctx, ms, m, localID, item))

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeDuplicate, resp.Results[0].Outcome)
	assert.Equal(t, int32(0), comet.calls.Load(), "crash recovery must not rebroadcast committed state")
	origin, err := ms.GetSyncOrigin(ctx, "chain-b", "m-crash")
	require.NoError(t, err)
	assert.Equal(t, localID, origin.LocalMemoryID)
	_, err = ms.GetPendingSyncOrigin(ctx, "chain-b", "m-crash")
	assert.ErrorIs(t, err, sql.ErrNoRows)
	tags, err := ms.GetTags(ctx, localID)
	require.NoError(t, err)
	assert.Equal(t, []string{"recovered"}, tags)
}

func TestAmbiguousBroadcastKeepsQuarantineAndPromotesOnRetry(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometReject(11, "temporary admission result unavailable")}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	item := syncItem("m-ambiguous", "hr", "commit response was lost")
	localID := syncMemoryID("chain-b", item.OriginMemoryID)
	comet.after = func() { _ = seedSyncedMirror(ctx, ms, m, localID, item) }

	_, first := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, first)
	assert.Equal(t, SyncOutcomeRetry, first.Results[0].Outcome)
	isCopy, err := ms.IsSyncedCopy(ctx, localID)
	require.NoError(t, err)
	assert.True(t, isCopy, "ambiguous commit must remain durably quarantined")

	_, second := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, second)
	assert.Equal(t, SyncOutcomeDuplicate, second.Results[0].Outcome)
	assert.Equal(t, int32(1), comet.calls.Load(), "retry must promote the mirror without another broadcast")
}

func TestPendingRecoveryRefusesMismatchedLocalIdentity(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	item := syncItem("m-collision", "hr", "expected foreign content")
	localID := syncMemoryID("chain-b", item.OriginMemoryID)
	pending := store.SyncOriginPending{OriginChainID: "chain-b", OriginMemoryID: item.OriginMemoryID,
		LocalMemoryID: localID, DomainTag: item.Domain, ContentHash: item.ContentHash,
		Classification: item.Classification, MemoryType: "fact", SubmittingAgent: hex.EncodeToString(m.agentPub)}
	require.NoError(t, ms.StageSyncOrigin(ctx, pending))
	wrongHash := sha256.Sum256([]byte("unrelated native content"))
	require.NoError(t, seedCommittedMemory(ctx, ms, localID, item.Domain, "unrelated native content", wrongHash[:]))

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRetry, resp.Results[0].Outcome)
	assert.Equal(t, int32(0), comet.calls.Load(), "identity collision must fail closed before broadcast")
	_, err := ms.GetSyncOrigin(ctx, "chain-b", item.OriginMemoryID)
	assert.ErrorIs(t, err, sql.ErrNoRows)
	_, err = ms.GetPendingSyncOrigin(ctx, "chain-b", item.OriginMemoryID)
	require.NoError(t, err, "mismatched row must remain quarantined")
}

func TestSyncPushStructuralViolations(t *testing.T) {
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "*")
	require.NoError(t, ms.SetSyncDomains(context.Background(), "chain-b", []string{"hr"}))

	// Origin chain != authenticated peer -> whole batch 400 (no laundering).
	forged := syncItem("m-1", "hr", "content")
	forged.OriginChainID = "chain-c"
	rr, _ := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{forged}})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Content-hash mismatch -> 400.
	tampered := syncItem("m-2", "hr", "content")
	tampered.Content = "tampered content"
	rr, _ = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{tampered}})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Batch bigger than SyncPushMaxItems -> 400.
	big := make([]SyncItem, SyncPushMaxItems+1)
	for i := range big {
		big[i] = syncItem(fmt.Sprintf("m-%d", i), "hr", fmt.Sprintf("content %d", i))
	}
	rr, _ = pushAs(t, m, peer, SyncPushRequest{Items: big})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Zero broadcasts happened: structural rejects never reach consensus.
	assert.EqualValues(t, 0, comet.calls.Load())
}

func TestSyncPushCrossDomainDup(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "*")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr", "eng"}))

	// Seed the same content committed locally in domain "eng".
	content := "identical shared fact"
	sum := sha256.Sum256([]byte(content))
	require.NoError(t, seedCommittedMemory(ctx, ms, "local-eng", "eng", content, sum[:]))

	// Pushed into a DIFFERENT domain -> B-D1 terminal reject.
	item := syncItem("m-dup", "hr", content)
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedXDomainDup, resp.Results[0].Outcome)

	// Pushed into the SAME domain -> duplicate success mapped to existing row.
	same := syncItem("m-same", "eng", content)
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{same}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeDuplicate, resp.Results[0].Outcome)
	assert.Equal(t, "local-eng", resp.Results[0].LocalMemoryID)

	// Neither path broadcast anything.
	assert.EqualValues(t, 0, comet.calls.Load())
}

func TestSyncPushNonceRaceRetriesOnce(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{
		cometReject(4, "nonce too low: got 5, expected > 6"),
		cometOK,
	}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "*")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{syncItem("m-n", "hr", "raced fact")}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome)
	assert.EqualValues(t, 2, comet.calls.Load(), "one nonce retry with a fresh tx")
}

// TestT2fWriteNeverWidens is T2(f): the WRITE-never-widens end-to-end invariant
// (docs/v11.8-PLAN.md §8, plan T2(f)). A synced item is admitted ONLY by the
// receiver's OWN operator-signed MemorySubmit (buildSyncSubmitTx), which
// FinalizeBlock re-validates. When the receiver's operator has NO write access to
// the item's (non-owned) domain, the chain returns the SAME Code 11 "no write
// access to domain" it always has (app.go processMemorySubmit write gate, level-2
// HasWriteAccessMultiOrg), the sync layer surfaces it as
// SyncOutcomeRejectedWriteAccess, and NOTHING is persisted or recorded. The
// off-consensus app-v19 default-READ flip (web/handler.go) never opens a write
// path: no emit, widened getter, or read-default touches this gate. Hermetic — a
// scripted CometBFT stands in for FinalizeBlock (no real node, no make down-clean).
func TestT2fWriteNeverWidens(t *testing.T) {
	ctx := context.Background()
	// FinalizeBlock rejects the operator's MemorySubmit for lack of RBAC write
	// access on the receiver — the unchanged Code 11 write gate. The Log text is
	// the exact literal classifySyncBroadcast keys on (pinned by
	// TestClassifySyncBroadcast against internal/abci/app.go).
	comet := &scriptedComet{responses: []string{cometReject(11, "access denied: agent 0123456789abcdef has no write access to domain hr")}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	item := syncItem("m-nowrite", "hr", "receiver operator lacks write access to hr")
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 1)
	// A no-write-access reject is surfaced as its own terminal outcome — never
	// admitted, never silently widened into a local write despite any read-side flip.
	assert.Equal(t, SyncOutcomeRejectedWriteAccess, resp.Results[0].Outcome)
	assert.Empty(t, resp.Results[0].LocalMemoryID, "a write-gated item never lands a local copy")

	// Nothing durable: no admission ledger row (a config-dependent reject must stay
	// re-evaluable, never poison a later legitimate push) and no synced copy landed.
	_, err := ms.GetSyncOrigin(ctx, "chain-b", "m-nowrite")
	assert.ErrorIs(t, err, sql.ErrNoRows, "a write-gated item is never recorded as admitted")
	localID := syncMemoryID("chain-b", "m-nowrite")
	isCopy, err := ms.IsSyncedCopy(ctx, localID)
	require.NoError(t, err)
	assert.False(t, isCopy, "a write-gated item is never persisted as a synced copy")

	// The receiver's own operator-signed submit is required on EVERY attempt: a
	// redelivery re-runs the write gate through a fresh broadcast rather than
	// replaying a cached admission, so the sync path can never bypass the gate.
	broadcastsAfterFirst := comet.calls.Load()
	_, resp2 := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp2)
	assert.Equal(t, SyncOutcomeRejectedWriteAccess, resp2.Results[0].Outcome)
	assert.Greater(t, comet.calls.Load(), broadcastsAfterFirst, "each attempt re-submits through the write gate; the reject is never cached as an admission")
}

func TestClassifySyncBroadcast(t *testing.T) {
	// Pins the Log-text soft contract with internal/abci/app.go. If a wording
	// change over there breaks these, update BOTH sides deliberately.
	cases := []struct {
		log  string
		code int
		want syncBcastClass
	}{
		{`memory abc already reached terminal status "committed"; re-submit rejected`, 11, syncBcastDuplicate},
		{"memory abc is a co-committed id and cannot be overwritten by a normal submit", 11, syncBcastDuplicate},
		{"access denied: agent 1234 has no write access to domain hr", 11, syncBcastScopeReject},
		{"nonce too low: got 3, expected > 4", 4, syncBcastNonceRace},
		{"nonce 0 not permitted", 4, syncBcastNonceRace},
		{"memory submit rejected: a non-empty domain_tag is required (app-v16)", 11, syncBcastRetry},
	}
	for _, c := range cases {
		err := fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", c.code, c.log)
		assert.Equal(t, c.want, classifySyncBroadcast(err), c.log)
	}
	assert.Equal(t, syncBcastOK, classifySyncBroadcast(nil))
}

// seedCommittedMemory inserts a committed row via the ordinary store path
// (the B-D1 gate reads the SQLite mirror's committed set).
func seedCommittedMemory(ctx context.Context, ms *store.SQLiteStore, id, domain, content string, hash []byte) error {
	return ms.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "seed-agent",
		Content:         content,
		ContentHash:     hash,
		MemoryType:      memory.TypeFact,
		DomainTag:       domain,
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	})
}

func seedSyncedMirror(ctx context.Context, ms *store.SQLiteStore, m *Manager, id string, item SyncItem) error {
	hash, err := hex.DecodeString(item.ContentHash)
	if err != nil {
		return err
	}
	if err := ms.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID: id, SubmittingAgent: hex.EncodeToString(m.agentPub), Content: item.Content, ContentHash: hash,
		MemoryType: memory.MemoryType(syncStoredMemoryType(item.MemoryType)), DomainTag: item.Domain,
		ConfidenceScore: item.ConfidenceScore, Status: memory.StatusCommitted, CreatedAt: time.Now(),
	}); err != nil {
		return err
	}
	return ms.UpdateMemoryClassification(ctx, id, store.ClearanceLevel(item.Classification))
}
