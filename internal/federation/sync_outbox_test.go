package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// newDrainTestManager wires a Manager with SQLite + Badger (for the
// agreement record) and a stubbed SyncPush seam.
func newDrainTestManager(t *testing.T) (*Manager, *store.SQLiteStore, *store.BadgerStore) {
	t.Helper()
	dir := t.TempDir()
	ms, err := store.NewSQLiteStore(context.Background(), filepath.Join(dir, "drain.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ms.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(dir, "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })

	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return &Manager{
		localChainID: "chain-local",
		agentKey:     priv,
		agentPub:     pub,
		badger:       bs,
		memStore:     ms,
		logger:       zerolog.Nop(),
		broadcastSem: make(chan struct{}, 4),
	}, ms, bs
}

func seedDrainAgreement(t *testing.T, bs *store.BadgerStore, chain string, maxClearance uint8, domains ...string) {
	t.Helper()
	require.NoError(t, bs.SetCrossFed(chain, "https://peer:8444", []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa"), maxClearance, 0, domains, []string{"*"}, "active"))
}

func seedCommitted(t *testing.T, ms *store.SQLiteStore, id, domain, content string) {
	t.Helper()
	sum := sha256.Sum256([]byte(content))
	require.NoError(t, seedCommittedMemory(context.Background(), ms, id, domain, content, sum[:]))
}

func TestListSyncCandidatesSubtreeAndExclusions(t *testing.T) {
	ctx := context.Background()
	_, ms, _ := newDrainTestManager(t)

	seedCommitted(t, ms, "m-hr", "hr", "hr fact")
	seedCommitted(t, ms, "m-hr-pub", "hr.public", "hr public fact")
	seedCommitted(t, ms, "m-eng", "eng", "eng fact")         // outside consent
	seedCommitted(t, ms, "m-queued", "hr", "queued fact")    // already queued
	seedCommitted(t, ms, "m-copy", "hr", "synced copy fact") // a copy: never re-forward
	seedCommitted(t, ms, "m-audit", "SAGE-SYNCAUDIT-GRP-ABC", "protocol anchor")
	_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "m-queued")
	require.NoError(t, err)
	require.NoError(t, ms.RecordSyncOrigin(ctx, store.SyncOrigin{
		OriginChainID: "chain-x", OriginMemoryID: "orig-9", LocalMemoryID: "m-copy",
		DomainTag: "hr", Outcome: store.SyncOutcomeAdmitted,
	}))

	cands, err := ms.ListSyncCandidates(ctx, "chain-b", []string{"hr"}, 100)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, c := range cands {
		ids[c.MemoryID] = true
	}
	assert.True(t, ids["m-hr"], "exact domain match")
	assert.True(t, ids["m-hr-pub"], "subtree match: consent 'hr' covers 'hr.public'")
	assert.False(t, ids["m-eng"], "outside consented subtree")
	assert.False(t, ids["m-queued"], "already in outbox")
	assert.False(t, ids["m-copy"], "synced copies are never re-forwarded")

	auditCandidates, err := ms.ListSyncCandidates(ctx, "chain-b", []string{"SAGE-SYNCAUDIT-GRP-ABC"}, 100)
	require.NoError(t, err)
	assert.Empty(t, auditCandidates, "protocol audit anchors never enter an outbound sync lane")
	auditCandidate, err := ms.GetSyncCandidateByID(ctx, "m-audit")
	require.NoError(t, err)
	assert.Nil(t, auditCandidate, "commit-tail lookup must enforce the same reservation as restart scans")
}

func TestSyncDrainEndToEnd(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	seedCommitted(t, ms, "m-1", "hr", "first shared fact")
	seedCommitted(t, ms, "m-2", "hr.public", "second shared fact")

	var pushed []SyncItem
	m.syncPushFn = func(_ context.Context, chain string, req *SyncPushRequest) (*SyncPushResponse, error) {
		assert.Equal(t, "chain-b", chain)
		pushed = append(pushed, req.Items...)
		results := make([]SyncItemResult, len(req.Items))
		for i, it := range req.Items {
			results[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: SyncOutcomeAccepted, LocalMemoryID: "peer-" + it.OriginMemoryID}
		}
		return &SyncPushResponse{Results: results}, nil
	}

	// One tick: scan enqueues both, drain delivers both.
	m.syncTick(ctx, ms)

	require.Len(t, pushed, 2)
	assert.Equal(t, "chain-local", pushed[0].OriginChainID, "origin stamps the LOCAL chain")
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 2, counts[store.SyncStateDelivered])

	// Second tick: anti-join finds nothing new, nothing re-pushed.
	pushed = nil
	m.syncTick(ctx, ms)
	assert.Empty(t, pushed)
}

func TestReconcileRetiredFederationStatePurgesOnlyDefinitiveRetirement(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedControl := func(chain, epoch string) {
		seedDrainAgreement(t, bs, chain, 2)
		agreement := mustDrainAgreement(t, m, chain)
		require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{
			RemoteChainID: chain, Role: "host", ControllerChainID: m.localChainID,
			ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: hex.EncodeToString(m.agentPub),
			PolicyEpoch: epoch, RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
		}))
		require.NoError(t, ms.ActivateSyncControl(ctx, chain, epoch))
	}
	seedControl("chain-retired", "epoch-retired")
	seedControl("chain-active", "epoch-active")
	require.NoError(t, bs.UpdateCrossFedStatus("chain-retired", "revoked"))

	m.reconcileRetiredFederationState(ctx, ms)
	retired, err := ms.GetSyncControl(ctx, "chain-retired")
	require.NoError(t, err)
	assert.Nil(t, retired, "tx-34 crash residue must be purged on reconciliation")
	active, err := ms.GetSyncControl(ctx, "chain-active")
	require.NoError(t, err)
	assert.NotNil(t, active, "an authoritative active agreement must never be treated as cleanup residue")
}

func TestRetiredReconcilerCannotPurgeFreshRepairGeneration(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-repair", 2)
	agreement := mustDrainAgreement(t, m, "chain-repair")
	base := store.SyncControl{
		RemoteChainID: "chain-repair", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: hex.EncodeToString(m.agentPub),
		RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}
	e1 := base
	e1.PolicyEpoch = "epoch-retired-e1"
	require.NoError(t, ms.PrepareSyncControl(ctx, e1))
	require.NoError(t, ms.ActivateSyncControl(ctx, e1.RemoteChainID, e1.PolicyEpoch))
	require.NoError(t, bs.UpdateCrossFedStatus(e1.RemoteChainID, "revoked"))

	barrierCalls := 0
	m.reconcileRetiredFederationStateAfterList(ctx, ms, func(listed store.SyncControl) {
		barrierCalls++
		require.Equal(t, e1.PolicyEpoch, listed.PolicyEpoch, "reconciler must hold the stale E1 list snapshot")
		// Model a complete fresh JOIN generation landing after ListSyncControls
		// but before this stale reconciler obtains agreementMutationMu and re-reads:
		// a competing old-generation cleanup completes, then JOIN installs E2.
		m.PurgeLocalFederationState(e1.RemoteChainID)
		seedDrainAgreement(t, bs, e1.RemoteChainID, 2)
		e2 := base
		e2.PolicyEpoch = "epoch-fresh-e2"
		require.NoError(t, ms.PrepareSyncControl(ctx, e2))
		require.NoError(t, ms.ActivateSyncControl(ctx, e2.RemoteChainID, e2.PolicyEpoch))
	})
	require.Equal(t, 1, barrierCalls)
	remaining, err := ms.GetSyncControl(ctx, e1.RemoteChainID)
	require.NoError(t, err)
	require.NotNil(t, remaining)
	assert.Equal(t, "epoch-fresh-e2", remaining.PolicyEpoch,
		"a stale retired-generation cleanup must not delete a fresh re-pair")
	_, err = m.ActiveAgreement(e1.RemoteChainID)
	require.NoError(t, err, "fresh authoritative E2 agreement must remain active")
}

func TestSyncDrainV3RequiresCopyGrantAndRecipientSubscription(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "legacy-only")
	operatorID := hex.EncodeToString(m.agentPub)
	require.NoError(t, bs.RegisterAgent(operatorID, "operator", "admin", "", "test", "", 1))
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	agreement := mustDrainAgreement(t, m, "chain-b")
	require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-b", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: operatorID, PeerAgentID: hex.EncodeToString(peerPub),
		PolicyEpoch: "epoch-v3", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
	}))
	require.NoError(t, ms.ActivateSyncControl(ctx, "chain-b", "epoch-v3"))
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-b", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true, Copy: true},
	})
	require.NoError(t, err)
	_, err = ms.ApplyLocalDirectionalSyncPolicy(ctx, "chain-b", "epoch-v3",
		SyncPolicyVersionPeerRBAC, 1, "local-1", []string{"tii"}, nil)
	require.NoError(t, err)
	_, err = ms.ApplyRemoteDirectionalSyncPolicy(ctx, "chain-b", "epoch-v3",
		SyncPolicyVersionPeerRBAC, 1, "remote-1", nil, []string{"tii.project"})
	require.NoError(t, err)
	require.NoError(t, ms.MarkSyncPolicyDelivered(ctx, "chain-b", "epoch-v3", 1))

	effective, v3, err := m.pairwiseEgressPolicy(ctx, ms, agreement)
	require.NoError(t, err)
	assert.True(t, v3)
	assert.Equal(t, []string{"tii.project"}, effective, "subtree intersection must narrow to the recipient subscription")
	paused, err := m.SetPeerRBACPaused(ctx, "chain-b", true)
	require.NoError(t, err)
	assert.True(t, paused.Paused)
	effective, v3, err = m.pairwiseEgressPolicy(ctx, ms, agreement)
	require.NoError(t, err)
	assert.True(t, v3)
	assert.Empty(t, effective, "pause must stop an already-configured Copy lane without deleting it")
	resumed, err := m.SetPeerRBACPaused(ctx, "chain-b", false)
	require.NoError(t, err)
	assert.False(t, resumed.Paused)
	effective, _, err = m.pairwiseEgressPolicy(ctx, ms, agreement)
	require.NoError(t, err)
	assert.Equal(t, []string{"tii.project"}, effective, "resume must restore the saved Copy lane without re-pairing")
	require.NoError(t, ms.DeletePeerRBACPolicy(ctx, "chain-b"))
	effective, v3, err = m.pairwiseEgressPolicy(ctx, ms, agreement)
	require.NoError(t, err)
	assert.True(t, v3)
	assert.Empty(t, effective, "active v3 without an explicit PeerRBAC policy is deny-all for Copy")
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-b", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true, Copy: true},
	})
	require.NoError(t, err)

	seedCommitted(t, ms, "m-v3", "tii.project", "copy outside the legacy treaty")
	var pushed []SyncItem
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushed = append(pushed, req.Items...)
		results := make([]SyncItemResult, len(req.Items))
		for i, item := range req.Items {
			results[i] = SyncItemResult{OriginMemoryID: item.OriginMemoryID, Outcome: SyncOutcomeAccepted}
		}
		return &SyncPushResponse{Results: results}, nil
	}
	m.syncTick(ctx, ms)
	require.Len(t, pushed, 1)
	assert.Equal(t, "m-v3", pushed[0].OriginMemoryID)

	// A PeerRBAC revoke is authoritative even if a stale local_publish row still
	// exists. The transport intersection must fail closed immediately.
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-b", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true},
	})
	require.NoError(t, err)
	seedCommitted(t, ms, "m-no-copy", "tii.project", "copy permission revoked")
	pushed = nil
	m.syncTick(ctx, ms)
	assert.Empty(t, pushed)

	// Restoring Copy alone is insufficient: the recipient must still opt in.
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-b", []store.PeerRBACDomainPermission{
		{Domain: "tii", Read: true, Copy: true},
	})
	require.NoError(t, err)
	_, err = ms.ApplyRemoteDirectionalSyncPolicy(ctx, "chain-b", "epoch-v3",
		SyncPolicyVersionPeerRBAC, 2, "remote-2", nil, nil)
	require.NoError(t, err)
	seedCommitted(t, ms, "m-no-subscribe", "tii.project", "recipient unsubscribed")
	pushed = nil
	m.syncTick(ctx, ms)
	assert.Empty(t, pushed)
}

func TestLegacyPeerRBACSnapshotImmediatelyClosesLegacyCopyLane(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "legacy")
	agreement := mustDrainAgreement(t, m, "chain-b")
	peerPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-b", Role: "host", ControllerChainID: m.localChainID,
		ControllerAgentID: hex.EncodeToString(m.agentPub), PeerAgentID: hex.EncodeToString(peerPub),
		PolicyEpoch: "legacy-upgrade", RemoteCAPin: hex.EncodeToString(agreement.PeerPubKey),
		PolicyVersion: SyncPolicyVersionLegacy,
	}))
	require.NoError(t, ms.ActivateSyncControl(ctx, "chain-b", "legacy-upgrade"))
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"legacy"}))

	// The snapshot commit atomically flips the local v3 marker before the
	// dashboard publishes directional lanes. If the process stops at this exact
	// boundary, absent lanes are deny-all and stale tx-33 rows stay inert.
	_, err = m.ReplacePeerRBACPolicy(ctx, "chain-b", []store.PeerRBACDomainPermission{
		{Domain: "legacy", Read: true}, // Copy explicitly revoked.
	})
	require.NoError(t, err)
	control, err := ms.GetSyncControl(ctx, "chain-b")
	require.NoError(t, err)
	require.Equal(t, SyncPolicyVersionPeerRBAC, control.PolicyVersion)

	egress, v3, err := m.pairwiseEgressPolicy(ctx, ms, agreement)
	require.NoError(t, err)
	assert.True(t, v3, "a bound PeerRBAC snapshot must immediately become the version marker")
	assert.Empty(t, egress, "stale legacy sync_domains must not survive a Copy revoke")

	ingress, v3, err := m.pairwiseIngressPolicy(ctx, ms, agreement, hex.EncodeToString(peerPub))
	require.NoError(t, err)
	assert.True(t, v3)
	assert.Empty(t, ingress, "the same migration boundary must fail closed for inbound copies")
}

func TestSyncDrainReplicatesMemoryTags(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-tags", "hr", "tagged shared fact")
	require.NoError(t, ms.SetTags(ctx, "m-tags", []string{"eurorack", "oscillator"}))
	require.NoError(t, func() error { _, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "m-tags"); return err }())

	var got []string
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		require.Len(t, req.Items, 1)
		got = append([]string(nil), req.Items[0].Tags...)
		return &SyncPushResponse{Results: []SyncItemResult{{OriginMemoryID: "m-tags", Outcome: SyncOutcomeAccepted}}}, nil
	}
	m.syncDrain(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"})
	assert.Equal(t, []string{"eurorack", "oscillator"}, got)
}

func TestSyncDrainerStopWaitsAndRecoversClaim(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-stop", "hr", "shutdown-safe fact")
	entered := make(chan struct{})
	m.syncPushFn = func(ctx context.Context, _ string, _ *SyncPushRequest) (*SyncPushResponse, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	parent, cancel := context.WithCancel(context.Background())
	m.StartSyncDrainer(parent)
	m.StartSyncDrainer(parent) // idempotent: must not launch a second worker pair
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("drainer never entered push")
	}
	cancel()
	done := make(chan struct{})
	go func() { m.StopSyncDrainer(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("StopSyncDrainer did not join canceled workers")
	}
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Zero(t, counts[store.SyncStateDelivering])
	assert.Equal(t, 1, counts[store.SyncStatePending])
}

func TestSyncPolicyRemovalWaitsForInflightPush(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-policy-race", "hr", "policy race fact")
	m.syncScan(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"})
	entered := make(chan struct{})
	release := make(chan struct{})
	var pushes int
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushes++
		close(entered)
		<-release
		return &SyncPushResponse{Results: []SyncItemResult{{OriginMemoryID: req.Items[0].OriginMemoryID, Outcome: SyncOutcomeAccepted}}}, nil
	}
	drainDone := make(chan struct{})
	go func() { m.syncDrain(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"}); close(drainDone) }()
	<-entered
	removed := make(chan struct{})
	go func() { _ = ms.SetSyncDomains(ctx, "chain-b", nil); close(removed) }()
	select {
	case <-removed:
		t.Fatal("policy removal returned while an old-policy push was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-drainDone
	<-removed
	m.syncDrain(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"})
	assert.Equal(t, 1, pushes, "no push may begin under old consent after removal returns")
}

func TestSyncDataWaitsForHostPolicyAcknowledgement(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.PrepareSyncControl(ctx, store.SyncControl{RemoteChainID: "chain-b", Role: "host",
		ControllerChainID: m.localChainID, ControllerAgentID: "operator", PolicyEpoch: "epoch", RemoteCAPin: "pin",
		PolicyVersion: SyncPolicyVersionLegacy}))
	require.NoError(t, ms.ActivateSyncControl(ctx, "chain-b", "epoch"))
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	// Simulate a widened host snapshot that has not reached the guest yet.
	_, err := ms.ApplySyncPolicy(ctx, "chain-b", "epoch", 1, "hash", []string{"hr"})
	require.NoError(t, err)
	seedCommitted(t, ms, "m-policy-first", "hr", "policy must arrive first")
	m.syncScan(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"})
	pushes := 0
	m.syncPushFn = func(_ context.Context, _ string, _ *SyncPushRequest) (*SyncPushResponse, error) {
		pushes++
		return nil, fmt.Errorf("must not be called")
	}
	m.syncDrain(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"})
	assert.Zero(t, pushes)
	require.NoError(t, ms.MarkSyncPolicyDelivered(ctx, "chain-b", "epoch", 1))
	require.NoError(t, ms.MarkSyncOutboxRetry(ctx, "chain-b", "m-policy-first", 0, time.Now().Add(-time.Second), "due"))
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushes++
		return &SyncPushResponse{Results: []SyncItemResult{{OriginMemoryID: req.Items[0].OriginMemoryID, Outcome: SyncOutcomeAccepted}}}, nil
	}
	m.syncDrain(ctx, ms, mustDrainAgreement(t, m, "chain-b"), []string{"hr"})
	assert.Equal(t, 1, pushes)
}

func mustDrainAgreement(t *testing.T, m *Manager, chain string) *store.CrossFedRecord {
	t.Helper()
	a, err := m.ActiveAgreement(chain)
	require.NoError(t, err)
	return a
}

func TestSyncDrainOutcomeMapping(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	seedCommitted(t, ms, "m-ok", "hr", "accepted fact")
	seedCommitted(t, ms, "m-dup", "hr", "cross domain dup fact")
	seedCommitted(t, ms, "m-retry", "hr", "retry fact")

	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		results := make([]SyncItemResult, len(req.Items))
		for i, it := range req.Items {
			switch it.OriginMemoryID {
			case "m-ok":
				results[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: SyncOutcomeAccepted}
			case "m-dup":
				results[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: SyncOutcomeRejectedXDomainDup}
			default:
				results[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: SyncOutcomeRetry}
			}
		}
		return &SyncPushResponse{Results: results}, nil
	}
	m.syncTick(ctx, ms)

	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateDelivered], "accepted -> delivered")
	assert.Equal(t, 1, counts[store.SyncStateRejected], "B-D1 -> rejected, surfaced")
	assert.Equal(t, 1, counts[store.SyncStatePending], "retry -> pending with backoff")

	rejected, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStateRejected, 10)
	require.NoError(t, err)
	require.Len(t, rejected, 1)
	assert.Equal(t, SyncOutcomeRejectedXDomainDup, rejected[0].LastError)

	// The retry row backs off into the future — an immediate second tick
	// must not redeliver it.
	var pushedAgain int
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushedAgain += len(req.Items)
		return &SyncPushResponse{Results: make([]SyncItemResult, len(req.Items))}, nil
	}
	m.syncTick(ctx, ms)
	assert.Zero(t, pushedAgain)
}

func TestSyncDrainUnsupportedPeerParksRows(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-1", "hr", "fact for old peer")

	m.syncPushFn = func(_ context.Context, _ string, _ *SyncPushRequest) (*SyncPushResponse, error) {
		return nil, ErrSyncUnsupported
	}
	m.syncTick(ctx, ms)

	pending, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStatePending, 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "peer does not support sync", pending[0].LastError)
	assert.True(t, pending[0].NextAttemptAt.After(time.Now().Add(50*time.Minute)), "1h floor backoff")
}

func TestSyncDrainSendTimeGates(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	// Queue a row whose memory then gets re-domained out of scope, and one
	// whose classification is raised above the ceiling.
	seedCommitted(t, ms, "m-moved", "hr", "will be re-domained")
	seedCommitted(t, ms, "m-secret", "hr", "will be reclassified")
	_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "m-moved")
	require.NoError(t, err)
	_, err = ms.EnqueueSyncOutbox(ctx, "chain-b", "m-secret")
	require.NoError(t, err)
	// Re-domain via the documented InsertMemory upsert behavior (ON CONFLICT
	// rewrites domain_tag), then raise the other row's classification.
	sum := sha256.Sum256([]byte("will be re-domained"))
	require.NoError(t, seedCommittedMemory(ctx, ms, "m-moved", "finance", "will be re-domained", sum[:]))
	require.NoError(t, ms.UpdateMemoryClassification(ctx, "m-secret", store.ClearanceLevel(4)))

	pushes := 0
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		pushes += len(req.Items)
		return &SyncPushResponse{Results: make([]SyncItemResult, len(req.Items))}, nil
	}
	m.syncTick(ctx, ms)

	assert.Zero(t, pushes, "both rows must be dropped at send time, never pushed")
	rejected, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStateRejected, 10)
	require.NoError(t, err)
	assert.Len(t, rejected, 2)
}

func TestSyncDeliveringRowsRecoverOnStartup(t *testing.T) {
	ctx := context.Background()
	_, ms, _ := newDrainTestManager(t)

	// Simulate a crash mid-delivery: claim rows (pending->delivering) and die
	// before recording an outcome.
	for _, id := range []string{"m-1", "m-2"} {
		_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", id)
		require.NoError(t, err)
	}
	claimed, err := ms.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	require.NoError(t, err)
	require.Len(t, claimed, 2)
	counts, _ := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.Equal(t, 2, counts[store.SyncStateDelivering], "rows stuck delivering after simulated crash")

	// Startup recovery returns them to pending so they can be re-claimed.
	n, err := ms.ResetDeliveringToPending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	counts, _ = ms.CountSyncOutboxByState(ctx, "chain-b")
	assert.Equal(t, 2, counts[store.SyncStatePending])
	assert.Equal(t, 0, counts[store.SyncStateDelivering])
	reclaimed, err := ms.ClaimDueSyncOutbox(ctx, "chain-b", 10)
	require.NoError(t, err)
	assert.Len(t, reclaimed, 2, "recovered rows are claimable again")
}

func TestSyncBringUpRaceSelfHeals(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-early", "hr", "pushed before peer consented")

	// Round 1: the receiver has not configured consent yet -> not_consented.
	consented := false
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		results := make([]SyncItemResult, len(req.Items))
		for i, it := range req.Items {
			out := SyncOutcomeRejectedConsent
			if consented {
				out = SyncOutcomeAccepted
			}
			results[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: out}
		}
		return &SyncPushResponse{Results: results}, nil
	}
	m.syncTick(ctx, ms)

	// The item is NOT terminally rejected: config-dependent rejections stay
	// retryable (pending) so a later consent change delivers them.
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStatePending], "not_consented is retryable, not terminal")
	assert.Zero(t, counts[store.SyncStateRejected])

	// The row is backed off into the future; force it due to simulate the
	// next eligible tick after the operator consents.
	require.NoError(t, ms.MarkSyncOutboxRetry(ctx, "chain-b", "m-early", 1, time.Now().Add(-time.Second), "was not consented"))
	consented = true
	m.syncTick(ctx, ms)

	counts, err = ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateDelivered], "self-heals once the peer consents")
}

func TestSyncAttemptsCapFailsRow(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-stuck", "hr", "peer keeps saying retry")

	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		results := make([]SyncItemResult, len(req.Items))
		for i, it := range req.Items {
			results[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: SyncOutcomeRetry}
		}
		return &SyncPushResponse{Results: results}, nil
	}

	// Drive it up to the cap, forcing each backoff due.
	for i := 0; i < 15; i++ {
		m.syncTick(ctx, ms)
		_ = ms.MarkSyncOutboxRetry(ctx, "chain-b", "m-stuck", i+1, time.Now().Add(-time.Second), "retry")
	}
	// After enough attempts the row is failed terminal, not retried forever.
	m.syncTick(ctx, ms)
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Equal(t, 1, counts[store.SyncStateFailed], "capped -> failed, no infinite churn")
}

// TestSyncOfflinePeerNeverGivesUp is the "closed laptop overnight" regression:
// a peer that is UNREACHABLE (transport error) must keep the memory queued
// indefinitely — never marked failed — so it delivers whenever the peer returns.
// Only a peer that ACTIVELY rejects via a successful broadcast may eventually
// give up (covered by TestSyncAttemptsCapFailsRow).
func TestSyncOfflinePeerNeverGivesUp(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "m-away", "hr", "written while the peer was offline")

	// Peer is unreachable: SyncPush returns a transport error every time.
	m.syncPushFn = func(_ context.Context, _ string, _ *SyncPushRequest) (*SyncPushResponse, error) {
		return nil, fmt.Errorf("peer chain-b unreachable: dial tcp: connect: connection refused")
	}
	// Far more ticks than the attempts cap — an offline peer must NOT be failed.
	for i := 0; i < syncMaxAttempts+8; i++ {
		m.syncTick(ctx, ms)
		_ = ms.MarkSyncOutboxRetry(ctx, "chain-b", "m-away", i+1, time.Now().Add(-time.Second), "unreachable")
	}
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Zero(t, counts[store.SyncStateFailed], "an offline peer must never be given up on")
	assert.Equal(t, 1, counts[store.SyncStatePending], "the memory stays queued for when the peer returns")

	// Peer comes back online — it delivers.
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		res := make([]SyncItemResult, len(req.Items))
		for i, it := range req.Items {
			res[i] = SyncItemResult{OriginMemoryID: it.OriginMemoryID, Outcome: SyncOutcomeAccepted}
		}
		return &SyncPushResponse{Results: res}, nil
	}
	require.NoError(t, ms.MarkSyncOutboxRetry(ctx, "chain-b", "m-away", 99, time.Now().Add(-time.Second), "due"))
	m.syncTick(ctx, ms)
	counts, _ = ms.CountSyncOutboxByState(ctx, "chain-b")
	assert.Equal(t, 1, counts[store.SyncStateDelivered], "backlog delivers once the peer is reachable again")
}

func TestSyncResultsMatchedByOriginNotIndex(t *testing.T) {
	ctx := context.Background()
	m, ms, bs := newDrainTestManager(t)
	seedDrainAgreement(t, bs, "chain-b", 2, "hr")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))
	seedCommitted(t, ms, "aaa", "hr", "first")
	seedCommitted(t, ms, "bbb", "hr", "second")

	// Peer returns results in REVERSED order with distinct outcomes: aaa
	// accepted, bbb cross-domain-dup. Index-based mapping would swap them.
	m.syncPushFn = func(_ context.Context, _ string, req *SyncPushRequest) (*SyncPushResponse, error) {
		out := map[string]string{"aaa": SyncOutcomeAccepted, "bbb": SyncOutcomeRejectedXDomainDup}
		results := make([]SyncItemResult, 0, len(req.Items))
		// Emit in reverse of the request order.
		for i := len(req.Items) - 1; i >= 0; i-- {
			id := req.Items[i].OriginMemoryID
			results = append(results, SyncItemResult{OriginMemoryID: id, Outcome: out[id]})
		}
		return &SyncPushResponse{Results: results}, nil
	}
	m.syncTick(ctx, ms)

	delivered, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStateDelivered, 10)
	require.NoError(t, err)
	require.Len(t, delivered, 1)
	assert.Equal(t, "aaa", delivered[0].MemoryID, "accepted outcome bound to the right row")
	rejected, err := ms.ListSyncOutbox(ctx, "chain-b", store.SyncStateRejected, 10)
	require.NoError(t, err)
	require.Len(t, rejected, 1)
	assert.Equal(t, "bbb", rejected[0].MemoryID, "rejection bound to the right row")
}

func TestSyncBackoff(t *testing.T) {
	assert.Equal(t, 30*time.Second, syncBackoff(0))
	assert.Equal(t, time.Minute, syncBackoff(1))
	assert.Equal(t, 32*time.Minute, syncBackoff(6))
	assert.Equal(t, time.Hour, syncBackoff(7))
	assert.Equal(t, time.Hour, syncBackoff(50), "shift-capped, no overflow")
	assert.Equal(t, 30*time.Second, syncBackoff(-3))
}

// Guard: the drainer builds item hashes with the same function the receiver
// verifies with.
func TestContentHashHexMatchesReceiverCheck(t *testing.T) {
	content := "round trip"
	item := SyncItem{
		OriginChainID:  "chain-b",
		OriginMemoryID: "m-x",
		Domain:         "hr",
		Classification: 1,
		Content:        content,
		ContentHash:    contentHashHex(content),
	}
	require.NoError(t, validateSyncItem("chain-b", &item))
	item.Content += "tamper"
	require.Error(t, validateSyncItem("chain-b", &item))
	_ = fmt.Sprintf // keep fmt referenced if asserts change
}
