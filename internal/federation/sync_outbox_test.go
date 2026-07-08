package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
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
	seedCommitted(t, ms, "m-eng", "eng", "eng fact")          // outside consent
	seedCommitted(t, ms, "m-queued", "hr", "queued fact")     // already queued
	seedCommitted(t, ms, "m-copy", "hr", "synced copy fact")  // a copy: never re-forward
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
