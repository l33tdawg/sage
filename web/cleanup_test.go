package web

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func cleanupTestRecord(t *testing.T, f appV23DashboardRouteFixture, id string) {
	t.Helper()
	hash := sha256.Sum256([]byte(id))
	r := &memory.MemoryRecord{MemoryID: id, SubmittingAgent: f.ids["member"], Content: id,
		ContentHash: hash[:], MemoryType: memory.TypeObservation, DomainTag: "member.home",
		ConfidenceScore: 0.85, Status: memory.StatusCommitted, CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	require.NoError(t, f.sql.InsertMemory(context.Background(), r))
	publishAppV23DashboardRecord(t, f.sql, f.badger, id, uint8(store.ClearanceInternal), true)
}

func cleanupTestPolicy(t *testing.T, f appV23DashboardRouteFixture) cleanupPolicy {
	t.Helper()
	root, _, broker := f.handler.appV23RootBrokerKey()
	require.True(t, broker.Available, broker.Message)
	p := cleanupPolicy{Config: memory.DefaultCleanupConfig(), Credential: root.CredentialID, Generation: root.Generation}
	return p
}

func TestCanonicalCleanupPlanHasNo500Cap(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	for i := 0; i < 2051; i++ {
		cleanupTestRecord(t, f, fmt.Sprintf("cleanup-%04d", i))
	}
	run, err := f.handler.cleanupPlan(context.Background(), cleanupPolicy{Config: memory.DefaultCleanupConfig()})
	require.NoError(t, err)
	require.True(t, run.Complete)
	require.Equal(t, 2051, run.Checked)
	require.Equal(t, 2051, run.Eligible)
	require.Len(t, run.IDs, 2051)
	require.Equal(t, "cleanup-2050", run.IDs[2050], "equal timestamps must cross multiple pages without omissions")
	require.Zero(t, run.Deprecated)
	r, err := f.sql.GetMemory(context.Background(), "cleanup-1000")
	require.NoError(t, err)
	require.Equal(t, memory.StatusCommitted, r.Status)
}

func TestCanonicalCleanupExecutesBeyond500(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	ctx := context.Background()
	for i := 0; i < 501; i++ {
		cleanupTestRecord(t, f, fmt.Sprintf("execute-%04d", i))
	}
	seen := make(map[string]bool)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		id := parsed.MemoryChallenge.MemoryID
		require.False(t, seen[id], "no duplicate target")
		seen[id] = true
		rec, err := f.sql.GetMemory(ctx, id)
		require.NoError(t, err)
		require.NoError(t, f.badger.SetMemoryHash(id, rec.ContentHash, string(memory.StatusDeprecated)))
		writeCommitOKAt(w, r, int64(10+len(seen)), "")
	}))
	t.Cleanup(rpc.Close)
	f.handler.CometBFTRPC = rpc.URL
	require.NoError(t, f.handler.cleanupWrite(ctx, cleanupRunKey, cleanupRun{State: "queued", Policy: cleanupTestPolicy(t, f)}))
	for i := 0; i < 502; i++ {
		f.handler.cleanupTick(ctx)
	}
	var run cleanupRun
	require.NoError(t, f.handler.cleanupRead(ctx, cleanupRunKey, &run))
	require.Equal(t, "completed", run.State)
	require.Equal(t, 501, run.Submitted)
	require.Equal(t, 501, run.Deprecated)
	require.Len(t, seen, 501)
	rec, err := f.sql.GetMemory(ctx, "execute-0500")
	require.NoError(t, err)
	require.Equal(t, memory.StatusCommitted, rec.Status, "worker never writes local lifecycle")
}

func TestCanonicalCleanupEligibility(t *testing.T) {
	now := time.Now()
	cfg := memory.DefaultCleanupConfig()
	r := memory.MemoryRecord{Status: memory.StatusCommitted, MemoryType: memory.TypeFact,
		DomainTag: "general", CreatedAt: now, ConfidenceScore: 0.01}
	require.True(t, cleanupEligible(&r, cfg, now, 0), "low confidence facts are included")
	r.ConfidenceScore = 0.99
	require.False(t, cleanupEligible(&r, cfg, now, 0))
	r.MemoryType = memory.TypeObservation
	r.CreatedAt = now.Add(-3 * 24 * time.Hour)
	r.DomainTag = "session-context"
	require.True(t, cleanupEligible(&r, cfg, now, 0))
	r.DomainTag = "general"
	require.False(t, cleanupEligible(&r, cfg, now, 0))
	r.Status = memory.StatusDeprecated
	r.ConfidenceScore = 0.01
	require.False(t, cleanupEligible(&r, cfg, now, 0))
	require.False(t, cleanupEligible(nil, cfg, now, 0))
	r.Status = memory.StatusCommitted
	r.MemoryType = memory.TypeTask
	r.TaskStatus = memory.TaskStatusInProgress
	require.False(t, cleanupEligible(&r, cfg, now, 0), "open tasks never qualify")
}

func TestCanonicalCleanupWorkerWaitsForCanonicalOutcome(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	cleanupTestRecord(t, f, "cleanup-one")
	var calls int
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		require.Equal(t, tx.TxTypeMemoryChallenge, parsed.Type)
		require.Equal(t, "cleanup-one", parsed.MemoryChallenge.MemoryID)
		calls++
		writeCommitOKAt(w, r, 10, "")
	}))
	t.Cleanup(rpc.Close)
	f.handler.CometBFTRPC = rpc.URL
	p := cleanupTestPolicy(t, f)
	ctx := context.Background()
	require.NoError(t, f.handler.cleanupWrite(ctx, cleanupRunKey, cleanupRun{State: "queued", Policy: p}))
	f.handler.cleanupTick(ctx)
	require.Equal(t, 1, calls, "must not resubmit pending work")
	var run cleanupRun
	require.NoError(t, f.handler.cleanupRead(ctx, cleanupRunKey, &run))
	require.Equal(t, "confirmation_pending", run.State)
	require.Equal(t, 1, run.Submitted)
	require.Zero(t, run.Deprecated)
	require.NotEmpty(t, run.PendingBytes, "exact signed bytes survive a restart")
	parsed, err := tx.DecodeTx(run.PendingBytes)
	require.NoError(t, err)
	valid, err := tx.VerifyTx(parsed)
	require.NoError(t, err)
	require.True(t, valid)
	rec, err := f.sql.GetMemory(ctx, "cleanup-one")
	require.NoError(t, err)
	require.Equal(t, memory.StatusCommitted, rec.Status, "no local lifecycle mutation")
	require.NoError(t, f.badger.SetMemoryHash(rec.MemoryID, rec.ContentHash, string(memory.StatusDeprecated)))
	f.handler.cleanupTick(ctx)
	require.NoError(t, f.handler.cleanupRead(ctx, cleanupRunKey, &run))
	require.Equal(t, "completed", run.State)
	require.True(t, run.Complete)
	require.Equal(t, 1, run.Deprecated)
	require.Equal(t, 1, calls)
}

func TestCanonicalCleanupDisableReconcilesPendingWithoutNextSubmission(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	ctx := context.Background()
	p := cleanupTestPolicy(t, f)
	p.Config.Enabled = true
	cleanupTestRecord(t, f, "first")
	cleanupTestRecord(t, f, "second")
	rec, err := f.sql.GetMemory(ctx, "first")
	require.NoError(t, err)
	require.NoError(t, f.badger.SetMemoryHash(rec.MemoryID, rec.ContentHash, string(memory.StatusChallenged)))
	run := cleanupRun{State: "confirmation_pending", Policy: p, Automatic: true,
		Pending: "first", PendingConfirmed: true, Submitted: 1, IDs: []string{"first", "second"}, Eligible: 2}
	require.NoError(t, f.handler.cleanupWrite(ctx, cleanupRunKey, run))
	p.Config.Enabled = false
	require.NoError(t, f.handler.cleanupWrite(ctx, cleanupPolicyKey, p))
	f.handler.cleanupTick(ctx)
	var got cleanupRun
	require.NoError(t, f.handler.cleanupRead(ctx, cleanupRunKey, &got))
	require.Equal(t, "cancelled", got.State)
	require.Empty(t, got.Pending)
	require.Equal(t, 1, got.Challenged)
	require.Equal(t, 1, got.Next)
	require.Equal(t, 1, got.Submitted)
}

func TestCanonicalCleanupPreviewDoesNotAuthorizeAutomation(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	f.handler.CometBFTRPC = "http://127.0.0.1:1"
	cleanupTestRecord(t, f, "preview-only")
	response := appV23CanonicalMutationRequest(t, f, http.MethodPost, "/v1/dashboard/cleanup/run",
		[]byte(`{"dry_run":true,"config":{"enabled":true,"observation_ttl_days":7,"session_ttl_days":2,"stale_threshold":0.1,"cleanup_interval_hours":24}}`))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"eligible":1`)
	for _, key := range []string{cleanupPolicyKey, cleanupRunKey} {
		value, err := f.sql.GetPreference(context.Background(), key)
		require.NoError(t, err)
		require.Empty(t, value)
	}
}

func TestCanonicalCleanupDurabilityFailurePreventsBroadcast(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	var calls int
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		writeCommitOKAt(w, r, 10, "")
	}))
	t.Cleanup(rpc.Close)
	f.handler.CometBFTRPC = rpc.URL
	_, _, _, err := f.handler.signAndBroadcastCommitDurable(context.Background(),
		&tx.ParsedTx{Type: tx.TxTypeMemoryChallenge, MemoryChallenge: &tx.MemoryChallenge{MemoryID: "never-send", Reason: "test"}},
		f.keys["current-root"], nil, func(encoded []byte) error {
			require.NotEmpty(t, encoded)
			return errors.New("disk full")
		})
	require.ErrorContains(t, err, "disk full")
	require.Zero(t, calls)
}

func TestCanonicalCleanupRecoversExactPersistedTransaction(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	ctx := context.Background()
	p := cleanupTestPolicy(t, f)
	cleanupTestRecord(t, f, "restart")
	parsed := &tx.ParsedTx{Type: tx.TxTypeMemoryChallenge, Timestamp: time.Now(), Nonce: 123,
		MemoryChallenge: &tx.MemoryChallenge{MemoryID: "restart", Reason: "operator-authorized memory cleanup"}}
	require.NoError(t, tx.SignTx(parsed, f.keys["current-root"]))
	encoded, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	hash := tx.CometTxHash(encoded)
	var lookups int
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/tx", r.URL.Path, "indexed outcome needs no resubmission")
		require.Equal(t, "0x"+strings.ToUpper(hex.EncodeToString(hash[:])), r.URL.Query().Get("hash"))
		lookups++
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": "", "result": map[string]any{
			"hash": hex.EncodeToString(hash[:]), "height": "10", "tx_result": map[string]any{"code": 0},
		}}))
	}))
	t.Cleanup(rpc.Close)
	f.handler.CometBFTRPC = rpc.URL
	rec, err := f.sql.GetMemory(ctx, "restart")
	require.NoError(t, err)
	require.NoError(t, f.badger.SetMemoryHash(rec.MemoryID, rec.ContentHash, string(memory.StatusDeprecated)))
	require.NoError(t, f.handler.cleanupWrite(ctx, cleanupRunKey, cleanupRun{
		State: "confirmation_pending", Policy: p, IDs: []string{"restart"}, Eligible: 1,
		Pending: "restart", PendingBytes: encoded,
	}))
	f.handler.cleanupTick(ctx)
	var got cleanupRun
	require.NoError(t, f.handler.cleanupRead(ctx, cleanupRunKey, &got))
	require.Equal(t, "completed", got.State)
	require.Equal(t, 1, got.Submitted)
	require.Equal(t, 1, got.Deprecated)
	require.Equal(t, 1, lookups)
	require.NotContains(t, got.public(), "pending_bytes")
	require.NotContains(t, got.public(), "policy")
}

func TestCanonicalCleanupIgnoresLegacyOptIn(t *testing.T) {
	f := newAppV23DashboardRouteFixture(t)
	ctx := context.Background()
	require.NoError(t, f.sql.SetPreference(ctx, "cleanup_enabled", "true"))
	f.handler.cleanupTick(ctx)
	value, err := f.sql.GetPreference(ctx, cleanupRunKey)
	require.NoError(t, err)
	require.Empty(t, value)
}
