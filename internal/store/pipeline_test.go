package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

type queryCountingSQLQuerier struct {
	sqlQuerier
	queryContextCalls    int
	queryRowContextCalls int
}

func (q *queryCountingSQLQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.queryContextCalls++
	return q.sqlQuerier.QueryContext(ctx, query, args...)
}

func (q *queryCountingSQLQuerier) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.queryRowContextCalls++
	return q.sqlQuerier.QueryRowContext(ctx, query, args...)
}

func TestGetAgentDirectoryEntriesUsesOneMetadataQueryAndMatchesGetAgentLegacySemantics(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "agent-directory.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	for _, agent := range []*AgentEntry{
		{AgentID: "agent-a", Name: "Mutable A", RegisteredName: "", Provider: "claude-code", Status: "active"},
		{AgentID: "agent-b", Name: "Mutable B", RegisteredName: "saved/b", Provider: "codex", Status: "active"},
		{AgentID: "agent-removed", Name: "Former agent", RegisteredName: "saved/removed", Provider: "codex", Status: "removed"},
	} {
		require.NoError(t, s.CreateAgent(ctx, agent))
	}

	legacyViaGet, err := s.GetAgent(ctx, "agent-a")
	require.NoError(t, err)
	require.Equal(t, "Mutable A", legacyViaGet.RegisteredName)

	queries := &queryCountingSQLQuerier{sqlQuerier: s.conn}
	s.conn = queries
	entries, err := s.GetAgentDirectoryEntries(ctx, []string{
		"agent-a", "agent-b", "agent-a", "agent-removed", "agent-unknown",
	})
	require.NoError(t, err)
	require.Equal(t, 1, queries.queryContextCalls,
		"a deduplicated exact-ID batch must execute one metadata query")
	require.Zero(t, queries.queryRowContextCalls,
		"the projection must not degrade into per-agent query-row reads")
	require.Len(t, entries, 3,
		"unknown IDs stay absent while exact historical removed rows remain available")
	require.Equal(t, "agent-a", entries[0].AgentID)
	require.Equal(t, legacyViaGet.RegisteredName, entries[0].RegisteredName,
		"batch and GetAgent must expose identical legacy compatibility semantics")
	require.Equal(t, "saved/b", entries[1].RegisteredName)
	require.Equal(t, "agent-removed", entries[2].AgentID,
		"the exact metadata projection must not add a status filter")
}

// TestPipelineSizeCaps verifies the E8c size guards at the store boundary,
// including the off-by-one at exactly the cap (which must pass).
func TestPipelineSizeCaps(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	mk := func(id, payload, intent string) *PipelineMessage {
		return &PipelineMessage{
			PipeID:    id,
			FromAgent: "agent-alice",
			ToAgent:   "agent-bob",
			Intent:    intent,
			Payload:   payload,
			Status:    "pending",
			CreatedAt: now,
			ExpiresAt: now.Add(time.Hour),
		}
	}

	// Payload exactly at the cap is accepted.
	require.NoError(t, s.InsertPipeline(ctx, mk("pipe-cap-ok", strings.Repeat("x", MaxPipeContentBytes), "ok")))
	// One byte over is rejected.
	err = s.InsertPipeline(ctx, mk("pipe-cap-over", strings.Repeat("x", MaxPipeContentBytes+1), "over"))
	require.ErrorIs(t, err, ErrPipePayloadTooLarge)
	// Intent over the cap is rejected.
	err = s.InsertPipeline(ctx, mk("pipe-intent-over", "small", strings.Repeat("i", MaxPipeIntentBytes+1)))
	require.ErrorIs(t, err, ErrPipeIntentTooLarge)

	// Result cap: claim a pipe then try to complete it with an oversized result.
	require.NoError(t, s.InsertPipeline(ctx, mk("pipe-result", "work", "task")))
	require.NoError(t, s.ClaimPipeline(ctx, "pipe-result", "agent-bob"))
	err = s.CompletePipeline(ctx, "pipe-result", "agent-bob", strings.Repeat("r", MaxPipeContentBytes+1), "journal-x")
	require.ErrorIs(t, err, ErrPipeResultTooLarge)
	// A result exactly at the cap completes.
	require.NoError(t, s.CompletePipeline(ctx, "pipe-result", "agent-bob", strings.Repeat("r", MaxPipeContentBytes), "journal-y"))
}

// TestPipelineQuotaPerAgent verifies the per-requester open-pipe cap, the
// off-by-one at the boundary, per-agent isolation, and that terminal pipes free
// quota back up.
func TestPipelineQuotaPerAgent(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	insert := func(agent, id string) error {
		return s.InsertPipeline(ctx, &PipelineMessage{
			PipeID:    id,
			FromAgent: agent,
			ToAgent:   "agent-bob",
			Intent:    "task",
			Payload:   "work",
			Status:    "pending",
			CreatedAt: now,
			ExpiresAt: now.Add(time.Hour),
		})
	}

	// Fill exactly to the cap — all succeed.
	for i := 0; i < MaxOpenPipesPerAgent; i++ {
		require.NoError(t, insert("agent-alice", "pipe-alice-"+strconv.Itoa(i)))
	}
	// The cap+1th open pipe is rejected.
	err = insert("agent-alice", "pipe-alice-over")
	require.ErrorIs(t, err, ErrPipeQuotaPerAgent)

	// A different requester is unaffected (per-agent isolation).
	require.NoError(t, insert("agent-carol", "pipe-carol-0"))

	// Draining an open pipe to a terminal state frees quota back up.
	require.NoError(t, s.ClaimPipeline(ctx, "pipe-alice-0", "agent-bob"))
	require.NoError(t, s.CompletePipeline(ctx, "pipe-alice-0", "agent-bob", "done", "journal-z"))
	require.NoError(t, insert("agent-alice", "pipe-alice-refill"))
}

// TestPipelineQuotaPerAgentConcurrent proves the quota check and INSERT are one
// serialized operation. Without that critical section, a parallel burst can
// have every request observe a below-cap count and all enqueue successfully.
func TestPipelineQuotaPerAgentConcurrent(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "pipes.db"))
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	var succeeded, quotaRejected, unexpected atomic.Int64
	var wg sync.WaitGroup
	const burstExtra = 64
	for i := 0; i < MaxOpenPipesPerAgent+burstExtra; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := s.InsertPipeline(ctx, &PipelineMessage{
				PipeID:    "pipe-concurrent-" + strconv.Itoa(i),
				FromAgent: "agent-alice",
				ToAgent:   "agent-bob",
				Intent:    "task",
				Payload:   "work",
				Status:    "pending",
				CreatedAt: now,
				ExpiresAt: now.Add(time.Hour),
			})
			switch {
			case err == nil:
				succeeded.Add(1)
			case errors.Is(err, ErrPipeQuotaPerAgent):
				quotaRejected.Add(1)
			default:
				unexpected.Add(1)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(MaxOpenPipesPerAgent), succeeded.Load())
	assert.Equal(t, int64(burstExtra), quotaRejected.Load())
	assert.Zero(t, unexpected.Load())
	var open int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_messages WHERE from_agent = ? AND status IN ('pending','claimed')`,
		"agent-alice").Scan(&open))
	assert.Equal(t, MaxOpenPipesPerAgent, open)
}

// TestPipelineStaleExpiry verifies ExpireStalePipelines force-expires an old
// never-claimed pipe even when its TTL is set far in the future, while leaving a
// fresh pipe untouched — the hard lifetime backstop from E8c.
func TestPipelineStaleExpiry(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	// Old pipe with a deliberately oversized TTL — TTL-based ExpirePipelines
	// would never reap it.
	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID:    "pipe-stale",
		FromAgent: "agent-alice",
		ToAgent:   "agent-bob",
		Intent:    "task",
		Payload:   "old work",
		Status:    "pending",
		CreatedAt: now.Add(-72 * time.Hour),
		ExpiresAt: now.Add(240 * time.Hour), // far future
	}))
	// Fresh pipe — must survive the staleness sweep.
	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID:    "pipe-fresh",
		FromAgent: "agent-alice",
		ToAgent:   "agent-bob",
		Intent:    "task",
		Payload:   "new work",
		Status:    "pending",
		CreatedAt: now,
		ExpiresAt: now.Add(time.Hour),
	}))

	// TTL-based expiry leaves both alone (neither is past its expires_at).
	n, err := s.ExpirePipelines(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// Staleness sweep flips only the old one.
	n, err = s.ExpireStalePipelines(ctx, now.Add(-48*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	stale, err := s.GetPipeline(ctx, "pipe-stale")
	require.NoError(t, err)
	assert.Equal(t, "expired", stale.Status)
	fresh, err := s.GetPipeline(ctx, "pipe-fresh")
	require.NoError(t, err)
	assert.Equal(t, "pending", fresh.Status)

	// A freshly-expired row gets the full terminal retention window even though
	// its original created_at is old.
	purged, err := s.PurgePipelines(ctx, now.Add(-24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 0, purged)

	// Once the terminal transition itself is older than the retention cutoff,
	// the row is purgeable.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(now.Add(-25*time.Hour)), "pipe-stale")
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, now.Add(-24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, 1, purged)
}

func TestPipelineRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	// Insert a pipeline message
	now := time.Now().UTC()
	msg := &PipelineMessage{
		PipeID:       "pipe-test-001",
		FromAgent:    "agent-alice",
		FromProvider: "claude-code",
		ToAgent:      "",
		ToProvider:   "perplexity",
		Intent:       "research",
		Payload:      "Find BFT papers from 2024",
		Status:       "pending",
		CreatedAt:    now,
		ExpiresAt:    now.Add(1 * time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))

	// Get it back
	got, err := s.GetPipeline(ctx, "pipe-test-001")
	require.NoError(t, err)
	assert.Equal(t, "pipe-test-001", got.PipeID)
	assert.Equal(t, "claude-code", got.FromProvider)
	assert.Equal(t, "perplexity", got.ToProvider)
	assert.Equal(t, "research", got.Intent)
	assert.Equal(t, "pending", got.Status)

	// Inbox — should show up for perplexity provider
	inbox, err := s.GetInbox(ctx, "agent-bob", "perplexity", 10)
	require.NoError(t, err)
	assert.Len(t, inbox, 1)
	assert.Equal(t, "pipe-test-001", inbox[0].PipeID)

	// Inbox — should NOT show up for chatgpt
	inbox2, err := s.GetInbox(ctx, "agent-charlie", "chatgpt", 10)
	require.NoError(t, err)
	assert.Len(t, inbox2, 0)

	// Claim it
	require.NoError(t, s.ClaimPipeline(ctx, "pipe-test-001", "agent-bob"))
	gotClaimed, err := s.GetPipeline(ctx, "pipe-test-001")
	require.NoError(t, err)
	assert.Equal(t, "agent-bob", gotClaimed.ClaimedBy)

	// Double claim should fail
	err = s.ClaimPipeline(ctx, "pipe-test-001", "agent-charlie")
	assert.Error(t, err)
	err = s.CompletePipeline(ctx, "pipe-test-001", "agent-charlie", "forged result", "journal-forged")
	assert.Error(t, err)

	// Should no longer appear in inbox
	inbox3, err := s.GetInbox(ctx, "agent-bob", "perplexity", 10)
	require.NoError(t, err)
	assert.Len(t, inbox3, 0)

	// Claiming takes an item out of the actionable queue, never out of the
	// recipient's retained history. The recipient must be able to reopen the
	// original request before completing it.
	inboxHistory, err := s.GetInboxHistory(ctx, "agent-bob", "perplexity", 10)
	require.NoError(t, err)
	require.Len(t, inboxHistory, 1)
	assert.Equal(t, "pipe-test-001", inboxHistory[0].PipeID)
	assert.Equal(t, "claimed", inboxHistory[0].Status)
	assert.Equal(t, "Find BFT papers from 2024", inboxHistory[0].Payload)
	assert.Equal(t, "agent-bob", inboxHistory[0].ClaimedBy)

	outbox, err := s.GetOutbox(ctx, "agent-alice", 10)
	require.NoError(t, err)
	require.Len(t, outbox, 1)
	assert.Equal(t, "claimed", outbox[0].Status)
	assert.Equal(t, "Find BFT papers from 2024", outbox[0].Payload)

	// Complete it
	require.NoError(t, s.CompletePipeline(ctx, "pipe-test-001", "agent-bob", "Found 5 papers", "journal-001"))

	// Get completed — should show result
	got2, err := s.GetPipeline(ctx, "pipe-test-001")
	require.NoError(t, err)
	assert.Equal(t, "completed", got2.Status)
	assert.Equal(t, "Found 5 papers", got2.Result)
	assert.Equal(t, "journal-001", got2.JournalID)
	assert.NotNil(t, got2.CompletedAt)

	// GetCompletedForSender
	completed, err := s.GetCompletedForSender(ctx, "agent-alice", 10)
	require.NoError(t, err)
	assert.Len(t, completed, 1)
	assert.Equal(t, "Found 5 papers", completed[0].Result)

	// Both parties retain a readable transcript after completion. This is a
	// passive view: it does not re-queue or re-claim the completed item.
	inboxHistory, err = s.GetInboxHistory(ctx, "agent-bob", "perplexity", 10)
	require.NoError(t, err)
	require.Len(t, inboxHistory, 1)
	assert.Equal(t, "completed", inboxHistory[0].Status)
	assert.Equal(t, "Found 5 papers", inboxHistory[0].Result)

	outbox, err = s.GetOutbox(ctx, "agent-alice", 10)
	require.NoError(t, err)
	require.Len(t, outbox, 1)
	assert.Equal(t, "completed", outbox[0].Status)
	assert.Equal(t, "Found 5 papers", outbox[0].Result)

	// ListPipelines — all
	all, err := s.ListPipelines(ctx, "", 50)
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// ListPipelines — filter by status
	pending, err := s.ListPipelines(ctx, "pending", 50)
	require.NoError(t, err)
	assert.Len(t, pending, 0)

	completedList, err := s.ListPipelines(ctx, "completed", 50)
	require.NoError(t, err)
	assert.Len(t, completedList, 1)

	// Stats
	stats, err := s.PipelineStats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, stats["completed"])
}

func TestPipelineProviderHistoryLocksClaimedWorkToClaimant(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	defer s.Close()

	now := time.Now().UTC()
	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID:     "provider-history",
		FromAgent:  "sender",
		ToProvider: "shared-provider",
		Intent:     "review",
		Payload:    "private work request",
		Status:     "pending",
		CreatedAt:  now,
		ExpiresAt:  now.Add(time.Hour),
	}))
	require.NoError(t, s.ClaimPipeline(ctx, "provider-history", "claimant"))

	claimantHistory, err := s.GetInboxHistory(ctx, "claimant", "shared-provider", 10)
	require.NoError(t, err)
	require.Len(t, claimantHistory, 1)
	assert.Equal(t, "provider-history", claimantHistory[0].PipeID)

	otherProviderHistory, err := s.GetInboxHistory(ctx, "other-provider-agent", "shared-provider", 10)
	require.NoError(t, err)
	assert.Empty(t, otherProviderHistory, "provider peers must not browse work after another agent claimed it")
}

func TestPipelineVaultEncryptionAndFederationProvenance(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "pipes.db"))
	require.NoError(t, err)
	defer s.Close()
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyFile, "pipeline-passphrase"))
	v, err := vault.Open(keyFile, "pipeline-passphrase")
	require.NoError(t, err)
	s.SetVault(v)

	now := time.Now().UTC()
	msg := &PipelineMessage{
		PipeID: "pipe-foreign", FromAgent: strings.Repeat("a", 64), ToAgent: strings.Repeat("b", 64),
		Intent: "analyze", Payload: "sensitive transient work", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		SourceChainID: "chain-peer", SourcePipeID: "peer-pipe-7",
		FederationPolicyEpoch: "epoch-7", FederationAgreementID: strings.Repeat("c", 64),
		FederationContactID: strings.Repeat("d", 64),
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))
	var rawIntent, rawPayload string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT intent, payload FROM pipeline_messages WHERE pipe_id=?`, msg.PipeID).Scan(&rawIntent, &rawPayload))
	assert.NotEqual(t, msg.Intent, rawIntent)
	assert.NotEqual(t, msg.Payload, rawPayload)
	assert.True(t, strings.HasPrefix(rawIntent, encPrefix))
	assert.True(t, strings.HasPrefix(rawPayload, encPrefix))

	got, err := s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	assert.Equal(t, msg.Intent, got.Intent)
	assert.Equal(t, msg.Payload, got.Payload)
	assert.Equal(t, msg.SourceChainID, got.SourceChainID)
	assert.Equal(t, msg.SourcePipeID, got.SourcePipeID)
	assert.Equal(t, msg.FederationPolicyEpoch, got.FederationPolicyEpoch)
	assert.Equal(t, msg.FederationAgreementID, got.FederationAgreementID)
	assert.Equal(t, msg.FederationContactID, got.FederationContactID)

	require.NoError(t, s.ClaimPipeline(ctx, msg.PipeID, msg.ToAgent))
	require.NoError(t, s.CompletePipeline(ctx, msg.PipeID, msg.ToAgent, "sensitive result", ""))
	var rawResult string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT result FROM pipeline_messages WHERE pipe_id=?`, msg.PipeID).Scan(&rawResult))
	assert.NotEqual(t, "sensitive result", rawResult)
	assert.True(t, strings.HasPrefix(rawResult, encPrefix))

	s.SetVault(nil)
	s.SetVaultExpected(true)
	_, err = s.GetPipeline(ctx, msg.PipeID)
	require.ErrorIs(t, err, ErrPipeContentUnavailable)
}

func TestPipelineFederationNamespacesCannotEnterLocalInboxOrResults(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	now := time.Now().UTC()
	collidingAgentID := "same-agent-id"

	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID: "pipe-outbound", FromAgent: "local-sender", ToAgent: collidingAgentID,
		DestinationChainID: "chain-peer", Intent: "remote", Payload: "work", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	inbox, err := s.GetInbox(ctx, collidingAgentID, "", 10)
	require.NoError(t, err)
	assert.Empty(t, inbox, "outbound remote rows must never become locally claimable")

	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID: "pipe-imported", FromAgent: collidingAgentID, ToAgent: "local-recipient",
		SourceChainID: "chain-peer", SourcePipeID: "remote-id", Intent: "foreign", Payload: "work", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	inbox, err = s.GetInbox(ctx, "local-recipient", "", 10)
	require.NoError(t, err)
	require.Len(t, inbox, 1)
	require.Equal(t, "chain-peer", inbox[0].SourceChainID)
	require.NoError(t, s.ClaimPipeline(ctx, "pipe-imported", "local-recipient"))
	require.NoError(t, s.CompletePipeline(ctx, "pipe-imported", "local-recipient", "done", ""))
	results, err := s.GetCompletedForSender(ctx, collidingAgentID, 10)
	require.NoError(t, err)
	assert.Empty(t, results, "a foreign sender id must not collide into a local sender's result list")
}

func TestPipelineExpiry(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	// Insert an already-expired message
	msg := &PipelineMessage{
		PipeID:       "pipe-expired-001",
		FromAgent:    "agent-alice",
		FromProvider: "claude-code",
		ToProvider:   "chatgpt",
		Intent:       "test",
		Payload:      "this should expire",
		Status:       "pending",
		CreatedAt:    now.Add(-2 * time.Hour),
		ExpiresAt:    now.Add(-1 * time.Hour), // Already expired
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))

	// Expire
	n, err := s.ExpirePipelines(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Verify expired
	got, err := s.GetPipeline(ctx, "pipe-expired-001")
	require.NoError(t, err)
	assert.Equal(t, "expired", got.Status)

	// Expiry starts a fresh terminal retention window; the original send and
	// expires_at timestamps must not make the row disappear immediately.
	purged, err := s.PurgePipelines(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 0, purged)
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(now.Add(-time.Hour)), msg.PipeID)
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, purged)
}

func TestPurgePipelinesConservativelyFloorsSubMillisecondCutoff(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	cutoff := time.Date(2026, time.August, 9, 1, 2, 3, 500_000, time.UTC)
	msg := &PipelineMessage{
		PipeID: "pipe-after-whole-second-cutoff", FromAgent: "agent-alice", ToAgent: "agent-bob",
		Intent: "test", Payload: "retain me", Status: "pending",
		CreatedAt: cutoff.Add(-time.Hour), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))
	require.NoError(t, s.ClaimPipeline(ctx, msg.PipeID, msg.ToAgent))
	require.NoError(t, s.CompletePipeline(ctx, msg.PipeID, msg.ToAgent, "done", ""))

	// SQLite resolves fractional seconds only to milliseconds. A terminal event
	// objectively newer than the nanosecond cutoff but in its floor millisecond
	// must never round across the boundary and disappear.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(cutoff.Add(200*time.Microsecond)), msg.PipeID)
	require.NoError(t, err)
	purged, err := s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged)
	_, err = s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)

	// An older instant in the same ambiguous millisecond is retained
	// conservatively. The next sweep reclaims it once the cutoff crosses a
	// representable millisecond boundary.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(cutoff.Add(-200*time.Microsecond)), msg.PipeID)
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged)

	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(cutoff.Add(-2*time.Millisecond)), msg.PipeID)
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, 1, purged)
}

func TestPurgePipelinesUsesTheConservativeBoundaryForOutboxAndParent(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	cutoff := time.Date(2026, time.August, 9, 1, 2, 3, 500_000, time.UTC)
	proof := testPipelineTransportProof(t)
	msg := &PipelineMessage{
		PipeID: "pipe-retention-outbox", FromAgent: proof.AgentID, ToAgent: strings.Repeat("b", 64),
		DestinationChainID: "peer-chain", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "work", Status: "pending",
		CreatedAt: cutoff.Add(-time.Hour), ExpiresAt: cutoff.Add(time.Hour),
	}
	event := &PipelineTransportOutbox{
		EventID: "event-retention-outbox", PipeID: msg.PipeID, RemoteChainID: msg.DestinationChainID,
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		SourceAgentID: msg.FromAgent, TargetAgentID: msg.ToAgent, Proof: proof,
		CreatedAt: cutoff.Add(-time.Hour), ExpiresAt: cutoff.Add(time.Hour),
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, msg, event))
	require.NoError(t, s.MarkPipelineTransportDelivered(ctx, event.EventID))
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET status='expired', terminal_at=? WHERE pipe_id=?`,
		formatTime(cutoff.Add(200*time.Microsecond)), msg.PipeID)
	require.NoError(t, err)

	purged, err := s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged)
	var outboxRows int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_transport_outbox WHERE event_id=?`, event.EventID).Scan(&outboxRows))
	require.Equal(t, 1, outboxRows, "stage one must retain transport metadata with its newer parent")

	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(cutoff.Add(-2*time.Millisecond)), msg.PipeID)
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_transport_outbox WHERE event_id=?`, event.EventID).Scan(&outboxRows))
	require.Zero(t, outboxRows, "eligible transport metadata and its parent must purge atomically")
}

func TestPurgePipelinesTreatsAmbiguousOrMalformedReadReceiptAsRetention(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	cutoff := time.Date(2026, time.August, 9, 1, 2, 3, 500_000, time.UTC)
	msg := &PipelineMessage{
		PipeID: "pipe-read-receipt-retention", FromAgent: "agent-alice", ToAgent: "agent-bob",
		Payload: "work", Status: "pending", CreatedAt: cutoff.Add(-time.Hour), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))
	require.NoError(t, s.ClaimPipeline(ctx, msg.PipeID, msg.ToAgent))
	require.NoError(t, s.CompletePipeline(ctx, msg.PipeID, msg.ToAgent, "done", ""))
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(cutoff.Add(-2*time.Millisecond)), msg.PipeID)
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx,
		`INSERT INTO message_read_receipts(message_id,receiver_agent_id,read_at) VALUES(?,?,?)`,
		msg.PipeID, msg.ToAgent, formatTime(cutoff.Add(200*time.Microsecond)))
	require.NoError(t, err)

	purged, err := s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "a fresh receipt in the cutoff millisecond must retain")
	_, err = s.writeExecContext(ctx, `UPDATE message_read_receipts SET read_at='not-a-time' WHERE message_id=?`, msg.PipeID)
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "malformed retained evidence must fail safe instead of authorizing deletion")

	_, err = s.writeExecContext(ctx, `UPDATE message_read_receipts SET read_at=? WHERE message_id=?`,
		formatTime(cutoff.Add(-2*time.Millisecond)), msg.PipeID)
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Equal(t, 1, purged, "an unambiguously old valid receipt must not retain the parent")
}

func TestPipelineDirectAgentRouting(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	now := time.Now().UTC()
	msg := &PipelineMessage{
		PipeID:    "pipe-direct-001",
		FromAgent: "agent-alice",
		ToAgent:   "agent-bob-specific",
		Intent:    "review",
		Payload:   "review this code",
		Status:    "pending",
		CreatedAt: now,
		ExpiresAt: now.Add(1 * time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))

	// Should show up for agent-bob-specific
	inbox, err := s.GetInbox(ctx, "agent-bob-specific", "any-provider", 10)
	require.NoError(t, err)
	assert.Len(t, inbox, 1)

	// Should NOT show up for other agents
	inbox2, err := s.GetInbox(ctx, "agent-charlie", "any-provider", 10)
	require.NoError(t, err)
	assert.Len(t, inbox2, 0)
}

func TestGetAgentByName(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()

	// Register an agent
	agent := &AgentEntry{
		AgentID:   "deadbeef01234567890abcdef01234567890abcdef01234567890abcdef012345",
		Name:      "claude-code/sage",
		Role:      "assistant",
		Status:    "active",
		Clearance: 5,
		Provider:  "claude-code",
	}
	require.NoError(t, s.CreateAgent(ctx, agent))

	// Look up by name — should find it
	found, err := s.GetAgentByName(ctx, "claude-code/sage")
	require.NoError(t, err)
	require.NotNil(t, found)
	assert.Equal(t, agent.AgentID, found.AgentID)
	assert.Equal(t, "claude-code", found.Provider)

	// Look up non-existent name — should return nil, nil
	notFound, err := s.GetAgentByName(ctx, "nonexistent/agent")
	require.NoError(t, err)
	assert.Nil(t, notFound)
}
