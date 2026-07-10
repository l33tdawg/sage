package store

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// The now-expired stale row is purgeable by the retention sweep.
	purged, err := s.PurgePipelines(ctx, now.Add(-24*time.Hour))
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

	// Purge
	purged, err := s.PurgePipelines(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, purged)
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
