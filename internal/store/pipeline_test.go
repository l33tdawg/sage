package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
