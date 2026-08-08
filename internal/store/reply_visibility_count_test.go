package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSummarizeCompletedForSenderMatchesGetCompletedForSenderScope pins the
// payload-free probe used by the unified inbox pointer to exactly the same
// authorization predicate as the projection it advertises. If the two ever
// diverge, the inbox would announce replies an agent cannot read, or hide
// replies it can.
func TestSummarizeCompletedForSenderMatchesGetCompletedForSenderScope(t *testing.T) {
	s, ctx := newReplyVisibilityStore(t)
	now := time.Now().UTC()
	collidingAgentID := replyVisibilitySender

	seedCompletedLocalReply(t, s, ctx, "pipe-count-a", replyVisibilitySender, replyVisibilityRecipient)
	seedCompletedLocalReply(t, s, ctx, "pipe-count-b", replyVisibilitySender, replyVisibilityRecipient)

	// Pending work by the same sender is not a reply.
	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID: "pipe-count-pending", FromAgent: replyVisibilitySender, ToAgent: replyVisibilityRecipient,
		Intent: "later", Payload: "still open", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	// An imported foreign row from a colliding agent id must not be counted.
	require.NoError(t, s.InsertPipeline(ctx, &PipelineMessage{
		PipeID: "pipe-count-imported", FromAgent: collidingAgentID, ToAgent: "local-other-recipient",
		SourceChainID: "chain-peer", SourcePipeID: "remote-count", Intent: "foreign",
		Payload: "foreign work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}))
	require.NoError(t, s.ClaimPipeline(ctx, "pipe-count-imported", "local-other-recipient"))
	require.NoError(t, s.CompletePipeline(ctx, "pipe-count-imported", "local-other-recipient", "foreign result", ""))

	items, err := s.GetCompletedForSender(ctx, replyVisibilitySender, 100)
	require.NoError(t, err)
	summary, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	assert.Equal(t, 2, summary.Count)
	assert.Equal(t, len(items), summary.Count, "the probe and the projection must agree exactly")

	// The high-water mark makes the retained snapshot actionable without any
	// server-held read state. MCP polling uses it as an inclusive `since`
	// boundary and deduplicates equal-millisecond rows by message id.
	require.NotNil(t, summary.NewestCompletedAt, "a non-zero probe must name its newest reply instant")
	require.NotNil(t, items[0].CompletedAt)
	assert.WithinDuration(t, *items[0].CompletedAt, *summary.NewestCompletedAt, time.Millisecond,
		"newest_completed_at must be the completed_at of the newest row the projection returns")

	for _, other := range []string{replyVisibilityRecipient, "local-other-recipient", "unrelated-agent", "root"} {
		otherSummary, countErr := s.SummarizeCompletedForSender(ctx, other)
		require.NoError(t, countErr)
		assert.Zero(t, otherSummary.Count, "%s must not be told a reply exists for it", other)
		assert.Nil(t, otherSummary.NewestCompletedAt,
			"%s must not learn when another agent's reply landed", other)
	}

	// The probe is passive and repeatable, like the projection.
	repeat, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	assert.Equal(t, summary.Count, repeat.Count)
	assert.Equal(t, summary.NewestCompletedAt, repeat.NewestCompletedAt)
	inbox, err := s.GetInbox(ctx, replyVisibilityRecipient, "", 10)
	require.NoError(t, err)
	require.Len(t, inbox, 1, "counting replies must not claim the sender's still-pending request")
	assert.Equal(t, "pipe-count-pending", inbox[0].PipeID)
}

// TestSummarizeCompletedForSenderKeepsCanonicalReplies is the store-level
// durability statement for canonical replies. Reading the replies changes
// nothing, and no sweeper removes a completed msg-* row
// (ExpirePipelines touches only pending/claimed; PurgePipelines excludes
// pipe_id LIKE 'msg-%'), so the total is strictly non-decreasing forever. Any
// caller that renders it as "replies are waiting, read them" is asserting a
// pending state that can never clear.
func TestSummarizeCompletedForSenderKeepsCanonicalReplies(t *testing.T) {
	s, ctx := newReplyVisibilityStore(t)

	// msg-* is the prefix every locally minted id carries (generatePipeID) and
	// every federated import carries (newImportedPipeID).
	seedCompletedLocalReply(t, s, ctx, "msg-archive-1", replyVisibilitySender, replyVisibilityRecipient)

	first, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	require.Equal(t, 1, first.Count)

	// Read the reply the way the sender would.
	read, err := s.GetCompletedForSender(ctx, replyVisibilitySender, 10)
	require.NoError(t, err)
	require.Len(t, read, 1)

	afterRead, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	assert.Equal(t, 1, afterRead.Count,
		"reading a reply cannot decrement the total: no read state exists on this path")

	// Neither expiry sweeper touches a completed row.
	_, err = s.ExpirePipelines(ctx)
	require.NoError(t, err)
	_, err = s.ExpireStalePipelines(ctx, time.Now().UTC().Add(24*time.Hour))
	require.NoError(t, err)
	// Nor does the purge sweeper, which exempts every msg-* id.
	purged, err := s.PurgePipelines(ctx, time.Now().UTC().Add(365*24*time.Hour))
	require.NoError(t, err)
	assert.Zero(t, purged, "PurgePipelines exempts msg-* rows, so a reply is retained for the life of the database")

	final, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	assert.Equal(t, 1, final.Count, "canonical msg-* replies remain retained")
}

func TestSummarizeCompletedForSenderIsCurrentRetainedTotalWhenLegacyRowsPurge(t *testing.T) {
	s, ctx := newReplyVisibilityStore(t)
	seedCompletedLocalReply(t, s, ctx, "pipe-legacy-reply", replyVisibilitySender, replyVisibilityRecipient)
	forceCompletedAt(t, s, ctx, "pipe-legacy-reply", "2026-08-01T00:00:00.000Z")
	// Retention is decided by COALESCE(terminal_at, completed_at, created_at),
	// and the stamp_pipeline_terminal_after_update trigger already set
	// terminal_at to the real completion instant. Backdating completed_at alone
	// therefore leaves the row anchored to "now" and makes this test a race
	// against the clock: PurgePipelines compares the stored millisecond stamp
	// ("…123Z") against formatTime's RFC3339Nano bound ("…123456789Z") as TEXT,
	// and 'Z' sorts above every digit, so a row stamped inside the boundary's
	// own millisecond reads as NEWER than the boundary and survives. Backdate
	// the column retention actually reads.
	forceTerminalAt(t, s, ctx, "pipe-legacy-reply", "2026-08-01T00:00:00.000Z")

	before, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	require.Equal(t, 1, before.Count)

	purged, err := s.PurgePipelines(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	after, err := s.SummarizeCompletedForSender(ctx, replyVisibilitySender)
	require.NoError(t, err)
	assert.Zero(t, after.Count,
		"deprecated pipe-* replies age out, so the aggregate is a retained snapshot rather than a lifetime monotonic counter")
}
