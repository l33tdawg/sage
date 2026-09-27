package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/vault"
)

func gateMemory(t *testing.T, s *SQLiteStore, id, content string, status memory.MemoryStatus) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agent", content, "general-notes")))
	if status != memory.StatusProposed {
		require.NoError(t, s.UpdateStatus(ctx, id, status, time.Now().UTC()))
	}
}

func TestMemoryGate_VerdictsAreCachedPerJudgeVersion(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	gateMemory(t, s, "m1", "The build server moved to the east rack in March.", memory.StatusProposed)
	require.NoError(t, s.RecordSemanticVerdict(ctx, "m1", "judges-v1",
		memory.SemanticVerdict{Verdict: memory.VerdictPass, P: 0.97, Reason: "lasting p=0.97"}))

	v, ok, err := s.SemanticVerdict(ctx, "m1", "judges-v1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, memory.VerdictPass, v.Verdict)
	require.InDelta(t, 0.97, v.P, 1e-9)

	_, ok, err = s.SemanticVerdict(ctx, "m1", "judges-v2")
	require.NoError(t, err)
	require.False(t, ok, "a verdict from another judge version is never reused")
}

func TestMemoryGate_ReviewFlow(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	gateMemory(t, s, "m-pass", "The build server moved to the east rack in March.", memory.StatusProposed)
	gateMemory(t, s, "m-held", "The quarterly figure was around two hundred units.", memory.StatusProposed)
	require.NoError(t, s.RecordSemanticVerdict(ctx, "m-pass", "v1", memory.SemanticVerdict{Verdict: memory.VerdictPass, P: 0.97}))
	require.NoError(t, s.RecordSemanticVerdict(ctx, "m-held", "v1",
		memory.SemanticVerdict{Verdict: memory.VerdictAbstain, P: 0.7, Reason: "held for review: lasting-memory uncertain (p=0.70)"}))

	queue, err := s.ReviewQueue(ctx, "v1", ReviewQueueCursor{}, 10)
	require.NoError(t, err)
	require.Len(t, queue, 1)
	require.Equal(t, "m-held", queue[0].MemoryID)
	require.Contains(t, queue[0].Reason, "p=0.70")

	other, err := s.ReviewQueue(ctx, "v2", ReviewQueueCursor{}, 10)
	require.NoError(t, err)
	require.Empty(t, other, "the queue shows only memories held under the current judge version")

	require.ErrorIs(t, s.SetReviewDecision(ctx, "m-pass", "v1", "accept", "operator", ""), ErrNotAwaitingReview)
	require.Error(t, s.SetReviewDecision(ctx, "m-held", "v1", "maybe", "operator", ""))
	require.NoError(t, s.SetReviewDecision(ctx, "m-held", "v1", "accept", "operator", "checked the source"))
	require.ErrorIs(t, s.SetReviewDecision(ctx, "m-held", "v1", "reject", "operator", ""), ErrNotAwaitingReview,
		"a decision is final")

	d, ok, err := s.ReviewDecision(ctx, "m-held")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "accept", d)

	queue, err = s.ReviewQueue(ctx, "v1", ReviewQueueCursor{}, 10)
	require.NoError(t, err)
	require.Empty(t, queue, "a decided memory leaves the queue")

	vs, err := s.GateVerdicts(ctx, "m-held")
	require.NoError(t, err)
	require.Len(t, vs, 1)
}

func TestMemoryGate_NoCiphertextEverLeaves(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "memory-gate-test"))
	v, err := vault.Open(keyPath, "memory-gate-test")
	require.NoError(t, err)
	s.SetVault(v)
	gateMemory(t, s, "m-enc", "The depot opens at 07:00 on weekdays.", memory.StatusProposed)

	content, err := s.JudgeableContent(ctx, "m-enc")
	require.NoError(t, err)
	require.Equal(t, "The depot opens at 07:00 on weekdays.", content)
	rec, available, err := s.ReviewMemory(ctx, "m-enc")
	require.NoError(t, err)
	require.True(t, available)
	require.Equal(t, "The depot opens at 07:00 on weekdays.", rec.Content)

	s.SetVault(nil) // locked
	_, err = s.JudgeableContent(ctx, "m-enc")
	require.ErrorIs(t, err, ErrContentUnavailable, "a locked vault must never send ciphertext or the placeholder to a judge")
	rec, available, err = s.ReviewMemory(ctx, "m-enc")
	require.NoError(t, err)
	require.False(t, available)
	require.Empty(t, rec.Content, "review content is cleared, not replaced by ciphertext or a placeholder")
}

func TestMemoryGate_ReviewCursorPreservesTimestampAndTies(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	for _, id := range []string{"c", "a", "b"} {
		gateMemory(t, s, id, "Lasting knowledge about "+id, memory.StatusProposed)
		require.NoError(t, s.RecordSemanticVerdict(ctx, id, "v1", memory.SemanticVerdict{Verdict: memory.VerdictAbstain, P: 0.7}))
	}
	// Preserve the exact stored representation as well as the ID tie-breaker.
	const at = "2026-09-27T07:00:00.000000000Z"
	_, err := s.writeExecContext(ctx, `UPDATE memory_gate_verdicts SET created_at = ?`, at)
	require.NoError(t, err)
	first, err := s.ReviewQueue(ctx, "v1", ReviewQueueCursor{}, 1)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, "a", first[0].MemoryID)
	require.Equal(t, at, first[0].Cursor.CreatedAt)
	require.NoError(t, s.SetReviewDecision(ctx, "a", "v1", "accept", "operator", ""))
	next, err := s.ReviewQueue(ctx, "v1", first[0].Cursor, 10)
	require.NoError(t, err)
	require.Len(t, next, 2)
	require.Equal(t, "b", next[0].MemoryID)
	require.Equal(t, "c", next[1].MemoryID)
	_, err = s.ReviewQueue(ctx, "v2", first[0].Cursor, 10)
	require.Error(t, err)
}
