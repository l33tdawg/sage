package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/l33tdawg/sage/internal/voter"
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

func TestMemoryGate_EvidenceIsNodeLocalBoundedAndNeverCiphertext(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	_, ok, err := s.JudgeableEvidence(ctx, "m1")
	require.NoError(t, err)
	require.False(t, ok, "no evidence means the evidence check is not asked, not empty evidence")

	_, err = s.CreateMemoryEvidence(ctx, "agent-a", string(make([]byte, MaxEvidenceBytes+1)))
	require.Error(t, err)
	_, err = s.CreateMemoryEvidence(ctx, "agent-a", "")
	require.Error(t, err)

	keyPath := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyPath, "evidence-test"))
	v, err := vault.Open(keyPath, "evidence-test")
	require.NoError(t, err)
	s.SetVault(v)
	const ev = "Minutes of the 3 March meeting: the rack move was approved."
	id, err := s.CreateMemoryEvidence(ctx, "agent-a", ev)
	require.NoError(t, err)
	var raw string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT evidence FROM memory_evidence WHERE evidence_id = ?`, id).Scan(&raw))
	require.NotContains(t, raw, "rack move", "evidence is encrypted at rest like memory content")

	_, ok, err = s.JudgeableEvidence(ctx, "m1")
	require.NoError(t, err)
	require.False(t, ok, "uploaded evidence belongs to no memory until a submission claims it")
	require.NoError(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", "m1"))
	got, ok, err := s.JudgeableEvidence(ctx, "m1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ev, got)

	s.SetVault(nil)
	_, _, err = s.JudgeableEvidence(ctx, "m1")
	require.ErrorIs(t, err, ErrContentUnavailable, "locked evidence fails; it is never reported as absent or sent as ciphertext")
}

func TestMemoryGate_EvidenceIsClaimedOnceByItsOwnAgent(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	id, err := s.CreateMemoryEvidence(ctx, "agent-a", "Datasheet: rated power 40 kW.")
	require.NoError(t, err)
	require.ErrorIs(t, s.ClaimMemoryEvidence(ctx, id, "agent-b", "m1"), ErrEvidenceUnavailable, "another agent cannot claim it")
	require.ErrorIs(t, s.ClaimMemoryEvidence(ctx, "ev-unknown", "agent-a", "m1"), ErrEvidenceUnavailable)
	require.NoError(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", "m1"))
	require.ErrorIs(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", "m2"), ErrEvidenceUnavailable, "one id, one memory")

	old, err := s.CreateMemoryEvidence(ctx, "agent-a", "stale upload")
	require.NoError(t, err)
	_, err = s.conn.ExecContext(ctx, `UPDATE memory_evidence SET created_at = ? WHERE evidence_id = ?`,
		time.Now().UTC().Add(-2*UnclaimedEvidenceTTL).Format(time.RFC3339Nano), old)
	require.NoError(t, err)
	require.ErrorIs(t, s.ClaimMemoryEvidence(ctx, old, "agent-a", "m3"), ErrEvidenceUnavailable, "expired evidence cannot be claimed")

	for i := 0; i < MaxUnclaimedEvidence; i++ {
		_, err = s.CreateMemoryEvidence(ctx, "agent-c", "evidence")
		require.NoError(t, err)
	}
	_, err = s.CreateMemoryEvidence(ctx, "agent-c", "one too many")
	require.ErrorIs(t, err, ErrTooMuchUnclaimedEvidence)
	_, err = s.CreateMemoryEvidence(ctx, "agent-a", "other agents are not affected")
	require.NoError(t, err)
}

func TestMemoryGate_EvidenceRetention(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	claim := func(memoryID string) string {
		t.Helper()
		id, err := s.CreateMemoryEvidence(ctx, "agent-a", "evidence for "+memoryID)
		require.NoError(t, err)
		require.NoError(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", memoryID))
		return id
	}
	has := func(memoryID string) bool {
		t.Helper()
		_, ok, err := s.JudgeableEvidence(ctx, memoryID)
		require.NoError(t, err)
		return ok
	}
	gateMemory(t, s, "m-proposed", "Held for review.", memory.StatusProposed)
	gateMemory(t, s, "m-committed", "Voted and committed.", memory.StatusProposed)
	gateMemory(t, s, "m-deprecated", "Voted down.", memory.StatusProposed)
	for _, id := range []string{"m-proposed", "m-committed", "m-deprecated", "m-fresh-orphan", "m-old-orphan"} {
		claim(id)
	}
	require.NoError(t, s.UpdateStatus(ctx, "m-committed", memory.StatusCommitted, time.Now().UTC()))
	require.NoError(t, s.UpdateStatus(ctx, "m-deprecated", memory.StatusDeprecated, time.Now().UTC()))
	_, err := s.conn.ExecContext(ctx, `UPDATE memory_evidence SET claimed_at = ? WHERE memory_id = ?`,
		time.Now().UTC().Add(-2*OrphanedEvidenceGrace).Format(time.RFC3339Nano), "m-old-orphan")
	require.NoError(t, err)

	require.NoError(t, s.PruneMemoryEvidence(ctx, time.Now()))
	require.True(t, has("m-proposed"), "a proposed memory keeps its evidence for judging and review")
	require.False(t, has("m-committed"), "a decided memory's evidence is deleted")
	require.False(t, has("m-deprecated"), "a decided memory's evidence is deleted")
	require.True(t, has("m-fresh-orphan"), "a claim whose transaction may still commit is kept within the grace")

	// Past the grace the TEXT is deleted, but the claim stays as an expired
	// marker: the memory's absence does not prove it can never arrive.
	_, ok, err := s.JudgeableEvidence(ctx, "m-old-orphan")
	require.ErrorIs(t, err, memory.ErrEvidenceExpired)
	require.True(t, ok, "expired evidence is never reported as absent")
	var raw string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT evidence FROM memory_evidence WHERE memory_id = ?`, "m-old-orphan").Scan(&raw))
	require.Empty(t, raw, "the evidence text itself is deleted")

	// If the memory does arrive and is decided, the marker goes with it.
	gateMemory(t, s, "m-old-orphan", "Arrived late.", memory.StatusCommitted)
	require.NoError(t, s.PruneMemoryEvidence(ctx, time.Now()))
	_, ok, err = s.JudgeableEvidence(ctx, "m-old-orphan")
	require.NoError(t, err)
	require.False(t, ok)
}

// fixedLastingJudge and fixedSupportJudge answer with a fixed probability.
type fixedLastingJudge float64

func (p fixedLastingJudge) LastingProbability(context.Context, string) (float64, error) {
	return float64(p), nil
}

type countingSupportJudge struct {
	p     float64
	calls atomic.Int64
}

func (j *countingSupportJudge) SupportedProbability(context.Context, string, string) (float64, error) {
	j.calls.Add(1)
	return j.p, nil
}

// TestMemoryGate_DelayedMemoryWithExpiredEvidenceIsHeldNotPassed is the
// reviewer's reproduction: evidence that does not support a claim, claimed
// for a memory whose projection arrives only after the retention grace. With
// the evidence text gone the memory must NOT silently take the no-evidence
// path (which would pass it); it is held for review.
func TestMemoryGate_DelayedMemoryWithExpiredEvidenceIsHeldNotPassed(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		t.Run(fmt.Sprintf("delayed=%v", delayed), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := newTestStore(t)
			evID, err := s.CreateMemoryEvidence(ctx, "agent-a", "At the 2023 inspection the alarm was disabled.")
			require.NoError(t, err)
			require.NoError(t, s.ClaimMemoryEvidence(ctx, evID, "agent-a", "m-late"))
			if delayed {
				require.NoError(t, s.PruneMemoryEvidence(ctx, time.Now().Add(25*time.Hour)))
			}
			gateMemory(t, s, "m-late", "The alarm is disabled.", memory.StatusProposed)

			support := &countingSupportJudge{p: 0.01}
			g := &voter.Gate{Judges: []voter.LastingJudge{fixedLastingJudge(0.99)},
				SupportJudges: []voter.SupportJudge{support}, Version: "delayed-v1"}
			g.Start(ctx, s, zerolog.Nop())
			rec, err := s.GetMemory(ctx, "m-late")
			require.NoError(t, err)
			_, _ = g.Apply(ctx, s, rec, voter.Decision{Accept: true, Reason: "passes all checks"}, zerolog.Nop())
			var v memory.SemanticVerdict
			require.Eventually(t, func() bool {
				var ok bool
				v, ok, _ = s.SemanticVerdict(ctx, "m-late", "delayed-v1")
				return ok
			}, 5*time.Second, 10*time.Millisecond)
			if !delayed {
				require.Equal(t, memory.VerdictReject, v.Verdict, "evidence present: judged unsupported")
				require.EqualValues(t, 1, support.calls.Load())
				return
			}
			require.Equal(t, memory.VerdictAbstain, v.Verdict, "evidence expired: held for review, never passed")
			require.Contains(t, v.Reason, "expired")
			require.Zero(t, support.calls.Load())
		})
	}
}

func TestMemoryGate_ReleasedEvidenceCanBeClaimedAgain(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	id, err := s.CreateMemoryEvidence(ctx, "agent-a", "Datasheet: rated power 40 kW.")
	require.NoError(t, err)
	require.NoError(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", "m-unsent"))
	require.NoError(t, s.ReleaseMemoryEvidence(ctx, id, "other-memory"), "a release names the exact claim")
	require.ErrorIs(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", "m-retry"), ErrEvidenceUnavailable)

	require.NoError(t, s.ReleaseMemoryEvidence(ctx, id, "m-unsent"))
	_, ok, err := s.JudgeableEvidence(ctx, "m-unsent")
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, s.ClaimMemoryEvidence(ctx, id, "agent-a", "m-retry"), "the retry claims the same evidence")
	got, ok, err := s.JudgeableEvidence(ctx, "m-retry")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "Datasheet: rated power 40 kW.", got)
}
