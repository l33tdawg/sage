package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newOutcomeTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "outcome.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestMemoryOutcome_StateSyncTreatsRowsAsLocal pins that the table is
// node-local: an empty table is pristine, and a receiving node with feedback
// rows is refused like any other populated off-chain table, so state sync can
// never carry or silently keep them.
func TestMemoryOutcome_StateSyncTreatsRowsAsLocal(t *testing.T) {
	ctx := context.Background()
	s := newOutcomeTestStore(t)
	if err := s.RequirePristineStateSyncProjection(ctx); err != nil {
		t.Fatalf("fresh store with empty outcome table not pristine: %v", err)
	}
	if err := s.RecordMemoryOutcome(ctx, "m1", "a1", MemoryOutcomeHelped, time.Now(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.RequirePristineStateSyncProjection(ctx); err == nil {
		t.Fatal("populated outcome table passed the pristine state-sync check")
	}
}

func TestMemoryOutcome_RecordAndAggregate(t *testing.T) {
	ctx := context.Background()
	s := newOutcomeTestStore(t)
	t0 := time.Unix(1_700_000_000, 0)
	steps := []struct {
		agent, outcome string
		at             time.Time
	}{
		{"a1", MemoryOutcomeHelped, t0},
		{"a1", MemoryOutcomeHelped, t0.Add(11 * time.Second)},
		{"a2", MemoryOutcomeHurt, t0},
		{"a3", MemoryOutcomeUnused, t0},
	}
	for _, st := range steps {
		if err := s.RecordMemoryOutcome(ctx, "m1", st.agent, st.outcome, st.at, 10*time.Second); err != nil {
			t.Fatalf("record %s/%s: %v", st.agent, st.outcome, err)
		}
	}
	stats, err := s.GetMemoryOutcomeStats(ctx, []string{"m1", "m2"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := stats["m1"]
	if !ok {
		t.Fatal("m1 stats missing")
	}
	if got.Helped != 2 || got.Hurt != 1 || got.Unused != 1 || got.Agents != 3 {
		t.Fatalf("stats = %+v", got)
	}
	if !got.LastAt.Equal(t0.Add(11 * time.Second)) {
		t.Fatalf("last_at = %v", got.LastAt)
	}
	if _, has := stats["m2"]; has {
		t.Fatal("m2 has no reports and must be absent")
	}
}

func TestMemoryOutcome_RateLimitChangesNothing(t *testing.T) {
	ctx := context.Background()
	s := newOutcomeTestStore(t)
	t0 := time.Unix(1_700_000_000, 0)
	if err := s.RecordMemoryOutcome(ctx, "m1", "a1", MemoryOutcomeHelped, t0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	err := s.RecordMemoryOutcome(ctx, "m1", "a1", MemoryOutcomeHurt, t0.Add(9*time.Second), 10*time.Second)
	if !errors.Is(err, ErrMemoryOutcomeRateLimited) {
		t.Fatalf("second report = %v, want rate limited", err)
	}
	// Another agent and another memory are independent.
	if err := s.RecordMemoryOutcome(ctx, "m1", "a2", MemoryOutcomeHurt, t0.Add(time.Second), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordMemoryOutcome(ctx, "m2", "a1", MemoryOutcomeHurt, t0.Add(time.Second), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GetMemoryOutcomeStats(ctx, []string{"m1"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if st := stats["m1"]; st.Helped != 1 || st.Hurt != 1 || !st.LastAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("refused report leaked into stats: %+v", st)
	}
}

func TestMemoryOutcome_PerAgentCap(t *testing.T) {
	ctx := context.Background()
	s := newOutcomeTestStore(t)
	t0 := time.Unix(1_700_000_000, 0)
	for i := 0; i < 20; i++ {
		if err := s.RecordMemoryOutcome(ctx, "m1", "spammer", MemoryOutcomeHelped, t0.Add(time.Duration(i)*time.Minute), 10*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.RecordMemoryOutcome(ctx, "m1", "other", MemoryOutcomeHurt, t0, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GetMemoryOutcomeStats(ctx, []string{"m1"}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if st := stats["m1"]; st.Helped != 5 || st.Hurt != 1 || st.Agents != 2 {
		t.Fatalf("cap not applied: %+v", st)
	}
}

func TestMemoryOutcome_RejectsBadInput(t *testing.T) {
	ctx := context.Background()
	s := newOutcomeTestStore(t)
	now := time.Now()
	if err := s.RecordMemoryOutcome(ctx, "m1", "a1", "great", now, time.Second); err == nil {
		t.Fatal("unknown outcome accepted")
	}
	if err := s.RecordMemoryOutcome(ctx, "", "a1", MemoryOutcomeHelped, now, time.Second); err == nil {
		t.Fatal("empty memory id accepted")
	}
	if err := s.RecordMemoryOutcome(ctx, "m1", "", MemoryOutcomeHelped, now, time.Second); err == nil {
		t.Fatal("empty agent accepted")
	}
}

// TestMemoryOutcome_NeverTouchesMemoryRecord pins that feedback is off the
// record: content hash, stored confidence and status are unchanged.
func TestMemoryOutcome_NeverTouchesMemoryRecord(t *testing.T) {
	ctx := context.Background()
	s := newOutcomeTestStore(t)
	rec := testMemory("m-outcome", "agent1", "outcome content", "general")
	if err := s.InsertMemory(ctx, rec); err != nil {
		t.Fatal(err)
	}
	before, err := s.GetMemory(ctx, rec.MemoryID)
	if err != nil {
		t.Fatal(err)
	}
	for i, o := range []string{MemoryOutcomeHelped, MemoryOutcomeHurt, MemoryOutcomeUnused} {
		if err := s.RecordMemoryOutcome(ctx, rec.MemoryID, "a1", o, time.Unix(int64(1_700_000_000+60*i), 0), time.Second); err != nil {
			t.Fatal(err)
		}
	}
	after, err := s.GetMemory(ctx, rec.MemoryID)
	if err != nil {
		t.Fatal(err)
	}
	if string(before.ContentHash) != string(after.ContentHash) ||
		before.ConfidenceScore != after.ConfidenceScore ||
		before.Status != after.Status || before.Content != after.Content {
		t.Fatalf("memory record changed: before=%+v after=%+v", before, after)
	}
}
