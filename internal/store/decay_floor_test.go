package store

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decayRec builds a committed record with an embedding for the store DecayFloor
// tests. These tests exercise the REAL store filter (applyDecayFloor + the
// filter-before-trim wiring in QuerySimilar/SearchByText/SearchHybrid), which the
// api/rest tests can't reach because they run against the mock's reimplementation.
func decayRec(id, content string, conf float64, created time.Time, emb []float32) *memory.MemoryRecord {
	h := sha256.Sum256([]byte(content))
	return &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "agent1",
		Content:         content,
		ContentHash:     h[:],
		MemoryType:      memory.TypeFact,
		DomainTag:       "general",
		ConfidenceScore: conf,
		Status:          memory.StatusCommitted,
		CreatedAt:       created,
		Embedding:       emb,
	}
}

func idSet(recs []*memory.MemoryRecord) map[string]bool {
	m := make(map[string]bool, len(recs))
	for _, r := range recs {
		m[r.MemoryID] = true
	}
	return m
}

// TestQuerySimilar_DecayFloor is the core store-level guard: the floor is enforced
// on the task-aware DECAYED confidence, dropping an aged sub-floor memory while
// keeping a fresh one AND a corroboration-boosted one whose stored value is below
// the floor. Mutating applyDecayFloor's comparison must fail this test (the api/rest
// suite can't, since it runs against the mock).
func TestQuerySimilar_DecayFloor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	emb := []float32{0.5, 0.5, 0.5}

	require.NoError(t, s.InsertMemory(ctx, decayRec("aged", "aged", 0.95, now.Add(-130*24*time.Hour), emb)))
	require.NoError(t, s.InsertMemory(ctx, decayRec("fresh", "fresh", 0.95, now, emb)))
	require.NoError(t, s.InsertMemory(ctx, decayRec("boosted", "boosted", 0.65, now, emb)))
	require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{MemoryID: "boosted", AgentID: "a2", CreatedAt: now}))
	require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{MemoryID: "boosted", AgentID: "a3", CreatedAt: now}))

	// Sanity on the decay math for this fixture.
	require.Less(t, memory.ComputeConfidence(0.95, now.Add(-130*24*time.Hour), now, 0, "general"), 0.70)
	require.Greater(t, memory.ComputeConfidence(0.65, now, now, 2, "general"), 0.70)

	got, err := s.QuerySimilar(ctx, emb, QueryOptions{TopK: 10, DecayFloor: 0.70, DecayNow: now})
	require.NoError(t, err)
	ids := idSet(got)
	assert.NotContains(t, ids, "aged", "aged memory decayed below the floor must be dropped by the store")
	assert.Contains(t, ids, "fresh")
	assert.Contains(t, ids, "boosted", "corroboration-boosted memory above the decayed floor must survive")
}

// TestQuerySimilar_DecayFloor_FillsTopK proves the store drops sub-floor records
// BEFORE the top-K trim: aged has the highest similarity, but with top_k=1 the one
// qualifying record still comes back rather than the slot being eaten by the dropped
// aged one.
func TestQuerySimilar_DecayFloor_FillsTopK(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// aged is the closest match to the query; fresh is farther.
	require.NoError(t, s.InsertMemory(ctx, decayRec("aged", "aged", 0.95, now.Add(-130*24*time.Hour), []float32{1, 0, 0})))
	require.NoError(t, s.InsertMemory(ctx, decayRec("fresh", "fresh", 0.95, now, []float32{0.7, 0.7, 0})))

	got, err := s.QuerySimilar(ctx, []float32{1, 0, 0}, QueryOptions{TopK: 1, DecayFloor: 0.70, DecayNow: now})
	require.NoError(t, err)
	require.Len(t, got, 1, "top_k=1 must be filled by the qualifying record, not the higher-ranked dropped one")
	assert.Equal(t, "fresh", got[0].MemoryID)
}

// TestQuerySimilar_DecayFloor_OpenTaskExempt: open tasks don't decay, so an aged
// open task passes the floor while an equally-aged normal fact is dropped.
func TestQuerySimilar_DecayFloor_OpenTaskExempt(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	emb := []float32{0.5, 0.5, 0.5}

	task := decayRec("task", "open task", 0.90, now.Add(-200*24*time.Hour), emb)
	task.MemoryType = memory.TypeTask
	task.TaskStatus = memory.TaskStatusPlanned
	require.NoError(t, s.InsertMemory(ctx, task))
	require.NoError(t, s.InsertMemory(ctx, decayRec("fact", "aged fact", 0.90, now.Add(-200*24*time.Hour), emb)))

	got, err := s.QuerySimilar(ctx, emb, QueryOptions{TopK: 10, DecayFloor: 0.70, DecayNow: now})
	require.NoError(t, err)
	ids := idSet(got)
	assert.Contains(t, ids, "task", "an open task must not be decayed out of a floored recall")
	assert.NotContains(t, ids, "fact", "an equally-aged normal fact must be dropped")
}

// TestQuerySimilar_DecayFloor_FailsClosedOnCorroborationError: if the corroboration
// count fetch fails under a floor, the store returns an error rather than silently
// filtering on a boost-less (understated) confidence.
func TestQuerySimilar_DecayFloor_FailsClosedOnCorroborationError(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	emb := []float32{0.5, 0.5, 0.5}
	require.NoError(t, s.InsertMemory(ctx, decayRec("m1", "m1", 0.9, now, emb)))

	// Force GetCorroborationCounts to error by removing the table it reads.
	_, err := s.conn.ExecContext(ctx, "DROP TABLE corroborations")
	require.NoError(t, err)

	_, err = s.QuerySimilar(ctx, emb, QueryOptions{TopK: 10, DecayFloor: 0.70, DecayNow: now})
	require.Error(t, err, "a corroboration-count failure under a floor must fail closed, not silently drop the boost")

	// Without a floor the same query must still succeed (no corroboration fetch).
	_, err = s.QuerySimilar(ctx, emb, QueryOptions{TopK: 10})
	require.NoError(t, err, "the no-floor path must not depend on the corroborations table")
}

// TestSearchByText_DecayFloor pins the floor for the FTS path (over-fetch pool then
// Go-filter before trim).
func TestSearchByText_DecayFloor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	require.NoError(t, s.InsertMemory(ctx, decayRec("aged", "widget maintenance note", 0.95, now.Add(-130*24*time.Hour), nil)))
	require.NoError(t, s.InsertMemory(ctx, decayRec("fresh", "widget rollout note", 0.95, now, nil)))

	got, err := s.SearchByText(ctx, "widget", QueryOptions{TopK: 10, DecayFloor: 0.70, DecayNow: now})
	require.NoError(t, err)
	ids := idSet(got)
	assert.NotContains(t, ids, "aged", "aged memory below the floor must be dropped by the FTS path")
	assert.Contains(t, ids, "fresh")
}

// TestSearchByText_DecayFloor_FillsTopK proves the FTS path over-fetches and filters
// BEFORE the top-K trim: an aged sub-floor record that ranks FIRST by BM25 must not
// eat the single top_k slot — the qualifying fresh record fills it. Fails if the
// over-fetch (scanLimit=decayFilterScanCap) ever reverts to scanLimit=opts.TopK.
func TestSearchByText_DecayFloor_FillsTopK(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// The exact term "widget" outranks the longer "widget rollout ..." for query
	// "widget" (higher BM25 term density), so aged is fetched first under a LIMIT.
	require.NoError(t, s.InsertMemory(ctx, decayRec("aged", "widget", 0.95, now.Add(-130*24*time.Hour), nil)))
	require.NoError(t, s.InsertMemory(ctx, decayRec("fresh", "widget rollout note today", 0.95, now, nil)))

	got, err := s.SearchByText(ctx, "widget", QueryOptions{TopK: 1, DecayFloor: 0.70, DecayNow: now})
	require.NoError(t, err)
	require.Len(t, got, 1, "top_k=1 must be filled by the qualifying record, not the higher-ranked dropped one")
	assert.Equal(t, "fresh", got[0].MemoryID)
}

// TestSearchHybrid_DecayFloor pins that DecayFloor rides subOpts into BOTH fused
// leaves: a sub-floor aged record reachable via the BM25 (text) leaf must not
// surface even when it matches the query text.
func TestSearchHybrid_DecayFloor(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	// aged matches the query text and has an embedding; fresh likewise. If the floor
	// failed to reach either leaf, aged would surface.
	require.NoError(t, s.InsertMemory(ctx, decayRec("aged", "hybrid widget note", 0.95, now.Add(-130*24*time.Hour), []float32{0.5, 0.5, 0.5})))
	require.NoError(t, s.InsertMemory(ctx, decayRec("fresh", "hybrid widget note two", 0.95, now, []float32{0.5, 0.5, 0.5})))

	got, err := s.SearchHybrid(ctx, "widget", []float32{0.5, 0.5, 0.5}, QueryOptions{TopK: 10, DecayFloor: 0.70, DecayNow: now})
	require.NoError(t, err)
	ids := idSet(got)
	assert.NotContains(t, ids, "aged", "aged memory below the floor must be dropped from hybrid recall via both leaves")
	assert.Contains(t, ids, "fresh")
}

// TestQuerySimilar_NoDecayFloor_Unfiltered confirms the opt-in nature: with no floor
// the aged memory is returned and no corroboration lookup is required.
func TestQuerySimilar_NoDecayFloor_Unfiltered(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	emb := []float32{0.5, 0.5, 0.5}
	require.NoError(t, s.InsertMemory(ctx, decayRec("aged", "aged", 0.95, now.Add(-200*24*time.Hour), emb)))

	got, err := s.QuerySimilar(ctx, emb, QueryOptions{TopK: 10})
	require.NoError(t, err)
	assert.Contains(t, idSet(got), "aged", "no floor => aged memory is returned unfiltered")
}
