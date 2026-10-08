package store

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeReranker rescores by giving any candidate whose content contains a
// magic substring a perfect score and everything else a near-zero score.
// That lets the test prove the reranker pass actually changes the order
// returned to the caller, without depending on a real model.
type fakeReranker struct {
	winnerSubstr string
	calls        int
	lastQuery    string
	lastTexts    []string
}

func (f *fakeReranker) Rerank(_ context.Context, query string, texts []string) ([]embedding.RerankResult, error) {
	f.calls++
	f.lastQuery = query
	f.lastTexts = texts
	out := make([]embedding.RerankResult, len(texts))
	for i, t := range texts {
		score := 0.01
		if strings.Contains(t, f.winnerSubstr) {
			score = 0.99
		}
		out[i] = embedding.RerankResult{Index: i, Score: score}
	}
	return out, nil
}

type errReranker struct{ msg string }

func (e *errReranker) Rerank(_ context.Context, _ string, _ []string) ([]embedding.RerankResult, error) {
	return nil, errRerankUpstream{msg: e.msg}
}

type errRerankUpstream struct{ msg string }

func (e errRerankUpstream) Error() string { return e.msg }

type scoredReranker struct{ scores []embedding.RerankResult }

func (r scoredReranker) Rerank(context.Context, string, []string) ([]embedding.RerankResult, error) {
	return r.scores, nil
}

func TestApplyRerankerTopKBounds(t *testing.T) {
	tests := []struct {
		name       string
		candidates int
		topK       int
		want       int
	}{
		{"empty pool", 0, math.MaxInt, 0},
		{"minimum integer", 3, math.MinInt, 0},
		{"negative", 3, -1, 0},
		{"zero", 3, 0, 0},
		{"one", 3, 1, 1},
		{"exact pool", 3, 3, 3},
		{"pool smaller than request", 3, math.MaxInt, 3},
		{"below result cap", 1200, 999, 999},
		{"at result cap", 1200, 1000, 1000},
		{"above result cap", 1200, 1001, 1000},
		{"maximum integer", 1200, math.MaxInt, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := make([]*memory.MemoryRecord, tt.candidates)
			scores := make([]embedding.RerankResult, tt.candidates)
			for i := range candidates {
				candidates[i] = &memory.MemoryRecord{Content: "candidate"}
				scores[i] = embedding.RerankResult{Index: i, Score: float64(i)}
			}
			modes := []struct {
				name     string
				reranker embedding.Reranker
				reranked bool
			}{
				{"valid scores", scoredReranker{scores}, true},
				{"upstream failure", &errReranker{msg: "unavailable"}, false},
				{"malformed scores", scoredReranker{nil}, false},
			}
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					got, err := (&SQLiteStore{}).applyReranker(context.Background(), "query", candidates, tt.topK, mode.reranker)
					require.NoError(t, err)
					require.Len(t, got, tt.want)
					require.NotNil(t, got, "empty results must remain a non-nil slice")
					for i, record := range got {
						wantIndex := i
						if mode.reranked {
							wantIndex = len(candidates) - 1 - i
						}
						require.Same(t, candidates[wantIndex], record, "result %d must preserve the expected order", i)
					}
				})
			}
		})
	}
}

func TestApplyRerankerMalformedScoresPreserveRRFTopK(t *testing.T) {
	candidates := []*memory.MemoryRecord{
		{MemoryID: "rrf-first", Content: "first", ConfidenceScore: 0.85},
		{MemoryID: "rrf-second", Content: "second", ConfidenceScore: 0.80},
		{MemoryID: "rrf-third", Content: "third", ConfidenceScore: 0.75},
		{MemoryID: "rrf-fourth", Content: "fourth", ConfidenceScore: 0.70},
	}
	valid := []embedding.RerankResult{
		{Index: 3, Score: 4}, {Index: 2, Score: 3},
		{Index: 1, Score: 2}, {Index: 0, Score: 1},
	}
	tests := []struct {
		name   string
		scores []embedding.RerankResult
	}{
		{"empty", nil},
		{"partial", valid[:2]},
		{"too many", append(append([]embedding.RerankResult(nil), valid...), valid[0])},
		{"duplicate", []embedding.RerankResult{{Index: 3, Score: 4}, {Index: 2, Score: 3}, {Index: 1, Score: 2}, {Index: 3, Score: 1}}},
		{"negative index", []embedding.RerankResult{{Index: -1, Score: 4}, {Index: 2, Score: 3}, {Index: 1, Score: 2}, {Index: 0, Score: 1}}},
		{"index beyond pool", []embedding.RerankResult{{Index: 4, Score: 4}, {Index: 2, Score: 3}, {Index: 1, Score: 2}, {Index: 0, Score: 1}}},
		{"nan", []embedding.RerankResult{{Index: 3, Score: math.NaN()}, {Index: 2, Score: 3}, {Index: 1, Score: 2}, {Index: 0, Score: 1}}},
		{"positive infinity", []embedding.RerankResult{{Index: 3, Score: math.Inf(1)}, {Index: 2, Score: 3}, {Index: 1, Score: 2}, {Index: 0, Score: 1}}},
		{"negative infinity", []embedding.RerankResult{{Index: 3, Score: math.Inf(-1)}, {Index: 2, Score: 3}, {Index: 1, Score: 2}, {Index: 0, Score: 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (&SQLiteStore{}).applyReranker(context.Background(), "query", candidates, 3, scoredReranker{tt.scores})
			require.NoError(t, err)
			require.Equal(t, candidates[:3], got, "malformed scores must not remove or reorder healthy RRF results")
		})
	}
	got, err := (&SQLiteStore{}).applyReranker(context.Background(), "query", candidates, 3, scoredReranker{valid})
	require.NoError(t, err)
	require.Equal(t, []*memory.MemoryRecord{candidates[3], candidates[2], candidates[1]}, got,
		"a complete finite score set must still rerank, including scores outside [0,1]")
}

func TestSearchHybrid_RerankerReordersTopK(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s)

	// Pick a fake reranker that boosts the n+1-query record. That record has
	// neither strong BM25 nor strong vector affinity for "jwt auth", so it
	// would normally not lead the RRF output. Confirming it leads after
	// reranking proves the rerank pass is actually applied.
	fake := &fakeReranker{winnerSubstr: "n+1 query"}
	s.SetReranker(fake, 2)

	results, err := s.SearchHybrid(context.Background(),
		"jwt auth", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 3})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.Equal(t, "db-query-perf", results[0].MemoryID,
		"reranker winner must appear at rank 1 in the returned slice")
	assert.Equal(t, 1, fake.calls)
	assert.Equal(t, "jwt auth", fake.lastQuery,
		"reranker should receive the original query verbatim")
}

func TestSearchHybrid_NoRerankerLeavesRRFOrder(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s)

	// Sanity: with no reranker the result for "jwt auth" still contains the
	// auth-themed memories near the top (they did on the existing baseline
	// test), so the rerank pass is purely additive.
	results, err := s.SearchHybrid(context.Background(),
		"jwt auth", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 3})
	require.NoError(t, err)
	require.NotEmpty(t, results)

	ids := make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.MemoryID)
	}
	// The fake reranker's winner (db-query-perf) should NOT be #1 here.
	assert.NotEqual(t, "db-query-perf", results[0].MemoryID,
		"without a reranker the n+1-query record should not lead the auth query")
	assert.Contains(t, ids, "auth-jwt-keyword")
}

func TestSearchHybrid_RerankerFailureFallsBackToRRF(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s)
	s.SetReranker(&errReranker{msg: "upstream model not loaded"}, 2)

	// Reranker errors must not break recall: we should still get RRF-ordered
	// results, just without the cross-encoder refinement.
	results, err := s.SearchHybrid(context.Background(),
		"jwt auth", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 3})
	require.NoError(t, err)
	require.NotEmpty(t, results, "reranker failure should not zero out the recall")
	assert.LessOrEqual(t, len(results), 3, "TopK still respected on fallback")
}

func TestSearchHybrid_RerankerNotInvokedForEmptyQuery(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s)
	fake := &fakeReranker{winnerSubstr: "n+1 query"}
	s.SetReranker(fake, 2)

	// Vector-only path (empty query) skips reranker entirely: cross-encoder
	// needs the query text and there's nothing to rescore against.
	_, err := s.SearchHybrid(context.Background(),
		"", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 3})
	require.NoError(t, err)
	assert.Equal(t, 0, fake.calls,
		"reranker should not run when SearchHybrid degrades to vector-only")
}

func TestSearchHybrid_RerankerCandidatePoolIsOversampled(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s)
	fake := &fakeReranker{winnerSubstr: "never-matches"}
	s.SetReranker(fake, 3) // ask for 3x oversample

	_, err := s.SearchHybrid(context.Background(),
		"jwt auth", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 2})
	require.NoError(t, err)
	require.Equal(t, 1, fake.calls, "reranker called once")
	// TopK=2 with oversample=3 means up to 6 candidates fed to the reranker.
	// The seed corpus only has 5 records, so we expect all 5 to be sent.
	assert.GreaterOrEqual(t, len(fake.lastTexts), 2)
	assert.LessOrEqual(t, len(fake.lastTexts), 6)
}

func TestSetReranker_NilDisables(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s)
	fake := &fakeReranker{winnerSubstr: "n+1 query"}
	s.SetReranker(fake, 2)

	// Disable by passing nil.
	s.SetReranker(nil, 0)

	results, err := s.SearchHybrid(context.Background(),
		"jwt auth", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 3})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	assert.Equal(t, 0, fake.calls, "reranker must not be called after SetReranker(nil)")
	assert.NotEqual(t, "db-query-perf", results[0].MemoryID,
		"with reranker disabled, RRF ordering rules")
}

// Regression lock (reported on 10.5.2): the reranker/RRF ordering score must NEVER
// leak into a result's ConfidenceScore. That field is the STORED institutional
// confidence; exposing the retrieval score there broke min_confidence and
// confidence-aware clients. Every seeded record stores 0.85; the fake reranker
// assigns 0.99 to the winner and 0.01 to the rest. The returned records must all
// still carry 0.85, proving the ordering score stays separate from confidence.
func TestSearchHybrid_RerankPreservesStoredConfidence(t *testing.T) {
	s := newTestStore(t)
	seedHybridCorpus(t, s) // testMemory stores ConfidenceScore 0.85 for every record
	fake := &fakeReranker{winnerSubstr: "n+1 query"}
	s.SetReranker(fake, 2)

	results, err := s.SearchHybrid(context.Background(),
		"jwt auth", []float32{1.0, 0.0, 0.0}, QueryOptions{TopK: 3})
	require.NoError(t, err)
	require.NotEmpty(t, results)
	require.Equal(t, "db-query-perf", results[0].MemoryID, "rerank winner must lead")
	for _, r := range results {
		assert.InDelta(t, 0.85, r.ConfidenceScore, 1e-9,
			"result %s must carry the STORED confidence (0.85), not the rerank score (0.99/0.01)", r.MemoryID)
	}
}
