package store

import (
	"context"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/embedding"
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
