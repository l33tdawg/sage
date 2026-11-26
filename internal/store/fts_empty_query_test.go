package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFtsEscapeQuery_NoSearchableTerm(t *testing.T) {
	for _, q := range []string{"", " ", "\t\n", `"`, `""`, `" "`, `"" ""`} {
		assert.Equal(t, "", ftsEscapeQuery(q), "query %q", q)
	}
	assert.Equal(t, `"a" "b"`, ftsEscapeQuery(`a " b`))
	assert.Equal(t, `"say" "hi"`, ftsEscapeQuery(`"say" hi"`))
}

// Blank or quotes-only text used to reach FTS5 with an empty MATCH string
// and fail with "fts5: syntax error". It now matches nothing.
func TestSearchByText_NoSearchableTermMatchesNothing(t *testing.T) {
	s := seedNotTagsStore(t)
	ctx := context.Background()
	for _, q := range []string{" ", `"`, `"" ""`} {
		got, err := s.SearchByText(ctx, q, QueryOptions{TopK: 10})
		require.NoError(t, err, "query %q", q)
		assert.Empty(t, got, "query %q", q)
	}
}

// Under a decayed-confidence floor a BM25 leaf error is fail-closed, so a
// quotes-only query used to fail the whole hybrid recall. It now ranks on the
// vector leaf alone.
func TestSearchHybrid_QuotesOnlyQueryFallsBackToVector(t *testing.T) {
	s := seedNotTagsStore(t)
	ctx := context.Background()
	got, err := s.SearchHybrid(ctx, `"`, []float32{0.5, 0.5, 0.5}, QueryOptions{
		TopK:       10,
		DecayFloor: 0.01,
		DecayNow:   time.Now(),
	})
	require.NoError(t, err)
	assert.Len(t, got, 4)
}
