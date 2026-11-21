package store

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"testing"

	"github.com/pashagolub/pgxmock/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

func seedNotTagsStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s := newTestStore(t)
	ctx := context.Background()
	for i, tags := range [][]string{{"insight", "gen:1"}, {"insight", "gen:2"}, {"other"}, nil} {
		id := fmt.Sprintf("m%d", i)
		rec := testMemory(id, "agent1", fmt.Sprintf("searchable lesson %d", i), "general")
		rec.Embedding = []float32{0.5, 0.5, 0.5}
		require.NoError(t, s.InsertMemory(ctx, rec))
		if tags != nil {
			require.NoError(t, s.SetTags(ctx, id, tags))
		}
	}
	return s
}

func notTagsIDs(recs []*memory.MemoryRecord) []string {
	ids := make([]string, 0, len(recs))
	for _, r := range recs {
		ids = append(ids, r.MemoryID)
	}
	sort.Strings(ids)
	return ids
}

func TestQuerySimilar_NotTagsFilter(t *testing.T) {
	s := seedNotTagsStore(t)
	ctx := context.Background()
	query := []float32{0.5, 0.5, 0.5}

	// Exclusion alone keeps untagged and otherwise-tagged memories.
	got, err := s.QuerySimilar(ctx, query, QueryOptions{TopK: 10, NotTags: []string{"gen:1"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"m1", "m2", "m3"}, notTagsIDs(got))

	// Any listed tag excludes (OR).
	got, err = s.QuerySimilar(ctx, query, QueryOptions{TopK: 10, NotTags: []string{"gen:1", "other"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"m1", "m3"}, notTagsIDs(got))

	// Combined with Tags: "insight and not gen:1".
	got, err = s.QuerySimilar(ctx, query, QueryOptions{TopK: 10, Tags: []string{"insight"}, NotTags: []string{"gen:1"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"m1"}, notTagsIDs(got))

	// A tag nothing carries excludes nothing.
	got, err = s.QuerySimilar(ctx, query, QueryOptions{TopK: 10, NotTags: []string{"nonexistent"}})
	require.NoError(t, err)
	assert.Len(t, got, 4)
}

func TestSearchByText_NotTagsFilter(t *testing.T) {
	s := seedNotTagsStore(t)
	ctx := context.Background()

	got, err := s.SearchByText(ctx, "searchable", QueryOptions{TopK: 10, NotTags: []string{"insight"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"m2", "m3"}, notTagsIDs(got))

	got, err = s.SearchByText(ctx, "searchable", QueryOptions{TopK: 10, Tags: []string{"insight"}, NotTags: []string{"gen:2"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"m0"}, notTagsIDs(got))
}

func TestSearchHybrid_NotTagsFilter(t *testing.T) {
	s := seedNotTagsStore(t)
	got, err := s.SearchHybrid(context.Background(), "searchable", []float32{0.5, 0.5, 0.5},
		QueryOptions{TopK: 10, NotTags: []string{"insight"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"m2", "m3"}, notTagsIDs(got))
}

func TestPostgresQuerySimilar_NotTagsUsesNotExists(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	mock.ExpectQuery(
		regexp.QuoteMeta("AND EXISTS (SELECT 1 FROM memory_tags mt WHERE mt.memory_id = memories.memory_id AND mt.tag IN ($2))") +
			".*" +
			regexp.QuoteMeta("AND NOT EXISTS (SELECT 1 FROM memory_tags mt WHERE mt.memory_id = memories.memory_id AND mt.tag IN ($3,$4))"),
	).
		WithArgs(pgxmock.AnyArg(), "insight", "gen:1", "gen:2", pgxmock.AnyArg()).
		WillReturnRows(postgresRecallRows("kept")).
		RowsWillBeClosed()

	got, err := s.QuerySimilar(context.Background(), []float32{1, 0}, QueryOptions{
		TopK: 5, Tags: []string{"insight"}, NotTags: []string{"gen:1", "gen:2"},
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}
