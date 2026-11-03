package store

import (
	"context"
	"math"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

func TestSQLiteDomainSpaceTaintCompletenessSnapshotFence(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const active = "active-space"
	for _, id := range []string{"tainted-outside", "clean-active"} {
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agent", "content-"+id, "work")))
		_, err := s.conn.ExecContext(ctx, `UPDATE memories SET status='committed' WHERE memory_id=?`, id)
		require.NoError(t, err)
	}
	require.NoError(t, s.UpdateMemoryEmbedding(ctx, "clean-active", []float32{0.1, 0.2}, active))
	require.NoError(t, s.ReplaceMemoryTaintMirror(ctx, 7, []string{"tainted-outside"}))
	out, established, matched, err := s.DomainSpaceCompletenessAtTaintRevision(ctx, "work", active, 1, 7)
	require.NoError(t, err)
	require.True(t, matched)
	require.True(t, established, "NOT EXISTS must exclude withheld rows before LIMIT")
	require.False(t, out)

	// An empty candidate set must still carry a revision result. An absent
	// match is not an empty/complete domain, even if a later publisher restores r.
	out, established, matched, err = s.DomainSpaceCompletenessAtTaintRevision(ctx, "empty", active, 1, 8)
	require.NoError(t, err)
	require.False(t, matched)
	require.False(t, established)
	require.False(t, out)
	require.NoError(t, s.ReplaceMemoryTaintMirror(ctx, 8, nil))
	out, established, matched, err = s.DomainSpaceCompletenessAtTaintRevision(ctx, "empty", active, 1, 8)
	require.NoError(t, err)
	require.True(t, matched)
	require.True(t, established)
	require.False(t, out)
	out, established, matched, err = s.DomainSpaceCompletenessAtTaintRevision(ctx, "work", active, 2, 8)
	require.NoError(t, err)
	require.True(t, matched)
	require.True(t, established)
	require.True(t, out, "the now-unwithheld unindexed row is an existence proof")
	_, err = s.conn.ExecContext(ctx, `DELETE FROM memory_taint_mirror_revision`)
	require.NoError(t, err)
	_, established, matched, err = s.DomainSpaceCompletenessAtTaintRevision(ctx, "empty", active, 1, 0)
	require.NoError(t, err)
	require.False(t, matched, "missing revision is not zero revision")
	require.False(t, established)
}

func TestSQLiteDomainSpaceTaintCompletenessBound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"one", "two"} {
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agent", id, "work")))
		_, err := s.conn.ExecContext(ctx, `UPDATE memories SET status='committed' WHERE memory_id=?`, id)
		require.NoError(t, err)
		require.NoError(t, s.UpdateMemoryEmbedding(ctx, id, []float32{0.1, 0.2}, "active"))
	}
	out, established, matched, err := s.DomainSpaceCompletenessAtTaintRevision(ctx, "work", "active", 1, 0)
	require.NoError(t, err)
	require.True(t, matched)
	require.False(t, established)
	require.False(t, out)
}

func TestPostgresDomainSpaceTaintCompletenessExactStatement(t *testing.T) {
	for _, tc := range []struct {
		name                                      string
		matched, present, embNull                 int
		provider                                  string
		wantMatched, wantEstablished, wantOutside bool
	}{
		{"empty matched", 1, 0, 0, "", true, true, false},
		{"empty mismatch", 0, 0, 0, "", false, false, false},
		{"outside mismatch", 0, 1, 1, "", false, false, false},
		{"outside matched", 1, 1, 1, "", true, true, true},
		{"in-space matched", 1, 1, 0, "active", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			t.Cleanup(mock.Close)
			s := &PostgresStore{db: mock}
			mock.ExpectQuery(regexp.QuoteMeta(postgresDomainSpaceAtTaintRevisionQuery)).
				WithArgs(int64(7), "work", 2).
				WillReturnRows(pgxmock.NewRows([]string{"matched", "present", "emb_null", "provider"}).
					AddRow(tc.matched, tc.present, tc.embNull, tc.provider))
			out, established, matched, err := s.DomainSpaceCompletenessAtTaintRevision(context.Background(), "work", "active", 1, 7)
			require.NoError(t, err)
			require.Equal(t, tc.wantMatched, matched)
			require.Equal(t, tc.wantEstablished, established)
			require.Equal(t, tc.wantOutside, out)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestTaintCompletenessRejectsUnboundedProbe(t *testing.T) {
	for _, cap := range []int{-1, 0, CandidateFilterScanBudget + 1} {
		require.Error(t, validateTaintCompletenessProbe(cap, 0))
	}
	require.Error(t, validateTaintCompletenessProbe(1, uint64(math.MaxInt64)+1))
}
