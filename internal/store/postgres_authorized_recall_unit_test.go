package store

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/pgvector/pgvector-go"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

func postgresRecallRows(ids ...string) *pgxmock.Rows {
	columns := []string{
		"memory_id", "submitting_agent", "content", "content_hash", "embedding",
		"memory_type", "domain_tag", "provider", "confidence_score", "status",
		"parent_hash", "created_at", "committed_at", "deprecated_at",
		"task_status", "distance",
	}
	rows := pgxmock.NewRows(columns)
	createdAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	for _, id := range ids {
		vector := pgvector.NewVector([]float32{1, 0})
		rows.AddRow(
			id, "agent", "content", []byte(id), &vector,
			string(memory.TypeObservation), "domain", "", 0.9,
			string(memory.StatusCommitted), nil, createdAt, nil, nil, "", 0.1,
		)
	}
	return rows
}

func TestPostgresRecallCandidateFilterPagesPastDeniedPrefix(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	deniedIDs := make([]string, 128)
	for i := range deniedIDs {
		deniedIDs[i] = fmt.Sprintf("denied-%03d", i)
	}
	pagePattern := regexp.QuoteMeta(
		"ORDER BY embedding <=> $1, memory_id ASC LIMIT $2 OFFSET $3",
	)
	mock.ExpectQuery(pagePattern).
		WithArgs(pgxmock.AnyArg(), 128, 0).
		WillReturnRows(postgresRecallRows(deniedIDs...)).
		RowsWillBeClosed()
	mock.ExpectQuery(pagePattern).
		WithArgs(pgxmock.AnyArg(), 128, 128).
		WillReturnRows(postgresRecallRows("allowed-1", "allowed-2")).
		RowsWillBeClosed()

	results, err := s.QuerySimilar(
		context.Background(),
		[]float32{1, 0},
		QueryOptions{
			TopK: 2,
			CandidateFilter: func(rec *memory.MemoryRecord) (bool, error) {
				return strings.HasPrefix(rec.MemoryID, "allowed-"), nil
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, "allowed-1", results[0].MemoryID)
	require.Equal(t, "allowed-2", results[1].MemoryID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresHybridCandidateFilterErrorCannotBecomePartialSuccess(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	pagePattern := regexp.QuoteMeta(
		"ORDER BY embedding <=> $1, memory_id ASC LIMIT $2 OFFSET $3",
	)
	mock.ExpectQuery(pagePattern).
		WithArgs(pgxmock.AnyArg(), 128, 0).
		WillReturnRows(postgresRecallRows("candidate")).
		RowsWillBeClosed()

	results, err := s.SearchHybrid(
		context.Background(),
		"query ignored by the vector-only Postgres hybrid path",
		[]float32{1, 0},
		QueryOptions{
			TopK: 2,
			CandidateFilter: func(*memory.MemoryRecord) (bool, error) {
				return false, ErrCandidateFilterScanBudgetExceeded
			},
		},
	)
	require.Nil(t, results)
	require.ErrorIs(t, err, ErrCandidateFilterScanBudgetExceeded)
	require.NoError(t, mock.ExpectationsWereMet())
}
