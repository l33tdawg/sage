package store

import (
	"context"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

func TestPostgresListMemoriesAppliesCreatedRangeToCountAndPage(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	const from = "2026-07-29T12:00:00Z"
	const to = "2026-07-29T13:00:00Z"
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT COUNT(*) FROM memories WHERE 1=1 AND created_at >= $1 AND created_at <= $2`,
	)).
		WithArgs(from, to).
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at FROM memories WHERE 1=1 AND created_at >= $1 AND created_at <= $2 ORDER BY created_at ASC, memory_id ASC LIMIT $3 OFFSET $4`,
	)).
		WithArgs(from, to, 200, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"memory_id", "submitting_agent", "content", "content_hash",
			"memory_type", "domain_tag", "provider", "confidence_score",
			"status", "parent_hash", "created_at", "committed_at", "deprecated_at",
		}))

	records, total, err := s.ListMemories(context.Background(), ListOptions{
		CreatedFrom:  from,
		CreatedTo:    to,
		Limit:        200,
		Sort:         "oldest",
		StablePaging: true,
	})
	require.NoError(t, err)
	require.Empty(t, records)
	require.Zero(t, total)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresListMemoriesCanSkipTotal(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at FROM memories WHERE 1=1 ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
	)).
		WithArgs(50, 0).
		WillReturnRows(pgxmock.NewRows([]string{
			"memory_id", "submitting_agent", "content", "content_hash",
			"memory_type", "domain_tag", "provider", "confidence_score",
			"status", "parent_hash", "created_at", "committed_at", "deprecated_at",
		}))

	records, total, err := s.ListMemories(context.Background(), ListOptions{
		Limit: 50, SkipTotal: true,
	})
	require.NoError(t, err)
	require.Empty(t, records)
	require.Zero(t, total)
	require.NoError(t, mock.ExpectationsWereMet())
}
