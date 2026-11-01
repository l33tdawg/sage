package store

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

func TestPostgresDomainSpaceCompletenessTaintFilterRunsBeforeLimit(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	mock.ExpectQuery(`(?s)FROM memories.*NOT EXISTS \(\s*SELECT 1 FROM memory_taint_mirror tm.*LIMIT \$2`).
		WithArgs("work", 2).
		WillReturnRows(pgxmock.NewRows([]string{"embedding_is_null", "embedding_provider"}).
			AddRow(0, "active-space"))

	hasOutside, established, err := s.DomainSpaceCompletenessFiltered(
		context.Background(),
		"work",
		"active-space",
		1,
		true,
	)
	require.NoError(t, err)
	require.True(t, established)
	require.False(t, hasOutside)
	require.NoError(t, mock.ExpectationsWereMet())
}
