package store

import (
	"context"
	"encoding/hex"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresFindByContentHashCommittedOnly(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	store := &PostgresStore{db: mock}

	const contentHash = "e4acb5580a1cbab224b9c2f0f00ef759e76a8a4d08ce9d7e81e2cd00fd0d0e11"
	hashBytes, err := hex.DecodeString(contentHash)
	require.NoError(t, err)
	query := regexp.QuoteMeta(
		`SELECT EXISTS(SELECT 1 FROM memories WHERE content_hash = $1 AND status = 'committed')`,
	)

	for _, test := range []struct {
		name   string
		exists bool
	}{
		{name: "proposed candidate does not match itself", exists: false},
		{name: "committed memory matches", exists: true},
		{name: "deprecated memory does not match", exists: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock.ExpectQuery(query).
				WithArgs(hashBytes).
				WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(test.exists))

			exists, findErr := store.FindByContentHash(context.Background(), contentHash)
			require.NoError(t, findErr)
			assert.Equal(t, test.exists, exists)
		})
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresFindByContentHashRejectsMalformedHash(t *testing.T) {
	store := &PostgresStore{}
	_, err := store.FindByContentHash(context.Background(), "not-hex")
	require.ErrorContains(t, err, "decode content hash")
}
