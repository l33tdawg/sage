package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

const lineageParentID = "794f4a15-416e-4e32-920c-5406c86662aa"

func TestSQLiteFindMemoryParentHashAndLegacyID(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	parent := testMemory(lineageParentID, "agent", "original content", "original")
	require.NoError(t, s.InsertMemory(ctx, parent))
	pointer := hex.EncodeToString(parent.ContentHash)
	for _, lookup := range []string{pointer, parent.MemoryID} {
		found, err := s.FindMemoryParent(ctx, lookup)
		require.NoError(t, err)
		require.Equal(t, parent.MemoryID, found.MemoryID)
		require.Equal(t, parent.ContentHash, found.ContentHash)
		require.Equal(t, parent.SubmittingAgent, found.SubmittingAgent)
		require.Equal(t, parent.DomainTag, found.DomainTag)
	}
	// The same indexed lookup also works on a transaction-scoped connection.
	require.NoError(t, s.RunInTx(ctx, func(tx OffchainStore) error {
		found, err := tx.(MemoryLineageStore).FindMemoryParent(ctx, pointer)
		require.NoError(t, err)
		require.Equal(t, parent.MemoryID, found.MemoryID)
		return nil
	}))
	legacy := testMemory("legacy-parent", "agent", "old ID pointer", "legacy")
	require.NoError(t, s.InsertMemory(ctx, legacy))
	found, err := s.FindMemoryParent(ctx, legacy.MemoryID)
	require.NoError(t, err)
	require.Equal(t, legacy.MemoryID, found.MemoryID)
}

func TestSQLiteFindMemoryParentAmbiguousAndMissing(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	for _, id := range []string{lineageParentID, "0860f8fa-6031-48a7-8590-b8a70f4151dc"} {
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agent", "duplicated content", "domain")))
	}
	hash := sha256.Sum256([]byte("duplicated content"))
	missingHash := sha256.Sum256([]byte("absent content"))
	for _, pointer := range []string{"", "malformed", "aabb", hex.EncodeToString(hash[:]), hex.EncodeToString(missingHash[:])} {
		found, err := s.FindMemoryParent(ctx, pointer)
		require.NoError(t, err)
		require.Nil(t, found, pointer)
	}
	// Exact legacy IDs remain authoritative even when shaped like a hash.
	legacyID := hex.EncodeToString(hash[:])
	require.NoError(t, s.InsertMemory(ctx, testMemory(legacyID, "agent", "legacy exact-ID content", "domain")))
	found, err := s.FindMemoryParent(ctx, legacyID)
	require.NoError(t, err)
	require.Equal(t, legacyID, found.MemoryID)
}

func postgresLineageRows(id string, hash []byte) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"memory_id", "submitting_agent", "domain_tag", "content_hash",
	}).AddRow(id, "agent", "original", hash)
}

func TestPostgresFindMemoryParentHashAndLegacyID(t *testing.T) {
	for _, legacyID := range []bool{false, true} {
		t.Run(map[bool]string{false: "content-hash", true: "legacy-uuid"}[legacyID], func(t *testing.T) {
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			t.Cleanup(mock.Close)
			s := &PostgresStore{db: mock}
			hash := sha256.Sum256([]byte("original content"))
			pointer := hex.EncodeToString(hash[:])
			if legacyID {
				pointer = lineageParentID
				mock.ExpectQuery(regexp.QuoteMeta(postgresMemoryParentIDSQL)).WithArgs(lineageParentID).
					WillReturnRows(postgresLineageRows(lineageParentID, hash[:]))
			} else {
				// No UUID equality query is allowed for a content-hash pointer.
				mock.ExpectQuery(regexp.QuoteMeta(postgresMemoryParentHashSQL)).WithArgs(hash[:]).
					WillReturnRows(postgresLineageRows(lineageParentID, hash[:])).RowsWillBeClosed()
			}
			found, err := s.FindMemoryParent(context.Background(), pointer)
			require.NoError(t, err)
			require.Equal(t, lineageParentID, found.MemoryID)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestPostgresFindMemoryParentAmbiguousAndMalformed(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	hash := sha256.Sum256([]byte("duplicated content"))
	mock.ExpectQuery(regexp.QuoteMeta(postgresMemoryParentHashSQL)).WithArgs(hash[:]).
		WillReturnRows(postgresLineageRows(lineageParentID, hash[:]).
			AddRow("0860f8fa-6031-48a7-8590-b8a70f4151dc", "hidden-agent", "private", hash[:])).RowsWillBeClosed()
	found, err := s.FindMemoryParent(context.Background(), hex.EncodeToString(hash[:]))
	require.NoError(t, err)
	require.Nil(t, found)
	for _, pointer := range []string{"", "malformed", "aabb"} {
		found, err = s.FindMemoryParent(context.Background(), pointer)
		require.NoError(t, err)
		require.Nil(t, found)
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresFindMemoryParentNormalizesAcceptedLegacyUUID(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	hash := sha256.Sum256([]byte("original content"))
	mock.ExpectQuery(regexp.QuoteMeta(postgresMemoryParentIDSQL)).WithArgs(lineageParentID).
		WillReturnRows(postgresLineageRows(lineageParentID, hash[:]))
	found, err := s.FindMemoryParent(context.Background(), "urn:uuid:"+lineageParentID)
	require.NoError(t, err)
	require.Equal(t, lineageParentID, found.MemoryID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresFindMemoryParentPropagatesQueryAndRowFailures(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	hash := sha256.Sum256([]byte("original content"))
	pointer := hex.EncodeToString(hash[:])
	queryErr := errors.New("query unavailable")
	mock.ExpectQuery(regexp.QuoteMeta(postgresMemoryParentHashSQL)).WithArgs(hash[:]).WillReturnError(queryErr)
	found, err := s.FindMemoryParent(context.Background(), pointer)
	require.ErrorIs(t, err, queryErr)
	require.Nil(t, found)
	mock.ExpectQuery(regexp.QuoteMeta(postgresMemoryParentHashSQL)).WithArgs(hash[:]).
		WillReturnRows(postgresLineageRows(lineageParentID, hash[:]).RowError(0, queryErr)).RowsWillBeClosed()
	found, err = s.FindMemoryParent(context.Background(), pointer)
	require.ErrorIs(t, err, queryErr)
	require.Nil(t, found)
	require.NoError(t, mock.ExpectationsWereMet())
}
