package store

import (
	"context"
	"encoding/hex"
	"regexp"
	"strings"
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

// The SQLite and Postgres upserts must agree on which columns a conflicting
// insert refreshes. They diverged silently until v11.11: SQLite refreshed
// content/content_hash/domain_tag/memory_type/confidence_score (so a co-commit
// squat-reclaim replaces the squatter's off-chain row), Postgres refreshed none
// of them. That was inert only while PostgresStore.FindByContentHash returned a
// constant false. Once it became a live reader of content_hash, the stale column
// made SQLite and Postgres validators disagree about duplicates and made
// MergeProjectionSupplementary's "memory_id = $1 AND content_hash = $8"
// predicate match zero rows, which panics the node with "consensus cannot
// advance" and re-panics on every restart.
func TestPostgresUpsertRefreshesContentHashLikeSQLite(t *testing.T) {
	// These five are what a squat-reclaim depends on being overwritten.
	for _, column := range []string{
		"content = EXCLUDED.content",
		"content_hash = EXCLUDED.content_hash",
		"domain_tag = EXCLUDED.domain_tag",
		"memory_type = EXCLUDED.memory_type",
		"confidence_score = EXCLUDED.confidence_score",
	} {
		require.Containsf(t, postgresInsertMemorySQL, column,
			"postgres upsert must refresh %q on conflict, or a co-commit squat-reclaim "+
				"leaves a stale content_hash that diverges from SQLite and can halt the node", column)
	}

	// task_status must still be excluded: replay must not overwrite board state.
	require.NotContains(t, postgresInsertMemorySQL, "task_status =")
}

// The dedup query is only cheap if an index covers it. voter.Run evaluates
// FindByContentHash per pending memory on a 2s poll, against a table carrying
// content TEXT plus a 768-dimension vector.
func TestPostgresContentHashDedupIsIndexed(t *testing.T) {
	var indexed bool
	for _, stmt := range postgresTaskAssignmentSchema {
		if strings.Contains(stmt, "idx_memories_content_hash") {
			indexed = true
			require.Containsf(t, stmt, "status = 'committed'",
				"the index must be partial on the same predicate FindByContentHash uses, got %q", stmt)
		}
	}
	require.True(t, indexed,
		"existing Postgres databases need a content_hash index migration, not just deploy/init.sql")
}
