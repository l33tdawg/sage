package store

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/require"
)

func TestSQLiteListMemoryProjectionPageUsesStableKeyset(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	createdAt := time.Date(2026, 7, 31, 1, 2, 3, 456000000, time.UTC)

	for _, id := range []string{"memory-c", "memory-a", "memory-b"} {
		record := testMemory(id, "agent-local", "content-"+id, "research")
		record.CreatedAt = createdAt
		require.NoError(t, s.InsertMemory(ctx, record))
	}

	first, cursor, err := s.ListMemoryProjectionPage(
		ctx,
		ListOptions{Sort: "oldest"},
		MemoryProjectionPageCursor{},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"memory-a", "memory-b"}, recordIDs(first))
	require.Equal(t, "memory-b", cursor.MemoryID)
	require.NotEmpty(t, cursor.CreatedAt)

	second, cursor, err := s.ListMemoryProjectionPage(
		ctx,
		ListOptions{Sort: "oldest"},
		cursor,
		2,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"memory-c"}, recordIDs(second))
	require.Equal(t, "memory-c", cursor.MemoryID)

	last, cursor, err := s.ListMemoryProjectionPage(
		ctx,
		ListOptions{Sort: "oldest"},
		cursor,
		2,
	)
	require.NoError(t, err)
	require.Empty(t, last)
	require.Equal(t, MemoryProjectionPageCursor{}, cursor)
}

func TestSQLiteListMemoryProjectionPagePreservesListFilters(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	createdAt := time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)

	active := testMemory("active", "agent-a", "active", "research")
	active.CreatedAt = createdAt
	require.NoError(t, s.InsertMemory(ctx, active))

	deprecated := testMemory("deprecated", "agent-a", "deprecated", "research")
	deprecated.CreatedAt = createdAt.Add(time.Second)
	deprecated.Status = memory.StatusDeprecated
	require.NoError(t, s.InsertMemory(ctx, deprecated))

	internal := testMemory("internal", "agent-a", "internal", "sage-system/audit")
	internal.CreatedAt = createdAt.Add(2 * time.Second)
	require.NoError(t, s.InsertMemory(ctx, internal))

	records, _, err := s.ListMemoryProjectionPage(
		ctx,
		ListOptions{
			Status:                "active",
			SubmittingAgent:       "agent-a",
			ExcludeDomainPrefixes: []string{"SAGE-SYSTEM/"},
			Sort:                  "oldest",
		},
		MemoryProjectionPageCursor{},
		10,
	)
	require.NoError(t, err)
	require.Equal(t, []string{"active"}, recordIDs(records))
}

func TestMemoryProjectionPageRejectsPartialCursor(t *testing.T) {
	s := newTestStore(t)
	_, _, err := s.ListMemoryProjectionPage(
		context.Background(),
		ListOptions{},
		MemoryProjectionPageCursor{CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)},
		10,
	)
	require.ErrorContains(t, err, "created_at and memory_id together")
}

func TestPostgresListMemoryProjectionPageUsesKeysetWithoutCount(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	createdAt := time.Date(2026, 7, 31, 3, 4, 5, 600000000, time.UTC)
	after := MemoryProjectionPageCursor{
		CreatedAt: createdAt.Add(-time.Second).Format(time.RFC3339Nano),
		MemoryID:  "00000000-0000-0000-0000-000000000001",
	}
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at, COALESCE(task_status, '')
		FROM memories WHERE 1=1 AND domain_tag = $1 AND status != 'deprecated' AND (created_at > $2::timestamptz OR (created_at = $2::timestamptz AND memory_id > $3::uuid)) ORDER BY created_at ASC, memory_id ASC LIMIT $4`
	rows := pgxmock.NewRows([]string{
		"memory_id", "submitting_agent", "content", "content_hash",
		"memory_type", "domain_tag", "provider", "confidence_score",
		"status", "parent_hash", "created_at", "committed_at", "deprecated_at",
		"task_status",
	}).AddRow(
		"00000000-0000-0000-0000-000000000002",
		"agent-a",
		"content",
		[]byte{1, 2, 3},
		string(memory.TypeObservation),
		"research",
		"provider-a",
		0.9,
		string(memory.StatusCommitted),
		nil,
		createdAt,
		nil,
		nil,
		"",
	)
	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs("research", after.CreatedAt, after.MemoryID, 2).
		WillReturnRows(rows)

	records, next, err := s.ListMemoryProjectionPage(
		context.Background(),
		ListOptions{
			DomainTag: "research",
			Status:    "active",
			Sort:      "oldest",
		},
		after,
		2,
	)
	require.NoError(t, err)
	require.Equal(t,
		[]string{"00000000-0000-0000-0000-000000000002"},
		recordIDs(records),
	)
	require.Equal(t, records[0].MemoryID, next.MemoryID)
	require.Equal(t, createdAt.Format(time.RFC3339Nano), next.CreatedAt)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresMemoryProjectionRevisionAndVaultGeneration(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT revision FROM memory_projection_revision WHERE singleton = TRUE`,
	)).WillReturnRows(pgxmock.NewRows([]string{"revision"}).AddRow(int64(42)))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT revision FROM graph_projection_revision WHERE singleton = TRUE`,
	)).WillReturnRows(pgxmock.NewRows([]string{"revision"}).AddRow(int64(17)))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT revision FROM memory_space_revision WHERE singleton = TRUE`,
	)).WillReturnRows(pgxmock.NewRows([]string{"revision"}).AddRow(int64(9)))

	revision, err := s.MemoryProjectionRevision(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 42, revision)
	graphRevision, err := s.GraphProjectionRevision(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 17, graphRevision)
	spaceRevision, err := s.MemorySpaceRevision(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 9, spaceRevision)
	require.Zero(t, s.VaultGeneration())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresMemoryProjectionRevisionRejectsNegativeValue(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT revision FROM memory_projection_revision WHERE singleton = TRUE`,
	)).WillReturnRows(pgxmock.NewRows([]string{"revision"}).AddRow(int64(-1)))

	_, err = s.MemoryProjectionRevision(context.Background())
	require.ErrorContains(t, err, "negative")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresGraphProjectionRevisionRejectsNegativeValue(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT revision FROM graph_projection_revision WHERE singleton = TRUE`,
	)).WillReturnRows(pgxmock.NewRows([]string{"revision"}).AddRow(int64(-1)))

	_, err = s.GraphProjectionRevision(context.Background())
	require.ErrorContains(t, err, "negative")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresProjectionSchemaReplacesBroadTriggersWithScopedStatementTriggers(
	t *testing.T,
) {
	schema := strings.Join(postgresProjectionSchema, "\n")
	memoryDrop := strings.Index(
		schema,
		"DROP TRIGGER IF EXISTS memories_projection_revision_update_v1 ON memories",
	)
	memoryCreate := strings.Index(
		schema,
		"CREATE TRIGGER memories_projection_revision_update_v1",
	)
	require.GreaterOrEqual(t, memoryDrop, 0)
	require.Greater(t, memoryCreate, memoryDrop)
	require.Contains(t, schema,
		"AFTER UPDATE OF memory_id, submitting_agent, content, content_hash,")
	require.NotContains(t, schema, "AFTER UPDATE ON memories")
	require.Contains(t, schema,
		"AFTER UPDATE OF embedding, embedding_provider ON memories")
	require.Contains(t, schema,
		"FOR EACH STATEMENT EXECUTE FUNCTION sage_bump_memory_space_revision()")

	graphDrop := strings.Index(
		schema,
		"DROP TRIGGER IF EXISTS memories_graph_revision_update_v1 ON memories",
	)
	graphCreate := strings.Index(
		schema,
		"CREATE TRIGGER memories_graph_revision_update_v1",
	)
	require.GreaterOrEqual(t, graphDrop, 0)
	require.Greater(t, graphCreate, graphDrop)
	require.Contains(t, schema,
		"AFTER UPDATE OF memory_type, confidence_score, parent_hash ON memories")

	for _, table := range []string{
		"memory_tags",
		"corroborations",
		"memory_links",
	} {
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			require.Contains(t, schema,
				"AFTER "+operation+" ON "+table+" FOR EACH STATEMENT")
		}
	}

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	mock.ExpectExec(regexp.QuoteMeta(
		`SELECT pg_advisory_xact_lock($1)`,
	)).WithArgs(projectionSchemaLockKey).
		WillReturnResult(pgxmock.NewResult("SELECT", 1))
	for _, statement := range postgresProjectionSchema {
		mock.ExpectExec(regexp.QuoteMeta(statement)).
			WillReturnResult(pgxmock.NewResult("DDL", 0))
	}
	s := &PostgresStore{db: mock}
	require.NoError(t, s.ensureProjectionSchema(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}
