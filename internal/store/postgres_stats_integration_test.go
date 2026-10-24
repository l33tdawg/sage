//go:build integration

package store

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Run the same lifecycle contract as SQLite against actual PostgreSQL queries.
// A transaction-local table isolates the fixture from the deployment schema and
// other integration tests; these aggregates do not need the pgvector extension.
func TestPostgresStatsLifecycleParity(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, agentTestDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	txn, err := conn.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = txn.Rollback(ctx) })
	_, err = txn.Exec(ctx, `CREATE TEMP TABLE memories (
		memory_id TEXT PRIMARY KEY,
		domain_tag TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL
	) ON COMMIT DROP`)
	require.NoError(t, err)
	for _, record := range statsLifecycleRecords() {
		_, err = txn.Exec(ctx, `INSERT INTO memories (memory_id, domain_tag, status, created_at)
			VALUES ($1, $2, $3, $4)`, record.MemoryID, record.DomainTag, string(record.Status), record.CreatedAt)
		require.NoError(t, err)
	}
	assertStatsLifecycleCounts(t, &PostgresStore{db: txn})
}
