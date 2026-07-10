package store

import (
	"context"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// challengedCols reads the app-v17 dispute columns for a memory row.
func challengedCols(t *testing.T, s *SQLiteStore, id string) (status string, height, quorum int64) {
	t.Helper()
	err := s.conn.QueryRowContext(context.Background(),
		`SELECT status, disputed_height, disputed_quorum FROM memories WHERE memory_id = ?`, id).
		Scan(&status, &height, &quorum)
	require.NoError(t, err)
	return status, height, quorum
}

// TestMigrateDisputedOnExistingDB proves the migration is schema-safe for a DB
// that predates the columns: after dropping them (simulating an older mirror) and
// re-running the migration, the columns are re-added and a pre-existing row reads
// the default 0 (never re-measured/backfilled). Idempotent on a second call.
func TestMigrateDisputedOnExistingDB(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Seed a row while the columns exist, then drop the columns to emulate a
	// pre-v11.5 database that already carried memories.
	require.NoError(t, s.InsertMemory(ctx, testMemory("legacy1", "agentA", "old belief content here", "general")))
	_, err := s.writeExecContext(ctx, `ALTER TABLE memories DROP COLUMN disputed_height`)
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `ALTER TABLE memories DROP COLUMN disputed_quorum`)
	require.NoError(t, err)

	// Column truly gone.
	var cnt int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='disputed_height'`).Scan(&cnt))
	require.Equal(t, 0, cnt)

	// Run the migration (as NewSQLiteStore would on boot of the upgraded binary).
	s.migrateDisputed(ctx)

	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='disputed_height'`).Scan(&cnt))
	require.Equal(t, 1, cnt)
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('memories') WHERE name='disputed_quorum'`).Scan(&cnt))
	require.Equal(t, 1, cnt)

	// Pre-existing row got the default 0 (distinguishable as "legacy" by the sweep).
	_, h, q := challengedCols(t, s, "legacy1")
	assert.Equal(t, int64(0), h)
	assert.Equal(t, int64(0), q)

	// Idempotent — a second call is a no-op, not an error.
	s.migrateDisputed(ctx)
	_, h, _ = challengedCols(t, s, "legacy1")
	assert.Equal(t, int64(0), h)
}

// TestMarkDisputedPersistsMetadata verifies the park mirror write records status
// 'challenged' plus the challenge-execution height and measured quorum (C-D3),
// without stamping committed_at/deprecated_at.
func TestMarkDisputedPersistsMetadata(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rec := testMemory("m1", "agentA", "disputed content here", "general")
	rec.Status = memory.StatusCommitted
	require.NoError(t, s.InsertMemory(ctx, rec))

	require.NoError(t, s.MarkDisputed(ctx, "m1", 4242, 3, time.Now().UTC()))

	status, h, q := challengedCols(t, s, "m1")
	assert.Equal(t, "challenged", status)
	assert.Equal(t, int64(4242), h)
	assert.Equal(t, int64(3), q)

	// committed_at / deprecated_at untouched by a park.
	var committedAt, deprecatedAt *string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT committed_at, deprecated_at FROM memories WHERE memory_id = ?`, "m1").
		Scan(&committedAt, &deprecatedAt))
	assert.Nil(t, deprecatedAt)
}

// TestResolveChallengedMemoriesScoping is the core boot-sweep safety test: the
// sweep still deprecates LEGACY challenged rows (disputed_height=0) but MUST leave
// live post-v17 disputes (disputed_height>0) exactly as they are.
func TestResolveChallengedMemoriesScoping(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Legacy limbo row: challenged with disputed_height defaulting to 0.
	legacy := testMemory("legacy1", "agentA", "legacy challenged content", "general")
	legacy.Status = memory.StatusChallenged
	require.NoError(t, s.InsertMemory(ctx, legacy))

	// Live post-v17 dispute: parked via MarkDisputed (disputed_height>0).
	live := testMemory("live1", "agentB", "live disputed content here", "general")
	live.Status = memory.StatusCommitted
	require.NoError(t, s.InsertMemory(ctx, live))
	require.NoError(t, s.MarkDisputed(ctx, "live1", 900, 2, time.Now().UTC()))

	n, err := s.ResolveChallengedMemories(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "only the legacy challenged row should be swept")

	legacyStatus, _, _ := challengedCols(t, s, "legacy1")
	assert.Equal(t, "deprecated", legacyStatus, "legacy challenged row swept to deprecated")

	liveStatus, liveH, liveQ := challengedCols(t, s, "live1")
	assert.Equal(t, "challenged", liveStatus, "live v17 dispute must survive the sweep")
	assert.Equal(t, int64(900), liveH)
	assert.Equal(t, int64(2), liveQ)
}

// TestRecallAdmitsDisputedRows verifies the read paths keep disputed-but-live rows
// recallable under a "committed" filter ONLY when IncludeDisputed is set, and that
// committed rows are returned regardless (never dropped or altered).
func TestRecallAdmitsDisputedRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	committed := testMemory("c1", "agentA", "quantum tunneling committed fact", "physics")
	committed.Status = memory.StatusCommitted
	require.NoError(t, s.InsertMemory(ctx, committed))

	disputed := testMemory("d1", "agentA", "quantum tunneling disputed fact", "physics")
	disputed.Status = memory.StatusCommitted
	require.NoError(t, s.InsertMemory(ctx, disputed))
	require.NoError(t, s.MarkDisputed(ctx, "d1", 500, 2, time.Now().UTC()))

	// Baseline "committed" filter (no IncludeDisputed): only the committed row.
	base, err := s.SearchByText(ctx, "quantum tunneling", QueryOptions{StatusFilter: "committed", TopK: 10})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c1"}, recordIDs(base), "disputed row hidden without IncludeDisputed")

	// IncludeDisputed: both surface; disputed row carries status 'challenged'.
	withDisp, err := s.SearchByText(ctx, "quantum tunneling", QueryOptions{StatusFilter: "committed", IncludeDisputed: true, TopK: 10})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c1", "d1"}, recordIDs(withDisp))
	for _, r := range withDisp {
		if r.MemoryID == "d1" {
			assert.Equal(t, memory.StatusChallenged, r.Status)
		}
		if r.MemoryID == "c1" {
			assert.Equal(t, memory.StatusCommitted, r.Status)
		}
	}

	// QuerySimilar honors the same admission rule.
	emb := []float32{0.1, 0.2, 0.3}
	for _, id := range []string{"c1", "d1"} {
		_, uerr := s.writeExecContext(ctx, `UPDATE memories SET embedding = ? WHERE memory_id = ?`, encodeEmbedding(emb), id)
		require.NoError(t, uerr)
	}
	simBase, err := s.QuerySimilar(ctx, emb, QueryOptions{StatusFilter: "committed", TopK: 10})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c1"}, recordIDs(simBase))

	simDisp, err := s.QuerySimilar(ctx, emb, QueryOptions{StatusFilter: "committed", IncludeDisputed: true, TopK: 10})
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"c1", "d1"}, recordIDs(simDisp))
}
