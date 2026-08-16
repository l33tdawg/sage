package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestEngramIndexDeclaredOnBothBackends: the submitting_agent index must be
// created for NEW databases (initSchema) AND EXISTING ones (the idempotent
// migration mirror) on sqlite, and on postgres — a plan test alone can't catch a
// dropped declaration, and a migration-only or new-db-only copy silently leaves
// half the fleet on a full scan.
func TestEngramIndexDeclaredOnBothBackends(t *testing.T) {
	const idx = "idx_memories_submitting_agent ON memories(submitting_agent, confidence_score)"
	sqlSrc, err := os.ReadFile("sqlite.go")
	require.NoError(t, err)
	require.GreaterOrEqual(t, strings.Count(string(sqlSrc), idx), 2,
		"sqlite must declare the index in BOTH initSchema and the migration mirror")

	pgSrc, err := os.ReadFile("postgres.go")
	require.NoError(t, err)
	require.Contains(t, string(pgSrc), "idx_memories_submitting_agent ON memories (submitting_agent, confidence_score)",
		"postgres must declare the submitting_agent index too")
}

// TestCorroborationIndexDeclaredOnBothBackends mirrors the engram-index guard for
// idx_corroborations_memory_order. The corroborations child FK column is not
// auto-indexed by sqlite, so the CEREBRUM distributed-engram per-memory read
// (and GetCorroborationCounts) would full-scan without it. It must be declared for
// NEW databases (initSchema) AND EXISTING ones (the migration mirror) on sqlite,
// and on postgres.
func TestCorroborationIndexDeclaredOnBothBackends(t *testing.T) {
	const idx = "idx_corroborations_memory_order"
	sqlSrc, err := os.ReadFile("sqlite.go")
	require.NoError(t, err)
	require.GreaterOrEqual(t, strings.Count(string(sqlSrc), idx), 2,
		"sqlite must declare the corroborations index in BOTH initSchema and the migration mirror")
	require.Contains(t, string(sqlSrc),
		"WHERE memory_id = ? ORDER BY created_at, agent_id, id LIMIT ?",
		"sqlite must apply the total order and row bound in SQL")

	pgSrc, err := os.ReadFile("postgres.go")
	require.NoError(t, err)
	require.Contains(t, string(pgSrc), "idx_corroborations_memory_order ON corroborations (memory_id, created_at, agent_id, id)",
		"postgres must declare the corroborations index too")
	require.Contains(t, string(pgSrc),
		"ORDER BY created_at, agent_id, id LIMIT $2",
		"postgres must keep the same total order and database-side row bound")

	// The UNBOUNDED GetCorroborations read must carry the SAME total order. The `, memoryID)`
	// tail (no LIMIT parameter) distinguishes it from the bounded sibling above; reverting it
	// to plain created_at reintroduces the same-block nondeterminism this closes. (The SQLite
	// behaviour is planner-masked by its INDEXED BY hint, so the deterministic same-timestamp
	// ordering is exercised end-to-end against real PostgreSQL in the integration test.)
	const unboundedOrder = "ORDER BY created_at, agent_id, id`, memoryID)"
	require.Contains(t, string(sqlSrc), unboundedOrder,
		"sqlite GetCorroborations (unbounded) must keep the total order")
	require.Contains(t, string(pgSrc), unboundedOrder,
		"postgres GetCorroborations (unbounded) must keep the total order")
	require.Contains(t, string(sqlSrc),
		"FROM corroborations INDEXED BY idx_corroborations_memory_order\n\t\tWHERE memory_id = ? ORDER BY created_at, agent_id, id`",
		"sqlite GetCorroborations must pin the composite index so the order needs no temp sort")
}

// TestCorroborationIndexServesPerMemoryRead pins the bounded CEREBRUM query:
// it must seek one memory, satisfy the total order from the composite index,
// and apply LIMIT without a table scan or temporary sort.
//
// Like the engram-index test, this deliberately does NOT ANALYZE: SAGE never runs
// ANALYZE, so a real node has no sqlite_stat1. Pinning the production query here
// catches a regression to a scan or a temporary sort (the former needs the same
// INDEXED BY protection learned in PR #181).
func TestCorroborationIndexServesPerMemoryRead(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	at := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("m%03d", i)
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "author", "content-"+id, "dom")))
	}
	for i := 0; i < 200; i++ {
		require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
			MemoryID:  fmt.Sprintf("m%03d", i%20),
			AgentID:   fmt.Sprintf("agent-%d", i),
			CreatedAt: at.Add(time.Duration(i) * time.Second),
		}))
	}

	rows, err := s.conn.QueryContext(ctx,
		`EXPLAIN QUERY PLAN SELECT id, memory_id, agent_id, evidence, created_at
		 FROM corroborations INDEXED BY idx_corroborations_memory_order
		 WHERE memory_id = ? ORDER BY created_at, agent_id, id LIMIT ?`,
		"m003", 12)
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var plan string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())

	require.Contains(t, plan, "idx_corroborations_memory_order",
		"the per-memory corroborator read must SEARCH via the index, not a full scan; plan was:\n"+plan)
	require.NotContains(t, plan, "SCAN corroborations",
		"a full scan of corroborations per engram is exactly what the index must prevent; plan was:\n"+plan)
	require.NotContains(t, plan, "USE TEMP B-TREE",
		"the composite index must satisfy the deterministic order without a temp sort; plan was:\n"+plan)
}

func TestGetCorroborationsBoundedCapsAndTotallyOrdersRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	at := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, s.InsertMemory(ctx, testMemory("m1", "author", "content-m1", "dom")))
	for i := 19; i >= 0; i-- {
		require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
			MemoryID: "m1", AgentID: fmt.Sprintf("agent-%02d", i), CreatedAt: at,
		}))
	}

	got, err := s.GetCorroborationsBounded(ctx, "m1", 12)
	require.NoError(t, err)
	require.Len(t, got, 12, "the database result itself must be capped")
	for i, corr := range got {
		require.Equal(t, fmt.Sprintf("agent-%02d", i), corr.AgentID,
			"same-timestamp rows must use agent_id as a stable tie-breaker")
	}
	_, err = s.GetCorroborationsBounded(ctx, "m1", 0)
	require.Error(t, err, "an absent SQL bound must fail closed")
}

// TestGetCorroborationsTotallyOrdered pins the UNBOUNDED GetCorroborations read to the same
// deterministic total order (created_at, agent_id, id) as its bounded sibling. created_at
// alone is not a total order — corroborations committed in one block share the block's
// canonical timestamp — so without the agent_id/id tiebreak the row order is arbitrary and
// diverges across SQLite/Postgres and across runs. The detail endpoint that renders these
// rows must not reshuffle between reads. Removing the tiebreak fails this.
func TestGetCorroborationsTotallyOrdered(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	at := time.Unix(1_700_000_000, 0).UTC()
	require.NoError(t, s.InsertMemory(ctx, testMemory("m1", "author", "content-m1", "dom")))
	// All 20 share one timestamp, inserted in REVERSE, so only the tiebreak yields a stable order.
	for i := 19; i >= 0; i-- {
		require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
			MemoryID: "m1", AgentID: fmt.Sprintf("agent-%02d", i), CreatedAt: at,
		}))
	}

	got, err := s.GetCorroborations(ctx, "m1")
	require.NoError(t, err)
	require.Len(t, got, 20)
	for i, corr := range got {
		require.Equal(t, fmt.Sprintf("agent-%02d", i), corr.AgentID,
			"same-timestamp rows must be ordered by the agent_id tiebreak, not arbitrary order")
	}

	names := func(cs []*Corroboration) []string {
		out := make([]string, len(cs))
		for i, c := range cs {
			out[i] = c.AgentID
		}
		return out
	}
	again, err := s.GetCorroborations(ctx, "m1")
	require.NoError(t, err)
	require.Equal(t, names(got), names(again), "the unbounded read must be a stable total order")
}

// TestGetCorroborationsServesIndexNoTempSort pins the unbounded read's plan: it must SEARCH
// via idx_corroborations_memory_order and satisfy ORDER BY without a temp b-tree — the same
// INDEXED BY protection the bounded read carries, verified WITHOUT ANALYZE (SAGE never runs
// ANALYZE, per PR #181). Without the INDEXED BY hint the total order regresses to a temp sort.
func TestGetCorroborationsServesIndexNoTempSort(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	at := time.Unix(1_700_000_000, 0).UTC()
	for i := 0; i < 20; i++ {
		require.NoError(t, s.InsertMemory(ctx, testMemory(fmt.Sprintf("m%03d", i), "author", "c", "dom")))
	}
	for i := 0; i < 200; i++ {
		require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
			MemoryID: fmt.Sprintf("m%03d", i%20), AgentID: fmt.Sprintf("agent-%d", i),
			CreatedAt: at.Add(time.Duration(i) * time.Second),
		}))
	}

	rows, err := s.conn.QueryContext(ctx,
		`EXPLAIN QUERY PLAN SELECT id, memory_id, agent_id, evidence, created_at
		 FROM corroborations INDEXED BY idx_corroborations_memory_order
		 WHERE memory_id = ? ORDER BY created_at, agent_id, id`, "m003")
	require.NoError(t, err)
	defer rows.Close() //nolint:errcheck
	var plan string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		plan += detail + "\n"
	}
	require.NoError(t, rows.Err())

	require.Contains(t, plan, "idx_corroborations_memory_order",
		"the unbounded corroborator read must SEARCH via the composite index; plan was:\n"+plan)
	require.NotContains(t, plan, "SCAN corroborations",
		"a full scan is what the index must prevent; plan was:\n"+plan)
	require.NotContains(t, plan, "USE TEMP B-TREE",
		"the composite index must satisfy the order without a temp sort; plan was:\n"+plan)
}

// The CEREBRUM agent-as-lobe read is `WHERE submitting_agent = ? [AND status = ?]
// ORDER BY confidence_score DESC LIMIT N`. idx_memories_submitting_agent
// (submitting_agent, confidence_score) must satisfy BOTH the equality and the
// order — no full table scan, no temp b-tree sort.
//
// Crucially this must hold WITHOUT ANALYZE: SAGE never runs ANALYZE, so
// sqlite_stat1 is absent on a real node, and a new index can be silently ignored
// in favour of another low-cardinality index plus a temp sort (learned on the
// pipe connectome index, PR #181). This test deliberately does not ANALYZE,
// mirroring production; if the plan ever regresses to a scan/temp-sort the query
// needs an INDEXED BY hint.
func TestEngramIndexServesAgentTopByConfidence(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Populate across several agents and statuses so the planner has a realistic
	// table (and idx_memories_status exists as a competing candidate).
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("m%03d", i)
		agent := fmt.Sprintf("agent-%d", i%8)
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, agent, "content-"+id, "dom")))
	}

	queryPlan := func(sql string) string {
		rows, err := s.conn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+sql, "agent-3", "committed")
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck
		var plan string
		for rows.Next() {
			var id, parent, notUsed int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
			plan += detail + "\n"
		}
		require.NoError(t, rows.Err())
		return plan
	}

	// Both shapes ListMemories actually issues for the lobe: the legacy path runs
	// the plain confidence order; the appV23 path adds StablePaging's memory_id
	// tiebreak. Both must SEARCH via the index (a per-agent seek, not a full scan).
	legacy := queryPlan(`SELECT memory_id FROM memories WHERE submitting_agent = ? AND status = ? ORDER BY confidence_score DESC LIMIT 24`)
	appv23 := queryPlan(`SELECT memory_id FROM memories WHERE submitting_agent = ? AND status = ? ORDER BY confidence_score DESC, memory_id ASC LIMIT 512`)

	for name, plan := range map[string]string{"legacy": legacy, "appv23 StablePaging": appv23} {
		require.Contains(t, plan, "idx_memories_submitting_agent",
			name+" agent-lobe query must SEARCH via the submitting_agent index, not a full scan; plan was:\n"+plan)
	}
	// The legacy shape is fully index-ordered — no temp sort at all. (The appV23
	// shape may add a small sort only for the memory_id tiebreak within equal
	// confidence, over the one agent's already index-selected rows — not a scan.)
	require.NotContains(t, legacy, "TEMP B-TREE",
		"the index order must satisfy ORDER BY confidence_score — no temp sort; plan was:\n"+legacy)
}
