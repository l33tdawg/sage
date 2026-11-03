package store

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DomainSpaceCompleteness is the completeness check behind an empty recall's
// index_status. It carries NO per-record authorization (the handler only calls it
// for a caller proven to see every record in the domain), so these tests pin the
// pure space/status/bounding behaviour: (1) unindexed and foreign-space rows are
// out-of-space, (2) a domain fully in the active space is complete, (3) it mirrors
// recall's status visibility (challenged in, deprecated out), and (4) it refuses
// to prove absence over a domain larger than the bound (established=false), while
// still settling incompleteness cheaply from a single out-of-space row.
func TestDomainSpaceCompleteness(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const active = "openai-compatible:gte-Qwen2-1.5B-instruct:1536"
	const cap = 8192

	setStatus := func(id, status string) {
		_, err := s.conn.ExecContext(ctx, `UPDATE memories SET status = ? WHERE memory_id = ?`, status, id)
		require.NoError(t, err)
	}
	mk := func(id, domain, status, provider string, withEmbedding bool) {
		require.NoError(t, s.InsertMemory(ctx, testMemory(id, "agent", "c-"+id, domain)))
		setStatus(id, status)
		if withEmbedding {
			require.NoError(t, s.UpdateMemoryEmbedding(ctx, id, []float32{0.1, 0.2, 0.3}, provider))
		}
	}

	t.Run("an unindexed row is out-of-space", func(t *testing.T) {
		mk("u1", "d1", "committed", "", false)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d1", active, cap)
		require.NoError(t, err)
		assert.True(t, est)
		assert.True(t, out)
	})

	t.Run("a foreign-space row is out-of-space", func(t *testing.T) {
		mk("f1", "d2", "committed", "hash", true)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d2", active, cap)
		require.NoError(t, err)
		assert.True(t, est)
		assert.True(t, out)
	})

	t.Run("a domain fully in the active space is complete", func(t *testing.T) {
		mk("a1", "d3", "committed", active, true)
		mk("a2", "d3", "committed", active, true)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d3", active, cap)
		require.NoError(t, err)
		assert.True(t, est)
		assert.False(t, out)
	})

	t.Run("one out-of-space row among active rows makes it incomplete", func(t *testing.T) {
		mk("m1", "d4", "committed", active, true)
		mk("m2", "d4", "committed", "hash", true)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d4", active, cap)
		require.NoError(t, err)
		assert.True(t, est)
		assert.True(t, out)
	})

	t.Run("challenged-but-live counts, deprecated does not", func(t *testing.T) {
		mk("ch", "d5", "challenged", "hash", true)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d5", active, cap)
		require.NoError(t, err)
		assert.True(t, est)
		assert.True(t, out, "challenged-but-live rows are in recall's visible set")

		// A domain whose ONLY out-of-space row is deprecated is complete: deprecated
		// rows are not recall candidates, so they never make an empty recall suspect.
		mk("dep", "d6", "deprecated", "hash", true)
		out, est, err = s.DomainSpaceCompleteness(ctx, "d6", active, cap)
		require.NoError(t, err)
		assert.True(t, est)
		assert.False(t, out)
	})

	t.Run("a domain larger than the bound cannot prove absence", func(t *testing.T) {
		// cap+1 in-space rows: the bounded prefix fills without an out-of-space row,
		// so completeness cannot be proven -> established=false (handler: unavailable).
		mk("b1", "d7", "committed", active, true)
		mk("b2", "d7", "committed", active, true)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d7", active, 1)
		require.NoError(t, err)
		assert.False(t, est, "two in-space rows under a bound of one cannot settle absence")
		assert.False(t, out)
	})

	t.Run("incompleteness still settles cheaply under a tight bound", func(t *testing.T) {
		// An out-of-space row within the bounded prefix settles incomplete even when
		// the domain is larger than the bound -- an existence proof, not a scan.
		mk("t1", "d8", "committed", "hash", true)
		mk("t2", "d8", "committed", active, true)
		out, est, err := s.DomainSpaceCompleteness(ctx, "d8", active, 1)
		require.NoError(t, err)
		assert.True(t, est)
		assert.True(t, out)
	})
}

func TestDomainSpaceCompletenessUsesLiveDomainIndex(t *testing.T) {
	s := newTestStore(t)
	rows, err := s.conn.QueryContext(context.Background(), `
		EXPLAIN QUERY PLAN
		SELECT CASE WHEN embedding IS NULL THEN 1 ELSE 0 END,
		       COALESCE(embedding_provider, '')
		FROM memories
		WHERE domain_tag = ? AND status IN ('committed','challenged')
		ORDER BY status, memory_id
		LIMIT ?`, "bounded", 8193)
	require.NoError(t, err)
	defer rows.Close()

	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	require.NoError(t, rows.Err())
	assert.Contains(t, plan.String(), "idx_memories_domain_live_status",
		"the cap must bound examined live rows, not only returned rows")
}
