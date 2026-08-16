//go:build integration

package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// TestPostgresGetCorroborationsBoundedCapsAndTotallyOrdersRows exercises the
// production PostgreSQL query, not a source-string mirror. It pins database-side
// LIMIT, deterministic same-timestamp ordering, and fail-closed invalid limits.
func TestPostgresGetCorroborationsBoundedCapsAndTotallyOrdersRows(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	memoryID := uuid.NewString()
	content := "postgres bounded engram " + memoryID
	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: "engram-postgres-test",
		Content:         content,
		ContentHash:     memory.ComputeContentHash(content),
		MemoryType:      memory.TypeFact,
		DomainTag:       "engram-postgres-test",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(ctx, record))
	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM corroborations WHERE memory_id = $1`, memoryID)
		_, _ = s.db.Exec(ctx, `DELETE FROM memories WHERE memory_id = $1`, memoryID)
	})

	at := time.Unix(1_700_000_000, 0).UTC()
	for i := 19; i >= 0; i-- {
		require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
			MemoryID:  memoryID,
			AgentID:   fmt.Sprintf("agent-%02d", i),
			Evidence:  fmt.Sprintf("evidence-%02d", i),
			CreatedAt: at,
		}))
	}

	got, err := s.GetCorroborationsBounded(ctx, memoryID, 12)
	require.NoError(t, err)
	require.Len(t, got, 12, "PostgreSQL must apply LIMIT before returning rows")
	for i, corr := range got {
		require.Equal(t, fmt.Sprintf("agent-%02d", i), corr.AgentID,
			"same-timestamp PostgreSQL rows must use agent_id as the next tie-breaker")
	}
	for _, invalid := range []int{0, -1} {
		_, err = s.GetCorroborationsBounded(ctx, memoryID, invalid)
		require.Error(t, err, "invalid PostgreSQL bounds must fail closed")
	}
}

// TestPostgresGetCorroborationsTotallyOrdersRows pins the UNBOUNDED production query's total
// order on real PostgreSQL — where, unlike SQLite's INDEXED BY hint, nothing forces the
// composite index, so the ORDER BY tiebreak itself is what makes same-block rows deterministic.
// Reverting created_at, agent_id, id to plain created_at returns same-timestamp rows in
// arbitrary heap order and fails this — the load-bearing regression guard for the fix.
func TestPostgresGetCorroborationsTotallyOrdersRows(t *testing.T) {
	s := setupTestStore(t)
	ctx := context.Background()
	memoryID := uuid.NewString()
	content := "postgres unbounded engram " + memoryID
	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: "engram-postgres-test",
		Content:         content,
		ContentHash:     memory.ComputeContentHash(content),
		MemoryType:      memory.TypeFact,
		DomainTag:       "engram-postgres-test",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().UTC(),
	}
	require.NoError(t, s.InsertMemory(ctx, record))
	t.Cleanup(func() {
		_, _ = s.db.Exec(ctx, `DELETE FROM corroborations WHERE memory_id = $1`, memoryID)
		_, _ = s.db.Exec(ctx, `DELETE FROM memories WHERE memory_id = $1`, memoryID)
	})

	at := time.Unix(1_700_000_000, 0).UTC()
	for i := 19; i >= 0; i-- {
		require.NoError(t, s.InsertCorroboration(ctx, &Corroboration{
			MemoryID:  memoryID,
			AgentID:   fmt.Sprintf("agent-%02d", i),
			Evidence:  fmt.Sprintf("evidence-%02d", i),
			CreatedAt: at,
		}))
	}

	got, err := s.GetCorroborations(ctx, memoryID)
	require.NoError(t, err)
	require.Len(t, got, 20)
	for i, corr := range got {
		require.Equal(t, fmt.Sprintf("agent-%02d", i), corr.AgentID,
			"same-timestamp PostgreSQL rows must be ordered by the agent_id tiebreak, not heap order")
	}
}
