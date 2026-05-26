//go:build integration

package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

// These tests require a running PostgreSQL instance with pgvector.
// Run with: go test -tags=integration ./internal/store/...

func setupTestStore(t *testing.T) *PostgresStore {
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, agentTestDSN()) // honors SAGE_TEST_POSTGRES_DSN
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func TestInsertGetMemory(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	record := &memory.MemoryRecord{
		MemoryID:        uuid.NewString(), // memory_id is a UUID column
		SubmittingAgent: "agent-test",
		Content:         "test memory content",
		ContentHash:     memory.ComputeContentHash("test memory content"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.85,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	err := store.InsertMemory(ctx, record)
	require.NoError(t, err)

	got, err := store.GetMemory(ctx, record.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, record.MemoryID, got.MemoryID)
	assert.Equal(t, record.Content, got.Content)
	assert.Equal(t, record.MemoryType, got.MemoryType)
}

func TestPostgresUpdateStatus(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	id := uuid.NewString()
	record := &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "agent-test",
		Content:         "status test",
		ContentHash:     memory.ComputeContentHash("status test"),
		MemoryType:      memory.TypeObservation,
		DomainTag:       "challenge_generation",
		ConfidenceScore: 0.7,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}

	require.NoError(t, store.InsertMemory(ctx, record))
	require.NoError(t, store.UpdateStatus(ctx, id, memory.StatusCommitted, time.Now()))

	got, err := store.GetMemory(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, memory.StatusCommitted, got.Status)
	assert.NotNil(t, got.CommittedAt)
}

func TestVotes(t *testing.T) {
	store := setupTestStore(t)
	ctx := context.Background()

	memID := uuid.NewString()
	record := &memory.MemoryRecord{
		MemoryID:        memID,
		SubmittingAgent: "agent-test",
		Content:         "vote test",
		ContentHash:     memory.ComputeContentHash("vote test"),
		MemoryType:      memory.TypeFact,
		DomainTag:       "crypto",
		ConfidenceScore: 0.9,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now(),
	}
	require.NoError(t, store.InsertMemory(ctx, record))

	vote := &ValidationVote{
		MemoryID:     memID,
		ValidatorID:  "validator-1",
		Decision:     "accept",
		Rationale:    "looks good",
		WeightAtVote: 0.25,
		BlockHeight:  100,
		CreatedAt:    time.Now(),
	}
	require.NoError(t, store.InsertVote(ctx, vote))

	votes, err := store.GetVotes(ctx, memID)
	require.NoError(t, err)
	assert.Len(t, votes, 1)
	assert.Equal(t, "accept", votes[0].Decision)
}
